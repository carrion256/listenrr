//! `listenrr` binary: config load, runtime, task wiring, graceful shutdown (plan S1.1).
//!
//! Subcommands:
//! - `run` — the service: ingest (async), the KWS thread ([`kws::run_kws_thread`] off the
//!   runtime, plan principle 3), the coordinator loop, capture, transcription, the verify
//!   gate, HA, clips and the retention sweep.
//! - `replay <wav>` — the offline tuning instrument (S3.4).
//! - `bench-kws --model --wav` — the KWS realtime-factor gate (S2.4, AC3).
//! - `health` — validate the config and exit 0 (a systemd `ExecStartPre` hook).
//!
//! [`Wiring::dispatch`] is the wiring contract between the coordinator and the outside
//! world: every [`CoordOut`] arm performs real work or spawns the task that does, so the
//! coordinator loop itself never blocks on a socket, a model or a disk write.
//!
//! # The sample domain (R-A)
//!
//! Every span this binary passes around — a wake's `kw_span`, a capture's
//! `utterance_start..endpoint`, a clip's padding — is in the ring's **write cursor**
//! domain, i.e. [`SourceRing::written`], *not* `seq × 1024`: the ring is append-only, so
//! a sequence gap compresses rather than leaving a hole. `seq` counts frames for gap
//! detection and dropped-frame accounting only. Capture therefore reads the ring by index
//! on a poll (it is the authoritative store — the frame broadcast is the convenience), and
//! a reconnect realigns both clocks because `register()` replaces the ring and ingest's
//! restart marker rebases the spotter's counter ([`kws::SampleCounters::rebase`]).

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use clap::{Parser, Subcommand};
use tokio::sync::{broadcast, mpsc, watch};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use listenrr::capture::{
    backward_bounds_with_preroll, EndpointMode, EndpointReason, ForwardEndpointer, SileroVad,
    VadEngine,
};
use listenrr::clips::{self, StageLatencies, TriggerReason, TriggerRecord};
use listenrr::config::{Config, Playback, Source};
use listenrr::coordinator::{CoordIn, CoordOut, Coordinator, LatencyStage, Session, SystemClock};
use listenrr::ha::{HaClient, PipelineRequest};
use listenrr::ingest::{self, ControlSend, IngestEvent, SourceHandle, SourceRegistry};
use listenrr::kws::{self, KwsEngine, WakeEvent};
use listenrr::parakeet::{self, WyomingClient};
use listenrr::ring::with_ring;
use listenrr::verify;
use listenrr::{ms_to_samples, proto, SAMPLE_RATE_HZ};

/// Default config path, matching the deployment layout (`config/listenrr.toml`).
const DEFAULT_CONFIG: &str = "config/listenrr.toml";
/// Coordinator pump interval: fine relative to the 250 ms arbitration window, cheap
/// enough to run beside other services on a small host.
const COORDINATOR_TICK: Duration = Duration::from_millis(100);
/// How often a capture task re-reads its ring (plan invariant: the ring is the store).
/// Well under a 64 ms frame, so an endpoint is never late by more than one poll.
const CAPTURE_POLL: Duration = Duration::from_millis(20);
/// Extra wall-clock grace past `hard_cap_s` before a capture gives up on its source.
const CAPTURE_GRACE: Duration = Duration::from_secs(5);
/// How long a follow-up listener waits after `playback_done` before it starts reading. The
/// speaker has a ~100 ms output buffer and the room rings briefly after it, and there is no
/// echo cancellation (`has_aec = false`), so listening at once would hear Luna's own tail as
/// the user starting to speak.
const FOLLOWUP_SETTLE: Duration = Duration::from_millis(400);
/// Coordinator input queue depth: the same order as ingest's event queue.
const COORD_IN_CAPACITY: usize = 256;
/// How long either shutdown path waits for the KWS thread to finish (L2/L3). The
/// thread ends when the last KWS sender drops, and per-connection tasks inside
/// ingest hold clones of it; a satellite that keeps streaming must not hold the
/// process exit hostage — the unit allows the whole stop `TimeoutStopSec=30`,
/// this takes a few seconds of it and logs, rather than hangs, on expiry.
const KWS_JOIN_GRACE: Duration = Duration::from_secs(5);
/// Daily clip retention sweep (S8.1, AC9).
const RETENTION_INTERVAL: Duration = Duration::from_secs(24 * 3600);

#[derive(Parser)]
#[command(
    name = "listenrr",
    version,
    about = "Luna voice backend: central wake detection, capture, verify gate and HA Assist entry"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the service: ingest + KWS thread + coordinator.
    Run {
        /// Path to the TOML config.
        #[arg(long, default_value = DEFAULT_CONFIG)]
        config: PathBuf,
    },
    /// Replay a WAV through KWS → capture → transcribe → verify (offline, no HA).
    Replay {
        /// 16 kHz mono PCM WAV to replay.
        wav: PathBuf,
        /// Path to the TOML config (windows, model, ASR endpoint).
        #[arg(long, default_value = DEFAULT_CONFIG)]
        config: PathBuf,
        /// Override `[kws] model` for this run.
        #[arg(long)]
        model: Option<PathBuf>,
    },
    /// Benchmark the keyword spotter's realtime factor (AC3).
    BenchKws {
        /// KWS model directory.
        #[arg(long)]
        model: PathBuf,
        /// 16 kHz mono PCM WAV to decode.
        #[arg(long)]
        wav: PathBuf,
    },
    /// Validate the configuration and exit 0 — a systemd `ExecStartPre`/watchdog hook.
    ///
    /// Checks the file parses and passes [`Config::validate`], that the clip directory
    /// exists and is writable by this user, and that the VAD model every wake turn
    /// loads actually loads. It opens no sockets and contacts no service, so it is
    /// safe to run before HA or Parakeet is up.
    Health {
        /// Path to the TOML config.
        #[arg(long, default_value = DEFAULT_CONFIG)]
        config: PathBuf,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    init_tracing();
    match cli.command {
        Command::Run { config } => {
            let cfg = load_config(&config)?;
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .context("building the tokio runtime")?;
            rt.block_on(run_service(cfg))
        }
        Command::Replay { wav, config, model } => {
            let cfg = load_config(&config)?;
            let model_dir = match &model {
                Some(dir) => {
                    // `--model` replaces the transducer bundle, and replay.rs now
                    // resolves every model path it owns against it — but
                    // `keywords_file` is not a model path: it is a tokenized
                    // artifact of whatever bundle it was produced for, consumed
                    // verbatim. An override that leaves it behind scores the new
                    // model against the old vocabulary, and the battery reports
                    // `woke=false` with a green exit — the exact false negative
                    // `config/listenrr.toml.example` warns about. Say it loudly.
                    if !Path::new(&cfg.kws.keywords_file).starts_with(dir) {
                        tracing::warn!(
                            model = %dir.display(),
                            keywords_file = %cfg.kws.keywords_file,
                            "--model overrides the model bundle; the keyword list is \
                             still the one tokenized for `[kws] model` ({}). Keywords \
                             the new model never emits will simply not fire — pass a \
                             keywords_file produced for the new bundle (sherpa-onnx \
                             text2token) to judge this model fairly.",
                            cfg.kws.model
                        );
                    }
                    dir.clone()
                }
                None => PathBuf::from(&cfg.kws.model),
            };
            replay_battery(&model_dir, &wav, &cfg)
        }
        Command::BenchKws { model, wav } => {
            let report = kws::bench(&model, &wav)?;
            let text =
                serde_json::to_string_pretty(&report).context("serializing the bench report")?;
            println!("{text}");
            if report.passes_ac3 {
                Ok(())
            } else {
                anyhow::bail!(
                    "AC3 gate failed: {}x realtime is below the {}x floor",
                    report.realtime_factor,
                    kws::BENCH_MIN_REALTIME_FACTOR
                );
            }
        }
        Command::Health { config } => {
            let cfg = load_config(&config)?;
            // The two promises the service makes at startup that `Config::validate`
            // cannot answer from the file alone: clips must land, and every wake
            // turn must be able to load its VAD. Both are checked for real here so
            // the ExecStartPre says what it checks (L9/L10).
            probe_clips_dir(Path::new(&cfg.clips.dir))
                .with_context(|| format!("clip directory {} is not usable", cfg.clips.dir))?;
            probe_vad_model(&cfg)?;
            println!(
                "ok: config valid, {} source(s), {} group(s), listening on {}",
                cfg.sources.len(),
                cfg.groups.len(),
                cfg.server.listen
            );
            Ok(())
        }
    }
}

/// Run the offline chain over one WAV, or every WAV in a directory, and print each report
/// (S3.4). A directory is the AC4 fixture battery: one line of JSON per clip (that is the
/// record `docs/design/TEST-NOTES.md` quotes), then a table an operator can read, then the worst
/// utterance-end margin, which is the number `trailing_silence_ms` tuning moves.
///
/// The AC4 clauses are counters, not absences of evidence: a rejected transcript that still
/// reached HA, or an accepted one that did not, is a defect in the chain and exits non-zero.
/// A run that cannot fail is not an instrument.
fn replay_battery(model_dir: &Path, target: &Path, cfg: &Config) -> anyhow::Result<()> {
    let clips = wavs_to_replay(target)?;
    anyhow::ensure!(
        !clips.is_empty(),
        "{} contains no 16 kHz mono WAV to replay",
        target.display()
    );

    let mut rows: Vec<String> = Vec::new();
    let mut woke = 0usize;
    let mut accepted = 0usize;
    let mut worst_margin_ms = 0u64;
    let mut worst_clip = String::new();

    for clip in &clips {
        let name = clip
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("<clip>")
            .to_string();
        let report = listenrr::replay::run(model_dir, clip, cfg)
            .with_context(|| format!("replaying {}", clip.display()))?;
        println!(
            "{}",
            serde_json::to_string(&report).context("serializing the replay report")?
        );

        if report.woke {
            woke += 1;
        }
        if report.verify_passed {
            accepted += 1;
            if report.transcript.is_none() {
                anyhow::bail!("{name}: the verify gate accepted a transcript that does not exist");
            }
            if report.ha_calls != 1 {
                anyhow::bail!(
                    "{name}: an accepted transcript reached the pipeline {} time(s), expected 1 \
                     (plan C2-1)",
                    report.ha_calls
                );
            }
        } else if report.ha_calls != 0 {
            anyhow::bail!(
                "{name}: the verify gate rejected {:?} and HA was still called {} time(s) \
                 (AC4's negative clause)",
                report.transcript,
                report.ha_calls
            );
        }
        if let Some(margin) = report.utterance_end_margin_ms {
            if margin > worst_margin_ms {
                worst_margin_ms = margin;
                worst_clip.clone_from(&name);
            }
        }
        rows.push(format!(
            "{name:<28} woke={:<5} kw={:<14} bounds={:<16} last_speech={:<9} verify={:<6} \
             ha={} margin={:>5}ms  {}",
            report.woke,
            fmt_span(report.kw_span.as_ref()),
            fmt_span(report.utterance_bounds.as_ref()),
            report
                .last_speech
                .map_or_else(|| "-".to_string(), |v| v.to_string()),
            if report.verify_passed {
                "pass"
            } else {
                "reject"
            },
            report.ha_calls,
            report.utterance_end_margin_ms.unwrap_or(0),
            report.transcript.as_deref().unwrap_or("-"),
        ));
    }

    println!(
        "\n{:<28} {:<10} {:<15} {:<17} {:<10} {:<7} {:<3} {:>7}  transcript",
        "clip", "woke", "kw_span", "utterance", "last_speech", "verify", "ha", "margin"
    );
    for row in &rows {
        println!("{row}");
    }
    println!(
        "\n{} clip(s): {woke} woke, {accepted} passed the verify gate; \
         max(|margin|) = {worst_margin_ms}ms ({})",
        clips.len(),
        if worst_clip.is_empty() {
            "no utterance was endpointed"
        } else {
            &worst_clip
        },
    );
    Ok(())
}

/// A span as `a..b`, or `-` when the stage never ran.
fn fmt_span(span: Option<&std::ops::Range<u64>>) -> String {
    span.map_or_else(|| "-".to_string(), |s| format!("{}..{}", s.start, s.end))
}

/// The WAVs to replay: one file, or every `*.wav` in a directory, sorted by name so a
/// battery reads the same way twice.
fn wavs_to_replay(target: &Path) -> anyhow::Result<Vec<PathBuf>> {
    if !target.is_dir() {
        return Ok(vec![target.to_path_buf()]);
    }
    let mut wavs = Vec::new();
    for entry in
        std::fs::read_dir(target).with_context(|| format!("reading {}", target.display()))?
    {
        let entry = entry.with_context(|| format!("reading {}", target.display()))?;
        let path = entry.path();
        if path
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("wav"))
        {
            wavs.push(path);
        }
    }
    wavs.sort();
    Ok(wavs)
}

fn load_config(path: &PathBuf) -> anyhow::Result<Config> {
    Config::load(path).with_context(|| format!("loading config {}", path.display()))
}

/// The `health` half of the clips promise: the directory exists **and** this user
/// can create a file in it. `create_dir_all` answers `Ok` for an existing
/// directory whatever its mode — a `0555` or root-owned clips dir used to pass
/// `health` and then fail the first false-accept clip with `Permission denied`
/// (L10). The probe file is removed again; `health` leaves nothing behind.
fn probe_clips_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let probe = dir.join(".listenrr-health-probe");
    std::fs::write(&probe, b"health").and_then(|()| std::fs::remove_file(&probe))
}

/// The `health` half of the capture promise (L9): the VAD model every wake turn
/// loads is loadable now, not discovered broken on the first household wake —
/// where the failure mode was a per-wake warning and a session held to
/// `hard_cap_s`. This is the same load [`run_capture`] performs, so a typo'd
/// `capture.vad_model` or a KWS bundle without the VAD file fails the
/// `ExecStartPre` with the path that needs fixing.
fn probe_vad_model(cfg: &Config) -> anyhow::Result<()> {
    let model = vad_model_path(cfg);
    // The point is only that it loads; the instance is worth nothing after that,
    // so the value is dropped at the end of the statement.
    SileroVad::new(&model)
        .with_context(|| format!("loading the Silero VAD from {}", model.display()))?;
    Ok(())
}

/// Structured JSON logs, filtered by `RUST_LOG` (default `info`) — plan S1.1, AC5.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt = tracing_subscriber::fmt::layer()
        .json()
        .with_target(true)
        .with_writer(std::io::stderr);
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .try_init();
}

/// Start the service and run until ingest stops or Ctrl-C arrives (plan S1.1: a backend
/// restart must be safe — satellites reconnect with backoff, no reflash).
async fn run_service(cfg: Config) -> anyhow::Result<()> {
    if cfg.server.auth_token.is_none() {
        // R11: accepted, but say it loudly once at startup.
        tracing::warn!(
            "server.auth_token is NOT set: any LAN peer can register a source_id and \
             drive HA intents under this token. Set server.auth_token for the satellite \
             path (a VPN overlay cannot carry the ESP32 satellites)."
        );
    }
    tracing::info!(
        listen = %cfg.server.listen,
        sources = cfg.sources.len(),
        groups = cfg.groups.len(),
        allow_unknown_sources = cfg.server.allow_unknown_sources,
        "listenrr starting"
    );

    let registry = Arc::new(ingest::SourceRegistry::new(&cfg));

    // KWS channel (bounded, drop-oldest + counter — plan S1.4, R9) and the per-source
    // frame broadcast capture subscribes to.
    let (kws_tx, kws_rx) =
        crossbeam_channel::bounded::<(String, Arc<[i16]>)>(ingest::KWS_CHANNEL_CAPACITY);
    let (frame_tx, _frames) =
        broadcast::channel::<(String, Arc<[i16]>)>(ingest::FRAME_BROADCAST_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel::<IngestEvent>(ingest::EVENT_QUEUE_CAPACITY);
    let (wake_tx, wake_rx) = mpsc::channel::<WakeEvent>(64);
    let (suppress_tx, suppress_rx) = watch::channel::<HashSet<String>>(HashSet::new());
    let (stop_tx, stop_rx) = mpsc::channel::<kws::StopEvent>(64);
    // The stop lane's listen set (plan 4.2): which sources' audio the stop spotter may
    // see. Derived by the coordinator, published on every transition that changes it.
    let (stop_listen_tx, stop_listen_rx) = watch::channel::<HashSet<String>>(HashSet::new());
    let (mute_tx, mute_rx) = mpsc::channel::<(String, bool)>(32);
    // The single input the coordinator loop reads: wake, ingest, mute, tick and every
    // result a spawned task produced all arrive here, which is what lets `dispatch`
    // return immediately instead of waiting on a socket.
    let (coord_tx, mut coord_rx) = mpsc::channel::<CoordIn>(COORD_IN_CAPACITY);

    // One dedicated KWS thread (plan principle 3): the runtime never runs DSP.
    let engine = spotter(&cfg);
    let stop_engine = stop_spotter(&cfg);
    let kws_thread = std::thread::Builder::new()
        .name("kws".to_string())
        .spawn(move || {
            kws::run_kws_thread(
                kws_rx,
                wake_tx,
                suppress_rx,
                engine,
                kws::StopLane::new(stop_engine, stop_listen_rx, stop_tx),
            )
        })
        .context("spawning the KWS thread")?;
    // The KWS thread is watched from the moment it starts, not only joined at exit:
    // the watcher owns the `JoinHandle` and reports the thread's result exactly
    // once, which is what both the supervision arm of the select below and the
    // bounded shutdown drain read (L2/L3/L12). A panic here used to leave this
    // process up, listening, and completely deaf, with nothing to restart it.
    let mut kws_died = watch_kws_thread(kws_thread)?;

    let ha = HaClient::new(
        cfg.ha.url.clone(),
        cfg.ha.token.clone(),
        cfg.ha.pipeline_id.clone(),
    );
    let parakeet = Arc::new(WyomingClient::with_fallbacks(
        cfg.parakeet.addr.clone(),
        cfg.parakeet.fallback_addrs.clone(),
    ));
    let mute_entities: Vec<String> = cfg
        .sources
        .iter()
        .filter_map(|s| s.mute_entity.clone())
        .collect();

    // Forwarders: every external producer hands the coordinator loop a `CoordIn` and is
    // gone; the loop itself holds the only mutable state in the process.
    let forward_wake = spawn_forward(wake_rx, coord_tx.clone(), CoordIn::Wake);
    let forward_event = spawn_forward(event_rx, coord_tx.clone(), into_coord_in);
    let forward_mute = spawn_forward(mute_rx, coord_tx.clone(), |(entity, muted)| {
        CoordIn::MuteChanged { entity, muted }
    });
    let forward_stop = spawn_forward(stop_rx, coord_tx.clone(), CoordIn::Stop);

    // Coordinator loop: the only stateful component, driven by events + a tick.
    let mut coordinator = Coordinator::new(&cfg, Box::new(SystemClock));
    let mut wiring = Wiring::new(
        &cfg,
        Arc::clone(&registry),
        ha.clone(),
        parakeet,
        suppress_tx.clone(),
        stop_listen_tx,
        coord_tx,
    );
    let mut coordinator_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(COORDINATOR_TICK);
        loop {
            tokio::select! {
                input = coord_rx.recv() => match input {
                    Some(input) => wiring.dispatch(&mut coordinator, input).await,
                    None => break,
                },
                _ = tick.tick() => wiring.dispatch(&mut coordinator, CoordIn::Tick).await,
            }
        }
    });

    // HA mute subscriptions (S4.5), best-effort: a reconnect loop inside ha.rs owns them.
    let mute_task = {
        let ha = ha.clone();
        tokio::spawn(async move {
            if let Err(e) = ha.subscribe_mute_states(&mute_entities, mute_tx).await {
                tracing::warn!(error = %e, "mute subscription ended");
            }
        })
    };

    // Daily clip retention (S8.1, AC9), once at startup and then on its interval.
    let retention_task = spawn_retention_sweep(cfg.clips.dir.clone(), cfg.clips.retention_days);

    let serve = ingest::serve(
        &cfg,
        Arc::clone(&registry),
        kws_tx,
        frame_tx,
        event_tx,
        suppress_tx.subscribe(),
    );

    // The service runs until ingest stops, until Ctrl-C arrives — the unit's
    // `KillSignal`, and the only signal handled — or until one of the two
    // components nothing else can replace dies. The coordinator loop and the KWS
    // thread used to be able to panic and leave this process alive, accepting
    // sockets, and permanently deaf; `Restart=` never fires for a live process, so
    // either death now ends it with an error and systemd brings it back (L12).
    let outcome = tokio::select! {
        r = serve => r.context("ingest serve loop ended"),
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("shutdown requested: stopping ingest, then draining the KWS thread");
            Ok(())
        }
        r = &mut coordinator_task => match r {
            Ok(()) => Err(anyhow::anyhow!(
                "the coordinator loop ended while the service was running; exiting \
                 non-zero so systemd restarts the service"
            )),
            Err(e) => Err(anyhow::anyhow!(
                "the coordinator task died ({e}); exiting non-zero so systemd restarts \
                 the service"
            )),
        },
        died = kws_died.recv() => match died {
            Some(Ok(frames)) => Err(anyhow::anyhow!(
                "the KWS thread exited after {frames} frames while the service was \
                 running; exiting non-zero so systemd restarts the service"
            )),
            Some(Err(_)) => Err(anyhow::anyhow!(
                "the KWS thread panicked; exiting non-zero so systemd restarts the \
                 service"
            )),
            None => Err(anyhow::anyhow!("the KWS thread watcher vanished")),
        },
    };

    // One shutdown path for every exit (L2/L3), so the arm that logs "draining"
    // actually drains. Stop producing as far as this function can reach: the
    // `serve` future — and with it the accept loop's KWS sender — was dropped by
    // the select above; the forwarders and the mute subscription are aborted so
    // nothing new reaches the coordinator, which is then stopped. The KWS thread's
    // `for` loop ends when the last connection task drops its sender; that is
    // ingest's state, so the wait is bounded and its expiry is logged rather than
    // hung on (the unbounded join here was L3's hang).
    forward_wake.abort();
    forward_event.abort();
    forward_mute.abort();
    forward_stop.abort();
    coordinator_task.abort();
    mute_task.abort();
    retention_task.abort();
    drain_kws_thread(&mut kws_died, KWS_JOIN_GRACE).await;
    outcome
}

/// Map an ingest event into the coordinator's input type (a plain adapter for the
/// forwarder, so `IngestEvent` never has to know about `CoordIn`).
fn into_coord_in(event: IngestEvent) -> CoordIn {
    CoordIn::Ingest(event)
}

/// Spawn a task that turns every item of `rx` into a `CoordIn` for the coordinator loop.
fn spawn_forward<T: Send + 'static>(
    mut rx: mpsc::Receiver<T>,
    coord_tx: mpsc::Sender<CoordIn>,
    map: fn(T) -> CoordIn,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(item) = rx.recv().await {
            if coord_tx.send(map(item)).await.is_err() {
                break; // the coordinator loop is gone
            }
        }
    })
}

/// Watch the KWS thread (L12, and the join both shutdown paths perform).
///
/// A watcher OS-thread owns the `JoinHandle` and reports the result exactly once
/// into this channel: the select loop in [`run_service`] hears about a death that
/// happens while the service should still be running, and the shutdown drain gets
/// its bounded wait on the same report. Doing the join here — the only place the
/// handle exists — is what lets both consumers race safely: a `JoinHandle` can be
/// joined once.
fn watch_kws_thread(
    thread: std::thread::JoinHandle<u64>,
) -> anyhow::Result<mpsc::Receiver<std::thread::Result<u64>>> {
    let (tx, rx) = mpsc::channel(1);
    std::thread::Builder::new()
        .name("kws-watch".to_string())
        .spawn(move || {
            // Room always exists (capacity 1, one result); if the receiver is gone
            // the process is already exiting.
            let _ = tx.blocking_send(thread.join());
        })
        .context("spawning the KWS watcher thread")?;
    Ok(rx)
}

/// The shutdown drain every exit path takes (L2/L3): wait at most `grace` for the
/// watched KWS thread to end, and say what happened either way. Both halves of
/// the promise are the point — the arm that claims to drain must attempt it, and
/// neither path may block forever on a thread whose senders a live satellite
/// connection still holds.
async fn drain_kws_thread(
    kws_died: &mut mpsc::Receiver<std::thread::Result<u64>>,
    grace: Duration,
) {
    match tokio::time::timeout(grace, kws_died.recv()).await {
        Ok(Some(Ok(frames))) => tracing::info!(frames, "KWS thread drained and stopped"),
        Ok(Some(Err(_))) => tracing::error!("the KWS thread panicked during the shutdown drain"),
        Ok(None) => tracing::error!("the KWS thread watcher closed without reporting"),
        Err(_) => tracing::error!(
            grace_secs = grace.as_secs(),
            "the KWS thread did not stop within the grace period (a satellite connection \
             is still holding its sender open); exiting without it",
        ),
    }
}

/// The keyword spotter, or a clearly-deaf engine when `[kws] model` cannot be loaded.
///
/// Loading a model is the one startup step that can fail for a reason the operator can
/// fix without a code change (a missing `scripts/get_models.sh` run). Refusing to bind
/// would also make every satellite reconnect fail, so the service comes up, logs the
/// reason at `error!`, and stays deaf until it is restarted with the model present.
fn spotter(cfg: &Config) -> Box<dyn KwsEngine> {
    match kws::SherpaEngine::new(&cfg.kws) {
        Ok(engine) => Box::new(engine),
        Err(e) => {
            tracing::error!(
                error = %e,
                model = %cfg.kws.model,
                keywords_file = %cfg.kws.keywords_file,
                "keyword spotter unavailable: the service is serving but NO WAKE will be \
                 detected. Fetch the model (scripts/get_models.sh) and restart."
            );
            Box::new(SpotterUnavailable)
        }
    }
}

/// The stop lane's keyword spotter, or `None` when the audio stop is off.
///
/// Built only when `[kws] stop_keywords_file` names a file: "luna stop" needs its own
/// spotter because the wake file's `luna` line would fire and reset the stream before
/// "stop" ever decoded (plan 2.3), so this instance reads the stop keywords at its own
/// threshold (`stop_threshold`, falling back to `threshold`) with the wake boosts
/// cleared. A file that fails to load is the same operator-fixable condition as
/// [`spotter`]'s and is logged at `error!` — but the degradation here is *audio stop
/// off* (`None`), not a deaf engine: the transcript stop phrases keep working, and a
/// missing stop file must never take the wake service down with it. No option set is
/// the operator choosing audio stop off; the log stays silent in that case.
fn stop_spotter(cfg: &Config) -> Option<Box<dyn KwsEngine>> {
    let mut stop_kws = cfg.kws.clone();
    stop_kws.keywords_file = cfg.kws.stop_keywords_file.clone()?;
    stop_kws.threshold = cfg.kws.stop_threshold.unwrap_or(cfg.kws.threshold);
    stop_kws.boosts.clear();
    match kws::SherpaEngine::new(&stop_kws) {
        Ok(engine) => Some(Box::new(engine) as Box<dyn KwsEngine>),
        Err(e) => {
            tracing::error!(
                error = %e,
                model = %stop_kws.model,
                keywords_file = %stop_kws.keywords_file,
                "stop spotter unavailable: the service is serving but the AUDIO stop is \
                 off — \"luna stop\" over a playing reply will not be heard (the \
                 transcript stop still works). Fix the file and restart."
            );
            None
        }
    }
}

/// A spotter that never reports a keyword: the degraded path [`spotter`] selects, so the
/// rest of the service (ingest, clips, mute, HA) stays observable.
struct SpotterUnavailable;

impl KwsEngine for SpotterUnavailable {
    fn accept(&mut self, _source: &str, _samples: &[f32]) {}
    fn decode_ready(&mut self) -> Vec<kws::Detected> {
        Vec::new()
    }
    fn reset(&mut self, _source: &str) {}
}

/// Run the clip retention sweep now, then every [`RETENTION_INTERVAL`] (S8.1, AC9).
fn spawn_retention_sweep(dir: String, keep_days: u32) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let target = PathBuf::from(dir.clone());
            match tokio::task::spawn_blocking(move || clips::retention_sweep(&target, keep_days))
                .await
            {
                Ok(Ok(removed)) if removed > 0 => {
                    tracing::info!(dir = %dir, removed, keep_days, "clip retention sweep");
                }
                Ok(Ok(_)) => {}
                Ok(Err(e)) => tracing::warn!(dir = %dir, error = %e, "clip retention sweep failed"),
                Err(e) => tracing::warn!(error = %e, "clip retention sweep panicked"),
            }
            tokio::time::sleep(RETENTION_INTERVAL).await;
        }
    })
}

/// One turn in flight for a source: the connection that captured it, the conversation that
/// turn belongs to, and the keyword span its false-accept clip pads.
///
/// The handle is the point of this struct. Everything downstream of a capture — the samples a
/// transcript is built from, the bytes a false-accept clip saves — is audio **that
/// connection** delivered, and a ring is per-connection state: `registry.get(source)` asked
/// later answers with whichever connection holds the id *now*, a different ring measured in a
/// different sample domain (docs/protocol.md §12 replaces the entry, and §11's "a span never
/// crosses a reconnect"). Re-resolving by id after the fact is how a turn gets read out of
/// audio it never heard.
///
/// Written exactly where capture starts, read only by the turn it was written for, and dropped
/// where the turn ends.
struct Turn {
    /// The conversation `CoordOut::Endpoint` named for this capture.
    conversation_id: String,
    /// The connection whose ring holds the audio.
    handle: Arc<SourceHandle>,
    /// The keyword span, used only as the false-accept clip's fallback window (R-D) when no
    /// transcription happened; `None` for a capture with no keyword, which has no span.
    kw_span: Option<Range<u64>>,
    /// The exact sample range handed to the ASR, recorded when the request is made.
    ///
    /// This exists because the clip and the transcript have to describe the same bytes:
    /// `verify::Decision::FalseAccept` says the clip and the text that caused the rejection
    /// always agree, and the two are computed by different rules otherwise — the ASR is fed
    /// `kw_span.start..endpoint`, while the padded keyword window below is
    /// `kw_span.start − 1 s..kw_span.end + 1 s`. Deriving the clip from the keyword instead
    /// of from this field cuts off everything after the keyword plus one second, so the
    /// operator tuning a KWS threshold hears two seconds around the wake word while the
    /// sidecar quotes speech that is not in the file beside it.
    transcribed: Option<Range<u64>>,
}

/// Everything `dispatch` needs to execute a coordinator decision.
///
/// The three small maps are the wiring's only state: the turn in flight per source (the
/// capability its audio must be read from, plus what its clip pads), the last `kws_drops`
/// value per source (the drift warning, R-A), and the moment a `tts-end` was seen per
/// conversation (the fifth latency stage, AC5). Each is keyed by an in-flight id and removed
/// when used.
struct Wiring {
    cfg: Config,
    registry: Arc<SourceRegistry>,
    ha: HaClient,
    /// Shared, not copied: tier cooldown state has to survive across turns, and each
    /// transcription task is spawned with a clone of this `Arc`.
    parakeet: Arc<WyomingClient>,
    suppress_tx: watch::Sender<HashSet<String>>,
    /// The stop lane's listen set (plan 4.2): the sources whose audio the stop spotter
    /// may see, derived by the coordinator and republished here on every
    /// `CoordOut::StopListenChanged`. Read by the KWS thread's `StopLane`.
    stop_listen_tx: watch::Sender<HashSet<String>>,
    coord_in: mpsc::Sender<CoordIn>,
    turns: HashMap<String, Turn>,
    /// The newest capture task per source (see `start_capture`): at most one live capture can
    /// speak for a source at a time.
    captures: HashMap<String, tokio::task::JoinHandle<()>>,
    last_kws_drops: HashMap<String, u64>,
    tts_end_at: Arc<Mutex<HashMap<String, Instant>>>,
}

impl Wiring {
    fn new(
        cfg: &Config,
        registry: Arc<SourceRegistry>,
        ha: HaClient,
        parakeet: Arc<WyomingClient>,
        suppress_tx: watch::Sender<HashSet<String>>,
        stop_listen_tx: watch::Sender<HashSet<String>>,
        coord_in: mpsc::Sender<CoordIn>,
    ) -> Self {
        Self {
            cfg: cfg.clone(),
            registry,
            ha,
            parakeet,
            suppress_tx,
            stop_listen_tx,
            coord_in,
            turns: HashMap::new(),
            captures: HashMap::new(),
            last_kws_drops: HashMap::new(),
            tts_end_at: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Execute what the coordinator decided (plan S4.4/S5/S6).
    ///
    /// Nothing here awaits a network or a disk: slow work is spawned, and its result
    /// comes back through `coord_in` as the next `CoordIn`. That is what keeps the
    /// 100 ms tick — and so every `Speaking`/`FollowUp` deadline — honest.
    async fn dispatch(&mut self, coordinator: &mut Coordinator, input: CoordIn) {
        if let CoordIn::Wake(wake) = &input {
            if !self.wake_domain_ok(wake) {
                // R-A's other half: the wake is refused, not warned about and dispatched.
                // The coordinator never sees it, so no session, no LED, no capture that can
                // only hold the group until `hard_cap_s` on an unreadable span.
                return;
            }
        }
        // A `PipelineFinished` with a TTS url stamped `tts_end_at` in the pipeline
        // task before the coordinator decided anything. When the turn is stale
        // (`serve_turn`) the coordinator emits no `PlayMedia`, and `play_media` —
        // the only remover — never runs: without this the map grew one entry per
        // stale pipeline result for the life of the process.
        let stamped_tts = match &input {
            CoordIn::PipelineFinished {
                conversation_id,
                tts_url: Some(_),
                ..
            } => Some(conversation_id.clone()),
            _ => None,
        };
        let outs = coordinator.handle(input);
        if let Some(id) = &stamped_tts {
            let answered = outs.iter().any(|out| {
                matches!(out, CoordOut::PlayMedia { conversation_id, .. } if conversation_id == id)
            });
            if !answered {
                if let Ok(mut at) = self.tts_end_at.lock() {
                    at.remove(id);
                }
            }
        }
        for out in outs {
            self.execute_out(out).await;
        }
        // A turn's capability belongs to the turn. Whatever path ended it — the pipeline
        // finishing, a `SourceGone`, a timeout, a mute — the entry must not survive to be
        // read as if it still described live audio.
        self.prune_finished_turns(coordinator);
    }

    /// Execute one coordinator decision: `dispatch`'s per-`CoordOut` step, run in the
    /// order the coordinator emitted them (plan 4.2 publishes the stop-listen set before
    /// the suppression set, and that order is the contract). Separate from `dispatch` so
    /// a wiring test can drive exactly one output the way the loop does.
    async fn execute_out(&mut self, out: CoordOut) {
        match out {
            CoordOut::SuppressionChanged { suppressed } => {
                // S5.2/S4.5: the window itself. Ingest keeps the rings full and
                // starves only the spotter; the KWS thread resets what it skipped.
                let set: HashSet<String> = suppressed.into_iter().collect();
                let _ = self.suppress_tx.send(set);
            }
            CoordOut::StopListenChanged { sources } => {
                // Plan 4.2: the stop lane's derived listen set — whose audio the stop
                // spotter may see. The same quiet ignore as the suppression watch: at
                // shutdown the KWS thread may already be gone.
                let set: HashSet<String> = sources.into_iter().collect();
                let _ = self.stop_listen_tx.send(set);
            }
            CoordOut::Latency {
                stage,
                conversation_id,
                ms,
            } => log_latency(stage, &conversation_id, ms),
            CoordOut::Control { source, control } => {
                // To the owning connection's socket (docs/protocol.md): this is what lights
                // the ring. A source that is not connected is an expected, quiet case: the
                // coordinator also tells a source going idle that has just left.
                match self
                    .registry
                    .send_control(&source, proto::encode_control(&control))
                {
                    ControlSend::Queued => {}
                    ControlSend::NotConnected => {
                        tracing::debug!(source = %source, ?control, "control not sent: source not connected");
                    }
                    ControlSend::QueueFull => {
                        tracing::warn!(source = %source, ?control, "control dropped: the satellite is not draining its downlink queue");
                    }
                }
            }
            CoordOut::Endpoint {
                source,
                kw_span,
                conversation_id,
            } => self.start_capture(source, kw_span, conversation_id),
            CoordOut::Transcribe {
                source,
                conversation_id,
                utterance_start,
                endpoint,
            } => self.start_transcription(source, conversation_id, utterance_start, endpoint),
            CoordOut::Verify {
                source,
                conversation_id,
                transcript,
                wake_initiated,
            } => {
                self.run_verify(source, conversation_id, transcript, wake_initiated)
                    .await
            }
            CoordOut::RunPipeline {
                source,
                conversation_id,
                text,
            } => self.run_pipeline(source, conversation_id, text),
            CoordOut::PlayMedia {
                source,
                entity_id,
                url,
                conversation_id,
                mime_type,
            } => self.play_media(source, entity_id, url, conversation_id, mime_type),
            CoordOut::StopPlayback { source, entity } => self.stop_playback(source, entity),
            CoordOut::FalseAcceptClip { source, transcript } => {
                self.write_false_accept_clip(source, transcript)
            }
            CoordOut::MissedWakeClip { source } => self.write_missed_wake_clip(source),
        }
    }

    /// Drop the turn state of every group whose session has ended.
    ///
    /// Keyed on the coordinator's own answer rather than on a message that merely *tends* to
    /// accompany a turn ending: a `Led::Idle` is also what muting produces, and pruning on
    /// that would throw away the ring a false-accept clip is about to read.
    fn prune_finished_turns(&mut self, coordinator: &Coordinator) {
        let finished: Vec<String> = self
            .turns
            .iter()
            .filter(|(source, turn)| {
                matches!(
                    coordinator.session(&self.group_for(source, &turn.handle)),
                    Session::Idle
                )
            })
            .map(|(source, _)| source.clone())
            .collect();
        for source in finished {
            self.turns.remove(&source);
        }
    }

    /// R-A: whether this wake's span is inside the ring the capture would read — and the
    /// diagnostics that make any drift in that domain diagnosable rather than silent.
    ///
    /// The KWS thread counts the samples it received, which equals the ring's write cursor
    /// only while nothing was dropped: each dropped frame leaves it 1024 samples behind.
    /// A re-registration resets both clocks together (ingest replaces the ring and sends
    /// the restart marker), so a *change* in `kws_drops` since this source's previous
    /// wake is exactly the case where the capture window is offset.
    ///
    /// `false` means `dispatch` must not hand this wake to the coordinator at all. A span
    /// past the write cursor of the ring the capture would use can never be satisfied: the
    /// usual cause is a wake that crossed a reconnect, where the spotter's counter belongs to
    /// the connection that has just been replaced while the entry — and the ring — belong to
    /// the new one. A capture started from such a span reads nothing and holds the group in
    /// `Capturing` until `hard_cap_s`; a `warn!` that dispatches it anyway is a warning
    /// standing in for a decision. The same applies to a wake from a source with no
    /// registration: there is no ring to capture from, so the turn could only ever time out.
    fn wake_domain_ok(&mut self, wake: &WakeEvent) -> bool {
        let Some(handle) = self.registry.get(&wake.source) else {
            tracing::warn!(
                source = %wake.source,
                kw_span = ?wake.kw_span,
                "wake refused: no connection is registered for this source, so there is no \
                 ring for a capture to read (R-A)",
            );
            return false;
        };
        let drops = handle
            .stats
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .kws_drops;
        let previous = self.last_kws_drops.insert(wake.source.clone(), drops);
        match previous {
            Some(prev) if prev > drops => tracing::warn!(
                source = %wake.source,
                kws_drops = drops,
                previous_drops = prev,
                "kws drop counter restarted: this source re-registered since its last \
                 wake, so its spans are measured from the new ring cursor (0)",
            ),
            Some(prev) if prev < drops => tracing::warn!(
                source = %wake.source,
                kws_drops = drops,
                previous_drops = prev,
                drift_samples = (drops - prev) * proto::FRAME_SAMPLES as u64,
                drift_frames = drops - prev,
                "wake span may be offset: frames were dropped between this wake and the \
                 previous one, and the spotter's clock fell behind the ring by that much \
                 (R9) — expect the capture to start early",
            ),
            _ => {}
        }
        // The one invariant that must never break: a span is inside what was written. Here
        // "broken" is not a warning, it is a capture that cannot be completed.
        let written = with_ring(&handle.ring, |ring| ring.written());
        if wake.kw_span.end > written {
            tracing::warn!(
                source = %wake.source,
                kw_span = ?wake.kw_span,
                ring_written = written,
                "wake refused: its span is past this ring's write cursor, so the capture \
                 would read a short or empty window — a span that crossed a reconnect (R-A)",
            );
            return false;
        }
        true
    }

    /// S3.2/S6.1: watch the winner's ring forward until the endpointer ends the turn.
    fn start_capture(
        &mut self,
        source: String,
        kw_span: Option<Range<u64>>,
        conversation_id: String,
    ) {
        let Some(handle) = self.registry.get(&source) else {
            tracing::warn!(source = %source, "endpoint requested for an unknown source");
            return;
        };
        // The capability, recorded at the one moment it can be obtained honestly: this
        // connection is live, and its ring is the one this capture will read. Everything
        // downstream — the transcript, the false-accept clip — reads *this* handle, because
        // `registry.get(source)` at that later moment answers with whichever connection holds
        // the id by then, and its ring knows nothing about the audio just delivered.
        self.turns.insert(
            source.clone(),
            Turn {
                conversation_id: conversation_id.clone(),
                handle: Arc::clone(&handle),
                kw_span: kw_span.clone(),
                transcribed: None,
            },
        );
        let mode = match kw_span {
            Some(span) => EndpointMode::Wake { kw_span: span },
            None => EndpointMode::FollowUp,
        };
        let registry = Arc::clone(&self.registry);
        let capture_cfg = self.cfg.capture.clone();
        let vad_model = vad_model_path(&self.cfg);
        let coord_in = self.coord_in.clone();
        let key = source.clone();
        let task = tokio::spawn(async move {
            run_capture(
                registry,
                handle,
                source,
                conversation_id,
                capture_cfg,
                vad_model,
                mode,
                coord_in,
            )
            .await;
        });
        // One live capture per source. A follow-up listener outlives the turn that started
        // it (it waits out its window), and its endpoint names the source, not the
        // conversation: left running when a wake opens a new turn, it could end the *new*
        // turn with its own stale endpoint. Aborting the previous capture here makes the
        // newest request the only one that can speak for the source. A finished task is
        // aborted harmlessly.
        if let Some(previous) = self.captures.insert(key, task) {
            previous.abort();
        }
    }

    /// S3.3: read the utterance out of the ring and hand it to Parakeet off-loop.
    fn start_transcription(
        &mut self,
        source: String,
        conversation_id: String,
        utterance_start: u64,
        endpoint: u64,
    ) {
        let Some(pcm) = self.turn_audio(&source, &conversation_id, utterance_start..endpoint)
        else {
            // Not "unknown source" — the source may well be connected, and after a reconnect
            // it certainly is. What is missing is *this turn's* capture, so the ring that
            // would answer belongs to somebody else and is counted in another domain. Reading
            // it would transcribe audio this conversation never heard.
            tracing::debug!(
                source = %source,
                conversation_id = %conversation_id,
                utterance_start,
                endpoint,
                "transcribe for a turn this wiring is not serving: dropped",
            );
            return;
        };
        if pcm.is_empty() {
            // A session whose capture window held nothing is not worth a request
            // (`parakeet::transcribe` says the same); `Tick` closes the session.
            tracing::warn!(
                source = %source,
                conversation_id = %conversation_id,
                utterance_start,
                endpoint,
                "nothing to transcribe: the capture window read 0 samples from the ring",
            );
            return;
        }
        // Record the bytes that are actually about to be sent, so a false-accept clip can be
        // sliced from this range instead of re-derived from the keyword (`Turn::transcribed`).
        // Only after the read succeeded and produced samples: a range that was never sent must
        // not become the evidence window.
        if let Some(turn) = self
            .turns
            .get_mut(&source)
            .filter(|turn| turn.conversation_id == conversation_id)
        {
            turn.transcribed = Some(utterance_start..endpoint);
        }
        let client = self.parakeet.clone();
        let coord_in = self.coord_in.clone();
        tokio::spawn(async move {
            match parakeet::transcribe(&client, &pcm).await {
                Ok(text) => {
                    let _ = coord_in
                        .send(CoordIn::Transcript {
                            source,
                            conversation_id,
                            text,
                        })
                        .await;
                }
                Err(e) => tracing::warn!(
                    source = %source,
                    conversation_id = %conversation_id,
                    error = %e,
                    samples = pcm.len(),
                    "transcription failed: the session will time out",
                ),
            }
        });
    }

    /// The audio one turn's capture delivered, sliced from the ring of the connection that
    /// captured it, and only for the conversation that capture was started for.
    ///
    /// `None` means Wiring holds no turn for that source/conversation pair — a late or
    /// duplicate request whose capture has already ended. Both are answered from the turn,
    /// never from `registry.get(source)`: after a reconnect that answers with the *new*
    /// connection's ring, which is empty or holds unrelated audio, sliced at the old
    /// connection's offsets.
    fn turn_audio(
        &self,
        source: &str,
        conversation_id: &str,
        range: Range<u64>,
    ) -> Option<Vec<i16>> {
        let turn = self.turn(source, conversation_id)?;
        Some(with_ring(&turn.handle.ring, |ring| ring.range(range)))
    }

    /// The turn in flight for `source`, but only if it is the one `conversation_id` names.
    fn turn(&self, source: &str, conversation_id: &str) -> Option<&Turn> {
        let turn = self.turns.get(source)?;
        (turn.conversation_id == conversation_id).then_some(turn)
    }

    /// S4.2: the gate is pure and fast, so it runs inline — but HA is still only reached
    /// through the coordinator's next input, never from here.
    async fn run_verify(
        &mut self,
        source: String,
        conversation_id: String,
        transcript: String,
        wake_initiated: bool,
    ) {
        match verify::verify_utterance(&transcript, wake_initiated, &self.cfg.kws.wake_words) {
            Ok(text) => {
                // The gate passed, so the only thing that could still want this turn's ring
                // is the false-accept clip, and there will not be one. The capability goes
                // with the work it existed for rather than outliving the turn.
                self.turns.remove(&source);
                // This task *is* the coordinator loop: `dispatch` runs inline in the
                // only consumer of `coord_in`, so awaiting a self-send deadlocks the
                // moment the queue is full — the receiver that would drain it is
                // blocked here, on the send. `Verified` ends or continues the turn, so
                // dropping it is not acceptable either; the answer is forwarded from a
                // spawned task exactly as `start_transcription` and `run_pipeline`
                // already do. The one-tick delay a full queue can cost is the same
                // delay a busy queue already produces.
                let coord_in = self.coord_in.clone();
                tokio::spawn(async move {
                    let _ = coord_in
                        .send(CoordIn::Verified {
                            source,
                            conversation_id,
                            text,
                        })
                        .await;
                });
            }
            Err(rejected) => {
                // The turn's own clip follows from this line, so the capability stays until
                // `write_false_accept_clip` has taken its bytes from it. Same reasoning as
                // above: never await a self-send from inside the loop.
                let coord_in = self.coord_in.clone();
                tokio::spawn(async move {
                    let _ = coord_in
                        .send(CoordIn::VerifyRejected {
                            source,
                            conversation_id,
                            transcript: rejected.transcript,
                        })
                        .await;
                });
            }
        }
    }

    /// S4.3: enter the Assist pipeline at the intent stage, then feed the result back.
    fn run_pipeline(&mut self, source: String, conversation_id: String, text: String) {
        let device_id = self.device_id_for(&source);
        let ha = self.ha.clone();
        let coord_in = self.coord_in.clone();
        let tts_end_at = Arc::clone(&self.tts_end_at);
        let request = PipelineRequest {
            text,
            device_id,
            conversation_id: Some(conversation_id.clone()),
        };
        tokio::spawn(async move {
            let outcome = ha.run_pipeline(&request).await;
            // AC5's fourth boundary is measured here, where `tts-end` is actually seen;
            // the coordinator stamps its own TranscriptToTtsEnd from its clock.
            let (tts_url, mime_type) = match &outcome {
                Ok(outcome) => (outcome.tts_url.clone(), outcome.mime_type.clone()),
                Err(_) => (None, None),
            };
            if tts_url.is_some() {
                if let Ok(mut at) = tts_end_at.lock() {
                    at.insert(conversation_id.clone(), Instant::now());
                }
            }
            if let Err(e) = &outcome {
                tracing::warn!(
                    error = %e,
                    conversation_id = %conversation_id,
                    "pipeline run failed: no TTS for this turn",
                );
            }
            let _ = coord_in
                .send(CoordIn::PipelineFinished {
                    source,
                    conversation_id,
                    tts_url,
                    mime_type,
                })
                .await;
        });
    }

    /// S4.3/O2: announce the TTS on the winner's entity, naming the format the pipeline
    /// actually returned (R-C). A source configured `playback = "source"` has no HA
    /// `media_player`: it is sent a `play` control instead and fetches the audio itself.
    fn play_media(
        &mut self,
        source: String,
        entity_id: String,
        url: String,
        conversation_id: String,
        mime_type: Option<String>,
    ) {
        let ha = self.ha.clone();
        // Taken for BOTH routes: this is the stamp's remover once a `PlayMedia` was emitted
        // (the dispatch guard removes it when none was), and `TtsEndToPlaybackStart` needs it.
        let started = self
            .tts_end_at
            .lock()
            .ok()
            .and_then(|mut at| at.remove(&conversation_id));
        if self
            .cfg
            .source(&source)
            .is_some_and(|s| s.playback == listenrr::config::Playback::Source)
        {
            let control = direct_play_control(&reply_base(&self.cfg.ha), &url, mime_type.as_deref());
            match self
                .registry
                .send_control(&source, proto::encode_control(&control))
            {
                ControlSend::Queued => {
                    // On this route the backend can only observe that the `play` control was
                    // queued on the socket, not that the source accepted it: the source's own
                    // `playback_done` is the only completion signal.
                    if let Some(at) = started {
                        log_latency(
                            LatencyStage::TtsEndToPlaybackStart,
                            &conversation_id,
                            at.elapsed().as_millis() as u64,
                        );
                    }
                    tracing::info!(
                        source = %source,
                        conversation_id = %conversation_id,
                        ?control,
                        "playback started: play control queued on the source's socket"
                    );
                }
                ControlSend::NotConnected => tracing::warn!(
                    source = %source,
                    conversation_id = %conversation_id,
                    "play not sent: the source is not connected; the group will time out of Speaking"
                ),
                ControlSend::QueueFull => tracing::warn!(
                    source = %source,
                    conversation_id = %conversation_id,
                    "play dropped: the source is not draining its downlink queue; the group will time out of Speaking"
                ),
            }
            return;
        }
        tokio::spawn(async move {
            match ha.play_media(&entity_id, &url, mime_type.as_deref()).await {
                Ok(()) => {
                    // AC5's fifth boundary: `tts-end` → the satellite accepted playback.
                    if let Some(at) = started {
                        log_latency(
                            LatencyStage::TtsEndToPlaybackStart,
                            &conversation_id,
                            at.elapsed().as_millis() as u64,
                        );
                    }
                    tracing::info!(
                        source = %source,
                        entity_id = %entity_id,
                        conversation_id = %conversation_id,
                        "playback started"
                    );
                }
                Err(e) => tracing::warn!(
                    error = %e,
                    source = %source,
                    entity_id = %entity_id,
                    conversation_id = %conversation_id,
                    "play_media failed: the group will time out of Speaking",
                ),
            }
        });
    }

    /// Stop the winner's playback on the route its config picks (plan step 5).
    ///
    /// The route comes from [`stop_route`], never from convenience: invariant I3 —
    /// the ESP32 firmware hard-disconnects on a control `type` it does not know, so
    /// `Control::Stop` is tablet-only and a `playback = "ha"` source is always stopped
    /// through Home Assistant. An empty entity is the coordinator's `output_entity`
    /// answer of "nowhere to play": a logged skip, not a call. The spawned HA call is
    /// fire-and-forget exactly as `play_media` is; its deadline lives in `HaClient`.
    fn stop_playback(&self, source: String, entity: String) {
        let Some(configured) = self.cfg.source(&source) else {
            tracing::warn!(source = %source, "stop dropped: the source is not configured");
            return;
        };
        match stop_route(configured.playback) {
            StopRoute::Control => {
                match self
                    .registry
                    .send_control(&source, proto::encode_control(&proto::Control::Stop))
                {
                    ControlSend::Queued => tracing::info!(
                        source = %source,
                        "stop queued on the source's socket"
                    ),
                    ControlSend::NotConnected => tracing::warn!(
                        source = %source,
                        "stop not sent: the source is not connected"
                    ),
                    ControlSend::QueueFull => tracing::warn!(
                        source = %source,
                        "stop dropped: the source is not draining its downlink queue"
                    ),
                }
            }
            StopRoute::HaStop => {
                if entity.is_empty() {
                    tracing::warn!(
                        source = %source,
                        "stop skipped: the source has no output entity to stop"
                    );
                    return;
                }
                let ha = self.ha.clone();
                tokio::spawn(async move {
                    match ha.media_stop(&entity).await {
                        Ok(()) => tracing::info!(
                            source = %source,
                            entity_id = %entity,
                            "stop sent to Home Assistant"
                        ),
                        Err(e) => tracing::warn!(
                            error = %e,
                            source = %source,
                            entity_id = %entity,
                            "media_stop failed: the reply may play to its natural end",
                        ),
                    }
                });
            }
        }
    }

    /// The handle for a caller that only needs audio a source **already delivered** and has
    /// no turn to name it by: the `save_buffer` dump, which is precisely the case where no
    /// wake was accepted, so there is no capture and no turn record to read a handle from.
    ///
    /// A caller that *is* serving a turn must read that turn's ring instead — see
    /// [`Wiring::turn_audio`] and [`Wiring::false_accept_audio`]. This one is a last resort,
    /// not a convenience: `get` first, because a live source is the normal case, and the
    /// retired registration only when the source has gone. Anything that must not act on a
    /// stale source (admission, attribution, presence) should call `get` alone.
    fn handle_for_already_received(&self, source: &str) -> Option<Arc<SourceHandle>> {
        self.registry
            .get(source)
            .or_else(|| self.registry.recently_gone(source))
    }

    /// The bytes a false-accept clip saves: the rejected turn's keyword span ±1 s, read from
    /// **that turn's** ring.
    ///
    /// The span and the ring come from one turn record because they belong to one connection.
    /// `kw_span` is an index into the ring the spotter was fed from; after a reconnect the
    /// entry the id resolves to is a fresh ring counting from 0, and slicing *it* at the old
    /// offsets writes a clip of audio that was never the rejected utterance — worse than no
    /// clip, because the clip and its sidecar are evidence.
    fn false_accept_audio(&self, source: &str) -> Option<(Vec<i16>, Arc<SourceHandle>)> {
        let turn = self.turns.get(source)?;
        let pcm = with_ring(&turn.handle.ring, |ring| {
            Self::false_accept_window(
                turn.transcribed.clone(),
                turn.kw_span.clone(),
                ring.retained_start(),
            )
            .map(|window| ring.range(window))
        });
        Some((pcm?, Arc::clone(&turn.handle)))
    }

    /// The sample range a false-accept clip must hold: **exactly the audio the ASR was
    /// given**, so the clip and the transcript that caused the rejection describe the same
    /// bytes (`verify::Decision::FalseAccept` promises they agree).
    ///
    /// The keyword-padded window is only a fallback for a turn that was never transcribed.
    /// It must not be the primary rule, because it disagrees with what the ASR heard at both
    /// ends: the ASR is fed from `kw_span.start` to the endpointer's end, while
    /// `kw ± CLIP_PADDING_SECONDS` stops one second after the keyword — so the sidecar quoted
    /// speech that is not in the file beside it, and the operator reading the clip to judge a
    /// false accept heard two seconds around the wake word instead of the utterance
    /// (`clips::CLIP_PADDING_SECONDS`, R-D).
    ///
    /// `retained_start` clamps the start to what the ring still holds: `range` would
    /// zero-fill, and a clip that opens with silence the satellite never sent is worse than a
    /// short one.
    fn false_accept_window(
        transcribed: Option<Range<u64>>,
        kw_span: Option<Range<u64>>,
        retained_start: u64,
    ) -> Option<Range<u64>> {
        let raw = transcribed.or_else(|| {
            let span = kw_span?;
            let pad = clips::CLIP_PADDING_SECONDS as u64 * u64::from(SAMPLE_RATE_HZ);
            Some(span.start.saturating_sub(pad)..span.end.saturating_add(pad))
        })?;
        Some(raw.start.max(retained_start)..raw.end)
    }

    /// S4.2/S8.1: write the rejected turn's audio — the bytes the ASR was given, falling
    /// back to the keyword ± 1 s only if nothing was transcribed (`false_accept_window`;
    /// R-D — `write_trigger_clip` takes audio that is already padded).
    fn write_false_accept_clip(&mut self, source: String, transcript: String) {
        // The turn ends here either way: the clip is the last thing that can need the
        // capturing connection's ring, so the bytes are taken and the capability dropped
        // before anything awaits.
        let clip = self.false_accept_audio(&source);
        self.turns.remove(&source);
        let Some((pcm, handle)) = clip else {
            tracing::warn!(
                source = %source,
                "false-accept clip needs the rejected turn's own capture: no keyword span \
                 from a capturing connection is recorded for this source, so nothing was \
                 written — a clip sliced from whichever ring the id resolves to now would be \
                 a guess about audio this turn never sent",
            );
            return;
        };
        let room = handle.room.clone();
        let group = self.group_for(&source, &handle);
        let rec = TriggerRecord {
            source: source.clone(),
            room,
            group,
            wall_clock: wall_clock_now(),
            conversation_id: None,
            // The candidate list is the coordinator's, and its frozen API does not
            // expose it; a false accept has no winner either way.
            candidates: Vec::new(),
            winner: None,
            transcript: Some(transcript),
            verify_passed_text: None,
            // A rejection can only come from a wake-initiated turn: a follow-up has no
            // keyword to require and always passes (S6.1).
            wake_initiated: true,
            stages: StageLatencies::default(),
            ended_by: None,
            reason: TriggerReason::FalseAccept,
        };
        let dir = PathBuf::from(self.cfg.clips.dir.clone());
        tokio::spawn(async move {
            match tokio::task::spawn_blocking(move || {
                clips::write_trigger_clip(&dir, &source, &pcm, &rec)
            })
            .await
            {
                Ok(Ok(path)) => tracing::info!(clip = %path.display(), "false-accept clip written"),
                Ok(Err(e)) => tracing::warn!(error = %e, "false-accept clip failed"),
                Err(e) => tracing::warn!(error = %e, "false-accept clip panicked"),
            }
        });
    }

    /// S8.2/AC11: the `save_buffer` ("that was for you") dump — the source's last 10 s.
    fn write_missed_wake_clip(&mut self, source: String) {
        let Some(handle) = self.handle_for_already_received(&source) else {
            tracing::warn!(source = %source, "save_buffer from an unknown source");
            return;
        };
        let ring = Arc::clone(&handle.ring);
        let dir = PathBuf::from(self.cfg.clips.dir.clone());
        tokio::spawn(async move {
            match tokio::task::spawn_blocking(move || clips::save_buffer_dump(&ring, &source, &dir))
                .await
            {
                Ok(Ok(path)) => tracing::info!(clip = %path.display(), "missed-wake clip written"),
                Ok(Err(e)) => tracing::warn!(error = %e, "missed-wake clip failed"),
                Err(e) => tracing::warn!(error = %e, "missed-wake clip panicked"),
            }
        });
    }

    /// `[[sources]] device_id`, else the group's `output_device_id`, else the source's
    /// `fallback_device_id` (S7.1) — the same precedence the coordinator uses for the
    /// output entity.
    fn device_id_for(&self, source_id: &str) -> Option<String> {
        let configured = self.cfg.source(source_id);
        let group = configured
            .map(|s| s.group.clone())
            .or_else(|| self.registry.get(source_id).and_then(|h| h.group.clone()))?;
        let group_cfg = self.cfg.group(&group);
        configured
            .and_then(|s| s.device_id.clone())
            .or_else(|| group_cfg.and_then(|g| g.output_device_id.clone()))
            .or_else(|| configured.and_then(|s| s.fallback_device_id.clone()))
    }

    /// The group a source belongs to: config first, hello second, the source's own id as
    /// the last resort (an ad-hoc source is a group of one — S7.1).
    fn group_for(&self, source_id: &str, handle: &SourceHandle) -> String {
        self.cfg
            .source(source_id)
            .map(|s: &Source| s.group.clone())
            .or_else(|| handle.group.clone())
            .unwrap_or_else(|| source_id.to_string())
    }
}

/// Whether the connection a capture was started for can still put a sample in its ring.
///
/// The answer is the registry's current entry compared against **the handle this capture was
/// given**, not `registry.get(source).is_none()`. The second question is about the id, and a
/// reconnect keeps the id registered: it answers "still there" about a socket the server
/// replaced two polls ago, the poll then keeps reading a ring nothing will ever write again,
/// and the turn sits there until `hard_cap_s` ends it with whatever the endpointer happened to
/// hear before the link changed hands. Asking about the handle instead makes a replacement
/// visible the moment it happens, which is also what makes the audio already in the ring
/// reachable: it belongs to this connection, and this is the only reader that knows so.
///
/// A *decision*, not a gate on a mutation: what follows is ending the capture, and the ring is
/// never written through this answer. That is why it may use [`SourceRegistry::is_current`]
/// rather than the registry's lock-held ownership guard.
fn capture_connection_ended(registry: &SourceRegistry, handle: &Arc<SourceHandle>) -> bool {
    !registry.is_current(handle)
}

/// Poll one source's ring and endpoint the turn (S3.2).
///
/// The ring is polled by index rather than driven from the frame broadcast: the ring is
/// the authoritative store (plan §3), a broadcast subscriber can lag and has to re-read it
/// anyway, and reading by index keeps every index in the write-cursor domain R-A defines.
/// The ring is reached through the [`SourceHandle`] the capture was started with, and the
/// capture's life is bounded by that handle's ownership, not by its source id's presence.
// 8 args, one over the default limit. Collapsing these into a params struct is a real
// refactor (callers, the spawn site, tests) and is out of scope for a lint pass.
#[allow(clippy::too_many_arguments)]
async fn run_capture(
    registry: Arc<SourceRegistry>,
    handle: Arc<SourceHandle>,
    source: String,
    conversation_id: String,
    capture_cfg: listenrr::config::Capture,
    vad_model: PathBuf,
    mode: EndpointMode,
    coord_in: mpsc::Sender<CoordIn>,
) {
    let ring = Arc::clone(&handle.ring);
    let started = Instant::now();
    // A fresh VAD per pass: S3.1's backwards pass and S3.2's forward pass MUST NOT share
    // state, so a wake turn loads two instances rather than reusing one. Loading them
    // off-loop keeps the coordinator's tick on time.
    let needs_backward = matches!(mode, EndpointMode::Wake { .. });
    let loaded = {
        let model = vad_model.clone();
        tokio::task::spawn_blocking(move || {
            let forward: Box<dyn VadEngine> =
                SileroVad::new(&model).map(|v| Box::new(v) as Box<dyn VadEngine>)?;
            let backward: Option<Box<dyn VadEngine>> = if needs_backward {
                Some(SileroVad::new(&model).map(|v| Box::new(v) as Box<dyn VadEngine>)?)
            } else {
                None
            };
            Ok::<_, anyhow::Error>((forward, backward))
        })
        .await
    };
    let (vad, backward_vad) = match loaded {
        Ok(Ok(vads)) => vads,
        Ok(Err(e)) => {
            tracing::warn!(
                source = %source,
                error = %e,
                model = %vad_model.display(),
                "no VAD for this capture: the session will time out",
            );
            return;
        }
        Err(e) => {
            tracing::warn!(source = %source, error = %e, "VAD load failed");
            return;
        }
    };

    // ── S3.1: the backwards pass, once, before any audio is streamed forward ─────────
    // R-B: the window is `ring.range(kw_span.end − BACKWARD_LOOKBACK_SAMPLES ..
    // kw_span.end)` — exclusive at the keyword end, and `kw_span` is in the ring-cursor
    // domain R-A defines. The result is the start of the speech segment the keyword sits
    // in, and `coordinator.rs` takes `start.min(utterance_start)` against `kw_span.start`:
    // the ASR gets everything from the segment's start, so a command spoken before the
    // keyword ("what's the weather, Luna?") reaches it, and the keyword itself is always in
    // the input, which is what puts the wake token in the transcript for the verify gate
    // (S4.2). The owner decided this on 2026-11-03; it used to be a `max` that began every
    // request at the keyword and left the pre-roll unreachable. A follow-up turn has no
    // keyword, so its start is whatever the endpointer heard. The clip does not use this
    // range either — it is sliced from `Turn::transcribed`, the bytes actually sent.
    let bounds: Option<Range<u64>> = match (&mode, backward_vad) {
        (EndpointMode::Wake { kw_span }, Some(mut backward_vad)) => {
            let base = kw_span
                .end
                .saturating_sub(listenrr::capture::BACKWARD_LOOKBACK_SAMPLES as u64);
            let recent = with_ring(&ring, |r| r.range(base..kw_span.end.min(r.written())));
            let bounds = backward_bounds_with_preroll(
                &recent,
                kw_span,
                backward_vad.as_mut(),
                ms_to_samples(capture_cfg.preroll_ms),
            );
            tracing::debug!(
                source = %source,
                conversation_id = %conversation_id,
                kw_span = ?kw_span,
                start = bounds.start,
                end = bounds.end,
                window = recent.len(),
                "backwards bounds derived",
            );
            Some(bounds)
        }
        _ => None,
    };
    let utterance_start = bounds.as_ref().map(|b| b.start);

    let mut endpointer = ForwardEndpointer::new(&capture_cfg, vad, mode.clone());
    // A follow-up listener starts the instant the winner's speaker stops, with no echo
    // cancellation: the room still rings with Luna's own last syllable, and a VAD that heard
    // that as the user starting to speak would "answer" itself. A short settle skips it.
    if matches!(mode, EndpointMode::FollowUp) {
        tokio::time::sleep(FOLLOWUP_SETTLE).await;
    }
    // Start reading where the utterance's audio actually begins: just past the keyword on
    // a wake turn, at the current cursor for a follow-up (which has no keyword).
    let mut cursor = match &mode {
        EndpointMode::Wake { kw_span } => kw_span.end,
        EndpointMode::FollowUp => with_ring(&ring, |r| r.written()),
    };
    // A wake turn runs to the hard cap. A follow-up listener runs only for the window the
    // coordinator opened, measured from when this task started (the coordinator's own clock
    // started at the same `playback_done`), and is extended to the cap once speech begins. If
    // nobody speaks, the deadline ends it with nothing heard, and `finish_capture` sends
    // nothing: a quiet room is not an utterance.
    let mut onset_reported = !matches!(mode, EndpointMode::FollowUp);
    let mut deadline = match &mode {
        EndpointMode::FollowUp => started + Duration::from_secs(capture_cfg.followup_s),
        EndpointMode::Wake { .. } => {
            Instant::now() + Duration::from_secs(capture_cfg.hard_cap_s) + CAPTURE_GRACE
        }
    };
    let mut poll = tokio::time::interval(CAPTURE_POLL);

    loop {
        poll.tick().await;
        if capture_connection_ended(&registry, &handle) {
            // The connection this capture was given no longer owns the source's entry, so
            // nothing can be written to the ring it polls again. That includes a reconnect,
            // where the id is still registered and the old reading said "still alive" about a
            // socket the server had already replaced. What this connection already delivered
            // is in this ring and is still worth transcribing, so the turn is ended through
            // the same path a real disconnect uses (S3.2), not abandoned.
            finish_capture(
                &mut endpointer,
                EndpointReason::SourceGone,
                &source,
                &conversation_id,
                utterance_start,
                &coord_in,
            )
            .await;
            return;
        }
        let (next, chunk) = with_ring(&ring, |r| {
            let end = r.written();
            let chunk = if end > cursor {
                r.range(cursor..end)
            } else {
                Vec::new()
            };
            (end, chunk)
        });
        if !chunk.is_empty() {
            if let Some(endpoint) = endpointer.push(cursor, &chunk) {
                // `speech_seen` and `silent_ms` say why a turn ended the way it did. A
                // HardCap with `speech_seen = false` means the VAD never registered the
                // speaker (too quiet for it, which `TrailingSilence` cannot recover from);
                // with `speech_seen = true` and small `silent_ms` it means speech never
                // stopped. Without them the two look identical in the log.
                //
                // `tail_dbfs` separates the two kinds of "speech never stopped": a loud last
                // second means someone really was still talking; a quiet one (near the
                // microphone's floor) with `speech_seen` still true means the VAD was held
                // open by something steady, which is a detector problem, not a talker.
                let tail = with_ring(&ring, |r| {
                    r.range(endpoint.end.saturating_sub(SAMPLE_RATE_HZ as u64)..endpoint.end)
                });
                let tail_dbfs = if tail.is_empty() {
                    f64::NEG_INFINITY
                } else {
                    let mean_sq = tail.iter().map(|&s| f64::from(s).powi(2)).sum::<f64>()
                        / tail.len() as f64;
                    20.0 * (mean_sq.sqrt().max(1.0) / 32768.0).log10()
                };
                tracing::info!(
                    source = %source,
                    conversation_id = %conversation_id,
                    start = endpoint.start,
                    end = endpoint.end,
                    reason = ?endpoint.reason,
                    speech_seen = endpointer.speech_started(),
                    silent_ms = endpoint.end.saturating_sub(endpoint.last_speech) / 16,
                    tail_dbfs = format!("{tail_dbfs:.1}"),
                    "utterance endpoint reached",
                );
                let _ = coord_in
                    .send(CoordIn::EndpointReached {
                        source,
                        utterance_start: utterance_start.unwrap_or(endpoint.start),
                        endpoint: endpoint.end,
                    })
                    .await;
                return;
            }
            cursor = next;
            // S6.1: first speech on a follow-up listener. The coordinator's window is
            // `followup_s`, and a listener that stopped at that deadline would cut off the
            // answer it had just heard begin; from here the utterance may run to the cap.
            if !onset_reported && endpointer.speech_started() {
                onset_reported = true;
                deadline = Instant::now()
                    + Duration::from_secs(capture_cfg.hard_cap_s)
                    + CAPTURE_GRACE;
                let _ = coord_in
                    .send(CoordIn::FollowUpSpeech {
                        source: source.clone(),
                    })
                    .await;
            }
        }
        if Instant::now() >= deadline {
            // The hard cap is the endpointer's business; this is only the "no frames are
            // coming" backstop, and it never invents an endpoint that was never heard.
            finish_capture(
                &mut endpointer,
                EndpointReason::HardCap,
                &source,
                &conversation_id,
                utterance_start,
                &coord_in,
            )
            .await;
            return;
        }
    }
}

/// Force an endpoint and feed it back, if anything was actually heard. The utterance start
/// is the S3.1 backwards bound when there is one (a wake turn); a follow-up uses whatever
/// the endpointer heard.
async fn finish_capture(
    endpointer: &mut ForwardEndpointer,
    reason: EndpointReason,
    source: &str,
    conversation_id: &str,
    utterance_start: Option<u64>,
    coord_in: &mpsc::Sender<CoordIn>,
) {
    match endpointer.finish(reason) {
        Some(endpoint) => {
            tracing::info!(
                source = %source,
                conversation_id = %conversation_id,
                start = endpoint.start,
                end = endpoint.end,
                ?reason,
                "utterance ended without a trailing-silence endpoint",
            );
            let _ = coord_in
                .send(CoordIn::EndpointReached {
                    source: source.to_string(),
                    utterance_start: utterance_start.unwrap_or(endpoint.start),
                    endpoint: endpoint.end,
                })
                .await;
        }
        None => tracing::debug!(
            source = %source,
            conversation_id = %conversation_id,
            ?reason,
            "capture closed with nothing heard",
        ),
    }
}

/// One AC5 stage line, from the one place the field names are spelled.
fn log_latency(stage: LatencyStage, conversation_id: &str, ms: u64) {
    tracing::info!(?stage, conversation_id = %conversation_id, ms, "stage latency");
}

/// The base URL a *source* uses to fetch a reply from Home Assistant: `ha.internal_url` when it
/// is set (trailing `/` trimmed), otherwise `ha.url` reduced to scheme + authority with the
/// WebSocket scheme mapped to its HTTP twin. `config.rs` documents an empty `internal_url` as
/// "satellites can reach `ha.url` directly", and `ha.url` is a `ws://` address, which no HTTP
/// client can fetch an mp3 from.
fn reply_base(ha: &listenrr::config::Ha) -> String {
    let internal = ha.internal_url.trim();
    if !internal.is_empty() {
        return internal.trim_end_matches('/').to_string();
    }
    let url = ha.url.trim();
    let (scheme, rest) = url.split_once("://").unwrap_or(("http", url));
    let scheme = match scheme {
        "ws" => "http",
        "wss" => "https",
        other => other,
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    format!("{scheme}://{authority}")
}

/// The `format` token of a `play` control from the TTS mime type Home Assistant returned.
/// Parameters (`; charset=…`) are ignored. An unrecognised type gives its subtype, and a
/// missing one gives `auto`: the field is informational for the source, which sniffs the
/// stream itself, so guessing a codec here would only be a way to be wrong.
fn audio_format(mime: Option<&str>) -> String {
    let Some(mime) = mime else {
        return "auto".to_string();
    };
    let essence = mime.split(';').next().unwrap_or(mime).trim().to_ascii_lowercase();
    match essence.as_str() {
        "audio/mpeg" => "mp3".to_string(),
        "audio/flac" | "audio/x-flac" => "flac".to_string(),
        "audio/wav" | "audio/x-wav" | "audio/wave" => "wav".to_string(),
        other => match other.split_once('/') {
            // 15 bytes is the documented `play.format` cap (docs/protocol.md section 8); a
            // longer subtype is not a codec name anyone needs, so it becomes "auto" rather than
            // a control that a stricter source would refuse, dropping the connection with it.
            Some((_, subtype)) if !subtype.is_empty() && subtype.len() <= 15 => {
                subtype.to_string()
            }
            _ => "auto".to_string(),
        },
    }
}

/// The `play` control for a source that plays its own replies (`playback = "source"`): the
/// reply URL made absolute against [`reply_base`] and the format token. An absolute URL is
/// passed through unchanged.
fn direct_play_control(base: &str, url: &str, mime: Option<&str>) -> proto::Control {
    let url = if url.contains("://") {
        url.to_string()
    } else {
        format!("{}/{}", base.trim_end_matches('/'), url.trim_start_matches('/'))
    };
    proto::Control::Play {
        url,
        format: audio_format(mime),
    }
}

/// Which stop mechanism a source's playback route requires (plan step 5, AC7a).
///
/// Invariant I3 decides it: the ESP32 firmware hard-disconnects on a control `type` it
/// does not know (`luna_stream.h`), and `Control::Stop` is tablet-only, so a
/// `playback = "ha"` source must be stopped through Home Assistant even though every
/// source has a socket a control could be queued on. Pure so the fork itself is
/// testable without a socket or an HA call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopRoute {
    /// `playback = "source"` (the tablet app): queue `Control::Stop` on its socket.
    Control,
    /// `playback = "ha"` (every ESP32 board): call Home Assistant's
    /// `media_player.media_stop` on the resolved entity.
    HaStop,
}

/// The stop route for a source's playback config: `Source` takes the socket control,
/// `Ha` takes Home Assistant (see [`StopRoute`] for why the choice is invariant I3).
fn stop_route(playback: Playback) -> StopRoute {
    match playback {
        Playback::Source => StopRoute::Control,
        Playback::Ha => StopRoute::HaStop,
    }
}

/// The Silero VAD path: `[capture] vad_model`, else the KWS bundle, which is where
/// sherpa-onnx ships it (contract choice 6).
fn vad_model_path(cfg: &Config) -> PathBuf {
    PathBuf::from(
        cfg.capture
            .vad_model
            .clone()
            .unwrap_or_else(|| cfg.kws.model.clone()),
    )
}

/// RFC 3339 UTC, the only wall clock that reaches disk (plan §3). No date crate: this is
/// the civil-days conversion, and nothing else in the binary needs calendar arithmetic.
fn wall_clock_now() -> String {
    format_wall_clock(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    )
}

/// The formatting half of [`wall_clock_now`], separate so the epoch arithmetic is testable.
fn format_wall_clock(secs: u64) -> String {
    let (hour, minute, second) = (secs / 3600 % 24, secs / 60 % 60, secs % 60);
    let (year, month, day) = civil_from_days(secs / 86_400);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Days since the Unix epoch → proleptic Gregorian `(year, month, day)`.
///
/// Howard Hinnant's `civil_from_days`, shifted to a 0000-03-01 epoch so the leap-day
/// arithmetic fits in one era; correct for every timestamp this binary can produce.
fn civil_from_days(days: u64) -> (i64, u64, u64) {
    let z = i64::try_from(days).unwrap_or(i64::MAX) + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = u64::try_from(doy - (153 * mp + 2) / 5 + 1).unwrap_or(1);
    let month = u64::try_from(mp + if mp < 10 { 3 } else { -9 }).unwrap_or(1);
    (y + i64::from(month <= 2), month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wall_clock_is_rfc3339_utc() {
        // 2026-11-01T00:00:00Z, a date this project will actually put in a sidecar.
        assert_eq!(format_wall_clock(1_793_491_200), "2026-11-01T00:00:00Z");
        assert_eq!(format_wall_clock(1_793_520_061), "2026-11-01T08:01:01Z");
    }

    #[test]
    fn civil_from_days_handles_leap_and_century_boundaries() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_898), (2024, 6, 24));
        assert_eq!(
            civil_from_days(11_016),
            (2000, 2, 29),
            "a century leap year"
        );
        assert_eq!(
            civil_from_days(47_541),
            (2100, 3, 1),
            "1900/2100 are not leap years"
        );
        // The day before a March 1 in a common year: the leap-day offset in action.
        assert_eq!(civil_from_days(20_147), (2025, 2, 28));
        assert_eq!(civil_from_days(20_148), (2025, 3, 1));
    }

    #[test]
    fn vad_model_falls_back_to_the_kws_bundle() {
        let mut cfg = Config::parse_toml_str(&minimal_config()).unwrap();
        assert_eq!(
            vad_model_path(&cfg),
            PathBuf::from("/models/kws"),
            "contract choice 6: no key means the KWS bundle"
        );
        cfg.capture.vad_model = Some("/models/silero.pt".to_string());
        assert_eq!(vad_model_path(&cfg), PathBuf::from("/models/silero.pt"));
    }

    #[test]
    fn false_accept_uses_the_transcribed_range_not_the_keyword_pad() {
        // The live numbers from the first wake turn ever transcribed: keyword at
        // 51 840..56 320, endpoint reached after trailing silence. The old rule saved
        // `kw ± 1 s` and the sidecar quoted speech beyond it, so a transcript of
        // "…the squalid quarter of the brothels" sat beside a 2.28 s clip that did not
        // contain it.
        let kw: Range<u64> = 51_840..56_320;
        let endpoint: u64 = 100_000;
        assert_eq!(
            Wiring::false_accept_window(Some(kw.start..endpoint), Some(kw.clone()), 0),
            Some(kw.start..endpoint),
            "the clip must be the bytes the ASR was given"
        );
        // …which the keyword-derived rule could never produce: it stops one second after
        // the keyword, well short of the endpoint.
        let padded = Wiring::false_accept_window(None, Some(kw.clone()), 0).unwrap();
        assert!(
            padded.end < endpoint,
            "the fallback ({padded:?}) must be the shorter window, or this test proves nothing"
        );
    }

    #[test]
    fn false_accept_falls_back_to_the_keyword_pad_when_nothing_was_transcribed() {
        // R-D: a turn that never reached the ASR has no transcribed range, and the keyword
        // ± CLIP_PADDING_SECONDS is all there is to show.
        let kw: Range<u64> = 51_840..56_320;
        let pad = clips::CLIP_PADDING_SECONDS as u64 * u64::from(SAMPLE_RATE_HZ);
        assert_eq!(
            Wiring::false_accept_window(None, Some(kw.clone()), 0),
            Some(kw.start - pad..kw.end + pad)
        );
        // No keyword and no transcription is not a clip: guessing from whatever ring the id
        // resolves to now would be evidence about audio this turn never sent.
        assert_eq!(Wiring::false_accept_window(None, None, 0), None);
    }

    #[test]
    fn false_accept_clamps_the_start_to_what_the_ring_retains() {
        // `Ring::range` zero-fills below the retained start, so a clip that opens with
        // silence the satellite never sent would be worse than a short one. Both paths.
        assert_eq!(
            Wiring::false_accept_window(Some(100..5_000), None, 4_000),
            Some(4_000..5_000)
        );
        assert_eq!(
            // The fallback pads ± 1 s first, then clamps: 10 000 − 16 000 saturates to 0, so
            // the retained start is what decides the beginning, and the end is 12 000 + pad.
            Wiring::false_accept_window(None, Some(10_000..12_000), 9_500),
            Some(9_500..12_000 + clips::CLIP_PADDING_SECONDS as u64 * u64::from(SAMPLE_RATE_HZ)),
            "the keyword fallback gets the same clamp"
        );
    }

    fn minimal_config() -> String {
        "[server]\nlisten = \"127.0.0.1:10700\"\n\
         \n[ha]\nurl = \"ws://ha.example.lan:8123\"\ntoken = \"ha\"\npipeline_id = \"p\"\n\
         \n[parakeet]\naddr = \"asr.example.lan:10200\"\n\
         \n[kws]\nmodel = \"/models/kws\"\nkeywords_file = \"/models/keywords.txt\"\n\
         \n[clips]\ndir = \"/tmp/listenrr-clips\"\n"
            .to_string()
    }

    /// Register a source the way a connection's hello does, and hand back the handle that
    /// connection owns — the capability a capture is started with, and the only thing that
    /// can identify whose audio a span refers to.
    fn register(registry: &SourceRegistry, id: &str) -> Arc<SourceHandle> {
        registry.register(
            &proto::Hello {
                v: proto::PROTOCOL_VERSION,
                source_id: id.to_string(),
                room: id.to_string(),
                token: None,
                caps: proto::Caps::default(),
                fw: "test/1".to_string(),
            },
            None,
            4096,
        )
    }

    /// A wiring with no live network behind anything: neither client connects on construction,
    /// and the two channels are local. Enough to decide *which* audio a turn acts on; the
    /// parts that reach a socket or a model are the ones these tests do not cover.
    fn new_wiring(cfg: &Config, registry: Arc<SourceRegistry>) -> Wiring {
        let (suppress_tx, _suppress_rx) = watch::channel(HashSet::new());
        let (stop_listen_tx, _stop_listen_rx) = watch::channel(HashSet::new());
        let (coord_in, _coord_rx) = mpsc::channel(8);
        Wiring::new(
            cfg,
            registry,
            HaClient::new("ws://ha.example.lan:8123", "ha", "p"),
            Arc::new(WyomingClient::new("asr.example.lan:10200")),
            suppress_tx,
            stop_listen_tx,
            coord_in,
        )
    }

    /// A wake with the span the test needs, rather than the fixed 500 ms one.
    fn wake_span(source: &str, start: u64, end: u64) -> WakeEvent {
        WakeEvent {
            source: source.to_string(),
            kw_span: start..end,
            arrived: Instant::now(),
        }
    }

    /// 5B. The poll loop's liveness question is about the connection it was handed, not about
    /// the id. A reconnect keeps the id registered, so the old question —
    /// `registry.get(source).is_none()` — answered "still alive" about a socket the server had
    /// already replaced, and the capture went on polling a ring nothing would ever write again
    /// until `hard_cap_s` (+ `CAPTURE_GRACE`) ran out and ended the turn with whatever the
    /// endpointer had heard before the link changed hands.
    #[test]
    fn a_capture_ends_when_the_connection_it_was_given_is_superseded() {
        let cfg = Config::parse_toml_str(&minimal_config()).unwrap();
        let registry = SourceRegistry::new(&cfg);
        let captured = register(&registry, "kitchen");
        assert!(
            !capture_connection_ended(&registry, &captured),
            "while it is the live registration, the capture keeps polling"
        );

        // The satellite reconnects. The id stays registered — `get` cannot tell the two
        // connections apart, which is exactly why it cannot answer this question.
        let replacement = register(&registry, "kitchen");
        assert!(
            registry.get("kitchen").is_some(),
            "the id is still registered, so an id-based liveness check still says 'there'"
        );
        assert!(
            capture_connection_ended(&registry, &captured),
            "the capture's own connection is no longer current: its ring can receive nothing \
             more, so the poll must end through the source-gone path with the audio already \
             in it"
        );
        assert!(
            !capture_connection_ended(&registry, &replacement),
            "the connection that took the id over is not ended, and the source keeps working"
        );
    }

    /// 5C, the wiring half. The endpoint's `utterance_start..endpoint` is measured in the
    /// capturing connection's sample domain, so the transcript has to be sliced from that
    /// connection's ring. Resolving `registry.get(source)` when the request is acted on reads
    /// the *new* connection's ring — empty, or unrelated audio — at the old connection's
    /// offsets, and the utterance is replaced by whatever the reconnect had sent.
    #[test]
    fn the_audio_of_a_turn_is_read_from_the_connection_that_captured_it() {
        let cfg = Config::parse_toml_str(&minimal_config()).unwrap();
        let registry = Arc::new(SourceRegistry::new(&cfg));
        let captured = register(&registry, "kitchen");
        let spoken: Vec<i16> = (0..8).collect();
        with_ring(&captured.ring, |ring| {
            ring.write(&spoken);
        });
        let mut wiring = new_wiring(&cfg, Arc::clone(&registry));
        // What `start_capture` writes when the coordinator arms a capture.
        wiring.turns.insert(
            "kitchen".to_string(),
            Turn {
                conversation_id: "listenrr-1".to_string(),
                handle: Arc::clone(&captured),
                kw_span: None,
                transcribed: None,
            },
        );

        // The satellite reconnects before the transcript request is reached.
        let replacement = register(&registry, "kitchen");
        with_ring(&replacement.ring, |ring| {
            ring.write(&[9i16; 8]);
        });
        assert!(!Arc::ptr_eq(&replacement, &captured));

        assert_eq!(
            with_ring(&replacement.ring, |ring| ring.range(0..8)),
            vec![9i16; 8],
            "the live ring genuinely holds different audio, so the assertion below cannot \
             pass by reading the same bytes twice"
        );
        assert_eq!(
            wiring.turn_audio("kitchen", "listenrr-1", 0..8),
            Some(spoken),
            "the turn's audio comes from the ring that received it, not from whichever ring \
             the id resolves to now"
        );
        assert_eq!(
            wiring.turn_audio("kitchen", "listenrr-2", 0..8),
            None,
            "a request naming a conversation this wiring is not serving is dropped rather \
             than answered from somebody else's ring"
        );
    }

    /// 5D. The false-accept clip pads the *rejected turn's* keyword span, and a span is an
    /// index into one ring. `handle_for_already_received` prefers the live registration, so
    /// after a reconnect the clip was sliced from the new connection's ring at the old
    /// connection's offsets — a clip whose bytes have nothing to do with the transcript in its
    /// own sidecar, which is worse than no clip because it is evidence.
    #[test]
    fn a_false_accept_clip_is_sliced_from_the_capturing_connections_ring() {
        let cfg = Config::parse_toml_str(&minimal_config()).unwrap();
        let registry = Arc::new(SourceRegistry::new(&cfg));
        let captured = register(&registry, "kitchen");
        with_ring(&captured.ring, |ring| {
            ring.write(&vec![3i16; 4000]);
        });
        let mut wiring = new_wiring(&cfg, Arc::clone(&registry));
        wiring.turns.insert(
            "kitchen".to_string(),
            Turn {
                conversation_id: "listenrr-1".to_string(),
                handle: Arc::clone(&captured),
                kw_span: Some(1000..2048),
                transcribed: None,
            },
        );

        // Reconnect: a fresh ring, counted from 0, and the id points at it now.
        let replacement = register(&registry, "kitchen");
        with_ring(&replacement.ring, |ring| {
            ring.write(&vec![-7i16; 4000]);
        });
        assert!(registry.get("kitchen").is_some());

        let (pcm, handle) = wiring
            .false_accept_audio("kitchen")
            .expect("the turn has a keyword span, so the clip has bytes to write");
        assert!(
            Arc::ptr_eq(&handle, &captured),
            "the clip's room, group and retention bound all come from the connection that \
             captured the turn"
        );
        assert_eq!(
            pcm,
            vec![3i16; 4000],
            "and so do its samples; the live ring holds {:?}, which is what the id-based \
             lookup handed out",
            with_ring(&replacement.ring, |ring| ring.last(3))
        );

        // Nothing to pad, nothing to write: no invented clip from the live ring.
        assert!(
            wiring.false_accept_audio("attic").is_none(),
            "a source with no turn in flight has no clip"
        );
        let no_span = Turn {
            conversation_id: "listenrr-1".to_string(),
            handle: Arc::clone(&captured),
            kw_span: None,
            transcribed: None,
        };
        let mut wiring2 = new_wiring(&cfg, registry);
        wiring2.turns.insert("kitchen".to_string(), no_span);
        assert!(
            wiring2.false_accept_audio("kitchen").is_none(),
            "a capture with no keyword has no span to pad, and a guess is not a clip"
        );
    }

    /// The clip must be the audio the ASR heard, not a re-derivation from the keyword. On
    /// the first live wake turn the two disagreed at both ends — the transcript quoted
    /// "…the squalid quarter of the brothels" while the `.wav` beside it was the keyword ± 1 s
    /// and transcribed to different words entirely — and nothing failed, because the clip is
    /// written from one rule and the transcript from another.
    #[test]
    fn a_false_accept_clip_is_the_audio_the_asr_was_given() {
        let cfg = Config::parse_toml_str(&minimal_config()).unwrap();
        let registry = Arc::new(SourceRegistry::new(&cfg));
        let captured = register(&registry, "kitchen");
        // Distinguishable by position, so a window shifted by even one sample is detectable.
        // Sized inside the 4096-sample ring the test fixture registers, so the retained-start
        // clamp is not what is under test here.
        let spoken: Vec<i16> = (0..4000).map(|i| i as i16).collect();
        with_ring(&captured.ring, |ring| {
            ring.write(&spoken);
        });
        let mut wiring = new_wiring(&cfg, Arc::clone(&registry));
        let kw: Range<u64> = 1000..2048;
        let sent: Range<u64> = 1000..3000;
        wiring.turns.insert(
            "kitchen".to_string(),
            Turn {
                conversation_id: "listenrr-1".to_string(),
                handle: Arc::clone(&captured),
                kw_span: Some(kw.clone()),
                transcribed: Some(sent.clone()),
            },
        );

        let (pcm, _) = wiring
            .false_accept_audio("kitchen")
            .expect("a rejected turn that was transcribed has audio to keep");
        assert_eq!(
            pcm,
            spoken[sent.start as usize..sent.end as usize],
            "the clip is the bytes handed to the ASR"
        );
        // The keyword-derived window is a different, larger slice — so this assertion cannot
        // pass by coincidence of the two rules agreeing.
        assert_ne!(
            pcm.len(),
            with_ring(&captured.ring, |ring| {
                let pad = clips::CLIP_PADDING_SECONDS as u64 * u64::from(SAMPLE_RATE_HZ);
                ring.range(kw.start.saturating_sub(pad)..kw.end + pad)
            })
            .len(),
            "the old `kw ± padding` rule would have written a different clip"
        );
    }

    /// 5E. A wake whose span the capture's ring cannot satisfy is not started — it is refused.
    /// A `warn!` followed by a capture that can only read nothing holds the group in
    /// `Capturing` (LED up, arbitration state live) until `hard_cap_s`, for a turn that was
    /// never possible. The usual cause is a wake that crossed a reconnect: the span is in the
    /// replaced connection's counter, the ring is the new one's.
    #[tokio::test]
    async fn a_wake_whose_span_the_ring_cannot_hold_is_refused() {
        let cfg = Config::parse_toml_str(&minimal_config()).unwrap();
        let registry = Arc::new(SourceRegistry::new(&cfg));
        let handle = register(&registry, "kitchen");
        with_ring(&handle.ring, |ring| {
            ring.write(&vec![1i16; 1024]);
        });
        let mut wiring = new_wiring(&cfg, Arc::clone(&registry));
        let mut coordinator = Coordinator::new(&cfg, Box::new(SystemClock));

        // Past anything this ring holds.
        let beyond = wake_span("kitchen", 1024, 2048);
        assert!(
            !wiring.wake_domain_ok(&beyond),
            "a span the ring the capture would use cannot satisfy is refused"
        );
        // Refusing is what `dispatch` does with it: the coordinator never hears about the
        // wake, so no session opens, no LED goes up, and no capture starts in order to hang
        // in `Capturing` until `hard_cap_s` on a window that can only ever be empty.
        wiring
            .dispatch(&mut coordinator, CoordIn::Wake(beyond))
            .await;
        assert_eq!(
            coordinator.session("kitchen"),
            &Session::Idle,
            "a refused wake does not become a turn"
        );

        // The control: the same wake inside what was written is dispatched, so this is a
        // refusal of unreachable spans and not a blanket veto.
        let inside = wake_span("kitchen", 0, 1024);
        assert!(wiring.wake_domain_ok(&inside));
        wiring
            .dispatch(&mut coordinator, CoordIn::Wake(inside))
            .await;
        assert!(
            matches!(coordinator.session("kitchen"), Session::Capturing { .. }),
            "an answerable wake still opens its turn"
        );

        // A wake from a source with no registration is refused for the same reason: there is
        // no ring for a capture to read, so that turn could only ever time out.
        registry.unregister("kitchen");
        assert!(
            !wiring.wake_domain_ok(&wake_span("kitchen", 0, 512)),
            "an unregistered source cannot be captured from"
        );
    }

    /// The other half of carrying a capability: it is written where capture starts and it goes
    /// when the turn does. An entry left behind after its session has ended is a stale ring
    /// waiting to be read as though it were live audio — the same defect, one turn later.
    #[tokio::test]
    async fn turn_state_does_not_outlive_the_turn_it_describes() {
        let cfg = Config::parse_toml_str(&minimal_config()).unwrap();
        let registry = Arc::new(SourceRegistry::new(&cfg));
        let handle = register(&registry, "kitchen");
        with_ring(&handle.ring, |ring| {
            ring.write(&vec![1i16; 2048]);
        });
        let mut wiring = new_wiring(&cfg, Arc::clone(&registry));
        let mut coordinator = Coordinator::new(&cfg, Box::new(SystemClock));

        // An entry whose group has no session is dropped on sight, however it got there.
        wiring.turns.insert(
            "kitchen".to_string(),
            Turn {
                conversation_id: "listenrr-0".to_string(),
                handle: Arc::clone(&handle),
                kw_span: None,
                transcribed: None,
            },
        );
        wiring.dispatch(&mut coordinator, CoordIn::Tick).await;
        assert!(
            wiring.turns.is_empty(),
            "a group with no session keeps no capability: {:?}",
            wiring.turns.keys().collect::<Vec<_>>()
        );

        // The real thing: a dispatched wake arms the capture, and while that turn is live its
        // ring is exactly what the transcript must read.
        wiring
            .dispatch(
                &mut coordinator,
                CoordIn::Wake(wake_span("kitchen", 0, 1024)),
            )
            .await;
        assert!(matches!(
            coordinator.session("kitchen"),
            Session::Capturing { .. }
        ));
        assert!(
            wiring.turns.contains_key("kitchen"),
            "the live turn's capability must survive the dispatch that created it"
        );
        let conversation = wiring.turns["kitchen"].conversation_id.clone();

        // Now the turn crosses its round trips. `Processing` is the state the verify gate
        // answers into, and a rejected transcript reads this ring one message later to write
        // its false-accept clip — so pruning "after the transcript" would prune one message
        // too early and the clip would lose its bytes. The sweep asks the coordinator instead.
        wiring
            .dispatch(
                &mut coordinator,
                CoordIn::EndpointReached {
                    source: "kitchen".to_string(),
                    utterance_start: 0,
                    endpoint: 1024,
                },
            )
            .await;
        assert!(matches!(
            coordinator.session("kitchen"),
            Session::Processing { .. }
        ));
        wiring.dispatch(&mut coordinator, CoordIn::Tick).await;
        assert_eq!(
            wiring
                .turns
                .get("kitchen")
                .map(|turn| turn.conversation_id.as_str()),
            Some(conversation.as_str()),
            "a turn whose verify gate has not answered keeps the ring its rejection clip \
             will read"
        );

        // And it goes with the session, whichever path ended it.
        coordinator.release("kitchen");
        wiring.dispatch(&mut coordinator, CoordIn::Tick).await;
        assert!(
            wiring.turns.is_empty(),
            "no entry outlives the turn it was written for"
        );
    }

    /// L12 + the L2/L3 shutdown contract, tested at the two helpers the service
    /// itself calls. A KWS thread that dies must reach the watcher (that report is
    /// what makes `run_service` exit non-zero instead of leaving a live but deaf
    /// process), and the drain must return on its own clock: the unbounded
    /// `kws_thread.join()` at the heart of L3 hung the whole exit when a satellite
    /// connection still held a sender.
    #[tokio::test]
    async fn the_kws_watcher_reports_death_and_the_drain_bounds_itself() {
        let panicked = std::thread::spawn(|| -> u64 {
            panic!("the spotter died");
        });
        let mut died = watch_kws_thread(panicked).expect("watcher spawns");
        match tokio::time::timeout(Duration::from_secs(5), died.recv()).await {
            Ok(Some(Err(_))) => {}
            Ok(Some(Ok(frames))) => {
                panic!("a panicked KWS thread must surface as Err, got Ok({frames})")
            }
            Ok(None) => panic!("the watcher closed without reporting"),
            Err(_) => panic!("the watcher never reported the panic"),
        }

        let (hold_tx, hold_rx) = crossbeam_channel::bounded::<()>(1);
        let parked = std::thread::spawn(move || {
            let _ = hold_rx.recv();
            0u64
        });
        let mut died = watch_kws_thread(parked).expect("watcher spawns");
        let start = Instant::now();
        drain_kws_thread(&mut died, Duration::from_millis(50)).await;
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the drain returns on its own grace, not on the thread"
        );
        drop(hold_tx); // release the parked thread so the test leaves nothing behind
    }

    /// L4: the verify gate answers from inside the coordinator's own `dispatch`,
    /// and `dispatch` is the only consumer of `coord_in` — so an awaited self-send
    /// on a full queue could never complete, and the whole service went silent
    /// with the receiver blocked on the send it was waiting for.
    #[tokio::test]
    async fn the_verify_answer_survives_a_full_coordinator_queue() {
        let cfg = Config::parse_toml_str(&minimal_config()).unwrap();
        let registry = Arc::new(SourceRegistry::new(&cfg));
        let handle = register(&registry, "kitchen");
        with_ring(&handle.ring, |ring| {
            ring.write(&vec![1i16; 2048]);
        });
        // A live but deliberately undrained queue, filled to capacity before the
        // dispatch: the pre-fix inline `.send(..).await` here blocked forever.
        let (coord_in, mut coord_rx) = mpsc::channel::<CoordIn>(1);
        coord_in
            .try_send(CoordIn::Tick)
            .expect("prefill the single queue slot");
        let (suppress_tx, _suppress_rx) = watch::channel(HashSet::new());
        let (stop_listen_tx, _stop_listen_rx) = watch::channel(HashSet::new());
        let mut wiring = Wiring::new(
            &cfg,
            Arc::clone(&registry),
            HaClient::new("ws://ha.example.lan:8123", "ha", "p"),
            Arc::new(WyomingClient::new("asr.example.lan:10200")),
            suppress_tx,
            stop_listen_tx,
            coord_in,
        );
        let mut coordinator = Coordinator::new(&cfg, Box::new(SystemClock));

        // The state the verify gate answers into (the same walk
        // `turn_state_does_not_outlive_the_turn_it_describes` uses).
        wiring
            .dispatch(
                &mut coordinator,
                CoordIn::Wake(wake_span("kitchen", 0, 1024)),
            )
            .await;
        wiring
            .dispatch(
                &mut coordinator,
                CoordIn::EndpointReached {
                    source: "kitchen".to_string(),
                    utterance_start: 0,
                    endpoint: 2048,
                },
            )
            .await;
        assert!(matches!(
            coordinator.session("kitchen"),
            Session::Processing { .. }
        ));
        let conversation = wiring.turns["kitchen"].conversation_id.clone();

        let completed = tokio::time::timeout(
            Duration::from_secs(2),
            wiring.dispatch(
                &mut coordinator,
                CoordIn::Transcript {
                    source: "kitchen".to_string(),
                    conversation_id: conversation,
                    text: "luna what time is it".to_string(),
                },
            ),
        )
        .await;
        assert!(
            completed.is_ok(),
            "dispatch must not block on the coordinator's own full queue"
        );

        // And forwarding rather than dropping: once the queue drains, the answer
        // that ends or continues the turn is still there.
        assert!(matches!(
            coord_rx.recv().await,
            Some(CoordIn::Tick)
        ));
        let answer = tokio::time::timeout(Duration::from_secs(2), coord_rx.recv()).await;
        assert!(
            matches!(answer, Ok(Some(CoordIn::Verified { .. }))),
            "the verify answer must reach the coordinator, got {answer:?}"
        );
    }

    /// L14: `run_pipeline` stamps `tts_end_at` when HA returned a TTS url, and
    /// `play_media` is the only remover. When the coordinator drops the finished
    /// pipeline as stale (its `serve_turn` guard), no `PlayMedia` follows — so the
    /// stamp survived its turn forever, one leaked entry per stale completion.
    #[tokio::test]
    async fn a_stale_pipeline_result_does_not_leak_its_tts_end_stamp() {
        let cfg = Config::parse_toml_str(&minimal_config()).unwrap();
        let registry = Arc::new(SourceRegistry::new(&cfg));
        let mut wiring = new_wiring(&cfg, registry);
        let mut coordinator = Coordinator::new(&cfg, Box::new(SystemClock));
        // What the pipeline task writes the moment HA answers with a TTS url.
        wiring
            .tts_end_at
            .lock()
            .unwrap()
            .insert("listenrr-stale".to_string(), Instant::now());

        // Nobody is waiting for this conversation: the coordinator emits no
        // PlayMedia, so nothing downstream would remove the stamp.
        wiring
            .dispatch(
                &mut coordinator,
                CoordIn::PipelineFinished {
                    source: "kitchen".to_string(),
                    conversation_id: "listenrr-stale".to_string(),
                    tts_url: Some("/api/tts_proxy/stale.mp3".to_string()),
                    mime_type: None,
                },
            )
            .await;

        assert!(
            wiring.tts_end_at.lock().unwrap().is_empty(),
            "a stamp no PlayMedia will ever claim must go with the dropped result"
        );
    }

    fn ha_with(url: &str, internal_url: &str) -> listenrr::config::Ha {
        listenrr::config::Ha {
            url: url.to_string(),
            token: "t".to_string(),
            pipeline_id: "p".to_string(),
            internal_url: internal_url.to_string(),
        }
    }

    /// `internal_url` wins when set; when empty the documented fallback is `ha.url`, which is a
    /// `ws://` address that no HTTP client can fetch a reply from, so the scheme must be mapped.
    #[test]
    fn the_reply_base_prefers_internal_url_and_otherwise_maps_the_websocket_scheme() {
        let cases: &[(&str, &str, &str)] = &[
            ("ws://ha:8123", "http://192.168.1.20:8123", "http://192.168.1.20:8123"),
            ("ws://ha:8123", "http://192.168.1.20:8123/", "http://192.168.1.20:8123"),
            ("ws://ha:8123", "  http://lan:8123///  ", "http://lan:8123"),
            ("ws://192.168.1.20:8123", "", "http://192.168.1.20:8123"),
            ("wss://ha.example.org", "", "https://ha.example.org"),
            ("http://ha:8123", "", "http://ha:8123"),
            ("https://ha:8123", "", "https://ha:8123"),
            // Only scheme + authority survive: a websocket path must not leak into the base.
            ("ws://ha:8123/api/websocket", "", "http://ha:8123"),
            ("ws://ha:8123?x=1", "", "http://ha:8123"),
        ];
        for (url, internal, want) in cases {
            assert_eq!(
                reply_base(&ha_with(url, internal)),
                *want,
                "url={url:?} internal_url={internal:?}"
            );
        }
    }

    #[test]
    fn the_play_format_comes_from_the_tts_mime_type() {
        let cases: &[(Option<&str>, &str)] = &[
            (Some("audio/mpeg"), "mp3"),
            (Some("AUDIO/MPEG"), "mp3"),
            (Some("audio/mpeg; charset=binary"), "mp3"),
            (Some("audio/flac"), "flac"),
            (Some("audio/x-flac"), "flac"),
            (Some("audio/wav"), "wav"),
            (Some("audio/x-wav"), "wav"),
            (Some("audio/wave"), "wav"),
            // Anything else is its own subtype rather than a guess.
            (Some("audio/ogg"), "ogg"),
            (Some("audio/ogg; codecs=opus"), "ogg"),
            // Nothing usable: say so instead of inventing a codec.
            (None, "auto"),
            (Some(""), "auto"),
            (Some("mpeg"), "auto"),
            (Some("audio/"), "auto"),
            // Exactly at the 15-byte cap is kept; one byte over is not.
            (Some("audio/abcdefghijklmno"), "abcdefghijklmno"),
            (Some("audio/abcdefghijklmnop"), "auto"),
        ];
        for (mime, want) in cases {
            assert_eq!(audio_format(*mime), *want, "mime={mime:?}");
        }
    }

    #[test]
    fn a_direct_play_control_carries_an_absolute_url_and_the_format() {
        let base = "http://192.168.1.20:8123";
        let play = |url: &str, mime: Option<&str>| match direct_play_control(base, url, mime) {
            proto::Control::Play { url, format } => (url, format),
            other => panic!("expected Play, got {other:?}"),
        };
        // HA hands back a relative path: it is joined to the base with exactly one slash.
        assert_eq!(
            play("/api/tts_proxy/abc.mp3", Some("audio/mpeg")),
            (
                "http://192.168.1.20:8123/api/tts_proxy/abc.mp3".to_string(),
                "mp3".to_string()
            )
        );
        assert_eq!(
            play("api/tts_proxy/abc.mp3", None).0,
            "http://192.168.1.20:8123/api/tts_proxy/abc.mp3",
            "a path with no leading slash joins the same way"
        );
        // An absolute URL is not rebased onto ours.
        assert_eq!(
            play("http://elsewhere:9/x.mp3", Some("audio/flac")),
            ("http://elsewhere:9/x.mp3".to_string(), "flac".to_string())
        );
        // A base that still carries its trailing slash does not double it.
        match direct_play_control("http://h:1/", "/p.mp3", None) {
            proto::Control::Play { url, .. } => assert_eq!(url, "http://h:1/p.mp3"),
            other => panic!("expected Play, got {other:?}"),
        }
    }

    /// The tablet route has no HA call to hang the stamp's removal on, so it must still
    /// remove it (else one entry leaks per spoken reply), and a source that is not connected
    /// is a logged condition, never a panic.
    #[tokio::test]
    async fn a_direct_reply_to_an_unconnected_source_still_claims_its_stamp() {
        let text = format!(
            "{}\n[[sources]]\nid = \"office\"\nroom = \"office\"\ngroup = \"office\"\nplayback = \"source\"\n",
            minimal_config()
        );
        let cfg = Config::parse_toml_str(&text).unwrap();
        let registry = Arc::new(SourceRegistry::new(&cfg));
        let mut wiring = new_wiring(&cfg, registry);
        wiring
            .tts_end_at
            .lock()
            .unwrap()
            .insert("listenrr-7".to_string(), Instant::now());

        wiring.play_media(
            "office".to_string(),
            String::new(),
            "/api/tts_proxy/x.mp3".to_string(),
            "listenrr-7".to_string(),
            Some("audio/mpeg".to_string()),
        );

        assert!(
            wiring.tts_end_at.lock().unwrap().is_empty(),
            "the direct route must remove the stamp exactly as the HA route does"
        );
    }

    /// The bound every await in the stop-wiring tests carries, per the same rule the
    /// verify-queue test above follows. Neither arm under test can park — the watch
    /// `send` and `send_control` are both synchronous, neither spawns nor joins
    /// anything, and no socket, thread or fixed port is involved — so an expiry here
    /// means an arm gained an unbounded await, not that the machine is slow. It is
    /// reported as a named case timeout, never as a hang `cargo test` would swallow.
    const STOP_STEP_BOUND: Duration = Duration::from_secs(2);

    /// Await one `Wiring::execute_out` under [`STOP_STEP_BOUND`], failing by name.
    async fn stop_step(wiring: &mut Wiring, out: CoordOut, case: &str) {
        if tokio::time::timeout(STOP_STEP_BOUND, wiring.execute_out(out))
            .await
            .is_err()
        {
            panic!(
                "CASE TIMEOUT: {case} after {} ms",
                STOP_STEP_BOUND.as_millis()
            );
        }
    }

    /// AC7a: the stop route is the playback route, and neither arm of the fork is
    /// interchangeable — a `Control::Stop` reaching an ESP32 board disconnects it (I3),
    /// and an HA `media_stop` cannot stop a tablet that owns no entity. Swapping the
    /// two arms reddens this test; it touches no runtime (pure, no `await`).
    #[test]
    fn the_stop_route_follows_the_playback_route() {
        assert_eq!(stop_route(Playback::Ha), StopRoute::HaStop);
        assert_eq!(stop_route(Playback::Source), StopRoute::Control);
    }

    /// Plan 4.2's wiring half: the watch the KWS thread's `StopLane` reads holds
    /// exactly the set the coordinator last published — the members on a
    /// `StopListenChanged`, and nobody on the empty one that ends a session.
    #[tokio::test]
    async fn the_stop_listen_watch_carries_exactly_the_published_set() {
        let cfg = Config::parse_toml_str(&minimal_config()).unwrap();
        let registry = Arc::new(SourceRegistry::new(&cfg));
        let mut wiring = new_wiring(&cfg, registry);
        let stop_listen = wiring.stop_listen_tx.subscribe();

        stop_step(
            &mut wiring,
            CoordOut::StopListenChanged {
                sources: vec!["kitchen".to_string(), "living".to_string()],
            },
            "stop-listen publish",
        )
        .await;
        assert_eq!(
            *stop_listen.borrow(),
            HashSet::from(["kitchen".to_string(), "living".to_string()]),
            "the watch must hold exactly the published members, nothing else"
        );

        stop_step(
            &mut wiring,
            CoordOut::StopListenChanged {
                sources: Vec::new(),
            },
            "stop-listen clear",
        )
        .await;
        assert!(
            stop_listen.borrow().is_empty(),
            "an ended session feeds the stop spotter no source"
        );
    }

    /// The `StopPlayback` half on the harness that can observe neither a socket nor an
    /// HA call (`new_wiring`'s `HaClient` never connects; the registered source has no
    /// downlink, so `send_control` answers `NotConnected`): completing is the verdict —
    /// no panic on the missing downlink — and no HA call is possible on the control
    /// route because the route fork returns before any spawn. An unconfigured source
    /// and an HA source with no output entity are both logged returns, never a call.
    /// The per-route effects themselves are proven by ha.rs's `mock_ha` and ingest.rs's
    /// real-connection tests (AC7b/c), not here.
    #[tokio::test]
    async fn a_stop_playback_without_a_downlink_is_logged_and_never_reaches_home_assistant()
    {
        let text = format!(
            "{}\n[[sources]]\nid = \"office\"\nroom = \"office\"\ngroup = \"office\"\nplayback = \"source\"\n\n[[sources]]\nid = \"board\"\nroom = \"board\"\ngroup = \"board\"\nplayback = \"ha\"\nha_entity = \"media_player.board\"\n",
            minimal_config()
        );
        let cfg = Config::parse_toml_str(&text).unwrap();
        let registry = Arc::new(SourceRegistry::new(&cfg));
        // Registered the way a hello does, with no connection behind it: the control
        // route's `send_control` answer is `NotConnected`, a log, not an error.
        register(&registry, "office");
        let mut wiring = new_wiring(&cfg, registry);

        // The tablet route with no downlink: logged, and structurally unable to reach
        // `HaClient` — the control arm returns before the HA spawn exists.
        stop_step(
            &mut wiring,
            CoordOut::StopPlayback {
                source: "office".to_string(),
                entity: String::new(),
            },
            "stop-playback source route",
        )
        .await;

        // The HA route with nothing to stop: the empty entity is the coordinator's
        // "nowhere to play", a logged skip that never spawns a call.
        stop_step(
            &mut wiring,
            CoordOut::StopPlayback {
                source: "board".to_string(),
                entity: String::new(),
            },
            "stop-playback empty entity",
        )
        .await;

        // A source that is not configured reaches neither route.
        stop_step(
            &mut wiring,
            CoordOut::StopPlayback {
                source: "ghost".to_string(),
                entity: "media_player.ghost".to_string(),
            },
            "stop-playback unknown source",
        )
        .await;
    }

    /// L10: `health` is the unit's `ExecStartPre`, and `create_dir_all` on an
    /// existing directory cannot answer whether clips can be written there — only
    /// writing can.
    #[test]
    fn health_rejects_a_clips_directory_that_cannot_be_written() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let clips = dir.path().join("clips");
        std::fs::create_dir(&clips).unwrap();
        let mut perms = std::fs::metadata(&clips).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&clips, perms).unwrap();
        // Root bypasses the mode bits, so the canary — `std::fs::write` inside
        // the directory, the same call the probe makes — says whether this world
        // enforces them before the probe's premise is asserted. The probe itself
        // must run while the directory is STILL 0555: taking the permission away
        // first would have it write into a writable directory and green-light the
        // very thing this test exists to catch.
        let canary = clips.join(".canary");
        let enforced = std::fs::write(&canary, b"x").is_err();
        let probed = probe_clips_dir(&clips);
        // Restore the owner's write bit so the tempdir can be removed. An explicit
        // 0o755, not `set_readonly(false)`, which on Unix would make it world-writable.
        std::fs::set_permissions(&clips, std::fs::Permissions::from_mode(0o755)).unwrap();
        if !enforced {
            return;
        }
        assert!(
            probed.is_err(),
            "an existing directory the service cannot write to must fail health, not green-light it"
        );

        // The writable control: created, probed, and left with nothing behind.
        let writable = dir.path().join("clips-ok");
        assert!(
            probe_clips_dir(&writable).is_ok(),
            "a fresh directory is creatable and writable"
        );
        assert!(
            !writable.join(".listenrr-health-probe").exists(),
            "health leaves no probe file behind"
        );
    }

    /// L9: the VAD every wake turn loads must load at `health` time. The offline
    /// half is the error path — naming the path the operator has to fix.
    #[test]
    fn health_rejects_a_vad_model_that_is_not_there() {
        let mut cfg = Config::parse_toml_str(&minimal_config()).unwrap();
        cfg.capture.vad_model = Some("/nonexistent/silero_vad.onnx".to_string());
        let err = probe_vad_model(&cfg)
            .expect_err("a missing VAD model must fail the health probe, not pass it");
        let msg = err.to_string();
        assert!(
            msg.contains("/nonexistent/silero_vad.onnx"),
            "the error must name the path the operator has to fix: {msg}"
        );
    }
}
