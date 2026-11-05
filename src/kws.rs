//! Keyword spotting on a dedicated thread (plan S2).
//!
//! One OS thread drains the KWS channel, converts `i16` → `f32` (see
//! [`crate::pcm_to_f32`]), feeds a per-source stream, batch-decodes ready streams and
//! emits [`WakeEvent`]s. The async runtime never touches DSP (plan principle 3), and
//! ingest never blocks on it (plan §3 invariant).
//!
//! Spotter surface (source-verified in plan §2): `KeywordSpotter::{create,
//! create_stream, is_ready(&stream), decode(&stream), decode_multiple_streams(&[&stream]),
//! reset(&stream), get_result(&stream)}` — those loop methods take the stream, they are
//! *not* stream methods; `OnlineStream::accept_waveform(sample_rate, samples)` plus
//! `input_finished`.
//!
//! Span rule (S2.3, U3): the end is the accepted-sample count at detection; the start is
//! the segment-start-derived count, refined by the first token timestamp (`f32` seconds ×
//! 16000 into the sample domain) **only** when that refinement lands inside the last
//! [`SPAN_REFINE_WINDOW_S`] seconds. Record what the timestamps actually did in
//! `docs/design/TEST-NOTES.md`.
//!
//! LANE-OWNED SEAM (Lane A, S2/S2.3/S2.4). Public signatures are frozen — implement the
//! bodies; add only private items. Report a mismatch instead of editing.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::config::Kws;

/// Sample rate fed to `accept_waveform`.
pub const SAMPLE_RATE_HZ: i32 = 16_000;
/// A first-token timestamp is only trusted inside this window before detection (U3).
pub const SPAN_REFINE_WINDOW_S: f32 = 3.0;
/// AC3: 1 h of audio must decode in < 3 min, i.e. at least this much realtime.
pub const BENCH_MIN_REALTIME_FACTOR: f64 = 20.0;

/// [`SPAN_REFINE_WINDOW_S`] in samples (3 s × 16 kHz).
const SPAN_REFINE_WINDOW_SAMPLES: u64 = crate::ms_to_samples(3_000);
/// Chunk size the bench feeds one stream (64 ms, the satellite frame size).
const BENCH_CHUNK_SAMPLES: usize = 1_024;
/// Backlog depth at which the KWS thread sheds the shared channel itself, keeping only
/// the newest frame per source (I2 shape B: crossbeam gives a producer no way to evict
/// what is already queued, so the consumer — the only side holding a receiver — is where
/// drop-oldest across the shared backlog can live). 128 frames is 8.2 s of one source's
/// audio, half the 15 s ring retention: a backlog deeper than this means the thread is
/// under-serviced (R9), and a wake that much behind would read an evicted window anyway.
const BACKLOG_SHED_FRAMES: usize = 128;
/// Synthetic source name the bench decodes under.
const BENCH_SOURCE: &str = "bench";

/// A candidate wake, proposed by the spotter. The transcript verify gate disposes (S4.2).
#[derive(Debug, Clone, PartialEq)]
pub struct WakeEvent {
    /// Source that produced it.
    pub source: String,
    /// Absolute sample span of the keyword in that source's ring (S2.3).
    pub kw_span: Range<u64>,
    /// Monotonic arrival time — used for cross-source arbitration and latency logs only.
    pub arrived: Instant,
}

/// A candidate stop, proposed by the stop-only spotter (plan rev3, step 3). Unlike a
/// [`WakeEvent`] it carries no span: the coordinator only needs who said it, and the
/// transcript verify gate is not involved — hearing the phrase is the whole decision.
#[derive(Debug, Clone, PartialEq)]
pub struct StopEvent {
    /// Source that produced it.
    pub source: String,
    /// Monotonic arrival time — latency logs only.
    pub arrived: Instant,
}

/// One spotter hit before span derivation.
#[derive(Debug, Clone, PartialEq)]
pub struct Detected {
    /// Source that produced it.
    pub source: String,
    /// Matched keyword text as configured in the keywords file.
    pub keyword: String,
    /// First token timestamp in seconds, when the spotter supplies one (U3: refine only,
    /// never primary).
    pub first_token_ts_s: Option<f32>,
}

/// A keyword spotter behind a trait so the KWS thread and `replay` can run against a
/// scripted engine while the real one stays swappable without touching thread logic.
pub trait KwsEngine: Send {
    /// Feed samples (already `f32` in `[-1, 1]`) for `source`'s stream.
    fn accept(&mut self, source: &str, samples: &[f32]);
    /// Decode every ready stream and return the hits (batched
    /// `decode_multiple_streams` in the real engine).
    fn decode_ready(&mut self) -> Vec<Detected>;
    /// Reset `source`'s stream: on a detection, and when it becomes suppressed.
    fn reset(&mut self, source: &str);
}

/// Per-source accepted-sample counters and span derivation (plan S2.3, U3).
///
/// The sample count is primary; `KeywordResult` timestamps only refine the start, and
/// only inside the last [`SPAN_REFINE_WINDOW_S`] seconds.
#[derive(Debug, Default)]
pub struct SampleCounters {
    per_source: HashMap<String, SourceCounters>,
}

/// One source's counters.
#[derive(Debug, Default, Clone)]
struct SourceCounters {
    accepted: u64,
    /// Accepted count at the last reset; `None` until the stream has been reset once.
    samples_at_reset: Option<u64>,
}

impl SampleCounters {
    /// Empty counters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `n` accepted samples for `source`; returns the new accepted total (the
    /// span end candidate).
    pub fn note(&mut self, source: &str, n: usize) -> u64 {
        let counters = self.per_source.entry(source.to_owned()).or_default();
        counters.accepted = counters.accepted.saturating_add(n as u64);
        counters.accepted
    }

    /// Total accepted samples for `source` (monotonic; the sample-domain clock).
    pub fn accepted(&self, source: &str) -> u64 {
        self.per_source.get(source).map_or(0, |c| c.accepted)
    }

    /// The accepted count at which this source's stream was last reset, so a stale
    /// segment cannot leak into the next span.
    pub fn samples_at_reset(&self, source: &str) -> Option<u64> {
        self.per_source.get(source).and_then(|c| c.samples_at_reset)
    }

    /// Note that `source`'s stream was reset at the current accepted count.
    pub fn note_reset(&mut self, source: &str) {
        let counters = self.per_source.entry(source.to_owned()).or_default();
        counters.samples_at_reset = Some(counters.accepted);
    }

    /// Restart `source`'s clock at zero (R-A).
    ///
    /// Called when a source's ring is replaced — i.e. when ingest re-registers it after a
    /// reconnect — so the counter's zero coincides with the new ring's write cursor again.
    /// Without this every span after a reconnect would be offset by the samples accepted
    /// during the previous connection, and `ring.range(span)` would read an empty window.
    /// A stream reset inside one connection is [`note_reset`](Self::note_reset), not this:
    /// that only bounds a span, it does not move the origin.
    pub fn rebase(&mut self, source: &str) {
        self.per_source.insert(
            source.to_owned(),
            SourceCounters {
                accepted: 0,
                samples_at_reset: None,
            },
        );
    }

    /// Derive the keyword span for a detection (S2.3).
    ///
    /// `segment_start` is the accepted count at the start of the current segment,
    /// `detected_at` the accepted count when the spotter reported the hit, and
    /// `first_token_ts_s` the optional token timestamp. The refinement applies only when
    /// it falls inside the last [`SPAN_REFINE_WINDOW_S`] seconds (U3); otherwise the
    /// count-derived start stands.
    pub fn span(
        &self,
        source: &str,
        segment_start: u64,
        detected_at: u64,
        first_token_ts_s: Option<f32>,
    ) -> Range<u64> {
        let end = detected_at;
        // The segment cannot start before the last reset, whatever the caller passed.
        let segment_start = match self.samples_at_reset(source) {
            Some(at) => segment_start.max(at),
            None => segment_start,
        };
        let mut start = segment_start.min(end);

        if let Some(ts) = first_token_ts_s {
            // sherpa-onnx timestamps are seconds relative to the stream start, and the
            // stream is reset at every detection, so the stream start is the segment
            // start in the accepted-sample domain. Junk timestamps (NaN, negative,
            // beyond the window) never move the start (U3).
            if ts.is_finite() && ts >= 0.0 {
                let refined = segment_start.saturating_add(crate::seconds_to_samples(ts) as u64);
                if refined >= end.saturating_sub(SPAN_REFINE_WINDOW_SAMPLES) && refined <= end {
                    start = refined;
                }
            }
        }

        start..end
    }
}

/// The stop half of the KWS thread (plan rev3, step 3): a second, stop-only spotter
/// fed only for the sources in the coordinator-derived stop-listen set. Bundled as one
/// value so [`run_kws_thread`] grows by a single parameter (plan §5 A3); the fields
/// stay private.
pub struct StopLane {
    /// The stop spotter, built by main.rs from a clone of the wake `Kws` config with
    /// `keywords_file` overridden, `threshold` = `stop_threshold` and no boosts. `None`
    /// (no `stop_keywords_file` configured) makes the lane inert: no frame is fed.
    engine: Option<Box<dyn KwsEngine>>,
    /// The current stop-listen set — members of the groups the coordinator is
    /// Processing or Speaking with, minus the muted ones (never set from here).
    listen: tokio::sync::watch::Receiver<HashSet<String>>,
    /// Sources whose stop stream is currently live. Entering the set starts a clean
    /// stream; leaving it discards the half-heard audio, so a source that stops
    /// listening never wakes the spotter with audio from before it went quiet.
    active: HashSet<String>,
    tx: mpsc::Sender<StopEvent>,
}

impl StopLane {
    /// A lane with `engine: None` is inert — the shape every wake-only test uses.
    pub fn new(
        engine: Option<Box<dyn KwsEngine>>,
        listen: tokio::sync::watch::Receiver<HashSet<String>>,
        tx: mpsc::Sender<StopEvent>,
    ) -> Self {
        Self {
            engine,
            listen,
            active: HashSet::new(),
            tx,
        }
    }
}

/// Run the KWS loop on the current thread until the channel closes (plan S2.2, S3.2).
///
/// Drains `rx`; for each `(source, samples)`: when `source` is in the current `suppress`
/// set, advance its clock and starve the engine (KWS-1 — the ring advanced for these
/// frames, so the span clock must too), else convert to `f32`, `engine.accept`, then
/// `engine.decode_ready` and map each [`Detected`] through [`SampleCounters::span`] into
/// a [`WakeEvent`] sent on `tx`. Suppressed sources keep filling their rings — that is
/// ingest's concern, not this thread's (plan invariant). A backlog deeper than
/// [`BACKLOG_SHED_FRAMES`] is shed drop-oldest, keeping the newest frame per source
/// (I2); shed frames likewise only advance the clock.
///
/// `stop` carries the second, stop-only spotter (plan rev3, step 3). Before the
/// suppression branch — so it runs for a group that is Speaking, which is exactly when
/// the wake engine is starved — its audio is fed to the stop engine while `source` is
/// in the lane's stop-listen set, and each hit becomes a [`StopEvent`] on the lane's
/// channel. The branch then falls through, so the wake path's starve-and-advance
/// behaviour below is unchanged (I5).
///
/// A zero-length `(source, [])` tuple is not audio: it is ingest's stream-restart
/// marker, sent when it registers (and unregisters) a source, and it rebases that
/// source's counter to zero so spans stay in the new ring's write-cursor domain (R-A).
/// See [`SampleCounters::rebase`]. It also ends any stop episode for that source.
///
/// Returns the number of frames processed (the `bench-kws` counter, AC3).
pub fn run_kws_thread(
    rx: crossbeam_channel::Receiver<(String, Arc<[i16]>)>,
    tx: mpsc::Sender<WakeEvent>,
    suppress: tokio::sync::watch::Receiver<HashSet<String>>,
    engine: Box<dyn KwsEngine>,
    stop: StopLane,
) -> u64 {
    let mut engine = engine;
    let mut stop = stop;
    let mut counters = SampleCounters::new();
    // Sources already reset for the current suppression episode: one C-API reset per
    // episode instead of one per dropped frame. An unsuppressed source needs no reset —
    // its stream was clean when suppression began and took no audio since.
    let mut muted: HashSet<String> = HashSet::new();
    let mut frames = 0u64;

    // One channel item, run identically by the main loop and by the post-shed pass.
    // `false` means the coordinator is gone and the thread must stop.
    // Eight parameters: bundling `muted`/`counters` into the lane struct instead would
    // churn every call site to save one argument, so the lint is allowed here (the
    // precedent is main.rs's `#[allow(clippy::too_many_arguments)]`).
    #[allow(clippy::too_many_arguments)]
    fn handle(
        item: (String, Arc<[i16]>),
        engine: &mut Box<dyn KwsEngine>,
        stop: &mut StopLane,
        counters: &mut SampleCounters,
        muted: &mut HashSet<String>,
        suppress: &tokio::sync::watch::Receiver<HashSet<String>>,
        tx: &mpsc::Sender<WakeEvent>,
        frames: &mut u64,
    ) -> bool {
        let (source, samples) = item;
        // R-A: a zero-length frame is ingest's "this stream restarts at ring cursor 0"
        // marker, sent when it registers (or, at teardown, unregisters) a source. Rebase
        // before anything else, so no sample of the previous connection leaks into a
        // span, and drop the suppression bookkeeping too: as far as this thread is
        // concerned the new connection is a source it has never heard from. It is not
        // audio, so it is not counted in the AC3 frame total.
        if samples.is_empty() {
            engine.reset(&source);
            counters.rebase(&source);
            muted.remove(&source);
            // A reconnect must not carry the old connection's audio into the stop
            // spotter either: its episode ends here, so the next listening frame starts
            // from a clean stream.
            stop.active.remove(&source);
            if let Some(stop_engine) = stop.engine.as_deref_mut() {
                stop_engine.reset(&source);
            }
            return true;
        }
        *frames += 1;

        // One conversion shared by both spotters: while a group is busy its members are
        // suppressed for the wake engine and stop-listening at the same time, so a
        // per-path conversion would pay for the same bytes twice on the hot path.
        let pcm = crate::pcm_to_f32(&samples);

        // The stop spotter (plan rev3, step 3). This runs *before* the suppression
        // early-return below and falls through to it: a group that is Speaking is
        // suppressed for the wake engine, and hearing "luna stop" over that reply is
        // the whole point — while the wake engine's starve-and-advance path, and with
        // it every span, stays exactly as it was (I5).
        let listening = stop.engine.is_some() && stop.listen.borrow().contains(&source);
        if let Some(stop_engine) = stop.engine.as_deref_mut() {
            if listening {
                if stop.active.insert(source.clone()) {
                    // Entering the set starts a clean stream: nothing heard while the
                    // group was idle may contribute to a stop.
                    stop_engine.reset(&source);
                }
                stop_engine.accept(&source, &pcm);
                for detected in stop_engine.decode_ready() {
                    let stop_event = StopEvent {
                        source: detected.source.clone(),
                        arrived: Instant::now(),
                    };
                    if stop.tx.blocking_send(stop_event).is_err() {
                        // Coordinator gone: shutdown, as for a wake.
                        return false;
                    }
                    // As on the wake side: without this the spotter keeps reporting the
                    // same keyword on every decode.
                    stop_engine.reset(&detected.source);
                }
            } else if stop.active.remove(&source) {
                // Leaving the set discards the half-heard stream, so a phrase that
                // started before the group went quiet can never stop a later session.
                stop_engine.reset(&source);
            }
        }

        if suppress.borrow().contains(&source) {
            if muted.insert(source.clone()) {
                engine.reset(&source);
                counters.note_reset(&source);
            }
            // KWS-1: a suppressed frame still reached this thread while the ring kept
            // advancing, so the clock must advance with it: a clock that skips skipped
            // frames places every later wake exactly this much too early against the
            // ring's write cursor (row 14). The engine is starved; the clock is not.
            counters.note(&source, samples.len());
            return true;
        }
        if muted.remove(&source) {
            // KWS-1: first audio after a suppression episode — the segment origin is the
            // clock *now*, when audio starts flowing to the engine again, not the stale
            // mark stamped when the episode began.
            engine.reset(&source);
            counters.note_reset(&source);
        }

        engine.accept(&source, &pcm);
        counters.note(&source, pcm.len());

        for detected in engine.decode_ready() {
            let detected_at = counters.accepted(&detected.source);
            let origin = counters.samples_at_reset(&detected.source).unwrap_or(0);
            // KWS-2: sherpa-onnx silently resets a stream after ~1.5 s of trailing
            // silence (keyword-spotter-transducer-impl.h, v2.0.1), so an origin older
            // than the refine window is untrusted — the engine's timestamps are then
            // relative to a reset this thread never observed. Bound the span to the
            // documented pre-roll window instead of anchoring on a stale origin, and
            // skip timestamp refinement: it would add an unknown offset to an unknown
            // origin.
            let origin_trusted =
                detected_at.saturating_sub(origin) <= SPAN_REFINE_WINDOW_SAMPLES;
            let (segment_start, refine_ts) = if origin_trusted {
                (origin, detected.first_token_ts_s)
            } else {
                (
                    detected_at.saturating_sub(SPAN_REFINE_WINDOW_SAMPLES),
                    None,
                )
            };
            let kw_span =
                counters.span(&detected.source, segment_start, detected_at, refine_ts);
            let wake = WakeEvent {
                source: detected.source.clone(),
                kw_span,
                arrived: Instant::now(),
            };
            if tx.blocking_send(wake).is_err() {
                // Coordinator gone: shutdown.
                return false;
            }
            // Without this the spotter keeps reporting the same keyword on every decode.
            engine.reset(&detected.source);
            counters.note_reset(&detected.source);
        }
        true
    }

    while let Ok(item) = rx.recv() {
        if !handle(
            item,
            &mut engine,
            &mut stop,
            &mut counters,
            &mut muted,
            &suppress,
            &tx,
            &mut frames,
        ) {
            return frames;
        }
        // I2: producers cannot evict a full shared backlog (crossbeam hands them a
        // Sender only), so this side — the only holder of a Receiver — sheds a deep
        // backlog itself instead of serving stale audio first. Shed frames still
        // advance the clock inside `shed_backlog`, so the invariant the restart marker
        // protects (row 14) survives the shedding.
        if rx.len() > BACKLOG_SHED_FRAMES {
            for item in shed_backlog(&rx, &mut counters) {
                if !handle(
                    item,
                    &mut engine,
                    &mut stop,
                    &mut counters,
                    &mut muted,
                    &suppress,
                    &tx,
                    &mut frames,
                ) {
                    return frames;
                }
            }
        }
    }

    frames
}

/// Drop-oldest across the shared backlog (I2): drain everything currently queued and
/// return only the newest frame per source — the one that can still contain a wake —
/// together with every marker, in queue order. Markers are never shed: they are
/// correctness messages (R-A). Every shed frame advances the clock by its sample count
/// without being fed to the engine: its audio already advanced the ring, and the clock
/// must never fall behind the ring's write cursor (row 14).
fn shed_backlog(
    rx: &crossbeam_channel::Receiver<(String, Arc<[i16]>)>,
    counters: &mut SampleCounters,
) -> Vec<(String, Arc<[i16]>)> {
    let drained: Vec<(String, Arc<[i16]>)> = rx.try_iter().collect();
    // The channel is FIFO, so the last index seen per source is its newest frame.
    let mut newest: HashMap<String, usize> = HashMap::new();
    for (i, (source, samples)) in drained.iter().enumerate() {
        if !samples.is_empty() {
            newest.insert(source.clone(), i);
        }
    }
    let mut kept: Vec<(String, Arc<[i16]>)> = Vec::new();
    let mut shed: u64 = 0;
    for (i, item) in drained.into_iter().enumerate() {
        if !item.1.is_empty() && newest.get(&item.0) != Some(&i) {
            counters.note(&item.0, item.1.len());
            shed += 1;
        } else {
            // The newest frame of its source, or a marker kept at its queue position.
            kept.push(item);
        }
    }
    if shed > 0 {
        // No registry handle reaches this thread (frozen seam), so the per-source
        // `kws_drops` stat cannot see consumer-side sheds; this line is the operator's
        // evidence instead. Producer-side drops stay counted exactly.
        tracing::warn!(
            frames = shed,
            kept = kept.len(),
            "KWS backlog shed: the thread was under-serviced; older audio dropped to \
             keep the newest frame per source (R9)"
        );
    }
    kept
}

/// Offline spotter benchmark report (S2.4, AC3). serde-serializable so numbers can be
/// recorded in `docs/design/TEST-NOTES.md` and diffed across runs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BenchReport {
    /// Model directory that was loaded.
    pub model: String,
    /// WAV file decoded.
    pub wav: String,
    /// Audio duration decoded, seconds.
    pub audio_seconds: f64,
    /// Wall-clock time spent decoding, seconds.
    pub wall_seconds: f64,
    /// `audio_seconds / wall_seconds`; AC3 requires ≥ [`BENCH_MIN_REALTIME_FACTOR`].
    pub realtime_factor: f64,
    /// Streams simulated (1 for the AC3 gate, 8 for the S0 budget check).
    pub streams: usize,
    /// Detections seen (diagnostic; a benchmark clip with no wake reports 0).
    pub detections: usize,
    /// True when `realtime_factor >= BENCH_MIN_REALTIME_FACTOR`.
    pub passes_ac3: bool,
}

/// Decode a finite buffer in frame-sized chunks the way `bench` and the replay path
/// do, with the end-of-input pass `finish` performs (KWS-3): after the last chunk the
/// feature extractor still owes a trailing window and the decoder its backlog, so a
/// keyword that ends with the clip surfaces only in that final pass. Continuous
/// satellite streams never finish, so [`run_kws_thread`] has no such pass. Also the
/// shape the replay path decodes finite clips with.
pub(crate) fn decode_finite<E: KwsEngine>(
    engine: &mut E,
    source: &str,
    samples: &[f32],
    finish: impl FnOnce(&mut E),
) -> usize {
    let mut detections = 0usize;
    for chunk in samples.chunks(BENCH_CHUNK_SAMPLES) {
        engine.accept(source, chunk);
        for _ in engine.decode_ready() {
            detections += 1;
            // Same rule as the thread: a hit is reset or the spotter repeats it.
            engine.reset(source);
        }
    }
    finish(engine);
    for _ in engine.decode_ready() {
        detections += 1;
        engine.reset(source);
    }
    detections
}

/// Decode a WAV and measure realtime factor (plan S2.4, AC3).
///
/// `model_dir` holds the spotter model; `wav` is 16 kHz mono PCM. The AC3 gate is
/// ≥ 20× realtime single-stream; ×8 is the S0 "well under 1 core" check.
pub fn bench(model_dir: &Path, wav: &Path) -> anyhow::Result<BenchReport> {
    let wave = sherpa_onnx::Wave::read(&wav.to_string_lossy())
        .ok_or_else(|| anyhow!("cannot read {} as a mono PCM WAV", wav.display()))?;
    anyhow::ensure!(
        wave.sample_rate() == SAMPLE_RATE_HZ,
        "{} is {} Hz; the spotter runs at {SAMPLE_RATE_HZ} Hz",
        wav.display(),
        wave.sample_rate()
    );

    // The bench decodes with the model's own keyword list: a downloaded bundle that ships
    // `keywords.txt` next to the model is self-contained (see `scripts/get_models.sh`).
    let cfg = Kws {
        model: model_dir.to_string_lossy().into_owned(),
        keywords_file: find_keywords_file(model_dir)?,
        // Throughput, not recall: the config default, no per-keyword boosts, and the
        // bench never runs the stop spotter.
        threshold: 0.25,
        boosts: HashMap::new(),
        stop_keywords_file: None,
        stop_threshold: None,
        wake_words: Vec::new(),
    };
    let mut engine = SherpaEngine::new(&cfg)?;

    let samples = wave.samples();
    let audio_seconds = samples.len() as f64 / f64::from(SAMPLE_RATE_HZ);
    let started = Instant::now();
    // KWS-3: the end-of-input drain is part of the measurement — a keyword that ends
    // with the clip surfaces only there.
    let detections = decode_finite(&mut engine, BENCH_SOURCE, samples, |e| {
        e.finish_input(BENCH_SOURCE)
    });
    let wall_seconds = started.elapsed().as_secs_f64();

    let realtime_factor = if wall_seconds > 0.0 {
        audio_seconds / wall_seconds
    } else {
        f64::INFINITY
    };

    Ok(BenchReport {
        model: model_dir.to_string_lossy().into_owned(),
        wav: wav.to_string_lossy().into_owned(),
        audio_seconds,
        wall_seconds,
        realtime_factor,
        streams: 1,
        detections,
        passes_ac3: realtime_factor >= BENCH_MIN_REALTIME_FACTOR,
    })
}

/// The real sherpa-onnx keyword spotter behind [`KwsEngine`] (plan O5: the official
/// crate, `sherpa-onnx = "2.0.1"`).
pub struct SherpaEngine {
    spotter: sherpa_onnx::KeywordSpotter,
    /// One `OnlineStream` per source, created on the source's first frame.
    streams: HashMap<String, sherpa_onnx::OnlineStream>,
}

impl fmt::Debug for SherpaEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `KeywordSpotter`/`OnlineStream` are raw C handles without `Debug`.
        f.debug_struct("SherpaEngine")
            .field("streams", &self.streams.len())
            .finish()
    }
}

impl SherpaEngine {
    /// Create the spotter from `[kws]` (model dir, keywords file, global threshold,
    /// per-keyword boosts).
    pub fn new(cfg: &Kws) -> anyhow::Result<Self> {
        let model_dir = resolve_model_dir(Path::new(&cfg.model))?;

        let encoder = find_model_file(&model_dir, "encoder")?;
        let decoder = find_model_file(&model_dir, "decoder")?;
        let joiner = find_model_file(&model_dir, "joiner")?;
        let tokens = find_tokens(&model_dir)?;
        let keywords = keywords_buf(&cfg.keywords_file, &cfg.boosts)?;

        let mut config = sherpa_onnx::KeywordSpotterConfig::default();
        config.model_config.transducer.encoder = Some(encoder.clone());
        config.model_config.transducer.decoder = Some(decoder.clone());
        config.model_config.transducer.joiner = Some(joiner.clone());
        config.model_config.tokens = Some(tokens.clone());
        // One KWS thread owns the spotter (plan §3: DSP stays on its own thread).
        config.model_config.num_threads = 1;
        config.keywords_threshold = cfg.threshold;
        match keywords {
            // `[kws] boosts` are applied by rewriting the lines, so the buffer replaces
            // the file (sherpa-onnx reads at most one of the two).
            Some(buf) => {
                tracing::info!(
                    model_dir = %model_dir.display(),
                    encoder,
                    decoder,
                    joiner,
                    tokens,
                    keywords_file = %cfg.keywords_file,
                    keywords = "buffer (boosts applied)",
                    keywords_threshold = cfg.threshold,
                    "keyword spotter loaded"
                );
                config.keywords_buf = Some(buf);
            }
            None => {
                tracing::info!(
                    model_dir = %model_dir.display(),
                    encoder,
                    decoder,
                    joiner,
                    tokens,
                    keywords_file = %cfg.keywords_file,
                    keywords_threshold = cfg.threshold,
                    "keyword spotter loaded"
                );
                config.keywords_file = Some(cfg.keywords_file.clone());
            }
        }

        let spotter = sherpa_onnx::KeywordSpotter::create(&config).ok_or_else(|| {
            anyhow!(
                "sherpa-onnx refused the KWS config for {}",
                model_dir.display()
            )
        })?;

        Ok(Self {
            spotter,
            streams: HashMap::new(),
        })
    }

    /// Signal end-of-input for `source` and drain the decoder backlog (KWS-3).
    ///
    /// A finite buffer — `bench`, `replay` — still owes the feature extractor its
    /// trailing window and the decoder its backlog: without this pass a keyword at the
    /// very end of a clip is reported as no detection. The live thread never calls it:
    /// satellite streams are continuous by construction.
    pub fn finish_input(&mut self, source: &str) {
        if let Some(stream) = self.streams.get(source) {
            stream.input_finished();
        }
        // `decode_ready` deliberately performs one incremental step per call; here the
        // input is finite, so drain every pending step instead of spreading them over
        // frames that will never arrive.
        loop {
            let ready: Vec<&sherpa_onnx::OnlineStream> = self
                .streams
                .values()
                .filter(|stream| self.spotter.is_ready(stream))
                .collect();
            if ready.is_empty() {
                return;
            }
            self.spotter.decode_multiple_streams(&ready);
        }
    }
}

impl KwsEngine for SherpaEngine {
    fn accept(&mut self, source: &str, samples: &[f32]) {
        if !self.streams.contains_key(source) {
            let stream = self.spotter.create_stream();
            self.streams.insert(source.to_owned(), stream);
        }
        if let Some(stream) = self.streams.get(source) {
            stream.accept_waveform(SAMPLE_RATE_HZ, samples);
        }
    }

    fn decode_ready(&mut self) -> Vec<Detected> {
        // One batched incremental step: the thread calls this per 64 ms frame, so a
        // backlog drains across frames instead of spinning here.
        let ready: Vec<&sherpa_onnx::OnlineStream> = self
            .streams
            .values()
            .filter(|stream| self.spotter.is_ready(stream))
            .collect();
        if !ready.is_empty() {
            self.spotter.decode_multiple_streams(&ready);
        }

        let mut hits = Vec::new();
        for (source, stream) in self.streams.iter() {
            if let Some(result) = self.spotter.get_result(stream) {
                if result.keyword.is_empty() {
                    continue;
                }
                // Observable for threshold/boost tuning (plan S2.1) and for U3: whether
                // the token timestamps can be trusted to refine a span at all (R4).
                tracing::debug!(
                    source = %source,
                    keyword = %result.keyword,
                    token_count = result.timestamps.len(),
                    first_token_ts_s = ?result.timestamps.first(),
                    start_time = result.start_time,
                    "keyword spotter hit"
                );
                hits.push(Detected {
                    source: source.clone(),
                    keyword: result.keyword,
                    // U3: the timestamp only ever refines the span.
                    first_token_ts_s: result.timestamps.first().copied(),
                });
            }
        }
        hits
    }

    fn reset(&mut self, source: &str) {
        // KWS-5: drop the stream rather than C-resetting it in place. `accept`
        // recreates it lazily, so a removed stream is behaviourally a reset — and this
        // is the one removal path: the thread resets on every stream-restart marker,
        // which ingest now also sends when a source unregisters, so streams for ad-hoc
        // per-client source ids cannot accumulate native state for the life of the
        // process.
        self.streams.remove(source);
    }
}

/// Accept either the model directory or a file inside it (a bundle path). A path that
/// exists as neither is an error here, not a confusing "no encoder*.onnx" later.
fn resolve_model_dir(model: &Path) -> anyhow::Result<PathBuf> {
    if model.is_dir() {
        return Ok(model.to_path_buf());
    }
    if model.is_file() {
        if let Some(parent) = model.parent() {
            if parent.is_dir() {
                return Ok(parent.to_path_buf());
            }
        }
    }
    Err(anyhow!(
        "kws.model {} is neither a model directory nor a file in one",
        model.display()
    ))
}

/// Model file for `{stem}` in `dir`.
///
/// A release ships both `{stem}-*.onnx` and `{stem}-*.int8.onnx`; the full-precision
/// one wins (plan principle 1: recall is sacred, and the 3.3 M spotter has the AC3
/// headroom either way), int8 only when that is all the directory has. Sorted so two
/// hosts end up running the same file.
fn find_model_file(dir: &Path, stem: &str) -> anyhow::Result<String> {
    let mut hits: Vec<PathBuf> = fs::read_dir(dir)
        .with_context(|| format!("listing the KWS model directory {}", dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "onnx"))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(stem))
        })
        .collect();
    hits.sort();
    let full_precision = hits.iter().find(|path| {
        !path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.contains(".int8."))
    });
    full_precision
        .or_else(|| hits.first())
        .map(|path| path.to_string_lossy().into_owned())
        .ok_or_else(|| anyhow!("no {stem}*.onnx in {}", dir.display()))
}

fn find_tokens(dir: &Path) -> anyhow::Result<String> {
    let tokens = dir.join("tokens.txt");
    anyhow::ensure!(tokens.is_file(), "no tokens.txt in {}", dir.display());
    Ok(tokens.to_string_lossy().into_owned())
}

/// A `*.txt` keyword list next to the model (`keywords.txt` preferred).
fn find_keywords_file(dir: &Path) -> anyhow::Result<String> {
    let preferred = dir.join("keywords.txt");
    if preferred.is_file() {
        return Ok(preferred.to_string_lossy().into_owned());
    }
    let mut hits: Vec<PathBuf> = fs::read_dir(dir)
        .with_context(|| format!("listing the KWS model directory {}", dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "txt"))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains("keyword"))
        })
        .collect();
    hits.sort();
    hits.into_iter()
        .next()
        .map(|path| path.to_string_lossy().into_owned())
        .ok_or_else(|| {
            anyhow!(
                "{} has no keywords list; produce one with sherpa-onnx text2token",
                dir.display()
            )
        })
}

/// Render the keywords file with `[kws] boosts` applied, or `None` when no boost is
/// configured (the file is then used verbatim).
fn keywords_buf(path: &str, boosts: &HashMap<String, f32>) -> anyhow::Result<Option<String>> {
    if boosts.is_empty() {
        return Ok(None);
    }
    let raw =
        fs::read_to_string(path).with_context(|| format!("reading the keywords file {path}"))?;
    let (buf, unmatched) = apply_boosts(&raw, boosts);
    for key in unmatched {
        tracing::warn!(
            keywords_file = path,
            boost = key,
            "boost matches no keywords line"
        );
    }
    Ok(Some(buf))
}

/// Rewrite `TOKENS [:score] [#threshold]` lines so matching keywords carry their boost.
///
/// Pure so the boost mapping is testable without a model. Returns the buffer and the
/// sorted boost keys that matched no line.
fn apply_boosts(raw: &str, boosts: &HashMap<String, f32>) -> (String, Vec<String>) {
    let wanted: HashMap<String, f32> = boosts
        .iter()
        .map(|(key, boost)| (compact_key(key), *boost))
        .collect();
    let mut applied: HashSet<String> = HashSet::new();
    let mut out = String::new();

    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            out.push_str(trimmed);
            out.push('\n');
            continue;
        }

        let (tokens, params) = match trimmed.split_once(':') {
            Some((tokens, params)) => (tokens.trim_end(), Some(params)),
            None => (trimmed, None),
        };
        let threshold = params
            .and_then(|params| params.split_once('#'))
            .map(|(_, threshold)| threshold.trim());

        match wanted.get(&keyword_text(tokens)) {
            Some(boost) => {
                applied.insert(keyword_text(tokens));
                out.push_str(&format!("{tokens} :{boost}"));
                if let Some(threshold) = threshold {
                    out.push_str(&format!(" #{threshold}"));
                }
            }
            None => {
                // KWS-4: an unmatched keyword keeps its line as it came, parameters
                // included (`params` is everything after the ':', score and threshold
                // verbatim). Re-emitting only the tokens silently re-pinned every
                // unboosted keyword to sherpa's global `keywords_score` the moment
                // any other keyword got a boost.
                out.push_str(tokens);
                if let Some(params) = params {
                    out.push_str(&format!(" :{params}"));
                }
            }
        }
        out.push('\n');
    }

    let mut unmatched: Vec<String> = boosts
        .keys()
        .filter(|key| !applied.contains(&compact_key(key)))
        .cloned()
        .collect();
    unmatched.sort();
    (out, unmatched)
}

/// `luna`, `hey luna` → `heyluna`: spaces and case are not meaningful in the config key.
fn compact_key(key: &str) -> String {
    key.chars()
        .filter(|c| !c.is_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

/// The word a keywords-file token sequence spells, e.g. `▁HE LL O` → `hello`.
fn keyword_text(tokens: &str) -> String {
    let mut text = String::new();
    for token in tokens.split_whitespace() {
        let token = token
            .trim_start_matches('\u{2581}')
            .trim_start_matches("##");
        text.push_str(&token.to_lowercase());
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Samples the fake asserts on.
    const SUPPRESSED: &[i16] = &[1_000; 1_024];

    #[derive(Debug, Default)]
    struct FakeState {
        /// `(source, samples)` in accept order.
        accepts: Vec<(String, Vec<f32>)>,
        /// Sources the spotter currently reports (cleared by `reset`).
        latched: Vec<String>,
        /// Sources that have already latched once (`fire_on_accept` latches once, then
        /// the hit sticks until `reset` — what a real spotter does).
        fired: HashSet<String>,
        /// `reset` calls, in order.
        resets: Vec<String>,
        /// `decode_ready` calls.
        decodes: usize,
    }

    #[derive(Clone, Copy, Default)]
    struct FakeBehaviour {
        /// Latch a hit as soon as samples are accepted.
        fire_on_accept: bool,
        /// Timestamp reported with every hit.
        first_token_ts_s: Option<f32>,
        /// Ignore `reset`: a spotter that re-fires (negative control).
        ignore_reset: bool,
        /// Latch only on `finish_input`: the finite-buffer case where the hit lives in
        /// the decode after the end of input (KWS-3).
        latch_on_finish: bool,
        /// Latch only from the Nth accepted frame of a source: a quiet stretch during
        /// which no thread-side reset happens (KWS-2's idle-then-wake case). 0 = off.
        latch_after_accepts: usize,
    }

    struct FakeKwsEngine {
        state: Arc<Mutex<FakeState>>,
        behaviour: FakeBehaviour,
    }

    impl FakeKwsEngine {
        fn new(behaviour: FakeBehaviour) -> (Self, Arc<Mutex<FakeState>>) {
            let state = Arc::new(Mutex::new(FakeState::default()));
            (
                Self {
                    state: state.clone(),
                    behaviour,
                },
                state,
            )
        }

        /// Mirrors [`SherpaEngine::finish_input`]: the whole file has been fed, the
        /// trailing decode is due. With `latch_on_finish` the hit surfaces only here.
        fn finish_input(&mut self, source: &str) {
            if self.behaviour.latch_on_finish {
                let mut state = self.state.lock().unwrap();
                if !state.latched.iter().any(|s| s == source) {
                    state.latched.push(source.to_owned());
                }
            }
        }
    }

    impl KwsEngine for FakeKwsEngine {
        fn accept(&mut self, source: &str, samples: &[f32]) {
            let mut state = self.state.lock().unwrap();
            state.accepts.push((source.to_owned(), samples.to_vec()));
            let already = state.latched.iter().any(|s| s == source);
            if !already {
                let due = self.behaviour.latch_after_accepts;
                let late = due > 0
                    && state
                        .accepts
                        .iter()
                        .filter(|(s, _)| s == source)
                        .count()
                        >= due;
                if late || (self.behaviour.fire_on_accept && !state.fired.contains(source)) {
                    state.fired.insert(source.to_owned());
                    state.latched.push(source.to_owned());
                }
            }
        }

        fn decode_ready(&mut self) -> Vec<Detected> {
            let mut state = self.state.lock().unwrap();
            state.decodes += 1;
            state
                .latched
                .iter()
                .map(|source| Detected {
                    source: source.clone(),
                    keyword: "\u{2581}LU NA".to_owned(),
                    first_token_ts_s: self.behaviour.first_token_ts_s,
                })
                .collect()
        }

        fn reset(&mut self, source: &str) {
            let mut state = self.state.lock().unwrap();
            state.resets.push(source.to_owned());
            if !self.behaviour.ignore_reset {
                state.latched.retain(|s| s != source);
            }
        }
    }

    fn frame(source: &str, samples: Vec<i16>) -> (String, Arc<[i16]>) {
        (source.to_owned(), Arc::from(samples.into_boxed_slice()))
    }

    /// Run the real thread over `frames` with a fixed suppression set and no stop
    /// spotter. Every wake-only test uses this shape.
    fn run(
        engine: Box<dyn KwsEngine>,
        frames: Vec<(String, Arc<[i16]>)>,
        suppressed: &[&str],
    ) -> (u64, Vec<WakeEvent>) {
        let (stop, _stop_rx) = stop_lane(None, &[]);
        run_with_stop(engine, frames, suppressed, stop)
    }

    /// [`run`] with a caller-built stop lane. The caller keeps the event receiver (it
    /// came from [`stop_lane`]) and drains it after this returns.
    fn run_with_stop(
        engine: Box<dyn KwsEngine>,
        frames: Vec<(String, Arc<[i16]>)>,
        suppressed: &[&str],
        stop: StopLane,
    ) -> (u64, Vec<WakeEvent>) {
        let (frame_tx, frame_rx) = crossbeam_channel::unbounded::<(String, Arc<[i16]>)>();
        for item in frames {
            frame_tx.send(item).unwrap();
        }
        drop(frame_tx);

        // Big enough that `blocking_send` never blocks before the test drains.
        let (wake_tx, mut wake_rx) = mpsc::channel::<WakeEvent>(256);
        let (_watch_tx, watch_rx) =
            tokio::sync::watch::channel::<HashSet<String>>(set(suppressed));

        let processed = run_kws_thread(frame_rx, wake_tx, watch_rx, engine, stop);
        let mut wakes = Vec::new();
        while let Ok(wake) = wake_rx.try_recv() {
            wakes.push(wake);
        }
        (processed, wakes)
    }

    fn set(sources: &[&str]) -> HashSet<String> {
        sources.iter().map(|source| (*source).to_owned()).collect()
    }

    /// A test stop lane: `engine: None` is inert (what every wake-only test passes),
    /// `listening` is the fixed stop-listen set. Returns the lane and the receiver its
    /// events land on — keep that alive while the thread runs.
    fn stop_lane(
        engine: Option<Box<dyn KwsEngine>>,
        listening: &[&str],
    ) -> (StopLane, mpsc::Receiver<StopEvent>) {
        let (_watch_tx, watch_rx) =
            tokio::sync::watch::channel::<HashSet<String>>(set(listening));
        let (tx, rx) = mpsc::channel::<StopEvent>(256);
        (StopLane::new(engine, watch_rx, tx), rx)
    }

    fn drain(mut rx: mpsc::Receiver<StopEvent>) -> Vec<StopEvent> {
        let mut events = Vec::new();
        while let Ok(event) = rx.try_recv() {
            events.push(event);
        }
        events
    }

    /// The KWS thread for a test that must move the suppression or the stop-listen set
    /// while it runs. The `probe` source belongs to neither set: once one of its frames
    /// has reached an engine, every frame queued before it has too (FIFO, single
    /// consumer), so flipping a set cannot race an older frame.
    struct KwsThread {
        frames: crossbeam_channel::Sender<(String, Arc<[i16]>)>,
        suppress: tokio::sync::watch::Sender<HashSet<String>>,
        listen: tokio::sync::watch::Sender<HashSet<String>>,
        wake_rx: mpsc::Receiver<WakeEvent>,
        stop_rx: mpsc::Receiver<StopEvent>,
        join: Option<std::thread::JoinHandle<u64>>,
    }

    fn spawn_kws(
        engine: Box<dyn KwsEngine>,
        stop_engine: Option<Box<dyn KwsEngine>>,
        suppressed: &[&str],
        listening: &[&str],
    ) -> KwsThread {
        let (frames, frame_rx) = crossbeam_channel::unbounded::<(String, Arc<[i16]>)>();
        let (wake_tx, wake_rx) = mpsc::channel::<WakeEvent>(256);
        let (stop_tx, stop_rx) = mpsc::channel::<StopEvent>(256);
        let (suppress, suppress_rx) =
            tokio::sync::watch::channel::<HashSet<String>>(set(suppressed));
        let (listen, listen_rx) =
            tokio::sync::watch::channel::<HashSet<String>>(set(listening));
        let stop = StopLane::new(stop_engine, listen_rx, stop_tx);
        let join = std::thread::spawn(move || {
            run_kws_thread(frame_rx, wake_tx, suppress_rx, engine, stop)
        });
        KwsThread {
            frames,
            suppress,
            listen,
            wake_rx,
            stop_rx,
            join: Some(join),
        }
    }

    /// Block until `n` frames of `source` have reached the engine behind `state`, the
    /// same bounded poll the existing thread test uses.
    fn await_accepts(state: &Arc<Mutex<FakeState>>, source: &str, n: usize) -> bool {
        let count = |state: &Arc<Mutex<FakeState>>| {
            state
                .lock()
                .unwrap()
                .accepts
                .iter()
                .filter(|(s, _)| s == source)
                .count()
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while count(state) < n && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        count(state) >= n
    }

    impl KwsThread {
        fn send(&self, item: (String, Arc<[i16]>)) {
            self.frames.send(item).unwrap();
        }

        /// Replace the stop-listen set (the coordinator's privacy control, R4).
        fn set_listen(&self, listening: &[&str]) {
            self.listen
                .send(set(listening))
                .expect("the thread holds the receiver");
        }

        /// Replace the suppression set.
        fn set_suppressed(&self, suppressed: &[&str]) {
            self.suppress
                .send(set(suppressed))
                .expect("the thread holds the receiver");
        }

        /// Close the channel, join the thread, and drain both event streams. Moving
        /// `frames` out and dropping it is what ends the thread's `rx.recv()` loop.
        fn finish(mut self) -> (u64, Vec<WakeEvent>, Vec<StopEvent>) {
            drop(self.frames);
            let processed = self.join.take().unwrap().join().unwrap();
            let mut wakes = Vec::new();
            while let Ok(wake) = self.wake_rx.try_recv() {
                wakes.push(wake);
            }
            (processed, wakes, drain(self.stop_rx))
        }
    }

    #[test]
    fn refine_window_constant_matches_the_span_rule() {
        assert_eq!(SPAN_REFINE_WINDOW_S, 3.0);
        assert_eq!(
            SPAN_REFINE_WINDOW_SAMPLES,
            crate::seconds_to_samples(SPAN_REFINE_WINDOW_S) as u64
        );
        assert_eq!(SPAN_REFINE_WINDOW_SAMPLES, 48_000);
    }

    #[test]
    fn counters_track_accepted_samples_and_reset_marks() {
        let mut counters = SampleCounters::new();
        assert_eq!(counters.accepted("kitchen"), 0);
        assert_eq!(counters.samples_at_reset("kitchen"), None);
        assert_eq!(counters.note("kitchen", 1_024), 1_024);
        assert_eq!(counters.note("kitchen", 16_000), 17_024);
        assert_eq!(counters.accepted("kitchen"), 17_024);
        assert_eq!(counters.accepted("hall"), 0, "unknown sources read as zero");

        counters.note_reset("kitchen");
        assert_eq!(counters.samples_at_reset("kitchen"), Some(17_024));
        assert_eq!(counters.note("kitchen", 1), 17_025);
        assert_eq!(
            counters.samples_at_reset("kitchen"),
            Some(17_024),
            "accepting audio does not move the reset mark"
        );
    }

    /// R-A: a reconnect replaces the ring, so the counter's zero has to move with it.
    #[test]
    fn rebase_moves_the_clock_back_to_the_new_ring_cursor() {
        let mut counters = SampleCounters::new();
        counters.note("kitchen", 500_000);
        counters.note_reset("kitchen");
        counters.rebase("kitchen");
        assert_eq!(counters.accepted("kitchen"), 0, "the origin moved to 0");
        assert_eq!(
            counters.samples_at_reset("kitchen"),
            None,
            "a mark from the previous connection cannot bound a new span"
        );
        // The next span is therefore in the new ring's domain, not the old one's.
        counters.note("kitchen", 1_024);
        assert_eq!(
            counters.span("kitchen", 0, counters.accepted("kitchen"), None),
            0..1_024
        );
    }

    /// The same property through the thread: ingest's zero-length marker rebases the
    /// source, so the wake after a reconnect is in the new ring's sample domain, and the
    /// marker itself is not counted as audio.
    #[test]
    fn a_restart_marker_rebases_the_wake_span() {
        let (engine, state) = FakeKwsEngine::new(FakeBehaviour {
            fire_on_accept: true,
            first_token_ts_s: None,
            // The spotter keeps reporting the keyword, so the only thing separating the
            // second and third wakes is the counter — which is exactly what is pinned.
            ignore_reset: true,
            ..FakeBehaviour::default()
        });
        let loud = vec![1_000_i16; 1_024];
        let (processed, wakes) = run(
            Box::new(engine),
            vec![
                frame("kitchen", loud.clone()),
                frame("kitchen", loud.clone()),
                frame("kitchen", Vec::new()), // ingest's stream-restart marker
                frame("kitchen", loud),
            ],
            &[],
        );
        assert_eq!(processed, 3, "the marker is not audio");
        assert!(
            state
                .lock()
                .unwrap()
                .resets
                .contains(&"kitchen".to_string()),
            "the spotter stream is reset at the restart too"
        );
        let spans: Vec<_> = wakes.iter().map(|w| w.kw_span.clone()).collect();
        assert_eq!(
            spans,
            vec![0..1_024, 1_024..2_048, 0..1_024],
            "the third wake is measured from the new ring cursor, not 2 048"
        );
    }

    #[test]
    fn span_uses_count_bounds_and_never_starts_before_the_last_reset() {
        let mut counters = SampleCounters::new();
        counters.note("kitchen", 16_000);
        counters.note_reset("kitchen");
        counters.note("kitchen", 32_000);

        assert_eq!(
            counters.span("kitchen", 16_000, 48_000, None),
            16_000..48_000
        );
        assert_eq!(
            counters.span("kitchen", 0, 48_000, None),
            16_000..48_000,
            "a stale segment start is clamped to the last reset"
        );
        assert_eq!(counters.span("hall", 0, 4_000, None), 0..4_000);
    }

    #[test]
    fn span_refines_the_start_inside_the_last_three_seconds() {
        let mut counters = SampleCounters::new();
        counters.note("kitchen", 160_000); // 10 s, no reset yet

        assert_eq!(
            counters.span("kitchen", 0, 160_000, Some(9.0)),
            144_000..160_000
        );
        assert_eq!(
            counters.span("kitchen", 0, 160_000, Some(7.0)),
            112_000..160_000,
            "the exact window edge is inside"
        );
    }

    #[test]
    fn span_ignores_token_timestamps_outside_the_window() {
        let mut counters = SampleCounters::new();
        counters.note("kitchen", 160_000);

        assert_eq!(
            counters.span("kitchen", 0, 160_000, Some(3.0)),
            0..160_000,
            "3.0 s is older than the last 3 s of a 10 s segment"
        );
        assert_eq!(counters.span("kitchen", 0, 160_000, Some(10.5)), 0..160_000);
        assert_eq!(
            counters.span("kitchen", 0, 160_000, Some(f32::NAN)),
            0..160_000
        );
        assert_eq!(counters.span("kitchen", 0, 160_000, Some(-1.0)), 0..160_000);
    }

    #[test]
    fn span_refinement_is_relative_to_the_segment_start() {
        let mut counters = SampleCounters::new();
        counters.note("kitchen", 160_000);
        counters.note_reset("kitchen"); // stream restarts at 160_000
        counters.note("kitchen", 160_000);

        assert_eq!(
            counters.span("kitchen", 160_000, 320_000, Some(9.0)),
            304_000..320_000,
            "9 s into the post-reset stream, not 9 s into the source"
        );
    }

    /// S2.3 refinement still applies while the segment is inside the trust bound: a
    /// stream that cannot have been reset internally for lack of elapsed time is a
    /// stream whose timestamps mean what they say.
    #[test]
    fn thread_emits_a_wake_with_the_refined_span() {
        let (engine, state) = FakeKwsEngine::new(FakeBehaviour {
            fire_on_accept: true,
            first_token_ts_s: Some(1.0),
            ignore_reset: false,
            ..FakeBehaviour::default()
        });

        let (processed, wakes) = run(
            Box::new(engine),
            vec![frame("kitchen", vec![0; 20_000])],
            &[],
        );

        assert_eq!(processed, 1);
        assert_eq!(wakes.len(), 1);
        assert_eq!(wakes[0].source, "kitchen");
        // 1.0 s into a segment the engine cannot have reset: refined, not count-derived.
        assert_eq!(wakes[0].kw_span, 16_000..20_000);

        let state = state.lock().unwrap();
        assert_eq!(state.accepts.len(), 1);
        assert_eq!(state.accepts[0].0, "kitchen");
        assert_eq!(state.accepts[0].1.len(), 20_000);
        assert_eq!(state.resets, vec!["kitchen".to_owned()]);
    }

    /// KWS-2: past the trust bound the engine's word is worthless — sherpa-onnx resets a
    /// stream internally after ~1.5 s of trailing silence, and the thread cannot see it.
    /// The count-derived start is then bounded to the documented pre-roll window, and the
    /// stream-relative timestamp is not applied at all (it would have moved the start to
    /// 144 000, an unknown distance from the truth).
    #[test]
    fn thread_bounds_an_untrusted_segment_and_ignores_the_timestamp() {
        let (engine, _state) = FakeKwsEngine::new(FakeBehaviour {
            fire_on_accept: true,
            first_token_ts_s: Some(9.0),
            ignore_reset: false,
            ..FakeBehaviour::default()
        });

        let (_, wakes) = run(
            Box::new(engine),
            vec![frame("kitchen", vec![0; 160_000])],
            &[],
        );

        assert_eq!(wakes.len(), 1);
        assert_eq!(
            wakes[0].kw_span,
            112_000..160_000,
            "bounded to the 3 s window; never the stale origin, never the timestamp"
        );
    }

    #[test]
    fn reset_prevents_refiring_unlike_an_engine_that_ignores_it() {
        let (engine, state) = FakeKwsEngine::new(FakeBehaviour {
            fire_on_accept: true,
            ..FakeBehaviour::default()
        });
        let (processed, wakes) = run(
            Box::new(engine),
            vec![
                frame("kitchen", vec![0; 1_024]),
                frame("kitchen", vec![0; 1_024]),
                frame("kitchen", vec![0; 1_024]),
            ],
            &[],
        );
        assert_eq!(processed, 3);
        assert_eq!(wakes.len(), 1, "one hit, then the stream is reset");
        let state = state.lock().unwrap();
        assert_eq!(state.resets.len(), 1);
        assert_eq!(
            state.decodes, 3,
            "one batched decode pass per frame, not one per source"
        );
        drop(state);

        // Control: a spotter that ignores `reset` re-reports on every decode, which is
        // what the thread's reset prevents.
        let (engine, _state) = FakeKwsEngine::new(FakeBehaviour {
            fire_on_accept: true,
            ignore_reset: true,
            ..FakeBehaviour::default()
        });
        let (_, wakes) = run(
            Box::new(engine),
            vec![frame("kitchen", vec![0; 1_024]); 3],
            &[],
        );
        assert_eq!(wakes.len(), 3);
    }

    #[test]
    fn suppressed_source_drops_frames_and_is_reset_once() {
        let (engine, state) = FakeKwsEngine::new(FakeBehaviour {
            fire_on_accept: true,
            ..FakeBehaviour::default()
        });

        let (processed, wakes) = run(
            Box::new(engine),
            vec![
                frame("kitchen", SUPPRESSED.to_vec()),
                frame("kitchen", SUPPRESSED.to_vec()),
                frame("hall", vec![0; 1_024]),
            ],
            &["kitchen"],
        );

        assert_eq!(processed, 3, "suppressed frames are still counted");
        assert_eq!(wakes.len(), 1);
        assert_eq!(wakes[0].source, "hall");

        let state = state.lock().unwrap();
        assert!(
            state.accepts.iter().all(|(source, _)| source == "hall"),
            "no audio reaches a suppressed source's stream: {:?}",
            state.accepts
        );
        // `hall`'s reset is the post-detection one; `kitchen` is reset once for the
        // whole suppression episode, not once per dropped frame.
        assert_eq!(
            state.resets.iter().filter(|s| *s == "kitchen").count(),
            1,
            "one reset per suppression episode: {:?}",
            state.resets
        );
        assert!(
            state.resets.iter().any(|s| s == "hall"),
            "the detecting source is reset too: {:?}",
            state.resets
        );
    }

    /// KWS-1: the suppression clock regression. Three frames arrive while `kitchen` is
    /// suppressed, the set is cleared, and a fourth frame carries the wake. The ring
    /// advanced for all four, so the wake must be measured against 4 096 samples — the
    /// pre-fix thread skipped suppressed frames entirely and reported this wake at 1 024,
    /// reading a window three frames too early.
    #[test]
    fn suppressed_frames_advance_the_clock_and_the_resume_rebases_the_origin() {
        let (engine, state) = FakeKwsEngine::new(FakeBehaviour {
            fire_on_accept: true,
            ..FakeBehaviour::default()
        });
        let (frame_tx, frame_rx) = crossbeam_channel::unbounded::<(String, Arc<[i16]>)>();
        let (wake_tx, mut wake_rx) = mpsc::channel::<WakeEvent>(256);
        let (suppress_tx, suppress_rx) =
            tokio::sync::watch::channel(HashSet::from(["kitchen".to_owned()]));
        let (stop, _stop_rx) = stop_lane(None, &[]);
        let engine: Box<dyn KwsEngine> = Box::new(engine);
        let thread = std::thread::spawn(move || {
            run_kws_thread(frame_rx, wake_tx, suppress_rx, engine, stop)
        });

        // The probe source is never suppressed: once its audio has been processed, every
        // frame queued before it (FIFO, single consumer) has too — so the flip below
        // cannot race the thread reading the watch for an earlier kitchen frame.
        frame_tx.send(frame("kitchen", vec![0i16; 1_024])).unwrap();
        frame_tx.send(frame("kitchen", vec![0i16; 1_024])).unwrap();
        frame_tx.send(frame("kitchen", vec![0i16; 1_024])).unwrap();
        frame_tx.send(frame("probe", vec![1i16; 1_024])).unwrap();
        let probe_count = |state: &Arc<Mutex<FakeState>>| -> usize {
            state
                .lock()
                .unwrap()
                .accepts
                .iter()
                .filter(|(s, _)| s == "probe")
                .count()
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while probe_count(&state) < 1 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(probe_count(&state) >= 1, "the probe frame was never processed");

        suppress_tx.send(HashSet::new()).expect("the thread holds the receiver");
        // The wake frame: the only kitchen audio the engine may ever see.
        frame_tx.send(frame("kitchen", vec![2i16; 1_024])).unwrap();
        frame_tx.send(frame("probe", vec![3i16; 1_024])).unwrap();
        while probe_count(&state) < 2 && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(probe_count(&state) >= 2, "the second probe frame was never processed");
        drop(frame_tx);
        let processed = thread.join().unwrap();
        assert_eq!(processed, 6, "every frame counted, probes included");

        let mut kitchen_wakes: Vec<WakeEvent> = Vec::new();
        while let Ok(wake) = wake_rx.try_recv() {
            if wake.source == "kitchen" {
                kitchen_wakes.push(wake);
            }
        }
        assert_eq!(kitchen_wakes.len(), 1);
        // Clock in ring coordinates: three suppressed frames plus the wake frame. The
        // origin moved to where audio started flowing again (3 072) — not the entry's
        // stale mark (0) and not the wake frame alone (1 024).
        assert_eq!(kitchen_wakes[0].kw_span, 3 * 1_024..4 * 1_024);

        let state = state.lock().unwrap();
        assert_eq!(
            state.accepts.iter().filter(|(s, _)| s == "kitchen").count(),
            1,
            "only the unsuppressed frame was fed to the engine"
        );
        assert_eq!(
            state.resets.iter().filter(|s| *s == "kitchen").count(),
            3,
            "reset entering the episode, reset at the resume, reset after the wake"
        );
    }

    /// AC5d: the stop spotter is fed only for sources the coordinator put in the
    /// stop-listen set. An idle group's audio must never reach it (R4).
    #[test]
    fn a_source_outside_the_stop_listen_set_is_never_fed_to_it() {
        let (wake_engine, _wake_state) = FakeKwsEngine::new(FakeBehaviour::default());
        let (stop_engine, stop_state) = FakeKwsEngine::new(FakeBehaviour::default());
        let (stop, stop_rx) = stop_lane(Some(Box::new(stop_engine)), &[]);

        let (processed, wakes) = run_with_stop(
            Box::new(wake_engine),
            vec![
                frame("kitchen", vec![0_i16; 1_024]),
                frame("hall", vec![0_i16; 1_024]),
            ],
            &[],
            stop,
        );
        assert_eq!(processed, 2);
        assert!(wakes.is_empty(), "a scripted engine never fires");

        let state = stop_state.lock().unwrap();
        assert!(
            state.accepts.is_empty(),
            "no audio reaches the stop engine outside the listen set: {:?}",
            state.accepts
        );
        assert!(
            state.resets.is_empty() && state.decodes == 0,
            "nor is it even poked: resets={:?} decodes={}",
            state.resets,
            state.decodes
        );
        assert!(drain(stop_rx).is_empty());
    }

    /// AC5d: the case the feature exists for. `kitchen` is suppressed (its group is
    /// Speaking) *and* in the stop-listen set: the stop engine must be fed while the
    /// wake engine stays starved, and the sample clock must still advance — the wake
    /// that follows the reply has to land at 3 072..4 096, not at 0..1 024 (KWS-1, I5).
    #[test]
    fn a_suppressed_source_in_the_listen_set_feeds_the_stop_engine_and_the_clock() {
        let (wake_engine, wake_state) = FakeKwsEngine::new(FakeBehaviour {
            fire_on_accept: true,
            ..FakeBehaviour::default()
        });
        let (stop_engine, stop_state) = FakeKwsEngine::new(FakeBehaviour::default());
        let thread = spawn_kws(
            Box::new(wake_engine),
            Some(Box::new(stop_engine)),
            &["kitchen"],
            &["kitchen"],
        );

        for _ in 0..3 {
            thread.send(frame("kitchen", SUPPRESSED.to_vec()));
        }
        thread.send(frame("probe", vec![1_i16; 1_024]));
        assert!(
            await_accepts(&wake_state, "probe", 1),
            "the probe frame was never processed"
        );

        {
            let wake = wake_state.lock().unwrap();
            assert_eq!(
                wake.accepts.iter().filter(|(s, _)| s == "kitchen").count(),
                0,
                "the wake engine stays starved while its group is busy: {:?}",
                wake.accepts
            );
            let stop = stop_state.lock().unwrap();
            assert_eq!(
                stop.accepts.iter().filter(|(s, _)| s == "kitchen").count(),
                3,
                "the stop engine is fed the frames the wake engine was denied"
            );
            assert_eq!(
                stop.accepts.len(),
                3,
                "the probe is not in the listen set: {:?}",
                stop.accepts
            );
        }

        thread.set_suppressed(&[]);
        // The wake frame: the only kitchen audio the wake engine may ever see.
        thread.send(frame("kitchen", vec![2_i16; 1_024]));
        let (processed, wakes, stops) = thread.finish();
        assert_eq!(processed, 5, "every frame counted, the probe included");

        let kitchen_wakes: Vec<WakeEvent> = wakes
            .into_iter()
            .filter(|wake| wake.source == "kitchen")
            .collect();
        assert_eq!(kitchen_wakes.len(), 1);
        assert_eq!(
            kitchen_wakes[0].kw_span,
            3 * 1_024..4 * 1_024,
            "the stop path may not skip the clock the suppressed frames own"
        );
        assert!(stops.is_empty(), "the scripted stop engine never detects");

        let wake = wake_state.lock().unwrap();
        assert_eq!(
            wake.accepts.iter().filter(|(s, _)| s == "kitchen").count(),
            1,
            "only the unsuppressed frame was fed to the wake engine"
        );
        let stop = stop_state.lock().unwrap();
        assert_eq!(
            stop.accepts.iter().filter(|(s, _)| s == "kitchen").count(),
            4,
            "all four kitchen frames reached the stop engine, including the one the \
             wake engine also saw"
        );
    }

    /// AC5d: leaving the stop-listen set discards the half-heard stream — exactly once,
    /// not once per frame afterwards (the same episode discipline the suppression path
    /// uses). A phrase that began before the group went quiet must never stop a later
    /// session, and a source that stops listening must cost no per-frame C-API resets.
    #[test]
    fn leaving_the_stop_listen_set_resets_the_stop_engine_once() {
        let (wake_engine, wake_state) = FakeKwsEngine::new(FakeBehaviour::default());
        let (stop_engine, stop_state) = FakeKwsEngine::new(FakeBehaviour::default());
        let thread = spawn_kws(
            Box::new(wake_engine),
            Some(Box::new(stop_engine)),
            &[],
            &["kitchen"],
        );

        thread.send(frame("kitchen", vec![0_i16; 1_024]));
        thread.send(frame("kitchen", vec![0_i16; 1_024]));
        thread.send(frame("probe", vec![1_i16; 1_024]));
        assert!(
            await_accepts(&wake_state, "probe", 1),
            "the probe frame was never processed"
        );

        thread.set_listen(&[]);
        // Two frames after leaving the set: the stream is discarded once, not per frame.
        thread.send(frame("kitchen", vec![0_i16; 1_024]));
        thread.send(frame("kitchen", vec![0_i16; 1_024]));
        let (processed, wakes, stops) = thread.finish();
        assert_eq!(processed, 5, "two frames listening, the probe, two frames after");
        assert!(wakes.is_empty() && stops.is_empty());

        let state = stop_state.lock().unwrap();
        assert_eq!(
            state.accepts.iter().filter(|(s, _)| s == "kitchen").count(),
            2,
            "nothing is fed after the source leaves the set: {:?}",
            state.accepts
        );
        assert_eq!(
            state.resets.iter().filter(|s| *s == "kitchen").count(),
            2,
            "one reset entering the set, one leaving it, none per frame: {:?}",
            state.resets
        );
    }

    /// AC5d: a stream-restart marker must leave neither the old connection's audio nor
    /// a live episode behind, so the first frame of the new stream starts a clean one.
    #[test]
    fn a_restart_marker_resets_the_stop_engine_and_clears_its_episode() {
        let (wake_engine, _wake_state) = FakeKwsEngine::new(FakeBehaviour::default());
        let (stop_engine, stop_state) = FakeKwsEngine::new(FakeBehaviour::default());
        let (stop, stop_rx) = stop_lane(Some(Box::new(stop_engine)), &["kitchen"]);

        let (processed, wakes) = run_with_stop(
            Box::new(wake_engine),
            vec![
                frame("kitchen", vec![0_i16; 1_024]),
                frame("kitchen", vec![0_i16; 1_024]),
                frame("kitchen", Vec::new()), // ingest's stream-restart marker
                frame("kitchen", vec![0_i16; 1_024]),
            ],
            &[],
            stop,
        );
        assert_eq!(processed, 3, "the marker is not audio");
        assert!(wakes.is_empty() && drain(stop_rx).is_empty());

        let state = stop_state.lock().unwrap();
        assert_eq!(
            state.accepts.iter().filter(|(s, _)| s == "kitchen").count(),
            3,
            "the source is listening throughout: {:?}",
            state.accepts
        );
        // Entering the set, the marker, and re-entering it afterwards. The third reset
        // is what proves the marker cleared the episode: had the episode survived, the
        // frame after the reconnect would have been appended to the dead stream.
        assert_eq!(
            state.resets,
            vec!["kitchen".to_owned(); 3],
            "reset entering, reset at the restart, reset again on the new stream"
        );
    }

    /// AC6: one detection is exactly one `StopEvent`, the stop stream is reset behind
    /// it, and the stop spotter produces no `WakeEvent`.
    #[test]
    fn a_stop_detection_emits_one_stop_event_and_resets_the_stream() {
        let (wake_engine, wake_state) = FakeKwsEngine::new(FakeBehaviour::default());
        // The same behaviour as the wake side: latch on the first frame, then stay
        // latched until `reset` — which is exactly what the thread owes a hit.
        let (stop_engine, stop_state) = FakeKwsEngine::new(FakeBehaviour {
            fire_on_accept: true,
            ..FakeBehaviour::default()
        });
        let (stop, stop_rx) = stop_lane(Some(Box::new(stop_engine)), &["kitchen"]);

        let (processed, wakes) = run_with_stop(
            Box::new(wake_engine),
            vec![
                frame("kitchen", vec![0_i16; 1_024]),
                frame("kitchen", vec![0_i16; 1_024]),
            ],
            &[],
            stop,
        );
        assert_eq!(processed, 2);

        let events = drain(stop_rx);
        assert_eq!(events.len(), 1, "one detection, one event: {events:?}");
        assert_eq!(events[0].source, "kitchen");

        let state = stop_state.lock().unwrap();
        assert_eq!(state.accepts.iter().filter(|(s, _)| s == "kitchen").count(), 2);
        // The first is entering the set, the second follows the detection — without it,
        // the second frame's decode would report the same keyword again.
        assert_eq!(
            state.resets,
            vec!["kitchen".to_owned(), "kitchen".to_owned()],
            "the stop stream is reset after the detection"
        );
        drop(state);

        // The wake engine is fed as usual (the stop path falls through to it) but never
        // fires, so nothing here can be a wake the stop spotter invented.
        assert!(wakes.is_empty(), "the stop spotter emits no WakeEvent");
        let wake = wake_state.lock().unwrap();
        assert_eq!(
            wake.accepts.iter().filter(|(s, _)| s == "kitchen").count(),
            2,
            "the wake path still saw the same audio: {:?}",
            wake.accepts
        );
    }

    /// I2: when the consumer sheds a stale backlog, shed frames must still advance the
    /// clock — their audio already advanced the ring, and the span clock may never fall
    /// behind the ring's write cursor (row 14). With the shed frames dropped outright,
    /// the final wake would end at 2 048 samples, three orders of magnitude off.
    #[test]
    fn a_shed_backlog_keeps_the_clock_even_with_the_ring() {
        let (engine, state) = FakeKwsEngine::new(FakeBehaviour {
            fire_on_accept: true,
            ignore_reset: true, // keep latching so every kept frame reports a wake
            ..FakeBehaviour::default()
        });
        let mut frames: Vec<_> = (0..200u16)
            .map(|i| frame("kitchen", vec![i as i16; 1_024]))
            .collect();
        frames.extend((0..5u16).map(|i| frame("hall", vec![i as i16; 1_024])));
        let (processed, wakes) = run(Box::new(engine), frames, &[]);
        assert_eq!(processed, 3, "the first frame, then the two kept newest");

        let state = state.lock().unwrap();
        let fed: Vec<&String> = state.accepts.iter().map(|(s, _)| s).collect();
        assert_eq!(
            fed.iter().filter(|s| **s == "kitchen").count(),
            2,
            "only the first and the newest kitchen frame reach the engine: {fed:?}"
        );
        assert_eq!(fed.iter().filter(|s| **s == "hall").count(), 1);
        let last_kitchen = state
            .accepts
            .iter()
            .rev()
            .find(|(s, _)| s == "kitchen")
            .unwrap()
            .1
            .clone();
        assert_eq!(
            last_kitchen,
            crate::pcm_to_f32(&[199i16; 1_024]),
            "the kept kitchen frame is the NEWEST, not a stale one"
        );
        drop(state);

        let kitchen_end = wakes
            .iter()
            .filter(|w| w.source == "kitchen")
            .map(|w| w.kw_span.end)
            .max()
            .expect("a kitchen wake");
        assert_eq!(
            kitchen_end,
            200 * 1_024,
            "the clock counted every shed frame: the ring advanced for all 200"
        );
        let hall_end = wakes
            .iter()
            .filter(|w| w.source == "hall")
            .map(|w| w.kw_span.end)
            .max()
            .expect("a hall wake");
        assert_eq!(hall_end, 5 * 1_024, "same for the shed hall frames");
    }

    /// I2's critical half: a stream-restart marker is never the item that gets shed —
    /// losing one re-opens the I1 wound (a clock matching no ring). Here the marker sits
    /// deep inside a backlog that is otherwise dropped, and still rebases the clock: the
    /// kept frame is counted from 0, where dropping the marker would have left the wake
    /// ending at 132 288 samples.
    #[test]
    fn a_marker_survives_the_backlog_shed_and_rebases() {
        let (engine, state) = FakeKwsEngine::new(FakeBehaviour {
            fire_on_accept: true,
            ignore_reset: true,
            ..FakeBehaviour::default()
        });
        let mut frames: Vec<_> = (0..130u16)
            .map(|_| frame("kitchen", vec![0i16; 1_024]))
            .collect();
        frames.push(frame("kitchen", Vec::new())); // the restart marker, mid-backlog
        frames.push(frame("kitchen", vec![1i16; 1_024])); // post-restart audio
        let (_, wakes) = run(Box::new(engine), frames, &[]);

        let last_end = wakes.iter().map(|w| w.kw_span.end).max().expect("a wake");
        assert_eq!(
            last_end, 1_024,
            "the kept frame was counted after the marker's rebase"
        );
        let state = state.lock().unwrap();
        let last_fed = state.accepts.iter().rev().find(|(s, _)| s == "kitchen").unwrap();
        assert_eq!(
            last_fed.1,
            crate::pcm_to_f32(&[1i16; 1_024]),
            "the frame kept alongside the marker is the newest one"
        );
    }

    /// KWS-2: the common idle case. Nothing fires while the room is quiet, so no
    /// thread-side reset ever marks the segment — and when a hit finally arrives after a
    /// long stretch, the stream is stale in the way the thread cannot see (sherpa-onnx
    /// resets internally after ~1.5 s of silence). The start must not be the hours-old
    /// implicit origin 0, and a timestamp must not be applied to it: it is bounded to the
    /// documented pre-roll window.
    #[test]
    fn a_wake_after_a_long_quiet_stretch_is_bounded_to_the_preroll() {
        let (engine, _state) = FakeKwsEngine::new(FakeBehaviour {
            latch_after_accepts: 100,
            first_token_ts_s: Some(6.0),
            ..FakeBehaviour::default()
        });
        let (_, wakes) = run(
            Box::new(engine),
            vec![frame("kitchen", vec![0i16; 1_024]); 100],
            &[],
        );
        assert_eq!(wakes.len(), 1, "the hit lands once, at the 100th frame");
        // A trusted origin would have refined to 96 000 (6.0 s); the untrusted path
        // ignores the timestamp and bounds the window instead.
        assert_eq!(
            wakes[0].kw_span,
            54_400..102_400,
            "end at detection, start bounded to 3 s — never the stale origin"
        );
    }

    /// KWS-3: a finite buffer's keyword lives in the decode AFTER the input ends —
    /// the feature extractor's trailing window and the decoder backlog. This is exactly
    /// what `bench` and `replay` used to be: chunk, decode, done — and a clip whose
    /// wake word ends with the file reported zero detections.
    #[test]
    fn decode_finite_drains_the_end_of_input_pass() {
        let (mut engine, state) = FakeKwsEngine::new(FakeBehaviour {
            latch_on_finish: true,
            ..FakeBehaviour::default()
        });
        let samples = vec![0.0f32; 3 * 1_024];

        // Without the end-of-input pass the hit is invisible, even though every chunk
        // was fed: this pins both halves of the contract.
        let plain = decode_finite(&mut engine, "kitchen", &samples, |_| {});
        assert_eq!(plain, 0, "a chunk loop alone never drains the final decode");
        assert_eq!(
            state.lock().unwrap().accepts.len(),
            3,
            "every frame-sized chunk was fed to the engine"
        );

        let with_finish = decode_finite(&mut engine, "kitchen", &samples, |e| {
            e.finish_input("kitchen")
        });
        assert_eq!(with_finish, 1, "the end-of-input pass surfaces the final hit");
        let state = state.lock().unwrap();
        assert_eq!(state.accepts.len(), 6, "the second decode re-feeds three chunks");
        assert_eq!(
            state.resets.iter().filter(|s| *s == "kitchen").count(),
            1,
            "only the post-detection reset; the hit is not reported twice"
        );
    }

    #[test]
    fn i16_to_f32_conversion_is_exact_at_both_rails() {
        let input = [0, i16::MAX, i16::MIN, 1, -1];
        let (engine, state) = FakeKwsEngine::new(FakeBehaviour::default());
        let _ = run(
            Box::new(engine),
            vec![frame("kitchen", input.to_vec())],
            &[],
        );

        let state = state.lock().unwrap();
        let converted = &state.accepts[0].1;
        assert_eq!(converted.len(), input.len());
        assert_eq!(converted[0], 0.0);
        assert_eq!(converted[1], f32::from(i16::MAX) / 32_768.0);
        assert!(converted[1] < 1.0, "positive rail never reaches +1.0");
        assert_eq!(converted[2], -1.0);
        assert_eq!(converted[3], 1.0 / 32_768.0);
        assert_eq!(converted[4], -1.0 / 32_768.0);
        assert_eq!(*converted, crate::pcm_to_f32(&input));
    }

    #[test]
    fn boosts_rewrite_matching_lines_and_report_the_rest() {
        // The shipped gigaspeech bundle writes lines as `▁HE LL O ▁WORLD` (a bare `▁`
        // token marks a word start), and `test_keywords.txt` as `▁ L IGHT ▁UP`.
        let raw = "\u{2581}HE LL O :1.0 #0.35\n\u{2581}LU NA\n\u{2581} L IGHT \u{2581}UP\n# keywords comment\n\n";
        let boosts: HashMap<String, f32> = [
            ("hello".to_owned(), 2.0),
            ("lu na".to_owned(), 3.5),
            ("light up".to_owned(), 1.5),
            ("nobody".to_owned(), 4.0),
        ]
        .into_iter()
        .collect();

        let (buffered, unmatched) = apply_boosts(raw, &boosts);

        assert!(
            buffered.contains("\u{2581}HE LL O :2 #0.35"),
            "boost replaces the inline score and the threshold survives: {buffered}"
        );
        assert!(
            buffered.contains("\u{2581}LU NA :3.5"),
            "a boost applies to a line without an inline score: {buffered}"
        );
        assert!(
            buffered.contains("\u{2581} L IGHT \u{2581}UP :1.5"),
            "the config key \"light up\" matches the tokenized line: {buffered}"
        );
        assert!(buffered.contains("# keywords comment"));
        assert_eq!(unmatched, vec!["nobody".to_owned()]);

        // No boosts configured: the file is used verbatim, no buffer is produced.
        assert!(keywords_buf("/nonexistent/keywords.txt", &HashMap::new())
            .unwrap()
            .is_none());
    }

    /// KWS-4: an unboosted keyword must keep its line verbatim. Before the fix, an
    /// unmatched line was re-emitted as bare tokens, silently re-pinning every
    /// unboosted keyword to sherpa's global `keywords_score` the moment any other
    /// keyword got a boost.
    #[test]
    fn an_unmatched_line_keeps_its_inline_score_and_threshold() {
        let raw = "\u{2581}LU NA\n\u{2581}H EE Y \u{2581}M Y \u{2581}F R IE ND :0.9 #0.6\n\u{2581}B Y E :2.0\n";
        let boosts: HashMap<String, f32> =
            [("lu na".to_owned(), 3.0f32)].into_iter().collect();

        let (buffered, unmatched) = apply_boosts(raw, &boosts);

        assert!(
            buffered.contains("\u{2581}LU NA :3"),
            "the boosted line is rewritten: {buffered}"
        );
        assert!(
            buffered
                .contains("\u{2581}H EE Y \u{2581}M Y \u{2581}F R IE ND :0.9 #0.6"),
            "an unboosted keyword keeps its inline score AND threshold: {buffered}"
        );
        assert!(
            buffered.contains("\u{2581}B Y E :2.0"),
            "an unboosted keyword keeps its inline score: {buffered}"
        );
        assert!(unmatched.is_empty(), "every boost matched: {unmatched:?}");
    }

    #[test]
    fn model_discovery_prefers_full_precision_weights() {
        let dir = tempfile::tempdir().unwrap();
        let touch = |name: &str| {
            fs::write(dir.path().join(name), b"").unwrap();
        };
        // What `scripts/get_models.sh` unpacks: int8 and full precision per stem.
        touch("encoder-epoch-12.int8.onnx");
        touch("encoder-epoch-12.onnx");
        touch("decoder-epoch-12.int8.onnx");
        touch("decoder-epoch-12.onnx");
        touch("joiner-epoch-12.onnx");
        touch("tokens.txt");
        touch("keywords.txt");

        for (stem, expected) in [
            ("encoder", "encoder-epoch-12.onnx"),
            ("decoder", "decoder-epoch-12.onnx"),
            ("joiner", "joiner-epoch-12.onnx"),
        ] {
            let found = find_model_file(dir.path(), stem).unwrap();
            assert!(
                found.ends_with(expected),
                "{stem}: picked {found}, wanted the full-precision file"
            );
        }
        assert!(find_tokens(dir.path()).unwrap().ends_with("tokens.txt"));
        assert!(find_keywords_file(dir.path())
            .unwrap()
            .ends_with("keywords.txt"));

        // int8 only: still usable.
        let int8_only = tempfile::tempdir().unwrap();
        fs::write(int8_only.path().join("encoder-9.int8.onnx"), b"").unwrap();
        assert!(find_model_file(int8_only.path(), "encoder")
            .unwrap()
            .ends_with("encoder-9.int8.onnx"));

        // Nothing for a stem, and no keyword list: honest errors, no panics.
        let empty = tempfile::tempdir().unwrap();
        assert!(find_model_file(empty.path(), "encoder").is_err());
        assert!(find_tokens(empty.path()).is_err());
        assert!(find_keywords_file(empty.path()).is_err());
        assert!(resolve_model_dir(&empty.path().join("missing")).is_err());
        assert_eq!(
            resolve_model_dir(&dir.path().join("tokens.txt")).unwrap(),
            dir.path()
        );
    }

    #[test]
    fn bench_realtime_floor_is_the_plan_gate() {
        assert_eq!(BENCH_MIN_REALTIME_FACTOR, 20.0);
    }
}
