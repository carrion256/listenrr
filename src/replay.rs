//! Offline instruments: `replay <wav>` and the shared tuning harness (plan S3.4, S2.4).
//!
//! `replay` runs the whole decision chain over a WAV with no network: KWS → capture →
//! transcribe → verify, with HA replaced by a **counting no-op sink**. That sink is what
//! makes AC4's negative clause measurable — "0 HA calls on the negative set" is a
//! counter, not an absence of evidence (plan C2-1).
//!
//! It prints/asserts: wake, keyword span, utterance bounds, transcript, verify result,
//! HA-call count, and the **utterance-end margin** (last-speech → endpoint distance),
//! which is the truncation-rate instrument for tuning `capture.trailing_silence_ms`
//! (S3.4). Fixture results belong in `docs/design/TEST-NOTES.md`, and a failing check is never
//! weakened to pass — tune via config and record the change (plan §7).
//!
//! # Where the transcript comes from
//!
//! **Zero audio samples leave the machine.** The ASR stage is the `listenrr-replay` fake:
//! if a sidecar `<stem>.transcript.json` sits next to the WAV, its `transcript` is used
//! and no socket is opened — which is what makes the fixture battery reproducible and
//! CI-able. With no sidecar, the real [`crate::parakeet`] client runs, which is the plan's
//! AC4 form: the instrument is then executed *on* the backend host, next to the Wyoming service, so
//! the samples still do not leave that machine. Which path ran is stated in the log line.
//!
//! The sidecar is JSON with two keys, `transcript` required and `expected_endpoint_ms`
//! optional:
//!
//! ```text
//! {"transcript": "luna what's the weather", "expected_endpoint_ms": 3120}
//! ```
//!
//! With `expected_endpoint_ms` present the margin is measured against that ground-truth
//! endpoint (`expected − last_speech`; negative means we cut the speaker off) instead of
//! against the derived window end, which is what `capture.trailing_silence_ms` tuning
//! needs. Without it the documented `utterance_bounds.end − last_speech` stands.
//!
//! AC4 runs this against the live Parakeet endpoint (the transcribe stage is a real
//! Wyoming client); everything before it stays offline. That endpoint does not exist yet —
//! see the `[parakeet]` note in `config/listenrr.toml.example` — so until a Wyoming ASR
//! service is stood up, only the stages before transcribe can be exercised against real
//! audio, and a run without a `<stem>.transcript.json` sidecar fails at the connect.

use std::future::Future;
use std::ops::Range;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::capture::{
    backward_bounds_with_preroll, EndpointMode, EndpointReason, ForwardEndpointer, SileroVad,
    VadEngine, BACKWARD_LOOKBACK_SAMPLES,
};
use crate::config::{Config, Kws};
use crate::ha::{PipelineOutcome, PipelineRequest};
use crate::kws::{KwsEngine, SampleCounters, SherpaEngine};
use crate::parakeet::{self, WyomingClient};
use crate::proto::FRAME_SAMPLES;
use crate::verify::{self, FalseAccept};
use crate::{ms_to_samples, pcm_to_f32, samples_to_ms, SAMPLE_RATE_HZ};

/// Outcome of one offline replay of a fixture WAV (plan S3.4). serde-serializable so a
/// run can be recorded in `docs/design/TEST-NOTES.md` and diffed against the previous one.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReplayReport {
    /// WAV that was replayed.
    pub wav: PathBuf,
    /// Model directory used for KWS.
    pub model: PathBuf,
    /// True when the KWS stage produced a wake at all (AC4: ≥ 9/10 positives must).
    pub woke: bool,
    /// Keyword span found, in the file's sample domain.
    pub kw_span: Option<Range<u64>>,
    /// Utterance bounds the capture stage derived (S3.1/S3.2), or `None` if it never ran.
    pub utterance_bounds: Option<Range<u64>>,
    /// Sample index one past the last detected speech, from the endpointer.
    pub last_speech: Option<u64>,
    /// **Utterance-end margin** in samples: `utterance_bounds.end - last_speech`. The
    /// truncation-rate instrument for tuning `trailing_silence_ms` (S3.4).
    pub utterance_end_margin_samples: Option<u64>,
    /// Same margin in milliseconds, for the notes table.
    pub utterance_end_margin_ms: Option<u64>,
    /// Transcript from the ASR stage.
    pub transcript: Option<String>,
    /// True when the verify gate accepted (S4.2).
    pub verify_passed: bool,
    /// Stripped text the gate would have handed HA.
    pub verified_text: Option<String>,
    /// Why the gate rejected, when it did (AC4's negative set must report a reason).
    pub verify_reject_reason: Option<String>,
    /// **HA calls the counting no-op sink saw.** AC4's negative clause requires 0 on the
    /// negative set; a positive fixture expects 1 (plan C2-1).
    pub ha_calls: u32,
    /// Pipeline runs requested (text + device_id + conversation_id captured, not sent).
    pub pipeline_requests: u32,
}

/// The `listenrr-replay` ASR fake: a transcript read from disk instead of a wire.
#[derive(Debug, Clone, Deserialize)]
struct Sidecar {
    /// The transcript the real ASR produced for this fixture.
    transcript: String,
    /// Ground-truth utterance end, ms from the start of the file. Optional: without it
    /// the margin is measured against the derived window end (the field docs of
    /// [`ReplayReport::utterance_end_margin_samples`]).
    #[serde(default)]
    expected_endpoint_ms: Option<u64>,
}

/// Replay one WAV through KWS → capture → transcribe → verify (plan S3.4).
///
/// No sockets are opened: `[ha]` is replaced by a sink that counts calls, so AC4's
/// "0 HA calls on the negative set" is measured rather than assumed. `[parakeet]` is the
/// real client (AC4 runs on the backend host against the live endpoint). Windows come from `cfg`, so
/// a tuning change is a config edit, not a code edit (plan principle 4).
///
/// `model_dir` overrides `cfg.kws.model` when the caller wants a different bundle; pass
/// `cfg.kws.model` for the normal case.
pub fn run(model_dir: &Path, wav: &Path, cfg: &Config) -> anyhow::Result<ReplayReport> {
    let pcm = read_wav(wav)?;
    let mut report = ReplayReport {
        wav: wav.to_path_buf(),
        model: model_dir.to_path_buf(),
        ..ReplayReport::default()
    };
    let source = wav
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("replay")
        .to_string();

    // ── S2: keyword spotting over the file, framed exactly like the wire ───────────
    let kws_cfg = Kws {
        model: model_dir.to_string_lossy().into_owned(),
        ..cfg.kws.clone()
    };
    let mut engine = SherpaEngine::new(&kws_cfg)?;
    let mut counters = SampleCounters::new();
    let mut kw_span: Option<Range<u64>> = None;
    'spot: for chunk in pcm.chunks(FRAME_SAMPLES) {
        engine.accept(&source, &pcm_to_f32(chunk));
        counters.note(&source, chunk.len());
        if let Some(detected) = engine.decode_ready().into_iter().next() {
            let detected_at = counters.accepted(&detected.source);
            let segment_start = counters.samples_at_reset(&detected.source).unwrap_or(0);
            let span = counters.span(
                &detected.source,
                segment_start,
                detected_at,
                detected.first_token_ts_s,
            );
            engine.reset(&detected.source);
            counters.note_reset(&detected.source);
            kw_span = Some(span);
            break 'spot; // one clip per fixture: the first wake is the clip
        }
    }
    let Some(kw_span) = kw_span else {
        // No wake: everything after this stage never ran, and the counters stay 0 — the
        // AC4 miss case, reported rather than assumed.
        tracing::info!(wav = %wav.display(), "no keyword detected");
        return Ok(report);
    };
    report.woke = true;
    report.kw_span = Some(kw_span.clone());

    // ── S3.1: backwards bounds (R-B: the window's last sample is exclusive at the
    // keyword end, and the span is in this file's absolute sample domain) ───────────
    let window_base = kw_span
        .end
        .saturating_sub(BACKWARD_LOOKBACK_SAMPLES as u64)
        .min(pcm.len() as u64);
    let recent = &pcm[window_base as usize..kw_span.end.min(pcm.len() as u64) as usize];
    let vad_model = vad_model_path(cfg, &kws_cfg.model);
    let mut backward_vad: Box<dyn VadEngine> = Box::new(SileroVad::new(&vad_model)?);
    let bounds = backward_bounds_with_preroll(
        recent,
        &kw_span,
        backward_vad.as_mut(),
        ms_to_samples(cfg.capture.preroll_ms),
    );
    report.utterance_bounds = Some(bounds.clone());

    // ── S3.2: forward endpointing from the keyword end to the end of the file ──────
    let forward_vad: Box<dyn VadEngine> = Box::new(SileroVad::new(&vad_model)?);
    let mut endpointer = ForwardEndpointer::new(
        &cfg.capture,
        forward_vad,
        EndpointMode::Wake {
            kw_span: kw_span.clone(),
        },
    );
    let mut endpoint = None;
    let tail_start = kw_span.end.min(pcm.len() as u64) as usize;
    for (index, chunk) in pcm[tail_start..].chunks(FRAME_SAMPLES).enumerate() {
        let frame_start = tail_start as u64 + (index as u64 * FRAME_SAMPLES as u64);
        if let Some(reached) = endpointer.push(frame_start, chunk) {
            endpoint = Some(reached);
            break;
        }
    }
    // The file ending is the source going away: deliver what was heard, exactly as the
    // live path does when a satellite drops mid-utterance.
    let endpoint = endpoint.or_else(|| endpointer.finish(EndpointReason::SourceGone));
    let Some(endpoint) = endpoint else {
        tracing::info!(
            wav = %wav.display(),
            kw_span = ?kw_span,
            "no speech after the keyword: nothing was captured",
        );
        return Ok(report);
    };
    let start = usize::try_from(bounds.start.min(endpoint.end)).unwrap_or(usize::MAX);
    let end = usize::try_from(endpoint.end.min(pcm.len() as u64)).unwrap_or(usize::MAX);
    report.last_speech = Some(endpoint.last_speech);
    report.utterance_bounds = Some(start as u64..end as u64);

    // ── S3.3: ASR — the sidecar fake when present, the real Wyoming client otherwise ─
    let utterance = &pcm[start.min(end)..end];
    let sidecar = read_sidecar(wav)?;
    let transcript = match &sidecar {
        Some(sidecar) => {
            tracing::info!(wav = %wav.display(), asr = "listenrr-replay sidecar", "transcript");
            sidecar.transcript.clone()
        }
        None => {
            tracing::info!(wav = %wav.display(), asr = %cfg.parakeet.addr, "transcript");
            block_on(parakeet::transcribe(
                &WyomingClient::with_fallbacks(
                    &cfg.parakeet.addr,
                    cfg.parakeet.fallback_addrs.clone(),
                ),
                utterance,
            ))?
        }
    };
    report.transcript = Some(transcript.clone());

    // ── S4.2: the verify gate ───────────────────────────────────────────────────────
    let rejected = match verify::verify_utterance(&transcript, true, &cfg.kws.wake_words) {
        Ok(text) => {
            report.verify_passed = true;
            report.verified_text = Some(text);
            false
        }
        Err(FalseAccept { transcript }) => {
            report.verify_reject_reason = Some(format!(
                "no wake token at the start of the transcript (S4.2); rejected text was {transcript:?}"
            ));
            true
        }
    };

    // ── S4.3: HA by counter, never by socket ────────────────────────────────────────
    // Only a transcript that passed the gate may reach the pipeline: the negative set's
    // zero is asserted here, not inferred from the absence of a call.
    let mut sink = HaSink::default();
    if !rejected {
        sink.run_pipeline(&PipelineRequest {
            text: report.verified_text.clone().unwrap_or_default(),
            device_id: cfg.source(&source).and_then(|s| s.device_id.clone()),
            conversation_id: Some(format!("listenrr-replay-{}", report.wav.display())),
        });
    }
    report.ha_calls = sink.calls;
    report.pipeline_requests = sink.pipeline_requests;

    // ── S3.4: the truncation instrument ─────────────────────────────────────────────
    let expected = sidecar
        .and_then(|s| s.expected_endpoint_ms)
        .map(ms_to_samples);
    let margin = utterance_end_margin(endpoint.end, endpoint.last_speech, expected);
    report.utterance_end_margin_samples = Some(margin.unsigned_abs());
    report.utterance_end_margin_ms = Some(samples_to_ms(margin.unsigned_abs()));
    Ok(report)
}

/// The utterance-end margin in samples: positive is slack (we waited too long), negative
/// is truncation (the speaker was cut off). With a ground-truth endpoint the comparison is
/// against that; otherwise against the derived window end (see the field docs).
fn utterance_end_margin(window_end: u64, last_speech: u64, expected_endpoint: Option<u64>) -> i64 {
    let reference = expected_endpoint.unwrap_or(window_end);
    i64::try_from(reference).unwrap_or(i64::MAX) - i64::try_from(last_speech).unwrap_or(i64::MAX)
}

/// HA replaced by a counter (plan C2-1): the same call the service makes, with nothing
/// behind it. A run that reaches it is recorded, never silently allowed.
#[derive(Debug, Default)]
struct HaSink {
    calls: u32,
    pipeline_requests: u32,
}

impl HaSink {
    fn run_pipeline(&mut self, _req: &PipelineRequest) -> PipelineOutcome {
        self.calls += 1;
        self.pipeline_requests += 1;
        PipelineOutcome::default()
    }
}

/// Drive the one async stage (`parakeet::transcribe`) from this synchronous instrument.
///
/// `replay` is a CLI tool called outside any runtime, so a current-thread runtime is
/// built for this single call. Calling [`run`] from inside a running runtime would panic
/// in `block_on`, and nothing in this crate does that.
fn block_on<T>(future: impl Future<Output = anyhow::Result<T>>) -> anyhow::Result<T> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("building the replay ASR runtime: {e}"))?;
    runtime.block_on(future)
}

/// Read a 16 kHz mono PCM WAV as `i16` samples.
fn read_wav(path: &Path) -> anyhow::Result<Vec<i16>> {
    let mut reader = hound::WavReader::open(path)
        .map_err(|e| anyhow::anyhow!("opening {}: {e}", path.display()))?;
    let spec = reader.spec();
    anyhow::ensure!(
        spec.sample_rate == SAMPLE_RATE_HZ,
        "{} is {} Hz; every instrument here is defined at {SAMPLE_RATE_HZ} Hz",
        path.display(),
        spec.sample_rate
    );
    anyhow::ensure!(
        spec.channels == 1,
        "{} has {} channels; replay is mono",
        path.display(),
        spec.channels
    );
    anyhow::ensure!(
        spec.sample_format == hound::SampleFormat::Int && spec.bits_per_sample == 16,
        "{} is {:?} {}-bit; replay reads 16-bit PCM",
        path.display(),
        spec.sample_format,
        spec.bits_per_sample
    );
    reader
        .samples::<i16>()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))
}

/// The `listenrr-replay` ASR fake's sidecar: `<stem>.transcript.json` next to the WAV.
fn read_sidecar(wav: &Path) -> anyhow::Result<Option<Sidecar>> {
    let path = sidecar_path(wav);
    let Some(text) = read_if_exists(&path)? else {
        return Ok(None);
    };
    let sidecar = serde_json::from_str::<Sidecar>(&text)
        .map_err(|e| anyhow::anyhow!("parsing {}: {e}", path.display()))?;
    Ok(Some(sidecar))
}

/// `<stem>.transcript.json` for `stem.wav`. A name `*.wav.json` (the clip sidecar rule in
/// `.gitignore`) does not match, so fixture ground truth is committed with the fixture.
fn sidecar_path(wav: &Path) -> PathBuf {
    let name = format!(
        "{}.transcript.json",
        wav.file_stem().and_then(|s| s.to_str()).unwrap_or("replay")
    );
    wav.with_file_name(name)
}

fn read_if_exists(path: &Path) -> anyhow::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(anyhow::anyhow!("reading {}: {e}", path.display())),
    }
}

/// The Silero VAD path: `[capture] vad_model`, else the KWS bundle actually in use
/// (contract choice 6). The caller passes the effective bundle — `--model` overrides
/// `cfg.kws.model` for KWS, and the VAD fallback must follow the same bundle, not the
/// stale configured one.
fn vad_model_path(cfg: &Config, effective_kws_model: &str) -> PathBuf {
    PathBuf::from(
        cfg.capture
            .vad_model
            .clone()
            .unwrap_or_else(|| effective_kws_model.to_owned()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report_with(margin: i64) -> ReplayReport {
        ReplayReport {
            utterance_end_margin_samples: Some(margin.unsigned_abs()),
            ..ReplayReport::default()
        }
    }

    #[test]
    fn margin_is_slack_against_the_window_end_and_truncation_against_ground_truth() {
        // No ground truth: the documented `bounds.end − last_speech` (slack, positive).
        assert_eq!(utterance_end_margin(50_000, 48_000, None), 2_000);
        // Ground truth earlier than the window end: negative is a cut-off speaker.
        assert_eq!(
            utterance_end_margin(50_000, 48_000, Some(47_500)),
            -500,
            "the fixture's expected endpoint wins over the derived one"
        );
        // The report carries the magnitude; the sign is for the operator's reading.
        assert_eq!(report_with(-500).utterance_end_margin_samples, Some(500));
    }

    #[test]
    fn sidecar_path_is_not_the_clip_sidecar_rule() {
        let path = sidecar_path(Path::new("/tmp/fixtures/hello.wav"));
        assert_eq!(path, Path::new("/tmp/fixtures/hello.transcript.json"));
        // `.gitignore` ignores `*.wav.json`; this name must survive `git add`.
        assert!(!path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .ends_with(".wav.json"));
    }

    #[test]
    fn sidecar_rejects_a_missing_transcript_and_allows_the_endpoint() {
        let ok: Sidecar =
            serde_json::from_str(r#"{"transcript": "luna what's the weather"}"#).unwrap();
        assert_eq!(ok.transcript, "luna what's the weather");
        assert_eq!(ok.expected_endpoint_ms, None);
        let with_expected: Sidecar =
            serde_json::from_str(r#"{"transcript": "x", "expected_endpoint_ms": 3120}"#).unwrap();
        assert_eq!(with_expected.expected_endpoint_ms, Some(3120));
        assert!(serde_json::from_str::<Sidecar>(r#"{"expected_endpoint_ms": 1}"#).is_err());
    }

    #[test]
    fn a_replay_that_never_wakes_reaches_no_ha() {
        // The default report is exactly the "no wake" answer: nothing ran, and the
        // counters are zero because nothing was allowed to reach them.
        let report = ReplayReport::default();
        assert!(!report.woke);
        assert_eq!(report.ha_calls, 0);
        assert_eq!(report.pipeline_requests, 0);
        let mut sink = HaSink::default();
        sink.run_pipeline(&PipelineRequest {
            text: "weather".into(),
            device_id: None,
            conversation_id: None,
        });
        assert_eq!((sink.calls, sink.pipeline_requests), (1, 1));
    }

    #[test]
    fn replay_refuses_a_wav_it_cannot_play_without_panicking() {
        let cfg = crate::config::Config::parse_toml_str(&minimal_config()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let wav = dir.path().join("silent.wav");
        std::fs::write(&wav, b"not a wav at all").unwrap();
        let err = run(Path::new("/nonexistent-model"), &wav, &cfg)
            .expect_err("a non-WAV must be an error, not a panic");
        assert!(
            err.to_string().contains("not a wav") || err.to_string().contains("opening"),
            "{err}"
        );
    }

    #[test]
    fn replay_names_the_sample_rate_it_refuses() {
        let cfg = crate::config::Config::parse_toml_str(&minimal_config()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let wav = dir.path().join("8k.wav");
        let mut writer = hound::WavWriter::create(
            &wav,
            hound::WavSpec {
                channels: 1,
                sample_rate: 8_000,
                bits_per_sample: 16,
                sample_format: hound::SampleFormat::Int,
            },
        )
        .unwrap();
        writer.write_sample(0i16).unwrap();
        writer.finalize().unwrap();
        let err = run(Path::new("/nonexistent-model"), &wav, &cfg).unwrap_err();
        assert!(err.to_string().contains("8000 Hz"), "{err}");
    }

    #[test]
    fn vad_model_follows_the_effective_bundle_not_the_stale_config() {
        // F3: `--model` replaces the bundle for the whole instrument, not just KWS.
        // The fallback resolves against the effective bundle, never `cfg.kws.model`;
        // an explicit `[capture] vad_model` still wins (contract choice 6).
        let mut cfg = crate::config::Config::parse_toml_str(&minimal_config()).unwrap();
        assert_eq!(cfg.kws.model, "/models/kws", "the configured path is the stale one");
        assert_eq!(
            vad_model_path(&cfg, "/models/override"),
            PathBuf::from("/models/override"),
            "with no explicit vad_model, the effective override is the fallback"
        );
        cfg.capture.vad_model = Some("/models/silero.pt".to_string());
        assert_eq!(
            vad_model_path(&cfg, "/models/override"),
            PathBuf::from("/models/silero.pt"),
            "an explicit vad_model beats the override"
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
}
