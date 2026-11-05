//! Capture: backwards utterance bounds, live forward endpointing, follow-up onset
//! (plan S3.1, S3.2, S6.1).
//!
//! Two passes, both on the DSP thread — never the async runtime (plan principle 3):
//!
//! 1. **Backwards** ([`backward_bounds`]) — a *fresh* Silero VAD instance over the
//!    winner's last 10 s. The utterance start is the start of the speech segment that
//!    contains the keyword span; if VAD finds no speech there, the fallback is
//!    `kw_start − 300 ms` (S3.1).
//! 2. **Forward** ([`ForwardEndpointer`]) — a live VAD fed with the winner's broadcast
//!    frames. `capture.trailing_silence_ms` (700 ms) of silence or `capture.hard_cap_s`
//!    (15 s) ends the utterance (S3.2). In follow-up mode it instead reports the first
//!    speech onset with no keyword required (S6.1); the coordinator applies
//!    `capture.min_utterance_ms`.
//!
//! The endpointer's last-speech→endpoint distance is the truncation-rate instrument
//! `replay` reports (S3.4) — keep it exact.
//!
//! Window alignment (the frozen backwards signature carries no base index, so the
//! convention is fixed here and every caller must honour it): `recent` is the audio
//! ending at the keyword — its last sample is the sample one past `kw_span.end`. A
//! caller reads exactly that with `ring.range(kw_span.end.saturating_sub(LOOKBACK)..
//! kw_span.end)`. With `ring.last(LOOKBACK)` taken after a wake the window also holds
//! the few frames that arrived after detection, which shifts every returned index by at
//! most that post-detection lag (≤ one frame) — the ring can still serve it.
//!
//! LANE-OWNED SEAM (Lane A, S3.1/S3.2/S6.1). Public signatures are frozen — implement
//! the bodies; add only private items. Report a mismatch instead of editing.

use std::fmt;
use std::ops::Range;
use std::path::Path;

use sherpa_onnx::{SileroVadModelConfig, VadModelConfig, VoiceActivityDetector};

use crate::config::Capture;
use crate::ms_to_samples;

/// Sample rate of the audio this module is defined at.
pub const SAMPLE_RATE_HZ: usize = 16_000;
/// How far back the backwards VAD pass looks (S3.1: the winner's last 10 s).
pub const BACKWARD_LOOKBACK_SECONDS: usize = 10;
/// Samples in that lookback window.
pub const BACKWARD_LOOKBACK_SAMPLES: usize = BACKWARD_LOOKBACK_SECONDS * SAMPLE_RATE_HZ;
/// The S3.1 fallback padding when VAD finds no speech: `kw_start − 300 ms`. Equals the
/// plan's `capture.preroll_ms` default; use [`backward_bounds_with_preroll`] when the
/// config sets something else.
pub const DEFAULT_PREROLL_MS: u64 = 300;

/// Silero's analysis window: 512 samples (32 ms) at 16 kHz. Both passes step the VAD one
/// window at a time, so a [`VadEngine`] verdict always describes a fixed span of audio.
const VAD_WINDOW_SAMPLES: usize = 512;
/// The same window as the sherpa-onnx config wants it.
const VAD_WINDOW_SIZE: i32 = VAD_WINDOW_SAMPLES as i32;
/// Samples fed to the VAD per [`VadEngine::process`] call from the backwards pass.
const BACKWARD_CHUNK_SAMPLES: usize = VAD_WINDOW_SAMPLES;
/// Detector result buffer, in seconds: above `capture.hard_cap_s`'s 15 s, so the
/// detector can never truncate an utterance the endpointer considers legal.
const VAD_BUFFER_SECONDS: f32 = 30.0;
/// Silero speech probability threshold (the model's documented default).
const VAD_THRESHOLD: f32 = 0.5;
/// Silence that closes a detector segment. Below `capture.trailing_silence_ms`'s 700 ms
/// default, so the caller's endpoint window dominates; long enough that the ordinary
/// word gaps inside a phrase stay in one segment for the backwards pass.
const VAD_MIN_SILENCE_S: f32 = 0.25;
/// Speech that counts as an onset; well inside `capture.preroll_ms`'s 300 ms default.
const VAD_MIN_SPEECH_S: f32 = 0.10;
/// Forced detector segment split. Above `capture.hard_cap_s`, so it never cuts a legal
/// utterance (Silero's own default is 20 s, which would sit inside a 15 s cap).
const VAD_MAX_SPEECH_S: f32 = 20.0;
/// How long a wake turn waits after the keyword for the command to *start* before it gives
/// up and ends the turn. Longer than `capture.trailing_silence_ms` (which only applies once
/// the command has started): a pause after "hey Luna" is normal, and a sentence that ended on
/// the keyword needs a deadline of its own, because the forward VAD never hears it.
pub const WAKE_COMMAND_GRACE_MS: u64 = 1500;

/// A speech/non-speech detector behind a trait, so both VAD passes can be tested with a
/// scripted engine while Silero stays swappable (plan §3: VAD on the DSP thread).
pub trait VadEngine: Send {
    /// Feed the next block; return whether speech is active after it.
    fn process(&mut self, samples: &[f32]) -> bool;
    /// Discard all state — a fresh pass must never inherit the previous one (S3.1 uses a
    /// *fresh* instance per pass; S5.1's noise-window pass needs the same isolation).
    fn reset(&mut self);
}

/// The Silero VAD from the official sherpa-onnx crate (plan §4: "Silero VAD in-process").
///
/// `#[derive(Debug)]` is not available here: [`VoiceActivityDetector`] does not implement
/// `Debug`, so the impl below reports the observable state instead.
pub struct SileroVad {
    detector: VoiceActivityDetector,
    /// Sub-window remainder. Silero only advances on whole 512-sample windows and a
    /// broadcast frame can end mid-window, so the tail must survive to the next call;
    /// dropping it would shift every later verdict.
    pending: Vec<f32>,
    /// Speech state after the last *processed* window. A call that consumes no whole
    /// window repeats the previous verdict rather than inventing silence.
    speech: bool,
}

impl fmt::Debug for SileroVad {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SileroVad")
            .field("pending_samples", &self.pending.len())
            .field("speech", &self.speech)
            .finish_non_exhaustive()
    }
}

impl SileroVad {
    /// Load a Silero VAD model. `model` is `capture.vad_model`, or the `kws.model`
    /// directory when that key is unset (S3.1).
    pub fn new(model: &Path) -> anyhow::Result<Self> {
        let config = VadModelConfig {
            silero_vad: SileroVadModelConfig {
                model: Some(model.to_string_lossy().into_owned()),
                threshold: VAD_THRESHOLD,
                min_silence_duration: VAD_MIN_SILENCE_S,
                min_speech_duration: VAD_MIN_SPEECH_S,
                window_size: VAD_WINDOW_SIZE,
                max_speech_duration: VAD_MAX_SPEECH_S,
            },
            sample_rate: SAMPLE_RATE_HZ as i32,
            num_threads: 1,
            ..VadModelConfig::default()
        };
        let detector =
            VoiceActivityDetector::create(&config, VAD_BUFFER_SECONDS).ok_or_else(|| {
                anyhow::anyhow!(
                    "sherpa-onnx could not load a Silero VAD model from {}",
                    model.display()
                )
            })?;
        Ok(Self {
            detector,
            pending: Vec::with_capacity(VAD_WINDOW_SAMPLES),
            speech: false,
        })
    }
}

impl VadEngine for SileroVad {
    fn process(&mut self, samples: &[f32]) -> bool {
        self.pending.extend_from_slice(samples);
        let mut consumed = false;
        let mut offset = 0;
        while offset + VAD_WINDOW_SAMPLES <= self.pending.len() {
            self.detector
                .accept_waveform(&self.pending[offset..offset + VAD_WINDOW_SAMPLES]);
            offset += VAD_WINDOW_SAMPLES;
            consumed = true;
        }
        if offset > 0 {
            self.pending.drain(..offset);
        }
        if consumed {
            self.speech = self.detector.detected();
        }
        self.speech
    }

    fn reset(&mut self) {
        self.detector.reset();
        self.detector.clear();
        self.pending.clear();
        self.speech = false;
    }
}

/// Result of an endpointing decision (S3.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// First sample of the utterance, absolute sample index.
    pub start: u64,
    /// One past the last accepted sample, absolute sample index.
    pub end: u64,
    /// What ended it.
    pub reason: EndpointReason,
    /// Absolute sample index of the end of the last detected speech. `end - last_speech`
    /// is the trailing margin `replay` reports (S3.4: the `trailing_silence_ms` tuning
    /// instrument).
    pub last_speech: u64,
}

/// Why an utterance ended (S3.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointReason {
    /// `trailing_silence_ms` of silence elapsed after last speech.
    TrailingSilence,
    /// `hard_cap_s` reached (15 s).
    HardCap,
    /// The source disconnected mid-utterance; deliver what exists.
    SourceGone,
}

/// What a forward pass is doing (S6.1 changes the rules, not the code path).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointMode {
    /// Wake turn: the keyword span is known and bounds the utterance start.
    Wake {
        /// Keyword span from the KWS thread.
        kw_span: Range<u64>,
    },
    /// Follow-up turn: any speech from the winner counts, no keyword required (S6.1).
    FollowUp,
}

/// A captured utterance, ready for transcription (S3.2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Utterance {
    /// Source that spoke.
    pub source: String,
    /// Session this belongs to.
    pub conversation_id: String,
    /// The captured PCM.
    pub pcm: Vec<i16>,
    /// Keyword span, `None` for a follow-up turn (S6.1).
    pub kw_span: Option<Range<u64>>,
    /// How the capture ended.
    pub endpoint: Endpoint,
}

/// `i16` → `f32` into an existing buffer, with exactly the scaling
/// [`crate::pcm_to_f32`] defines (a test pins them equal). Allocation-free because both
/// VAD passes run per chunk/frame on the DSP thread.
fn to_f32_into(dst: &mut Vec<f32>, pcm: &[i16]) {
    dst.reserve(pcm.len());
    for &sample in pcm {
        dst.push(f32::from(sample) / 32768.0);
    }
}

/// Find the utterance start by running a VAD pass backwards over the winner's recent
/// audio (plan S3.1).
///
/// `recent` is the last [`BACKWARD_LOOKBACK_SAMPLES`] samples of the source's ring
/// (`ring.last(10 * 16000)`), whose last element is the newest sample; `kw_span` is the
/// absolute keyword span in the same sample domain. The returned range is absolute.
///
/// Returns the absolute range of the speech segment containing `kw_span`; when VAD finds
/// no speech around the keyword, the documented fallback `kw_start − 300 ms`
/// ([`DEFAULT_PREROLL_MS`]) is returned instead (S3.1).
pub fn backward_bounds(
    recent: &[i16],
    kw_span: &Range<u64>,
    vad: &mut dyn VadEngine,
) -> Range<u64> {
    backward_bounds_with_preroll(recent, kw_span, vad, ms_to_samples(DEFAULT_PREROLL_MS))
}

/// [`backward_bounds`] with an explicit preroll, for a non-default
/// `capture.preroll_ms`; the S3.1 fallback keeps the same shape.
pub fn backward_bounds_with_preroll(
    recent: &[i16],
    kw_span: &Range<u64>,
    vad: &mut dyn VadEngine,
    preroll_samples: u64,
) -> Range<u64> {
    if recent.is_empty() {
        // Nothing was analysed, so there is no window to clamp into: answer with the
        // documented fallback rather than the empty range at 0.
        let start = kw_span.start.saturating_sub(preroll_samples);
        return start..kw_span.end.max(start);
    }

    // The window's last sample is the sample one past the keyword (see the module docs).
    let base = kw_span.end.saturating_sub(recent.len() as u64);
    let window_end = base + recent.len() as u64;

    // S3.1 fallback, clamped into the audio we were actually given: the caller cannot
    // expect the pass to reach behind the window it supplied.
    let fallback = || {
        let start = kw_span
            .start
            .saturating_sub(preroll_samples)
            .max(base)
            .min(window_end);
        let end = kw_span.end.max(start).min(window_end);
        start..end
    };

    // A fresh pass never inherits the previous utterance's detector state (S3.1).
    vad.reset();

    // Speech segments as runs of consecutive speech chunks, window-relative.
    let mut segments: Vec<(u64, u64)> = Vec::new();
    let mut open: Option<u64> = None;
    let mut scratch: Vec<f32> = Vec::with_capacity(BACKWARD_CHUNK_SAMPLES);
    let mut idx = 0usize;
    while idx < recent.len() {
        let end = (idx + BACKWARD_CHUNK_SAMPLES).min(recent.len());
        scratch.clear();
        to_f32_into(&mut scratch, &recent[idx..end]);
        if vad.process(&scratch) {
            open.get_or_insert(idx as u64);
        } else if let Some(start) = open.take() {
            segments.push((start, idx as u64));
        }
        idx = end;
    }
    if let Some(start) = open {
        // The window ends mid-segment: it still is the segment the keyword may sit in.
        segments.push((start, recent.len() as u64));
    }

    let kw = kw_span.start.saturating_sub(base);
    let chosen = segments
        .iter()
        .find(|(start, end)| kw >= *start && kw < *end)
        // The keyword landed in a gap (chunk quantisation, or VAD hysteresis): the
        // segment that speaks last before it is the closest onset.
        .or_else(|| segments.iter().rev().find(|(start, _)| kw >= *start))
        // Otherwise the first segment that follows the keyword.
        .or_else(|| segments.first());

    match chosen {
        Some(&(start, end)) => {
            let start = (base + start).clamp(base, window_end);
            let end = (base + end).clamp(start, window_end);
            start..end
        }
        None => fallback(),
    }
}

/// Live forward endpointer for one in-flight utterance (plan S3.2, S6.1).
///
/// Fed with the winner's broadcast frames; owns the live VAD instance; driven by the
/// coordinator from the DSP thread. `Wake` mode requires the keyword span; `FollowUp`
/// mode reports the first speech onset with no keyword (S6.1).
pub struct ForwardEndpointer {
    cfg: Capture,
    vad: Box<dyn VadEngine>,
    mode: EndpointMode,
    // Lane A keeps last-speech/started state here — never wall-clock time (plan §3).
    /// `capture.preroll_ms` in samples.
    preroll_samples: u64,
    /// `capture.trailing_silence_ms` in samples; the endpoint fires strictly past it.
    trailing_silence_samples: u64,
    /// `capture.hard_cap_s` in samples.
    hard_cap_samples: u64,
    /// [`WAKE_COMMAND_GRACE_MS`] in samples.
    command_grace_samples: u64,
    /// Absolute first sample of the utterance once anchored: the keyword anchor on a
    /// wake turn, onset − preroll on a follow-up (S6.1).
    capture_start: Option<u64>,
    /// Absolute capture start, set when speech begins — the follow-up onset test (S6.1).
    speech_start: Option<u64>,
    /// Absolute index of the first speech sample (pre-preroll), for the S6.2 length check.
    onset: Option<u64>,
    /// Absolute index one past the last speech sample.
    last_speech: Option<u64>,
    /// Silence accumulated since the last speech, in samples.
    silence_run: u64,
    /// Absolute index one past the newest pushed sample.
    next_index: Option<u64>,
    /// The endpoint was emitted; further pushes are no-ops.
    done: bool,
    /// Frame-sized `i16` → `f32` scratch: this path runs every frame, so it never
    /// allocates after the first push.
    scratch: Vec<f32>,
}

impl ForwardEndpointer {
    /// A fresh endpointer for one utterance, using `cfg`'s windows and its own VAD.
    pub fn new(cfg: &Capture, vad: Box<dyn VadEngine>, mode: EndpointMode) -> Self {
        let preroll_samples = ms_to_samples(cfg.preroll_ms);
        let capture_start = match &mode {
            EndpointMode::Wake { kw_span } => Some(kw_span.start.saturating_sub(preroll_samples)),
            // Follow-up has no anchor until the first speech (S6.1).
            EndpointMode::FollowUp => None,
        };
        Self {
            preroll_samples,
            trailing_silence_samples: ms_to_samples(cfg.trailing_silence_ms),
            hard_cap_samples: cfg.hard_cap_s * SAMPLE_RATE_HZ as u64,
            cfg: cfg.clone(),
            vad,
            mode,
            capture_start,
            speech_start: None,
            onset: None,
            last_speech: None,
            silence_run: 0,
            next_index: None,
            command_grace_samples: ms_to_samples(WAKE_COMMAND_GRACE_MS),
            done: false,
            scratch: Vec::with_capacity(VAD_WINDOW_SAMPLES * 4),
        }
    }

    /// Feed one frame whose first sample is at absolute index `frame_start`.
    ///
    /// Returns the completed [`Endpoint`] exactly once, when trailing silence or the hard
    /// cap ends the utterance.
    pub fn push(&mut self, frame_start: u64, samples: &[i16]) -> Option<Endpoint> {
        if self.done || samples.is_empty() {
            return None;
        }
        let frame_end = frame_start.saturating_add(samples.len() as u64);
        self.next_index = Some(frame_end);

        self.scratch.clear();
        to_f32_into(&mut self.scratch, samples);
        let speech = self.vad.process(&self.scratch);

        if speech {
            self.last_speech = Some(frame_end);
            self.silence_run = 0;
            if self.onset.is_none() {
                self.onset = Some(frame_start);
            }
            if self.speech_start.is_none() {
                let onset_start = frame_start.saturating_sub(self.preroll_samples);
                // A wake turn keeps its keyword anchor; a follow-up pads the onset.
                let start = self.capture_start.unwrap_or(onset_start);
                self.speech_start = Some(start);
                if self.capture_start.is_none() {
                    self.capture_start = Some(start);
                }
            }
        } else if self.speech_start.is_some() {
            self.silence_run = self.silence_run.saturating_add(samples.len() as u64);
        }

        // The 15 s cap is absolute: it ends the utterance whenever it is reached.
        if let Some(start) = self.capture_start {
            if frame_end.saturating_sub(start) >= self.hard_cap_samples {
                return Some(self.emit(frame_end, EndpointReason::HardCap));
            }
        }

        // A wake turn that hears nothing *after* the keyword. Without this the only way out
        // was the 15 s hard cap, because trailing silence is only counted once speech has
        // been seen. That is exactly what a command spoken before the keyword ("what's the
        // weather, Luna?") or a bare "hey Luna" looks like to the forward VAD, and it made
        // both wait for the cap. The grace is longer than `trailing_silence_ms` on purpose:
        // someone who says "hey Luna" and then thinks for a second has not finished, whereas
        // someone whose sentence ended on the keyword has.
        let kw_end = match &self.mode {
            EndpointMode::Wake { kw_span } => Some(kw_span.end),
            EndpointMode::FollowUp => None,
        };
        if let Some(kw_end) = kw_end {
            if self.speech_start.is_none()
                && frame_end.saturating_sub(kw_end) > self.command_grace_samples
            {
                self.last_speech.get_or_insert(kw_end);
                return Some(self.emit(frame_end, EndpointReason::TrailingSilence));
            }
        }

        // Strictly past the threshold: a pause that merely equals
        // `trailing_silence_ms` is not yet an endpoint, so the frame that fires is the
        // one after the window elapsed (S3.2, and the margin `replay` measures).
        if self.speech_start.is_some() && self.silence_run > self.trailing_silence_samples {
            if self.too_short() {
                // S6.2: a blip is not a follow-up. Drop it and keep listening.
                self.restart();
                return None;
            }
            return Some(self.emit(frame_end, EndpointReason::TrailingSilence));
        }

        None
    }

    /// True once speech has started — the follow-up onset test (S6.1) and the optimistic
    /// LED's cue.
    pub fn speech_started(&self) -> bool {
        self.speech_start.is_some()
    }

    /// Absolute sample index of the first accepted speech, once started.
    pub fn speech_start(&self) -> Option<u64> {
        self.speech_start
    }

    /// Force an endpoint now (source disconnected, or the hard cap with no frame in
    /// flight).
    pub fn finish(&mut self, reason: EndpointReason) -> Option<Endpoint> {
        if self.done {
            return None;
        }
        // No frames were fed, or a follow-up never heard speech: nothing to deliver.
        let end = self.next_index?;
        self.capture_start?;
        Some(self.emit(end, reason))
    }

    /// Close the utterance: the one place `done` is set, so `push`/`finish` can never
    /// both report the same capture.
    fn emit(&mut self, end: u64, reason: EndpointReason) -> Endpoint {
        self.done = true;
        let start = self.capture_start.unwrap_or(end).min(end);
        let last_speech = self.last_speech.unwrap_or(start).clamp(start, end);
        Endpoint {
            start,
            end,
            reason,
            last_speech,
        }
    }

    /// S6.2: is the speech heard too short to be a follow-up utterance? 0 disables the
    /// check; the hard cap and `finish` never ask.
    fn too_short(&self) -> bool {
        let min = ms_to_samples(self.cfg.min_utterance_ms);
        if min == 0 {
            return false;
        }
        let Some(onset) = self.onset else {
            return false;
        };
        // Length of the *speech*, not of the capture: the trailing window and the
        // preroll are both padding and must not launder a blip into an utterance.
        let spoken = self.last_speech.unwrap_or(onset).saturating_sub(onset);
        spoken < min
    }

    /// Discard a rejected capture and listen again from the same anchor.
    fn restart(&mut self) {
        self.vad.reset();
        self.speech_start = None;
        self.onset = None;
        self.last_speech = None;
        self.silence_run = 0;
        self.capture_start = match &self.mode {
            EndpointMode::Wake { kw_span } => {
                Some(kw_span.start.saturating_sub(self.preroll_samples))
            }
            EndpointMode::FollowUp => None,
        };
    }
}

/// Samples in `ms`, for callers that prefer the conversion spelled out locally.
pub fn samples_for_ms(ms: u64) -> u64 {
    ms_to_samples(ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pcm_to_f32;

    /// Scripted VAD: one verdict per `process` call, repeating the last one once the
    /// script runs out. No model, no file, no network — the tests are fully offline.
    #[derive(Debug)]
    struct FakeVad {
        script: Vec<bool>,
        next: usize,
        processes: usize,
        resets: usize,
    }

    impl FakeVad {
        fn new(script: Vec<bool>) -> Self {
            assert!(
                !script.is_empty(),
                "a scripted VAD needs at least one verdict"
            );
            Self {
                script,
                next: 0,
                processes: 0,
                resets: 0,
            }
        }
    }

    impl VadEngine for FakeVad {
        fn process(&mut self, _samples: &[f32]) -> bool {
            self.processes += 1;
            let idx = self.next.min(self.script.len() - 1);
            self.next += 1;
            self.script[idx]
        }

        fn reset(&mut self) {
            self.resets += 1;
        }
    }

    /// Silent PCM of `n` samples: the scripted VAD ignores the content, only the length
    /// matters for the sample arithmetic under test.
    fn pcm(n: usize) -> Vec<i16> {
        vec![0; n]
    }

    fn speech_chunks(pattern: &[(bool, usize)]) -> Vec<bool> {
        let mut script = Vec::new();
        for &(verdict, count) in pattern {
            script.extend(std::iter::repeat_n(verdict, count));
        }
        script
    }

    #[test]
    fn backward_bounds_take_the_segment_that_contains_the_span() {
        // 8 Silero windows: silence, silence, 4 speech windows, silence, silence.
        let mut vad = FakeVad::new(speech_chunks(&[(false, 2), (true, 4), (false, 2)]));
        let recent = pcm(8 * VAD_WINDOW_SAMPLES);
        let kw_span = 1_500u64..1_600;

        let bounds = backward_bounds(&recent, &kw_span, &mut vad);

        assert_eq!(bounds, 1_024..3_072);
        assert_eq!(vad.resets, 1, "S3.1: one fresh pass, exactly one reset");
        assert_eq!(vad.processes, 8, "the whole window is analysed");
    }

    #[test]
    fn backward_bounds_pick_the_segment_holding_the_span_not_the_first_one() {
        // Segment A = windows 0..=1, gap at 2, segment B = windows 3..=4.
        let mut vad = FakeVad::new(speech_chunks(&[
            (true, 2),
            (false, 1),
            (true, 2),
            (false, 3),
        ]));
        let recent = pcm(8 * VAD_WINDOW_SAMPLES);
        let kw_span = 2_000u64..2_100; // inside B (1536..2560)

        assert_eq!(
            backward_bounds(&recent, &kw_span, &mut vad),
            1_536..2_560,
            "the onset is segment B's start, not segment A's"
        );
    }

    #[test]
    fn no_speech_falls_back_to_exactly_start_minus_preroll() {
        let mut vad = FakeVad::new(vec![false]);
        let recent = pcm(16_000);
        let kw_span = 8_000u64..9_000;

        // DEFAULT_PREROLL_MS = 300 ms = 4_800 samples.
        assert_eq!(backward_bounds(&recent, &kw_span, &mut vad), 3_200..9_000);
        // An explicit preroll keeps the same shape (capture.preroll_ms != 300).
        assert_eq!(
            backward_bounds_with_preroll(&recent, &kw_span, &mut vad, 0),
            8_000..9_000,
            "a zero preroll falls back to the keyword start"
        );
        assert_eq!(
            backward_bounds_with_preroll(&recent, &kw_span, &mut vad, 1_600),
            6_400..9_000
        );
    }

    #[test]
    fn the_fallback_saturates_at_the_window_start() {
        let mut vad = FakeVad::new(vec![false]);
        let recent = pcm(16_000);
        let kw_span = 100u64..200;

        assert_eq!(
            backward_bounds(&recent, &kw_span, &mut vad),
            0..200,
            "kw_start - 300 ms cannot go negative"
        );
    }

    #[test]
    fn bounds_are_absolute_for_a_window_that_starts_past_zero() {
        let mut vad = FakeVad::new(speech_chunks(&[(false, 2), (true, 4), (false, 2)]));
        let recent = pcm(8 * VAD_WINDOW_SAMPLES);
        // The window ends one past kw_span.end, so its first sample is this base.
        let kw_span = 497_500u64..497_600;
        let base = kw_span.end - recent.len() as u64;

        let bounds = backward_bounds(&recent, &kw_span, &mut vad);

        assert_eq!(bounds, base + 1_024..base + 3_072);
        assert!(
            bounds.start > recent.len() as u64,
            "absolute, not window-relative"
        );
    }

    #[test]
    fn an_empty_window_answers_with_the_fallback_and_runs_no_pass() {
        let mut vad = FakeVad::new(vec![false]);

        assert_eq!(
            backward_bounds(&[], &(8_000u64..9_000), &mut vad),
            3_200..9_000
        );
        assert_eq!(vad.processes, 0);
    }

    #[test]
    fn to_f32_into_matches_the_contract_conversion() {
        let samples: Vec<i16> = vec![0, 1, -1, i16::MAX, i16::MIN, 1_234, -32_767];
        let mut out = vec![0.0f32]; // proves the buffer is appended to, not replaced

        to_f32_into(&mut out, &samples);

        assert_eq!(out[0], 0.0);
        assert_eq!(&out[1..], pcm_to_f32(&samples).as_slice());
    }

    #[test]
    fn trailing_silence_fires_one_frame_past_the_threshold() {
        // trailing_silence_ms = 700 = 11_200 samples
        let cfg = Capture {
            preroll_ms: 0,
            ..Default::default()
        };
        let frame = 1_120usize; // 70 ms
        let mut script = vec![true];
        script.extend(std::iter::repeat_n(false, 40));
        let mut ep = ForwardEndpointer::new(
            &cfg,
            Box::new(FakeVad::new(script)),
            EndpointMode::Wake { kw_span: 0..1 },
        );
        let audio = pcm(frame);

        assert!(ep.push(0, &audio).is_none(), "speech does not end anything");

        for k in 1..=10u64 {
            assert!(
                ep.push(k * frame as u64, &audio).is_none(),
                "frame {k} leaves {} samples of silence, not yet past the 11_200 threshold",
                k * frame as u64
            );
        }

        let endpoint = ep
            .push(11 * frame as u64, &audio)
            .expect("the first frame past the threshold ends the utterance");
        assert_eq!(endpoint.reason, EndpointReason::TrailingSilence);
        assert_eq!(endpoint.start, 0);
        assert_eq!(endpoint.last_speech, frame as u64);
        assert_eq!(endpoint.end, 12 * frame as u64);
        assert_eq!(
            endpoint.end - endpoint.last_speech,
            11 * frame as u64,
            "S3.4's margin is the silence that was actually accumulated"
        );
        assert!(
            ep.push(12 * frame as u64, &audio).is_none(),
            "one endpoint per utterance"
        );
    }

    #[test]
    fn a_pause_shorter_than_the_threshold_does_not_end_the_utterance() {
        let cfg = Capture {
            preroll_ms: 0,
            ..Default::default()
        };
        let frame = 1_120usize;
        let mut script = vec![true];
        script.extend(std::iter::repeat_n(false, 9)); // 630 ms < 700 ms
        script.push(true);
        script.extend(std::iter::repeat_n(false, 11));
        let mut ep = ForwardEndpointer::new(
            &cfg,
            Box::new(FakeVad::new(script)),
            EndpointMode::Wake { kw_span: 0..1 },
        );
        let audio = pcm(frame);

        assert!(ep.push(0, &audio).is_none());
        for k in 1..=9u64 {
            assert!(
                ep.push(k * frame as u64, &audio).is_none(),
                "a 630 ms pause is not an endpoint (frame {k})"
            );
        }
        assert!(
            ep.push(10 * frame as u64, &audio).is_none(),
            "speech resumes and restarts the silence count"
        );
        for k in 11..=20u64 {
            assert!(ep.push(k * frame as u64, &audio).is_none());
        }
        let endpoint = ep
            .push(21 * frame as u64, &audio)
            .expect("past the threshold after the second speech run");
        assert_eq!(endpoint.last_speech, 11 * frame as u64);
        assert_eq!(endpoint.end - endpoint.last_speech, 11 * frame as u64);
    }

    /// "What's the weather, Luna?" and a bare "hey Luna" both look like silence to the forward
    /// VAD, which starts reading after the keyword. Trailing silence is only counted once
    /// speech has been heard, so both used to run to the 15 s cap.
    #[test]
    fn a_wake_turn_with_nothing_after_the_keyword_ends_after_the_grace_not_the_cap() {
        let cfg = Capture {
            preroll_ms: 0,
            ..Default::default()
        };
        let frame = 1_120usize;
        let kw = 1_000u64..2_000;
        let mut ep = ForwardEndpointer::new(
            &cfg,
            Box::new(FakeVad::new(vec![false])),
            EndpointMode::Wake {
                kw_span: kw.clone(),
            },
        );
        let audio = pcm(frame);
        let grace = ms_to_samples(WAKE_COMMAND_GRACE_MS);

        let mut cursor = kw.end;
        let endpoint = loop {
            if let Some(endpoint) = ep.push(cursor, &audio) {
                break endpoint;
            }
            cursor += frame as u64;
            assert!(cursor < kw.end + 4 * grace, "never ended: ran toward the cap");
        };

        assert_eq!(endpoint.reason, EndpointReason::TrailingSilence);
        let waited = endpoint.end - kw.end;
        assert!(
            waited > grace && waited <= grace + frame as u64,
            "ended the first frame past the grace, not before or long after: waited {waited}"
        );
        assert_eq!(
            endpoint.last_speech, kw.end,
            "the keyword is the last speech there was, so the margin is the real silence"
        );
    }

    /// The other half of the same rule: someone who says "hey Luna" and then thinks for a
    /// second has not finished, so speech that starts inside the grace takes over from it.
    #[test]
    fn speech_inside_the_grace_cancels_it_and_normal_trailing_silence_applies() {
        let cfg = Capture {
            preroll_ms: 0,
            ..Default::default()
        };
        let frame = 1_120usize;
        let kw = 1_000u64..2_000;
        let grace = ms_to_samples(WAKE_COMMAND_GRACE_MS);
        // 10 frames of silence (11_200 samples, inside the grace), then speech that runs well
        // past where the grace would have fired, then silence.
        let mut script = vec![false; 10];
        script.extend(vec![true; 30]); // 33_600 samples of speech
        script.push(false);
        let mut ep = ForwardEndpointer::new(
            &cfg,
            Box::new(FakeVad::new(script)),
            EndpointMode::Wake {
                kw_span: kw.clone(),
            },
        );
        let audio = pcm(frame);

        let mut cursor = kw.end;
        let endpoint = loop {
            if let Some(endpoint) = ep.push(cursor, &audio) {
                break endpoint;
            }
            cursor += frame as u64;
            assert!(cursor < kw.end + 8 * grace, "never ended");
        };

        assert_eq!(endpoint.reason, EndpointReason::TrailingSilence);
        assert!(
            endpoint.end - kw.end > grace + 20 * frame as u64,
            "the grace must not have cut the command off: ended {} after the keyword",
            endpoint.end - kw.end
        );
        assert!(
            endpoint.last_speech > kw.end + 30 * frame as u64,
            "last_speech is the end of the command, not the keyword"
        );
        assert_eq!(
            endpoint.end - endpoint.last_speech,
            11 * frame as u64,
            "and the 700 ms trailing silence ended it, as for any other utterance"
        );
    }

    #[test]
    fn the_hard_cap_ends_a_long_utterance() {
        // hard_cap_s = 15 -> 240_000 samples
        let cfg = Capture {
            preroll_ms: 0,
            ..Default::default()
        };
        let mut ep = ForwardEndpointer::new(
            &cfg,
            Box::new(FakeVad::new(vec![true])),
            EndpointMode::Wake { kw_span: 0..1 },
        );
        let frame = 1_024usize;
        let audio = pcm(frame);

        for k in 0..234u64 {
            assert!(
                ep.push(k * frame as u64, &audio).is_none(),
                "no endpoint before 15 s (frame {k}, {} samples)",
                (k + 1) * frame as u64
            );
        }

        let endpoint = ep
            .push(234 * frame as u64, &audio)
            .expect("the frame that reaches 15 s ends the utterance");
        assert_eq!(endpoint.reason, EndpointReason::HardCap);
        assert_eq!(endpoint.start, 0);
        assert_eq!(endpoint.end, 235 * frame as u64);
        assert_eq!(endpoint.last_speech, endpoint.end);
        assert!(ep.push(235 * frame as u64, &audio).is_none());
    }

    #[test]
    fn follow_up_onset_starts_capture_a_preroll_before_the_speech() {
        let cfg = Capture::default(); // preroll_ms = 300 = 4_800 samples
        let frame = 1_024usize;
        let first_speech = 103_072u64;
        let mut script = vec![false, false, false];
        script.extend(std::iter::repeat_n(true, 4));
        script.extend(std::iter::repeat_n(false, 40));
        // `EndpointMode::FollowUp` carries no keyword span at all: a follow-up turn
        // cannot be bounded by one (S6.1).
        let mut ep =
            ForwardEndpointer::new(&cfg, Box::new(FakeVad::new(script)), EndpointMode::FollowUp);
        let audio = pcm(frame);

        for k in 0..3u64 {
            assert!(ep.push(100_000 + k * frame as u64, &audio).is_none());
            assert!(!ep.speech_started(), "silence is not an onset (frame {k})");
        }
        assert_eq!(ep.speech_start(), None);

        assert!(
            ep.push(first_speech, &audio).is_none(),
            "the first speech frame starts the utterance, it does not end it"
        );
        assert!(ep.speech_started());
        assert_eq!(
            ep.speech_start(),
            Some(first_speech - 4_800),
            "the onset is padded by capture.preroll_ms"
        );

        for k in 1..4u64 {
            assert!(ep.push(first_speech + k * frame as u64, &audio).is_none());
        }
        let mut endpoint = None;
        let mut k = 4u64;
        while endpoint.is_none() && k < 40 {
            endpoint = ep.push(first_speech + k * frame as u64, &audio);
            k += 1;
        }
        let endpoint = endpoint.expect("trailing silence ends the follow-up");
        assert_eq!(endpoint.reason, EndpointReason::TrailingSilence);
        assert_eq!(endpoint.start, first_speech - 4_800);
        assert!(endpoint.last_speech >= first_speech);
    }

    #[test]
    fn min_utterance_rejects_a_short_follow_up_and_keeps_listening() {
        let cfg = Capture {
            preroll_ms: 0,
            min_utterance_ms: 500, // 8_000 samples of speech minimum
            ..Default::default()
        };
        let frame = 1_120usize;
        // One scripted verdict per `process` call, in the exact order the frames below
        // are pushed: a 70 ms blip, the 11 silent frames that pass the 700 ms threshold
        // (and must be rejected), 10 frames of real speech, then its trailing silence.
        let mut script = vec![true];
        script.extend(std::iter::repeat_n(false, 11));
        script.extend(std::iter::repeat_n(true, 10));
        script.extend(std::iter::repeat_n(false, 20));
        let mut ep =
            ForwardEndpointer::new(&cfg, Box::new(FakeVad::new(script)), EndpointMode::FollowUp);
        let audio = pcm(frame);

        let mut cursor = 0u64;
        assert!(ep.push(cursor, &audio).is_none());
        cursor += frame as u64;
        for _ in 0..11 {
            assert!(
                ep.push(cursor, &audio).is_none(),
                "min_utterance_ms rejects the blip instead of ending the utterance"
            );
            cursor += frame as u64;
        }
        assert!(
            !ep.speech_started(),
            "the rejected capture is reset before listening again"
        );
        assert_eq!(ep.speech_start(), None);

        // The next utterance is long enough and is accepted.
        let second_onset = cursor;
        for _ in 0..10 {
            assert!(ep.push(cursor, &audio).is_none());
            cursor += frame as u64;
        }
        let mut endpoint = None;
        let mut fired = 0;
        while endpoint.is_none() && fired < 20 {
            endpoint = ep.push(cursor, &audio);
            cursor += frame as u64;
            fired += 1;
        }
        let endpoint = endpoint.expect("a long enough follow-up is accepted");
        assert_eq!(endpoint.reason, EndpointReason::TrailingSilence);
        assert_eq!(endpoint.start, second_onset, "preroll is 0 in this config");
    }

    #[test]
    fn min_utterance_zero_accepts_the_same_short_utterance() {
        let cfg = Capture {
            preroll_ms: 0,
            ..Default::default()
        };
        assert_eq!(cfg.min_utterance_ms, 0, "the plan's default disables S6.2");
        let frame = 1_120usize;
        let mut script = vec![true];
        script.extend(std::iter::repeat_n(false, 20));
        let mut ep =
            ForwardEndpointer::new(&cfg, Box::new(FakeVad::new(script)), EndpointMode::FollowUp);
        let audio = pcm(frame);

        assert!(ep.push(0, &audio).is_none());
        let mut endpoint = None;
        let mut k = 1u64;
        while endpoint.is_none() && k < 20 {
            endpoint = ep.push(k * frame as u64, &audio);
            k += 1;
        }
        assert!(
            endpoint.is_some(),
            "with the check disabled every spoken follow-up ends normally"
        );
    }

    #[test]
    fn finish_delivers_what_exists_exactly_once() {
        let cfg = Capture {
            preroll_ms: 0,
            ..Default::default()
        };
        let mut ep = ForwardEndpointer::new(
            &cfg,
            Box::new(FakeVad::new(vec![true])),
            EndpointMode::Wake {
                kw_span: 1_000..1_100,
            },
        );
        let audio = pcm(1_024);

        assert!(
            ep.finish(EndpointReason::SourceGone).is_none(),
            "no frames were fed, so there is nothing to deliver"
        );

        assert!(ep.push(1_000, &audio).is_none());
        let endpoint = ep
            .finish(EndpointReason::SourceGone)
            .expect("a disconnected source still delivers its audio");
        assert_eq!(endpoint.reason, EndpointReason::SourceGone);
        assert_eq!(endpoint.start, 1_000);
        assert_eq!(endpoint.end, 2_024);
        assert_eq!(endpoint.last_speech, 2_024);
        assert!(ep.finish(EndpointReason::SourceGone).is_none());
        assert!(ep.push(2_024, &audio).is_none());
    }

    #[test]
    fn a_follow_up_with_no_onset_delivers_nothing() {
        let cfg = Capture::default();
        let mut ep = ForwardEndpointer::new(
            &cfg,
            Box::new(FakeVad::new(vec![false])),
            EndpointMode::FollowUp,
        );
        let audio = pcm(1_024);

        for k in 0..5u64 {
            assert!(ep.push(k * 1_024, &audio).is_none());
        }
        assert!(!ep.speech_started());
        assert!(ep.finish(EndpointReason::SourceGone).is_none());
    }

    #[test]
    fn an_empty_frame_is_a_noop() {
        let cfg = Capture::default();
        let mut ep = ForwardEndpointer::new(
            &cfg,
            Box::new(FakeVad::new(vec![false])),
            EndpointMode::FollowUp,
        );

        assert!(ep.push(0, &[]).is_none());
        assert!(!ep.speech_started());
        assert!(ep.finish(EndpointReason::SourceGone).is_none());
    }
}
