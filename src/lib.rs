//! `listenrr` — the Luna voice backend (plan §4 module layout).
//!
//! The crate is a single binary (`src/main.rs`) plus this library root so the offline
//! instruments (`replay`, `bench-kws`), unit tests, and the `tests/` integration
//! fixtures (S7/AC10's scripted source client) all compile against one module graph.
//!
//! Contract layer — frozen, lanes MUST NOT edit (see `docs/design/CONTRACT-NOTES.md`):
//! [`proto`], [`ring`], [`config`].
//!
//! Lane-owned seams — public signatures are frozen; the bodies landed with their lanes:
//! [`ingest`], [`kws`], [`coordinator`], [`capture`], [`verify`], [`parakeet`], [`ha`],
//! [`clips`], [`replay`].
//!
//! Invariants that every module must respect (plan §3):
//! - A sample index is a position in the ring's write-cursor domain —
//!   `SourceRing::written()`, `docs/design/CONTRACT-NOTES.md` §6 rows 14–15. A frame's `seq` counts
//!   gaps, it is never a position.
//! - Sample-index `u64` arithmetic for all buffer/span math; [`std::time::Instant`] only
//!   for cross-source arbitration and latency logs.
//! - ingest never blocks on DSP or network calls: KWS runs on its own thread
//!   ([`kws::run_kws_thread`]), Parakeet/HA on async tasks.
//! - The coordinator owns all session state ([`coordinator::Coordinator`]); everything
//!   else is stateless per message.
//! - Wake-initiated text reaches Home Assistant only after [`verify`] passes.

pub mod capture;
pub mod clips;
pub mod config;
pub mod coordinator;
pub mod ha;
pub mod ingest;
pub mod kws;
pub mod parakeet;
pub mod proto;
pub mod replay;
pub mod ring;
pub mod verify;

/// Sample rate everything in this crate is defined against (16 kHz mono).
pub const SAMPLE_RATE_HZ: u32 = 16_000;

/// Convert a duration in milliseconds to a sample count at [`SAMPLE_RATE_HZ`].
pub const fn ms_to_samples(ms: u64) -> u64 {
    ms * (SAMPLE_RATE_HZ as u64 / 1000)
}

/// Convert a sample count to whole milliseconds.
pub const fn samples_to_ms(samples: u64) -> u64 {
    samples / (SAMPLE_RATE_HZ as u64 / 1000)
}

/// Convert a timestamp in seconds (sherpa-onnx `KeywordResult.timestamps`) to a sample
/// offset. Used only as the refinement input gated by the last-3-s rule (S2.3, U3).
/// Not `const`: `f64::round` is not a const fn.
pub fn seconds_to_samples(seconds: f32) -> i64 {
    (f64::from(seconds) * SAMPLE_RATE_HZ as f64).round() as i64
}

/// Linear `i16` → `f32` conversion in `[-1, 1]`, as sherpa-onnx's `accept_waveform`
/// and Silero VAD expect.
pub fn pcm_to_f32(samples: &[i16]) -> Vec<f32> {
    samples.iter().map(|&s| f32::from(s) / 32768.0).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sample_time_conversions_round_trip_on_frame_boundaries() {
        assert_eq!(ms_to_samples(64), 1024);
        assert_eq!(ms_to_samples(250), 4000);
        assert_eq!(ms_to_samples(700), 11200);
        assert_eq!(ms_to_samples(300), 4800);
        assert_eq!(samples_to_ms(1024), 64);
        assert_eq!(samples_to_ms(11200), 700);
        assert_eq!(seconds_to_samples(0.064), 1024);
        assert_eq!(seconds_to_samples(3.0), 48000);
        assert_eq!(seconds_to_samples(-0.5), -8000);
        assert_eq!(SAMPLE_RATE_HZ, 16_000);
    }

    #[test]
    fn pcm_conversion_is_scaled_and_lossless_at_zero() {
        let out = pcm_to_f32(&[0, i16::MAX, i16::MIN, 32767, -32768]);
        assert_eq!(out[0], 0.0);
        assert!((out[1] - i16::MAX as f32 / 32768.0).abs() < 1e-6);
        assert_eq!(out[2], -1.0);
        assert!(out[1] < 1.0, "never reaches +1.0");
        assert_eq!(pcm_to_f32(&[]).len(), 0);
    }
}
