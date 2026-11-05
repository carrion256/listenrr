//! Clip evidence and retention: "evidence from day one" (plan S8.1, S8.2, principle 4).
//!
//! Every trigger leaves a WAV plus a sidecar JSON, so tuning is data-driven and a
//! rejected trigger can be replayed (S8's verify: a false-accept clip round-trips through
//! `replay`). Household audio lives only under `clips.dir` on the backend host (default) or another trusted host,
//! with 30-day retention (plan §3 invariant, AC9).
//!
//! Layout under `clips.dir`:
//! - per-trigger clips (kw_span ± 1 s) written by [`write_trigger_clip`],
//! - `false_accept/` for verify-gate rejections (S4.2),
//! - `missed/` for "that was for you" dumps written by [`save_buffer_dump`] (S8.2, AC11).
//!
//! A minimal wav-dump path is needed from S2 (to record fixture clips via the satellite's
//! `save_buffer` event) and generalized here in S8 — implement [`save_buffer_dump`] first
//! if the fixture battery needs it earlier (plan S2's note).
//!
//! LANE-OWNED SEAM (Lane A, S8.1/S8.2 + S2's wav dump). Public signatures are frozen —
//! implement the bodies; add only private items. Report a mismatch instead of editing.

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::ring::{with_ring, RingHandle};

/// Subdirectory for verify-gate rejections (plan S4.2).
pub const FALSE_ACCEPT_DIR: &str = "false_accept";
/// Subdirectory for missed-wake captures (plan S8.2, AC11).
pub const MISSED_DIR: &str = "missed";
/// Seconds of audio a missed-wake capture holds (plan S8.2: "that source's last 10 s").
pub const MISSED_SECONDS: usize = 10;
/// Padding written around the keyword span in a per-trigger clip (plan S8.1: kw_span ± 1 s).
pub const CLIP_PADDING_SECONDS: usize = 1;
/// Sidecar extension appended to the `.wav` name, so a clip and its record stay adjacent
/// in `ls` output and are swept together.
pub const SIDECAR_SUFFIX: &str = ".json";

/// How a Speaking session ended — recorded because the two cases need different tuning
/// (plan S8.1: "whether Speaking ended via playback_done or the 30 s timeout").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndedBy {
    /// The satellite reported `playback_done`.
    PlaybackDone,
    /// `capture.speaking_timeout_s` elapsed with no report.
    Timeout,
}

/// Stage latencies in milliseconds, `None` when the stage never happened. AC5's chain:
/// wake → endpoint → transcript → tts-end → playback_start.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageLatencies {
    /// Wake detected → utterance endpoint.
    pub wake_to_endpoint_ms: Option<u64>,
    /// Endpoint → transcript.
    pub endpoint_to_transcript_ms: Option<u64>,
    /// Transcript → tts-end.
    pub transcript_to_tts_end_ms: Option<u64>,
    /// tts-end → playback start.
    pub tts_end_to_playback_start_ms: Option<u64>,
}

/// One arbitration candidate as elected (plan S5.1: "the window actually used is logged
/// with the candidates").
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandidateRecord {
    /// Candidate source id.
    pub source: String,
    /// The [`crate::coordinator::snr`] value that decided the election.
    pub snr: f64,
    /// Absolute sample span of the candidate's keyword, when known.
    pub kw_span: Option<Range<u64>>,
    /// Noise window actually used after the S5.1/B2-1 shrink — the field that makes a
    /// voiced-denominator bug visible in the evidence.
    pub noise_window_used: Option<Range<u64>>,
    /// True for the winner.
    pub winner: bool,
}

/// Why a clip exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerReason {
    /// A normal trigger, kept for tuning (S8.1).
    Trigger,
    /// Rejected by the verify gate (S4.2; feeds threshold raises, R5).
    FalseAccept,
    /// "That was for you": a `save_buffer` event (S8.2, AC11).
    MissedWake,
}

/// Sidecar record for one trigger (plan S8.1). serde-serializable so `replay` can
/// round-trip a false-accept clip back through the pipeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TriggerRecord {
    /// Source that triggered.
    pub source: String,
    /// Room of that source, from config/hello.
    pub room: String,
    /// Group the session belonged to.
    pub group: String,
    /// Wall-clock time of the trigger, RFC 3339 — the only wall time on disk besides logs
    /// (plan §4: monotonic clock internally, wall clock in logs and sidecars only).
    pub wall_clock: String,
    /// Session/conversation id.
    pub conversation_id: Option<String>,
    /// All arbitration candidates with their SNRs and the noise window each actually used.
    pub candidates: Vec<CandidateRecord>,
    /// Winning source, if one was elected.
    pub winner: Option<String>,
    /// Transcript as returned by Parakeet.
    pub transcript: Option<String>,
    /// Verify gate outcome: `Some(stripped)` accepted, `None` rejected (S4.2).
    pub verify_passed_text: Option<String>,
    /// True when the trigger was a follow-up turn with no keyword (S6.1).
    pub wake_initiated: bool,
    /// Stage timings (AC5).
    pub stages: StageLatencies,
    /// How Speaking ended, if it got that far (S8.1).
    pub ended_by: Option<EndedBy>,
    /// Why the clip exists.
    pub reason: TriggerReason,
}

/// Write a per-trigger WAV (kw_span ± [`CLIP_PADDING_SECONDS`] s) plus its sidecar
/// (plan S8.1).
///
/// Returns the clip path; the sidecar is the same name + [`SIDECAR_SUFFIX`]. Creates the
/// directory tree on demand. `pcm` is exactly what was captured for the trigger; the
/// sidecar's `wall_clock` is filled by the caller, since monotonic time never reaches disk.
pub fn write_trigger_clip(
    dir: &Path,
    source: &str,
    pcm: &[i16],
    rec: &TriggerRecord,
) -> anyhow::Result<PathBuf> {
    let target = target_dir(dir, rec.reason);
    std::fs::create_dir_all(&target)?;
    let clip = unique_clip_path(&target, &clip_prefix(Some(&rec.wall_clock)), source, "");
    write_wav(&clip, pcm)?;
    let json = serde_json::to_vec_pretty(rec)?;
    std::fs::write(sidecar_path(&clip), json)?;
    Ok(clip)
}

/// Write the last [`MISSED_SECONDS`] seconds of a source to `clips/missed/` (plan S8.2,
/// AC11) — the "that was for you" path, triggered by the protocol's `save_buffer` event
/// (never a `button` event; see [`crate::proto::Event::SaveBuffer`]).
///
/// Reads through the ring handle without consuming, so the same window stays available to
/// capture and to a later trigger. A source younger than 10 s writes what exists (see
/// [`crate::ring::SourceRing::last`]). Returns the clip path.
pub fn save_buffer_dump(ring: &RingHandle, source: &str, dir: &Path) -> anyhow::Result<PathBuf> {
    let want = MISSED_SECONDS * crate::SAMPLE_RATE_HZ as usize;
    // `last` clamps to what the source has written, so a young source yields a short clip.
    let pcm = with_ring(ring, |r| r.last(want));
    let target = dir.join(MISSED_DIR);
    std::fs::create_dir_all(&target)?;
    let clip = unique_clip_path(&target, &clip_prefix(None), source, "-missed");
    write_wav(&clip, &pcm)?;
    Ok(clip)
}

/// Delete clips older than `keep_days` under `dir` (plan S8.1, AC9); run daily.
///
/// Returns the number of files removed. A sidecar is removed with its WAV; a file whose
/// mtime cannot be read is left alone rather than guessed away.
pub fn retention_sweep(dir: &Path, keep_days: u32) -> anyhow::Result<usize> {
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(u64::from(keep_days) * 86_400))
        .unwrap_or(UNIX_EPOCH);
    // Everything deleted is discovered by walking `dir`, so nothing outside it is ever
    // a candidate; symlinks are skipped rather than followed for the same reason.
    let mut expired = Vec::new();
    collect_expired(dir, true, cutoff, &mut expired);

    let mut removed = 0usize;
    for path in expired {
        if is_sidecar(&path) {
            // An orphan sidecar (its clip already gone, or never written) is worthless
            // on its own; a live clip's sidecar is only removed together with the WAV,
            // which the branch below handles.
            let live = clip_for_sidecar(&path).is_some_and(|clip| clip.exists());
            if live {
                continue;
            }
        } else {
            // Take the sidecar with its clip; a missing sidecar is not an error.
            let sidecar = sidecar_path(&path);
            if std::fs::remove_file(&sidecar).is_ok() {
                removed += 1;
            }
        }
        if std::fs::remove_file(&path).is_ok() {
            removed += 1;
        }
    }
    Ok(removed)
}

/// Walk the areas this module creates, collecting expired clips and sidecars. Only
/// `dir` itself and its [`MISSED_DIR`] / [`FALSE_ACCEPT_DIR`] areas are visited —
/// files in an unrelated subdirectory belong to someone else (F2). Errors while
/// descending are swallowed: a daily sweep must not fail the service over one unreadable
/// directory, and an unreadable mtime is left alone rather than guessed away.
fn collect_expired(dir: &Path, is_root: bool, cutoff: SystemTime, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if meta.is_dir() {
            // Recurse only into the two subdirectories this module creates, and only
            // from the root; `symlink_metadata` never follows a symlink, so a linked
            // directory is skipped rather than swept through (nothing leaves the root).
            let area = path.file_name().and_then(|n| n.to_str());
            if is_root && (area == Some(MISSED_DIR) || area == Some(FALSE_ACCEPT_DIR)) {
                collect_expired(&path, false, cutoff, out);
            }
        } else if meta.is_file()
            && is_clip_artifact(&path)
            && matches!(meta.modified(), Ok(mtime) if mtime < cutoff)
        {
            out.push(path);
        }
    }
}

/// True for the two file kinds this module owns: a clip it generated or that clip's
/// sidecar. Anything else under `clips.dir` belongs to someone else and is never
/// deleted (F2: matching on the extension alone deleted foreign `.wav`/`.json` files
/// that merely lived in the same tree).
///
/// The grammar is exactly `unique_clip_path`'s output — the `clip_prefix` stamp, the
/// sequence field, a `slug` token, the caller's optional suffix (`-missed` is the only
/// one generated) — ending in `.wav`. `sidecar_path` appends `.json` to that *whole*
/// name, so a sidecar is `<generated>.wav.json`; the `.json`-only spelling is tolerated
/// too, since recognising either one still requires the generated stem.
fn is_clip_artifact(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    // Strip in the order the writers built them: `.json` off the sidecar first, then
    // `.wav` off the clip. Anything left must be a name the generator could emit.
    let stem = name.strip_suffix(SIDECAR_SUFFIX).unwrap_or(name);
    let stem = stem.strip_suffix(".wav").unwrap_or(stem);
    // Unrecognised extension (nothing stripped), or nothing in front of it.
    if stem.is_empty() || stem == name {
        return false;
    }
    is_generated_stem(stem)
}

/// `<stamp>-<seq>-<slug>[-<suffix>]` as emitted by `unique_clip_path`.
fn is_generated_stem(stem: &str) -> bool {
    let Some((stamp, rest)) = stem.split_once('-') else {
        return false;
    };
    // `clip_prefix`: `format!("{}T{}Z", 8 digits, 6 digits)` — eight digits, `T`,
    // six digits, `Z` = sixteen bytes. (The indices below are only valid for this
    // exact width, so the width check comes first.)
    let b = stamp.as_bytes();
    if b.len() != 16 || b[8] != b'T' || b[15] != b'Z' {
        return false;
    }
    if !b[..8].iter().all(u8::is_ascii_digit) || !b[9..15].iter().all(u8::is_ascii_digit) {
        return false;
    }
    // Sequence: the `{seq:06}` counter form (a counter past six digits is still that
    // format) or the `{nanos:09}` collision fallback — both all-digits, at least six.
    let Some((seq, rest)) = rest.split_once('-') else {
        return false;
    };
    if seq.len() < 6 || !seq.as_bytes().iter().all(u8::is_ascii_digit) {
        return false;
    }
    // Slug, then the optional `-<suffix>` segment.
    let (slug, suffix) = match rest.split_once('-') {
        Some((slug, suffix)) => (slug, Some(suffix)),
        None => (rest, None),
    };
    is_slug_token(slug) && suffix.is_none_or(is_slug_token)
}

/// The character set `slug` emits: lowercase `[a-z0-9_]`, non-empty.
fn is_slug_token(s: &str) -> bool {
    !s.is_empty()
        && s.as_bytes()
            .iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
}

fn is_sidecar(path: &Path) -> bool {
    path.extension().and_then(|e| e.to_str()) == Some(SIDECAR_SUFFIX.trim_start_matches('.'))
}

/// The clip a sidecar belongs to: the same name without [`SIDECAR_SUFFIX`].
fn clip_for_sidecar(sidecar: &Path) -> Option<PathBuf> {
    let name = sidecar.file_name()?.to_str()?;
    name.strip_suffix(SIDECAR_SUFFIX)
        .map(|stem| sidecar.with_file_name(stem))
}

/// The sidecar for a clip: the clip's name plus [`SIDECAR_SUFFIX`], so the pair stays
/// adjacent in `ls` and is swept together.
fn sidecar_path(clip: &Path) -> PathBuf {
    let mut name = clip.as_os_str().to_os_string();
    name.push(SIDECAR_SUFFIX);
    PathBuf::from(name)
}

/// Where a record's clip lives: rejections under [`FALSE_ACCEPT_DIR`], missed-wake
/// captures under [`MISSED_DIR`], ordinary triggers in the clips dir itself.
fn target_dir(dir: &Path, reason: TriggerReason) -> PathBuf {
    match reason {
        TriggerReason::Trigger => dir.to_path_buf(),
        TriggerReason::FalseAccept => dir.join(FALSE_ACCEPT_DIR),
        TriggerReason::MissedWake => dir.join(MISSED_DIR),
    }
}

/// Fixed-width `YYYYMMDDTHHMMSSZ` (UTC) prefix, so a plain `ls` sorts clips
/// chronologically. Taken from the record's `wall_clock` when it carries a timestamp —
/// that is the time being sorted — and from the clock otherwise (a missed-wake dump has
/// no record).
fn clip_prefix(wall_clock: Option<&str>) -> String {
    let digits: String = wall_clock
        .unwrap_or_default()
        .chars()
        .filter(|c| c.is_ascii_digit())
        .collect();
    if digits.len() >= 14 {
        format!("{}T{}Z", &digits[..8], &digits[8..14])
    } else {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let (year, month, day) = civil_from_days(secs.div_euclid(86_400));
        let clock = secs.rem_euclid(86_400);
        format!(
            "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
            clock / 3_600,
            (clock % 3_600) / 60,
            clock % 60
        )
    }
}

/// Days-since-epoch → (year, month, day), the civil-from-days inverse of Howard Hinnant's
/// `days_from_civil`. Needs no dependency because `clips` may not add one (plan §4).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = (z - era * 146_097) as u64; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365; // [0, 399]
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100); // [0, 365]
    let shifted_month = (5 * day_of_year + 2) / 153; // [0, 11], March = 0
    let day = (day_of_year - (153 * shifted_month + 2) / 5 + 1) as u32; // [1, 31]
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    } as u32; // [1, 12]
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// Sanitized source id for a filename: lowercase `[a-z0-9_]`, no separators, bounded
/// length — a hostile or unicode id can never walk out of the clips directory.
fn slug(source: &str) -> String {
    let mut out = String::new();
    for c in source.chars() {
        if out.len() >= 32 {
            break;
        }
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    let trimmed = out.trim_matches('_').to_string();
    if trimmed.is_empty() {
        "source".to_string()
    } else {
        trimmed
    }
}

/// Process-global counter mixed into every clip name, so two triggers sharing a source and
/// a timestamp still get distinct, write-ordered files.
static CLIP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// `<prefix>-<sequence>-<slug><suffix>.wav`. The prefix sorts, the sequence disambiguates.
fn unique_clip_path(dir: &Path, prefix: &str, source: &str, suffix: &str) -> PathBuf {
    let name_slug = slug(source);
    for _ in 0..64 {
        let seq = CLIP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = dir.join(format!("{prefix}-{seq:06}-{name_slug}{suffix}.wav"));
        if !candidate.exists() {
            return candidate;
        }
    }
    // 64 collisions means another process is writing the same millisecond; sub-second
    // nanos are then enough to stay collision-free.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    dir.join(format!("{prefix}-{nanos:09}-{name_slug}{suffix}.wav"))
}

/// Write 16 kHz mono PCM16 with hound, header flushed by `finalize` so the data size on
/// disk matches `pcm.len()` exactly.
fn write_wav(path: &Path, pcm: &[i16]) -> anyhow::Result<()> {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: crate::SAMPLE_RATE_HZ,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut writer = hound::WavWriter::create(path, spec)?;
    for &sample in pcm {
        writer.write_sample(sample)?;
    }
    writer.finalize()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic non-silent PCM: a sawtooth over `period`, so a truncation or an
    /// off-by-one in the sample count is visible rather than masked by zeros.
    fn ramp(n: usize) -> Vec<i16> {
        (0..n).map(|i| ((i % 2_000) as i16) - 1_000).collect()
    }

    fn record(reason: TriggerReason) -> TriggerRecord {
        TriggerRecord {
            source: "kitchen".to_string(),
            room: "Kitchen".to_string(),
            group: "downstairs".to_string(),
            wall_clock: "2026-11-01T21:15:00.123Z".to_string(),
            conversation_id: Some("11111111-2222-3333-4444-555555555555".to_string()),
            candidates: vec![
                CandidateRecord {
                    source: "kitchen".to_string(),
                    snr: 18.5,
                    kw_span: Some(160_000..172_000),
                    noise_window_used: Some(140_000..160_000),
                    winner: true,
                },
                CandidateRecord {
                    source: "study".to_string(),
                    snr: 4.25,
                    kw_span: Some(161_000..173_000),
                    noise_window_used: None,
                    winner: false,
                },
            ],
            winner: Some("kitchen".to_string()),
            transcript: Some("luna turn on the lights".to_string()),
            verify_passed_text: Some("turn on the lights".to_string()),
            wake_initiated: true,
            stages: StageLatencies {
                wake_to_endpoint_ms: Some(1_200),
                endpoint_to_transcript_ms: Some(310),
                transcript_to_tts_end_ms: Some(90),
                tts_end_to_playback_start_ms: Some(45),
            },
            ended_by: Some(EndedBy::Timeout),
            reason,
        }
    }

    fn read_wav(path: &Path) -> (hound::WavSpec, Vec<i16>) {
        let mut reader = hound::WavReader::open(path).expect("clip is a readable WAV");
        let spec = reader.spec();
        let samples: Vec<i16> = reader
            .samples::<i16>()
            .map(|s| s.expect("PCM16 sample decodes"))
            .collect();
        (spec, samples)
    }

    /// Backdate a file's mtime with `File::set_modified`, the knob that makes the
    /// retention sweep testable without sleeping.
    fn backdate(path: &Path, days: u32) {
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("clip opens for mtime update");
        file.set_modified(SystemTime::now() - Duration::from_secs(u64::from(days) * 86_400))
            .expect("mtime is settable");
    }

    #[test]
    fn clip_is_16khz_mono_pcm16_with_the_exact_sample_count() {
        let dir = tempfile::tempdir().unwrap();
        let pcm = ramp(48_000); // 3 s
        let clip = write_trigger_clip(dir.path(), "kitchen", &pcm, &record(TriggerReason::Trigger))
            .unwrap();

        let (spec, samples) = read_wav(&clip);
        assert_eq!(spec.channels, 1);
        assert_eq!(spec.sample_rate, 16_000);
        assert_eq!(spec.bits_per_sample, 16);
        assert_eq!(spec.sample_format, hound::SampleFormat::Int);
        assert_eq!(samples.len(), pcm.len(), "no padding, no truncation");
        assert_eq!(samples, pcm, "the captured bytes round-trip");
    }

    #[test]
    fn sidecar_round_trips_every_field() {
        let dir = tempfile::tempdir().unwrap();
        let original = record(TriggerReason::Trigger);
        let clip = write_trigger_clip(dir.path(), "kitchen", &ramp(1_600), &original).unwrap();

        let sidecar = sidecar_path(&clip);
        assert!(
            sidecar.to_string_lossy().ends_with(".wav.json"),
            "sidecar sits next to its clip: {sidecar:?}"
        );
        let on_disk: TriggerRecord =
            serde_json::from_slice(&std::fs::read(&sidecar).unwrap()).unwrap();
        assert_eq!(on_disk, original);
        // Spell out the fields the plan singles out, so a rename cannot pass silently.
        assert_eq!(on_disk.source, "kitchen");
        assert_eq!(on_disk.wall_clock, "2026-11-01T21:15:00.123Z");
        assert_eq!(on_disk.candidates[0].snr, 18.5);
        assert_eq!(on_disk.candidates[1].snr, 4.25);
        assert_eq!(
            on_disk.candidates[0].noise_window_used,
            Some(140_000..160_000),
            "the window actually used survives the round trip"
        );
        assert_eq!(on_disk.candidates[1].noise_window_used, None);
        assert_eq!(on_disk.winner.as_deref(), Some("kitchen"));
        assert_eq!(
            on_disk.transcript.as_deref(),
            Some("luna turn on the lights")
        );
        assert_eq!(
            on_disk.verify_passed_text.as_deref(),
            Some("turn on the lights")
        );
        assert_eq!(on_disk.stages.wake_to_endpoint_ms, Some(1_200));
        assert_eq!(on_disk.stages.tts_end_to_playback_start_ms, Some(45));
    }

    #[test]
    fn sidecar_records_both_ways_speaking_ended() {
        let dir = tempfile::tempdir().unwrap();
        for (ended, needle) in [
            (
                Some(EndedBy::PlaybackDone),
                "\"ended_by\": \"playback_done\"",
            ),
            (Some(EndedBy::Timeout), "\"ended_by\": \"timeout\""),
            (None, "\"ended_by\": null"),
        ] {
            let mut rec = record(TriggerReason::Trigger);
            rec.ended_by = ended;
            let clip = write_trigger_clip(dir.path(), "kitchen", &ramp(160), &rec).unwrap();
            let json = std::fs::read_to_string(sidecar_path(&clip)).unwrap();
            assert!(
                json.contains(needle),
                "end reason {ended:?} must be visible as {needle}"
            );
            let back: TriggerRecord = serde_json::from_str(&json).unwrap();
            assert_eq!(back.ended_by, ended);
        }
    }

    #[test]
    fn false_accept_clip_lands_in_the_false_accept_location() {
        let dir = tempfile::tempdir().unwrap();
        let clip = write_trigger_clip(
            dir.path(),
            "kitchen",
            &ramp(1_600),
            &record(TriggerReason::FalseAccept),
        )
        .unwrap();
        assert_eq!(clip.parent().unwrap(), dir.path().join(FALSE_ACCEPT_DIR));
        assert!(clip.exists() && sidecar_path(&clip).exists());

        let missed = write_trigger_clip(
            dir.path(),
            "kitchen",
            &ramp(1_600),
            &record(TriggerReason::MissedWake),
        )
        .unwrap();
        assert_eq!(missed.parent().unwrap(), dir.path().join(MISSED_DIR));

        let plain = write_trigger_clip(
            dir.path(),
            "kitchen",
            &ramp(1_600),
            &record(TriggerReason::Trigger),
        )
        .unwrap();
        assert_eq!(plain.parent().unwrap(), dir.path());
    }

    #[test]
    fn clip_names_sort_by_time_and_never_collide() {
        let dir = tempfile::tempdir().unwrap();
        // Same record, three times: the names must still be distinct and ordered.
        let mut names = Vec::new();
        for _ in 0..3 {
            let clip = write_trigger_clip(
                dir.path(),
                "kitchen",
                &ramp(160),
                &record(TriggerReason::Trigger),
            )
            .unwrap();
            names.push(clip.file_name().unwrap().to_str().unwrap().to_string());
        }
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(sorted, names, "names sort in write order");
        assert_eq!(
            names.iter().collect::<std::collections::HashSet<_>>().len(),
            3
        );
        for name in &names {
            assert!(
                name.starts_with("20261101T211500Z"),
                "sortable prefix: {name}"
            );
            assert!(name.ends_with("-kitchen.wav"));
        }

        // A later trigger sorts after an earlier one.
        let mut later = record(TriggerReason::Trigger);
        later.wall_clock = "2026-11-02T00:00:00Z".to_string();
        let clip = write_trigger_clip(dir.path(), "kitchen", &ramp(160), &later).unwrap();
        let later_name = clip.file_name().unwrap().to_str().unwrap().to_string();
        assert!(
            later_name > names[0],
            "{later_name} sorts after {}",
            names[0]
        );
    }

    #[test]
    fn hostile_source_ids_cannot_escape_the_clip_directory() {
        let dir = tempfile::tempdir().unwrap();
        for hostile in ["../../etc/passwd", "kitchen #1/ok", "", "///", "été…"] {
            let clip = write_trigger_clip(
                dir.path(),
                hostile,
                &ramp(160),
                &record(TriggerReason::Trigger),
            )
            .unwrap();
            assert_eq!(clip.parent().unwrap(), dir.path(), "{hostile:?} escaped");
            let name = clip.file_name().unwrap().to_str().unwrap();
            assert!(!name.contains('/') && !name.contains(".."), "{name}");
            assert!(name.len() < 80, "{name}");
        }
        assert_eq!(slug("../../etc/passwd"), "etc_passwd");
        assert_eq!(slug("###"), "source");
        assert_eq!(slug("Kitchen TV"), "kitchen_tv");
    }

    #[test]
    fn buffer_dump_writes_exactly_ten_seconds() {
        let dir = tempfile::tempdir().unwrap();
        let ring = crate::ring::new_handle(crate::ring::RING_SAMPLES);
        let fed = ramp(crate::ring::RING_SAMPLES); // 15 s, so the ring has wrapped
        with_ring(&ring, |r| {
            r.write(&fed);
        });

        let clip = save_buffer_dump(&ring, "kitchen", dir.path()).unwrap();
        assert_eq!(clip.parent().unwrap(), dir.path().join(MISSED_DIR));
        let (spec, samples) = read_wav(&clip);
        assert_eq!(spec.channels, 1);
        assert_eq!(spec.sample_rate, 16_000);
        assert_eq!(
            samples.len(),
            MISSED_SECONDS * 16_000,
            "the missed-wake clip holds exactly 10 s"
        );
        assert_eq!(
            samples,
            fed[fed.len() - samples.len()..],
            "and it is the newest 10 s"
        );
        // Reading must not consume the ring (plan rule 5).
        assert_eq!(with_ring(&ring, |r| r.written()), fed.len() as u64);
    }

    #[test]
    fn buffer_dump_of_a_young_source_writes_what_exists() {
        let dir = tempfile::tempdir().unwrap();
        let ring = crate::ring::new_handle(crate::ring::RING_SAMPLES);
        let fed = ramp(32_000); // 2 s
        with_ring(&ring, |r| {
            r.write(&fed);
        });
        let clip = save_buffer_dump(&ring, "study", dir.path()).unwrap();
        let (_, samples) = read_wav(&clip);
        assert_eq!(samples, fed, "2 s of source yields 2 s, not 10 s of zeros");
    }

    #[test]
    fn sweep_deletes_only_backdated_pairs_and_counts_them() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();

        let mut old = Vec::new();
        for reason in [
            TriggerReason::Trigger,
            TriggerReason::FalseAccept,
            TriggerReason::MissedWake,
        ] {
            let clip =
                write_trigger_clip(dir.path(), "kitchen", &ramp(160), &record(reason)).unwrap();
            backdate(&clip, 45);
            backdate(&sidecar_path(&clip), 45);
            old.push(clip);
        }
        // Exactly at the boundary is still kept: the threshold is strictly older than 30 d.
        let boundary = write_trigger_clip(
            dir.path(),
            "kitchen",
            &ramp(160),
            &record(TriggerReason::Trigger),
        )
        .unwrap();
        backdate(&boundary, 29);
        let fresh = write_trigger_clip(
            dir.path(),
            "kitchen",
            &ramp(160),
            &record(TriggerReason::Trigger),
        )
        .unwrap();
        // Anything that is not a clip or a sidecar is none of our business, even when
        // old — including files that merely share the extension (F2: to an
        // extension-only rule `calibration.json` looks like an orphan sidecar and
        // `foreign.wav` like a clip).
        let stray = dir.path().join("notes.txt");
        std::fs::write(&stray, b"keep me").unwrap();
        backdate(&stray, 400);
        let foreign_json = dir.path().join("calibration.json");
        std::fs::write(&foreign_json, b"{\"keep\":true}").unwrap();
        backdate(&foreign_json, 400);
        let foreign_wav = dir.path().join("foreign.wav");
        std::fs::write(&foreign_wav, b"not ours").unwrap();
        backdate(&foreign_wav, 400);
        // A foreign file inside an area this module owns, and a clip-shaped name in a
        // subdirectory this module never creates: each guard stands on its own.
        let foreign_in_missed = dir.path().join(MISSED_DIR).join("session.json");
        std::fs::write(&foreign_in_missed, b"{\"keep\":true}").unwrap();
        backdate(&foreign_in_missed, 400);
        let recordings = dir.path().join("recordings");
        std::fs::create_dir(&recordings).unwrap();
        let nested_wav = recordings.join("20261101T211500Z-000042-kitchen.wav");
        std::fs::write(&nested_wav, b"not ours").unwrap();
        backdate(&nested_wav, 400);
        let untouched = outside.path().join("old.wav");
        std::fs::write(&untouched, b"not ours").unwrap();
        backdate(&untouched, 400);

        assert_eq!(
            retention_sweep(dir.path(), 30).unwrap(),
            6,
            "three stale clips plus their three sidecars"
        );
        for clip in &old {
            assert!(!clip.exists(), "stale clip {clip:?} must be gone");
            assert!(
                !sidecar_path(clip).exists(),
                "stale sidecar must go with it"
            );
        }
        assert!(
            boundary.exists() && sidecar_path(&boundary).exists(),
            "29 d is inside the window"
        );
        assert!(
            fresh.exists() && sidecar_path(&fresh).exists(),
            "fresh clips are kept"
        );
        assert!(stray.exists(), "non-clip files are never deleted");
        assert!(
            foreign_json.exists() && foreign_wav.exists(),
            "foreign .json/.wav under clips.dir are not this module's to delete"
        );
        assert!(
            foreign_in_missed.exists(),
            "a foreign file inside an owned area is still foreign"
        );
        assert!(
            nested_wav.exists(),
            "an unrelated subdirectory is not swept, even with a clip-shaped name"
        );
        assert!(
            untouched.exists(),
            "nothing outside the clips dir is touched"
        );
        assert_eq!(
            retention_sweep(dir.path(), 30).unwrap(),
            0,
            "second sweep is a no-op"
        );
    }

    #[test]
    fn sweep_removes_orphan_sidecars_and_tolerates_a_missing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let clip = write_trigger_clip(
            dir.path(),
            "kitchen",
            &ramp(160),
            &record(TriggerReason::Trigger),
        )
        .unwrap();
        let sidecar = sidecar_path(&clip);
        std::fs::remove_file(&clip).unwrap(); // the clip is gone; the sidecar is dead weight
        backdate(&sidecar, 40);

        assert_eq!(retention_sweep(dir.path(), 30).unwrap(), 1);
        assert!(!sidecar.exists());
        // A live clip's sidecar is not swept on its own.
        let kept = write_trigger_clip(
            dir.path(),
            "kitchen",
            &ramp(160),
            &record(TriggerReason::Trigger),
        )
        .unwrap();
        backdate(&sidecar_path(&kept), 40);
        assert_eq!(retention_sweep(dir.path(), 30).unwrap(), 0);
        assert!(sidecar_path(&kept).exists());

        let missing = dir.path().join("does-not-exist");
        assert_eq!(retention_sweep(&missing, 30).unwrap(), 0);
    }

    #[test]
    fn is_clip_artifact_accepts_only_generated_names() {
        // The sweep test above only ever writes ordinary trigger names; this pins the
        // forms `unique_clip_path` can actually emit — the `-missed` suffix and the
        // nine-digit nanosecond fallback — so a genuine clip is never un-purgeable,
        // and the names it can never emit are not deletable (F2).
        let ours = [
            // `unique_clip_path`'s output, exactly as written.
            "20261101T211500Z-000042-kitchen.wav",
            // `sidecar_path` appends `.json` to that *whole* name — this is the
            // spelling the module actually writes, so it must be recognized.
            "20261101T211500Z-000042-kitchen.wav.json",
            "20261101T211500Z-000042-study_desk.wav",
            "20261101T211500Z-000042-study_desk.wav.json",
            "20261101T211500Z-000042-study_desk-missed.wav",
            "20261101T211500Z-000042-study_desk-missed.wav.json",
            // The `{nanos:09}` collision fallback, and a counter past six digits
            // (still `{seq:06}` output).
            "20261101T211500Z-123456789-kitchen-missed.wav",
            "20261101T211500Z-1000000-kitchen.wav",
            // Tolerated: a generated stem carrying only `.json`. The module never
            // writes this, but recognising it can never delete a foreign name.
            "20261101T211500Z-000042-kitchen.json",
        ];
        for name in ours {
            assert!(is_clip_artifact(Path::new(name)), "{name} is a generated name");
        }
        let theirs = [
            "calibration.json",
            "foreign.wav",
            // The chained strip must not turn these into clip names.
            "calibration.wav.json",
            "foreign.wav.json",
            "20261101T211500Z-000042-kitchen.json.json",
            "README.json",
            "notes.txt",
            ".json",
            ".wav",
            ".wav.json",
            "20261101T211500Z-000042-kitchen.mp3",
            // Wrong stamp, wrong sequence width, hostile slug.
            "20261101T21150Z-000042-kitchen.wav",
            "20261101T211500X-000042-kitchen.wav",
            "20261101T211500Z-00004-kitchen.wav",
            "20261101T211500Z-00004a-kitchen.wav",
            "20261101T211500Z-000042-kit-chen!.wav",
            "20261101T211500Z-000042-.wav",
            "kitchen.wav",
            "2026-11-01T21:15:00Z-000042-kitchen.wav",
        ];
        for name in theirs {
            assert!(!is_clip_artifact(Path::new(name)), "{name} is not ours to delete");
        }
    }

    /// The property that actually matters: every name the real generator emits must be
    /// recognized, or the sweep silently stops purging that kind of clip and household
    /// audio accumulates forever (AC9). The names come from `write_trigger_clip`,
    /// `save_buffer_dump`, `unique_clip_path` and `sidecar_path` — never typed by hand
    /// — so a grammar drift that rejects a generated name reddens this test instead of
    /// quietly disabling retention.
    #[test]
    fn every_name_the_real_generator_emits_is_recognized() {
        let dir = tempfile::tempdir().unwrap();

        for reason in [
            TriggerReason::Trigger,
            TriggerReason::FalseAccept,
            TriggerReason::MissedWake,
        ] {
            let clip =
                write_trigger_clip(dir.path(), "kitchen", &ramp(160), &record(reason)).unwrap();
            assert!(is_clip_artifact(&clip), "generated clip {}", clip.display());
            let sidecar = sidecar_path(&clip);
            assert!(
                is_clip_artifact(&sidecar),
                "generated sidecar {}",
                sidecar.display()
            );
        }

        // The missed-wake dump has its own writer and its own `-missed` suffix.
        let ring = crate::ring::new_handle(crate::ring::RING_SAMPLES);
        let fed = ramp(160);
        with_ring(&ring, |r| {
            r.write(&fed);
        });
        let dump = save_buffer_dump(&ring, "study desk", dir.path()).unwrap();
        assert!(
            is_clip_artifact(&dump),
            "missed-wake dump {}",
            dump.display()
        );

        // Hostile-but-accepted source ids and both `clip_prefix` sources (the record's
        // timestamp and the wall clock), run through the generator itself.
        let rec = record(TriggerReason::Trigger);
        let from_record = clip_prefix(Some(&rec.wall_clock));
        let from_clock = clip_prefix(None);
        let long_id = "x".repeat(64);
        for source in ["kitchen", "study-desk", "Bed Room!!", "", "Ünïcödé 42", long_id.as_str()] {
            for prefix in [from_clock.as_str(), from_record.as_str()] {
                for suffix in ["", "-missed"] {
                    let clip = unique_clip_path(dir.path(), prefix, source, suffix);
                    assert!(
                        is_clip_artifact(&clip),
                        "generator emitted {} with suffix {suffix:?}",
                        clip.display()
                    );
                    let sidecar = sidecar_path(&clip);
                    assert!(is_clip_artifact(&sidecar), "sidecar {}", sidecar.display());
                }
            }
        }
    }

    #[test]
    fn civil_date_conversion_matches_known_days() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29)); // leap day
        assert_eq!(civil_from_days(20_758), (2026, 11, 1)); // today
    }
}
