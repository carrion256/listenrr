//! Sample-indexed ring buffer: one [`SourceRing`] per connected source (plan §4, S1.3).
//!
//! Time in this backend is a `u64` **sample index** at 16 kHz, never a wall clock
//! (plan §3 invariant). The ring is the authoritative store: capture reads backwards by
//! absolute sample index, a lagged broadcast subscriber recovers a missed span by
//! re-reading it here, and `save_buffer` dumps the last 10 s from here. Reads never
//! consume — a span can be re-read any number of times while it is still retained.
//!
//! Retention policy (deliberate, frozen choice): a sample older than the retained
//! window — i.e. below `written() - capacity()` — reads back as **zero**, it is not an
//! error. Callers bound their ranges to `written()`/`retained_start()`; a zero-filled
//! hole is a benign, testable answer for "the satellite never sent that", and it keeps
//! the capture/clip paths free of error plumbing for what is normally a bounded read.
//! [`SourceRing::retained_start`] is exported so a caller that wants to *detect* the
//! hole can compare before reading.
//!
//! This module is a **contract file**: lanes MUST NOT edit it (see `docs/design/CONTRACT-NOTES.md`).

use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard};

/// Ring capacity in samples: 15 s at 16 kHz (plan §4: fixed 240 000 samples).
pub const RING_SAMPLES: usize = 240_000;
/// Sample rate the ring's indices are defined against.
pub const SAMPLE_RATE_HZ: u32 = 16_000;

/// A shareable, lockable ring handle. ingest owns the writer; capture/clip paths read.
pub type RingHandle = Arc<Mutex<SourceRing>>;

/// Fixed-capacity, sample-indexed ring of `i16` mono 16 kHz PCM.
#[derive(Debug, Clone)]
pub struct SourceRing {
    buf: Vec<i16>,
    cap: usize,
    written: u64,
}

impl SourceRing {
    /// Empty ring holding `capacity_samples` samples. Panics on a zero capacity.
    pub fn new(capacity_samples: usize) -> Self {
        assert!(capacity_samples > 0, "ring capacity must be positive");
        Self {
            buf: vec![0; capacity_samples],
            cap: capacity_samples,
            written: 0,
        }
    }

    /// Production ring: [`RING_SAMPLES`] (15 s at 16 kHz).
    pub fn with_capacity() -> Self {
        Self::new(RING_SAMPLES)
    }

    /// Capacity in samples.
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Monotonically increasing count of samples ever written. It also *is* the sample
    /// index one past the newest sample, so `written()` is "now" in the sample domain.
    pub fn written(&self) -> u64 {
        self.written
    }

    /// Absolute sample index of the oldest sample still retrievable. Anything below this
    /// reads back as zero (see the module docs).
    pub fn retained_start(&self) -> u64 {
        self.written.saturating_sub(self.cap as u64)
    }

    /// Append samples, advancing [`written`](Self::written) by `samples.len()`.
    /// Returns the new `written()` value so ingest can hand the span end to KWS.
    ///
    /// A single write longer than the capacity keeps only its newest `capacity` samples
    /// (the older ones were never going to be readable anyway) but still advances the
    /// index by the full length, which is what keeps sample indices honest.
    pub fn write(&mut self, samples: &[i16]) -> u64 {
        if samples.is_empty() {
            return self.written;
        }
        let n = samples.len();
        // Only the newest `cap` samples of the write can land; index math stays exact.
        let skip = n.saturating_sub(self.cap);
        let first_abs = self.written + skip as u64;
        let cap = self.cap as u64;
        let mut pos = (first_abs % cap) as usize;
        for &s in &samples[skip..] {
            self.buf[pos] = s;
            pos += 1;
            if pos == self.cap {
                pos = 0;
            }
        }
        self.written += n as u64;
        self.written
    }

    /// Read absolute samples `r.start..r.end` without consuming.
    ///
    /// `r.end` is clamped to `written()`; `start >= end` yields an empty vector; indices
    /// below [`retained_start`](Self::retained_start) yield zeros.
    pub fn range(&self, r: Range<u64>) -> Vec<i16> {
        let end = r.end.min(self.written);
        if r.start >= end {
            return Vec::new();
        }
        let retained = self.retained_start();
        let cap = self.cap as u64;
        let mut out = vec![0i16; (end - r.start) as usize];
        if r.start >= retained {
            // Whole range is live: walk the buffer with at most one wrap.
            let mut pos = (r.start % cap) as usize;
            for slot in out.iter_mut() {
                *slot = self.buf[pos];
                pos += 1;
                if pos == self.cap {
                    pos = 0;
                }
            }
        } else {
            for (i, idx) in (r.start..end).enumerate() {
                if idx >= retained {
                    out[i] = self.buf[(idx % cap) as usize];
                }
            }
        }
        out
    }

    /// Read the newest `n` samples without consuming.
    ///
    /// Returns at most `written()` samples: a source that has been up 2 s and is asked
    /// for its last 10 s yields 2 s of audio, which is what S8.2's missed-wake dump
    /// wants. Beyond the retained window the samples are zero-filled by
    /// [`range`](Self::range).
    pub fn last(&self, n: usize) -> Vec<i16> {
        let take = (n as u64).min(self.written);
        self.range(self.written - take..self.written)
    }
}

/// Create a shareable handle for a source, sized in samples.
pub fn new_handle(capacity_samples: usize) -> RingHandle {
    Arc::new(Mutex::new(SourceRing::new(capacity_samples)))
}

/// Lock a handle for access, recovering from a poisoned mutex (a panicking writer must
/// not take the whole backend down; the ring's invariants are plain integers).
pub fn with_ring<R>(handle: &RingHandle, f: impl FnOnce(&mut SourceRing) -> R) -> R {
    let mut guard: MutexGuard<'_, SourceRing> = match handle.lock() {
        Ok(g) => g,
        Err(poisoned) => poisoned.into_inner(),
    };
    f(&mut guard)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A ramp with a distinctive sign pattern, so a wrong offset is obvious.
    fn ramp(len: usize, seed: u64) -> Vec<i16> {
        (0..len)
            .map(|i| {
                let v = ((seed + i as u64) * 37) % 65536;
                (v as i32 - 32768) as i16
            })
            .collect()
    }

    #[test]
    fn constants_are_the_frozen_window() {
        assert_eq!(RING_SAMPLES, 240_000);
        assert_eq!(RING_SAMPLES, SAMPLE_RATE_HZ as usize * 15);
        assert_eq!(SAMPLE_RATE_HZ, 16_000);
    }

    #[test]
    fn new_ring_is_empty() {
        let r = SourceRing::new(16);
        assert_eq!(r.written(), 0);
        assert_eq!(r.capacity(), 16);
        assert_eq!(r.retained_start(), 0);
        assert!(r.range(0..16).is_empty());
        assert!(r.last(4).is_empty());
    }

    #[test]
    #[should_panic(expected = "positive")]
    fn zero_capacity_is_rejected() {
        SourceRing::new(0);
    }

    #[test]
    fn write_advances_index_monotonically_and_empty_write_is_a_noop() {
        let mut r = SourceRing::new(8);
        assert_eq!(r.write(&[]), 0);
        assert_eq!(r.write(&[1, 2, 3]), 3);
        assert_eq!(r.written(), 3);
        assert_eq!(r.write(&[]), 3);
        assert_eq!(r.write(&[4, 5]), 5);
        assert_eq!(r.written(), 5);
        assert_eq!(r.range(0..5), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn range_reads_without_consuming() {
        let mut r = SourceRing::new(64);
        let data = ramp(64, 0);
        r.write(&data);
        let a = r.range(10..20);
        let b = r.range(10..20);
        assert_eq!(a, b);
        assert_eq!(a, data[10..20]);
        assert_eq!(r.written(), 64);
        // Repeated reads never change contents.
        for _ in 0..3 {
            assert_eq!(r.range(0..64), data);
        }
    }

    #[test]
    fn reads_backwards_and_non_aligned() {
        let mut r = SourceRing::new(100);
        let data = ramp(100, 5);
        r.write(&data);
        // Backwards walk: read a span, then the span before it, then one straddling it.
        assert_eq!(r.range(60..90), data[60..90]);
        assert_eq!(r.range(20..60), data[20..60]);
        assert_eq!(r.range(55..61), data[55..61]);
        // Single sample.
        assert_eq!(r.range(7..8), vec![data[7]]);
        // A kw_span-style read whose declared end exceeds what exists clamps.
        let mut r2 = SourceRing::new(100);
        r2.write(&data[..10]);
        assert_eq!(r2.range(0..5), data[..5]);
        assert_eq!(r2.range(0..10_000).len(), 10, "end clamps to written");
    }

    #[test]
    fn degenerate_ranges_are_empty_not_panics() {
        let mut r = SourceRing::new(16);
        r.write(&ramp(16, 0));
        assert!(r.range(5..5).is_empty());
        // False positive: the reversed range is the input under test (start > end must
        // read back empty), not a range anyone intends to iterate.
        #[allow(clippy::reversed_empty_ranges)]
        {
            assert!(r.range(9..3).is_empty(), "start > end yields empty");
        }
        assert!(r.range(1000..2000).is_empty(), "past written yields empty");
    }

    #[test]
    fn wraparound_preserves_the_newest_window() {
        const CAP: usize = 8;
        let mut r = SourceRing::new(CAP);
        let a: Vec<i16> = (0..5).map(|i| i as i16 + 1).collect();
        let b: Vec<i16> = (5..11).map(|i| i as i16 + 1).collect();
        r.write(&a);
        assert_eq!(r.written(), 5);
        assert_eq!(r.range(0..5), a);
        r.write(&b);
        assert_eq!(r.written(), 11);
        assert_eq!(r.retained_start(), 3);
        // Retained tail is exact.
        assert_eq!(r.range(3..11), vec![4, 5, 6, 7, 8, 9, 10, 11]);
        assert_eq!(r.range(8..11), vec![9, 10, 11]);
        // Older-than-retained reads as zeros.
        assert_eq!(r.range(0..3), vec![0, 0, 0]);
        // A read straddling the retention boundary: zeros then real data.
        assert_eq!(r.range(1..6), vec![0, 0, 4, 5, 6]);
        // Capacity-sized read is always the newest window.
        assert_eq!(r.last(CAP), vec![4, 5, 6, 7, 8, 9, 10, 11]);
    }

    #[test]
    fn exact_capacity_boundary_write_has_no_off_by_one() {
        const CAP: usize = 16;
        let mut r = SourceRing::new(CAP);
        let data = ramp(CAP, 0);
        r.write(&data);
        assert_eq!(r.written(), CAP as u64);
        assert_eq!(r.retained_start(), 0);
        assert_eq!(r.range(0..CAP as u64), data);
        assert_eq!(r.last(CAP), data);
        // 16 + 15 + 1 = 32 samples written; the newest window must stay exact.
        r.write(&data[1..]);
        r.write(&[1234]);
        assert_eq!(r.written(), (CAP + 16) as u64);
        assert_eq!(r.retained_start(), 16);
        assert_eq!(r.last(1), vec![1234]);
        assert_eq!(r.range((CAP + 15) as u64..(CAP + 16) as u64), vec![1234]);
        // The last 16 samples are data[1..] followed by the single new sample.
        let mut expect = data[1..].to_vec();
        expect.push(1234);
        assert_eq!(r.last(CAP), expect);
    }

    #[test]
    fn many_wraps_keep_the_recent_window_exact() {
        // Simulate 3× capacity of frames-worth of writes and compare against ground truth.
        const CAP: usize = 2048;
        const TOTAL: usize = CAP * 3 + 777;
        let mut r = SourceRing::new(CAP);
        let all = ramp(TOTAL, 1);
        for chunk in all.chunks(313) {
            r.write(chunk);
        }
        assert_eq!(r.written(), TOTAL as u64);
        assert_eq!(r.retained_start(), (TOTAL - CAP) as u64);
        assert_eq!(
            r.range((TOTAL - CAP) as u64..TOTAL as u64),
            all[TOTAL - CAP..]
        );
        assert_eq!(r.last(1024), all[TOTAL - 1024..]);
        // Arbitrary window near the retention edge.
        let lo = TOTAL - 1500;
        assert_eq!(r.range(lo as u64..(lo + 200) as u64), all[lo..lo + 200]);
    }

    #[test]
    fn single_write_larger_than_capacity_keeps_newest_and_advances_index() {
        const CAP: usize = 10;
        let mut r = SourceRing::new(CAP);
        let big = ramp(25, 0);
        assert_eq!(r.write(&big), 25);
        assert_eq!(r.written(), 25);
        assert_eq!(r.retained_start(), 15);
        assert_eq!(r.range(15..25), big[15..25]);
        assert_eq!(r.last(CAP), big[15..25]);
        // Then a normal write still lands correctly after the jump.
        r.write(&[7; 3]);
        assert_eq!(r.written(), 28);
        assert_eq!(r.range(25..28), vec![7, 7, 7]);
    }

    #[test]
    fn last_clamps_to_what_exists() {
        let mut r = SourceRing::new(240_000);
        let data = ramp(32_000, 9); // 2 s
        r.write(&data);
        // "Save the last 10 s" of a source that has only sent 2 s: 2 s of audio.
        let ten = r.last(160_000);
        assert_eq!(ten.len(), 32_000);
        assert_eq!(ten, data);
        assert_eq!(r.last(0).len(), 0);
        assert_eq!(r.last(100), data[31_900..]);
    }

    #[test]
    fn handle_is_shared_across_threads_and_serialises_writes() {
        let h = new_handle(4096);
        let mut writers = Vec::new();
        for t in 0..4u64 {
            let h = Arc::clone(&h);
            writers.push(std::thread::spawn(move || {
                for i in 0..16u64 {
                    let seed = t * 1000 + i;
                    let chunk: Vec<i16> = (0..64u64)
                        .map(|s| ((seed * 64 + s) % 65536) as u16 as i16)
                        .collect();
                    with_ring(&h, |r| {
                        r.write(&chunk);
                    });
                }
            }));
        }
        for w in writers {
            w.join().unwrap();
        }
        assert_eq!(with_ring(&h, |r| r.written()), 4 * 16 * 64);
        // No sample is lost or torn: the tail is a contiguous run of one writer's chunk.
        let tail = with_ring(&h, |r| r.last(64));
        assert_eq!(tail.len(), 64);
        let first = u64::from(tail[0] as u16);
        assert!(
            tail.iter()
                .enumerate()
                .all(|(i, &v)| u64::from(v as u16) == (first + i as u64) % 65536),
            "torn write: {tail:?}"
        );
    }

    #[test]
    fn production_sized_ring_serves_the_documented_use_cases() {
        // 15 s hard cap, a 2 s noise window read, and a 10 s save_buffer dump.
        let mut r = SourceRing::with_capacity();
        let full = ramp(RING_SAMPLES, 3);
        r.write(&full);
        assert_eq!(r.written(), RING_SAMPLES as u64);
        assert_eq!(r.range(0..RING_SAMPLES as u64), full);
        let noise_window = SAMPLE_RATE_HZ as u64 * 2;
        assert_eq!(
            r.range(1000..1000 + noise_window),
            full[1000..1000 + noise_window as usize]
        );
        assert_eq!(
            r.last(SAMPLE_RATE_HZ as usize * 10),
            full[RING_SAMPLES - 160_000..]
        );
    }
}
