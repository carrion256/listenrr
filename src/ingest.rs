//! Uplink ingest: one tokio task per satellite connection (plan S1.4).
//!
//! Per connection: read the hello line → authenticate → register the source → then
//! decode [`proto::FrameDecoder`] output and fan it out three ways, without ever
//! blocking on DSP or network work (plan §3 invariant):
//! 1. write the frame's PCM into the source's [`ring::SourceRing`] (authoritative store),
//! 2. hand the frame to the KWS thread over `kws_tx` (bounded 4096, drop-oldest + counter),
//! 3. publish it on the frame broadcast for live capture; a `Lagged` subscriber recovers
//!    the missed sample span by re-reading the ring by index.
//!
//! Events (including `save_buffer`) go to the coordinator over `event_tx`. Sequence gaps
//! and inter-arrival jitter are counted per source and logged as one structured line per
//! source per minute (S5.3). Unknown `source_id` ⇒ close + log unless
//! `server.allow_unknown_sources` (S7.1 flips it).
//!
//! Hello auth (R11): when `server.auth_token` is configured the hello token must match,
//! compared with [`proto::constant_time_eq`]; mismatch/missing ⇒ close + a warn limited
//! to one per second per peer IP, tracked in a map shared across all connections
//! ([`WarnThrottle`]).
//!
//! LANE-OWNED SEAM (Lane A, S1.4/S5.3/S7.1). Public signatures are frozen — implement
//! the bodies; add only private items. Report a signature mismatch instead of editing.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::IpAddr;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, watch};

use crate::config::Config;
use crate::proto::{self, Caps, Event, FrameDecoder, Hello, UpMessage};
use crate::ring::{new_handle, with_ring, RingHandle};

/// Inter-arrival samples kept per source for the jitter percentiles (S5.3).
///
/// One minute of 64 ms frames is ~940 entries, so 4096 absorbs a stalled timer without
/// growing unbounded.
const JITTER_WINDOW_SAMPLES: usize = 4096;
/// A throttle entry older than this many intervals carries no information.
const THROTTLE_PRUNE_HORIZON: u32 = 10;
/// Upper bound on the throttle map before stale entries are pruned (bounded memory).
const THROTTLE_PRUNE_THRESHOLD: usize = 256;
/// Socket read size per poll: a whole number of audio records.
const READ_CHUNK: usize = proto::AUDIO_RECORD_LEN * 4;
/// Controls queued per connection before [`SourceRegistry::send_control`] reports
/// [`ControlSend::QueueFull`]. A conversation sends a handful (listening, speaking, idle),
/// so a queue this deep only fills when the satellite has stopped reading its socket.
const DOWNLINK_QUEUE: usize = 32;
/// How long one downlink write may take before the connection is closed as unreadable.
const DOWNLINK_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a connection may stay silent before the server stops waiting for it.
///
/// Every connected satellite has a heartbeat: `maybe_send_rssi_` emits an `rssi` event on
/// `RSSI_INTERVAL_MS` (30 s) from the firmware's main loop for as long as it is in the
/// Connected state, so a satellite that is alive puts a byte on this socket at least that
/// often. Ten missed heartbeats is not a satellite that is merely idle.
///
/// The server needs a bound like this because nothing else here can discover a peer that
/// vanished without a FIN: the connection writes only the occasional LED or earcon control,
/// and a write to a dead peer succeeds into the kernel's send buffer, so no write error
/// reliably notices either,
/// and the socket option that would — `SO_KEEPALIVE` — is not reachable from this
/// dependency set: `std`'s setter is an unstable feature on the pinned 1.99.0 toolchain
/// (rust-lang/rust#155889), tokio exposes no setter, and `libc`/`socket2` as direct
/// dependencies were ruled out. The OS keepalive defaults would wait hours of idle before
/// their first probe in any case. This bounds the leak of one task and one fd per
/// unnoticed disconnection at minutes, which is what makes the eventual reap real.
const READ_IDLE_DEADLINE: Duration = Duration::from_secs(10 * 30);

/// Frames buffered for the KWS thread before drop-oldest kicks in (plan S1.4).
pub const KWS_CHANNEL_CAPACITY: usize = 4096;
/// Broadcast depth for live capture subscribers; a lagged subscriber recovers from the
/// ring, so this only needs to absorb a scheduling hiccup.
pub const FRAME_BROADCAST_CAPACITY: usize = 512;
/// Coordinator event queue depth.
pub const EVENT_QUEUE_CAPACITY: usize = 256;
/// Per-source metric line cadence (S5.3).
pub const METRIC_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// Minimum gap between two auth warnings for the same peer IP (R11, P3).
pub const WARN_THROTTLE_INTERVAL: Duration = Duration::from_secs(1);

/// A message the coordinator needs from ingest.
#[derive(Debug, Clone, PartialEq)]
pub enum IngestEvent {
    /// A protocol event from a source (`playback_done`, `rssi`, `button`, `save_buffer`).
    Event {
        /// Source id as registered from hello.
        source: String,
        /// The decoded event.
        event: Event,
    },
    /// The connection closed or was rejected; drop per-connection state.
    SourceGone {
        /// Source id.
        source: String,
    },
}

/// Per-source stream health counters, surfaced as one structured line per source per
/// minute (S5.3).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SourceStats {
    /// Frames accepted since connect.
    pub frames: u64,
    /// Frames whose `seq` was not `last_seq + 1` (R7).
    pub seq_gaps: u64,
    /// Samples lost to those gaps (sum of gap sizes).
    pub gap_samples: u64,
    /// Last seen sequence number.
    pub last_seq: Option<u32>,
    /// Inter-arrival jitter, median over the reporting window, ms (-1 = no samples).
    pub jitter_p50_ms: f64,
    /// Inter-arrival jitter, 95th percentile, ms (-1 = no samples).
    pub jitter_p95_ms: f64,
    /// Frames dropped from the KWS channel by drop-oldest (R9).
    pub kws_drops: u64,
    /// Connections accepted since process start.
    pub reconnects: u64,
    /// Last reported RSSI, dBm.
    pub rssi_dbm: Option<f32>,
    /// Monotonic connect marker, for the metric line's uptime field.
    pub connected_at: Option<Instant>,
}

/// One registered source: its ring, advertised capabilities, and live counters.
#[derive(Debug)]
pub struct SourceHandle {
    /// Source id (hello `source_id`).
    pub source_id: String,
    /// Room from hello.
    pub room: String,
    /// Group from `[[sources]]` when configured; `None` for an ad-hoc source admitted by
    /// `allow_unknown_sources` (S7.1).
    pub group: Option<String>,
    /// Capabilities from hello.
    pub caps: Caps,
    /// The authoritative ring, shared with capture and clip writers.
    pub ring: RingHandle,
    /// Stream health counters (S5.3).
    pub stats: Mutex<SourceStats>,
    /// Where the coordinator's LED and earcon messages for this source's socket are queued.
    /// `None` until the owning connection has finished its hello and installed it. Private:
    /// [`SourceRegistry::send_control`] is the only way in.
    downlink: Mutex<Option<mpsc::Sender<Vec<u8>>>>,
}

/// What [`SourceRegistry::send_control`] did with the bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlSend {
    /// On the owning connection's queue; it is written to the socket in order.
    Queued,
    /// No live connection owns the source (never connected, gone, or still mid-hello).
    NotConnected,
    /// The connection is not draining its queue.
    QueueFull,
}

/// How long a registration removed by [`SourceRegistry::unregister`] stays reachable through
/// [`SourceRegistry::recently_gone`] (docs/protocol.md §12).
///
/// This is for one narrow kind of caller: one that needs audio the source had **already**
/// delivered — a missed-wake or false-accept clip whose bytes are still sitting in that
/// source's ring — and which asks for the ring a moment *after* the event that justifies it.
/// The window only has to cover the scheduling gap between forwarding an event and the
/// coordinator asking for the ring, which is normally microseconds; five seconds is three
/// orders of magnitude of headroom and is not a cache of live state. Nothing may *write*
/// through it: [`SourceRegistry::recently_gone`] refuses while the id is still registered.
const RECENTLY_GONE_WINDOW: Duration = Duration::from_secs(5);

/// Bound on the retired list, so a flapping source cannot grow it; the oldest entry is
/// dropped past this many. Eight exceeds any plausible number of satellites reconnecting at
/// once, which is the only way entries accumulate.
const RECENTLY_GONE_MAX: usize = 8;

/// Frame arrival times per source, each queue tagged with the connection that sampled it
/// (I3). A named alias keeps the registry field readable.
type ArrivalQueues = HashMap<String, (Arc<SourceHandle>, VecDeque<Instant>)>;

/// Registry of live sources: `source_id` → ring + caps + stats (plan S1.4).
///
/// Shared as `Arc<SourceRegistry>` across ingest, capture and the coordinator.
#[derive(Debug, Default)]
pub struct SourceRegistry {
    inner: Mutex<HashMap<String, Arc<SourceHandle>>>,
    /// Registrations their owning connection retired, newest first, within
    /// [`RECENTLY_GONE_WINDOW`]. Deliberately **not** in `inner`: every other reader must
    /// keep seeing only live sources.
    retired: Mutex<VecDeque<(String, Arc<SourceHandle>, Instant)>>,
    /// Shared across all connections so the auth warn is throttled per peer IP, not per
    /// connection (R11, P3).
    warn_throttle: WarnThrottle,
    /// Frame arrival times per source, drained by [`SourceRegistry::maybe_log_metrics`]
    /// into the inter-arrival percentiles (S5.3). Each queue is tagged with the
    /// connection that sampled it (I3): the id alone cannot tell a live connection's
    /// cadence from a superseded handle reading through the same key after a reconnect.
    arrivals: Mutex<ArrivalQueues>,
    /// When each source's metric line was last emitted, so one line per source per
    /// [`METRIC_LOG_INTERVAL`] is the only thing that reaches the log (S5.3).
    last_metric: Mutex<HashMap<String, Instant>>,
}

/// Lock one of the registry's maps, recovering from a poison: a panicking connection task
/// must not take every other source's accounting down with it (same rule as
/// [`crate::ring::with_ring`]).
fn locked<T>(map: &Mutex<T>) -> MutexGuard<'_, T> {
    map.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Lock a source's counters with the same poison recovery.
fn stats(handle: &SourceHandle) -> MutexGuard<'_, SourceStats> {
    handle
        .stats
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl SourceRegistry {
    /// Empty registry bound to a config (source of the admission policy and ring sizing).
    pub fn new(cfg: &Config) -> Self {
        // The admission policy and ring sizing are read by [`serve`] / [`register`],
        // which receive the config; an empty registry holds no derived state.
        let _ = cfg;
        Self::default()
    }

    /// Register a source after a successful hello, creating its ring. Returns the handle
    /// the connection task writes frames through.
    ///
    /// Reconnecting with the same `source_id` replaces the entry and increments
    /// [`SourceStats::reconnects`].
    pub fn register(
        &self,
        hello: &Hello,
        group: Option<String>,
        ring_samples: usize,
    ) -> Arc<SourceHandle> {
        let id = hello.source_id.clone();
        let reconnects = locked(&self.inner)
            .get(&id)
            .map(|prev| stats(prev).reconnects + 1)
            .unwrap_or(0);
        let handle = Arc::new(SourceHandle {
            source_id: id.clone(),
            room: hello.room.clone(),
            group,
            caps: hello.caps,
            // A zero-capacity ring would panic inside `SourceRing::new`; one sample is
            // the honest minimum that keeps a misconfigured size non-fatal.
            ring: new_handle(ring_samples.max(1)),
            stats: Mutex::new(SourceStats {
                // `-1` is the documented "no samples yet" value for the percentiles.
                jitter_p50_ms: -1.0,
                jitter_p95_ms: -1.0,
                reconnects,
                connected_at: Some(Instant::now()),
                ..SourceStats::default()
            }),
            downlink: Mutex::new(None),
        });
        let mut inner = locked(&self.inner);
        inner.insert(id.clone(), Arc::clone(&handle));
        // A reconnect starts clean: stale arrival samples and metric cadence are dropped.
        // Same critical section as the insert, so a second connection that registers this
        // id while we are here cannot have its own fresh sampling removed by us.
        locked(&self.arrivals).remove(&id);
        locked(&self.last_metric).remove(&id);
        handle
    }

    /// Look up a live source.
    pub fn get(&self, source_id: &str) -> Option<Arc<SourceHandle>> {
        locked(&self.inner).get(source_id).cloned()
    }

    /// Queue encoded downlink bytes (an LED or earcon message) for the connection that
    /// currently owns `source_id`. Never blocks: the coordinator calls this from its own
    /// loop, and a satellite that is not reading must not stall it.
    ///
    /// Resolving the id here, not holding a handle, is what keeps a reconnect honest: the
    /// bytes go to whichever connection owns the source *now*.
    pub fn send_control(&self, source_id: &str, bytes: Vec<u8>) -> ControlSend {
        let Some(handle) = self.get(source_id) else {
            return ControlSend::NotConnected;
        };
        let tx = locked(&handle.downlink).clone();
        match tx.map(|tx| tx.try_send(bytes)) {
            Some(Ok(())) => ControlSend::Queued,
            Some(Err(mpsc::error::TrySendError::Full(_))) => ControlSend::QueueFull,
            // Closed: the owning task is already gone. None: it has not finished its hello.
            Some(Err(mpsc::error::TrySendError::Closed(_))) | None => ControlSend::NotConnected,
        }
    }

    /// Ids of all live sources (metric loop, debug).
    pub fn live_sources(&self) -> Vec<String> {
        let mut ids: Vec<String> = locked(&self.inner).keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Whether `handle` is still the connection registered for its `source_id` — asked, and
    /// answered **while holding the registry lock**, so that the caller can act on the answer.
    ///
    /// A reconnect *replaces* the entry (docs/protocol.md §12), so a connection can outlive
    /// its own registration and keep its socket and its task — the half-open socket of a
    /// satellite that has since reconnected. Such a connection owns nothing: it may not write
    /// the ring ([`SourceRegistry::record_frame`]), speak for the source (`UpMessage::Event`),
    /// tear the registration down, or announce the source gone. Every one of those is decided
    /// here, from the registry's current entry, rather than by whatever the connection happens
    /// to be holding.
    ///
    /// There is deliberately no "may I ask, then act" form of this. A check that releases the
    /// lock before the caller acts is a check a reconnect can answer in the gap: the id moves
    /// to a new connection while the superseded one goes on producing its effects — the ring,
    /// the seq-gap and jitter accounting, and the spotter's clock all take the consequence.
    /// `None` means the caller must end. [`Self::is_current`] is the one exception, and it is
    /// an exception in shape rather than in kind: it exists for a caller whose consequence is
    /// a *decision* with nothing mutated after it.
    ///
    /// The lock order taken here (`inner` → `stats` / `ring` / `arrivals`) is the order
    /// [`Self::register`] already takes; nothing acquires those in the opposite direction,
    /// so a caller may hold this guard while it writes the ring and hands frames to the
    /// channels. It must not call the `source_id`-keyed helpers (`record_kws_drop`,
    /// `record_rssi`, `get`) while holding it — they take the same lock; the
    /// `*_as_owner` variants exist for that.
    fn hold_ownership(
        &self,
        handle: &Arc<SourceHandle>,
    ) -> Option<MutexGuard<'_, HashMap<String, Arc<SourceHandle>>>> {
        let inner = locked(&self.inner);
        if matches!(inner.get(handle.source_id.as_str()), Some(live) if Arc::ptr_eq(live, handle)) {
            Some(inner)
        } else {
            None
        }
    }

    /// Whether `handle` is still the registration its `source_id` names: a **decision, not a
    /// claim**.
    ///
    /// Same comparison as [`Self::hold_ownership`], answered under the same lock and then
    /// released, which is legal for exactly one kind of caller — one that mutates nothing on
    /// the answer. That restriction is the whole reason this is not a reintroduction of the
    /// `owns_registration` helper that was deliberately deleted here: that one was a
    /// check-then-act, a question whose answer the caller carried into a mutation after the
    /// lock was gone, and a reconnect in the gap answered it wrongly. A decision has no gap:
    /// the only legal consequence is *stopping*. The one caller is a capture that has to know
    /// whether the connection owning its ring can still put a sample in it; when it cannot,
    /// the capture ends with what was already delivered, which is a decision about its own
    /// loop, not a mutation of the registry.
    ///
    /// Illegal uses, which must take [`Self::hold_ownership`] instead: gating a ring write,
    /// an event, a removal, or a `SourceGone` — each is a question followed by an action, and
    /// each has a real reconnect that lands in between.
    ///
    /// `false` is permanent and may be acted on for as long as the caller likes: a handle is
    /// made by one [`Self::register`] call, and the entry it names can only ever move to a
    /// newer one, never back. `true` is true only for the instant it is returned.
    pub fn is_current(&self, handle: &Arc<SourceHandle>) -> bool {
        matches!(
            locked(&self.inner).get(handle.source_id.as_str()),
            Some(live) if Arc::ptr_eq(live, handle)
        )
    }

    /// Record a decoded frame from the connection that still owns `handle`: ring write,
    /// seq-gap accounting, jitter sampling.
    ///
    /// Attribution is by connection, not by `source_id`. Keyed on the id alone, a
    /// superseded socket keeps feeding the ring of whichever connection took the id over,
    /// and the gap accounting charges the new owner for the old one's sequence — both
    /// connections restart `seq` at 0 ([`restart_marker`]), so the live source is blamed
    /// for a gap it never caused. There is one writer per source, and this is where it is
    /// enforced.
    ///
    /// Returns the ring's write cursor one past the frame — what the KWS channel's
    /// receiver needs to reconstruct spans, and the same domain [`SourceHandle::ring_range`]
    /// and [`SourceHandle::recover_missed`] read in. The ring is append-only, so this is
    /// the cursor, not `seq * FRAME_SAMPLES`: a sequence gap compresses rather than leaving
    /// a hole (see the note in the tests). `None` if this handle no longer owns the
    /// registration — never registered, superseded by a newer connection, or the newer one
    /// has already gone — which the connection treats as "end quietly".
    pub fn record_frame(&self, handle: &Arc<SourceHandle>, seq: u32, pcm: &[i16]) -> Option<u64> {
        let _owner = self.hold_ownership(handle)?;
        Some(self.write_frame(handle, seq, pcm))
    }

    /// The mutation half of [`Self::record_frame`], with ownership already decided **and the
    /// registry still locked on its behalf**.
    ///
    /// A connection that decides once and then acts several times — ring, seq-gap and
    /// jitter accounting, then the hand-off to the spotter and the capture broadcast —
    /// calls this under its own [`Self::hold_ownership`] guard, so no reconnect can slip in
    /// between the decision and the last of its effects. Everything else goes through
    /// [`Self::record_frame`], which takes the guard itself.
    fn write_frame(&self, handle: &Arc<SourceHandle>, seq: u32, pcm: &[i16]) -> u64 {
        {
            let mut st = stats(handle);
            if let Some(last) = st.last_seq {
                let expected = last.wrapping_add(1) as i64;
                let delta = i64::from(seq) - expected;
                if delta != 0 {
                    // Any non-`last+1` sequence is a gap (R7); only a forward skip loses
                    // audio — a late/duplicate frame loses none.
                    st.seq_gaps += 1;
                    st.gap_samples += delta.max(0) as u64 * proto::FRAME_SAMPLES as u64;
                }
            }
            st.last_seq = Some(seq);
            st.frames += 1;
        }
        let end = with_ring(&handle.ring, |ring| ring.write(pcm));
        let mut arrivals = locked(&self.arrivals);
        // Called under `hold_ownership`, so this handle is the entry's owner and the
        // queue it samples belongs to it (I3): the tag is set when the queue is created
        // and `update_jitter` honours it.
        let (_, queue) = arrivals
            .entry(handle.source_id.clone())
            .or_insert_with(|| (Arc::clone(handle), VecDeque::new()));
        queue.push_back(Instant::now());
        if queue.len() > JITTER_WINDOW_SAMPLES {
            let excess = queue.len() - JITTER_WINDOW_SAMPLES;
            queue.drain(..excess);
        }
        end
    }

    /// Count a KWS channel drop against the source this id names **right now** (R9), and
    /// against [`frame_owner`] when nobody holds that id any more.
    ///
    /// Keyed by id, deliberately, and not by connection: the channel is bounded and shared
    /// per source, so a drop is a property of that source's channel at that moment, and the
    /// audio that never reached the spotter belonged to that source whichever connection holds
    /// the id when the count is taken — which is precisely when the epilogue of a superseded
    /// connection takes it. The fallback is not a second opinion, it is the only way the
    /// count is never *lost*: if the newer connection has itself gone, there is no live entry
    /// to charge, and the alternative was for a frame that never reached the spotter to leave
    /// no trace anywhere. It is still the same visible counter, on the only handle that can
    /// still account for that audio.
    /// A caller that already holds the registry lock on a handle's behalf
    /// ([`Self::hold_ownership`]) uses [`Self::record_kws_drop_as_owner`] instead, because
    /// this one takes that same lock.
    pub fn record_kws_drop(&self, source_id: &str, frame_owner: &Arc<SourceHandle>) {
        let target = self.get(source_id).unwrap_or_else(|| frame_owner.clone());
        stats(&target).kws_drops += 1;
    }

    /// Count a KWS drop against the connection the frame belonged to, for a caller that has
    /// the registry locked on that handle's behalf and therefore already knows it is the
    /// current entry. Same visible counter as [`Self::record_kws_drop`]; takes no lock,
    /// because the caller holds it.
    fn record_kws_drop_as_owner(&self, handle: &Arc<SourceHandle>) {
        stats(handle).kws_drops += 1;
    }

    /// Record a signal reading for the metric line.
    ///
    /// Attributed to the connection that measured it, not to the id. A satellite that has
    /// since reconnected can still have a buffered `rssi` on its old socket, and that
    /// reading describes the radio as *that* socket saw it; writing it into the live
    /// source's stats would publish a signal level nobody measured (docs/protocol.md §12).
    /// Requires the caller to hold [`Self::hold_ownership`].
    fn record_rssi(&self, handle: &Arc<SourceHandle>, dbm: f32) {
        stats(handle).rssi_dbm = Some(dbm);
    }

    /// Remove a source by id, whatever is there, and retire the registration for
    /// [`RECENTLY_GONE_WINDOW`] so the audio it already delivered is still reachable — see
    /// [`SourceRegistry::recently_gone`].
    ///
    /// A connection that is ending must use [`Self::unregister_if_owner`] instead; this
    /// removes by name and so cannot tell whether the caller still owns the entry.
    pub fn unregister(&self, source_id: &str) -> Option<Arc<SourceHandle>> {
        let mut inner = locked(&self.inner);
        let removed = inner.remove(source_id);
        // The removal, the per-connection sampling and the retirement are one critical
        // section, and the guard is held across all three on purpose. Releasing it in
        // between lets a connection that registers this `source_id` in the gap have *its*
        // arrival samples and metric cadence deleted by the connection that is leaving, and
        // — if it also disconnects before we reach `retire` — its handle pushed behind the
        // one retired here, so [`Self::recently_gone`] would hand out the older ring.
        self.forget_per_connection(source_id);
        if let Some(handle) = removed.as_ref() {
            self.retire(source_id, handle);
        }
        removed
    }

    /// Remove this handle's registration **only if the registry still points at it**, with
    /// the test and the removal in one critical section, and retire it as
    /// [`Self::unregister`] does. Returns `true` only when it was this connection's own
    /// entry that went.
    ///
    /// Asking whether the handle owns the entry and *then* calling [`Self::unregister`] is
    /// not enough, and this is where it would matter most: a satellite can reconnect in the
    /// gap, and the dying connection then removes the *new* registration and announces a live
    /// source gone — the exact failure the ownership rule exists to prevent
    /// (docs/protocol.md §12).
    fn unregister_if_owner(&self, handle: &Arc<SourceHandle>) -> bool {
        let id = handle.source_id.as_str();
        let mut inner = locked(&self.inner);
        if !matches!(inner.get(id), Some(live) if Arc::ptr_eq(live, handle)) {
            return false;
        }
        let removed = inner.remove(id).expect("matched a moment ago");
        // Held across the retirement too — see [`Self::unregister`] for what the gap costs.
        self.forget_per_connection(id);
        self.retire(id, &removed);
        true
    }

    /// Drop the per-connection sampling kept for a source that no longer has a connection.
    fn forget_per_connection(&self, source_id: &str) {
        locked(&self.arrivals).remove(source_id);
        locked(&self.last_metric).remove(source_id);
    }

    /// Put a removed registration where [`Self::recently_gone`] can reach it. What arrives
    /// here was removed while it was the current entry, so its ring holds the last audio
    /// that source wrote.
    fn retire(&self, source_id: &str, handle: &Arc<SourceHandle>) {
        let mut retired = locked(&self.retired);
        retired.push_front((source_id.to_string(), Arc::clone(handle), Instant::now()));
        while retired.len() > RECENTLY_GONE_MAX {
            retired.pop_back();
        }
    }

    /// The handle a source had just before its own connection removed it, if that happened
    /// within [`RECENTLY_GONE_WINDOW`] and the id is **not** currently registered.
    ///
    /// For the one caller that cannot use [`SourceRegistry::get`] and must not lose data: the
    /// clip writers, which are asked to save audio a source already sent and can therefore
    /// arrive after the source itself has gone. Their alternative is a `warn!` and no clip,
    /// for bytes that are still in memory. Never use this to decide whether a source is
    /// present, to write a frame, or to attribute anything live — those must use `get`, and
    /// this returns `None` while the id is registered so nothing can write through a retired
    /// ring by accident.
    pub fn recently_gone(&self, source_id: &str) -> Option<Arc<SourceHandle>> {
        self.recently_gone_at(source_id, Instant::now())
    }

    /// [`SourceRegistry::recently_gone`] with the clock as an argument, so the window itself
    /// is testable (same reason [`SourceRegistry::maybe_log_metrics`] takes `now`).
    fn recently_gone_at(&self, source_id: &str, now: Instant) -> Option<Arc<SourceHandle>> {
        if self.get(source_id).is_some() {
            return None;
        }
        // Newest first, so the first match for an id is the most recent thing that served it.
        locked(&self.retired)
            .iter()
            .find(|(id, _, _)| id == source_id)
            .filter(|(_, _, at)| now.duration_since(*at) < RECENTLY_GONE_WINDOW)
            .map(|(_, handle, _)| Arc::clone(handle))
    }

    /// Emit one structured metric line per live source when the reporting window has
    /// elapsed (S5.3). Called from the connection task's timer, not a dedicated thread.
    pub fn maybe_log_metrics(&self, now: Instant) {
        let handles: Vec<Arc<SourceHandle>> = locked(&self.inner).values().cloned().collect();
        for handle in handles {
            let id = handle.source_id.clone();
            {
                let mut cadence = locked(&self.last_metric);
                if let Some(last) = cadence.get(&id) {
                    if now.duration_since(*last) < METRIC_LOG_INTERVAL {
                        continue;
                    }
                }
                cadence.insert(id.clone(), now);
            }
            let st = self.update_jitter(&handle, now);
            let uptime_s = handle
                .stats
                .lock()
                .ok()
                .and_then(|s| s.connected_at)
                .map(|at| now.saturating_duration_since(at).as_secs())
                .unwrap_or(0);
            tracing::info!(
                source = %id,
                room = %handle.room,
                group = ?handle.group,
                frames = st.frames,
                seq_gaps = st.seq_gaps,
                gap_samples = st.gap_samples,
                last_seq = ?st.last_seq,
                jitter_p50_ms = st.jitter_p50_ms,
                jitter_p95_ms = st.jitter_p95_ms,
                kws_drops = st.kws_drops,
                reconnects = st.reconnects,
                rssi_dbm = ?st.rssi_dbm,
                uptime_s,
                "source stream health"
            );
        }
    }

    /// The shared auth warn throttle.
    pub fn warn_throttle(&self) -> &WarnThrottle {
        &self.warn_throttle
    }

    /// Recompute a connection's inter-arrival percentiles over the reporting window, store
    /// them in **its** stats, and return the counters for the metric line. Takes the handle
    /// the caller is iterating rather than resolving one by `source_id`: a source that
    /// reconnected while this loop was running would otherwise get the old connection's
    /// percentiles written into the new connection's stats — overwriting its "no samples
    /// yet" sentinel — and a log line splicing one connection's room and uptime to the
    /// other's counters.
    fn update_jitter(&self, handle: &Arc<SourceHandle>, now: Instant) -> SourceStats {
        let gaps: Vec<f64> = {
            let mut arrivals = locked(&self.arrivals);
            match arrivals.get_mut(handle.source_id.as_str()) {
                // I3: consume the queue only when this handle is the connection that
                // sampled it. A handle superseded by a reconnect reads the same id-key
                // too, and without the tag its line would splice the new connection's
                // cadence into the dead connection's stats — and the side effect could
                // even suppress the new connection's own first metric line for a whole
                // reporting window.
                Some((owner, queue)) if Arc::ptr_eq(owner, handle) => {
                    // Keep only the reporting window; drop anything older unless it is the
                    // single remaining sample (which still anchors one delta).
                    while queue.len() > 1 {
                        match queue.front() {
                            Some(&first) if now.duration_since(first) > METRIC_LOG_INTERVAL => {
                                queue.pop_front();
                            }
                            _ => break,
                        }
                    }
                    let mut prev: Option<Instant> = None;
                    let mut out = Vec::with_capacity(queue.len().saturating_sub(1));
                    for &at in queue.iter() {
                        if let Some(prev_at) = prev {
                            out.push(at.saturating_duration_since(prev_at).as_secs_f64() * 1e3);
                        }
                        prev = Some(at);
                    }
                    out
                }
                // No queue at all, or a queue belonging to another connection:
                // this handle has no samples (I3).
                _ => return SourceStats::default(),
            }
        };
        let (p50, p95) = percentiles_ms(&gaps);
        let mut st = stats(handle);
        st.jitter_p50_ms = p50;
        st.jitter_p95_ms = p95;
        st.clone()
    }
}

/// Median and 95th percentile of `samples`, in ms; `-1.0` when there is nothing to
/// report (the documented no-samples value, so a metric line never reads as "0 ms jitter"
/// for a source that never sent two frames).
fn percentiles_ms(samples: &[f64]) -> (f64, f64) {
    if samples.is_empty() {
        return (-1.0, -1.0);
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let pick = |fraction: f64| -> f64 {
        let idx = ((sorted.len() as f64 - 1.0) * fraction).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    };
    (pick(0.5), pick(0.95))
}

/// Rate-limits hello-auth warnings to one per peer IP per
/// [`WARN_THROTTLE_INTERVAL`], across all connections (R11).
#[derive(Debug, Default)]
pub struct WarnThrottle {
    // Read by should_warn() once S1.4/R11 lands.
    #[allow(dead_code)]
    last: Mutex<HashMap<IpAddr, Instant>>,
}

impl WarnThrottle {
    /// A fresh throttle.
    pub fn new() -> Self {
        Self::default()
    }

    /// `true` if a warning for `peer` should be emitted now (and records the emission).
    ///
    /// A burst of failed handshakes from one host must produce one line, not hundreds.
    /// Stale entries are pruned here.
    pub fn should_warn(&self, peer: IpAddr, now: Instant) -> bool {
        let mut last = self
            .last
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if last.len() > THROTTLE_PRUNE_THRESHOLD {
            last.retain(|_, at| {
                now.duration_since(*at) < WARN_THROTTLE_INTERVAL * THROTTLE_PRUNE_HORIZON
            });
        }
        match last.get(&peer) {
            Some(at) if now.duration_since(*at) < WARN_THROTTLE_INTERVAL => false,
            _ => {
                last.insert(peer, now);
                true
            }
        }
    }
}

/// Hello authentication decision (R11).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthFailure {
    /// `server.auth_token` is set but the hello carried no token.
    MissingToken,
    /// A token was present but did not match.
    TokenMismatch,
}

/// Check a hello against the configured shared secret.
///
/// `expected` is `server.auth_token`. When it is `None`, every hello is accepted (the
/// caller logs one loud startup warning instead — R11). The comparison goes through
/// [`proto::constant_time_eq`] so timing cannot leak the secret.
pub fn check_hello_auth(hello: &Hello, expected: Option<&str>) -> Result<(), AuthFailure> {
    let Some(expected) = expected else {
        // No secret configured: every hello is accepted and `serve` logs the one loud
        // startup warning instead (R11).
        return Ok(());
    };
    match hello.token.as_deref() {
        None => Err(AuthFailure::MissingToken),
        Some(token) => {
            if proto::constant_time_eq(token.as_bytes(), expected.as_bytes()) {
                Ok(())
            } else {
                Err(AuthFailure::TokenMismatch)
            }
        }
    }
}

/// Accept satellite connections and drive the pipeline (plan S1.4).
///
/// Binds `cfg.server.listen` and, per connection:
/// - decodes hello ([`proto::FrameDecoder`] / [`proto::decode_hello_line`]);
/// - enforces [`check_hello_auth`] and the `allow_unknown_sources` policy;
/// - for each audio frame: [`SourceRegistry::record_frame`] — which accepts a frame only
///   from the connection that owns the source's entry — then send
///   `(source_id, Arc<[i16]>)` on `kws_tx` (bounded, drop-oldest + counter) and the same
///   tuple on the frame broadcast for capture;
/// - for each event: send [`IngestEvent::Event`] on `event_tx`;
/// - on disconnect: unregister the source and send [`IngestEvent::SourceGone`] — but only
///   from the connection that still owns the entry. A connection superseded by a newer one
///   for the same `source_id` (docs/protocol.md §12 replaces the entry) ends without
///   touching either, because the source is still connected on the newer socket and its
///   entry belongs to that connection; such an exit is logged, never silent. Either way a
///   frame the connection was still holding back is counted first (R9), whichever way the
///   read loop ended;
/// - a connection that puts nothing on the socket for [`READ_IDLE_DEADLINE`] is closed: a
///   satellite that vanished without a FIN would otherwise hold a task and an fd here
///   forever, since nothing else in this path ever discovers it.
///
/// Frames from sources named by `kws_suppress` are still written to the ring (suppressed
/// sources keep filling their buffers — plan invariant) and still handed to the KWS
/// thread: the thread owns suppression (it starves the engine but advances its sample
/// clock with them — KWS-1, because the ring advanced for those frames too).
/// Never blocks on DSP or network I/O beyond the socket read.
pub async fn serve(
    cfg: &Config,
    registry: Arc<SourceRegistry>,
    kws_tx: crossbeam_channel::Sender<(String, Arc<[i16]>)>,
    frame_rx: broadcast::Sender<(String, Arc<[i16]>)>,
    event_tx: mpsc::Sender<IngestEvent>,
    kws_suppress: watch::Receiver<HashSet<String>>,
) -> anyhow::Result<()> {
    if cfg.server.auth_token.as_deref().unwrap_or("").is_empty() {
        // R11: one loud line at startup, not one per connection.
        tracing::warn!(
            "server.auth_token is NOT set: any satellite that can reach this port will be \
             accepted without a hello token — set the shared secret before exposing this \
             port beyond the household network"
        );
    }
    let listener = TcpListener::bind(&cfg.server.listen)
        .await
        .map_err(|e| anyhow::anyhow!("listening on {}: {e}", cfg.server.listen))?;
    serve_on(
        listener,
        cfg,
        registry,
        kws_tx,
        frame_rx,
        event_tx,
        kws_suppress,
    )
    .await
}

/// [`serve`]'s accept loop without the bind: the caller supplies the listening socket, so
/// it can hand in `127.0.0.1:0` and learn the port instead of racing a fixed one.
///
/// Same admission, registration and fan-out as [`serve`], and the same lifetime shape: it
/// accepts until `accept` fails or the task is cancelled, with no drain and no graceful
/// shutdown. Cancelling it aborts the in-flight connection tasks too (they are owned by a
/// `JoinSet` here), which is what lets the KWS channel close. `pub` so
/// `tests/listener.rs` can drive it over real sockets; [`serve`] stays the entry point.
pub async fn serve_on(
    listener: TcpListener,
    cfg: &Config,
    registry: Arc<SourceRegistry>,
    kws_tx: crossbeam_channel::Sender<(String, Arc<[i16]>)>,
    frame_rx: broadcast::Sender<(String, Arc<[i16]>)>,
    event_tx: mpsc::Sender<IngestEvent>,
    // Kept in the signature for the frozen `serve`/`serve_on` seam, but no longer read
    // here: filtering suppressed sources at the socket was KWS-1's clock break — the
    // ring advanced for them while the spotter's clock did not. The KWS thread owns
    // suppression, and it needs to see those frames to advance its clock with them.
    _kws_suppress: watch::Receiver<HashSet<String>>,
) -> anyhow::Result<()> {
    let policy = Arc::new(Policy::of(cfg));
    // The connection tasks live in this set, so dropping this future — which is how
    // `run_service` stops ingest on Ctrl-C — aborts every one of them. Spawned loose,
    // they outlived the accept loop, each still holding a clone of `kws_tx`, so the KWS
    // thread could never see its channel close and the shutdown drain always ran out
    // its grace period and logged an error.
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let ctx = Arc::new(ConnCtx {
                    policy: Arc::clone(&policy),
                    registry: Arc::clone(&registry),
                    kws_tx: kws_tx.clone(),
                    frame_tx: frame_rx.clone(),
                    event_tx: event_tx.clone(),
                });
                tracing::debug!(peer = %peer, "satellite connection accepted");
                connections.spawn(async move {
                    if let Err(e) = connection(stream, peer.ip(), ctx, READ_IDLE_DEADLINE).await {
                        tracing::debug!(error = %e, "connection ended");
                    }
                });
            }
            // Reap finished connections so the set does not grow for the life of the
            // service. With nothing in the set this branch is disabled, not busy.
            Some(_) = connections.join_next() => {}
        }
    }
}

/// Everything a connection task needs from the config, read once at startup (S1.4/S7.1).
#[derive(Debug, Clone, Default)]
struct Policy {
    /// `server.auth_token`; `None`/empty accepts every hello (R11).
    auth_token: Option<String>,
    /// `server.allow_unknown_sources` (S7.1's switch).
    allow_unknown_sources: bool,
    /// Configured `source_id` → its group. Absent ⇒ unknown source.
    known: HashMap<String, Option<String>>,
    /// Ring depth in samples: `capture.hard_cap_s` of audio, which the plan equates with
    /// the ring depth.
    ring_samples: usize,
}

impl Policy {
    fn of(cfg: &Config) -> Self {
        Self {
            auth_token: cfg.server.auth_token.clone().filter(|t| !t.is_empty()),
            allow_unknown_sources: cfg.server.allow_unknown_sources,
            known: cfg
                .sources
                .iter()
                .map(|s| (s.id.clone(), Some(s.group.clone())))
                .collect(),
            ring_samples: crate::ms_to_samples(cfg.capture.hard_cap_s.saturating_mul(1000)).max(1)
                as usize,
        }
    }
}

/// Cloned per connection: the shared halves of the fan-out.
struct ConnCtx {
    policy: Arc<Policy>,
    registry: Arc<SourceRegistry>,
    kws_tx: crossbeam_channel::Sender<(String, Arc<[i16]>)>,
    frame_tx: broadcast::Sender<(String, Arc<[i16]>)>,
    event_tx: mpsc::Sender<IngestEvent>,
}

/// One connection: hello → policy → decoded frames/events, until EOF, a latched protocol
/// error, or `read_deadline` of silence.
///
/// `read_deadline` is a parameter rather than a use of [`READ_IDLE_DEADLINE`] so the
/// bounded reap is reachable by a test; the accept loop passes the production value.
async fn connection(
    mut stream: TcpStream,
    peer: IpAddr,
    ctx: Arc<ConnCtx>,
    read_deadline: Duration,
) -> anyhow::Result<()> {
    let _ = stream.set_nodelay(true);
    let mut decoder = FrameDecoder::new();
    let mut buf = [0u8; READ_CHUNK];
    let mut source: Option<Arc<SourceHandle>> = None;
    let mut hold = KwsHold { pending: None };
    // The downlink: the coordinator's LED and earcon messages for this satellite arrive here
    // through [`SourceRegistry::send_control`] and are written to the socket by this task,
    // the only owner of it. This function keeps its own sender alive so the receiver never
    // reports "closed" before the hello installs a clone in the handle.
    let (downlink_tx, mut downlink_rx) = mpsc::channel::<Vec<u8>>(DOWNLINK_QUEUE);
    // The idle deadline is an instant that only a *read* moves. Wrapping each read in a
    // timeout would restart it whenever a control was written, and a peer that vanished
    // without a FIN would then never be reaped while the backend kept talking to it.
    let mut idle_at = tokio::time::Instant::now() + read_deadline;

    // Labeled because a frame that arrives after this connection was superseded must leave
    // the *read* loop, not merely the decode loop it is nested in: every way of ending a
    // connection goes through the one epilogue below.
    'read: loop {
        // Bounded, not endless: see [`READ_IDLE_DEADLINE`] for why silence from a peer
        // that is supposed to have a 30 s heartbeat means the peer is gone.
        let n = tokio::select! {
            read = stream.read(&mut buf) => {
                idle_at = tokio::time::Instant::now() + read_deadline;
                read.map_err(|e| anyhow::anyhow!("reading from {peer}: {e}"))?
            }
            queued = downlink_rx.recv() => {
                if let Some(bytes) = queued {
                    // A satellite that cannot take a few dozen bytes within the bound is
                    // not reading its socket; closing is the only honest answer, and a
                    // partial write would leave the stream mid-frame anyway.
                    match tokio::time::timeout(DOWNLINK_WRITE_TIMEOUT, stream.write_all(&bytes)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            tracing::warn!(peer = %peer, error = %e, "downlink write failed: closing");
                            break 'read;
                        }
                        Err(_elapsed) => {
                            tracing::warn!(peer = %peer, "downlink write timed out: closing");
                            break 'read;
                        }
                    }
                }
                continue 'read;
            }
            () = tokio::time::sleep_until(idle_at) => {
                // A satellite that lost its network without a FIN lands here. The epilogue
                // below does the rest, so this exit is subject to the same ownership rules
                // as every other: it may not unregister a source that a newer connection
                // now owns.
                tracing::warn!(
                    peer = %peer,
                    idle_secs = read_deadline.as_secs(),
                    "no byte from this satellite within its idle deadline: closing \
                     (a connected satellite emits an rssi event every 30 s)"
                );
                break 'read;
            }
        };
        if n == 0 {
            break; // EOF: the satellite closed.
        }
        // §12's "closed when it next sends *anything*", at the byte level. The gates below
        // decide per decoded message, so a peer that trickles bytes which never complete a
        // frame or an event would keep its task and its socket alive indefinitely — every
        // successful read restarts the idle deadline, so the reap could never reach it. This
        // asks the same question as `hold_ownership` and takes no action on it beyond leaving
        // the loop (the one epilogue below still runs), which is what makes it legal here and
        // not a check-then-act. Asked only once this connection has registered: before the
        // hello there is no registration to be superseded *out of*, and an unregistered peer
        // must not be closed for not being current. One `inner` lock per successful read adds
        // no new cost class — the frame path already takes that lock once per frame.
        if let Some(handle) = source.as_ref() {
            if !ctx.registry.is_current(handle) {
                break 'read;
            }
        }
        for message in decoder.feed(&buf[..n]) {
            match message {
                UpMessage::Hello(hello) => {
                    if let Err(reason) = check_hello_auth(&hello, ctx.policy.auth_token.as_deref())
                    {
                        // R11/P3: a misconfigured fleet must not fill the log; one line
                        // per peer IP per second, from the map shared across connections.
                        if ctx
                            .registry
                            .warn_throttle()
                            .should_warn(peer, Instant::now())
                        {
                            tracing::warn!(
                                peer = %peer,
                                source = %hello.source_id,
                                reason = ?reason,
                                "hello rejected"
                            );
                        }
                        return Ok(());
                    }
                    let configured = ctx.policy.known.get(&hello.source_id).cloned().flatten();
                    if configured.is_none() && !ctx.policy.allow_unknown_sources {
                        tracing::warn!(
                            peer = %peer,
                            source = %hello.source_id,
                            "unknown source_id and server.allow_unknown_sources is false: closing \
                             (add it under [[sources]] or enable the switch — S7.1)"
                        );
                        return Ok(());
                    }
                    tracing::info!(
                        source = %hello.source_id,
                        room = %hello.room,
                        group = ?configured,
                        fw = %hello.fw,
                        caps = ?hello.caps,
                        "source registered"
                    );
                    let handle = ctx
                        .registry
                        .register(&hello, configured, ctx.policy.ring_samples);
                    // R-A: this registration replaced the ring, so its write cursor is
                    // back at 0 while the KWS thread's span clock is not. Tell it to
                    // rebase before any frame of this connection is counted. The marker
                    // is a correctness message, not best-effort audio (I1): a registered
                    // but unrebased source has every wake refused by the wake path's
                    // domain check until some later registration happens to deliver one,
                    // which is the silent-deafness failure this path must not have. A
                    // full channel therefore unwinds the registration and closes the
                    // connection: the satellite retries with its backoff after the
                    // backlog drains, and no source is left registered-and-unrebased.
                    if ctx
                        .kws_tx
                        .try_send(restart_marker(&hello.source_id))
                        .is_err()
                    {
                        tracing::warn!(
                            peer = %peer,
                            source = %hello.source_id,
                            "KWS channel full: restart marker not accepted, closing this \
                             connection instead of registering a source whose span clock \
                             still counts the previous connection (R-A)"
                        );
                        ctx.registry.unregister_if_owner(&handle);
                        send_event(&ctx, IngestEvent::SourceGone { source: hello.source_id.clone() });
                        return Ok(());
                    }
                    // From here the coordinator can reach this socket: the sender goes
                    // into the registered handle, and `send_control` resolves the live
                    // owner of the id through the registry, so a superseded connection's
                    // queue is never the one written to.
                    *locked(&handle.downlink) = Some(downlink_tx.clone());
                    source = Some(handle);
                }
                UpMessage::Frame(frame) => {
                    let Some(handle) = source.as_ref() else {
                        // Unreachable: `FrameDecoder` emits nothing before the hello.
                        return Ok(());
                    };
                    let id = handle.source_id.clone();
                    // Decide once, then do all of it under that one decision: the ring write
                    // and the seq-gap/jitter accounting, and the hand-off to the spotter and
                    // the capture broadcast. Deciding and *then* acting let a reconnect land
                    // in the gap, after which this connection's frame was counted against the
                    // new owner and advanced the spotter's clock for a source that had just
                    // been rebased — audio it never sent (docs/protocol.md §12, one writer
                    // per source).
                    let Some(_owner) = ctx.registry.hold_ownership(handle) else {
                        // This connection no longer owns the registration: a newer one has
                        // taken the id, or the one that took it has already gone. Ending is
                        // right; the epilogue decides what the registry is told, which is
                        // nothing — this connection has no claim on the source's entry, and
                        // announcing it gone would be a lie about a source that is still
                        // connected on another socket.
                        break 'read;
                    };
                    ctx.registry.write_frame(handle, frame.seq, &frame.pcm);
                    let pcm: Arc<[i16]> = Arc::from(frame.pcm.as_slice());
                    // Every frame goes to the spotter thread, suppressed sources
                    // included (KWS-1): the ring just advanced for this audio, so the
                    // thread must advance its span clock with it — suppressing at the
                    // socket left the clock behind the write cursor and misplaced every
                    // later wake. Starving the engine is the thread's job (S5.2).
                    push_kws(&ctx, &mut hold, handle, (id.clone(), Arc::clone(&pcm)));
                    // The capture broadcast is best-effort by design: a `Lagged`
                    // subscriber re-reads the span from the ring ([`recover_missed`]).
                    let _ = ctx.frame_tx.send((id.clone(), pcm));
                }
                UpMessage::Event(event) => {
                    let Some(handle) = source.as_ref() else {
                        return Ok(());
                    };
                    let id = handle.source_id.clone();
                    // The same rule as audio — and this arm had no gate at all, so a
                    // superseded socket was free to speak for the source: its stale `rssi`
                    // overwrote what the live connection had measured, a ghost
                    // `playback_done` could end the live session's Speaking state, and
                    // because any inbound byte resets the read deadline, a peer that only
                    // ever sends its 30 s heartbeat was never reaped. §12 says a replaced
                    // connection is closed when it next sends *anything*; this is that
                    // sentence, and it is what makes the reap bounded for such a peer.
                    let Some(_owner) = ctx.registry.hold_ownership(handle) else {
                        break 'read;
                    };
                    if let Event::Rssi { dbm } = &event {
                        ctx.registry.record_rssi(handle, *dbm);
                    }
                    send_event(&ctx, IngestEvent::Event { source: id, event });
                }
            }
        }
        if let Some(err) = decoder.take_error() {
            // The first error latches (contract choice 10): close once, with the reason.
            tracing::warn!(peer = %peer, error = %err, "protocol error: closing connection");
            break;
        }
        ctx.registry.maybe_log_metrics(Instant::now());
    }

    // One epilogue for every way the read loop can end — EOF, a latched protocol error, or
    // a frame that arrived after this connection was superseded — so that no exit path can
    // skip the accounting below.
    if let Some(handle) = source {
        let id = handle.source_id.clone();
        // A frame this connection still owned when the satellite went away never reached
        // the spotter; the metric line must account for it (R9). This is accounted per
        // source_id, not per connection: the audio was that source's, whichever connection
        // holds the entry now, and counting it is the only way it is ever seen. When the id
        // has no entry any more — the newer connection went too — the count falls back to
        // this handle rather than disappearing.
        if hold.pending.take().is_some() {
            ctx.registry.record_kws_drop(&id, &handle);
        }
        // One critical section, not a question followed by an action: asking whether this
        // connection owns the id and *then* removing it let a reconnect land between the
        // two, and the dying connection would have removed the new registration and
        // announced a live source gone.
        if ctx.registry.unregister_if_owner(&handle) {
            // KWS-5: the same zero-length tuple that marks a restart also marks a
            // stream's death, so the spotter thread evicts its per-source state
            // (`SherpaEngine::reset` drops the stream) instead of carrying a native
            // stream for every ad-hoc id ever seen. Best-effort: a full channel here
            // only postpones the eviction to the id's next (mandatory, I1) registration
            // marker — the correctness path is never the audio path.
            let _ = ctx.kws_tx.try_send(restart_marker(&id));
            send_event(&ctx, IngestEvent::SourceGone { source: id });
        } else {
            // Superseded. `taken_over` distinguishes the two shapes of it: a newer
            // connection holds the id right now, or the newer connection has itself gone
            // and nobody holds it. Either way this connection may not touch the registry
            // (docs/protocol.md §12 gives the entry to the newest connection), and until
            // now it could close without a line of evidence anywhere — which is the one
            // failure mode in here an operator could not diagnose after the fact.
            let taken_over = ctx.registry.get(&id).is_some();
            tracing::warn!(
                peer = %peer,
                source = %id,
                taken_over,
                "closing a connection that no longer owns its source_id: a newer connection \
                 for this source took the registration"
            );
        }
    }
    Ok(())
}

/// Hand a frame to the KWS thread without ever blocking ingest.
///
/// Called by the frame path with [`SourceRegistry::hold_ownership`] held on `handle`, which
/// is why a dropped frame is counted against that connection
/// ([`SourceRegistry::record_kws_drop_as_owner`]) rather than re-resolved by id: under the
/// guard the two are the same thing, and re-resolving would take a lock the caller holds.
/// The epilogue's count for a frame this connection never handed on is the one that *resolves*
/// by id ([`SourceRegistry::record_kws_drop`]), because by then the id belongs to someone else
/// and the audio still belongs to the source; it takes this handle as a fallback so the count
/// survives the case where the id has no entry left to charge.
///
/// The channel is bounded and shared by every connection. crossbeam gives a producer no
/// way to evict what is already queued — `serve` is handed the [`crossbeam_channel::Sender`]
/// only — so the oldest frame ingest still controls is the one that goes: the frame this
/// connection was holding back. The newest audio survives, because that is the frame that
/// can still contain a wake (R9), and every frame that never reaches the spotter is
/// counted.
///
/// Returns the number of frames discarded.
fn push_kws(
    ctx: &ConnCtx,
    hold: &mut KwsHold,
    handle: &Arc<SourceHandle>,
    frame: (String, Arc<[i16]>),
) -> u64 {
    let mut dropped: u64 = 0;
    if ctx.kws_tx.is_full() {
        // Behind on a full channel: keep the newest frame, discard the older held one.
        if hold.pending.replace(frame).is_some() {
            dropped += 1;
        }
    } else {
        // I2 (shape A): the held frame may be flushed only when the channel can take
        // *both* it and the new frame. With a single free slot, sending the held frame
        // first hands that slot to the older audio and counts the newest frame — the
        // one that can still contain a wake — lost (R9). With no room for both, this
        // connection sheds its own older frame instead: drop-oldest.
        let room_for_both = ctx
            .kws_tx
            .capacity()
            .is_none_or(|cap| ctx.kws_tx.len() + 2 <= cap);
        if let Some(held) = hold.pending.take() {
            if room_for_both {
                if ctx.kws_tx.try_send(held).is_err() {
                    dropped += 1; // raced another connection's producer
                }
            } else {
                dropped += 1;
            }
        }
        if let Err(e) = ctx.kws_tx.try_send(frame) {
            // Raced full again: the newest frame stays under this connection's
            // control instead of being lost here.
            hold.pending = Some(e.into_inner());
        }
    }
    for _ in 0..dropped {
        ctx.registry.record_kws_drop_as_owner(handle);
    }
    dropped
}

/// The one frame this connection could not hand on, and therefore still owns.
struct KwsHold {
    pending: Option<(String, Arc<[i16]>)>,
}

/// The KWS thread's stream-restart marker (R-A): a zero-length frame for `source_id`.
///
/// [`crate::kws::run_kws_thread`] reads it as "this source's ring was just replaced;
/// rebase its span clock onto the new write cursor", never as audio — the sample domain
/// every span in the backend is defined against is the ring's ([`SourceRegistry::record_frame`]),
/// and a reconnect moves that origin back to 0 while the spotter's clock keeps running.
fn restart_marker(source_id: &str) -> (String, Arc<[i16]>) {
    (source_id.to_owned(), Arc::from(Vec::<i16>::new()))
}

/// Hand an event to the coordinator without ever blocking ingest: the queue is bounded,
/// and a stalled coordinator must not back-pressure the socket.
fn send_event(ctx: &ConnCtx, event: IngestEvent) {
    if let Err(e) = ctx.event_tx.try_send(event) {
        tracing::warn!(error = %e, "coordinator event queue full or closed: dropping event");
    }
}

/// Re-read the sample span a `broadcast::error::Lagged` capture subscriber missed
/// (S1.4): the frame broadcast is a convenience, [`crate::ring::SourceRing`] is the
/// authoritative store.
///
/// `missed` is the absolute sample-index span the subscriber failed to observe; the
/// result is exactly that span, clamped to what was ever written, with samples older than
/// the retained window zero-filled (contract choice 2 — "the satellite never sent that"
/// is a benign answer, not an error).
pub fn recover_missed(handle: &SourceHandle, missed: Range<u64>) -> Vec<i16> {
    with_ring(&handle.ring, |ring| ring.range(missed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{encode_event, encode_hello_line, PROTOCOL_VERSION};
    use std::net::SocketAddr;
    use tokio::io::AsyncWriteExt;

    const SECRET: &str = "household-shared-secret";

    /// A valid config with the two knobs these tests care about left explicit.
    fn cfg_with(auth: Option<&str>, allow_unknown: bool, sources: &[(&str, &str, &str)]) -> Config {
        let mut text = String::from("[server]\nlisten = \"127.0.0.1:10799\"\n");
        if let Some(token) = auth {
            text.push_str(&format!("auth_token = \"{token}\"\n"));
        }
        text.push_str(&format!("allow_unknown_sources = {allow_unknown}\n"));
        text.push_str(
            "\n[ha]\nurl = \"ws://ha.example.lan:8123\"\ntoken = \"ha\"\npipeline_id = \"p\"\n\
             \n[parakeet]\naddr = \"asr.example.lan:10200\"\n\
             \n[kws]\nmodel = \"/models/kws\"\nkeywords_file = \"/models/keywords.txt\"\n\
             \n[clips]\ndir = \"/tmp/listenrr-clips\"\n",
        );
        for (id, room, group) in sources {
            text.push_str(&format!(
                "\n[[sources]]\nid = \"{id}\"\nroom = \"{room}\"\ngroup = \"{group}\"\n"
            ));
        }
        Config::from_toml_str(&text).expect("the test config is valid")
    }

    fn hello(source: &str, room: &str, token: Option<&str>) -> Hello {
        Hello {
            v: PROTOCOL_VERSION,
            source_id: source.to_string(),
            room: room.to_string(),
            token: token.map(str::to_string),
            caps: Caps::default(),
            fw: "0.1.0".to_string(),
        }
    }

    /// One on-socket audio record: `seq` + a recognisable constant frame.
    fn record(seq: u32, value: i16) -> Vec<u8> {
        proto::encode_audio_frame(&proto::AudioFrame {
            seq,
            pcm: vec![value; proto::FRAME_SAMPLES],
        })
    }

    /// Poll `cond` for up to ~2 s: the connection task only becomes visible once the
    /// runtime schedules it.
    async fn settled<F: FnMut() -> bool>(mut cond: F) {
        for _ in 0..200 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not reached within 2 s");
    }

    /// Everything the spotter channel currently holds, markers included.
    fn kws_items(h: &Harness) -> Vec<(String, Vec<i16>)> {
        h.kws_rx
            .try_iter()
            .map(|(id, pcm)| (id, pcm.to_vec()))
            .collect()
    }

    /// Everything the spotter channel currently holds that is **audio**. Registration sends
    /// a [`restart_marker`] per connection (R-A) — a zero-length tuple that is not audio —
    /// so counting items on this channel is not the same as counting frames. The marker
    /// itself is what [`a_registration_sends_the_kws_stream_restart_marker`] asserts.
    fn kws_audio(h: &Harness) -> Vec<(String, Vec<i16>)> {
        kws_items(h)
            .into_iter()
            .filter(|(_, pcm)| !pcm.is_empty())
            .collect()
    }

    /// Drain until `want` audio frames have arrived (the connection task is only visible
    /// once the runtime schedules it, so this polls like [`settled`]).
    async fn kws_audio_until(h: &Harness, want: usize) -> Vec<(String, Vec<i16>)> {
        let mut audio: Vec<(String, Vec<i16>)> = Vec::new();
        for _ in 0..200 {
            audio.extend(kws_audio(h));
            if audio.len() >= want {
                return audio;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("only {} of {want} frames reached the spotter", audio.len());
    }

    /// As [`kws_audio_until`], counting markers too.
    async fn kws_items_until(h: &Harness, want: usize) -> Vec<(String, Vec<i16>)> {
        let mut items: Vec<(String, Vec<i16>)> = Vec::new();
        for _ in 0..200 {
            items.extend(kws_items(h));
            if items.len() >= want {
                return items;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "only {} of {want} items reached the spotter channel",
            items.len()
        );
    }

    /// The whole ingest pipeline behind a real loopback listener, with every sink held by
    /// the caller so nothing is closed early.
    struct Harness {
        addr: SocketAddr,
        registry: Arc<SourceRegistry>,
        kws_rx: crossbeam_channel::Receiver<(String, Arc<[i16]>)>,
        frame_rx: broadcast::Receiver<(String, Arc<[i16]>)>,
        event_rx: mpsc::Receiver<IngestEvent>,
        suppress: watch::Sender<HashSet<String>>,
    }

    async fn harness(cfg: Config, kws_capacity: usize) -> Harness {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let registry = Arc::new(SourceRegistry::new(&cfg));
        let (kws_tx, kws_rx) = crossbeam_channel::bounded(kws_capacity);
        let (frame_tx, frame_rx) = broadcast::channel(FRAME_BROADCAST_CAPACITY);
        let (event_tx, event_rx) = mpsc::channel(EVENT_QUEUE_CAPACITY);
        let (suppress, suppress_rx) = watch::channel(HashSet::new());
        let serving = Arc::clone(&registry);
        tokio::spawn(async move {
            let _ = serve_on(
                listener,
                &cfg,
                serving,
                kws_tx,
                frame_tx,
                event_tx,
                suppress_rx,
            )
            .await;
        });
        Harness {
            addr,
            registry,
            kws_rx,
            frame_rx,
            event_rx,
            suppress,
        }
    }

    /// Connect and speak the hello line.
    async fn connect(h: &Harness, source: &str, room: &str, token: Option<&str>) -> TcpStream {
        let mut stream = TcpStream::connect(h.addr).await.unwrap();
        stream
            .write_all(&encode_hello_line(&hello(source, room, token)))
            .await
            .unwrap();
        stream
    }

    async fn send_frames(stream: &mut TcpStream, count: u32, value: i16) {
        for seq in 0..count {
            stream.write_all(&record(seq, value)).await.unwrap();
        }
        stream.flush().await.unwrap();
    }

    fn frames_seen(registry: &SourceRegistry, source: &str) -> u64 {
        registry.get(source).map(|h| stats(&h).frames).unwrap_or(0)
    }

    /// `true` once the server has closed the connection (a read returns EOF).
    async fn closed(stream: &mut TcpStream) -> bool {
        let mut buf = [0u8; 1];
        matches!(stream.read(&mut buf).await, Ok(0))
    }

    #[test]
    fn hello_auth_accepts_the_matching_token_and_rejects_everything_else() {
        let good = hello("kitchen", "kitchen", Some(SECRET));
        assert_eq!(check_hello_auth(&good, Some(SECRET)), Ok(()));
        assert_eq!(
            check_hello_auth(&hello("kitchen", "kitchen", Some("guess")), Some(SECRET)),
            Err(AuthFailure::TokenMismatch)
        );
        let absent = hello("kitchen", "kitchen", None);
        assert_eq!(
            check_hello_auth(&absent, Some(SECRET)),
            Err(AuthFailure::MissingToken)
        );
        // R11: no secret configured ⇒ every hello is accepted; `serve` logs the warning.
        assert_eq!(check_hello_auth(&absent, None), Ok(()));
        assert_eq!(check_hello_auth(&good, None), Ok(()));
    }

    #[test]
    fn warn_throttle_allows_one_line_per_second_per_peer() {
        let throttle = WarnThrottle::new();
        let t0 = Instant::now();
        let a: IpAddr = "192.168.1.11".parse().unwrap();
        let b: IpAddr = "192.168.1.12".parse().unwrap();
        assert!(
            throttle.should_warn(a, t0),
            "the first rejection is reported"
        );
        assert!(!throttle.should_warn(a, t0 + Duration::from_millis(500)));
        assert!(!throttle.should_warn(a, t0 + Duration::from_millis(999)));
        assert!(
            throttle.should_warn(b, t0),
            "another peer is not the same peer"
        );
        assert!(!throttle.should_warn(b, t0 + Duration::from_millis(500)));
        assert!(
            throttle.should_warn(a, t0 + WARN_THROTTLE_INTERVAL),
            "the window is measured from the last emission, not from a fixed epoch"
        );
    }

    #[test]
    fn record_frame_writes_the_ring_and_counts_a_sequence_gap() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let registry = SourceRegistry::new(&cfg);
        let handle = registry.register(
            &hello("kitchen", "kitchen", Some(SECRET)),
            Some("kitchen".to_string()),
            64 * proto::FRAME_SAMPLES,
        );
        // A handle whose `source_id` is no longer registered is refused rather than a
        // panic: this is exactly what a connection holds once a newer connection has taken
        // its id, or after the one that took it has gone.
        let ghost = registry.register(
            &hello("ghost", "ghost", Some(SECRET)),
            None,
            64 * proto::FRAME_SAMPLES,
        );
        registry.unregister("ghost");
        assert_eq!(
            registry.record_frame(&ghost, 0, &[0i16; proto::FRAME_SAMPLES]),
            None,
            "a handle that owns nothing writes nothing, and does not panic"
        );
        assert_eq!(
            stats(&ghost).frames,
            0,
            "and the refusal is a refusal: nothing was counted against it"
        );
        assert_eq!(
            registry.record_frame(&handle, 0, &[7i16; proto::FRAME_SAMPLES]),
            Some(proto::FRAME_SAMPLES as u64)
        );
        assert_eq!(
            registry.record_frame(&handle, 1, &[7i16; proto::FRAME_SAMPLES]),
            Some(2 * proto::FRAME_SAMPLES as u64)
        );
        // Frames 2..4 never arrived: one gap, three frames of audio lost (R7). The frozen
        // [`RingBuffer`] API is append-only — there is no seek-to-`seq` write — so the
        // ring stays contiguous across a gap and this frame lands right after the last
        // one that arrived. `record_frame`'s return value is therefore the ring's write
        // cursor, not `seq * FRAME_SAMPLES`; spans must be read in that domain.
        assert_eq!(
            registry.record_frame(&handle, 5, &[7i16; proto::FRAME_SAMPLES]),
            Some(3 * proto::FRAME_SAMPLES as u64)
        );
        {
            let st = stats(&handle);
            assert_eq!(st.frames, 3);
            assert_eq!(st.seq_gaps, 1);
            assert_eq!(st.gap_samples, 3 * proto::FRAME_SAMPLES as u64);
            assert_eq!(st.last_seq, Some(5));
            assert_eq!(st.kws_drops, 0);
            assert_eq!(st.rssi_dbm, None);
            assert_eq!(
                st.jitter_p50_ms, -1.0,
                "the percentiles report the no-samples sentinel until a window runs"
            );
        }
        assert_eq!(
            with_ring(&handle.ring, |r| r.range(2 * 1024..3 * 1024)),
            vec![7i16; proto::FRAME_SAMPLES],
            "the ring is what was written, appended: the third frame landed at the cursor, \
             not at `seq * FRAME_SAMPLES`"
        );
        assert_eq!(registry.live_sources(), vec!["kitchen".to_string()]);
        registry.record_rssi(&handle, -62.5);
        registry.record_kws_drop("kitchen", &handle);
        {
            let st = stats(&handle);
            assert_eq!(st.rssi_dbm, Some(-62.5));
            assert_eq!(st.kws_drops, 1);
        }
        // A reconnect replaces the ring (fresh sequence space) and counts itself.
        let again = registry.register(
            &hello("kitchen", "kitchen", Some(SECRET)),
            Some("kitchen".to_string()),
            64 * proto::FRAME_SAMPLES,
        );
        assert_eq!(stats(&again).reconnects, 1);
        assert_eq!(with_ring(&again.ring, |r| r.written()), 0);
        assert!(registry.unregister("kitchen").is_some());
        assert!(registry.unregister("kitchen").is_none());
        assert!(registry.live_sources().is_empty());
    }

    /// The attribution rule itself: only the connection that currently owns a `source_id`
    /// may write that source's ring. Keyed on the id alone, a superseded socket keeps
    /// feeding the new owner's buffer — and since every new connection restarts `seq` at 0,
    /// the new owner is also blamed for a gap its ghost caused.
    #[test]
    fn only_the_connection_that_owns_a_source_id_may_write_its_ring() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let registry = SourceRegistry::new(&cfg);
        let ghost = registry.register(
            &hello("kitchen", "kitchen", Some(SECRET)),
            Some("kitchen".to_string()),
            64 * proto::FRAME_SAMPLES,
        );
        assert_eq!(
            registry.record_frame(&ghost, 0, &[1i16; proto::FRAME_SAMPLES]),
            Some(proto::FRAME_SAMPLES as u64)
        );
        let live = registry.register(
            &hello("kitchen", "kitchen", Some(SECRET)),
            Some("kitchen".to_string()),
            64 * proto::FRAME_SAMPLES,
        );
        assert!(Arc::ptr_eq(
            &registry
                .get("kitchen")
                .expect("the reconnect is the registered one"),
            &live
        ));

        // The ghost is still streaming: nothing has reaped its socket, and its sequence
        // starts at 0 as any connection's does.
        assert_eq!(
            registry.record_frame(&ghost, 0, &[9i16; proto::FRAME_SAMPLES]),
            None,
            "the ghost's frame is refused rather than appended to the live source's ring"
        );
        assert!(
            registry.hold_ownership(&ghost).is_none(),
            "the registry says what it means: this handle owns nothing"
        );
        assert!(registry.hold_ownership(&live).is_some());
        assert_eq!(
            stats(&ghost).frames,
            1,
            "a refused frame is not counted against the connection that sent it, either"
        );

        assert_eq!(
            registry.record_frame(&live, 0, &[2i16; proto::FRAME_SAMPLES]),
            Some(proto::FRAME_SAMPLES as u64)
        );
        {
            let st = stats(&live);
            assert_eq!(st.frames, 1);
            assert_eq!(
                st.seq_gaps, 0,
                "the live source is not charged a gap for a sequence it never saw"
            );
        }
        assert_eq!(
            with_ring(&live.ring, |r| r.range(0..proto::FRAME_SAMPLES as u64)),
            vec![2i16; proto::FRAME_SAMPLES],
            "the live ring holds the owner's audio and nothing else"
        );
        assert_eq!(
            with_ring(&ghost.ring, |r| r.range(0..proto::FRAME_SAMPLES as u64)),
            vec![1i16; proto::FRAME_SAMPLES],
            "and the ghost's own ring stops where its ownership did"
        );
    }

    /// What the idle deadline buys. A peer that never sends a FIN — the satellite that
    /// walked out of Wi-Fi range — is still reaped: the task ends of its own accord, its
    /// registration is released, the coordinator is told, and the peer sees EOF. Without
    /// the bound, nothing in here would ever notice it.
    #[tokio::test]
    async fn a_peer_that_goes_silent_is_reaped_and_its_source_released() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let registry = Arc::new(SourceRegistry::new(&cfg));
        let (kws_tx, _kws_rx) = crossbeam_channel::bounded(KWS_CHANNEL_CAPACITY);
        let (frame_tx, _frame_rx) = broadcast::channel(FRAME_BROADCAST_CAPACITY);
        let (event_tx, mut event_rx) = mpsc::channel(EVENT_QUEUE_CAPACITY);
        let ctx = Arc::new(ConnCtx {
            policy: Arc::new(Policy::of(&cfg)),
            registry: Arc::clone(&registry),
            kws_tx,
            frame_tx,
            event_tx,
        });

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (server, peer) = listener.accept().await.unwrap();
        // The hello is already sitting in the socket buffer when the task starts, so the
        // only thing left for it to wait for is the silence that never ends.
        client
            .write_all(&encode_hello_line(&hello(
                "kitchen",
                "kitchen",
                Some(SECRET),
            )))
            .await
            .unwrap();
        let task = tokio::spawn(connection(
            server,
            peer.ip(),
            ctx,
            Duration::from_millis(50),
        ));

        task.await
            .expect("the connection task ends on its own")
            .expect("and ends cleanly, not with an error");
        assert!(
            registry.get("kitchen").is_none(),
            "the reap released the registration, so the task and the fd it held are gone"
        );
        assert_eq!(
            event_rx.recv().await,
            Some(IngestEvent::SourceGone {
                source: "kitchen".to_string()
            }),
            "this source really did go, so the coordinator is told: the ownership guard \
             withholds this from a superseded connection only, never from the owner"
        );
        let mut probe = [0u8; 1];
        assert_eq!(
            client.read(&mut probe).await.unwrap(),
            0,
            "and the peer is told too: the server closed its side"
        );
    }

    /// Read exactly `len` bytes the server wrote to this client, or fail the test.
    async fn read_exact_within(stream: &mut TcpStream, len: usize) -> Vec<u8> {
        let mut got = vec![0u8; len];
        tokio::time::timeout(Duration::from_secs(2), stream.read_exact(&mut got))
            .await
            .expect("the control reached the socket")
            .unwrap();
        got
    }

    /// The downlink the lights depend on. Before it existed the coordinator encoded every LED
    /// message and dropped it, so no satellite ever received one.
    #[tokio::test]
    async fn controls_queued_for_a_registered_source_reach_its_socket_in_order() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let h = harness(cfg, KWS_CHANNEL_CAPACITY).await;
        let mut stream = connect(&h, "kitchen", "kitchen", Some(SECRET)).await;
        send_frames(&mut stream, 1, 5).await;
        settled(|| frames_seen(&h.registry, "kitchen") == 1).await;

        let listening = proto::encode_control(&proto::Control::Led {
            state: proto::LedState::Listening,
        });
        let speaking = proto::encode_control(&proto::Control::Led {
            state: proto::LedState::Speaking,
        });
        assert_eq!(
            h.registry.send_control("kitchen", listening.clone()),
            ControlSend::Queued
        );
        assert_eq!(
            h.registry.send_control("kitchen", speaking.clone()),
            ControlSend::Queued
        );
        assert_eq!(
            read_exact_within(&mut stream, listening.len()).await,
            listening
        );
        assert_eq!(
            read_exact_within(&mut stream, speaking.len()).await,
            speaking,
            "in the order the coordinator sent them"
        );
        // The uplink still works after a write: the loop returned to reading.
        send_frames(&mut stream, 2, 6).await;
        settled(|| frames_seen(&h.registry, "kitchen") == 3).await;
    }

    /// AC7c: the stop frame is a real downlink, not a coordinator-only event. A `Play` and
    /// the `Stop` that interrupts it must reach the tablet's socket in that order, and the
    /// stop record must be the bytes the tablet's handler is written against — a control the
    /// backend can only describe in prose would not end anything.
    #[tokio::test]
    async fn a_stop_reaches_the_socket_after_the_play_it_interrupts() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let h = harness(cfg, KWS_CHANNEL_CAPACITY).await;
        let mut stream = connect(&h, "kitchen", "kitchen", Some(SECRET)).await;
        send_frames(&mut stream, 1, 5).await;
        settled(|| frames_seen(&h.registry, "kitchen") == 1).await;

        let play = proto::encode_control(&proto::Control::Play {
            url: "http://ha.example.lan:8123/api/tts_proxy/abc".into(),
            format: "flac".into(),
        });
        let stop = proto::encode_control(&proto::Control::Stop);
        assert_eq!(h.registry.send_control("kitchen", play.clone()), ControlSend::Queued);
        assert_eq!(h.registry.send_control("kitchen", stop.clone()), ControlSend::Queued);
        assert_eq!(read_exact_within(&mut stream, play.len()).await, play);
        assert_eq!(
            read_exact_within(&mut stream, stop.len()).await,
            stop,
            "the stop arrives second: it interrupts the play, it does not precede it"
        );
        // The payload itself, not just whatever the encoder produced: the tablet matches on
        // this exact type tag.
        let stop_json = r#"{"type":"stop"}"#;
        assert_eq!(&stop[3..], stop_json.as_bytes());
        // The uplink still works after the stop: the write loop returned to reading.
        send_frames(&mut stream, 2, 6).await;
        settled(|| frames_seen(&h.registry, "kitchen") == 3).await;
    }

    #[tokio::test]
    async fn a_control_for_a_source_that_is_not_connected_is_reported_not_queued() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let mut h = harness(cfg, KWS_CHANNEL_CAPACITY).await;
        let bytes = proto::encode_control(&proto::Control::Led {
            state: proto::LedState::Idle,
        });
        assert_eq!(
            h.registry.send_control("kitchen", bytes.clone()),
            ControlSend::NotConnected,
            "declared in config, but nothing has connected"
        );
        let mut stream = connect(&h, "kitchen", "kitchen", Some(SECRET)).await;
        send_frames(&mut stream, 1, 5).await;
        settled(|| frames_seen(&h.registry, "kitchen") == 1).await;
        drop(stream);
        let _ = tokio::time::timeout(Duration::from_secs(1), h.event_rx.recv()).await;
        settled(|| h.registry.get("kitchen").is_none()).await;
        assert_eq!(
            h.registry.send_control("kitchen", bytes),
            ControlSend::NotConnected,
            "and again once it has gone"
        );
    }

    /// docs/protocol.md §12: a reconnect replaces the registration. A control sent after it
    /// must reach the new socket, not the superseded one.
    #[tokio::test]
    async fn a_control_goes_to_the_connection_that_owns_the_source_now() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let h = harness(cfg, KWS_CHANNEL_CAPACITY).await;
        let mut first = connect(&h, "kitchen", "kitchen", Some(SECRET)).await;
        send_frames(&mut first, 1, 1).await;
        settled(|| frames_seen(&h.registry, "kitchen") == 1).await;
        let mut second = connect(&h, "kitchen", "kitchen", Some(SECRET)).await;
        send_frames(&mut second, 1, 2).await;
        settled(|| {
            h.registry
                .get("kitchen")
                .map(|hd| stats(&hd).reconnects == 1)
                .unwrap_or(false)
        })
        .await;

        let led = proto::encode_control(&proto::Control::Led {
            state: proto::LedState::Speaking,
        });
        assert_eq!(
            h.registry.send_control("kitchen", led.clone()),
            ControlSend::Queued
        );
        assert_eq!(read_exact_within(&mut second, led.len()).await, led);
    }

    /// The reap is bounded only if the *peer's* silence is what is measured. A backend that
    /// keeps writing controls to a satellite that has vanished must not keep it alive.
    #[tokio::test]
    async fn writing_controls_does_not_extend_the_idle_deadline() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let registry = Arc::new(SourceRegistry::new(&cfg));
        let (kws_tx, _kws_rx) = crossbeam_channel::bounded(KWS_CHANNEL_CAPACITY);
        let (frame_tx, _frame_rx) = broadcast::channel(FRAME_BROADCAST_CAPACITY);
        let (event_tx, _event_rx) = mpsc::channel(EVENT_QUEUE_CAPACITY);
        let ctx = Arc::new(ConnCtx {
            policy: Arc::new(Policy::of(&cfg)),
            registry: Arc::clone(&registry),
            kws_tx,
            frame_tx,
            event_tx,
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (server, peer) = listener.accept().await.unwrap();
        client
            .write_all(&encode_hello_line(&hello("kitchen", "kitchen", Some(SECRET))))
            .await
            .unwrap();
        let task = tokio::spawn(connection(
            server,
            peer.ip(),
            ctx,
            Duration::from_millis(300),
        ));
        settled(|| registry.get("kitchen").is_some()).await;

        // Talk to the silent peer every 50 ms, four times the deadline's worth.
        let talker = {
            let registry = Arc::clone(&registry);
            tokio::spawn(async move {
                let bytes = proto::encode_control(&proto::Control::Led {
                    state: proto::LedState::Idle,
                });
                for _ in 0..24 {
                    let _ = registry.send_control("kitchen", bytes.clone());
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
        };
        let ended = tokio::time::timeout(Duration::from_millis(900), task).await;
        talker.abort();
        assert!(
            ended.is_ok(),
            "the connection was reaped on the peer's silence, not kept alive by our writes"
        );
    }

    /// The shutdown contract `run_service` depends on: stopping ingest must close the KWS
    /// channel, or the spotter thread never ends and the drain times out. A live satellite
    /// connection is exactly what used to hold it open.
    #[tokio::test]
    async fn cancelling_serve_aborts_live_connections_and_closes_the_kws_channel() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let registry = Arc::new(SourceRegistry::new(&cfg));
        let (kws_tx, kws_rx) = crossbeam_channel::bounded(KWS_CHANNEL_CAPACITY);
        let (frame_tx, _frame_rx) = broadcast::channel(FRAME_BROADCAST_CAPACITY);
        let (event_tx, _event_rx) = mpsc::channel(EVENT_QUEUE_CAPACITY);
        let (_suppress, suppress_rx) = watch::channel(HashSet::new());
        let serving = tokio::spawn(async move {
            let _ = serve_on(
                listener,
                &cfg,
                registry,
                kws_tx,
                frame_tx,
                event_tx,
                suppress_rx,
            )
            .await;
        });

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(&encode_hello_line(&hello("kitchen", "kitchen", Some(SECRET))))
            .await
            .unwrap();
        send_frames(&mut stream, 1, 5).await;
        // The connection is live and has handed the spotter its restart marker + frame.
        let _ = tokio::task::spawn_blocking({
            let kws_rx = kws_rx.clone();
            move || kws_rx.recv_timeout(Duration::from_secs(2))
        })
        .await
        .unwrap()
        .expect("the live connection reached the spotter channel");

        serving.abort();
        let _ = serving.await;
        // Drain what is queued; once every sender is gone the channel reports Disconnected
        // rather than Empty. Bounded: the aborted tasks need a scheduler turn to drop.
        let mut closed = false;
        for _ in 0..200 {
            match kws_rx.try_recv() {
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    closed = true;
                    break;
                }
                Ok(_) | Err(crossbeam_channel::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
        assert!(
            closed,
            "a live connection kept a KWS sender alive after ingest was cancelled"
        );
    }

    /// A frame whose first sample identifies its sequence number.
    fn kws_frame(seq: u32) -> (String, Arc<[i16]>) {
        (
            "kitchen".to_string(),
            Arc::from(vec![seq as i16; proto::FRAME_SAMPLES]),
        )
    }

    /// Drain the channel, returning the first sample of each frame that arrived.
    fn drained(rx: &crossbeam_channel::Receiver<(String, Arc<[i16]>)>) -> Vec<i16> {
        let mut out = Vec::new();
        while let Ok((_, pcm)) = rx.try_recv() {
            out.push(pcm[0]);
        }
        out
    }

    #[test]
    fn kws_channel_stays_bounded_and_counts_every_frame_that_never_arrives() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let registry = Arc::new(SourceRegistry::new(&cfg));
        registry.register(&hello("kitchen", "kitchen", Some(SECRET)), None, 1024);
        let (kws_tx, kws_rx) = crossbeam_channel::bounded(2);
        let (frame_tx, _frame_rx) = broadcast::channel(4);
        let (event_tx, _event_rx) = mpsc::channel(4);
        let ctx = ConnCtx {
            policy: Arc::new(Policy::default()),
            registry: Arc::clone(&registry),
            kws_tx,
            frame_tx,
            event_tx,
        };
        let drops = || stats(&registry.get("kitchen").unwrap()).kws_drops;
        let mut hold = KwsHold { pending: None };
        // Drop-counting is by connection, so the test hands `push_kws` the handle the
        // registry is actually holding — the same thing the frame path passes it.
        let handle = registry.get("kitchen").expect("kitchen is registered");

        assert_eq!(push_kws(&ctx, &mut hold, &handle, kws_frame(0)), 0);
        assert_eq!(push_kws(&ctx, &mut hold, &handle, kws_frame(1)), 0);
        // The channel is full: frame 2 is held back rather than blocking the socket read.
        assert_eq!(
            push_kws(&ctx, &mut hold, &handle, kws_frame(2)),
            0,
            "held, not dropped"
        );
        assert_eq!(drops(), 0, "holding is not a drop yet");
        // A newer frame on a full channel discards the older held one: the newest audio
        // is the one that can still contain a wake (R9).
        assert_eq!(push_kws(&ctx, &mut hold, &handle, kws_frame(3)), 1);
        assert_eq!(drops(), 1);
        assert_eq!(drained(&kws_rx), vec![0i16, 1], "ingest never blocked");

        // Space appearing flushes the held frame *ahead* of the new one: order survives.
        assert_eq!(push_kws(&ctx, &mut hold, &handle, kws_frame(4)), 0);
        assert_eq!(drained(&kws_rx), vec![3i16, 4]);
        assert_eq!(drops(), 1, "exactly frame 2 never reached the spotter");
    }

    /// The exact shape of I2: with a held frame and exactly one free slot, the older
    /// held frame is the shed one. Before the fix the free slot went to the held frame
    /// and the newest frame — the one that can still contain a wake — was the counted
    /// loss (R9 inverted).
    #[test]
    fn kws_overflow_sheds_the_older_held_frame_when_one_slot_is_free() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let registry = Arc::new(SourceRegistry::new(&cfg));
        registry.register(&hello("kitchen", "kitchen", Some(SECRET)), None, 1024);
        let (kws_tx, kws_rx) = crossbeam_channel::bounded(2);
        let (frame_tx, _frame_rx) = broadcast::channel(4);
        let (event_tx, _event_rx) = mpsc::channel(4);
        let ctx = ConnCtx {
            policy: Arc::new(Policy::default()),
            registry: Arc::clone(&registry),
            kws_tx,
            frame_tx,
            event_tx,
        };
        let drops = || stats(&registry.get("kitchen").unwrap()).kws_drops;
        let mut hold = KwsHold { pending: None };
        let handle = registry.get("kitchen").expect("kitchen is registered");

        assert_eq!(push_kws(&ctx, &mut hold, &handle, kws_frame(0)), 0);
        assert_eq!(push_kws(&ctx, &mut hold, &handle, kws_frame(1)), 0);
        // Full: frame 2 is held, then frame 3 replaces it and 2 is counted.
        assert_eq!(push_kws(&ctx, &mut hold, &handle, kws_frame(2)), 0);
        assert_eq!(push_kws(&ctx, &mut hold, &handle, kws_frame(3)), 1);
        assert_eq!(
            kws_rx.try_recv().unwrap().1[0],
            0,
            "the consumer frees exactly one slot"
        );
        assert_eq!(
            push_kws(&ctx, &mut hold, &handle, kws_frame(4)),
            1,
            "the older held frame is the shed one, not the new frame"
        );
        assert_eq!(drops(), 2, "frames 2 and 3 are counted; frame 4 is not");
        assert_eq!(
            drained(&kws_rx),
            vec![1i16, 4],
            "the newest frame survives the overflow"
        );
    }

    #[test]
    fn lagged_subscriber_reads_its_missed_span_back_out_of_the_ring() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let registry = SourceRegistry::new(&cfg);
        // A 2-frame ring: anything older than the newest 2048 samples is evicted.
        let handle = registry.register(
            &hello("kitchen", "kitchen", Some(SECRET)),
            None,
            2 * proto::FRAME_SAMPLES,
        );
        for seq in 0..4u32 {
            registry.record_frame(&handle, seq, &vec![seq as i16 + 1; proto::FRAME_SAMPLES]);
        }
        // The span a subscriber is still caught up on comes back byte-exact.
        let fresh = recover_missed(&handle, 2048..4096);
        assert_eq!(fresh.len(), 2048);
        assert!(fresh[..1024].iter().all(|&s| s == 3));
        assert!(fresh[1024..].iter().all(|&s| s == 4));
        // Evicted samples read back as zeros, never-written samples clamp, and a bad
        // range is empty rather than a panic (contract choices 2 and 3).
        assert_eq!(
            recover_missed(&handle, 0..1024),
            vec![0i16; proto::FRAME_SAMPLES]
        );
        assert!(recover_missed(&handle, 4096..8192).is_empty());
        // False positive: the reversed range is the input under test — a bad range must
        // read back empty rather than panic (contract choice 3).
        #[allow(clippy::reversed_empty_ranges)]
        {
            assert!(recover_missed(&handle, 3000..2000).is_empty());
        }
    }

    #[test]
    fn metric_line_reports_jitter_percentiles_once_per_window_per_source() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let registry = SourceRegistry::new(&cfg);
        let handle = registry.register(&hello("kitchen", "kitchen", Some(SECRET)), None, 1024);
        // Script a window of arrivals at the exact 64 ms frame cadence: the inter-arrival
        // percentiles are what the metric line must carry (S5.3).
        let base = Instant::now();
        let stamps: Vec<Instant> = (0..50)
            .map(|i| base + Duration::from_millis(64 * i))
            .collect();
        locked(&registry.arrivals)
            .insert("kitchen".to_string(), (Arc::clone(&handle), VecDeque::from(stamps.clone())));
        registry.maybe_log_metrics(*stamps.last().unwrap());
        assert!(
            (stats(&handle).jitter_p50_ms - 64.0).abs() < 0.01,
            "the median inter-arrival is the frame cadence"
        );
        assert!(
            (stats(&handle).jitter_p95_ms - 64.0).abs() < 0.01,
            "a steady stream has no jitter tail"
        );
        // Inside the window nothing recomputes: one line per source per minute.
        {
            let mut st = stats(&handle);
            st.jitter_p50_ms = -1.0;
            st.jitter_p95_ms = -1.0;
        }
        registry.maybe_log_metrics(*stamps.last().unwrap() + Duration::from_secs(59));
        assert_eq!(
            stats(&handle).jitter_p50_ms,
            -1.0,
            "the second call inside the window is silent"
        );
        // The next window reports again, from its own samples.
        let later = base + METRIC_LOG_INTERVAL;
        let stamps: Vec<Instant> = (0..50)
            .map(|i| later + Duration::from_millis(64 * i))
            .collect();
        locked(&registry.arrivals)
            .insert("kitchen".to_string(), (Arc::clone(&handle), VecDeque::from(stamps.clone())));
        registry.maybe_log_metrics(*stamps.last().unwrap());
        assert!(
            (stats(&handle).jitter_p50_ms - 64.0).abs() < 0.01,
            "the following window reports again"
        );
    }

    /// I3: a superseded handle must not consume the new connection's arrival queue.
    /// Before the queue carried its owner, `update_jitter` resolved by id and wrote the
    /// live connection's cadence into the dead handle's stats.
    #[test]
    fn a_superseded_handle_never_reads_the_new_connections_jitter() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let registry = SourceRegistry::new(&cfg);
        let stale = registry.register(&hello("kitchen", "kitchen", Some(SECRET)), None, 1024);
        let live = registry.register(&hello("kitchen", "kitchen", Some(SECRET)), None, 1024);
        let base = Instant::now();
        let stamps: Vec<Instant> = (0..50)
            .map(|i| base + Duration::from_millis(64 * i))
            .collect();
        let last = *stamps.last().unwrap();
        locked(&registry.arrivals)
            .insert("kitchen".to_string(), (Arc::clone(&live), VecDeque::from(stamps)));
        registry.update_jitter(&stale, last);
        assert_eq!(
            stats(&stale).jitter_p50_ms,
            -1.0,
            "the superseded handle keeps the no-samples sentinel (I3)"
        );
        assert_eq!(stats(&stale).jitter_p95_ms, -1.0);
        let st_live = registry.update_jitter(&live, last);
        assert!(
            (st_live.jitter_p50_ms - 64.0).abs() < 0.01,
            "the sampling connection still gets its own cadence"
        );
    }

    #[tokio::test]
    async fn accepted_source_frames_reach_the_ring_the_spotter_and_the_broadcast() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let mut h = harness(cfg, KWS_CHANNEL_CAPACITY).await;
        let mut stream = connect(&h, "kitchen", "kitchen", Some(SECRET)).await;
        send_frames(&mut stream, 2, 11).await;
        settled(|| frames_seen(&h.registry, "kitchen") == 2).await;

        let handle = h
            .registry
            .get("kitchen")
            .expect("hello registered the source");
        assert_eq!(handle.source_id, "kitchen");
        assert_eq!(handle.room, "kitchen");
        assert_eq!(handle.group.as_deref(), Some("kitchen"));
        assert_eq!(stats(&handle).last_seq, Some(1));
        assert!(
            with_ring(&handle.ring, |r| r.range(0..2048))
                .iter()
                .all(|&s| s == 11),
            "the ring holds the audio exactly as sent"
        );

        // Both frames reached the KWS thread (the R-A restart marker registration sent is
        // not audio and is not counted here — see `a_registration_rebases_the_spotter_clock`)…
        let audio = kws_audio_until(&h, 2).await;
        for (id, pcm) in &audio {
            assert_eq!(id, "kitchen");
            assert_eq!(pcm.len(), proto::FRAME_SAMPLES);
        }
        // …and to the capture broadcast.
        for i in 0..2 {
            let (id, _) = tokio::time::timeout(Duration::from_secs(1), h.frame_rx.recv())
                .await
                .unwrap_or_else(|_| panic!("frame {i} never reached the broadcast"))
                .unwrap();
            assert_eq!(id, "kitchen");
        }

        // Events go up to the coordinator, RSSI is recorded, and the disconnect is reported.
        stream
            .write_all(&encode_event(&Event::SaveBuffer))
            .await
            .unwrap();
        stream
            .write_all(&encode_event(&Event::Rssi { dbm: -62.5 }))
            .await
            .unwrap();
        let first = tokio::time::timeout(Duration::from_secs(1), h.event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            first,
            IngestEvent::Event {
                source: "kitchen".to_string(),
                event: Event::SaveBuffer
            }
        );
        let second = tokio::time::timeout(Duration::from_secs(1), h.event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            second,
            IngestEvent::Event {
                source: "kitchen".to_string(),
                event: Event::Rssi { dbm: -62.5 }
            }
        );
        settled(|| stats(&handle).rssi_dbm == Some(-62.5)).await;

        drop(stream);
        let gone = tokio::time::timeout(Duration::from_secs(1), h.event_rx.recv())
            .await
            .expect("the disconnect was reported")
            .unwrap();
        assert_eq!(
            gone,
            IngestEvent::SourceGone {
                source: "kitchen".to_string()
            }
        );
        assert!(
            h.registry.get("kitchen").is_none(),
            "the source is unregistered when its connection ends"
        );
    }

    #[tokio::test]
    async fn wrong_or_missing_token_closes_the_connection_and_the_right_one_still_works() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let h = harness(cfg, KWS_CHANNEL_CAPACITY).await;

        let mut mismatch = connect(&h, "kitchen", "kitchen", Some("guessed")).await;
        assert!(closed(&mut mismatch).await, "a mismatched token is closed");
        let mut missing = connect(&h, "kitchen", "kitchen", None).await;
        assert!(
            closed(&mut missing).await,
            "a missing token is a rejection, not a warning"
        );
        assert!(
            h.registry.live_sources().is_empty(),
            "a rejected hello never registers a source"
        );

        let mut good = connect(&h, "kitchen", "kitchen", Some(SECRET)).await;
        send_frames(&mut good, 1, 5).await;
        settled(|| frames_seen(&h.registry, "kitchen") == 1).await;
        assert_eq!(stats(&h.registry.get("kitchen").unwrap()).reconnects, 0);
    }

    #[tokio::test]
    async fn without_a_configured_token_any_hello_is_accepted() {
        let cfg = cfg_with(None, false, &[("kitchen", "kitchen", "kitchen")]);
        let h = harness(cfg, KWS_CHANNEL_CAPACITY).await;
        let mut stream = connect(&h, "kitchen", "kitchen", None).await;
        send_frames(&mut stream, 1, 4).await;
        settled(|| frames_seen(&h.registry, "kitchen") == 1).await;
        // A token it was never asked for is harmless too, and re-registering the same
        // id replaces the entry while counting the reconnect.
        let mut tokened = connect(&h, "kitchen", "kitchen", Some("whatever")).await;
        send_frames(&mut tokened, 1, 4).await;
        settled(|| {
            h.registry.get("kitchen").map(|hd| {
                let st = stats(&hd);
                (st.frames, st.reconnects)
            }) == Some((1, 1))
        })
        .await;
    }

    #[tokio::test]
    async fn unknown_source_is_closed_unless_the_switch_is_on() {
        let strict = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let h = harness(strict, KWS_CHANNEL_CAPACITY).await;
        let mut stranger = connect(&h, "attic", "attic", Some(SECRET)).await;
        assert!(
            closed(&mut stranger).await,
            "an unknown source_id is closed while the switch is off"
        );
        assert!(h.registry.live_sources().is_empty());
        // The token is still required: a VPN overlay does not replace the check (S7.1).
        let mut untokened = connect(&h, "attic", "attic", None).await;
        assert!(closed(&mut untokened).await);

        let permissive = cfg_with(Some(SECRET), true, &[("kitchen", "kitchen", "kitchen")]);
        let h2 = harness(permissive, KWS_CHANNEL_CAPACITY).await;
        let mut admitted = connect(&h2, "attic", "attic", Some(SECRET)).await;
        send_frames(&mut admitted, 1, 3).await;
        settled(|| frames_seen(&h2.registry, "attic") == 1).await;
        let handle = h2.registry.get("attic").expect("admitted by the switch");
        assert_eq!(handle.group, None, "an ad-hoc source has no group (S7.1)");
        assert_eq!(handle.room, "attic");
        assert_eq!(h2.registry.live_sources(), vec!["attic".to_string()]);
    }

    /// KWS-1: suppression is the KWS thread's job, not the socket's. Filtering here
    /// advanced the ring without the thread's span clock, and every wake after a
    /// suppression episode (every answered turn) read a window this much too early.
    /// The frames must reach the channel; the thread's own tests (`src/kws.rs`) pin the
    /// clock advance and the engine starvation.
    #[tokio::test]
    async fn a_suppressed_source_fills_its_ring_and_still_hands_frames_to_the_thread() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let h = harness(cfg, KWS_CHANNEL_CAPACITY).await;
        h.suppress
            .send(HashSet::from(["kitchen".to_string()]))
            .expect("the harness holds the receiver");

        let mut stream = connect(&h, "kitchen", "kitchen", Some(SECRET)).await;
        send_frames(&mut stream, 3, 9).await;
        settled(|| frames_seen(&h.registry, "kitchen") == 3).await;

        let handle = h.registry.get("kitchen").unwrap();
        assert_eq!(
            with_ring(&handle.ring, |r| r.written()),
            3 * proto::FRAME_SAMPLES as u64,
            "suppressed sources keep filling their rings (plan invariant)"
        );
        // The frames reach the thread rather than being filtered at the socket: the ring
        // advanced for them, so the thread must see them to advance its clock with them.
        let audio = kws_audio_until(&h, 3).await;
        assert_eq!(audio.len(), 3, "suppressed audio is handed on, not filtered");
        for (id, pcm) in &audio {
            assert_eq!(id, "kitchen");
            assert_eq!(pcm.len(), proto::FRAME_SAMPLES);
        }
        assert!(kws_audio(&h).is_empty(), "exactly the three frames, no duplicates");
        assert_eq!(stats(&handle).kws_drops, 0, "suppressed is not dropped");
    }

    /// R-A: the sample domain every span is defined against is the ring's write cursor,
    /// and [`SourceRegistry::register`] replaces that ring. The spotter thread counts its
    /// own frames, so registration has to tell it the origin moved — otherwise every span
    /// after a reconnect is offset by the samples the previous connection accepted and
    /// `ring.range(span)` reads an empty window.
    #[tokio::test]
    async fn a_registration_sends_the_kws_stream_restart_marker() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let h = harness(cfg, KWS_CHANNEL_CAPACITY).await;

        let mut stream = connect(&h, "kitchen", "kitchen", Some(SECRET)).await;
        send_frames(&mut stream, 1, 5).await;
        settled(|| frames_seen(&h.registry, "kitchen") == 1).await;

        let items = kws_items_until(&h, 2).await;
        assert_eq!(items.len(), 2, "the marker and the frame, in that order");
        assert_eq!(items[0].0, "kitchen");
        assert!(
            items[0].1.is_empty(),
            "the first item registration hands the spotter is the zero-length restart \
             marker (R-A), never audio"
        );
        assert_eq!(
            items[1].1.len(),
            proto::FRAME_SAMPLES,
            "and it precedes the audio it stands for"
        );

        // Reconnect: a new connection, a replaced ring, a cursor back at 0 — and a second
        // marker, because the spotter's clock has no way to know any of this happened.
        drop(stream);
        settled(|| h.registry.get("kitchen").is_none()).await;
        // Teardown marks the stream gone too (KWS-5), so an id that is not reused does not
        // leave the spotter holding per-source state for it: the same zero-length shape,
        // never audio.
        let dropped = kws_items_until(&h, 1).await;
        assert_eq!(dropped.len(), 1, "the unregister path emits a stream-drop marker");
        assert_eq!(dropped[0].0, "kitchen");
        assert!(dropped[0].1.is_empty(), "…and it is the zero-length form");
        let mut again = connect(&h, "kitchen", "kitchen", Some(SECRET)).await;
        send_frames(&mut again, 1, 7).await;
        settled(|| frames_seen(&h.registry, "kitchen") == 1).await;
        let handle = h.registry.get("kitchen").unwrap();
        assert_eq!(
            with_ring(&handle.ring, |r| r.written()),
            proto::FRAME_SAMPLES as u64,
            "a reconnect restarts the write cursor, which is exactly what the marker mirrors"
        );
        let second = kws_items_until(&h, 2).await;
        assert_eq!(second.len(), 2, "the new connection is a new stream");
        assert!(
            second[0].1.is_empty(),
            "…so registration marks it the same way, before any of its audio"
        );
        assert_eq!(second[1].1.len(), proto::FRAME_SAMPLES);
    }

    /// I1: a registration while the spotter channel is full must not stand. The restart
    /// marker is the only thing that realigns the thread's span clock with the replaced
    /// ring; a source registered with an unrebased clock has every later wake refused by
    /// the wake path's domain check — permanent deafness. The connection is refused
    /// instead, and nothing registered-and-unrebased is left behind.
    #[tokio::test]
    async fn a_registration_with_a_full_kws_channel_is_refused_not_left_unrebased() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        // One slot: the registration marker itself occupies it — exactly the I1
        // condition, a reconnect arriving while nothing can take the marker.
        let h = harness(cfg, 1).await;

        let mut first = connect(&h, "kitchen", "kitchen", Some(SECRET)).await;
        send_frames(&mut first, 1, 5).await;
        settled(|| frames_seen(&h.registry, "kitchen") == 1).await;
        assert_eq!(h.kws_rx.len(), 1, "the marker holds the only slot");

        let mut again = connect(&h, "kitchen", "kitchen", Some(SECRET)).await;
        assert!(
            closed(&mut again).await,
            "the reconnect is closed rather than registered against the previous \
             connection's clock (R-A)"
        );
        // The undone registration leaves no source behind, and its audio was never
        // counted: the pre-fix code kept it registered, lost the marker, and every wake
        // for the id was refused until some later reconnect got lucky.
        settled(|| h.registry.get("kitchen").is_none()).await;
        let item = h.kws_rx.try_recv().expect("the first connection's marker");
        assert_eq!(item.0, "kitchen");
        assert!(
            item.1.is_empty(),
            "the only queued item is the first marker — no audio from the refused \
             connection follows it"
        );
        assert_eq!(h.kws_rx.len(), 0, "and nothing else was handed on");

        // With room, the same reconnect succeeds and registers normally: the refusal was
        // a consequence of capacity, not of the id.
        let mut third = connect(&h, "kitchen", "kitchen", Some(SECRET)).await;
        send_frames(&mut third, 1, 9).await;
        settled(|| frames_seen(&h.registry, "kitchen") == 1).await;
        assert!(h.registry.get("kitchen").is_some(), "the retry is admitted");
    }

    #[tokio::test]
    async fn a_malformed_stream_is_closed_once_with_the_reason_latched() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let h = harness(cfg, KWS_CHANNEL_CAPACITY).await;
        let mut stream = connect(&h, "kitchen", "kitchen", Some(SECRET)).await;
        // A downlink control tag upstream is a firmware bug (contract choice 11).
        stream
            .write_all(&[proto::TAG_CONTROL, 0, 0, b'{', b'}'])
            .await
            .unwrap();
        assert!(
            closed(&mut stream).await,
            "a bad tag closes the connection instead of stalling the decoder"
        );
    }

    /// The audio a source already sent must outlive the source. Both clip writers ask for the
    /// ring *after* the event that justifies them, so a satellite dropping offline in between
    /// lost a clip whose samples were still in memory (docs/protocol.md §12).
    #[test]
    fn a_retired_registration_still_serves_the_audio_it_already_delivered() {
        let cfg = cfg_with(Some(SECRET), false, &[("kitchen", "kitchen", "kitchen")]);
        let registry = SourceRegistry::new(&cfg);
        let handle = registry.register(
            &hello("kitchen", "kitchen", Some(SECRET)),
            None,
            2 * proto::FRAME_SAMPLES,
        );
        let samples = vec![7; proto::FRAME_SAMPLES];
        with_ring(&handle.ring, |r| {
            r.write(&samples);
        });
        let at = Instant::now();

        assert!(
            registry.recently_gone_at("kitchen", at).is_none(),
            "while it is live the retired path must not answer: presence is `get`'s job, and \
             nothing may write through a ring a live connection owns"
        );

        let removed = registry
            .unregister("kitchen")
            .expect("the registration was there");
        assert!(
            Arc::ptr_eq(&removed, &handle),
            "what gets retired is exactly the live registration"
        );
        assert!(
            registry.get("kitchen").is_none(),
            "every other reader still sees it gone immediately"
        );

        let served = registry
            .recently_gone_at("kitchen", at)
            .expect("the source is gone; its audio is not");
        assert!(
            Arc::ptr_eq(&served, &handle),
            "the same ring, not a fresh empty one"
        );

        // The real consumer rather than a stand-in: this is the call `write_missed_wake_clip`
        // makes, and before the retirement window it produced a warning and no clip.
        let dir = tempfile::TempDir::new().unwrap();
        let clip = crate::clips::save_buffer_dump(&served.ring, "kitchen", dir.path())
            .expect("the missed-wake clip is written from the retired ring");
        let bytes = std::fs::metadata(&clip)
            .expect("the clip file exists")
            .len();
        assert!(
            bytes > 44 + 2 * proto::FRAME_SAMPLES as u64 - 2,
            "the clip holds the samples the source sent, not an empty wav"
        );

        // The window is the design: past it the source is simply gone, so this is a bounded
        // grace period and not a cache of dead sources.
        assert!(
            registry
                .recently_gone_at(
                    "kitchen",
                    at + RECENTLY_GONE_WINDOW + Duration::from_secs(1)
                )
                .is_none(),
            "a retired registration does not live forever"
        );
    }

    /// Retirement is what an owning connection's epilogue does — never replacement. A ring
    /// superseded by a reconnect must not be served as if it were current, and the list cannot
    /// grow without bound.
    #[test]
    fn retirement_never_shadows_a_live_or_superseded_registration() {
        let cfg = cfg_with(None, true, &[]);
        let registry = SourceRegistry::new(&cfg);
        let first = registry.register(&hello("kitchen", "kitchen", None), None, 1024);
        let second = registry.register(&hello("kitchen", "kitchen", None), None, 1024);
        assert!(
            registry.recently_gone("kitchen").is_none(),
            "a live source answers from `get`, never from the retired list"
        );

        registry.unregister("kitchen");
        let served = registry
            .recently_gone("kitchen")
            .expect("the newest registration is the one that was retired");
        assert!(
            Arc::ptr_eq(&served, &second) && !Arc::ptr_eq(&served, &first),
            "a ring superseded by a reconnect is never handed out as current"
        );

        // Bounded by construction: retire more than the cap and the oldest is simply absent.
        for i in 0..=RECENTLY_GONE_MAX {
            let id = format!("attic-{i}");
            registry.register(&hello(&id, "attic", None), None, 1024);
            registry.unregister(&id);
        }
        assert!(
            registry.recently_gone("attic-0").is_none(),
            "past the cap the oldest retirement is dropped, so a flapping source cannot grow \
             the list without bound"
        );
        assert!(
            registry
                .recently_gone(&format!("attic-{RECENTLY_GONE_MAX}"))
                .is_some(),
            "the newest retirement is still served"
        );
    }

    /// The ownership *decision*, for the caller that acts on it by stopping rather than by
    /// mutating. `get(source_id)` cannot answer this question at all: after a reconnect the id
    /// is still registered, so "is the source still there?" is true for a socket that owns
    /// nothing — which is precisely the reading that let a capture poll a dead ring until the
    /// hard cap.
    #[test]
    fn is_current_names_the_connection_and_not_the_id() {
        let cfg = cfg_with(None, true, &[]);
        let registry = SourceRegistry::new(&cfg);
        let first = registry.register(&hello("kitchen", "kitchen", None), None, 1024);
        assert!(
            registry.is_current(&first),
            "the connection that registered is current"
        );

        let second = registry.register(&hello("kitchen", "kitchen", None), None, 1024);
        assert!(
            registry.get("kitchen").is_some(),
            "the id is still registered — this is the reading the old liveness check used"
        );
        assert!(
            !registry.is_current(&first),
            "the superseded connection owns nothing, registered id or not"
        );
        assert!(
            registry.is_current(&second),
            "the connection that took the id over is the current one"
        );

        registry.unregister("kitchen");
        assert!(
            !registry.is_current(&second),
            "a source whose entry is gone is current for nobody, though its ring is still \
             reachable through `recently_gone`"
        );
    }
}
