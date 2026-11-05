//! Real-socket tests for the ingest accept loop — the software route to AC2.
//!
//! AC2 ("2–4 satellites connect") is listed as hardware-gated in `docs/design/TEST-NOTES.md`, but the
//! behaviour it names needs no satellite: a peer connects over TCP, is admitted, is
//! registered, and is streamed. `src/ingest.rs`'s own unit tests stop at the struct
//! boundary — they call `SourceRegistry` methods directly, so nothing ever accepts a
//! connection. This file drives [`ingest::serve_on`] with real [`TcpStream`] clients
//! speaking real protocol bytes, which is what makes the accept loop, the close-on-bad-hello
//! paths, multi-connection attribution, and `IngestEvent::SourceGone` testable at all.
//!
//! Deliberately **not** covered here: byte-level agreement with the firmware
//! (`tests/wire_contract.rs`) and the third-party hand-written client (AC10's
//! `tests/scripted_source.rs`). The client in this file therefore builds every byte with
//! `listenrr::proto`'s own encoders — it is testing the server's admission and fan-out
//! policy, not the codec.
//!
//! The listener is reached through the seam `serve_on` was split out for: the test binds
//! `127.0.0.1:0`, learns the port, and hands the socket over. `cfg.server.listen` is never
//! read, so no test races a fixed port or a released probe port.
//!
//! # Why the "the server closed it" assertions can fail
//!
//! A bare `read() == 0` would also pass if the listener had never bound. Three things make
//! these assertions bite:
//!
//! 1. [`Harness::connect`] panics if the connect fails, and the accept loop task panics if
//!    `accept` errors, so "never bound" is a hard failure, not a silent EOF.
//! 2. [`expect_open`] is the same read shape asserting the *opposite* outcome, and it is
//!    exercised against accepted hellos in the happy-path, no-token, ad-hoc, and
//!    suppressed-source cases. If the accept loop closed everything, those cases go red.
//! 3. Where a rejection is expected after a working hello (bad tag mid-stream), the frame
//!    before it is first confirmed on the capture broadcast, so the close cannot be credited
//!    to a connection that was never admitted.
//!
//! Every positive wait is answered by a channel (frame broadcast, KWS channel, event
//! queue) — there is no `sleep` standing in for synchronisation. The one bounded wait is
//! `SILENT` inside [`expect_open`]: "no EOF arrived" is a negative that no channel can
//! answer, so it gets an explicit bound, with the reason stated at its use site.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use listenrr::config::Config;
use listenrr::ingest::{self, IngestEvent, SourceRegistry, SourceStats};
use listenrr::proto::{self, AudioFrame, Caps, Control, Hello, FRAME_SAMPLES};
use listenrr::ring::with_ring;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, watch};

/// The shared secret, as a deployment sets `server.auth_token` (R11).
const SECRET: &str = "listener-test-shared-secret";

/// Upper bound on a message the server is expected to deliver. Generous on purpose: these
/// are local socket writes, so hitting it means a lost frame, not a slow machine.
const DELIVERED: Duration = Duration::from_secs(5);
/// Bound for the one negative that no channel can answer: an accepted connection producing
/// no EOF. 300 ms is ~two orders of magnitude above the loopback round trip these tests
/// otherwise complete in, so a close observed inside it is real, and its absence over that
/// span means the connection stayed open.
const SILENT: Duration = Duration::from_millis(300);

/// A config in the shape `[server]`/`[ha]`/`[parakeet]`/`[kws]`/`[clips]` requires, with
/// the two switches these cases flip and three configured sources (AC2's 2–4 satellites,
/// minus one so the concurrency case can drop a peer and still have two left). The
/// `listen` value only has to satisfy `Config::validate`; `serve_on` never binds it.
fn config(auth_token: Option<&str>, allow_unknown_sources: bool) -> Config {
    let auth = auth_token.map_or_else(String::new, |t| format!("auth_token = \"{t}\"\n"));
    let text = format!(
        "[server]\nlisten = \"127.0.0.1:10700\"\n{auth}allow_unknown_sources = {allow_unknown_sources}\n\
         \n[ha]\nurl = \"ws://ha.example.lan:8123\"\ntoken = \"ha\"\npipeline_id = \"luna\"\n\
         \n[parakeet]\naddr = \"asr.example.lan:10200\"\n\
         \n[kws]\nmodel = \"/models/kws\"\nkeywords_file = \"/models/keywords.txt\"\n\
         \n[clips]\ndir = \"/tmp/listenrr-listener-clips\"\n\
         \n[[sources]]\nid = \"kitchen\"\nroom = \"kitchen\"\ngroup = \"house\"\n\
         \n[[sources]]\nid = \"living\"\nroom = \"living\"\ngroup = \"house\"\n\
         \n[[sources]]\nid = \"office\"\nroom = \"office\"\ngroup = \"house\"\n"
    );
    Config::from_toml_str(&text).expect("the test config is valid")
}

/// The hello a satellite sends, built with `proto`'s own encoder — this file tests
/// admission, not the codec.
fn hello(source_id: &str, room: &str, token: Option<&str>) -> Hello {
    Hello {
        v: proto::PROTOCOL_VERSION,
        source_id: source_id.to_owned(),
        room: room.to_owned(),
        token: token.map(str::to_owned),
        caps: Caps {
            has_speaker: true,
            has_aec: false,
        },
        fw: "listener-test/1.0".to_owned(),
    }
}

/// Samples unique to one `(source tag, seq)` pair, so an attribution assertion compares
/// the audio itself rather than trusting a source id that could have been mislabelled.
fn samples(tag: i16, seq: u32) -> Vec<i16> {
    let base = tag.wrapping_add((seq as i16).wrapping_mul(7));
    (0..FRAME_SAMPLES)
        .map(|i| base.wrapping_add(i as i16))
        .collect()
}

async fn send_hello(stream: &mut TcpStream, hello: &Hello) {
    stream
        .write_all(&proto::encode_hello_line(hello))
        .await
        .expect("write the hello line");
    stream.flush().await.expect("flush the hello line");
}

async fn send_frame(stream: &mut TcpStream, seq: u32, pcm: &[i16]) {
    let record = proto::encode_audio_frame(&AudioFrame {
        seq,
        pcm: pcm.to_vec(),
    });
    assert_eq!(
        record.len(),
        proto::AUDIO_RECORD_LEN,
        "encode_audio_frame returns an empty Vec for a wrong sample count; a short write would look like a protocol error"
    );
    stream
        .write_all(&record)
        .await
        .expect("write an audio record");
    stream.flush().await.expect("flush an audio record");
}

/// The backend half: an accept loop on an ephemeral loopback port plus every sink it
/// writes to. Held by value so each `#[tokio::test]` owns its own — no shared fixtures.
struct Harness {
    addr: SocketAddr,
    registry: Arc<SourceRegistry>,
    frames: broadcast::Receiver<(String, Arc<[i16]>)>,
    events: mpsc::Receiver<IngestEvent>,
    kws: crossbeam_channel::Receiver<(String, Arc<[i16]>)>,
}

impl Harness {
    async fn start(cfg: &Config) -> Self {
        Self::start_suppressing(cfg, HashSet::new()).await
    }

    /// Bind first, then hand the already-listening socket to the accept loop: by the time
    /// this returns the listener is bound, so a client connects to a live socket instead of
    /// racing one, and no probe-port number is involved.
    async fn start_suppressing(cfg: &Config, suppressed: HashSet<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind an ephemeral loopback port");
        let addr = listener
            .local_addr()
            .expect("a bound listener has an address");
        let registry = Arc::new(SourceRegistry::new(cfg));
        let (kws_tx, kws) = crossbeam_channel::bounded(ingest::KWS_CHANNEL_CAPACITY);
        let (frame_tx, frames) = broadcast::channel(ingest::FRAME_BROADCAST_CAPACITY);
        let (event_tx, events) = mpsc::channel(ingest::EVENT_QUEUE_CAPACITY);
        // The sender is dropped on purpose: the KWS thread reads the last value of the
        // suppression watch, which is the set the caller asked for, and no test flips it
        // mid-run (ingest itself no longer consults it — KWS-1).
        let (_suppress_tx, suppress) = watch::channel(suppressed);

        let serving = Arc::clone(&registry);
        let cfg = cfg.clone();
        tokio::spawn(async move {
            // The loop only returns on an accept error. A silent return would let a dead
            // listener masquerade as a closed connection, so it panics instead.
            let result = ingest::serve_on(
                listener, &cfg, serving, kws_tx, frame_tx, event_tx, suppress,
            )
            .await;
            panic!(
                "the accept loop ended: {}",
                result.expect_err("serve never returns Ok")
            );
        });

        Harness {
            addr,
            registry,
            frames,
            events,
            kws,
        }
    }

    async fn connect(&self) -> TcpStream {
        let stream = TcpStream::connect(self.addr)
            .await
            .unwrap_or_else(|e| panic!("connect to the accept loop on {}: {e}", self.addr));
        stream.set_nodelay(true).expect("nodelay");
        stream
    }

    /// The next frame published for live capture. Reaching this point is itself the
    /// synchronisation: `connection` registers before it records, and records before it
    /// broadcasts, so a received frame proves the source was admitted.
    async fn next_frame(&mut self) -> (String, Vec<i16>) {
        let (source, pcm) = tokio::time::timeout(DELIVERED, self.frames.recv())
            .await
            .unwrap_or_else(|_| panic!("no frame on the capture broadcast within {DELIVERED:?}"))
            .expect("the frame broadcast is still open");
        (source, pcm.to_vec())
    }

    /// Consume the capture broadcast until one of `source`'s frames whose samples are
    /// exactly `pcm` is published.
    ///
    /// [`tokio::sync::broadcast`] holds only the last `FRAME_BROADCAST_CAPACITY` messages,
    /// so a backlog larger than that is reported as a lag and skipped — which is exactly
    /// what retiring a filler's backlog means, and why this cannot be written with
    /// [`Harness::next_frame`], whose `expect` would treat the lag as a dead broadcast.
    /// `what` names the wait, as in [`Harness::next_event`].
    async fn drain_until(&mut self, source: &str, pcm: &[i16], what: &str) {
        loop {
            match tokio::time::timeout(DELIVERED, self.frames.recv()).await {
                Err(_) => {
                    panic!("no frame on the capture broadcast for {what} within {DELIVERED:?}")
                }
                Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => continue,
                Ok(Err(e)) => panic!("the capture broadcast failed during {what}: {e}"),
                Ok(Ok((got_source, got_pcm))) => {
                    if got_source == source && got_pcm.as_ref() == pcm {
                        return;
                    }
                }
            }
        }
    }

    /// The next *audio* frame handed to the spotter. A connection's first KWS message is
    /// the zero-length stream-restart marker (R-A), which is control rather than audio, so
    /// it is skipped: counting it would let a source pass a forwarding assertion without
    /// any audio ever being forwarded.
    async fn next_kws_audio(&self, within: Duration) -> Option<(String, Vec<i16>)> {
        let kws = self.kws.clone();
        tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + within;
            loop {
                let left = deadline.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return None;
                }
                match kws.recv_timeout(left) {
                    Ok((_, pcm)) if pcm.is_empty() => continue,
                    Ok((source, pcm)) => return Some((source, pcm.to_vec())),
                    Err(_) => return None,
                }
            }
        })
        .await
        .expect("the KWS probe thread")
    }

    /// The next coordinator event. `what` names the wait, because two waits in one test
    /// must never be mistaken for each other in a failure report.
    async fn next_event(&mut self, what: &str) -> IngestEvent {
        tokio::time::timeout(DELIVERED, self.events.recv())
            .await
            .unwrap_or_else(|_| panic!("no coordinator event for {what} within {DELIVERED:?}"))
            .expect("the event queue is still open")
    }

    /// No coordinator event within `within`.
    ///
    /// For the one exit the ownership rule makes deliberately silent: a superseded
    /// connection ends without touching the registry, so there is no message it is allowed
    /// to send and therefore no channel that can answer "that task has finished". Where a
    /// close *is* observable, prefer the ordering fact — the server drops the socket only
    /// after its epilogue has run, so any event it was going to send is already queued by
    /// the time [`expect_closed`] returns. This wait is for the peer-initiated teardown,
    /// where no such observation exists; its bound is [`SILENT`] for the same reason
    /// [`expect_open`]'s is.
    async fn expect_no_event(&mut self, within: Duration, what: &str) {
        if let Ok(Some(event)) = tokio::time::timeout(within, self.events.recv()).await {
            panic!("{what}: unexpected coordinator event {event:?}");
        }
    }

    fn handle(&self, source_id: &str) -> Arc<ingest::SourceHandle> {
        self.registry
            .get(source_id)
            .unwrap_or_else(|| panic!("{source_id} is not registered"))
    }

    /// Everything the capture broadcast has already published. Same ordering argument as
    /// [`Harness::kws_queued`]: `connection` publishes a frame only after it has recorded
    /// it, so a frame the server refused cannot appear here once its close is observed.
    fn frames_queued(&mut self) -> Vec<(String, Vec<i16>)> {
        let mut out = Vec::new();
        while let Ok((source, pcm)) = self.frames.try_recv() {
            out.push((source, pcm.to_vec()));
        }
        out
    }

    /// Everything in the KWS queue right now. `connection` pushes to KWS *before* it
    /// sends to the broadcast for the same frame, so once a frame has been observed on the
    /// broadcast its KWS hand-off has already happened — an ordering fact, which is why the
    /// suppression case needs no wait.
    fn kws_queued(&self) -> Vec<(String, Vec<i16>)> {
        let mut out = Vec::new();
        while let Ok((source, pcm)) = self.kws.try_recv() {
            out.push((source, pcm.to_vec()));
        }
        out
    }
}

/// The server closed the connection.
///
/// A hang fails this test rather than passing it: the brief's R10 rule is an observable
/// close, and `Ok(0)` (FIN) or a transport error (RST) is what a close looks like from the
/// client side.
async fn expect_closed(stream: &mut TcpStream, what: &str) {
    let mut byte = [0u8; 1];
    match tokio::time::timeout(DELIVERED, stream.read(&mut byte)).await {
        Err(_) => panic!("{what}: the server neither closed nor sent anything within {DELIVERED:?}; a hang is not a close"),
        Ok(Ok(0)) => {}
        Ok(Ok(n)) => panic!("{what}: expected a close, but the server sent {n} byte(s)"),
        Ok(Err(_)) => {}
    }
}

/// The control for [`expect_closed`] (same read, opposite expectation): an accepted
/// connection must not produce EOF. Without a case that asserts this, `expect_closed`
/// would pass equally well against a server that closes every connection on sight.
async fn expect_open(stream: &mut TcpStream, what: &str) {
    let mut byte = [0u8; 1];
    match tokio::time::timeout(SILENT, stream.read(&mut byte)).await {
        Err(_) => {}
        Ok(Ok(0)) => panic!("{what}: the server closed a connection it accepted"),
        Ok(Ok(n)) => panic!("{what}: ingest never writes to a satellite, but {n} byte(s) arrived"),
        Ok(Err(e)) => panic!("{what}: the accepted connection was reset: {e}"),
    }
}

/// Read a source's counters without poisoning the registry on a contention surprise.
fn stats_of(handle: &Arc<ingest::SourceHandle>) -> SourceStats {
    handle
        .stats
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// The last `count` samples a source's connection wrote into its ring.
fn ring_tail(handle: &Arc<ingest::SourceHandle>, count: usize) -> Vec<i16> {
    with_ring(&handle.ring, |ring| ring.last(count))
}

/// 1. Happy path: a well-formed hello from a configured source registers it with the
/// configured group, and its frames reach the ring, the spotter, and the capture broadcast
/// with the right per-source attribution.
#[tokio::test]
async fn configured_source_registers_with_its_group_and_streams_to_every_sink() {
    let cfg = config(Some(SECRET), false);
    let mut h = Harness::start(&cfg).await;
    let mut client = h.connect().await;

    let sent_caps = hello("kitchen", "kitchen", Some(SECRET)).caps;
    send_hello(&mut client, &hello("kitchen", "kitchen", Some(SECRET))).await;
    // Control for every close assertion below: an admitted hello stays open.
    expect_open(&mut client, "a configured source's hello").await;

    let pcm = samples(1000, 0);
    send_frame(&mut client, 0, &pcm).await;

    let (source, broadcast_pcm) = h.next_frame().await;
    assert_eq!(source, "kitchen", "the broadcast names the sending source");
    assert_eq!(broadcast_pcm, pcm, "the broadcast carries the samples sent");

    let (kws_source, kws_pcm) = h
        .next_kws_audio(DELIVERED)
        .await
        .expect("an unsuppressed frame must reach the spotter");
    assert_eq!(kws_source, "kitchen");
    assert_eq!(kws_pcm, pcm, "the spotter is handed the same audio");

    let handle = h.handle("kitchen");
    assert_eq!(handle.source_id, "kitchen");
    assert_eq!(handle.room, "kitchen", "room comes from the hello");
    assert_eq!(
        handle.group.as_deref(),
        Some("house"),
        "group comes from the configured [[sources]] entry"
    );
    assert_eq!(handle.caps, sent_caps, "caps come from the hello");
    assert_eq!(
        ring_tail(&handle, FRAME_SAMPLES),
        pcm,
        "the ring is the authoritative store and must hold the frame"
    );

    let stats = stats_of(&handle);
    assert_eq!(stats.frames, 1, "one frame received is one frame counted");
    assert_eq!(stats.last_seq, Some(0));
    assert_eq!(stats.seq_gaps, 0, "an in-order stream has no gaps");
    assert_eq!(stats.kws_drops, 0, "an empty KWS channel drops nothing");
}

/// 2. A wrong token against a configured `server.auth_token` closes the connection and
/// registers nothing (R11).
#[tokio::test]
async fn wrong_token_closes_the_connection_and_registers_nothing() {
    let cfg = config(Some(SECRET), false);
    let h = Harness::start(&cfg).await;
    let mut client = h.connect().await;

    send_hello(
        &mut client,
        &hello("kitchen", "kitchen", Some("wrong-secret")),
    )
    .await;
    expect_closed(&mut client, "a hello carrying the wrong token").await;

    assert!(
        h.registry.get("kitchen").is_none(),
        "a rejected hello must not create a source"
    );
    assert!(
        h.registry.live_sources().is_empty(),
        "nothing is registered: {:?}",
        h.registry.live_sources()
    );
}

/// 3. A hello with no token at all, while a token is configured, is rejected like a wrong
/// one — the missing field is not treated as an empty match.
#[tokio::test]
async fn missing_token_closes_the_connection_when_a_token_is_configured() {
    let cfg = config(Some(SECRET), false);
    let h = Harness::start(&cfg).await;
    let mut client = h.connect().await;

    send_hello(&mut client, &hello("kitchen", "kitchen", None)).await;
    expect_closed(&mut client, "a hello with no token while auth_token is set").await;

    assert!(h.registry.get("kitchen").is_none());
    assert!(h.registry.live_sources().is_empty());
}

/// 4. With no token configured, a tokenless hello is accepted — the shipped default, and
/// the positive control that makes the two rejections above non-trivial.
#[tokio::test]
async fn no_token_configured_accepts_a_tokenless_hello() {
    let cfg = config(None, false);
    let mut h = Harness::start(&cfg).await;
    let mut client = h.connect().await;

    send_hello(&mut client, &hello("kitchen", "kitchen", None)).await;
    let pcm = samples(1000, 0);
    send_frame(&mut client, 0, &pcm).await;

    let (source, broadcast_pcm) = h.next_frame().await;
    assert_eq!(source, "kitchen", "the tokenless hello was admitted");
    assert_eq!(broadcast_pcm, pcm);
    // Same read shape as `expect_closed`, opposite expectation, on the very connection
    // that the rejection cases send: if the listener closed everything, this is red.
    expect_open(&mut client, "a tokenless hello with no token configured").await;
    assert_eq!(stats_of(&h.handle("kitchen")).frames, 1);
}

/// 5. An unknown source with `allow_unknown_sources = false` is closed and never
/// registered (S7.1's default).
#[tokio::test]
async fn unknown_source_is_closed_when_unknown_sources_are_not_allowed() {
    let cfg = config(Some(SECRET), false);
    let h = Harness::start(&cfg).await;
    let mut client = h.connect().await;

    send_hello(&mut client, &hello("garage", "garage", Some(SECRET))).await;
    expect_closed(&mut client, "an unknown source with the switch off").await;

    assert!(h.registry.get("garage").is_none());
    assert!(
        h.registry.live_sources().is_empty(),
        "an unconfigured source must not appear in the registry"
    );
}

/// 6. The same unknown source with `allow_unknown_sources = true` is admitted as ad-hoc:
/// no group, room taken from the hello, and it still gets a ring, counters, and KWS
/// frames — the same treatment a configured source gets, minus the group.
#[tokio::test]
async fn unknown_source_is_admitted_as_ad_hoc_when_allowed() {
    let cfg = config(Some(SECRET), true);
    let mut h = Harness::start(&cfg).await;
    let mut client = h.connect().await;

    send_hello(&mut client, &hello("garage", "den", Some(SECRET))).await;
    let pcm = samples(4000, 0);
    send_frame(&mut client, 0, &pcm).await;

    let (source, broadcast_pcm) = h.next_frame().await;
    assert_eq!(source, "garage");
    assert_eq!(
        broadcast_pcm, pcm,
        "an ad-hoc source streams like any other"
    );

    let handle = h.handle("garage");
    assert_eq!(
        handle.group, None,
        "an ad-hoc source has no configured group (S7.1)"
    );
    assert_eq!(handle.room, "den", "the room comes from the hello");
    assert_eq!(
        ring_tail(&handle, FRAME_SAMPLES),
        pcm,
        "an ad-hoc source still gets a real ring"
    );
    let (kws_source, kws_pcm) = h
        .next_kws_audio(DELIVERED)
        .await
        .expect("an ad-hoc source's frames reach the spotter");
    assert_eq!(kws_source, "garage");
    assert_eq!(kws_pcm, pcm);
    assert_eq!(stats_of(&handle).frames, 1, "and its counters run");
    expect_open(&mut client, "an admitted ad-hoc source").await;
}

/// 7. The AC2 stand-in: three sources connect and stay connected, each frame is
/// attributed to the source that sent it, and one of them leaving produces exactly one
/// `SourceGone` for that id while the other two keep streaming. Asserted against the real
/// channels.
#[tokio::test]
async fn three_concurrent_sources_stream_and_one_departure_is_isolated() {
    let cfg = config(Some(SECRET), false);
    let mut h = Harness::start(&cfg).await;
    let ids = ["kitchen", "living", "office"];

    let mut clients = Vec::new();
    for (index, id) in ids.iter().enumerate() {
        let mut client = h.connect().await;
        send_hello(&mut client, &hello(id, id, Some(SECRET))).await;
        send_frame(&mut client, 0, &samples(1000 * (index as i16 + 1), 0)).await;
        clients.push(client);
    }

    // All three are connected and every frame carries its own sender's audio.
    let mut seen = Vec::new();
    for _ in 0..ids.len() {
        seen.push(h.next_frame().await);
    }
    let mut sources: Vec<&str> = seen.iter().map(|(source, _)| source.as_str()).collect();
    sources.sort_unstable();
    sources.dedup();
    assert_eq!(
        sources, ids,
        "all three configured sources must be connected and heard from"
    );
    for (source, pcm) in &seen {
        let index = ids
            .iter()
            .position(|id| id == source)
            .expect("only configured sources are heard");
        assert_eq!(
            pcm,
            &samples(1000 * (index as i16 + 1), 0),
            "the frame attributed to {source} must carry that source's samples"
        );
    }
    for id in ids {
        assert!(h.registry.get(id).is_some(), "{id} stays registered");
    }

    // One peer goes away; the other two must not notice.
    drop(clients.remove(1));
    match h.next_event("the departed peer's departure").await {
        IngestEvent::SourceGone { source } => {
            assert_eq!(source, "living", "the departed id is the one reported gone")
        }
        other => panic!("expected SourceGone for living, got {other:?}"),
    }
    assert!(
        h.registry.get("living").is_none(),
        "the departed source is unregistered"
    );

    send_frame(&mut clients[0], 1, &samples(1000, 1)).await;
    send_frame(&mut clients[1], 1, &samples(3000, 1)).await;
    let mut after = Vec::new();
    for _ in 0..2 {
        after.push(h.next_frame().await);
    }
    for (source, pcm) in &after {
        let expected = match source.as_str() {
            "kitchen" => samples(1000, 1),
            "office" => samples(3000, 1),
            other => panic!("{other} was dropped; only kitchen and office are still connected"),
        };
        assert_eq!(pcm, &expected, "{source} keeps streaming after a peer left");
    }

    // Exactly one `SourceGone`: by now the departure is already delivered (it was consumed
    // above) and the survivors' frames have been observed, so a second departure would
    // already be queued.
    while let Ok(event) = h.events.try_recv() {
        assert!(
            !matches!(event, IngestEvent::SourceGone { .. }),
            "a second source was reported gone: {event:?}"
        );
    }
    assert!(h.registry.get("kitchen").is_some());
    assert!(h.registry.get("office").is_some());
}

/// 8. A malformed hello is a protocol error: closed, never registered.
#[tokio::test]
async fn malformed_hello_is_closed_and_never_registered() {
    const MALFORMED: &[u8] = br#"{"v":1,"source_id":"kitchen","room":"kitchen""#;
    assert!(
        proto::decode_hello_line(MALFORMED).is_err(),
        "the bytes below must be what proto considers malformed, or this case tests nothing"
    );

    let cfg = config(Some(SECRET), false);
    let h = Harness::start(&cfg).await;
    let mut client = h.connect().await;

    client
        .write_all(MALFORMED)
        .await
        .expect("write a truncated JSON hello");
    client.write_all(b"\n").await.expect("terminate the line");
    expect_closed(&mut client, "a malformed hello line").await;

    assert!(h.registry.get("kitchen").is_none());
    assert!(
        h.registry.live_sources().is_empty(),
        "a rejected hello leaves no source behind"
    );
}

/// 9. A bad tag byte mid-stream closes the connection (R10, catch drift early). The frame
/// sent before it is confirmed on the broadcast first, so the close cannot be attributed to
/// a connection that was never admitted — and the close is observable, not a hang.
#[tokio::test]
async fn a_downlink_tag_on_the_uplink_closes_the_connection() {
    let cfg = config(Some(SECRET), false);
    let mut h = Harness::start(&cfg).await;
    let mut client = h.connect().await;

    send_hello(&mut client, &hello("kitchen", "kitchen", Some(SECRET))).await;
    send_frame(&mut client, 0, &samples(1000, 0)).await;
    let (source, _) = h.next_frame().await;
    assert_eq!(
        source, "kitchen",
        "the connection is live before the bad byte"
    );

    // A satellite that turns a control it received back around upstream: `0x03` is
    // downlink-only, so this is exactly the firmware drift R10 is about.
    client
        .write_all(&proto::encode_control(&Control::Earcon {
            name: "ding".to_owned(),
        }))
        .await
        .expect("write a 0x03 control record upstream");
    expect_closed(&mut client, "a 0x03 (downlink control) tag on the uplink").await;

    assert!(
        h.registry.get("kitchen").is_none(),
        "the closed connection is unregistered, not left half-alive"
    );
}

/// 10. An oversized unterminated hello is closed, and the listener keeps accepting after
/// it — one bad peer must not wedge the accept loop.
#[tokio::test]
async fn oversized_unterminated_hello_closes_without_wedging_the_listener() {
    let cfg = config(Some(SECRET), false);
    let mut h = Harness::start(&cfg).await;
    let mut client = h.connect().await;

    // More than `MAX_HELLO_LINE` bytes with no newline: `FrameDecoder` latches
    // `LineTooLong` rather than buffering forever.
    let oversized = vec![b'x'; proto::MAX_HELLO_LINE + 1];
    client
        .write_all(&oversized)
        .await
        .expect("write an unterminated hello past MAX_HELLO_LINE");
    expect_closed(
        &mut client,
        &format!(
            "an unterminated hello over MAX_HELLO_LINE ({})",
            proto::MAX_HELLO_LINE
        ),
    )
    .await;
    assert!(h.registry.live_sources().is_empty());

    // The accept loop still works: a fresh, well-behaved source is served normally.
    let mut next = h.connect().await;
    send_hello(&mut next, &hello("kitchen", "kitchen", Some(SECRET))).await;
    let pcm = samples(1000, 0);
    send_frame(&mut next, 0, &pcm).await;
    let (source, broadcast_pcm) = h.next_frame().await;
    assert_eq!(
        source, "kitchen",
        "the listener still accepts after a bad peer"
    );
    assert_eq!(broadcast_pcm, pcm);
    assert!(h.registry.get("kitchen").is_some());
}

/// 11. A suppressed source keeps filling its ring and keeps appearing on the capture
/// broadcast, and its audio still reaches the KWS thread — KWS-1 moved suppression from
/// the socket into the thread, because the ring advances for these frames and the thread's
/// span clock has to advance with them (starving the engine for them is the thread's own
/// behaviour, pinned in `src/kws.rs`). Handling stays per-source: an unsuppressed peer in
/// the same run is still forwarded end to end.
#[tokio::test]
async fn a_suppressed_source_fills_its_ring_and_hands_its_audio_to_the_thread() {
    let cfg = config(Some(SECRET), false);
    let mut suppressed = HashSet::new();
    suppressed.insert("kitchen".to_owned());
    let mut h = Harness::start_suppressing(&cfg, suppressed).await;

    let mut client = h.connect().await;
    send_hello(&mut client, &hello("kitchen", "kitchen", Some(SECRET))).await;
    let pcm = samples(1000, 0);
    send_frame(&mut client, 0, &pcm).await;
    let (source, broadcast_pcm) = h.next_frame().await;
    assert_eq!(source, "kitchen");
    assert_eq!(
        broadcast_pcm, pcm,
        "a suppressed source is still published for live capture"
    );

    let handle = h.handle("kitchen");
    assert_eq!(
        ring_tail(&handle, FRAME_SAMPLES),
        pcm,
        "suppressed sources keep filling their buffers — the plan invariant"
    );
    assert_eq!(
        stats_of(&handle).frames,
        1,
        "and its frames are still counted"
    );

    // No wait here on purpose: `connection` hands the frame to KWS before it sends to the
    // broadcast, so having received the broadcast means the KWS hand-off already happened.
    // The frame reaching the thread is the KWS-1 fix: pre-fix, filtering it at the socket
    // advanced the ring without the thread's clock. The zero-length stream-restart marker
    // (R-A) may be present too; it is not audio.
    let queued = h.kws_queued();
    let audio: Vec<&(String, Vec<i16>)> =
        queued.iter().filter(|(_, pcm)| !pcm.is_empty()).collect();
    assert_eq!(
        audio.len(),
        1,
        "the suppressed source's frame is handed to the thread, got {} audio frame(s): \
         {:?}",
        audio.len(),
        audio
    );
    assert_eq!(audio[0].0, "kitchen");
    assert_eq!(
        audio[0].1, pcm,
        "the thread is handed the same audio a wake is later measured against"
    );

    // The control inside the same run: an unsuppressed source on the same listener *is*
    // forwarded, so the assertion above is about the channel being live, not suppression.
    let mut other = h.connect().await;
    send_hello(&mut other, &hello("living", "living", Some(SECRET))).await;
    let living_pcm = samples(2000, 0);
    send_frame(&mut other, 0, &living_pcm).await;
    let (living_source, _) = h.next_frame().await;
    assert_eq!(living_source, "living");
    let (kws_source, kws_pcm) = h
        .next_kws_audio(DELIVERED)
        .await
        .expect("an unsuppressed source still reaches the spotter");
    assert_eq!(kws_source, "living");
    assert_eq!(kws_pcm, living_pcm);
}

/// 12. Sequence gaps: every frame that arrives is counted, and a hole in `seq` shows up in
/// the gap counters, read back through the registry.
#[tokio::test]
async fn sequence_gaps_are_counted_and_frames_are_still_recorded() {
    let cfg = config(Some(SECRET), false);
    let mut h = Harness::start(&cfg).await;
    let mut client = h.connect().await;
    send_hello(&mut client, &hello("kitchen", "kitchen", Some(SECRET))).await;

    // 0, 1, then 4: seq 2 and 3 never arrived.
    let sent: Vec<(u32, Vec<i16>)> = [0u32, 1, 4]
        .into_iter()
        .map(|seq| (seq, samples(1000, seq)))
        .collect();
    for (seq, pcm) in &sent {
        send_frame(&mut client, *seq, pcm).await;
    }
    // Drain all three from the broadcast: that is the barrier after which the registry has
    // accounted for each of them (`record_frame` runs before the broadcast send).
    for (seq, pcm) in &sent {
        let (source, broadcast_pcm) = h.next_frame().await;
        assert_eq!(source, "kitchen");
        assert_eq!(&broadcast_pcm, pcm, "frame {seq} arrived");
    }

    let handle = h.handle("kitchen");
    let stats = stats_of(&handle);
    assert_eq!(
        stats.frames,
        u64::try_from(sent.len()).expect("three frames"),
        "frames counts what arrived, not what was expected"
    );
    assert_eq!(stats.seq_gaps, 1, "one hole in seq is one gap (R7)");
    assert_eq!(
        stats.gap_samples,
        2 * u64::try_from(proto::FRAME_SAMPLES).expect("a positive frame size"),
        "seq 2 and 3 are two frames of samples lost"
    );
    assert_eq!(stats.last_seq, Some(4));

    // The ring is append-only across a gap: three frames of audio, no hole (the sample
    // domain is the write cursor, not `seq * FRAME_SAMPLES`).
    assert_eq!(
        ring_tail(&handle, 3 * FRAME_SAMPLES),
        sent.iter()
            .flat_map(|(_, pcm)| pcm.clone())
            .collect::<Vec<i16>>(),
        "a gap compresses rather than leaving a hole in the ring"
    );
}

/// 13. The same-`source_id` reconnect, reaped the way a *noticed* teardown reaps it.
///
/// This is the shape of an ordinary recovery: a satellite whose link dropped reconnects
/// while the server still holds the old socket, because nothing has told either end yet
/// (`docs/protocol.md` §12 describes exactly it and says the backend replaces the entry).
/// The reconnection succeeds — the new connection owns the registry, its ring, its
/// counters — and the assertion this case exists for is what happens when the old socket
/// is *then* reaped: it must not disturb the owner. A superseded connection has no claim
/// on the entry, so its exit may not unregister the id (which by then identifies the live
/// connection) and may not report that source gone, because the source is connected and
/// streaming. It ends with nothing on the event queue and a `warn!` naming the peer and
/// the source; case 15 covers the frame-triggered shape of the same rule, where the close
/// itself supplies the ordering and no wait is needed.
///
/// What is asserted here is the approved policy — newest-wins, with the teardown guarded
/// on ownership — not what the code did before it: removing the guard reddens the
/// ownership assertion below.
#[tokio::test]
async fn a_reconnect_owns_the_registry_until_the_old_connection_is_reaped() {
    let cfg = config(Some(SECRET), false);
    let mut h = Harness::start(&cfg).await;

    // The old connection: registered, streaming, and still open as far as the server knows.
    let mut old = h.connect().await;
    send_hello(&mut old, &hello("kitchen", "kitchen", Some(SECRET))).await;
    send_frame(&mut old, 0, &samples(1000, 0)).await;
    assert_eq!(
        h.next_frame().await.0,
        "kitchen",
        "the old connection is live"
    );
    let old_handle = h.handle("kitchen");

    // The same source reconnects while that socket is still held.
    let mut fresh = h.connect().await;
    send_hello(&mut fresh, &hello("kitchen", "kitchen", Some(SECRET))).await;
    let fresh_pcm = samples(2000, 0);
    send_frame(&mut fresh, 0, &fresh_pcm).await;
    let (source, got) = h.next_frame().await;
    assert_eq!(source, "kitchen");
    assert_eq!(
        got, fresh_pcm,
        "the reconnecting connection is the one believed"
    );
    let fresh_handle = h.handle("kitchen");
    assert!(
        !Arc::ptr_eq(&fresh_handle, &old_handle),
        "§12: the reconnect replaces the source's entry"
    );
    assert_eq!(
        stats_of(&fresh_handle).reconnects,
        1,
        "§12: the `reconnects` counter increments"
    );
    // Nothing has told the superseded connection anything yet; it finds out on its next
    // frame, which case 15 drives. Here it is reaped by its peer instead.
    expect_open(&mut old, "the superseded connection").await;

    // The old socket is reaped the way a noticed teardown reaps it: the peer closes it
    // (`luna_stream.cpp`'s `disconnect_()` does exactly this on every teardown).
    drop(old);
    // Nothing may come of that reap. It does not own the entry, and the source it would
    // have named is connected, streaming, and about to send another frame. No channel can
    // answer "that task has finished" for a peer-initiated close, which is why this is a
    // bounded wait where case 15 gets to use an ordering fact instead.
    h.expect_no_event(SILENT, "the reap of a superseded connection")
        .await;

    // The entry it must not have touched is still the live connection's, and the live
    // connection is still a connection.
    assert!(
        Arc::ptr_eq(&h.handle("kitchen"), &fresh_handle),
        "the reap unregistered an entry it never owned"
    );
    expect_open(&mut fresh, "the live connection after the reap").await;

    // And it keeps working: its next frame is recorded, counted, and delivered.
    let more = samples(2000, 1);
    send_frame(&mut fresh, 1, &more).await;
    let (source, got) = h.next_frame().await;
    assert_eq!(source, "kitchen");
    assert_eq!(
        got, more,
        "a reaped ghost cannot take a live source's stream down with it"
    );
    assert_eq!(
        stats_of(&fresh_handle).frames,
        2,
        "both of the owner's frames are counted, and only those"
    );
}

/// 14. The same reconnect with the old socket *half-open* — the state a dropped Wi-Fi
/// link actually leaves behind, where neither end has noticed.
///
/// How the condition is fabricated, and honestly: the old client socket is leaked with
/// [`std::mem::forget`], so the fd stays open in this process and the kernel sends no FIN
/// and no RST. The server's task for it is a real task on a real ESTABLISHED socket whose
/// read has nothing to return. What this does **not** reproduce is a peer that is still
/// retransmitting audio and will therefore abort its own connection in tens of seconds —
/// that is the case which turns this into the surprise outage in case 13, and no test here
/// manufactures it. The claim this case can support is bounded on purpose: within the
/// observation window the half-open connection is not reaped, and it does not displace the
/// reconnected source.
///
/// The server does have a bound for exactly this peer — `READ_IDLE_DEADLINE` ends a
/// connection that has stayed silent for ten of the firmware's 30 s RSSI heartbeats — but
/// it is minutes, so nothing here waits for it. The bounded reap itself is tested where
/// the deadline is a parameter: `src/ingest.rs`'s
/// `a_peer_that_goes_silent_is_reaped_and_its_source_released`.
#[tokio::test]
async fn a_half_open_old_connection_does_not_displace_the_reconnected_source() {
    let cfg = config(Some(SECRET), false);
    let mut h = Harness::start(&cfg).await;

    let mut old = h.connect().await;
    send_hello(&mut old, &hello("kitchen", "kitchen", Some(SECRET))).await;
    send_frame(&mut old, 0, &samples(1000, 0)).await;
    let (source, _) = h.next_frame().await;
    assert_eq!(source, "kitchen", "the old connection registered normally");
    // The KWS channel is shared and FIFO, so the ghost's own audio is still sitting in it.
    // Observing its frame on the broadcast above proved its KWS push had already happened,
    // so draining now is complete rather than a race: anything that arrives afterwards can
    // only have been produced by the connection that registers next.
    let ghost = h.kws_queued();
    assert_eq!(
        ghost.iter().filter(|(_, pcm)| !pcm.is_empty()).count(),
        1,
        "the drain must remove exactly the ghost's one audio frame, got {ghost:?}",
    );
    std::mem::forget(old); // leaks one fd + one connection task; that leak is part of the finding.

    let mut fresh = h.connect().await;
    send_hello(&mut fresh, &hello("kitchen", "kitchen", Some(SECRET))).await;
    let fresh_pcm = samples(2000, 0);
    send_frame(&mut fresh, 0, &fresh_pcm).await;
    let (source, got) = h.next_frame().await;
    assert_eq!(source, "kitchen");
    assert_eq!(
        got, fresh_pcm,
        "a half-open predecessor must not stop the reconnected source from being believed"
    );

    let handle = h.handle("kitchen");
    assert_eq!(
        ring_tail(&handle, FRAME_SAMPLES),
        fresh_pcm,
        "the live connection's audio is in the ring the capture path reads"
    );
    assert_eq!(
        stats_of(&handle).frames,
        1,
        "only the live connection's frame is counted, not the ghost's"
    );
    let (kws_source, kws_pcm) = h
        .next_kws_audio(DELIVERED)
        .await
        .expect("the reconnected source reaches the spotter while the ghost holds a socket");
    assert_eq!(kws_source, "kitchen");
    assert_eq!(kws_pcm, fresh_pcm);
    expect_open(
        &mut fresh,
        "the reconnected source while the old socket stays half-open",
    )
    .await;
    // Silent, as far as this case can observe: nothing arrives for the source that is
    // streaming now. Note what this does *not* prove — nothing happens to the half-open
    // socket during this test, so it cannot see the guards that fire when something does.
    // Those are cases 13 (the old socket is closed by the peer), 15 (it keeps streaming
    // audio) and 17 (it sends only its heartbeat); removing the ownership guards reddens
    // those three and leaves this one green, which is why it is scoped this way.
    h.expect_no_event(SILENT, "a half-open connection that was never reaped")
        .await;
    assert!(
        Arc::ptr_eq(&h.handle("kitchen"), &handle),
        "the half-open predecessor displaced neither the entry nor its owner"
    );
}

/// 15. The superseded connection *keeps streaming* — the case that made the attribution
/// rule worth writing down.
///
/// A satellite whose link dropped does not stop producing audio just because the server
/// has decided it was replaced: it keeps sending frames on a socket it still believes in.
/// And every new connection restarts its sequence at 0, so the ghost's frame 1 lands
/// immediately after the live source's frame 0. Keyed on the `source_id` alone, that audio
/// is appended to the live source's ring and its sequence is charged a hole it never had —
/// and a listener reading the ring hears the superseded socket's audio at a position in the
/// timeline it cannot possibly be at.
///
/// The outcome asserted is the decided one, not the one the code produced when this case
/// was written: a source has one writer, the connection that owns its entry. Everything the
/// ghost produces after being superseded is refused — not recorded, not counted, not
/// forwarded to the spotter or to live capture — and the ghost is closed. Closed
/// *silently* on the event queue, because the source is still connected and streaming on
/// the live socket and `SourceGone` would be a false report.
#[tokio::test]
async fn a_superseded_connection_that_keeps_streaming_does_not_feed_the_live_source() {
    let cfg = config(Some(SECRET), false);
    let mut h = Harness::start(&cfg).await;

    // The ghost: registered, one frame delivered, its sequence at 1.
    let mut ghost = h.connect().await;
    send_hello(&mut ghost, &hello("kitchen", "kitchen", Some(SECRET))).await;
    send_frame(&mut ghost, 0, &samples(1000, 0)).await;
    assert_eq!(h.next_frame().await.0, "kitchen", "the ghost is live");
    let ghost_handle = h.handle("kitchen");
    // Its audio is now in the spotter queue. Draining it after the frame was observed on
    // the broadcast (which happens after the KWS hand-off) makes the drain complete rather
    // than racy: anything in the queue from here on belongs to the next connection.
    let handed_on = h
        .kws_queued()
        .into_iter()
        .filter(|(_, pcm)| !pcm.is_empty())
        .count();
    assert_eq!(
        handed_on, 1,
        "the ghost handed exactly its own one frame on"
    );

    // The same source reconnects. Like any new connection, it starts its sequence at 0.
    let mut live = h.connect().await;
    send_hello(&mut live, &hello("kitchen", "kitchen", Some(SECRET))).await;
    let live_pcm = samples(2000, 0);
    send_frame(&mut live, 0, &live_pcm).await;
    let (source, got) = h.next_frame().await;
    assert_eq!(
        (source.as_str(), got.as_slice()),
        ("kitchen", live_pcm.as_slice())
    );
    let live_handle = h.handle("kitchen");
    assert!(
        !Arc::ptr_eq(&live_handle, &ghost_handle),
        "the reconnect owns the entry now"
    );
    // The live connection's audio reaches the spotter too, which leaves both queues known
    // empty of everything either connection has produced so far.
    let (kws_source, kws_pcm) = h
        .next_kws_audio(DELIVERED)
        .await
        .expect("the live source reaches the spotter");
    assert_eq!(
        (kws_source.as_str(), kws_pcm.as_slice()),
        ("kitchen", live_pcm.as_slice())
    );

    // The ghost knows none of this, and keeps streaming: frames 1..3, exactly the sequence
    // numbers that would splice into the live source's stream.
    for seq in 1..4u32 {
        send_frame(&mut ghost, seq, &samples(1000, seq)).await;
    }

    // The server ends it. Observing the close is also the barrier: the task decides the
    // frame's fate, runs its epilogue, and only then drops the socket — so whatever this
    // connection was ever going to put on a queue is already there.
    expect_closed(&mut ghost, "the ghost's frame after being superseded").await;

    // One writer per source: the live ring holds the live audio and nothing else.
    assert_eq!(
        ring_tail(&live_handle, FRAME_SAMPLES),
        live_pcm,
        "no ghost audio in the ring the capture path reads"
    );
    let after = stats_of(&live_handle);
    assert_eq!(
        after.frames, 1,
        "the live source's counters count the live source's frames"
    );
    assert_eq!(
        after.seq_gaps, 0,
        "and are not charged for a sequence it never received"
    );
    assert!(
        h.frames_queued().is_empty(),
        "the ghost's frames reached no live-capture subscriber"
    );
    let spliced: Vec<String> = h
        .kws_queued()
        .into_iter()
        .filter(|(_, pcm)| !pcm.is_empty())
        .map(|(source, _)| source)
        .collect();
    assert!(
        spliced.is_empty(),
        "nor the spotter: {spliced:?} carried stale audio into the live stream"
    );
    // And no event, for the source that is still streaming. Deterministic rather than a
    // wait, for the ordering reason above.
    if let Ok(event) = h.events.try_recv() {
        panic!("a superseded connection reported {event:?} for a source that is streaming");
    }
    // The live source is entirely unaffected by having had a ghost.
    expect_open(&mut live, "the live source after its ghost was closed").await;
    let more = samples(2000, 1);
    send_frame(&mut live, 1, &more).await;
    let (source, got) = h.next_frame().await;
    assert_eq!(
        (source.as_str(), got.as_slice()),
        ("kitchen", more.as_slice())
    );
    let after = stats_of(&live_handle);
    assert_eq!(after.frames, 2);
    assert_eq!(
        after.seq_gaps, 0,
        "its own sequence stays contiguous throughout"
    );
}

/// 16. A frame the superseded connection was still holding is accounted for all the same.
///
/// R9's rule is that no frame simply vanishes: audio a connection owned and never handed
/// to the spotter is counted, so a missed wake is diagnosable afterwards. That accounting
/// lives in the one epilogue every exit passes through, and this case pins the part the
/// earlier design skipped — the exit taken when a frame arrives after the connection was
/// superseded, which used to `return` past it entirely. Removing that routing reddens this
/// case and nothing else.
///
/// The spotter channel is filled with a *different* source's audio first, so the frame the
/// ghost holds back is genuinely its own and held for a reason the test controls. The count
/// lands on the entry that holds the `source_id` now, which after the reconnect is the live
/// connection: the audio belonged to that source, and the metric line the count exists for
/// is read per source id, not per connection. The ghost's own counters staying clean is
/// what distinguishes that from the count landing wherever the code happened to be.
#[tokio::test]
async fn a_superseded_connection_still_accounts_for_the_frame_it_never_handed_on() {
    let cfg = config(Some(SECRET), false);
    let mut h = Harness::start(&cfg).await;

    // Another source fills the shared spotter channel — its registration's restart marker
    // takes one slot and the ghost's own registration marker must be left a free one (I1:
    // a registration whose marker cannot be sent is refused, never left unrebased), so
    // this many audio frames leave the ghost's first frame nowhere to go.
    const FILLER_FRAMES: u32 = (ingest::KWS_CHANNEL_CAPACITY - 2) as u32;
    let mut filler = h.connect().await;
    send_hello(&mut filler, &hello("living", "living", Some(SECRET))).await;
    for seq in 0..FILLER_FRAMES {
        send_frame(&mut filler, seq, &samples(3000, seq)).await;
    }
    // The client's writes complete long before the server has read them, so the backlog has
    // to be retired by waiting for the *last* frame rather than draining opportunistically:
    // anything still in flight would arrive after the drain and be mistaken for the ghost's.
    h.drain_until(
        "living",
        &samples(3000, FILLER_FRAMES - 1),
        "the filler's last frame",
    )
    .await;
    assert!(
        h.frames_queued().is_empty(),
        "the filler's backlog is not retired: anything published after this point could \
         have been its, not the ghost's"
    );

    // The ghost registers and sends a frame: recorded and broadcast, but the hand-off to
    // the spotter must find the channel full, so the frame stays in the connection's own
    // hands. That is a precondition of this case, so it is checked rather than assumed.
    let mut ghost = h.connect().await;
    send_hello(&mut ghost, &hello("kitchen", "kitchen", Some(SECRET))).await;
    let held = samples(1000, 0);
    send_frame(&mut ghost, 0, &held).await;
    let (source, got) = h.next_frame().await;
    assert_eq!(
        (source.as_str(), got.as_slice()),
        ("kitchen", held.as_slice()),
        "observing this is what proves the spotter push was attempted"
    );
    let handed_on = h
        .kws_queued()
        .into_iter()
        .filter(|(s, pcm)| !pcm.is_empty() && s == "kitchen")
        .count();
    assert_eq!(
        handed_on, 0,
        "the spotter had room after all, so nothing was held back and there is no \
         held frame for this case to account for"
    );
    let ghost_handle = h.handle("kitchen");
    assert_eq!(
        stats_of(&ghost_handle).kws_drops,
        0,
        "a frame still held back is not yet a dropped frame"
    );

    // The `kws_queued()` drain above emptied the channel — that is what leaves room for
    // the reconnection's own registration marker, which I1 makes a hard requirement
    // (a marker that cannot be sent is a refused registration). The ghost's held frame
    // is untouched: a held frame is only ever retried by this connection's next push,
    // and this one is about to be superseded before it gets one. That is what the
    // accounting below tests.

    // The reconnect takes the id — and its frame has to be observed on the broadcast
    // before its handle is captured: `handle()` returns whatever is registered at that
    // instant, and without this barrier it can still be the ghost's, which would make the
    // count below look like it went missing when it went to the right place.
    let mut live = h.connect().await;
    send_hello(&mut live, &hello("kitchen", "kitchen", Some(SECRET))).await;
    let live_pcm = samples(2000, 0);
    send_frame(&mut live, 0, &live_pcm).await;
    let (source, got) = h.next_frame().await;
    assert_eq!(
        (source.as_str(), got.as_slice()),
        ("kitchen", live_pcm.as_slice()),
        "the reconnect is the one believed"
    );
    let live_handle = h.handle("kitchen");
    assert!(
        !Arc::ptr_eq(&live_handle, &ghost_handle),
        "the reconnect owns the entry now"
    );

    // The ghost sends again and is refused.
    send_frame(&mut ghost, 1, &samples(1000, 1)).await;
    expect_closed(
        &mut ghost,
        "the held-frame connection after being superseded",
    )
    .await;

    // The epilogue ran, so the frame it was still holding is on the books — counted against
    // the source, which is what the metric line is keyed on.
    assert_eq!(
        stats_of(&live_handle).kws_drops,
        1,
        "the frame it owned and never handed on is accounted for (R9), on the exit that \
         used to skip the accounting entirely"
    );
    assert_eq!(
        stats_of(&ghost_handle).kws_drops,
        0,
        "and attributed to the source, not to whichever socket once held its id"
    );
    if let Ok(event) = h.events.try_recv() {
        panic!("a superseded connection reported {event:?}; the source is still connected");
    }
    expect_open(
        &mut live,
        "the live source while its ghost's held frame was accounted",
    )
    .await;
    // Dropped last: its own teardown is a legitimate `SourceGone`, which nothing here is
    // asserting about.
    drop(filler);
}

/// 17. A superseded peer whose only traffic is its heartbeat.
///
/// Cases 15 and 16 cover a ghost that keeps sending *audio*, which the frame path refused.
/// This is the arm that had no gate at all — `UpMessage::Event`. The firmware sends an
/// `rssi` every 30 s whatever else is happening (docs/protocol.md §7), so a ghost emitting
/// only that is the shape a dropped Wi-Fi link actually leaves behind, and before this gate
/// it (a) overwrote the live source's signal reading with a number a dead socket measured,
/// (b) could attribute any uplink event to a source it no longer represented — a ghost
/// `playback_done` ends the live session's Speaking state and a ghost `save_buffer` dumps
/// the live ring — and (c) was **never reaped at all**, because every inbound byte resets
/// the read deadline, so the one bound this backend has for a stuck peer could never fire.
#[tokio::test]
async fn a_superseded_connection_that_only_sends_its_heartbeat_is_closed_and_not_believed() {
    let cfg = config(Some(SECRET), false);
    let mut h = Harness::start(&cfg).await;

    // The ghost: registered, one frame delivered, no signal reading of its own yet.
    let mut ghost = h.connect().await;
    send_hello(&mut ghost, &hello("kitchen", "kitchen", Some(SECRET))).await;
    send_frame(&mut ghost, 0, &samples(1000, 0)).await;
    assert_eq!(h.next_frame().await.0, "kitchen", "the ghost is live");
    let ghost_handle = h.handle("kitchen");
    assert_eq!(
        h.kws_queued()
            .into_iter()
            .filter(|(_, pcm)| !pcm.is_empty())
            .count(),
        1,
        "the drain must remove exactly the ghost's own audio, so anything after it belongs \
         to the connection that registers next"
    );

    // The same source reconnects and owns the entry.
    let mut live = h.connect().await;
    send_hello(&mut live, &hello("kitchen", "kitchen", Some(SECRET))).await;
    let live_pcm = samples(2000, 0);
    send_frame(&mut live, 0, &live_pcm).await;
    h.drain_until("kitchen", &live_pcm, "the reconnect is registered")
        .await;
    let live_handle = h.handle("kitchen");
    assert!(
        !Arc::ptr_eq(&live_handle, &ghost_handle),
        "the reconnect owns the entry now"
    );
    assert_eq!(
        stats_of(&live_handle).rssi_dbm,
        None,
        "and has reported no signal level of its own"
    );

    // The ghost sends one heartbeat and nothing else — no audio, so the frame path is never
    // reached. Written only now: the server cannot have processed it before the reconnect was
    // observed, which makes the ordering a fact rather than a wait.
    let heartbeat = |dbm| proto::encode_event(&proto::Event::Rssi { dbm });
    ghost
        .write_all(&heartbeat(-77.5))
        .await
        .expect("write the ghost's rssi");
    ghost.flush().await.expect("flush the ghost's rssi");

    // §12: a replaced connection is closed when it next sends *anything*. This is that
    // sentence. Observing the close is also the barrier — whatever this connection was ever
    // going to put on a queue is on it by now.
    expect_closed(&mut ghost, "the ghost's heartbeat after being superseded").await;

    // The reading was believed by nobody: neither the live source's counters nor the ghost's
    // own carry it. This is where -77.5 used to land, under `kitchen`.
    assert_eq!(
        stats_of(&live_handle).rssi_dbm,
        None,
        "the live source's signal level cannot be a number a dead socket measured"
    );
    assert_eq!(
        stats_of(&ghost_handle).rssi_dbm,
        None,
        "and it is not parked on the socket that measured it either"
    );
    if let Ok(event) = h.events.try_recv() {
        panic!("a superseded connection reported {event:?}; the source is still connected");
    }

    // The live source is untouched. Its own heartbeat is believed, and recorded — the event
    // queue answers that, because the server records the reading before it forwards the
    // event, so seeing it here proves the write already happened.
    expect_open(
        &mut live,
        "the live source after its ghost's heartbeat was closed",
    )
    .await;
    live.write_all(&heartbeat(-52.25))
        .await
        .expect("write the live rssi");
    live.flush().await.expect("flush the live rssi");
    match h.next_event("the live source's own heartbeat").await {
        IngestEvent::Event {
            source,
            event: proto::Event::Rssi { dbm },
        } => {
            assert_eq!(
                (source.as_str(), dbm),
                ("kitchen", -52.25),
                "the source's own heartbeat reaches the coordinator"
            );
            assert_eq!(
                stats_of(&live_handle).rssi_dbm,
                Some(-52.25),
                "and the connection that does own the source is still recorded"
            );
        }
        other => panic!("expected the live source's rssi, got {other:?}"),
    }

    // Still streaming afterwards: closing a ghost must not cost the source its audio.
    let more = samples(2000, 1);
    send_frame(&mut live, 1, &more).await;
    h.drain_until(
        "kitchen",
        &more,
        "the live source still streams after its ghost's heartbeat was closed",
    )
    .await;
}

/// §12's sentence — a replaced connection is "closed when it next sends *anything*" — is
/// about bytes. The ownership gate that enforces it sits where a *complete* frame or event
/// has been decoded, so a superseded peer trickling bytes that never complete a message was
/// never asked the question, and each successful read restarts its idle deadline, so the
/// reap that would otherwise find it can never fire: the task and the socket live forever.
/// The case above is the sibling that does close, because a heartbeat is a complete event.
/// This is the half that did not.
#[tokio::test]
async fn a_superseded_connection_that_sends_only_partial_bytes_is_closed() {
    let cfg = config(Some(SECRET), false);
    let mut h = Harness::start(&cfg).await;

    // The ghost: admitted, registered, streaming.
    let mut ghost = h.connect().await;
    send_hello(&mut ghost, &hello("kitchen", "kitchen", Some(SECRET))).await;
    send_frame(&mut ghost, 0, &samples(1000, 0)).await;
    assert_eq!(h.next_frame().await.0, "kitchen", "the ghost is live");

    // The same source reconnects, which retires the ghost.
    let mut live = h.connect().await;
    send_hello(&mut live, &hello("kitchen", "kitchen", Some(SECRET))).await;
    let live_pcm = samples(2000, 0);
    send_frame(&mut live, 0, &live_pcm).await;
    h.drain_until("kitchen", &live_pcm, "the reconnect is registered")
        .await;

    // The ghost sends the first two bytes of an audio frame record and nothing else: the tag
    // and one byte of `seq`. The decoder buffers them waiting for the other 2051 — no
    // message, no decode error, nothing for a per-message gate to act on. Written only now,
    // so the ordering is a fact rather than a wait.
    let partial = proto::encode_audio_frame(&AudioFrame {
        seq: 1,
        pcm: samples(1000, 1),
    });
    ghost
        .write_all(&partial[..2])
        .await
        .expect("write half a frame header");
    ghost.flush().await.expect("flush half a frame header");

    // §12: closed when it next sends anything.
    expect_closed(
        &mut ghost,
        "the ghost's partial bytes after being superseded",
    )
    .await;

    // Observing the close is the barrier, so whatever that half-frame was ever going to
    // become is on the queues by now. It became nothing: half a frame is not an utterance,
    // and a connection that no longer owns its source must not put one on the pipeline.
    let queued = h.frames_queued();
    assert!(
        !queued
            .iter()
            .any(|(_, pcm)| pcm.as_slice() == samples(1000, 1).as_slice()),
        "the retired connection's incomplete frame reached the live pipeline: {queued:?}"
    );

    // The control that keeps this a supersession case and not a close-everything case: the
    // connection that does own the source is untouched, and still streaming.
    expect_open(
        &mut live,
        "the live source after its ghost's partial bytes were closed",
    )
    .await;
    let more = samples(2000, 1);
    send_frame(&mut live, 1, &more).await;
    h.drain_until(
        "kitchen",
        &more,
        "the live source still streams after its ghost's partial bytes were closed",
    )
    .await;

    // And bytes are only a reason to close once there is a registration to be superseded out
    // of: the gate asks nothing of a connection that has no handle yet. A peer whose hello
    // line has no newline is mid-sentence, not stale, and must stay open to finish it.
    let mut muttering = h.connect().await;
    muttering
        .write_all(b"{\"t\":\"hello\",\"v\":1")
        .await
        .expect("write a hello line with no newline");
    muttering.flush().await.expect("flush the incomplete hello");
    expect_open(&mut muttering, "an unregistered peer mid-hello").await;
}
