//! AC10 / S7: a scripted **third-party** source completes a full round trip over the wire.
//!
//! This is the protocol's own CI test: a plain [`TcpStream`] client speaking to the real
//! [`ingest::serve`] accept loop on loopback. Nothing here mocks the transport.
//!
//! The client half is **written by hand from `docs/protocol.md`** and shares no encoder with
//! the server: a literal JSON hello line (§3), and records assembled byte by byte — a `0x01`
//! tag, `seq` as `u32` LE, and 1024 `i16` LE samples (§6); or a `0x02` tag, a `u16` LE
//! payload length, and JSON (§7). No encoder from `proto` is called on the client side, so
//! the two ends cannot move together: a wire-format change that breaks a third party breaks
//! this test first.
//!
//! The numbers the hand-written client writes live in their own `HAND_*` constants and are
//! asserted against `proto`'s declared contract at each point of use ([`pin_contract`]):
//! change a tag, a length or the version and this file fails naming both values and the
//! document section they came from, instead of the client silently following the server.
//!
//! The round trip covered is the one AC10 names: an unknown-but-allowed source
//! (`server.allow_unknown_sources`, S7) registers, streams an utterance, and presses the
//! "for you" button, and the assertions prove the **samples themselves** arrived — the ring
//! is compared against what was sent, sample for sample, because that ring is what the
//! missed-wake clip (`clips/missed/`, AC11) is cut from.
//!
//! Where the boundary is: this test covers the ingress half (register → ring → spotter →
//! broadcast → button event). The transcript half of that sentence — ASR and the verify
//! gate — is exercised offline by `listenrr replay` (S3.4), which needs a spotter model on
//! the host, so it is not a CI test.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use listenrr::config::Config;
use listenrr::ingest::{self, IngestEvent, SourceRegistry};
use listenrr::proto::{self, Caps, Event, Hello, FRAME_SAMPLES};
use listenrr::ring::with_ring;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, watch};

/// The shared secret, as a deployment would set `server.auth_token` (R11).
const SECRET: &str = "scripted-source-shared-secret";

/// The firmware string this client advertises; `fw` is diagnostics only (§3).
const HAND_FW: &str = "third-party/1.0";

// The wire numbers as a third-party implementer reads them out of `docs/protocol.md`. They
// are written down **separately** from `proto`'s constants on purpose: this client must be
// buildable from the document alone, and [`pin_contract`] is what makes a change to the
// declared contract fail here instead of being quietly mirrored by the client.
/// §1/§3 — the only accepted `hello.v`.
const HAND_PROTOCOL_VERSION: u32 = 1;
/// §6 — byte 0 of an audio record.
const HAND_AUDIO_TAG: u8 = 0x01;
/// §7 — byte 0 of an event record.
const HAND_EVENT_TAG: u8 = 0x02;
/// §6 — samples per frame (64 ms at 16 kHz).
const HAND_FRAME_SAMPLES: usize = 1024;
/// §6 — PCM bytes per frame, one `i16` per sample.
const HAND_PCM_BYTES: usize = 2048;
/// §6 — the frame (`seq` + PCM), **without** the tag byte.
const HAND_AUDIO_FRAME_LEN: usize = 2052;
/// §6 — the frame as it sits on the socket, tag byte included.
const HAND_AUDIO_RECORD_LEN: usize = 2053;
/// §7 — tag byte + `len` as `u16` LE.
const HAND_TAGGED_HEADER_LEN: usize = 3;
/// §7 — the "for you" button emits `save_buffer`, never a `button` named `save_buffer`.
const HAND_SAVE_BUFFER_JSON: &[u8] = br#"{"type":"save_buffer"}"#;

/// Assert the hand-written client still matches the declared contract.
///
/// Every byte this file's client writes comes from the `HAND_*` numbers above — i.e. from
/// the document — so these asserts are the only thing tying the two ends together. If
/// anyone changes a constant in `src/proto.rs` (or the document, which is generated from
/// it), the client does **not** follow: the test fails here, naming both values and the
/// section each came from. That is the point — a silent agreement between a shared encoder
/// and its own test would prove nothing about a third party.
fn pin_contract() {
    assert_eq!(
        HAND_PROTOCOL_VERSION,
        proto::PROTOCOL_VERSION,
        "the hand-written hello sends v = {HAND_PROTOCOL_VERSION} (docs/protocol.md §1, §3) but proto::PROTOCOL_VERSION is now {}; a third-party client written from the document no longer matches the declared contract",
        proto::PROTOCOL_VERSION
    );
    assert_eq!(
        HAND_AUDIO_TAG,
        proto::TAG_AUDIO,
        "the hand-written client puts tag {:#04x} at byte 0 of an audio record (docs/protocol.md §6) but proto::TAG_AUDIO is now {:#04x}; the hand-written client no longer matches the declared contract",
        HAND_AUDIO_TAG,
        proto::TAG_AUDIO
    );
    assert_eq!(
        HAND_EVENT_TAG,
        proto::TAG_EVENT,
        "the hand-written client puts tag {:#04x} at byte 0 of an event record (docs/protocol.md §7) but proto::TAG_EVENT is now {:#04x}; the hand-written client no longer matches the declared contract",
        HAND_EVENT_TAG,
        proto::TAG_EVENT
    );
    assert_eq!(
        HAND_FRAME_SAMPLES,
        proto::FRAME_SAMPLES,
        "the hand-written client sends {HAND_FRAME_SAMPLES} samples per frame (docs/protocol.md §6) but proto::FRAME_SAMPLES is now {}; the hand-written client no longer matches the declared contract",
        proto::FRAME_SAMPLES
    );
    assert_eq!(
        HAND_PCM_BYTES,
        proto::FRAME_PCM_BYTES,
        "the hand-written client writes {HAND_PCM_BYTES} bytes of PCM per frame (docs/protocol.md §6) but proto::FRAME_PCM_BYTES is now {}; the hand-written client no longer matches the declared contract",
        proto::FRAME_PCM_BYTES
    );
    assert_eq!(
        HAND_AUDIO_FRAME_LEN,
        proto::AUDIO_FRAME_LEN,
        "the hand-written client builds a {}-byte frame of seq + PCM (docs/protocol.md §6) but proto::AUDIO_FRAME_LEN is now {}; the hand-written client no longer matches the declared contract",
        HAND_AUDIO_FRAME_LEN,
        proto::AUDIO_FRAME_LEN
    );
    assert_eq!(
        HAND_AUDIO_RECORD_LEN,
        proto::AUDIO_RECORD_LEN,
        "the hand-written client writes {} bytes per frame on the socket (docs/protocol.md §6) but proto::AUDIO_RECORD_LEN is now {}; the hand-written client no longer matches the declared contract",
        HAND_AUDIO_RECORD_LEN,
        proto::AUDIO_RECORD_LEN
    );
    assert_eq!(
        HAND_TAGGED_HEADER_LEN,
        proto::TAGGED_HEADER_LEN,
        "the hand-written client headers events with {} bytes of tag + u16 LE length (docs/protocol.md §7) but proto::TAGGED_HEADER_LEN is now {}; the hand-written client no longer matches the declared contract",
        HAND_TAGGED_HEADER_LEN,
        proto::TAGGED_HEADER_LEN
    );
    // The document's own arithmetic, so a typo in a HAND_* number is caught even before it
    // is compared with the contract: tag + frame, and seq + PCM.
    assert_eq!(
        1 + HAND_AUDIO_FRAME_LEN,
        HAND_AUDIO_RECORD_LEN,
        "docs/protocol.md §6: the record is the tag byte plus the {}-byte frame",
        HAND_AUDIO_FRAME_LEN
    );
    assert_eq!(
        4 + HAND_PCM_BYTES,
        HAND_AUDIO_FRAME_LEN,
        "docs/protocol.md §6: the frame is seq (4 B) plus {HAND_PCM_BYTES} bytes of PCM"
    );
    assert_eq!(
        HAND_FRAME_SAMPLES * 2,
        HAND_PCM_BYTES,
        "docs/protocol.md §6: {HAND_FRAME_SAMPLES} samples, one i16 each"
    );
}

/// The backend half of the protocol, on a loopback port the kernel picked.
struct Rig {
    registry: Arc<SourceRegistry>,
    kws_rx: crossbeam_channel::Receiver<(String, Arc<[i16]>)>,
    frame_rx: mpsc::Receiver<(String, Vec<i16>)>,
    event_rx: mpsc::Receiver<IngestEvent>,
    addr: SocketAddr,
}

/// A config in the shape `[server]`/`[ha]`/`[parakeet]`/`[kws]`/`[clips]` requires, with
/// the switches this file exercises. The `listen` port is a placeholder: [`rig`] validates
/// this config and then substitutes a port the kernel picked (`server.listen` must name a
/// real port, so 0 would be rejected here).
fn config(allow_unknown_sources: bool, auth_token: Option<&str>) -> Config {
    let auth = auth_token.map_or_else(String::new, |t| format!("auth_token = \"{t}\"\n"));
    let text = format!(
        "[server]\nlisten = \"127.0.0.1:10800\"\n{auth}allow_unknown_sources = {allow_unknown_sources}\n\
         \n[ha]\nurl = \"ws://ha.example.lan:8123\"\ntoken = \"ha\"\npipeline_id = \"luna\"\n\
         \n[parakeet]\naddr = \"asr.example.lan:10200\"\n\
         \n[kws]\nmodel = \"/models/kws\"\nkeywords_file = \"/models/keywords.txt\"\n\
         \n[clips]\ndir = \"/tmp/listenrr-scripted-source-clips\"\n\
         \n[[sources]]\nid = \"kitchen\"\nroom = \"kitchen\"\ngroup = \"kitchen\"\n"
    );
    Config::from_toml_str(&text).expect("the test config is valid")
}

/// Start [`ingest::serve`] on a free loopback port and hold every sink it writes to.
async fn rig(cfg: &Config) -> Rig {
    // Ask the kernel for a free port, release it, and hand that number to `serve`: it
    // binds `cfg.server.listen` itself, and the kernel does not hand out a port it has
    // just released to anyone else in the meantime.
    let port = {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("a free loopback port");
        let port = probe.local_addr().expect("the probe has an address").port();
        drop(probe);
        port
    };
    let mut cfg = cfg.clone();
    cfg.server.listen = format!("127.0.0.1:{port}");
    let addr: SocketAddr = cfg
        .server
        .listen
        .parse()
        .expect("the address was built from a port");

    let registry = Arc::new(SourceRegistry::new(&cfg));
    let (kws_tx, kws_rx) = crossbeam_channel::bounded(ingest::KWS_CHANNEL_CAPACITY);
    let (frame_tx, frame_rx) =
        broadcast::channel::<(String, Arc<[i16]>)>(ingest::FRAME_BROADCAST_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel(ingest::EVENT_QUEUE_CAPACITY);
    // Nobody suppresses anything in these tests; the sender only has to stay alive so the
    // connection tasks see an empty set rather than a closed one.
    let (_suppress, suppress_rx) = watch::channel::<HashSet<String>>(HashSet::new());
    // The capture side repacks the `Arc` into a `Vec` once here so the assertions below
    // read like the bytes on the wire.
    let frame_sink = async_move_repacker(frame_rx);

    let failure: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    {
        let failure = Arc::clone(&failure);
        let serving = Arc::clone(&registry);
        tokio::spawn(async move {
            if let Err(e) =
                ingest::serve(&cfg, serving, kws_tx, frame_tx, event_tx, suppress_rx).await
            {
                *failure.lock().expect("the failure slot is not poisoned") = Some(e.to_string());
            }
        });
    }

    let rig = Rig {
        registry,
        kws_rx,
        frame_rx: frame_sink,
        event_rx,
        addr,
    };
    for _ in 0..100 {
        if TcpStream::connect(addr).await.is_ok() {
            return rig;
        }
        if let Some(error) = failure.lock().expect("the failure slot").clone() {
            panic!("ingest::serve could not start on {addr}: {error}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("ingest::serve never accepted a connection on {addr}");
}

/// Re-broadcast frames into an owned `Vec` so a test can compare them with what it sent.
fn async_move_repacker(
    mut rx: broadcast::Receiver<(String, Arc<[i16]>)>,
) -> mpsc::Receiver<(String, Vec<i16>)> {
    let (tx, out) = mpsc::channel(ingest::FRAME_BROADCAST_CAPACITY);
    tokio::spawn(async move {
        while let Ok((source, pcm)) = rx.recv().await {
            if tx.send((source, pcm.to_vec())).await.is_err() {
                break;
            }
        }
    });
    out
}

/// The hello a third-party client sends, written by hand from `docs/protocol.md` §3/§9.4:
/// one `\n`-terminated UTF-8 JSON object with no tag byte, fields in the documented order,
/// minimal caps (no speaker, no AEC — the defaults keep a client valid). With no shared
/// secret the `token` field is **omitted**, which is the documented "optional, default
/// absent" form.
fn hello(source_id: &str, room: &str, token: Option<&str>) -> String {
    pin_contract();
    let token_field = token.map_or_else(String::new, |secret| format!(r#","token":"{secret}""#));
    let line = format!(
        r#"{{"v":{HAND_PROTOCOL_VERSION},"source_id":"{source_id}","room":"{room}"{token_field},"caps":{{"has_speaker":false,"has_aec":false}},"fw":"{HAND_FW}"}}"#
    );
    assert_eq!(
        line.as_bytes()[0],
        b'{',
        "§3: the hello is a bare JSON object, not a tagged record"
    );
    assert!(
        !line.contains('\n'),
        "§3: nothing but the JSON object is on the line"
    );
    assert!(
        line.len() < proto::MAX_HELLO_LINE,
        "§1: a hello must stay under MAX_HELLO_LINE ({} unterminated bytes); this one is {} bytes",
        proto::MAX_HELLO_LINE,
        line.len()
    );
    // One `\n` ends the hello (§2, §3); the decoder strips it, so this is what goes on the
    // socket and what the backend reads.
    let wire = format!("{line}\n");
    // The bytes are the client's own, so the only bridge back to the declared contract is
    // the documented decode: if the document and `proto` ever diverge, it shows up here.
    let decoded = proto::decode_hello_line(wire.as_bytes())
        .expect("the hello written from docs/protocol.md is the JSON the backend accepts");
    assert_eq!(
        decoded,
        Hello {
            v: HAND_PROTOCOL_VERSION,
            source_id: source_id.to_string(),
            room: room.to_string(),
            token: token.map(str::to_string),
            caps: Caps::default(),
            fw: HAND_FW.to_string(),
        },
        "§3: the hand-written hello no longer decodes to the declared `Hello` shape"
    );
    wire
}

/// Connect and speak the hand-written hello line, exactly as §2/§3 document it.
async fn connect(addr: SocketAddr, hello: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr)
        .await
        .unwrap_or_else(|e| panic!("connecting to {addr}: {e}"));
    stream
        .write_all(hello.as_bytes())
        .await
        .expect("the hello goes out");
    stream.flush().await.expect("the hello is flushed");
    stream
}

/// One `0x01` audio record assembled by hand from the offset table in `docs/protocol.md`
/// §6: the tag byte, `seq` as `u32` **little-endian**, then [`HAND_FRAME_SAMPLES`] `i16`
/// little-endian samples. `sample` picks each sample, so a test can trace a received sample
/// back to the frame it came from.
fn audio_record(seq: u32, sample: impl Fn(usize) -> i16) -> Vec<u8> {
    pin_contract();
    let mut record = Vec::with_capacity(HAND_AUDIO_RECORD_LEN);
    record.push(HAND_AUDIO_TAG); // offset 0: the tag byte
    record.extend_from_slice(&seq.to_le_bytes()); // offsets 1..5
    for index in 0..HAND_FRAME_SAMPLES {
        // offsets 5..2053, two bytes per sample, low byte first
        record.extend_from_slice(&sample(index).to_le_bytes());
    }
    assert_eq!(
        record.len(),
        HAND_AUDIO_RECORD_LEN,
        "§6: a source must write 1 + 4 + {HAND_PCM_BYTES} bytes per frame"
    );
    assert_eq!(record[0], 0x01, "§6: byte 0 is the TAG_AUDIO byte");
    assert_eq!(
        &record[1..5],
        &seq.to_le_bytes(),
        "§6: seq sits at offset 1 as a u32 in little-endian order"
    );
    assert_eq!(
        record.len(),
        HAND_AUDIO_FRAME_LEN + 1,
        "§6: the tag is record framing, not part of the {HAND_AUDIO_FRAME_LEN}-byte frame"
    );
    record
}

/// One `0x01` audio record with a constant-amplitude frame, so a received sample can be
/// traced back to the frame it came from.
fn audio(seq: u32, value: i16) -> Vec<u8> {
    audio_record(seq, |_| value)
}

/// One `0x02` event record assembled by hand from `docs/protocol.md` §7: the tag byte, the
/// payload length as a `u16` **little-endian** count of payload bytes (header excluded),
/// then the UTF-8 JSON payload.
fn event_record(payload: &[u8]) -> Vec<u8> {
    pin_contract();
    assert!(
        payload.len() <= proto::MAX_JSON_PAYLOAD,
        "§7: len is a u16, so a payload is at most {} bytes",
        proto::MAX_JSON_PAYLOAD
    );
    let length = payload.len() as u16;
    let mut record = Vec::with_capacity(HAND_TAGGED_HEADER_LEN + payload.len());
    record.push(HAND_EVENT_TAG); // offset 0: the tag byte
    record.extend_from_slice(&length.to_le_bytes()); // offsets 1..3
    record.extend_from_slice(payload); // offsets 3..3+len
    assert_eq!(record[0], 0x02, "§7: byte 0 is the TAG_EVENT byte");
    assert_eq!(
        &record[1..3],
        &length.to_le_bytes(),
        "§7: the payload length is a u16 in little-endian order, header excluded"
    );
    assert_eq!(
        record.len(),
        HAND_TAGGED_HEADER_LEN + payload.len(),
        "§7: the record is the 3-byte header plus exactly len payload bytes"
    );
    record
}

async fn send_audio(stream: &mut TcpStream, count: u32, first_value: i16) {
    for seq in 0..count {
        stream
            .write_all(&audio(seq, first_value + i16::try_from(seq).unwrap()))
            .await
            .expect("an audio record goes out");
    }
    stream.flush().await.expect("the audio is flushed");
}

/// `true` once the server has closed the connection (a read returns EOF).
async fn closed(stream: &mut TcpStream) -> bool {
    let mut byte = [0u8; 1];
    matches!(stream.read(&mut byte).await, Ok(0))
}

/// Samples the ring holds for `source`, or `None` while it is not registered.
fn ring_range(rig: &Rig, source: &str, span: Range<u64>) -> Option<Vec<i16>> {
    let handle = rig.registry.get(source)?;
    Some(with_ring(&handle.ring, |ring| ring.range(span)))
}

fn ring_written(rig: &Rig, source: &str) -> Option<u64> {
    let handle = rig.registry.get(source)?;
    Some(with_ring(&handle.ring, |ring| ring.written()))
}

/// Poll `cond` for up to ~2 s: the connection task only becomes visible once the runtime
/// schedules it.
async fn settled<F: FnMut() -> bool>(mut cond: F) {
    for _ in 0..200 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached within 2 s");
}

/// The audio a source registered — the full utterance, frame for frame, in order.
fn assert_ring_holds(rig: &Rig, source: &str, count: u32, first_value: i16) {
    let written = ring_written(rig, source)
        .unwrap_or_else(|| panic!("{source} was never registered by its hello"));
    assert_eq!(
        written,
        u64::from(count) * FRAME_SAMPLES as u64,
        "the ring must have advanced by exactly the audio sent"
    );
    for seq in 0..count {
        let value = first_value + i16::try_from(seq).unwrap();
        let start = u64::from(seq) * FRAME_SAMPLES as u64;
        let got = ring_range(rig, source, start..start + FRAME_SAMPLES as u64)
            .expect("the source is registered");
        assert_eq!(got.len(), FRAME_SAMPLES, "frame {seq} is short");
        assert!(
            got.iter().all(|&sample| sample == value),
            "frame {seq} of {source} is not the audio that was sent (expected {value})"
        );
    }
}

/// Everything the spotter channel holds that is audio: registration also sends a
/// zero-length stream-restart marker (R-A), which is not a frame.
fn kws_audio(rig: &Rig) -> Vec<(String, Vec<i16>)> {
    rig.kws_rx
        .try_iter()
        .filter(|(_, pcm)| !pcm.is_empty())
        .map(|(source, pcm)| (source, pcm.to_vec()))
        .collect()
}

/// The AC10 round trip: an unknown third-party source registers with the shared secret,
/// streams an utterance, and presses the "for you" button.
#[tokio::test]
async fn a_scripted_third_party_source_completes_a_round_trip() {
    let mut rig = rig(&config(true, Some(SECRET))).await;
    let mut client = connect(rig.addr, &hello("porch-radio", "porch", Some(SECRET))).await;

    send_audio(&mut client, 3, 100).await;
    settled(|| ring_written(&rig, "porch-radio") == Some(3 * FRAME_SAMPLES as u64)).await;

    // The samples themselves, not just a count: this ring is what a missed-wake clip is
    // cut from (AC11), so a byte that arrived wrong is a defect, not a detail.
    assert_ring_holds(&rig, "porch-radio", 3, 100);
    let stats = {
        let handle = rig.registry.get("porch-radio").unwrap();
        let stats = handle.stats.lock().expect("the counters");
        (
            stats.frames,
            stats.last_seq,
            stats.seq_gaps,
            stats.kws_drops,
        )
    };
    assert_eq!(
        stats,
        (3, Some(2), 0, 0),
        "three in-order frames are not a gap and not a drop"
    );

    // The same audio reaches the spotter and the capture broadcast…
    settled(|| kws_audio(&rig).len() == 3).await;
    let audio = kws_audio(&rig);
    for (index, (source, pcm)) in audio.iter().enumerate() {
        assert_eq!(source, "porch-radio");
        assert_eq!(pcm.len(), FRAME_SAMPLES);
        assert!(
            pcm.iter()
                .all(|&sample| sample == 100 + i16::try_from(index).unwrap()),
            "frame {index} reaching the spotter was altered in transit"
        );
    }
    for index in 0..3 {
        let (source, pcm) = tokio::time::timeout(Duration::from_secs(1), rig.frame_rx.recv())
            .await
            .unwrap_or_else(|_| panic!("frame {index} never reached the capture broadcast"))
            .expect("the broadcast is open");
        assert_eq!(source, "porch-radio");
        assert_eq!(pcm.len(), FRAME_SAMPLES);
    }

    // …and the "for you" button reaches the coordinator as `save_buffer`, which is the
    // trigger the missed-wake clip is written from (S8.2).
    client
        .write_all(&event_record(HAND_SAVE_BUFFER_JSON))
        .await
        .expect("the button press goes out");
    client.flush().await.expect("the button press is flushed");
    let event = tokio::time::timeout(Duration::from_secs(1), rig.event_rx.recv())
        .await
        .expect("no event reached the coordinator")
        .expect("the event channel is open");
    assert_eq!(
        event,
        IngestEvent::Event {
            source: "porch-radio".to_string(),
            event: Event::SaveBuffer,
        }
    );

    // Disconnecting reports the source gone, so per-connection state is dropped (S1.4).
    drop(client);
    settled(|| {
        matches!(
            rig.event_rx.try_recv(),
            Ok(IngestEvent::SourceGone { source }) if source == "porch-radio"
        )
    })
    .await;
}

/// Auth on: a wrong token is refused at the hello, before the source exists at all — and
/// the port keeps serving the client that knows the secret.
#[tokio::test]
async fn a_wrong_hello_token_is_refused_before_the_source_exists() {
    let rig = rig(&config(true, Some(SECRET))).await;
    let mut guesser = connect(rig.addr, &hello("porch-radio", "porch", Some("guess"))).await;
    assert!(
        closed(&mut guesser).await,
        "a bad token closes the connection rather than stalling the client"
    );
    assert!(
        rig.registry.get("porch-radio").is_none(),
        "a rejected hello must not leave a half-registered source behind"
    );
    assert!(
        kws_audio(&rig).is_empty(),
        "nothing of a rejected source reaches the spotter"
    );

    // The rejection is per-connection: the next client, correctly credentialed, works.
    let mut client = connect(rig.addr, &hello("porch-radio", "porch", Some(SECRET))).await;
    send_audio(&mut client, 1, 42).await;
    settled(|| ring_written(&rig, "porch-radio") == Some(FRAME_SAMPLES as u64)).await;
    assert_ring_holds(&rig, "porch-radio", 1, 42);
}

/// Auth off (R11's accepted configuration): no token is required, and a third-party client
/// that sends none is still registered and still gets its audio into the ring.
#[tokio::test]
async fn auth_off_accepts_a_source_that_sends_no_token() {
    let rig = rig(&config(true, None)).await;
    let mut client = connect(rig.addr, &hello("phone", "living", None)).await;
    send_audio(&mut client, 2, 7).await;
    settled(|| ring_written(&rig, "phone") == Some(2 * FRAME_SAMPLES as u64)).await;
    assert_ring_holds(&rig, "phone", 2, 7);
}

/// A sequence gap is counted, not silently absorbed (R7): the ring stays contiguous
/// (append-only) while `seq_gaps` and `gap_samples` record what was skipped, which is what
/// makes a misaligned capture diagnosable rather than silent.
#[tokio::test]
async fn a_sequence_gap_is_counted_without_leaving_a_hole_in_the_ring() {
    let rig = rig(&config(true, Some(SECRET))).await;
    let mut client = connect(rig.addr, &hello("porch-radio", "porch", Some(SECRET))).await;
    for seq in [0u32, 1, 3] {
        client
            .write_all(&audio(seq, i16::try_from(seq).unwrap() + 20))
            .await
            .unwrap();
    }
    client.flush().await.unwrap();
    settled(|| ring_written(&rig, "porch-radio") == Some(3 * FRAME_SAMPLES as u64)).await;

    let handle = rig.registry.get("porch-radio").unwrap();
    let counters = handle.stats.lock().expect("the counters");
    assert_eq!(counters.frames, 3);
    assert_eq!(counters.last_seq, Some(3));
    assert_eq!(
        counters.seq_gaps, 1,
        "seq 2 was skipped and must be visible"
    );
    assert_eq!(
        counters.gap_samples, FRAME_SAMPLES as u64,
        "the gap is accounted in samples — the same domain spans are in (R-A)"
    );
    drop(counters);
    assert_eq!(
        with_ring(&handle.ring, |ring| ring.written()),
        3 * FRAME_SAMPLES as u64,
        "a gap compresses the ring rather than leaving a hole in it"
    );
}

/// The hand-written client against the document's own worked bytes (§9.1, §9.3). A
/// third-party implementer copies those bytes off the page, so agreeing with the server in a
/// round trip is not enough: this test pins the exact record bytes with the server not
/// involved at all.
#[test]
fn the_hand_written_records_reproduce_the_documented_bytes() {
    // §9.1: one audio record with `seq = 7` and the test pattern `pcm[i] = i`.
    let frame = audio_record(7, |index| {
        i16::try_from(index).expect("the pattern fits an i16")
    });
    assert_eq!(
        frame.len(),
        HAND_AUDIO_RECORD_LEN,
        "§9.1: 1 + 4 + {HAND_PCM_BYTES} = 2053 bytes on the socket"
    );
    assert_eq!(
        &frame[..13],
        &[0x01, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00, 0x03, 0x00],
        "§9.1: the tag, then seq = 7 as u32 LE, then pcm[0..4] = 0, 1, 2, 3 as i16 LE"
    );
    assert_eq!(
        &frame[HAND_AUDIO_RECORD_LEN - 4..],
        &[0xFE, 0x03, 0xFF, 0x03],
        "§9.1: pcm[1022] = 1022 and pcm[1023] = 1023, low byte first"
    );
    assert_eq!(
        i16::from_le_bytes([frame[5], frame[6]]),
        0,
        "§9.1: pcm[0] = 0 is `00 00`"
    );
    // §9.1: a negative sample is two's complement — `pcm[0] = -2` encodes as `FE FF`.
    assert_eq!(
        &audio_record(0, |_| -2)[5..7],
        &[0xFE, 0xFF],
        "§6: i16 samples are two's complement, low byte first"
    );

    // §9.3: `02 16 00` then the 22-byte payload — 25 bytes.
    let mut expected = vec![HAND_EVENT_TAG, 0x16, 0x00];
    expected.extend_from_slice(HAND_SAVE_BUFFER_JSON);
    let button = event_record(HAND_SAVE_BUFFER_JSON);
    assert_eq!(
        button, expected,
        "§9.3: the missed-wake record byte for byte"
    );
    assert_eq!(button.len(), 25, "§9.3: 25 record bytes");
    assert_eq!(
        HAND_SAVE_BUFFER_JSON.len(),
        22,
        "§7: the `save_buffer` payload is 22 bytes"
    );
    assert_eq!(
        proto::parse_event(&button).expect("§7: the payload is the documented JSON"),
        Event::SaveBuffer,
        "§7: a payload of {{\"type\":\"save_buffer\"}} is the missed-wake trigger, never a \
         generic `button` whose name happens to be `save_buffer`"
    );
}
