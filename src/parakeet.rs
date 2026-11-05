//! Wyoming client for the Parakeet ASR service (plan S3.3, O1, R2).
//!
//! Transcription is one network hop to the Wyoming service; keeping the boundary is what
//! makes moving that service a URL change (plan O1). This call runs on an async task, never
//! on the KWS thread or the ingest hot path (plan §3). Where the service runs is
//! `docs/deployment.md`.
//!
//! **U1 is pinned, not assumed** (captured 2026-11-02 against `lmoe/wyoming-onnx-stt`
//! running Parakeet TDT 0.6b-v3 on a GPU host, wyoming 1.8.0). A message is a JSON line
//! terminated by `\n`, then an optional `data` JSON blob of exactly `data_length` bytes,
//! then an optional raw payload of exactly `payload_length` bytes:
//!
//! ```text
//! {"type": "audio-chunk", "version": "1.8.0", "data_length": 61, "payload_length": 2048}\n
//! {"rate": 16000, "width": 2, "channels": 1, "timestamp": null}
//! <2048 raw PCM bytes>
//! ```
//!
//! `data` is never inline in the line, and there is no length prefix of any kind. The line
//! is Python's `json.dumps(..., separators=(", ", ": "))` form, key order included, because
//! that is what `wyoming/event.py::async_write_event` writes. `version` is informational —
//! the service ignores it on read — and is pinned to the version we interop-tested. The
//! answer we wait for is a `transcript` event whose `data.text` lives in its data blob.
//!
//! `width` is **bytes per sample**, not bits: 16-bit PCM is `2`. A bit count (`16`) is
//! accepted by the parser and then kills the service inside `audioop.lin2lin` with
//! `Size should be 1, 2, 3 or 4`, so the constant here is named for bytes and there is no
//! bit-count constant to reach for by mistake.
//!
//! Every framing constant lives in [`framing`] so a future re-pin is a one-place edit
//! (plan R2).

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use serde_json::Value;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

/// The Wyoming wire framing, in one place so a live capture pins it with a single diff
/// (plan R2).
///
/// Message layout: `{"type": ..., "version": ..., "data_length": N, "payload_length": M}`
/// followed by `\n`, then `N` bytes of `data` JSON, then `M` bytes of payload. Both length
/// keys are omitted when their part is absent. The `data` blob never appears inline in the
/// line.
pub mod framing {
    /// The wyoming library version whose bytes this module reproduces. Informational: a
    /// service ignores it on read. Bump it when a live re-pin happens, never casually.
    pub const PROTOCOL_VERSION: &str = "1.8.0";

    /// An event line (with its `\n`) longer than this is a protocol error, not something to
    /// buffer.
    pub const MAX_LINE_BYTES: usize = 64 * 1024;
    /// A `data` blob larger than this is a protocol error (the audio blob is 61 bytes).
    pub const MAX_DATA_BYTES: usize = 64 * 1024;
    /// A payload larger than this is a protocol error (`audio-chunk` is 2 KiB).
    pub const MAX_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

    /// Line key: the event name.
    pub const KEY_TYPE: &str = "type";
    /// Line key: the event's fields, as parsed from the `data_length` blob.
    pub const KEY_DATA: &str = "data";
    /// Line key: byte length of the `data` JSON blob that follows the newline.
    pub const KEY_DATA_LENGTH: &str = "data_length";
    /// Line key: byte length of the raw payload that follows the data blob.
    pub const KEY_PAYLOAD_LENGTH: &str = "payload_length";
    /// `data` key: the transcript text in a [`EVENT_TRANSCRIPT`] event.
    pub const KEY_TEXT: &str = "text";
    /// `data` key: sample rate of the audio that follows.
    pub const KEY_RATE: &str = "rate";
    /// `data` key: sample width in BYTES per sample — 2 for 16-bit PCM, never the bit count.
    pub const KEY_WIDTH: &str = "width";
    /// `data` key: interleaved channel count.
    pub const KEY_CHANNELS: &str = "channels";
    /// `data` key: the language the service picks its model for.
    pub const KEY_LANGUAGE: &str = "language";
    /// `data` key: when the audio was captured; `null` for a live utterance.
    pub const KEY_TIMESTAMP: &str = "timestamp";

    /// Event: ask the service for a transcription (opens the ASR session).
    pub const EVENT_TRANSCRIBE: &str = "transcribe";
    /// Event: the audio format that follows.
    pub const EVENT_AUDIO_START: &str = "audio-start";
    /// Event: one chunk of PCM, `payload_length` bytes of it.
    pub const EVENT_AUDIO_CHUNK: &str = "audio-chunk";
    /// Event: the utterance is complete; the service answers with [`EVENT_TRANSCRIPT`].
    pub const EVENT_AUDIO_STOP: &str = "audio-stop";
    /// Event: the service's answer — `data.text` is the transcript.
    pub const EVENT_TRANSCRIPT: &str = "transcript";

    /// Sample rate the service expects (16 kHz mono, as everywhere else in the crate).
    pub const SAMPLE_RATE_HZ: u32 = 16_000;
    /// Channels the service expects (mono).
    pub const CHANNELS: u32 = 1;
    /// Samples per `audio-chunk` — 1024 samples = 64 ms, one uplink frame.
    pub const AUDIO_CHUNK_SAMPLES: usize = 1024;
    /// Bytes per PCM sample, and the `width` the wire format carries: wyoming counts
    /// **bytes**, so 16-bit PCM is `2`. The old name here was `SAMPLE_WIDTH_BITS = 16`, and
    /// that bit count is what made the live service die in `audioop.lin2lin` with
    /// `Size should be 1, 2, 3 or 4`; there is deliberately no bit-count constant.
    pub const SAMPLE_WIDTH_BYTES: usize = 2;
    /// Payload bytes of a full `audio-chunk`.
    pub const AUDIO_CHUNK_BYTES: usize = AUDIO_CHUNK_SAMPLES * SAMPLE_WIDTH_BYTES;

    /// The language tag sent with `transcribe`; the service uses it to pick a model. A
    /// future `parakeet.language` config key would override this constant (`src/config.rs`
    /// is deliberately untouched until someone needs it).
    pub const LANGUAGE: &str = "en";

    /// Give up on a transcription request after this many milliseconds (the service is
    /// local; a stall is a fault, and the plan's latency budget has no room for one).
    pub const REQUEST_TIMEOUT_MS: u64 = 10_000;

    /// One complete message: the header line, its `\n`, the data blob, then the payload.
    ///
    /// Hand-built rather than serialized, because the byte order (`type`, `version`,
    /// `data_length`, `payload_length`) and the `", "`/`": "` separators are part of the
    /// wire format and `serde_json`'s map would order keys alphabetically. Every value here
    /// is a constant of ours or a byte count, so there is nothing to escape.
    pub fn frame(kind: &str, data: &str, payload: &[u8]) -> Vec<u8> {
        let mut line = format!("{{\"type\": \"{kind}\", \"version\": \"{PROTOCOL_VERSION}\"");
        if !data.is_empty() {
            line.push_str(&format!(", \"{KEY_DATA_LENGTH}\": {}", data.len()));
        }
        if !payload.is_empty() {
            line.push_str(&format!(", \"{KEY_PAYLOAD_LENGTH}\": {}", payload.len()));
        }
        line.push('}');

        let mut out = Vec::with_capacity(line.len() + 1 + data.len() + payload.len());
        out.extend_from_slice(line.as_bytes());
        out.push(b'\n');
        out.extend_from_slice(data.as_bytes());
        out.extend_from_slice(payload);
        out
    }

    /// The `transcribe` data blob: the language tag. 18 bytes with [`LANGUAGE`] = `"en"`.
    pub fn transcribe_data() -> String {
        format!("{{\"{KEY_LANGUAGE}\": \"{LANGUAGE}\"}}")
    }

    /// The `audio-start`/`audio-chunk` data blob. 61 bytes: how to read the payload, so the
    /// service never has to infer it. `width` is bytes per sample ([`SAMPLE_WIDTH_BYTES`]),
    /// which is what the service hands `audioop.lin2lin`; the bit count would abort it.
    pub fn audio_data() -> String {
        format!(
            "{{\"{KEY_RATE}\": {SAMPLE_RATE_HZ}, \"{KEY_WIDTH}\": {SAMPLE_WIDTH_BYTES}, \
             \"{KEY_CHANNELS}\": {CHANNELS}, \"{KEY_TIMESTAMP}\": null}}"
        )
    }

    /// The `audio-stop` data blob. 19 bytes: the utterance ended, timestamp unknown.
    pub fn audio_stop_data() -> String {
        format!("{{\"{KEY_TIMESTAMP}\": null}}")
    }
}

/// Budget for a candidate that is **not** the last one, measured rather than guessed: a
/// healthy preferred tier answers the 6.6 s gate clip in 0.4 s (RTF 0.058 on the 5090) and
/// the worst legitimate first request after a reboot is 0.57 s, so 2.5 s is about 4× the
/// slowest healthy behaviour while staying far under the 10 s a caller will wait. This is
/// what catches the failure mode that matters — an ASR that accepts the connection, takes
/// the audio, and never answers.
pub const PROBE_TIMEOUT_MS: u64 = 2_500;

/// How long an endpoint that just failed is left alone before being probed again. Without
/// it, a GPU busy with someone else's job costs a probe on *every* utterance: 2.5 s of
/// dead air per command, forever. With it, that cost is paid once a minute.
pub const ENDPOINT_COOLDOWN: Duration = Duration::from_secs(60);

/// A Wyoming Parakeet endpoint, plus the ordered fallbacks to try when it is down
/// (plan `[parakeet] addr` / `[parakeet] fallback_addrs`).
///
/// Holds cooldown state, so there is exactly one client per process: `main` builds it once
/// and passes `&client` into every transcription task, which is what lets a tier's failure
/// be remembered across turns.
#[derive(Debug)]
pub struct WyomingClient {
    /// `host:port` from `parakeet.addr`. Always tried first.
    pub addr: String,
    /// `host:port` values from `parakeet.fallback_addrs`, in the order they should be tried.
    pub fallbacks: Vec<String>,
    /// When each endpoint last failed. Interior state so `transcribe` keeps taking `&self`
    /// and the health of a tier survives across the turns that need it.
    failed: Mutex<HashMap<String, Instant>>,
}

impl WyomingClient {
    /// A client for one endpoint, no fallbacks (no connection is made here).
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            fallbacks: Vec::new(),
            failed: Mutex::new(HashMap::new()),
        }
    }

    /// A client with a preferred endpoint and an ordered fallback list.
    pub fn with_fallbacks(addr: impl Into<String>, fallbacks: Vec<String>) -> Self {
        Self {
            addr: addr.into(),
            fallbacks,
            failed: Mutex::new(HashMap::new()),
        }
    }
}

/// Endpoints in the order to try them: preferred first, then fallbacks in config order, and
/// anything still in cooldown last rather than skipped — if every tier is down, continuing to
/// probe all of them is the only way back without a restart.
///
/// Returns that order **and the index of the attempt that must be allowed to finish**: the
/// last tier that is not in cooldown. That index is not simply the last entry. A cooled tier
/// still sits behind a working one, and if the working one were treated as "not last" it
/// would get the probe budget and be abandoned for being slow — which is a real failure, not
/// a hypothetical one: a CPU tier serving the household while the GPU is in cooldown needs
/// seconds, and was killed at 2.5 s by exactly that mistake (2026-11-02, turn B of the
/// deployment proof: `no transcript within 2500 ms` from the only working tier). With every
/// tier cooled, the last candidate overall gets that budget, so a service that is
/// cold-starting can still be reached.
fn candidate_order(client: &WyomingClient) -> (Vec<String>, usize) {
    let now = Instant::now();
    let failed = client.failed.lock().unwrap_or_else(PoisonError::into_inner);
    let mut ready = Vec::with_capacity(client.fallbacks.len() + 1);
    let mut cooled = Vec::new();
    for addr in std::iter::once(&client.addr).chain(client.fallbacks.iter()) {
        if ready.iter().chain(cooled.iter()).any(|seen| seen == addr) {
            continue; // the same address listed twice is one endpoint, not two
        }
        match failed.get(addr) {
            Some(until) if *until > now => cooled.push(addr.clone()),
            _ => ready.push(addr.clone()),
        }
    }
    let live = ready.len();
    let total = live + cooled.len();
    ready.extend(cooled);
    (ready, if live > 0 { live - 1 } else { total - 1 })
}

/// Transcribe PCM to text (plan S3.3), failing over across the configured tiers.
///
/// Opens a connection to the preferred endpoint, sends the framing described in [`framing`]
/// for `pcm` (16 kHz mono `i16`), and returns the transcript. If that endpoint is down,
/// refuses, or stays silent past [`PROBE_TIMEOUT_MS`], the next configured tier is tried.
/// Called from an async task with the utterance read out of the ring; must never be called
/// on the KWS thread.
///
/// The **last tier that is not in cooldown** is given the full *remaining* request
/// budget, not the probe budget: transcribing a long utterance on a CPU tier legitimately
/// takes seconds (RTF 0.5–1.8 × 6 s under load), and abandoning the only working server
/// because it is doing real work would turn a slow night into a failed one. A cooled tier
/// still counts as a candidate behind it, so "last" is not the end of the list — see
/// [`candidate_order`].
///
/// The tiers share **one** [`framing::REQUEST_TIMEOUT_MS`] deadline, computed when the
/// request starts (finding PK-1): a probe that consumed its 2.5 s leaves 7.5 s for the
/// tier behind it, it is not added to a fresh 10 s. Budgets that sum made the documented
/// caller budget a fiction — this deployment's own worst case, a wedged GPU tier followed
/// by a CPU tier measured at 9.75–9.99 s, cost 12.25–12.5 s under the summing rule.
///
/// An empty utterance is an error, not a request: it returns before connecting, so a
/// zero-sample capture can never hold a session open waiting for a transcript that will
/// not come.
pub async fn transcribe(client: &WyomingClient, pcm: &[i16]) -> anyhow::Result<String> {
    transcribe_within(client, pcm, Duration::from_millis(framing::REQUEST_TIMEOUT_MS)).await
}

/// [`transcribe`] with an explicit request-wide budget, so a test can prove the aggregate
/// deadline bites without waiting the documented 10 s (finding PK-1).
async fn transcribe_within(
    client: &WyomingClient,
    pcm: &[i16],
    overall: Duration,
) -> anyhow::Result<String> {
    if pcm.is_empty() {
        bail!(
            "refusing to transcribe an empty utterance (0 samples) for {}; nothing was sent",
            client.addr
        );
    }
    let request = encode_request(pcm);
    let (order, final_attempt) = candidate_order(client);
    let deadline = Instant::now() + overall;
    let mut failures = Vec::with_capacity(order.len());

    for (rank, addr) in order.iter().enumerate() {
        // Only the attempt that has to work gets the full budget; everything ahead of it is
        // a probe. See [`candidate_order`] for why that is the live boundary and not the
        // last entry in the list. Either way the budget is clamped to what the request-wide
        // deadline has actually left (PK-1): un-clamped budgets sum, so N tiers could cost
        // N budgets and the 10 s the caller was promised was never the real bound.
        let own = if rank < final_attempt {
            Duration::from_millis(PROBE_TIMEOUT_MS)
        } else {
            Duration::from_millis(framing::REQUEST_TIMEOUT_MS)
        };
        let budget = own.min(deadline.saturating_duration_since(Instant::now()));
        let attempt = tokio::time::timeout(budget, exchange(addr, &request)).await;
        match attempt {
            Ok(Ok(text)) => {
                client
                    .failed
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(addr);
                if *addr != client.addr {
                    // Keyed on **identity, not position in the attempt list.** Keying on
                    // `rank > 0` looked obviously right and was wrong where it mattered: a
                    // cooled preferred tier is demoted *behind* the fallback, so the
                    // fallback answers at rank 0 and the household's one signal that it is
                    // running degraded went silent — the common case, seen in the 2026-11-02
                    // deployment proof when the CPU tier served 4.4 s and 2.8 s turns with
                    // no warning at all. `rank` is still logged: a nonzero rank means the
                    // preferred tier is in cooldown, which is a different question from who
                    // answered.
                    tracing::warn!(
                        asr = %addr,
                        preferred = %client.addr,
                        rank,
                        tried = ?failures,
                        "transcribed by a fallback tier"
                    );
                }
                return Ok(text);
            }
            Ok(Err(err)) => failures.push(format!("{addr}: {err:#}")),
            Err(_elapsed) => failures.push(format!(
                "{addr}: no transcript within {} ms",
                budget.as_millis()
            )),
        }
        tracing::warn!(asr = %addr, failure = %failures.last().expect("just pushed"), "ASR tier failed");
        client
            .failed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(addr.clone(), Instant::now() + ENDPOINT_COOLDOWN);
    }

    bail!(
        "no ASR tier answered ({} tried): {}",
        order.len(),
        failures.join("; ")
    )
}

/// One request/response exchange on a fresh connection.
async fn exchange(addr: &str, request: &[u8]) -> anyhow::Result<String> {
    let mut stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("connecting to the Parakeet Wyoming service at {addr}"))?;
    // 64 ms chunks are already pacing the stream; latency matters more than coalescing.
    let _ = stream.set_nodelay(true);

    let (reader, mut writer) = stream.split();
    // Events are newline-delimited, so the reader needs buffering to find the line ends.
    let mut reader = BufReader::new(reader);
    writer
        .write_all(request)
        .await
        .with_context(|| format!("sending the transcription request to {addr}"))?;
    writer.flush().await.ok();

    loop {
        let (event, _payload) = read_message(&mut reader).await?;
        let kind = event.get(framing::KEY_TYPE).and_then(Value::as_str);
        if kind == Some(framing::EVENT_TRANSCRIPT) {
            // The transcript is the entire result of the request, so a `transcript`
            // event whose data blob carries no string `text` is a broken tier, not an
            // empty answer (finding PK-2): reading it as `""` marked the tier healthy,
            // silenced the fallback warning, and left the verify gate to file a false
            // accept while the configured healthy fallback went untried. An actual
            // `{"text": ""}` — the legitimate non-speech answer — still succeeds.
            let text = event
                .get(framing::KEY_DATA)
                .and_then(|data| data.get(framing::KEY_TEXT))
                .and_then(Value::as_str)
                .with_context(|| {
                    format!("a transcript event from {addr} has no string data.text")
                })?;
            return Ok(text.to_owned());
        }
        // Anything else before the result (the service echoes `audio-start`, for example)
        // is not the answer; keep reading until the transcript arrives.
    }
}

/// The whole uplink for one utterance: `transcribe`, `audio-start`, one `audio-chunk` per
/// 1024 samples (the last one short, never padded), `audio-stop`.
fn encode_request(pcm: &[i16]) -> Vec<u8> {
    let audio = framing::audio_data();
    let mut out = Vec::with_capacity(pcm.len() * framing::SAMPLE_WIDTH_BYTES + 4 * 160);

    out.extend_from_slice(&framing::frame(
        framing::EVENT_TRANSCRIBE,
        &framing::transcribe_data(),
        &[],
    ));
    out.extend_from_slice(&framing::frame(framing::EVENT_AUDIO_START, &audio, &[]));
    for chunk in pcm.chunks(framing::AUDIO_CHUNK_SAMPLES) {
        let mut payload = Vec::with_capacity(chunk.len() * framing::SAMPLE_WIDTH_BYTES);
        for sample in chunk {
            payload.extend_from_slice(&sample.to_le_bytes());
        }
        out.extend_from_slice(&framing::frame(
            framing::EVENT_AUDIO_CHUNK,
            &audio,
            &payload,
        ));
    }
    out.extend_from_slice(&framing::frame(
        framing::EVENT_AUDIO_STOP,
        &framing::audio_stop_data(),
        &[],
    ));
    out
}

/// Read one event: the header line, its `data` blob merged in under `data`, and the payload.
///
/// Generic over `AsyncBufRead` so the same decoder runs against a socket and, in tests,
/// against a byte slice — one decoder, so a framing bug cannot hide behind a test double.
async fn read_message<R>(reader: &mut R) -> anyhow::Result<(Value, Vec<u8>)>
where
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::with_capacity(96);
    // Take exactly the documented maximum (finding PK-3): a line of exactly
    // `MAX_LINE_BYTES` ending in its `\n` is legal, and anything longer is stopped
    // without a newline and rejected. Taking one byte more let a line over the limit
    // pass on the strength of a newline in the extra byte.
    let read = (&mut *reader)
        .take(framing::MAX_LINE_BYTES as u64)
        .read_until(b'\n', &mut line)
        .await
        .context("reading a Wyoming event line")?;
    if read == 0 {
        bail!("the Wyoming service closed the connection before sending an event");
    }
    if !line.ends_with(b"\n") {
        bail!(
            "a Wyoming event line ran past the {} byte limit without a newline",
            framing::MAX_LINE_BYTES
        );
    }
    let mut event: Value =
        serde_json::from_slice(&line[..line.len() - 1]).context("parsing a Wyoming event line")?;
    if !event.is_object() {
        bail!("a Wyoming event line is not a JSON object: {event}");
    }

    let data_len = length_field(&event, framing::KEY_DATA_LENGTH, framing::MAX_DATA_BYTES)?;
    if data_len > 0 {
        let mut blob = vec![0u8; data_len];
        reader
            .read_exact(&mut blob)
            .await
            .context("reading a Wyoming data blob")?;
        let data: Value = serde_json::from_slice(&blob).context("parsing a Wyoming data blob")?;
        if let Value::Object(map) = &mut event {
            map.insert(framing::KEY_DATA.to_owned(), data);
        }
    }

    let payload_len = length_field(
        &event,
        framing::KEY_PAYLOAD_LENGTH,
        framing::MAX_PAYLOAD_BYTES,
    )?;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        reader
            .read_exact(&mut payload)
            .await
            .context("reading a Wyoming payload")?;
    }
    Ok((event, payload))
}

/// A `*_length` line key, absent when the part is absent, and an error rather than a
/// truncated read when it is out of range or not a byte count.
fn length_field(event: &Value, key: &str, limit: usize) -> anyhow::Result<usize> {
    let Some(value) = event.get(key) else {
        return Ok(0);
    };
    let Some(len) = value.as_u64().and_then(|len| usize::try_from(len).ok()) else {
        bail!("Wyoming {key} is not a byte count: {value}");
    };
    if len > limit {
        bail!("Wyoming {key} {len} is over the {limit} byte limit");
    }
    Ok(len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    // Canonical bytes, captured from `wyoming` 1.8.0's own `async_write_event` against the
    // live service on 2026-11-02. Deliberately literals: a fixture built with our encoder
    // could only ever agree with itself, which is exactly the bug this pins.
    const TRANSCRIBE: &str =
        "{\"type\": \"transcribe\", \"version\": \"1.8.0\", \"data_length\": 18}\n\
         {\"language\": \"en\"}";
    const AUDIO_START: &str =
        "{\"type\": \"audio-start\", \"version\": \"1.8.0\", \"data_length\": 61}\n\
         {\"rate\": 16000, \"width\": 2, \"channels\": 1, \"timestamp\": null}";
    const AUDIO_CHUNK_2048: &str = "{\"type\": \"audio-chunk\", \"version\": \"1.8.0\", \"data_length\": 61, \"payload_length\": 2048}\n\
         {\"rate\": 16000, \"width\": 2, \"channels\": 1, \"timestamp\": null}";
    const AUDIO_CHUNK_14: &str = "{\"type\": \"audio-chunk\", \"version\": \"1.8.0\", \"data_length\": 61, \"payload_length\": 14}\n\
         {\"rate\": 16000, \"width\": 2, \"channels\": 1, \"timestamp\": null}";
    const AUDIO_STOP: &str =
        "{\"type\": \"audio-stop\", \"version\": \"1.8.0\", \"data_length\": 19}\n{\"timestamp\": null}";
    const TRANSCRIPT: &str =
        "{\"type\": \"transcript\", \"version\": \"1.8.0\", \"data_length\": 30}\n\
         {\"text\": \"turn on the lights\"}";

    /// `count` copies of one sample's little-endian bytes.
    fn pcm_bytes(sample: i16, count: usize) -> Vec<u8> {
        let bytes = sample.to_le_bytes();
        (0..count)
            .flat_map(|_| [bytes[0], bytes[1]])
            .collect::<Vec<u8>>()
    }

    /// The integer a fixture's header line announces for `key`.
    fn announced(line: &str, key: &str) -> usize {
        let marker = format!("\"{key}\": ");
        let (_, rest) = line
            .rsplit_once(marker.as_str())
            .unwrap_or_else(|| panic!("the line announces {key}: {line}"));
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        digits
            .parse()
            .unwrap_or_else(|_| panic!("{key} is not a byte count: {line}"))
    }

    #[test]
    fn fixtures_are_the_captured_bytes_and_their_data_lengths_agree() {
        assert_eq!(TRANSCRIBE.len(), 80);
        assert_eq!(AUDIO_START.len(), 124);
        assert_eq!(AUDIO_CHUNK_2048.len(), 148);
        assert_eq!(AUDIO_CHUNK_14.len(), 146);
        assert_eq!(AUDIO_STOP.len(), 81);
        assert_eq!(TRANSCRIPT.len(), 92);

        // Each `data_length` really is the length of the blob after its newline, and each
        // `payload_length` the payload the tests append.
        for fixture in [
            TRANSCRIBE,
            AUDIO_START,
            AUDIO_CHUNK_2048,
            AUDIO_CHUNK_14,
            AUDIO_STOP,
            TRANSCRIPT,
        ] {
            let (line, blob) = fixture
                .split_once('\n')
                .expect("a fixture is a line and a blob");
            assert_eq!(announced(line, "data_length"), blob.len(), "{line}");
        }
        for (fixture, payload_bytes) in [(AUDIO_CHUNK_2048, 2048), (AUDIO_CHUNK_14, 14)] {
            let (line, _) = fixture.split_once('\n').expect("a fixture is a line");
            assert_eq!(announced(line, "payload_length"), payload_bytes, "{line}");
        }
    }

    #[test]
    fn framing_constants_are_the_pinned_shape() {
        assert_eq!(
            framing::PROTOCOL_VERSION,
            "1.8.0",
            "the wyoming version we interop-tested"
        );
        assert_eq!(framing::AUDIO_CHUNK_SAMPLES, 1024);
        assert_eq!(framing::AUDIO_CHUNK_BYTES, 2048);
        assert_eq!(
            (
                framing::SAMPLE_RATE_HZ,
                framing::SAMPLE_WIDTH_BYTES,
                framing::CHANNELS
            ),
            (16_000, 2, 1),
            "`width` on the wire is bytes per sample, so 16-bit PCM is 2 — a 16 there is a
             bit count and the service aborts on it in `audioop.lin2lin`"
        );
        assert_eq!(framing::REQUEST_TIMEOUT_MS, 10_000);
        assert_eq!(framing::EVENT_TRANSCRIBE, "transcribe");
        assert_eq!(framing::EVENT_AUDIO_START, "audio-start");
        assert_eq!(framing::EVENT_AUDIO_CHUNK, "audio-chunk");
        assert_eq!(framing::EVENT_AUDIO_STOP, "audio-stop");
        assert_eq!(framing::EVENT_TRANSCRIPT, "transcript");
        assert_eq!(framing::KEY_TYPE, "type");
        assert_eq!(framing::KEY_DATA, "data");
        assert_eq!(framing::KEY_TEXT, "text");
        assert_eq!(framing::KEY_DATA_LENGTH, "data_length");
        assert_eq!(framing::KEY_PAYLOAD_LENGTH, "payload_length");
        assert_eq!(framing::LANGUAGE, "en");

        // The data blobs are the captured bytes, byte for byte — separators included.
        assert_eq!(framing::transcribe_data(), "{\"language\": \"en\"}");
        assert_eq!(
            framing::audio_data(),
            "{\"rate\": 16000, \"width\": 2, \"channels\": 1, \"timestamp\": null}"
        );
        assert_eq!(framing::audio_stop_data(), "{\"timestamp\": null}");
        assert_eq!(framing::transcribe_data().len(), 18);
        assert_eq!(framing::audio_data().len(), 61);
        assert_eq!(framing::audio_stop_data().len(), 19);
    }

    /// The width the service hands `audioop.lin2lin`. A bit count (`16`) parses cleanly and
    /// then aborts the service with `Size should be 1, 2, 3 or 4`, so it is pinned apart from
    /// the fixtures above: those were themselves regenerated once with the wrong value and
    /// still agreed with themselves.
    #[test]
    fn audio_chunks_announce_sample_width_in_bytes_not_bits() {
        let request = encode_request(&[0i16; framing::AUDIO_CHUNK_SAMPLES]);
        let text = String::from_utf8(request).expect("the framing is UTF-8");
        assert!(
            text.contains("\"width\": 2"),
            "the audio data blob must say width 2 (bytes per sample): {text}"
        );
        assert!(
            !text.contains("\"width\": 16"),
            "a bit count in `width` kills the service inside audioop.lin2lin"
        );
    }

    #[test]
    fn one_full_chunk_is_byte_identical_to_the_captured_request() {
        let pcm = vec![0x0102i16; framing::AUDIO_CHUNK_SAMPLES];
        let payload = pcm_bytes(0x0102, framing::AUDIO_CHUNK_SAMPLES);
        let expected = [
            TRANSCRIBE.as_bytes(),
            AUDIO_START.as_bytes(),
            AUDIO_CHUNK_2048.as_bytes(),
            payload.as_slice(),
            AUDIO_STOP.as_bytes(),
        ]
        .concat();

        assert_eq!(expected.len(), 80 + 124 + 2196 + 81);
        assert_eq!(encode_request(&pcm), expected);
    }

    #[test]
    fn request_splits_at_1024_samples_and_never_pads_the_tail() {
        let pcm = vec![7i16; framing::AUDIO_CHUNK_SAMPLES * 2 + 7];
        let full = pcm_bytes(7, framing::AUDIO_CHUNK_SAMPLES);
        let tail = pcm_bytes(7, 7);
        assert_eq!(tail.len(), 14);
        let expected = [
            TRANSCRIBE.as_bytes(),
            AUDIO_START.as_bytes(),
            AUDIO_CHUNK_2048.as_bytes(),
            full.as_slice(),
            AUDIO_CHUNK_2048.as_bytes(),
            full.as_slice(),
            AUDIO_CHUNK_14.as_bytes(),
            tail.as_slice(),
            AUDIO_STOP.as_bytes(),
        ]
        .concat();

        assert_eq!(encode_request(&pcm), expected);
    }

    #[tokio::test]
    async fn transcript_text_is_read_from_the_data_blob() {
        let mut cursor = TRANSCRIPT.as_bytes();
        let (event, payload) = read_message(&mut cursor).await.expect("decodes");
        assert_eq!(event[framing::KEY_TYPE], Value::String("transcript".into()));
        assert_eq!(
            event[framing::KEY_DATA][framing::KEY_TEXT],
            Value::String("turn on the lights".into()),
            "the text arrives in the data blob, not inline"
        );
        assert!(payload.is_empty(), "a transcript event carries no payload");

        // Header-only events with a blob merge it the same way (`audio-stop` is 19 bytes).
        let mut cursor = AUDIO_STOP.as_bytes();
        let (stop, _) = read_message(&mut cursor).await.expect("decodes");
        assert!(stop[framing::KEY_DATA][framing::KEY_TIMESTAMP].is_null());
    }

    /// The payload is read by count, not by scanning: an `audio-chunk` carries 2048 bytes
    /// after its 61-byte data blob, and reading the wrong number desynchronises every event
    /// after it. Decoding the `audio-stop` that follows is what proves the count was exact.
    #[tokio::test]
    async fn audio_chunk_reads_its_data_blob_then_exactly_payload_length_bytes() {
        let stream = [
            AUDIO_CHUNK_2048.as_bytes(),
            pcm_bytes(0x0102, framing::AUDIO_CHUNK_SAMPLES).as_slice(),
            AUDIO_STOP.as_bytes(),
        ]
        .concat();
        let mut cursor = stream.as_slice();

        let (chunk, payload) = read_message(&mut cursor).await.expect("decodes chunk");
        assert_eq!(
            chunk[framing::KEY_TYPE],
            Value::String("audio-chunk".into())
        );
        assert_eq!(
            chunk[framing::KEY_DATA][framing::KEY_RATE],
            Value::from(16_000_u64),
            "the data blob is merged before the payload is read"
        );
        assert_eq!(payload, pcm_bytes(0x0102, framing::AUDIO_CHUNK_SAMPLES));

        let (stop, _) = read_message(&mut cursor).await.expect("decodes stop");
        assert_eq!(stop[framing::KEY_TYPE], Value::String("audio-stop".into()));
        assert!(stop[framing::KEY_DATA][framing::KEY_TIMESTAMP].is_null());
    }

    #[tokio::test]
    async fn read_message_rejects_a_line_without_a_newline() {
        let long = vec![b'a'; framing::MAX_LINE_BYTES + 8];
        let mut cursor = long.as_slice();
        let err = read_message(&mut cursor).await.unwrap_err();
        assert!(err.to_string().contains("newline"), "{err}");
    }

    #[tokio::test]
    async fn transcribe_sends_the_captured_bytes_and_reads_the_transcript_blob() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
        let addr = listener.local_addr().expect("local addr");

        // 1 full chunk + a 7-sample tail: 2048-byte payload, then a 14-byte one.
        let pcm = vec![7i16; framing::AUDIO_CHUNK_SAMPLES + 7];
        let full = pcm_bytes(7, framing::AUDIO_CHUNK_SAMPLES);
        let tail = pcm_bytes(7, 7);
        let expected = [
            TRANSCRIBE.as_bytes(),
            AUDIO_START.as_bytes(),
            AUDIO_CHUNK_2048.as_bytes(),
            full.as_slice(),
            AUDIO_CHUNK_14.as_bytes(),
            tail.as_slice(),
            AUDIO_STOP.as_bytes(),
        ]
        .concat();

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accepts");
            // Read exactly the request: a short or padded uplink shows up as a mismatch.
            let mut seen = vec![0u8; expected.len()];
            socket
                .read_exact(&mut seen)
                .await
                .expect("reads the whole request");
            assert_eq!(
                seen, expected,
                "the request on the wire is the captured bytes"
            );
            // An unrelated captured event first: the client must skip to the transcript.
            socket
                .write_all(AUDIO_START.as_bytes())
                .await
                .expect("echo");
            socket
                .write_all(TRANSCRIPT.as_bytes())
                .await
                .expect("reply");
        });

        let client = WyomingClient::new(addr.to_string());
        let text = transcribe(&client, &pcm).await.expect("transcribes");
        assert_eq!(text, "turn on the lights");
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn empty_pcm_errors_cleanly_without_connecting() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
        let addr = listener.local_addr().expect("local addr");
        let client = WyomingClient::new(addr.to_string());

        let err = tokio::time::timeout(Duration::from_secs(2), transcribe(&client, &[]))
            .await
            .expect("must not hang")
            .expect_err("empty audio is an error");
        assert!(err.to_string().contains("empty"), "{err}");

        assert!(
            tokio::time::timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err(),
            "an empty utterance must not open a connection"
        );
    }

    // --- tiered failover -------------------------------------------------------------
    //
    // These describe behaviour an operator would notice: which box answered, how long the
    // household waited for it, and whether a tier that failed once is punished forever.
    // Each mock counts its own connections, because "was this tier even touched?" is the
    // question that distinguishes a real fallback from one that merely exists.

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A Wyoming server that answers with the captured `transcript` event, counting every
    /// connection it accepts. The first `hang_first` connections are accepted, read and then
    /// never answered — an ASR wedged inside its model, holding the socket open so the
    /// client cannot notice by itself.
    ///
    /// `request_len` is the exact byte count the client will send, so the mock reads a real
    /// uplink before replying instead of racing a half-written request.
    async fn asr_server(
        request_len: usize,
        hang_first: usize,
        hits: Arc<AtomicUsize>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
        let addr = listener.local_addr().expect("local addr").to_string();
        let task = tokio::spawn(async move {
            let mut seen = 0usize;
            loop {
                let (mut socket, _) = listener.accept().await.expect("accepts");
                hits.fetch_add(1, Ordering::SeqCst);
                let hang = seen < hang_first;
                seen += 1;
                tokio::spawn(async move {
                    let mut request = vec![0u8; request_len];
                    if socket.read_exact(&mut request).await.is_err() {
                        return;
                    }
                    if hang {
                        tokio::time::sleep(Duration::from_secs(600)).await;
                        return;
                    }
                    let _ = socket.write_all(TRANSCRIPT.as_bytes()).await;
                    let _ = socket.flush().await;
                    // Hold the connection until the client has read the answer and hung up.
                    let mut drain = [0u8; 64];
                    let _ = socket.read(&mut drain).await;
                });
            }
        });
        (addr, task)
    }

    /// An address with nothing behind it: connecting is refused at once.
    async fn closed_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
        let addr = listener.local_addr().expect("local addr").to_string();
        drop(listener);
        addr
    }

    /// A tier that is healthy but **slow**: it reads the whole request, spends `think`
    /// doing the transcription, then answers. This is a CPU tier with a real utterance, and
    /// the reason the final attempt needs the full budget rather than the probe budget.
    async fn slow_asr_server(
        request_len: usize,
        think: Duration,
        hits: Arc<AtomicUsize>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
        let addr = listener.local_addr().expect("local addr").to_string();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.expect("accepts");
                hits.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut request = vec![0u8; request_len];
                    if socket.read_exact(&mut request).await.is_err() {
                        return;
                    }
                    tokio::time::sleep(think).await;
                    let _ = socket.write_all(TRANSCRIPT.as_bytes()).await;
                    let _ = socket.flush().await;
                    let mut drain = [0u8; 64];
                    let _ = socket.read(&mut drain).await;
                });
            }
        });
        (addr, task)
    }

    /// A short utterance and the exact request bytes it produces.
    fn utterance() -> (Vec<i16>, usize) {
        let pcm = vec![7i16; 160];
        let len = encode_request(&pcm).len();
        (pcm, len)
    }

    #[tokio::test]
    async fn a_healthy_preferred_tier_never_touches_the_fallback() {
        let (pcm, len) = utterance();
        let primary_hits = Arc::new(AtomicUsize::new(0));
        let fallback_hits = Arc::new(AtomicUsize::new(0));
        let (primary, _keep) = asr_server(len, 0, primary_hits.clone()).await;
        let (fallback, _keep) = asr_server(len, 0, fallback_hits.clone()).await;
        let client = WyomingClient::with_fallbacks(primary, vec![fallback]);

        let text = transcribe(&client, &pcm)
            .await
            .expect("the preferred tier answers");
        assert_eq!(text, "turn on the lights");
        assert_eq!(
            fallback_hits.load(Ordering::SeqCst),
            0,
            "a working preferred tier must not cost the household a second connection"
        );
    }

    #[tokio::test]
    async fn a_refusing_preferred_tier_falls_back_and_is_remembered_as_failed() {
        let (pcm, len) = utterance();
        let fallback_hits = Arc::new(AtomicUsize::new(0));
        let (fallback, _keep) = asr_server(len, 0, fallback_hits.clone()).await;
        let dead = closed_addr().await;
        let client = WyomingClient::with_fallbacks(dead.clone(), vec![fallback]);

        let started = Instant::now();
        let text = transcribe(&client, &pcm)
            .await
            .expect("the fallback answers");
        assert_eq!(text, "turn on the lights");
        assert_eq!(fallback_hits.load(Ordering::SeqCst), 1);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "a refused connection is an answer, not a wait: {:?}",
            started.elapsed()
        );
        assert!(
            client.failed.lock().unwrap().contains_key(&dead),
            "the tier that just failed must be in cooldown"
        );
    }

    #[tokio::test]
    async fn a_wedged_preferred_tier_is_abandoned_at_the_probe_budget_not_the_request_timeout() {
        let (pcm, len) = utterance();
        let primary_hits = Arc::new(AtomicUsize::new(0));
        // The first connection to the preferred tier is swallowed: it accepts the audio and
        // says nothing, which is the failure a caller cannot tell from a dead service.
        let (wedged, _keep) = asr_server(len, 1, primary_hits.clone()).await;
        let (fallback, _keep) = asr_server(len, 0, Arc::new(AtomicUsize::new(0))).await;
        let client = WyomingClient::with_fallbacks(wedged, vec![fallback]);

        let started = Instant::now();
        let text = transcribe(&client, &pcm)
            .await
            .expect("the fallback carries the turn");
        let elapsed = started.elapsed();

        assert_eq!(text, "turn on the lights");
        assert!(
            elapsed < Duration::from_millis(PROBE_TIMEOUT_MS + 1_000),
            "a wedged tier must cost the probe budget, not the {request} ms the caller will \
             wait: {elapsed:?}",
            request = framing::REQUEST_TIMEOUT_MS
        );
        assert!(
            elapsed >= Duration::from_millis(PROBE_TIMEOUT_MS - 200),
            "it should have waited for the probe budget, not given up early: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn the_last_tier_is_let_to_finish_a_slow_transcription() {
        // RTF 0.48 on a CPU tier is ~5 s for a 10 s utterance, which is normal work, not a
        // fault. With no other tier to fall back to, the full request budget must apply —
        // abandoning the only server you have because it is busy would turn a slow night
        // into a failed one.
        let (pcm, len) = utterance();
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
        let addr = listener.local_addr().expect("local addr").to_string();
        let slow = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accepts");
            let mut request = vec![0u8; len];
            socket
                .read_exact(&mut request)
                .await
                .expect("reads request");
            tokio::time::sleep(Duration::from_millis(PROBE_TIMEOUT_MS * 2)).await;
            socket
                .write_all(TRANSCRIPT.as_bytes())
                .await
                .expect("replies");
            socket.flush().await.expect("flushes");
        });
        let client = WyomingClient::new(addr);

        let text = transcribe(&client, &pcm)
            .await
            .expect("a slow but working tier is not abandoned");
        assert_eq!(text, "turn on the lights");
        slow.await.expect("slow tier finished");
    }

    #[tokio::test]
    async fn a_failed_tier_is_left_alone_until_its_cooldown_and_then_preferred_again() {
        let (pcm, len) = utterance();
        let primary_hits = Arc::new(AtomicUsize::new(0));
        let fallback_hits = Arc::new(AtomicUsize::new(0));
        // The same port, busy once and free afterwards: someone else's GPU job.
        let (primary, _keep) = asr_server(len, 1, primary_hits.clone()).await;
        let (fallback, _keep) = asr_server(len, 0, fallback_hits.clone()).await;
        let client = WyomingClient::with_fallbacks(primary.clone(), vec![fallback.clone()]);

        // 1. The preferred tier wedges; the fallback carries the turn.
        assert_eq!(
            transcribe(&client, &pcm)
                .await
                .expect("fallback carries it"),
            "turn on the lights"
        );
        assert_eq!(primary_hits.load(Ordering::SeqCst), 1);

        // 2. The following turns must not pay for it again.
        for _ in 0..3 {
            assert_eq!(
                transcribe(&client, &pcm).await.expect("still served"),
                "turn on the lights"
            );
        }
        assert_eq!(
            primary_hits.load(Ordering::SeqCst),
            1,
            "a tier in cooldown must not be probed on every utterance"
        );
        assert_eq!(fallback_hits.load(Ordering::SeqCst), 4);

        // 3. Only the clock has passed. Preference must come back, not stay on the fallback.
        client.failed.lock().unwrap().clear();
        assert_eq!(
            transcribe(&client, &pcm).await.expect("recovered"),
            "turn on the lights"
        );
        assert_eq!(
            primary_hits.load(Ordering::SeqCst),
            2,
            "a recovered tier must be used again"
        );
        assert_eq!(
            fallback_hits.load(Ordering::SeqCst),
            4,
            "and the fallback must go quiet once the preferred tier is back"
        );
    }

    #[tokio::test]
    async fn when_no_tier_answers_every_tier_is_named_and_none_is_locked_out() {
        let (pcm, _len) = utterance();
        let first = closed_addr().await;
        let second = closed_addr().await;
        let client = WyomingClient::with_fallbacks(first.clone(), vec![second.clone()]);

        let err = transcribe(&client, &pcm).await.unwrap_err().to_string();
        assert!(
            err.contains(&first) && err.contains(&second),
            "the operator has to see which boxes failed: {err}"
        );
        assert!(
            err.contains("no ASR tier answered"),
            "and that this was the whole set: {err}"
        );

        // Everything is in cooldown now, and the next command must still try every one of
        // them: an ASR that is cold-starting has to become reachable again without
        // restarting listenrr, which is the only way back from a total outage.
        let again = transcribe(&client, &pcm).await.unwrap_err().to_string();
        assert!(
            again.contains(&first) && again.contains(&second),
            "a cooled tier must still be probed as a last resort: {again}"
        );
    }

    #[tokio::test]
    async fn tier_health_survives_the_arc_clone_every_transcription_task_gets() {
        // `main` spawns a task per turn with `self.parakeet.clone()`. If that clone copied
        // the cooldown map instead of sharing it, every turn would re-probe a wedged tier
        // and pay the probe budget forever — a bug that tests driving one client directly
        // cannot see, because they never take the path the binary does.
        let (pcm, len) = utterance();
        let primary_hits = Arc::new(AtomicUsize::new(0));
        let (wedged, _keep) = asr_server(len, usize::MAX, primary_hits.clone()).await;
        let (fallback, _keep) = asr_server(len, 0, Arc::new(AtomicUsize::new(0))).await;
        let shared = Arc::new(WyomingClient::with_fallbacks(wedged, vec![fallback]));

        for turn in 0..3 {
            let per_task = shared.clone();
            assert_eq!(
                transcribe(&per_task, &pcm)
                    .await
                    .unwrap_or_else(|e| panic!("turn {turn} failed: {e}")),
                "turn on the lights"
            );
        }
        assert_eq!(
            primary_hits.load(Ordering::SeqCst),
            1,
            "three turns across three tasks must probe the wedged tier once, not three times"
        );
    }

    #[tokio::test]
    async fn a_cooled_tier_does_not_demote_the_tier_that_is_actually_serving() {
        // The shape of the failure that reached the household on 2026-11-02, and the one a
        // single-tier test cannot see: the GPU tier is down and in cooldown, so the CPU tier
        // is tried first — but it is no longer the *last* entry in the list. Read naively,
        // that makes it "not the final attempt" and hands it the 2.5 s probe budget, so it
        // is abandoned mid-transcription and the turn fails with a usable ASR still running.
        // The log line said `no transcript within 2500 ms` about the only working tier.
        let (pcm, len) = utterance();
        let dead = closed_addr().await;
        let fallback_hits = Arc::new(AtomicUsize::new(0));
        let (slow, _keep) = slow_asr_server(
            len,
            Duration::from_millis(PROBE_TIMEOUT_MS * 2),
            fallback_hits.clone(),
        )
        .await;
        let client = WyomingClient::with_fallbacks(dead.clone(), vec![slow]);

        // Turn 1: preferred tier refused; the CPU tier is the last candidate and finishes.
        assert_eq!(
            transcribe(&client, &pcm).await.expect("first turn"),
            "turn on the lights"
        );
        assert!(
            client.failed.lock().unwrap().contains_key(&dead),
            "the dead tier must now be in cooldown, which is what makes turn 2 the interesting one"
        );

        // Turn 2: the dead tier sits behind the working one. It must not cost it the budget.
        let started = Instant::now();
        let text = transcribe(&client, &pcm)
            .await
            .expect("a tier in cooldown must not demote the tier that is actually serving");
        assert_eq!(text, "turn on the lights");
        assert!(
            started.elapsed() >= Duration::from_millis(PROBE_TIMEOUT_MS),
            "the slow tier should have been let finish, not cut off at the probe budget: {:?}",
            started.elapsed()
        );
        assert_eq!(fallback_hits.load(Ordering::SeqCst), 2);
    }

    /// PK-1: the contract is one request-wide [`framing::REQUEST_TIMEOUT_MS`], not one
    /// budget per tier. Two tiers that both stall must cost the request its deadline and
    /// nothing more; when each tier got a fresh budget this same shape cost a full probe
    /// plus a fresh full budget (the deployed pair measured 12.25–12.5 s against a
    /// documented 10 s).
    #[tokio::test]
    async fn stalling_tiers_share_the_request_deadline_instead_of_summing_their_budgets() {
        let (pcm, len) = utterance();
        let (first, _keep) = asr_server(len, usize::MAX, Arc::new(AtomicUsize::new(0))).await;
        let (second, _keep) = asr_server(len, usize::MAX, Arc::new(AtomicUsize::new(0))).await;
        let client = WyomingClient::with_fallbacks(first, vec![second]);

        let overall = Duration::from_millis(1_200);
        let started = Instant::now();
        let err = transcribe_within(&client, &pcm, overall)
            .await
            .expect_err("two wedged tiers are a failed request, not a permanent one");
        let elapsed = started.elapsed();
        assert!(err.to_string().contains("no ASR tier answered"), "{err}");
        assert!(
            elapsed < overall + Duration::from_millis(600),
            "the request must stop at its own deadline, not at the sum of the probe and \
             final budgets: {elapsed:?}"
        );
        assert!(
            elapsed >= overall - Duration::from_millis(200),
            "the first tier should have been given everything up to the deadline, not \
             abandoned early: {elapsed:?}"
        );
    }

    /// A one-shot Wyoming server: read the request, reply with these exact bytes, hold
    /// the socket until the client hangs up. For the replies the canned servers cannot
    /// express (a malformed transcript, an empty one).
    async fn replying_asr(request_len: usize, reply: &'static [u8]) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("binds");
        let addr = listener.local_addr().expect("local addr").to_string();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accepts");
            let mut request = vec![0u8; request_len];
            if socket.read_exact(&mut request).await.is_err() {
                return;
            }
            let _ = socket.write_all(reply).await;
            let _ = socket.flush().await;
            let mut drain = [0u8; 64];
            let _ = socket.read(&mut drain).await;
        });
        addr
    }

    /// PK-2: a `transcript` event whose data blob carries no `text` is a broken tier.
    /// Reading it as `Ok("")` marked that tier healthy, silenced the fallback warning,
    /// never tried the configured fallback, and left the verify gate to file the turn as
    /// a false accept.
    #[tokio::test]
    async fn a_transcript_event_without_text_fails_over_rather_than_succeeding_empty() {
        let (pcm, len) = utterance();
        let broken = replying_asr(
            len,
            b"{\"type\": \"transcript\", \"version\": \"1.8.0\", \"data_length\": 2}\n{}",
        )
        .await;
        let (fallback, _keep) = asr_server(len, 0, Arc::new(AtomicUsize::new(0))).await;
        let client = WyomingClient::with_fallbacks(broken, vec![fallback]);

        let text = transcribe(&client, &pcm)
            .await
            .expect("the malformed tier must fail over to the healthy one");
        assert_eq!(text, "turn on the lights");
    }

    /// The other side of PK-2: an actual `{"text": ""}` — the legitimate answer for
    /// non-speech audio — is a valid empty result, not a protocol fault.
    #[tokio::test]
    async fn an_empty_text_transcript_is_a_valid_empty_result() {
        let (pcm, len) = utterance();
        let quiet = replying_asr(
            len,
            b"{\"type\": \"transcript\", \"version\": \"1.8.0\", \"data_length\": 12}\n{\"text\": \"\"}",
        )
        .await;
        let client = WyomingClient::new(quiet);
        assert_eq!(
            transcribe(&client, &pcm)
                .await
                .expect("an empty transcript is an answer, not a fault"),
            ""
        );
    }

    /// PK-3: `MAX_LINE_BYTES` is documented as the maximum line size *including* its
    /// `\n`, so the boundary must be exact. Exactly the cap is accepted; one byte more
    /// is the cap error — not a longer line, and not a parse error.
    #[tokio::test]
    async fn a_line_of_exactly_the_cap_is_accepted_and_one_byte_more_is_not() {
        let header = b"{\"type\": \"audio-stop\"}";
        // Pad with trailing spaces: legal JSON whitespace, so the only thing under test
        // is the byte count.
        let pad = framing::MAX_LINE_BYTES - header.len() - 1; // -1 for the newline
        let mut at_cap = header.to_vec();
        at_cap.extend((0..pad).map(|_| b' '));
        at_cap.push(b'\n');
        assert_eq!(at_cap.len(), framing::MAX_LINE_BYTES, "exactly at the cap");

        let mut cursor = at_cap.as_slice();
        let (event, _) = read_message(&mut cursor)
            .await
            .expect("a line of exactly the cap is within the limit");
        assert_eq!(event[framing::KEY_TYPE], Value::String("audio-stop".into()));

        let mut over = at_cap.clone();
        over.insert(0, b' ');
        assert_eq!(over.len(), framing::MAX_LINE_BYTES + 1, "one byte over the cap");
        let mut cursor = over.as_slice();
        let err = read_message(&mut cursor).await.unwrap_err();
        assert!(
            err.to_string().contains("limit"),
            "one byte past the cap must be the cap error: {err}"
        );
    }
}
