//! Wire protocol: the frozen byte contract of plan §4 (the original design plan, not published).
//!
//! Transport is a persistent TCP stream (default `0.0.0.0:10700`). One connection
//! speaks, in this order:
//!
//! 1. **hello** — one `\n`-terminated JSON line, no tag byte:
//!    `{"v":1,"source_id":"kitchen","room":"kitchen","token":"<shared secret>","caps":{"has_speaker":true,"has_aec":false},"fw":"0.1.0"}`.
//!    An unknown `v` is rejected (`ProtoError::Version`); the connection is closed with a log.
//! 2. **audio frame** (up) — tag `0x01` + `seq: u32 LE` + exactly 2048 bytes of PCM
//!    (1024 samples × `i16` LE, 16 kHz mono). The frame itself is **2052 bytes**
//!    ([`AUDIO_FRAME_LEN`]: seq + PCM); with its [`TAG_AUDIO`] byte the record consumes
//!    2053 bytes from the socket ([`AUDIO_RECORD_LEN`]). The tag is record framing, not
//!    part of the frame — both constants exist so neither side has to do the arithmetic.
//!    `seq` starts at 0 and increments by 1 per frame; `seq * 1024` is the sample
//!    index into the source's [`crate::ring::SourceRing`].
//! 3. **event** (up) — tag `0x02` + `len: u16 LE` + JSON payload
//!    (`playback_done`, `rssi`, generic `button`, `save_buffer`).
//!    The satellite's "that was for you" button emits `save_buffer`, **never** `button`
//!    (plan B3-1: the binding lives in this contract, not in either side's code).
//! 4. **control** (down) — tag `0x03` + `len: u16 LE` + JSON payload (`led`, `earcon`, `play`).
//!
//! [`FrameDecoder`] is the incremental parser ingest feeds from the socket: it buffers
//! partial reads across `feed()` calls and never blocks.
//!
//! This module is a **contract file**: lanes MUST NOT edit it. If a signature or byte
//! here looks wrong, report the mismatch instead of changing it (see `docs/design/CONTRACT-NOTES.md`).

use serde::{Deserialize, Serialize};

/// Protocol version this backend speaks (`hello.v`). Any other value is rejected.
pub const PROTOCOL_VERSION: u32 = 1;

/// Uplink audio frame tag.
pub const TAG_AUDIO: u8 = 0x01;
/// Uplink event tag.
pub const TAG_EVENT: u8 = 0x02;
/// Downlink control tag.
pub const TAG_CONTROL: u8 = 0x03;

/// Samples per audio frame (64 ms at 16 kHz).
pub const FRAME_SAMPLES: usize = 1024;
/// PCM bytes per audio frame.
pub const FRAME_PCM_BYTES: usize = FRAME_SAMPLES * 2;
/// The audio **frame**: `seq(u32 LE)` + PCM = **2052 bytes**. This is the "2052 B frame"
/// of the frozen contract — the unit whose length the firmware and the tests agree on.
pub const AUDIO_FRAME_LEN: usize = 4 + FRAME_PCM_BYTES;
/// The audio frame as it appears **on the socket**: the [`TAG_AUDIO`] byte + the frame, so
/// 2053 bytes are consumed per frame. The tag is record framing, not part of the frame;
/// both numbers are stated because the plan's byte list (tag + seq + 2048 B PCM) sums to
/// 2053 while the contract names 2052 — see `docs/design/CONTRACT-NOTES.md`.
pub const AUDIO_RECORD_LEN: usize = 1 + AUDIO_FRAME_LEN;
/// Maximum JSON payload carried by an event/control message (`len` is `u16`).
pub const MAX_JSON_PAYLOAD: usize = u16::MAX as usize;
/// Bytes of the tag + `len: u16 LE` header on tagged JSON messages.
pub const TAGGED_HEADER_LEN: usize = 3;
/// Upper bound on a hello line's content (a trailing `\n`/`\r\n` is not counted); a
/// longer line is a protocol error, in every feed split alike.
pub const MAX_HELLO_LINE: usize = 4096;

/// Protocol-level framing/decoding failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtoError {
    /// `hello.v` is not [`PROTOCOL_VERSION`].
    Version { got: u32 },
    /// JSON was malformed, missing a required field, or carried an unknown variant.
    Json(String),
    /// A payload was not valid UTF-8.
    Utf8,
    /// A tag byte other than [`TAG_AUDIO`]/[`TAG_EVENT`] (uplink) or [`TAG_CONTROL`] (downlink).
    BadTag { tag: u8 },
    /// An audio record was shorter than [`AUDIO_RECORD_LEN`] bytes.
    ShortFrame { got: usize },
    /// An event/control `len` field exceeds [`MAX_JSON_PAYLOAD`].
    PayloadTooLong { got: usize },
    /// A tagged message's buffer is shorter than its declared length (needs more bytes).
    NeedMore { need: usize },
    /// A hello line exceeded [`MAX_HELLO_LINE`] bytes without a newline.
    LineTooLong,
}

impl std::fmt::Display for ProtoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtoError::Version { got } => write!(f, "unsupported protocol version {got}"),
            ProtoError::Json(msg) => write!(f, "invalid JSON payload: {msg}"),
            ProtoError::Utf8 => write!(f, "payload is not valid UTF-8"),
            ProtoError::BadTag { tag } => write!(f, "unexpected tag byte 0x{tag:02x}"),
            ProtoError::ShortFrame { got } => {
                write!(
                    f,
                    "audio record is {got} bytes, expected {AUDIO_RECORD_LEN}"
                )
            }
            ProtoError::PayloadTooLong { got } => {
                write!(f, "payload length {got} exceeds {MAX_JSON_PAYLOAD}")
            }
            ProtoError::NeedMore { need } => write!(f, "need {need} more bytes"),
            ProtoError::LineTooLong => write!(f, "hello line exceeds {MAX_HELLO_LINE} bytes"),
        }
    }
}

impl std::error::Error for ProtoError {}

impl From<serde_json::Error> for ProtoError {
    fn from(e: serde_json::Error) -> Self {
        ProtoError::Json(e.to_string())
    }
}

/// Constant-time byte comparison (XOR fold, hand-rolled — no dependency, plan §4).
///
/// Used by ingest for the hello token check so a network attacker cannot binary-search
/// the shared secret from response timing. Length is compared structurally, not branched
/// on early.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    // Deliberately no early return: the accumulator carries the length difference too.
    let len = a.len();
    let len2 = b.len();
    let mut diff: u8 = 0;
    let mut i = 0;
    while i < len && i < len2 {
        diff |= a[i] ^ b[i];
        i += 1;
    }
    diff |= (len ^ len2) as u8;
    // A length difference longer than 255 bytes must not alias to 0.
    diff |= ((len ^ len2) >> 8) as u8;
    diff == 0
}

/// Satellite capabilities advertised in hello (`caps`). Drives LED/earcon/`play` use
/// and the AEC assumption; both default false so a minimal client stays valid.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Caps {
    /// Satellite has a speaker (can play TTS via the `play` control / `media_player`).
    #[serde(default)]
    pub has_speaker: bool,
    /// Satellite has acoustic echo cancellation hardware/firmware support.
    #[serde(default)]
    pub has_aec: bool,
}

/// The hello handshake (first line on every connection).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// Protocol version; must be [`PROTOCOL_VERSION`].
    pub v: u32,
    /// Stable source identifier, matching a `[[sources]]` id (or unknown → policy).
    pub source_id: String,
    /// Room the source sits in (log/clip metadata, group resolution aid).
    pub room: String,
    /// Shared secret, required when `server.auth_token` is configured (R11).
    #[serde(default)]
    pub token: Option<String>,
    /// Advertised capabilities.
    #[serde(default)]
    pub caps: Caps,
    /// Firmware version string of the satellite (diagnostics only).
    #[serde(default)]
    pub fw: String,
}

/// Decode one hello line. Trailing `\r`/`\n` characters are stripped, so a blank line
/// after the hello is tolerated; the remaining bytes must be exactly the JSON object.
///
/// Errors: [`ProtoError::Version`] for an unknown `v`, [`ProtoError::Json`] for
/// malformed/missing-field JSON, [`ProtoError::Utf8`] for non-UTF-8 bytes.
pub fn decode_hello_line(line: &[u8]) -> Result<Hello, ProtoError> {
    let trimmed = strip_line_terminator(line);
    if trimmed.is_empty() {
        return Err(ProtoError::Json("empty hello line".into()));
    }
    let text = std::str::from_utf8(trimmed).map_err(|_| ProtoError::Utf8)?;
    let hello: Hello = serde_json::from_str(text)?;
    if hello.v != PROTOCOL_VERSION {
        return Err(ProtoError::Version { got: hello.v });
    }
    Ok(hello)
}

/// Encode a hello line (JSON + one `\n`) — used by `ingest`'s unit tests and by the
/// firmware's hello conformance test. The S7 scripted client deliberately does **not** use
/// this: it hand-encodes from `docs/protocol.md` so a wire-format change cannot move both
/// sides of the AC10 test together.
pub fn encode_hello_line(hello: &Hello) -> Vec<u8> {
    let mut out = serde_json::to_vec(hello).expect("Hello always serializes");
    out.push(b'\n');
    out
}

/// Strip trailing line terminators. Every trailing `\r`/`\n` is removed, so a hello
/// followed by a blank line still decodes (an empty extra line carries no information;
/// a second *non-empty* line is a JSON error, which is the case that matters).
fn strip_line_terminator(line: &[u8]) -> &[u8] {
    let mut end = line.len();
    while end > 0 && (line[end - 1] == b'\n' || line[end - 1] == b'\r') {
        end -= 1;
    }
    &line[..end]
}

/// One uplink audio frame: sequence number plus 1024 little-endian `i16` samples.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioFrame {
    /// Frame sequence number; `seq * FRAME_SAMPLES` is the first sample's index.
    pub seq: u32,
    /// Exactly [`FRAME_SAMPLES`] samples.
    pub pcm: Vec<i16>,
}

impl AudioFrame {
    /// Sample index of this frame's first sample.
    pub fn first_sample(&self) -> u64 {
        u64::from(self.seq) * FRAME_SAMPLES as u64
    }
    /// Sample index one past this frame's last sample.
    pub fn next_sample(&self) -> u64 {
        self.first_sample() + FRAME_SAMPLES as u64
    }
}

/// Encode an audio frame as its on-socket record: the [`TAG_AUDIO`] byte followed by the
/// exact 2052-byte frame ([`AUDIO_FRAME_LEN`]), i.e. [`AUDIO_RECORD_LEN`] bytes total.
///
/// The sample count is enforced rather than padded or truncated: a `pcm` whose length is
/// not [`FRAME_SAMPLES`] is a programming error, so this returns an empty `Vec` instead of
/// bytes (callers build frames from parsed data or a WAV slice).
pub fn encode_audio_frame(frame: &AudioFrame) -> Vec<u8> {
    if frame.pcm.len() != FRAME_SAMPLES {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(AUDIO_RECORD_LEN);
    out.push(TAG_AUDIO);
    out.extend_from_slice(&frame.seq.to_le_bytes());
    for s in &frame.pcm {
        out.extend_from_slice(&s.to_le_bytes());
    }
    out
}

/// Parse a complete on-socket audio record: the [`TAG_AUDIO`] byte plus an
/// [`AUDIO_FRAME_LEN`]-byte frame ([`AUDIO_RECORD_LEN`] bytes total).
///
/// Errors: [`ProtoError::ShortFrame`] when shorter, [`ProtoError::BadTag`] for a wrong
/// tag byte. Extra trailing bytes are ignored (the decoder slices precisely).
pub fn parse_audio_frame(buf: &[u8]) -> Result<AudioFrame, ProtoError> {
    if buf.len() < AUDIO_RECORD_LEN {
        return Err(ProtoError::ShortFrame { got: buf.len() });
    }
    if buf[0] != TAG_AUDIO {
        return Err(ProtoError::BadTag { tag: buf[0] });
    }
    let seq = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]);
    let mut pcm = Vec::with_capacity(FRAME_SAMPLES);
    for chunk in buf[5..AUDIO_RECORD_LEN].as_chunks::<2>().0 {
        pcm.push(i16::from_le_bytes(*chunk));
    }
    Ok(AudioFrame { seq, pcm })
}

/// Uplink event (`0x02` + len + JSON).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// Satellite finished playing (media_player went idle) — drives Speaking→FollowUp.
    #[serde(rename = "playback_done")]
    PlaybackDone,
    /// Wi-Fi signal report, sent every 30 s by the firmware (per-source metrics).
    Rssi { dbm: f32 },
    /// A generic physical button (the "for you" button is NOT here, see [`Event::SaveBuffer`]).
    Button { name: String },
    /// Missed-wake trigger: "that was for you". Backend writes the source's last 10 s
    /// to `clips/missed/` (plan B3-1/S8.2). Never confused with [`Event::Button`].
    #[serde(rename = "save_buffer")]
    SaveBuffer,
}

/// Decode an event JSON payload (no tag/length header).
pub fn parse_event_json(payload: &[u8]) -> Result<Event, ProtoError> {
    let text = std::str::from_utf8(payload).map_err(|_| ProtoError::Utf8)?;
    Ok(serde_json::from_str::<Event>(text)?)
}

/// Encode an event as `0x02` + len(u16 LE) + JSON.
pub fn encode_event(event: &Event) -> Vec<u8> {
    encode_tagged(
        TAG_EVENT,
        &serde_json::to_vec(event).expect("Event always serializes"),
    )
}

/// Parse a complete tagged event message (header + payload).
pub fn parse_event(buf: &[u8]) -> Result<Event, ProtoError> {
    let payload = parse_tagged(buf, TAG_EVENT)?;
    parse_event_json(payload)
}

/// LED state requested by the backend (`led` control). Serialized as lowercase strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LedState {
    /// Neutral/off.
    Idle,
    /// Wake detected, capturing.
    Listening,
    /// TTS playing.
    Speaking,
    /// Source muted (mute entity on, or suppressed).
    Muted,
}

/// Downlink control message (`0x03` + len + JSON).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Control {
    /// Set the status LED.
    Led { state: LedState },
    /// Play a locally-stored acknowledgement sound.
    Earcon { name: String },
    /// Fetch and play a URL on the satellite. Reserved for the later direct-TTS path
    /// (plan O2); HA `play_media` ships first.
    Play { url: String, format: String },
    /// End the conversation and stop any in-flight playback. Tablet-only: the ESP
    /// firmware refuses unknown control types and disconnects, so the backend must
    /// never send this to a `playback="ha"` source.
    Stop,
}

/// Decode a control JSON payload (no tag/length header). Used by firmware contract tests.
pub fn parse_control_json(payload: &[u8]) -> Result<Control, ProtoError> {
    let text = std::str::from_utf8(payload).map_err(|_| ProtoError::Utf8)?;
    Ok(serde_json::from_str::<Control>(text)?)
}

/// Encode a control message as `0x03` + len(u16 LE) + JSON.
pub fn encode_control(control: &Control) -> Vec<u8> {
    encode_tagged(
        TAG_CONTROL,
        &serde_json::to_vec(control).expect("Control always serializes"),
    )
}

/// Parse a complete tagged control message (header + payload).
pub fn parse_control(buf: &[u8]) -> Result<Control, ProtoError> {
    let payload = parse_tagged(buf, TAG_CONTROL)?;
    parse_control_json(payload)
}

fn encode_tagged(tag: u8, json: &[u8]) -> Vec<u8> {
    assert!(
        json.len() <= MAX_JSON_PAYLOAD,
        "JSON payload of {} bytes exceeds the u16 length field",
        json.len()
    );
    let mut out = Vec::with_capacity(TAGGED_HEADER_LEN + json.len());
    out.push(tag);
    out.extend_from_slice(&(json.len() as u16).to_le_bytes());
    out.extend_from_slice(json);
    out
}

/// Validate a tagged message buffer's header and return its payload slice.
fn parse_tagged(buf: &[u8], expected_tag: u8) -> Result<&[u8], ProtoError> {
    if buf.is_empty() {
        return Err(ProtoError::NeedMore { need: 1 });
    }
    if buf[0] != expected_tag {
        return Err(ProtoError::BadTag { tag: buf[0] });
    }
    if buf.len() < TAGGED_HEADER_LEN {
        return Err(ProtoError::NeedMore {
            need: TAGGED_HEADER_LEN - buf.len(),
        });
    }
    let len = u16::from_le_bytes([buf[1], buf[2]]) as usize;
    if len > MAX_JSON_PAYLOAD {
        return Err(ProtoError::PayloadTooLong { got: len });
    }
    let total = TAGGED_HEADER_LEN + len;
    if buf.len() < total {
        return Err(ProtoError::NeedMore {
            need: total - buf.len(),
        });
    }
    Ok(&buf[TAGGED_HEADER_LEN..total])
}

/// A decoded uplink message: whatever the satellite sends after connect.
#[derive(Debug, Clone, PartialEq)]
pub enum UpMessage {
    /// The handshake line.
    Hello(Hello),
    /// An audio frame.
    Frame(AudioFrame),
    /// An event.
    Event(Event),
}

/// Incremental uplink parser: feed it socket bytes, it yields whole messages.
///
/// Byte-at-a-time feeding must produce exactly the same messages as one big feed, which
/// is what makes TCP boundaries invisible to ingest. The first message is always the
/// hello line (parsed as one `\n`-terminated JSON line); everything after is tagged
/// binary. The first unrecoverable error is latched: `feed` then returns no messages and
/// [`FrameDecoder::take_error`] yields it, so ingest can close the connection once, with
/// the reason in the log.
#[derive(Debug)]
pub struct FrameDecoder {
    buf: Vec<u8>,
    hello_seen: bool,
    error: Option<ProtoError>,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameDecoder {
    /// A decoder that expects a hello line first.
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            hello_seen: false,
            error: None,
        }
    }

    /// Append bytes and return every message that is now complete, in order.
    ///
    /// Incomplete messages stay buffered; they are not errors. While an error is held
    /// this returns an empty vector; [`FrameDecoder::take_error`] consumes it and re-arms
    /// the decoder.
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<UpMessage> {
        if self.error.is_some() {
            return Vec::new();
        }
        self.buf.extend_from_slice(bytes);
        let mut out = Vec::new();
        loop {
            if !self.hello_seen {
                match self.take_hello_line() {
                    Ok(Some(h)) => {
                        self.hello_seen = true;
                        out.push(UpMessage::Hello(h));
                    }
                    Ok(None) => break,
                    Err(e) => {
                        self.fail(e);
                        break;
                    }
                }
                continue;
            }
            if self.buf.is_empty() {
                break;
            }
            let tag = self.buf[0];
            match tag {
                TAG_AUDIO => {
                    if self.buf.len() < AUDIO_RECORD_LEN {
                        break; // wait for the rest of the PCM
                    }
                    let chunk: Vec<u8> = self.buf[..AUDIO_RECORD_LEN].to_vec();
                    match parse_audio_frame(&chunk) {
                        Ok(frame) => {
                            self.buf.drain(..AUDIO_RECORD_LEN);
                            out.push(UpMessage::Frame(frame));
                        }
                        Err(e) => {
                            self.fail(e);
                            break;
                        }
                    }
                }
                TAG_EVENT => match self.take_tagged(TAG_EVENT) {
                    Ok(Some(payload)) => match parse_event_json(&payload) {
                        Ok(ev) => out.push(UpMessage::Event(ev)),
                        Err(e) => {
                            self.fail(e);
                            break;
                        }
                    },
                    Ok(None) => break,
                    Err(e) => {
                        self.fail(e);
                        break;
                    }
                },
                other => {
                    // Control (0x03) is downlink-only; anything else is framing garbage.
                    self.fail(ProtoError::BadTag { tag: other });
                    break;
                }
            }
        }
        out
    }

    /// The latched error, if any.
    pub fn error(&self) -> Option<&ProtoError> {
        self.error.as_ref()
    }

    /// Take the latched error (leaves `None`).
    pub fn take_error(&mut self) -> Option<ProtoError> {
        self.error.take()
    }

    /// Whether the hello line has been accepted.
    pub fn saw_hello(&self) -> bool {
        self.hello_seen
    }

    /// Bytes currently buffered (observability / tests).
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }

    fn fail(&mut self, e: ProtoError) {
        self.error = Some(e);
        self.buf.clear();
    }

    /// Try to consume one hello line. `Ok(None)` means "need more bytes".
    ///
    /// The cap applies to the line's *content* — the bytes before the terminator —
    /// so a hello of up to and including [`MAX_HELLO_LINE`] bytes is accepted and a
    /// 4097th content byte is `LineTooLong` whether the `\n` (or `\r\n`) arrives in
    /// the same read or a later one. Both branches measure with the same
    /// terminator-stripping rule `decode_hello_line` applies, which is what keeps
    /// one-shot and byte-at-a-time feeding equivalent (F1).
    fn take_hello_line(&mut self) -> Result<Option<Hello>, ProtoError> {
        match self.buf.iter().position(|b| *b == b'\n') {
            None => {
                // No `\n` yet, so trailing `\r`s may still turn out to be part of a
                // `\r\n` terminator; only bytes that cannot be a terminator count.
                if strip_line_terminator(&self.buf).len() > MAX_HELLO_LINE {
                    Err(ProtoError::LineTooLong)
                } else {
                    Ok(None)
                }
            }
            Some(nl) => {
                let line: Vec<u8> = self.buf.drain(..=nl).collect();
                if strip_line_terminator(&line).len() > MAX_HELLO_LINE {
                    return Err(ProtoError::LineTooLong);
                }
                Ok(Some(decode_hello_line(&line)?))
            }
        }
    }

    /// Try to consume one tagged message's payload. `Ok(None)` means "need more bytes".
    fn take_tagged(&mut self, expected_tag: u8) -> Result<Option<Vec<u8>>, ProtoError> {
        if self.buf.len() < TAGGED_HEADER_LEN {
            // Enough for a tag? Check the tag early so garbage is not buffered forever.
            if !self.buf.is_empty() && self.buf[0] != expected_tag {
                return Err(ProtoError::BadTag { tag: self.buf[0] });
            }
            return Ok(None);
        }
        if self.buf[0] != expected_tag {
            return Err(ProtoError::BadTag { tag: self.buf[0] });
        }
        let len = u16::from_le_bytes([self.buf[1], self.buf[2]]) as usize;
        if len > MAX_JSON_PAYLOAD {
            return Err(ProtoError::PayloadTooLong { got: len });
        }
        let total = TAGGED_HEADER_LEN + len;
        if self.buf.len() < total {
            return Ok(None);
        }
        let payload: Vec<u8> = self.buf.drain(TAGGED_HEADER_LEN..total).collect();
        self.buf.drain(..TAGGED_HEADER_LEN);
        Ok(Some(payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hello_json() -> &'static str {
        r#"{"v":1,"source_id":"kitchen","room":"kitchen","token":"s3cr3t","caps":{"has_speaker":true,"has_aec":false},"fw":"0.1.0"}"#
    }

    fn sample(i: usize) -> i16 {
        // Deterministic pattern covering negative and positive values.
        ((i as i32 * 7919) % 60000 - 30000) as i16
    }

    fn pcm() -> Vec<i16> {
        (0..FRAME_SAMPLES).map(sample).collect()
    }

    #[test]
    fn hello_round_trips_and_encodes_one_newline_line() {
        let h = decode_hello_line(hello_json().as_bytes()).unwrap();
        assert_eq!(h.v, 1);
        assert_eq!(h.source_id, "kitchen");
        assert_eq!(h.room, "kitchen");
        assert_eq!(h.token.as_deref(), Some("s3cr3t"));
        assert!(h.caps.has_speaker);
        assert!(!h.caps.has_aec);
        assert_eq!(h.fw, "0.1.0");

        let wire = encode_hello_line(&h);
        assert_eq!(wire.last(), Some(&b'\n'));
        assert_eq!(wire.len(), hello_json().len() + 1);
        assert_eq!(&wire[..hello_json().len()], hello_json().as_bytes());
        assert_eq!(decode_hello_line(&wire).unwrap(), h);
    }

    #[test]
    fn hello_tolerates_crlf_and_rejects_extra_line_content() {
        let mut bytes = hello_json().as_bytes().to_vec();
        bytes.extend_from_slice(b"\r\n");
        assert!(decode_hello_line(&bytes).is_ok());
        // A blank extra line carries nothing: tolerated.
        let mut blank = hello_json().as_bytes().to_vec();
        blank.extend_from_slice(b"\n\n");
        assert!(decode_hello_line(&blank).is_ok());
        // A second non-empty line is not part of the hello: rejected.
        let mut two = hello_json().as_bytes().to_vec();
        two.extend_from_slice(b"\nnot-json\n");
        assert!(matches!(decode_hello_line(&two), Err(ProtoError::Json(_))));
    }

    #[test]
    fn hello_token_may_be_absent() {
        let json = r#"{"v":1,"source_id":"pi","room":"study","caps":{"has_speaker":false,"has_aec":false},"fw":"0.0.1"}"#;
        let h = decode_hello_line(json.as_bytes()).unwrap();
        assert_eq!(h.token, None);
        assert!(!h.caps.has_speaker);
    }

    /// The other spelling the field can arrive in: present but `null`. This is not a
    /// hypothetical — `encode_hello_line` emits exactly this for `token: None` (serde with
    /// no `skip_serializing_if`), so the reference encoder's own output is only accepted if
    /// `null` decodes. `docs/protocol.md` §3.1 documents both forms as equivalent and
    /// marks the key-absent one as what a client should send; this is the test behind it.
    #[test]
    fn hello_token_present_but_null_decodes_as_absent() {
        let json = r#"{"v":1,"source_id":"pi","room":"study","token":null,"caps":{"has_speaker":false,"has_aec":false},"fw":"0.0.1"}"#;
        let h = decode_hello_line(json.as_bytes()).unwrap();
        assert_eq!(h.token, None, "a null token is absent, not an empty secret");

        // And the shape the encoder actually produces round-trips to None as well, so a
        // client that echoes back what `encode_hello_line` wrote is never rejected.
        let wire = encode_hello_line(&Hello {
            v: PROTOCOL_VERSION,
            source_id: "pi".into(),
            room: "study".into(),
            token: None,
            caps: Caps::default(),
            fw: "0.0.1".into(),
        });
        assert!(
            wire.windows(12).any(|w| w == br#""token":null"#),
            "the encoder is expected to emit a null token; if this changes, update \
             docs/protocol.md §3.1 rather than deleting this assertion"
        );
        assert_eq!(decode_hello_line(&wire).unwrap().token, None);
    }

    #[test]
    fn hello_caps_may_be_absent_and_defaults_false() {
        let json = r#"{"v":1,"source_id":"pi","room":"study","fw":"x"}"#;
        let h = decode_hello_line(json.as_bytes()).unwrap();
        assert_eq!(h.caps, Caps::default());
    }

    #[test]
    fn hello_unknown_version_is_rejected() {
        let json = hello_json().replace(r#""v":1"#, r#""v":2"#);
        assert_eq!(
            decode_hello_line(json.as_bytes()),
            Err(ProtoError::Version { got: 2 })
        );
        let json0 = hello_json().replace(r#""v":1"#, r#""v":0"#);
        assert_eq!(
            decode_hello_line(json0.as_bytes()),
            Err(ProtoError::Version { got: 0 })
        );
    }

    #[test]
    fn hello_malformed_table() {
        // Local to this table: the `(input, predicate)` shape is only used here.
        type Case = (&'static str, fn(&ProtoError) -> bool);
        let cases: Vec<Case> = vec![
            ("not json at all", |e| matches!(e, ProtoError::Json(_))),
            ("", |e| matches!(e, ProtoError::Json(_))),
            ("{}", |e| matches!(e, ProtoError::Json(_))),
            // missing required fields
            (r#"{"v":1}"#, |e| matches!(e, ProtoError::Json(_))),
            (
                r#"{"v":1,"source_id":"k","room":"k","caps":{"has_speaker":1},"fw":""}"#,
                |e| matches!(e, ProtoError::Json(_)),
            ),
            // truncated JSON
            (r#"{"v":1,"source_id":"k","#, |e| {
                matches!(e, ProtoError::Json(_))
            }),
        ];
        for (input, ok) in cases {
            let err = decode_hello_line(input.as_bytes()).unwrap_err();
            assert!(ok(&err), "for {input:?} got {err:?}");
        }
        // Non-UTF-8 payload.
        let mut bytes = vec![0xff, 0xfe, 0x00, 0x01];
        bytes.push(b'\n');
        assert_eq!(decode_hello_line(&bytes), Err(ProtoError::Utf8));
    }

    #[test]
    fn audio_frame_is_exactly_2052_bytes_and_byte_exact() {
        let frame = AudioFrame { seq: 7, pcm: pcm() };
        let wire = encode_audio_frame(&frame);
        // The frozen numbers: 2052-byte frame, 2053 bytes consumed with its tag byte.
        assert_eq!(AUDIO_FRAME_LEN, 2052);
        assert_eq!(AUDIO_RECORD_LEN, 2053);
        assert_eq!(wire.len(), AUDIO_RECORD_LEN);
        assert_eq!(FRAME_PCM_BYTES, 2048);
        assert_eq!(wire[0], 0x01);
        assert_eq!(&wire[1..5], &7u32.to_le_bytes());
        assert_eq!(&wire[5..7], &frame.pcm[0].to_le_bytes());
        assert_eq!(
            &wire[5 + 2 * (FRAME_SAMPLES - 1)..],
            &frame.pcm[FRAME_SAMPLES - 1].to_le_bytes()
        );
        assert_eq!(parse_audio_frame(&wire).unwrap(), frame);
        assert_eq!(frame.first_sample(), 7 * 1024);
        assert_eq!(frame.next_sample(), 8 * 1024);
    }

    #[test]
    fn audio_frame_sequence_is_little_endian_not_big() {
        let frame = AudioFrame {
            seq: 0x0102_0304,
            pcm: vec![0; FRAME_SAMPLES],
        };
        let wire = encode_audio_frame(&frame);
        assert_eq!(&wire[1..5], [0x04, 0x03, 0x02, 0x01]);
    }

    #[test]
    fn audio_frame_malformed_table() {
        let good = encode_audio_frame(&AudioFrame { seq: 0, pcm: pcm() });
        // Short frame: one byte short of the record.
        assert_eq!(
            parse_audio_frame(&good[..AUDIO_RECORD_LEN - 1]),
            Err(ProtoError::ShortFrame {
                got: AUDIO_RECORD_LEN - 1
            })
        );
        // A bare 2052-byte frame (no tag byte) is not a record either.
        assert_eq!(
            parse_audio_frame(&good[1..]),
            Err(ProtoError::ShortFrame {
                got: AUDIO_FRAME_LEN
            })
        );
        // Empty.
        assert_eq!(
            parse_audio_frame(&[]),
            Err(ProtoError::ShortFrame { got: 0 })
        );
        // Bad tag byte.
        let mut bad = good.clone();
        bad[0] = 0x09;
        assert_eq!(
            parse_audio_frame(&bad),
            Err(ProtoError::BadTag { tag: 0x09 })
        );
        // Event tag is not an audio frame.
        let mut ev_tag = good.clone();
        ev_tag[0] = TAG_EVENT;
        assert_eq!(
            parse_audio_frame(&ev_tag),
            Err(ProtoError::BadTag { tag: TAG_EVENT })
        );
        // A wrong-length pcm never encodes.
        assert!(encode_audio_frame(&AudioFrame {
            seq: 0,
            pcm: vec![0; FRAME_SAMPLES - 1]
        })
        .is_empty());
    }

    #[test]
    fn event_round_trips_are_byte_exact() {
        let cases = vec![
            (Event::PlaybackDone, r#"{"type":"playback_done"}"#),
            (Event::Rssi { dbm: -62.5 }, r#"{"type":"rssi","dbm":-62.5}"#),
            (
                Event::Button {
                    name: "night_mode".into(),
                },
                r#"{"type":"button","name":"night_mode"}"#,
            ),
            (Event::SaveBuffer, r#"{"type":"save_buffer"}"#),
        ];
        for (event, json) in cases {
            let wire = encode_event(&event);
            assert_eq!(wire[0], 0x02);
            assert_eq!(u16::from_le_bytes([wire[1], wire[2]]) as usize, json.len());
            assert_eq!(&wire[TAGGED_HEADER_LEN..], json.as_bytes());
            assert_eq!(parse_event(&wire).unwrap(), event);
            assert_eq!(parse_event_json(json.as_bytes()).unwrap(), event);
        }
    }

    #[test]
    fn save_buffer_is_the_missed_wake_trigger_and_button_stays_generic() {
        // Plan B3-1: the "for you" button arrives as save_buffer; the generic button
        // example is night_mode. These strings are part of the frozen contract.
        let for_you = br#"{"type":"save_buffer"}"#.to_vec();
        assert_eq!(parse_event_json(&for_you).unwrap(), Event::SaveBuffer);
        let generic = br#"{"type":"button","name":"night_mode"}"#.to_vec();
        assert_eq!(
            parse_event_json(&generic).unwrap(),
            Event::Button {
                name: "night_mode".into()
            }
        );
        // No spelling of "button" may produce SaveBuffer.
        let button_json = String::from(r#"{"type":"button","name":"save_buffer"}"#);
        match parse_event_json(button_json.as_bytes()).unwrap() {
            Event::Button { name } => assert_eq!(name, "save_buffer"),
            other => panic!("button mapped to {other:?}"),
        }
    }

    #[test]
    fn event_malformed_table() {
        let mk = |json: &str| -> Vec<u8> {
            let mut v = vec![0x02];
            v.extend_from_slice(&(json.len() as u16).to_le_bytes());
            v.extend_from_slice(json.as_bytes());
            v
        };
        assert!(matches!(
            parse_event(&mk(r#"{"type":"nope"}"#)),
            Err(ProtoError::Json(_))
        ));
        assert!(matches!(
            parse_event(&mk(r#"{"type":"rssi"}"#)),
            Err(ProtoError::Json(_))
        ));
        // Truncated JSON.
        assert!(matches!(
            parse_event(&mk(r#"{"type":"button","na"#)),
            Err(ProtoError::Json(_))
        ));
        // Header says more bytes than present: the deficit is exact.
        let payload = b"{\"type\":}";
        let mut short = vec![0x02];
        short.extend_from_slice(&100u16.to_le_bytes());
        short.extend_from_slice(payload);
        assert_eq!(
            parse_event(&short),
            Err(ProtoError::NeedMore {
                need: TAGGED_HEADER_LEN + 100 - (TAGGED_HEADER_LEN + payload.len())
            })
        );
        // Wrong tag.
        let mut wrong = mk(r#"{"type":"playback_done"}"#);
        wrong[0] = 0x07;
        assert_eq!(parse_event(&wrong), Err(ProtoError::BadTag { tag: 0x07 }));
        // Non-UTF-8 payload.
        let mut bin = vec![0x02];
        bin.extend_from_slice(&2u16.to_le_bytes());
        bin.extend_from_slice(&[0xff, 0xfe]);
        assert_eq!(parse_event(&bin), Err(ProtoError::Utf8));
        // Empty header.
        assert_eq!(parse_event(&[]), Err(ProtoError::NeedMore { need: 1 }));
        assert_eq!(
            parse_event(&[0x02]),
            Err(ProtoError::NeedMore {
                need: TAGGED_HEADER_LEN - 1
            })
        );
    }

    #[test]
    fn control_round_trips_are_byte_exact() {
        let cases = vec![
            (
                Control::Led {
                    state: LedState::Idle,
                },
                r#"{"type":"led","state":"idle"}"#,
            ),
            (
                Control::Led {
                    state: LedState::Listening,
                },
                r#"{"type":"led","state":"listening"}"#,
            ),
            (
                Control::Led {
                    state: LedState::Speaking,
                },
                r#"{"type":"led","state":"speaking"}"#,
            ),
            (
                Control::Led {
                    state: LedState::Muted,
                },
                r#"{"type":"led","state":"muted"}"#,
            ),
            (
                Control::Earcon { name: "ack".into() },
                r#"{"type":"earcon","name":"ack"}"#,
            ),
            (
                Control::Play {
                    url: "http://ha.example.lan:8123/api/tts_proxy/abc".into(),
                    format: "flac".into(),
                },
                r#"{"type":"play","url":"http://ha.example.lan:8123/api/tts_proxy/abc","format":"flac"}"#,
            ),
            (Control::Stop, r#"{"type":"stop"}"#),
        ];
        for (control, json) in cases {
            let wire = encode_control(&control);
            assert_eq!(wire[0], 0x03);
            assert_eq!(u16::from_le_bytes([wire[1], wire[2]]) as usize, json.len());
            assert_eq!(&wire[TAGGED_HEADER_LEN..], json.as_bytes());
            assert_eq!(parse_control(&wire).unwrap(), control);
            assert_eq!(parse_control_json(json.as_bytes()).unwrap(), control);
        }
    }

    #[test]
    fn control_malformed_table() {
        let mk = |json: &str| -> Vec<u8> {
            let mut v = vec![0x03];
            v.extend_from_slice(&(json.len() as u16).to_le_bytes());
            v.extend_from_slice(json.as_bytes());
            v
        };
        assert!(matches!(
            parse_control(&mk(r#"{"type":"led","state":"rainbow"}"#)),
            Err(ProtoError::Json(_))
        ));
        assert!(matches!(
            parse_control(&mk(r#"{"type":"led"}"#)),
            Err(ProtoError::Json(_))
        ));
        assert!(matches!(
            parse_control(&mk(r#"{"type":"play","url":"x"}"#)),
            Err(ProtoError::Json(_))
        ));
        assert!(matches!(
            parse_control(&mk(r#"{"type""earcon"}"#)),
            Err(ProtoError::Json(_))
        ));
        let mut wrong = mk(r#"{"type":"earcon","name":"ack"}"#);
        wrong[0] = TAG_AUDIO;
        assert_eq!(parse_control(&wrong), Err(ProtoError::BadTag { tag: 0x01 }));
    }

    #[test]
    fn decoder_handles_byte_at_a_time_feeding() {
        let hello = encode_hello_line(&Hello {
            v: 1,
            source_id: "kitchen".into(),
            room: "kitchen".into(),
            token: Some("t".into()),
            caps: Caps {
                has_speaker: true,
                has_aec: false,
            },
            fw: "0.1.0".into(),
        });
        let f0 = AudioFrame { seq: 0, pcm: pcm() };
        let f1 = AudioFrame {
            seq: 1,
            pcm: pcm().into_iter().map(|s| s.wrapping_add(3)).collect(),
        };
        let mut stream = hello.clone();
        stream.extend_from_slice(&encode_audio_frame(&f0));
        stream.extend_from_slice(&encode_event(&Event::SaveBuffer));
        stream.extend_from_slice(&encode_audio_frame(&f1));
        stream.extend_from_slice(&encode_event(&Event::Rssi { dbm: -70.0 }));

        let mut dec = FrameDecoder::new();
        let mut got: Vec<UpMessage> = Vec::new();
        for b in stream.iter() {
            got.extend(dec.feed(std::slice::from_ref(b)));
            assert!(dec.error().is_none(), "unexpected error at {got:?}");
        }
        assert_eq!(dec.buffered(), 0);
        assert!(dec.saw_hello());
        assert_eq!(got.len(), 5);
        assert_eq!(got[0], UpMessage::Hello(decode_hello_line(&hello).unwrap()));
        match (&got[1], &got[3]) {
            (UpMessage::Frame(a), UpMessage::Frame(b)) => {
                assert_eq!(a, &f0);
                assert_eq!(b, &f1);
            }
            other => panic!("frames not decoded in order: {other:?}"),
        }
        assert_eq!(got[2], UpMessage::Event(Event::SaveBuffer));
        assert_eq!(got[4], UpMessage::Event(Event::Rssi { dbm: -70.0 }));

        // Feeding the same bytes in one call yields the identical sequence.
        let mut one_shot = FrameDecoder::new();
        let once = one_shot.feed(&stream);
        assert_eq!(once, got);
    }

    #[test]
    fn decoder_buffers_partial_frame_and_partial_event() {
        let mut dec = FrameDecoder::new();
        dec.feed(b"{\"v\":1,\"source_id\":\"a\",\"room\":\"a\"}\n");
        // Partial audio frame: tag + seq + half the PCM.
        let full = encode_audio_frame(&AudioFrame { seq: 3, pcm: pcm() });
        assert!(dec.feed(&full[..1000]).is_empty());
        assert_eq!(dec.buffered(), 1000);
        assert!(dec.error().is_none());
        let out = dec.feed(&full[1000..]);
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], UpMessage::Frame(_)));

        // Partial event header, then the rest.
        let ev = encode_event(&Event::PlaybackDone);
        assert!(dec.feed(&ev[..2]).is_empty());
        let out = dec.feed(&ev[2..]);
        assert_eq!(out, vec![UpMessage::Event(Event::PlaybackDone)]);
        assert_eq!(dec.buffered(), 0);
    }

    #[test]
    fn decoder_latches_frame_errors() {
        // Bad tag byte after hello.
        let mut dec = FrameDecoder::new();
        dec.feed(b"{\"v\":1,\"source_id\":\"a\",\"room\":\"a\"}\n");
        assert!(dec.feed(&[0x09, 0, 0, 1, 2]).is_empty());
        assert_eq!(dec.error(), Some(&ProtoError::BadTag { tag: 0x09 }));
        // While the error is held, further bytes produce nothing.
        assert!(dec.feed(&encode_event(&Event::SaveBuffer)).is_empty());
        // Taking the error consumes it and re-arms the decoder (ingest closes the
        // connection instead; this keeps the semantics unambiguous).
        assert_eq!(dec.take_error(), Some(ProtoError::BadTag { tag: 0x09 }));
        assert!(dec.error().is_none());
        assert_eq!(
            dec.feed(&encode_event(&Event::SaveBuffer)),
            vec![UpMessage::Event(Event::SaveBuffer)]
        );

        // Control tag arriving upstream is a protocol error.
        let mut dec2 = FrameDecoder::new();
        dec2.feed(b"{\"v\":1,\"source_id\":\"a\",\"room\":\"a\"}\n");
        dec2.feed(&encode_control(&Control::Led {
            state: LedState::Idle,
        }));
        assert_eq!(dec2.error(), Some(&ProtoError::BadTag { tag: TAG_CONTROL }));

        // Truncated JSON inside a correctly-framed event.
        let mut dec3 = FrameDecoder::new();
        dec3.feed(b"{\"v\":1,\"source_id\":\"a\",\"room\":\"a\"}\n");
        let mut bad = vec![0x02];
        let body = b"{\"type\":\"but";
        bad.extend_from_slice(&(body.len() as u16).to_le_bytes());
        bad.extend_from_slice(body);
        assert!(dec3.feed(&bad).is_empty());
        assert!(matches!(dec3.take_error(), Some(ProtoError::Json(_))));

        // Short audio frame that is complete-but-short cannot happen after framing (the
        // decoder waits), so a short frame is exercised via parse_audio_frame above.
    }

    #[test]
    fn decoder_rejects_unknown_hello_version_and_stops() {
        let mut dec = FrameDecoder::new();
        let out = dec.feed(b"{\"v\":7,\"source_id\":\"a\",\"room\":\"a\"}\n");
        assert!(out.is_empty());
        assert_eq!(dec.take_error(), Some(ProtoError::Version { got: 7 }));
        assert!(!dec.saw_hello());
    }

    #[test]
    fn decoder_rejects_unterminated_oversized_hello_line() {
        let mut dec = FrameDecoder::new();
        let big = vec![b'x'; MAX_HELLO_LINE + 1];
        assert!(dec.feed(&big).is_empty());
        assert_eq!(dec.take_error(), Some(ProtoError::LineTooLong));
    }

    /// A well-formed hello whose JSON is exactly `content_len` bytes (padding `fw`),
    /// so the cap boundary can be probed with a hello that would otherwise decode.
    fn hello_json_of_len(content_len: usize) -> String {
        let prefix = r#"{"v":1,"source_id":"k","room":"k","fw":"0.1.0"#;
        let suffix = "\"}";
        assert!(content_len > prefix.len() + suffix.len());
        let pad = content_len - prefix.len() - suffix.len();
        format!("{}{}{}", prefix, "x".repeat(pad), suffix)
    }

    #[test]
    fn decoder_enforces_hello_cap_identically_across_feed_boundaries() {
        // F1: the same bytes must not have two outcomes depending on how TCP splits
        // them. A hello of up to and including MAX_HELLO_LINE content bytes is
        // accepted — the `\n`/`\r\n` terminator is not counted — and the 4097th
        // content byte latches LineTooLong, terminated or not.
        for (content_len, accepted) in [(MAX_HELLO_LINE, true), (MAX_HELLO_LINE + 1, false)] {
            for terminator in [b"\n".as_slice(), b"\r\n".as_slice()] {
                let mut wire = hello_json_of_len(content_len).into_bytes();
                wire.extend_from_slice(terminator);

                let mut one_shot = FrameDecoder::new();
                let out_one = one_shot.feed(&wire);

                let mut byte_by_byte = FrameDecoder::new();
                let mut out_inc = Vec::new();
                for b in &wire {
                    out_inc.extend(byte_by_byte.feed(std::slice::from_ref(b)));
                }

                assert_eq!(
                    one_shot.error(),
                    byte_by_byte.error(),
                    "one-shot and byte-at-a-time must agree (len {content_len}, {terminator:?})"
                );
                if accepted {
                    assert!(one_shot.error().is_none(), "len {content_len} must be accepted");
                    assert_eq!(out_one.len(), 1);
                    assert!(matches!(out_one[0], UpMessage::Hello(_)));
                    assert_eq!(out_inc.len(), 1);
                    assert!(matches!(out_inc[0], UpMessage::Hello(_)));
                } else {
                    assert_eq!(one_shot.error(), Some(&ProtoError::LineTooLong));
                    assert!(out_one.is_empty() && out_inc.is_empty());
                }
            }
        }
    }

    #[test]
    fn constant_time_eq_matches_and_discriminates() {
        assert!(constant_time_eq(b"", b""));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(!constant_time_eq(b"a", b""));
        assert!(constant_time_eq(b"secret", b"secret"));
        // First byte differs.
        assert!(!constant_time_eq(b"xecret", b"secret"));
        // Last byte differs.
        assert!(!constant_time_eq(b"secre9", b"secret"));
        // Middle byte differs.
        assert!(!constant_time_eq(b"secret", b"secxet"));
        // Same prefix, differing length (must not alias to equal).
        assert!(!constant_time_eq(b"secret", b"secret1"));
        assert!(!constant_time_eq(b"secret1", b"secret"));
        // Length difference that is a multiple of 256 must still differ.
        let short = vec![0u8; 0];
        let long = vec![0u8; 256];
        assert!(!constant_time_eq(&short, &long));
        let a = vec![1u8; 300];
        let mut b = vec![1u8; 300];
        b[299] = 2;
        assert!(!constant_time_eq(&a, &b));
        assert!(constant_time_eq(&a, &a.clone()));
        // Token comparison usage.
        let token = b"s3cr3t";
        assert!(constant_time_eq(token, b"s3cr3t"));
        assert!(!constant_time_eq(token, b"s3cr3"));
    }

    #[test]
    fn constants_match_the_frozen_contract() {
        assert_eq!(TAG_AUDIO, 0x01);
        assert_eq!(TAG_EVENT, 0x02);
        assert_eq!(TAG_CONTROL, 0x03);
        assert_eq!(FRAME_SAMPLES, 1024);
        assert_eq!(FRAME_PCM_BYTES, 2048);
        assert_eq!(AUDIO_FRAME_LEN, 2052);
        assert_eq!(AUDIO_RECORD_LEN, 2053);
        assert_eq!(TAGGED_HEADER_LEN, 3);
        assert_eq!(MAX_JSON_PAYLOAD, 65535);
        assert_eq!(PROTOCOL_VERSION, 1);
    }
}
