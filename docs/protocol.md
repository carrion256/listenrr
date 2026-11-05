# listenrr source protocol — v1

Wire contract between `listenrr` (the backend) and any **source** that streams
microphone audio: the ESPHome `luna_stream` component shipped in this repo, and any
third-party device or script that speaks the same bytes.

This document is normative for the bytes. It is generated from `src/proto.rs`, which is
the contract file: every number, tag and JSON shape below is the value of a constant or
`serde` attribute in that module. If this document and `src/proto.rs` ever disagree,
`src/proto.rs` is correct and this file is a bug.

---

## 1. Constants

| Constant (`src/proto.rs`) | Value | Meaning |
|---|---|---|
| `PROTOCOL_VERSION` | `1` | The only accepted `hello.v` |
| `TAG_AUDIO` | `0x01` | Uplink audio frame record |
| `TAG_EVENT` | `0x02` | Uplink event record |
| `TAG_CONTROL` | `0x03` | Downlink control record |
| `FRAME_SAMPLES` | `1024` | PCM samples per audio frame (64 ms at 16 kHz) |
| `FRAME_PCM_BYTES` | `2048` | `FRAME_SAMPLES × 2` |
| `AUDIO_FRAME_LEN` | `2052` | The **frame**: `seq` (4 B) + PCM (2048 B) |
| `AUDIO_RECORD_LEN` | `2053` | The frame **as it sits on the socket**: tag byte (1 B) + frame |
| `TAGGED_HEADER_LEN` | `3` | `tag` (1 B) + `len` (`u16` LE, 2 B) |
| `MAX_JSON_PAYLOAD` | `65535` | `u16::MAX` — the largest `len` a tagged record can carry |
| `MAX_HELLO_LINE` | `4096` | Longest hello still accepted while no `\n` has arrived: 4096 bytes are tolerated, the **4097th** latches `LineTooLong` (§3, §10) |

Audio is fixed by `src/lib.rs`: `SAMPLE_RATE_HZ = 16_000`, mono, signed 16-bit
little-endian PCM. A frame is 64 ms; 15 s of audio is 240 000 samples (the ring depth).

## 2. Transport and stream shape

- **TCP**, server-side default `0.0.0.0:10700` (`server.listen`; any `host:port`, port
  non-zero). One TCP connection carries exactly **one source**.
- The source speaks first. The connection begins with a single **hello line**, which has
  no tag byte and is newline-terminated (JSON text). Everything after the hello is
  **binary records** introduced by a tag byte.
- Uplink after hello: any interleaving of audio-frame records (`0x01`) and event
  records (`0x02`).
- Downlink: the server may send control records (`0x03`) at any time after the hello. The
  server never sends a hello and never echoes audio.
- There are **no timestamps on the wire** and no clock agreement: ordering and timing are
  carried by `seq` (see §8).
- Records are not length-prefixed as a whole: an audio record is fixed-length, an
  event/control record carries a `u16` payload length. A decoder must therefore read the
  tag first and then apply that tag's rule.

## 3. Hello

One `\n`-terminated UTF-8 JSON object. Nothing else is on the line.

```json
{"v":1,"source_id":"kitchen","room":"kitchen","token":"<shared secret>","caps":{"has_speaker":true,"has_aec":false},"fw":"0.1.0"}
```

| Field | JSON type | Required | Default | Notes |
|---|---|---|---|---|
| `v` | number (`u32`) | **yes** | — | Must equal `PROTOCOL_VERSION` (`1`) |
| `source_id` | string | **yes** | — | Matched against `[[sources]].id`; drives admission (§5) |
| `room` | string | **yes** | — | Room metadata; for an unknown source it is the only room information |
| `token` | string | optional | absent | Required when `server.auth_token` is set (§4) |
| `caps` | object | optional | `{}` ⇒ both false | Capability object, see below |
| `caps.has_speaker` | bool | optional | `false` | Source can render audio itself |
| `caps.has_aec` | bool | optional | `false` | Source has echo cancellation |
| `fw` | string | optional | `""` | Firmware/software version, diagnostics only |

Rules and edge cases:

- **Version rule.** `hello.v` is compared *after* the JSON parses. Any value other than
  `1` — including `0` — is `ProtoError::Version { got }`; the server closes the connection
  and logs it. There is no negotiation, no downgrade, and no second attempt on the same
  connection.
- **Line termination.** The first `\n` ends the hello. Trailing `\r`/`\n` bytes are
  stripped, so `…}\r\n` is fine and a blank line after the hello is tolerated. A second
  **non-empty** line is part of the hello line as far as the decoder is concerned and is a
  JSON error.
- **Length limit.** The bound in `FrameDecoder::take_hello_line` is measured on the hello
  line's *content* — the bytes before the terminating `\n` or `\r\n` — and is the same
  whether the line arrives in one read or one byte at a time. A hello is accepted at **any
  length up to and including 4096 bytes**, whether it is already terminated or still waiting
  for its newline; a longer one, terminated or not, latches `ProtoError::LineTooLong` and the
  connection is closed. For input that has not yet seen its newline the latch fires on the
  **4097th** byte that cannot be part of a `\r\n` terminator. The terminator is not part of
  the count, so a 4096-byte line followed by `\n` or `\r\n` is valid. Real hellos are about
  100 bytes; the limit exists so a peer cannot grow the buffer forever, or push an oversized
  line through in a single read.
- **Encoding.** Non-UTF-8 bytes are `ProtoError::Utf8`. An empty line (just `\n`) is a JSON
  error (`"empty hello line"`).
- **Unknown JSON fields are ignored.** `Hello` is deserialized with serde's defaults (no
  `deny_unknown_fields`), so a newer client may add fields; it must not remove, rename or
  retype the ones above.
- Missing required fields (`v`, `source_id`, `room`), wrong types (e.g.
  `"caps":{"has_speaker":1}`), and truncated JSON are all `ProtoError::Json`.

### 3.1 A tokenless hello — the two spellings, and which one a fresh install really sends

`server.auth_token` is unset by default (§4, plan R11), so the configuration a new deployment
ships with has **no secret** and the hello carries no credential. Two byte-level spellings
produce that: omit the `token` key, or send `"token":null`. Both decode to a `Hello` whose
`token` is `None`, and both are accepted by a server with no `auth_token` configured. They are
**not**, however, equally what happens on a fresh install: the shipped ESPHome component's
builder writes the key unconditionally, so a real Luna satellite sends `"token":null`, while the
key-absent form is what a new third-party client should send and what the backend's own
scripted client test sends. Read the byte counts with that in mind.

```json
{"v":1,"source_id":"study-pi","room":"study","caps":{"has_speaker":false,"has_aec":false},"fw":"third-party/1.0"}
```

**113 bytes of JSON, 114 bytes on the wire** with the terminating `\n` — for the key-absent
form. Nothing else differs from a token-bearing hello: same tags, same frame rules, same events,
same close-on-error behaviour. (The token-bearing example in §3 is 129 + 1 bytes and §9.4 dumps a
120 + 1 one; the only difference between those and this is the extra key.)

| Spelling | Bytes of JSON | Status |
|---|---|---|
| key absent — `{"v":1,…,"room":"study","caps":{…},"fw":"third-party/1.0"}` | 113 | **preferred for new clients**; the documented "optional, default absent" form |
| `"token":null` — `{"v":1,…,"token":null,"caps":{…},"fw":"third-party/1.0"}` | 126 | accepted, and **what the shipped firmware actually emits** |

Send the key **absent** if you are writing a client. The `null` spelling is accepted only as a
consequence of serde: `Hello.token` is `Option<String>` with `#[serde(default)]` and no
`skip_serializing_if` anywhere in `proto.rs`, so JSON `null` maps to `None` on the way in — and,
for the same reason, `encode_hello_line` itself emits `"token":null` when the field is `None`
(126 bytes for the example above, 13 more than the key-absent form). The firmware's C++ builder
makes the same choice, and it is a pinned one: the hello bytes are frozen by `test_hello` in the
codec's own self-test and by the differential harness's builder round-trip, so those extra 13
bytes are a property of the shipped satellite rather than an accident a client should copy or a
backend should correct. Nothing in the backend distinguishes the two —
`hello_token_present_but_null_decodes_as_absent` is the test that pins the equivalence.

## 4. Hello authentication

The token exists because the uplink socket drives Home Assistant intents under the
backend's long-lived HA token: an unauthenticated LAN peer could stream crafted
"luna …" audio, spoof a `source_id`, or inject `playback_done`/`button` events.

Decision per connection, `check_hello_auth(hello, server.auth_token)`:

| `server.auth_token` | Hello `token` | Outcome |
|---|---|---|
| set | present and byte-identical | accept |
| set | present, different | `AuthFailure::TokenMismatch` → **close + warn** |
| set | absent | `AuthFailure::MissingToken` → **close + warn** |
| unset | anything (absent or present) | accept; token is not consulted |

- **Comparison** goes through `proto::constant_time_eq` — a hand-rolled XOR fold with no
  early return, so response timing cannot be used to binary-search the secret. Length is
  folded into the same accumulator (including differences of 256 bytes and more), so a
  same-prefix, different-length token never compares equal.
- **Rejection is a close, not a retry.** The server drops the connection and logs. The
  warn is **rate-limited to one per peer IP per second**, tracked in a map shared across
  all connections (`WarnThrottle`, `WARN_THROTTLE_INTERVAL = 1 s`): a burst of bad
  handshakes from one host produces one log line, not hundreds.
- **Unset token ⇒ one loud warning at startup**, then every hello is accepted. This is the
  deliberate v1 default (plan R11) so a fresh install is not bricked by a missing secret —
  it is not a "secure mode". Setting `server.auth_token` and giving each source the same
  value is strongly recommended.
- A *set but blank* `server.auth_token` is rejected at config load
  (`server.auth_token, when set, must not be blank`), so "enabled but empty" cannot happen
  at runtime.
- One shared token means no per-source revocation; per-source credentials are the v2 lever
  behind the hello `v` bump. VPN/overlay enrollment (where the source is on the overlay) does
  **not** replace the token check — it is an additional path control.

## 5. Admission policy — `server.allow_unknown_sources`

`source_id` is looked up in `[[sources]]`:

| `source_id` | `server.allow_unknown_sources` | Outcome |
|---|---|---|
| configured | either | accept, using that entry's group/mute/fallback policy |
| unknown | `false` (default) | **close + log** |
| unknown | `true` | accept as an ad-hoc source (`group = None`) |

An ad-hoc source still gets a ring, sequence/jitter counters and a place in the metric
line, and its audio still reaches KWS, but the group-scoped policies do not apply: no
arbitration with other sources, no `mute_entity`, no group output fallback. Its `room`
comes from the hello line, since there is no config entry.

## 6. Uplink audio frame — `0x01`

The source's primary payload. One record carries exactly one 64 ms frame.

Record layout (**2053 bytes on the socket**):

| Offset | Size | Field | Encoding |
|---|---|---|---|
| 0 | 1 | tag | `0x01` (`TAG_AUDIO`) |
| 1 | 4 | `seq` | `u32`, little-endian |
| 5 | 2048 | `pcm` | 1024 × `i16`, little-endian, two's complement |
| 2053 | — | end of record | next byte is the next record's tag |

### `AUDIO_FRAME_LEN` (2052) vs `AUDIO_RECORD_LEN` (2053)

Both numbers are in the contract and both are needed; they describe different things:

- `AUDIO_FRAME_LEN = 2052` is the **frame**: `seq` + PCM, the unit firmware buffers and
  tests compare. `4 + 1024 × 2 = 2052`.
- `AUDIO_RECORD_LEN = 2053` is what the socket consumes per frame: the `TAG_AUDIO` byte
  **plus** the frame. The tag is record framing, not part of the frame.

The plan's byte list (`tag + seq u32 + 2048 B PCM`) sums to 2053 while the plan names a
"2052 B frame"; the contract keeps both constants so neither side has to do the
arithmetic. **A third-party source must write 2053 bytes per frame.** A source that sends
only the 2052-byte frame is one byte short: the tag byte it omitted becomes the tag of the
next record and the stream is misparsed from then on.

### `seq` and the sample index

- `seq` starts at **0** on every connection and increments by **1** per frame.
- `seq × FRAME_SAMPLES` (= `seq × 1024`) is the index of the frame's first sample in that
  source's ring; `(seq + 1) × 1024` is one past its last sample (§8).
- Frames are 64 ms; losing `seq` continuity is loss, not an error: the receiver counts
  `seq_gaps` and `gap_samples`, keeps the new frame, and moves on (§9).

### Decoding errors

| Condition | Error |
|---|---|
| Record shorter than 2053 bytes (e.g. a bare 2052-byte frame, or empty) | `ProtoError::ShortFrame { got }` |
| Byte 0 is not `0x01` | `ProtoError::BadTag { tag }` |
| `pcm` length ≠ 1024 when **encoding** | encoder returns an empty `Vec` (never pads or truncates) |

Trailing bytes past the 2053rd are ignored by `parse_audio_frame`; the incremental decoder
always slices exactly one record's worth, so in practice nothing trails.

## 7. Uplink events — `0x02`

Event record layout:

| Offset | Size | Field | Encoding |
|---|---|---|---|
| 0 | 1 | tag | `0x02` (`TAG_EVENT`) |
| 1 | 2 | `len` | `u16`, little-endian — JSON payload length in **bytes**, header excluded |
| 3 | `len` | payload | UTF-8 JSON, internally tagged by `"type"` |

`len` is a `u16`, so a payload is at most 65535 bytes — a satellite never approaches that.
The payload is one JSON object; extra unknown fields are ignored (serde default). An
unknown `"type"`, a missing required field, truncated JSON, or non-UTF-8 bytes make the
whole connection a `ProtoError::Json` / `ProtoError::Utf8` failure: the decoder latches the
error and the server closes the connection.

### Variants

`Event` is serialized with `#[serde(tag = "type", rename_all = "snake_case")]`, so the tag
is `"type"` and struct fields sit next to it.

| Variant | Exact JSON payload | Payload bytes | Record bytes | Meaning |
|---|---|---|---|---|
| `playback_done` | `{"type":"playback_done"}` | 24 | 27 | TTS playback finished (media_player went idle) → ends the Speaking session and opens the follow-up window |
| `rssi` | `{"type":"rssi","dbm":-62.5}` | 27 | 30 | Wi-Fi signal report, firmware sends it every 30 s; `dbm` is an `f32` JSON number |
| `button` | `{"type":"button","name":"night_mode"}` | 37 | 40 | Generic physical button; `name` is the firmware's trigger name. `night_mode` is the documented example |
| `save_buffer` | `{"type":"save_buffer"}` | 22 | 25 | **Missed-wake trigger** — "that was for you" |

`save_buffer` is not a spelling of `button`:

- The satellite's **"for you"** button emits `{"type":"save_buffer"}`. The backend writes
  the source's last 10 s of audio to `clips/missed/` (`MISSED_SECONDS = 10`).
- Every other button emits `{"type":"button","name":"…"}`.
- `{"type":"button","name":"save_buffer"}` is a **generic button whose name happens to be
  `save_buffer`** — it parses as `Event::Button { name: "save_buffer" }` and does **not**
  trigger the missed-wake dump. The binding lives in this contract, not in either side's
  code.

Serialized payloads above are the exact bytes the backend emits and accepts (compact JSON,
no spaces). A source may send a payload with different whitespace or key order — it is
JSON, and the backend parses rather than byte-compares — but the four payloads above are
the canonical forms.

## 8. Downlink control — `0x03`

Control record layout (server → source):

| Offset | Size | Field | Encoding |
|---|---|---|---|
| 0 | 1 | tag | `0x03` (`TAG_CONTROL`) |
| 1 | 2 | `len` | `u16`, little-endian — JSON payload length in bytes |
| 3 | `len` | payload | UTF-8 JSON, internally tagged by `"type"` |

`Control` uses `#[serde(tag = "type", rename_all = "lowercase")]`, i.e. the type names are
`led`, `earcon`, `play`, `stop`.

| Control | Exact JSON payload | Payload bytes | Record bytes | Meaning |
|---|---|---|---|---|
| `led` | `{"type":"led","state":"idle"}` | 29 | 32 | LED: neutral/off |
| `led` | `{"type":"led","state":"listening"}` | 34 | 37 | Wake detected, capturing |
| `led` | `{"type":"led","state":"speaking"}` | 33 | 36 | TTS playing |
| `led` | `{"type":"led","state":"muted"}` | 30 | 33 | Source muted (mute entity on, or suppressed) |
| `earcon` | `{"type":"earcon","name":"ack"}` | 30 | 33 | Play a locally stored acknowledgement sound |
| `play` | `{"type":"play","url":"http://ha.example.lan:8123/api/tts_proxy/abc","format":"flac"}` | 84 | 87 | Fetch and play a URL locally. Sent only to a source configured `playback = "source"` (see the notes below); never to a Home Assistant–played source |
| `stop` | `{"type":"stop"}` | 15 | 18 | Stop the reply that is playing, ending the conversation immediately. **Tablet-only**, and sent only to a source configured `playback = "source"`: the ESP firmware refuses unknown control types (and a refused control costs the connection), so routing it to a board would disconnect it. The tablet answers with the normal `playback_done` **iff a reply was playing** — a stop with nothing playing is silent. Boards are stopped through Home Assistant `media_player.media_stop` instead, which emits no wire message (see §13) |

Notes:

- `led.state` values are lowercase strings `idle` | `listening` | `speaking` | `muted`.
  Any other value is a JSON error on the receiving side.
- The source **must not send** `0x03` upstream. `0x03` is downlink-only; a source that
  sends it is a firmware bug and the backend latches `ProtoError::BadTag { tag: 0x03 }` and
  closes the connection. (Catching drift early is deliberate — plan R10.)
- The firmware decodes control fields — and the control record as a whole — into fixed
  buffers, so a sender **must not** put more than **127 bytes** in `earcon.name`, **511
  bytes** in `play.url`, or **15 bytes** in `play.format` (decoded UTF-8 bytes after any
  `\u` escapes, excluding the terminator), and **must not** send a control whose complete JSON
  payload exceeds **1024 bytes** (`MAX_CONTROL_JSON`, checked against the 3-byte length prefix
  before anything is parsed). The total is an independent limit: a control that sits inside all
  three field caps is still refused if the whole payload is over 1024. Past any of them the
  source refuses the **entire control** and logs it; it never truncates, because a truncated
  `earcon` name would play a different sound and look like success. **A refused control also
  costs the connection**: every downlink decoder error reaches
  `disconnect_("control framing error")`, so an oversized control means a reconnect and an
  audible gap, not a dropped control on an otherwise healthy stream — the caps are hard, not
  advisory. These are source-side limits only — the
  contract itself imposes no length on these fields and the backend's decoder accepts any length.
  Today the only production sender emits the fixed name `"ack"`, so nothing approaches a cap; the
  rule is written for the reserved `play` path above, whose URLs come from Home Assistant.
- The source may ignore controls it does not implement, but it must still consume the
  record: `len` is required to find the next record's tag.
- **`play` and `playback_done` (the `playback = "source"` route).** A source whose
  `[[sources]]` entry sets `playback = "source"` is sent `play` instead of Home Assistant being
  asked to play the reply on a `media_player`; a source on the default `playback = "ha"` is
  never sent one. `url` is absolute (the backend joins Home Assistant's relative
  `/api/tts_proxy/…` path to `ha.internal_url`, or to `ha.url` with `ws`→`http` when that is
  empty). `format` is `mp3` / `flac` / `wav` for the usual TTS types, otherwise the mime
  subtype, or `auto` when none is known or the subtype is over 15 bytes; it is informational and a
  source that can sniff the stream may ignore it. The source **must send `playback_done`
  when playback ends or fails** (fetch error, decode error, being replaced by a newer
  `play`, or being stopped by a `stop` control it implements — a `stop` that arrives with nothing
  playing is answered with nothing), exactly once per `play`: the backend suppresses the source's
  microphone from the wake detector until it hears it, and otherwise waits out
  `capture.speaking_timeout_s` (30 s).
- What the backend can observe on this route: `playback started` in the journal means
  the `play` control was **queued on the source's socket**, not that the source accepted or
  began playing it, so `TtsEndToPlaybackStart` measures tts-end → control queued. The
  source's own `playback_done` is the only completion signal, and the backend cannot tell a
  clean finish from a failure (a source that fails instantly reports a short
  `TtsEndToPlaybackDone`).

## 9. Worked byte examples

### 9.1 One audio record (2053 bytes)

Constructed with `seq = 7` and the test pattern `pcm[i] = i` (as `i16`, i.e. samples
`0, 1, 2, …, 1023`). Byte offsets are decimal, values hexadecimal.

| Offset | Field | Bytes | Value |
|---|---|---|---|
| 0 | tag | 1 | `01` |
| 1–4 | `seq = 7` | 4 | `07 00 00 00` |
| 5–6 | `pcm[0] = 0` | 2 | `00 00` |
| 7–8 | `pcm[1] = 1` | 2 | `01 00` |
| 9–10 | `pcm[2] = 2` | 2 | `02 00` |
| 11–12 | `pcm[3] = 3` | 2 | `03 00` |
| … | … | … | … |
| 2047–2048 | `pcm[1021] = 1021` | 2 | `FD 03` |
| 2049–2050 | `pcm[1022] = 1022` | 2 | `FE 03` |
| 2051–2052 | `pcm[1023] = 1023` | 2 | `FF 03` |

First 13 bytes on the wire:

```
01 07 00 00 00 00 00 01 00 02 00 03 00
│  └─────┬─────┘ └──┬──┘ └──┬──┘ └──┬──┘
│        │          │       │       └─ pcm[3]  = 0x0003 → 03 00
│        │          │       └───────── pcm[2]  = 0x0002 → 02 00
│        │          └───────────────── pcm[1]  = 0x0001 → 01 00
│        └──────────────────────────── seq = 7 as u32 LE
└───────────────────────────────────── tag 0x01 (TAG_AUDIO)
```

Last 4 bytes on the wire — note the little-endian order of a value whose high byte is
non-zero:

```
FE 03  FF 03
│  │   └─┴── pcm[1023] = 1023 = 0x03FF → FF 03
└──┴──────── pcm[1022] = 1022 = 0x03FE → FE 03
```

Total record length: `1 + 4 + 2048 = 2053` bytes (`AUDIO_RECORD_LEN`); the frame inside is
`2052` (`AUDIO_FRAME_LEN`). This frame covers sample indices `7 × 1024 = 7168` through
`8191` inclusive — 1024 samples, 64 ms.

A real frame's PCM is whatever the microphone captured; the pattern above only pins the
byte order. A negative sample uses two's complement, e.g. `pcm[0] = -2` encodes as
`FE FF`.

### 9.2 One control record (LED listening)

`Control::Led { state: LedState::Listening }` → payload `{"type":"led","state":"listening"}`
(34 bytes) → record `03 22 00` + payload (37 bytes total):

```
03 22 00 7B 22 74 79 70 65 22 3A 22 6C 65 64 22 2C 22 73 74 61 74 65 22 3A 22 6C 69 73 74 65 6E 69 6E 67 22 7D
│  └──┬─┘ └───────────────────────────────────────┬──────────────────────────────────────────────────────────┘
│     │                                          └─ {"type":"led","state":"listening"}  (UTF-8, 34 B, '0x22' = '"')
│     └─ len = 0x0022 = 34 (u16 LE)
└─────── tag 0x03 (TAG_CONTROL)
```

`0x22 0x00` is 34 little-endian. The record ends exactly at byte 36 (0-based), so the next
downlink record's tag follows immediately.

### 9.3 One event record (missed-wake trigger)

`Event::SaveBuffer` → payload `{"type":"save_buffer"}` (22 bytes) → record
`02 16 00` + payload (25 bytes):

```
02 16 00 7B 22 74 79 70 65 22 3A 22 73 61 76 65 5F 62 75 66 66 65 72 22 7D
│  └──┬─┘ └────────────────────────────┬─────────────────────────────────┘
│     │                               └─ {"type":"save_buffer"}
│     └─ len = 0x0016 = 22
└─────── tag 0x02 (TAG_EVENT)
```

### 9.4 The hello line

With a literal secret — `{"v":1,"source_id":"kitchen","room":"kitchen","token":"s3cr3t","caps":{"has_speaker":true,"has_aec":false},"fw":"0.1.0"}`
— the line is 120 bytes of JSON plus the terminating `\n` = **121 bytes**, with no tag byte
(the §3 example is the same shape with a placeholder secret, so it is 129 bytes + `\n`):

```
7B 22 76 22 3A 31 … 22 7D 0A
└─ '{' '"' 'v' '"' ':' '1' … '"' '}' └─ '\n'
```

`encode_hello_line` emits exactly `serde_json` bytes + one `\n`; `decode_hello_line` strips
any number of trailing `\r`/`\n` before parsing.

The token-less hello of §3.1 — the key-absent spelling a client should use when
`server.auth_token` is unset, the shipped default — is this same shape minus the `"token"` key:
**113 bytes of JSON, 114 with the `\n`**. Writing `"token":null` instead costs 13 more bytes
(126 + 1) and is what `encode_hello_line` produces for `token: None` because the field is an
`Option` with no `skip_serializing_if` — and also what the shipped firmware's builder emits,
pinned that way. Both parse to `token: None`; a new client should send the key-absent form
(§3.1).

## 10. Framing and error handling

`FrameDecoder` is the incremental parser the backend feeds from the socket. Its rules are
part of the contract:

1. **Hello first.** Until a `\n` is seen, incoming bytes are the hello line (`MAX_HELLO_LINE`
   limit). A hello is emitted once, as the first decoded message.
2. **Then tagged records.** The decoder peeks one tag byte:
   - `0x01`: needs `AUDIO_RECORD_LEN` (2053) bytes; fewer means *wait*, not an error. The
     complete record is parsed and consumed.
   - `0x02`: needs the 3-byte header, then `len` payload bytes; fewer means *wait*. Payload
     is parsed as JSON.
   - `0x03` upstream, or anything else: `ProtoError::BadTag` — latch and stop.
3. **Partial reads are invisible.** Feeding the stream byte-at-a-time yields exactly the
   same message sequence as one big `feed`, in order; nothing is blocked on and no record is
   ever half-emitted.
4. **First error latches.** `feed` returns no further messages while an error is held;
   `take_error` consumes it (and re-arms the decoder for library use). The server's action
   for a latched error is to close the connection once and log the reason.
5. **Backpressure is the socket.** Slow processing shows up as TCP backpressure on the
   source, never as dropped bytes inside the decoder.

Error catalogue (`ProtoError`):

| Error | Raised by |
|---|---|
| `Version { got }` | hello `v` ≠ `1` |
| `Json(String)` | malformed/missing-field/unknown-variant JSON; empty hello line |
| `Utf8` | non-UTF-8 payload or hello line |
| `BadTag { tag }` | wrong tag byte, including `0x03` upstream |
| `ShortFrame { got }` | audio parse given fewer than 2053 bytes |
| `PayloadTooLong { got }` | a declared payload length above `MAX_JSON_PAYLOAD` |
| `NeedMore { need }` | complete-record parse called with an incomplete buffer; the decoder itself waits instead |
| `LineTooLong` | > 4096 bytes of hello with no `\n` |

## 11. Time base

Two clocks, deliberately separated; a source needs neither of them synchronised to
anything.

- **Sample index (`u64`)** — the protocol's ordering. `seq × 1024` is the frame's first
  sample; the ring, capture spans, clip boundaries and all buffer arithmetic are in this
  domain. Within a connection the index is strictly increasing by 1024 per frame. There are
  no timestamps in any record.
- **Monotonic `Instant`** — backend-internal only: the coordinator's cross-source
  arbitration window (`capture.arbitration_window_ms = 250`), session timeouts
  (`followup_s`, `speaking_timeout_s`), latency stage logs and the 60 s metric line
  (`METRIC_LOG_INTERVAL`). Never transmitted, never persisted.
- **Wall clock** — logs and clip sidecars only: a sidecar is a clip's `.wav` name plus
  `SIDECAR_SUFFIX` (`.json`) and carries the wall-clock time of the trigger. Never in the
  audio path.
- **Gaps.** A missing `seq` is loss: the ring keeps a hole, reads over the hole return
  zero-filled samples, and the source's counters record `seq_gaps` and `gap_samples`. There
  is no retransmission and no ARQ — this is a realtime stream and the next frame is worth
  more than the lost one. The next frame's `seq × 1024` still gives the authoritative index;
  a source must not "close" a gap by renumbering frames.

## 12. Reconnect and backoff

- **Client behaviour (expected, not enforced by bytes):** on connection loss or a closed
  socket, reconnect with **exponential backoff starting at 1 s, doubling, capped at 30 s per
  attempt** (`reconnect_delay_ms` = `min(1000 << attempt, 30000)`). The attempt counter is
  reset only once a connection has **survived 5 s** (`reconnect_settled()`,
  `RECONNECT_SETTLE_MS`), not on a successful TCP connect: the protocol has no hello
  acknowledgement, so survival is the only observable proof that the backend admitted the
  source. A source that connects and is then refused (bad token, unknown `source_id`)
  therefore climbs the ladder 1, 2, 4 … 30 s instead of being retried every second forever;
  a connection that lived at least 5 s and then dropped retries after 1 s. The ladder also
  advances whenever `connect()` itself fails, which is the backend-down case. That
  matters for the target below: the cumulative wait to a *successful* reconnection is
  1, 3, 7, 15, 31, 61 s, so with these caps the ≤ 30 s target is missed at the **fifth**
  retry and the ≤ 60 s target at the **sixth**. It is a prediction from the constants, not a
  measured result; the pending soak is recorded under AC8 in `docs/design/TEST-NOTES.md`. Retrying
  hot-loop is not acceptable; neither is
  giving up — a backend restart must be invisible apart from a short gap (target: every
  source back within 60 s of a restart, a single satellite ≤ 30 s).
- **Fresh connection = fresh sequence.** A reconnect repeats the hello and restarts `seq` at
  `0`. The backend replaces the source's entry (`reconnects` counter increments) and gives
  it a new ring; a span never crosses a reconnect. The hello must be sent again — the
  decoder expects it on every connection, and there is no resumption token.
- **Server-side closes** that a source will see: unknown `source_id` with
  `allow_unknown_sources = false`; auth failure (§4); any latched `ProtoError` (bad tag,
  malformed JSON, over-long hello, wrong version); a connection this backend has replaced
  (see the ownership rule below), which is closed when it next sends anything or once
  `READ_IDLE_DEADLINE` passes with no bytes at all; and a peer that connects and never
  sends a hello. `READ_IDLE_DEADLINE` is 300 s — ten times the `rssi` interval a connected
  satellite is required to emit (§7). A peer that is producing audio, or succeeding at its
  heartbeats, cannot reach it, and this is not an idle timeout. The guarantee is not
  unconditional, though: the heartbeat is **best-effort on the source side** —
  `maybe_send_rssi_` returns without sending when the RSSI read fails or when the outbound
  queue rejects the record, and does not retry until the next interval — so a
  connected-but-silent peer (no audio, ten heartbeats that never left the device) is treated
  as gone. That is the intended reading of a peer that answers nothing for 300 s, but it is a
  live peer's *behaviour* the deadline depends on, not a property of the socket. Without the
  deadline a satellite that vanished without a FIN would hold a task and a socket here
  forever, because nothing else in the path ever discovers it. Each of these is a one-shot
  close. Every one produces a log line **except auth failure**, which is throttled to one
  line per peer IP per second by design (R11: a misconfigured fleet must not fill the log) —
  so repeated rejections from one peer close silently between lines. The source's backoff
  handles them the same way.
- **What the backend does when a connection ends.** Each connection owns the registration it
  was given, and only the connection that currently owns a `source_id` may write that
  source's ring, remove its entry, or report it gone. Ownership is decided by comparing
  against the registry's current entry — never by what a connection happens to be holding.
  - The **owning** connection ends — EOF, a latched `ProtoError`, or silence beyond
    `READ_IDLE_DEADLINE`: its entry is removed and `SourceGone` is emitted for that
    `source_id`. Delivery is **best-effort**: the event goes onto a bounded queue with
    `try_send`, and a full or closed queue drops it with a warning, because a stalled
    coordinator must never back-pressure a socket. What the coordinator does with it depends
    on whether that connection is still owed work. A turn owed its endpoint, or already
    handed to ASR, **stands**: the audio was delivered by that connection and is still worth
    transcribing, so the group is marked gone and the work already in flight is allowed to
    finish — a turn whose satellite has gone is never entered into `Speaking`, because there
    is no endpoint left to play on, and returns to Idle instead. A session with nothing owed
    it — `Speaking`, or a follow-up window whose speaker has walked away — does end, and the
    group's suppression goes with it. Audio the source had **already** delivered
    stays reachable for a short grace period after removal, which is what lets the two clip
    writers — the missed-wake dump and the false-accept clip, both asked to save audio *after*
    the event that justifies them — still write a source's last seconds. It is a grace period
    and not a cache of dead sources: a source is absent for every other purpose the instant its
    entry is removed, and nothing may write audio through a retired ring.
  - A connection that a later connection for the same `source_id` has **replaced** ends: it
    touches nothing and emits nothing, because its `source_id` now names a live connection
    and `SourceGone` would be false. It is closed, and the close is logged with the peer
    address, the `source_id`, and whether a newer connection holds the id — a satellite
    losing its connection this way has to be diagnosable, not silent.
  - A frame **or an event** arriving on a replaced connection is refused: neither is recorded,
    counted against the source, or forwarded. For audio the reason is the sequence: every
    connection restarts `seq` at 0 (the fresh-connection rule above), so accepting it would
    splice stale samples into the live stream *and* charge the live source a gap it never had.
    For events the reason is attribution: a ghost's `playback_done` would end a session the
    live connection is running, a ghost's `save_buffer` would dump the live ring, and a
    ghost's `rssi` would publish a signal level the live socket never measured.
    Refusal is also what makes the reap above reachable for this peer, and the ownership
    check is not limited to complete messages: it runs on **every** successful read, so a
    replaced connection is closed by the first byte it sends, however partial. Without that,
    a peer that trickled bytes which never completed a frame or an event would keep resetting
    the read deadline forever and never be closed at all — the promise that a replaced
    connection is closed when it next sends anything is only true because of this check. In
    both cases the close is the one described below.
  - Whichever way a connection ends, a frame it was still holding back for the KWS channel is
    counted first (R9). That count belongs to the `source_id`, not to the connection — the
    channel is bounded per source, so a drop is a property of that source's channel — and it
    is taken before the ownership branch rather than on whichever exit path happens to be
    reached. Where the id has no entry left to charge, because the newer connection has
    itself gone, the count falls back to the connection whose frame it was: the alternative
    was for audio that never reached the spotter to leave no trace anywhere.
- Satellites need no reflash and no config change when the backend restarts — that is the
  point of the reconnect rule.

## 13. Conformance checklist for a new source

Minimum a third-party source must do:

1. Open TCP to the backend, send exactly one `\n`-terminated JSON hello with `v: 1`, a
   configured `source_id`, a `room`, and the configured `token` when the household uses
   one.
2. Never send `0x03` upstream and never send a second hello.
3. Stream audio as 2053-byte records: `0x01`, `seq` as `u32` LE from 0, then 1024 `i16` LE
   samples of 16 kHz mono — no padding, no resampling, no jumbo frames carrying more than
   one frame.
4. Emit `playback_done` when TTS playback actually ends **or fails** (exactly once per `play` on
   a `playback = "source"` source; see §8), `rssi` about every 30 s, generic
   `button` events for other buttons, and `save_buffer` (never `button`) for the "for you"
   button.
5. Tolerate and act on the controls it implements (`led`, `earcon`, `play`; `stop` is
   tablet-only and optional — see item 7), consuming each record using `len` even when ignored.
6. Reconnect with 1 s → 30 s exponential backoff, restarting `seq` at 0 and re-sending the
   hello.
7. Do not expect the ESP firmware to understand `stop`. The backend **accepts** the control and
   the shipped firmware **refuses** it: its decoder treats an unknown control type as a decode
   error, and every control decode error reaches `disconnect_("control framing error")`, so
   sending `stop` to a board costs it the connection. (The protocol itself is more forgiving — a
   source may ignore a control it does not implement provided it consumes the record, §8 — but
   the shipped firmware does not take that option, so the backend must not rely on it.) That is
   why the backend gates the route on `playback`: `stop` goes to a `playback = "source"` tablet
   source only, and a Home Assistant–played board is stopped with the `media_player.media_stop`
   service call instead, which carries no wire message. A third-party source MAY implement
   `stop`: consume the record with `len`, stop the reply, and emit the ordinary `playback_done`
   if and only if one was playing. The asymmetry is pinned as an `INTENTIONAL_DIVERGENCES` row
   in `tests/wire_contract.rs`, where the firmware answers `"reject"` for it.
