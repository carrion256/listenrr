# CONTRACT-NOTES — listenrr contract layer

_Historical engineering notes from the original development. Host names and addresses have been generalised; references to "the plan" point to an unpublished design document._

Authoritative spec: the original design plan, not published (Revision 5).
This file is the build protocol + seam map for the lanes. The plan is the authority; where
this file and the plan disagree, the plan wins and this file is wrong — say so instead of
quietly editing the plan.

## 1. Build protocol (do this once per machine, then per lane)

`sherpa-onnx = "2.0.1"` builds against a prebuilt static library that `sherpa-onnx-sys`
fetches from GitHub releases on first build. Eight lane worktrees refetching a 22 MB
archive is eight avoidable downloads (plan R1: hermetic builds via
`SHERPA_ONNX_ARCHIVE_DIR`).

Already done on this host — the archive is cached, verified (`bzip2 -t` clean) and
content-addressed:

```
~/.cache/sherpa-onnx-archive/sherpa-onnx-v2.0.1-linux-x64-static-lib.tar.bz2
sha256  55cfadf65ba50d0afc1a802b7a337942819e51c94a5b5a384def1978f7c70871
```

If it is missing (new machine, new lane host), repopulate it once:

```sh
mkdir -p ~/.cache/sherpa-onnx-archive
curl -sSfL -o ~/.cache/sherpa-onnx-archive/sherpa-onnx-v2.0.1-linux-x64-static-lib.tar.bz2 \
  https://github.com/k2-fsa/sherpa-onnx/releases/download/v2.0.1/sherpa-onnx-v2.0.1-linux-x64-static-lib.tar.bz2
sha256sum ~/.cache/sherpa-onnx-archive/sherpa-onnx-v2.0.1-linux-x64-static-lib.tar.bz2
```

**Every lane exports this before cargo**, so no worktree touches the network for the
static lib:

```sh
export SHERPA_ONNX_ARCHIVE_DIR="$HOME/.cache/sherpa-onnx-archive"
```

`cargo build` on this host is green with that export set (it writes the extracted tree
into its own cache dir and reuses it). `SHERPA_ONNX_LIB_DIR` (already-extracted `lib/`)
also works if a lane wants to skip extraction entirely. The name the sys crate looks for
inside `SHERPA_ONNX_ARCHIVE_DIR` is exactly the release filename above — do not rename it.

Rust: `rust-toolchain.toml` pins 1.99.0 and is inert where rustup is absent (the Nix
profiles supply the toolchain; this host has rustc/cargo 1.99.0 and clang/g++).

## 2. File ownership

**Contract files — lanes MUST NOT edit. Report a mismatch instead of changing them.**

| File | Contains |
|---|---|
| `Cargo.toml`, `rust-toolchain.toml`, `Cargo.lock` | dependency + toolchain pins (ask before adding a dep) |
| `src/lib.rs` | module graph, `SAMPLE_RATE_HZ`, `ms_to_samples`, `samples_to_ms`, `seconds_to_samples`, `pcm_to_f32` |
| `src/proto.rs` | the frozen wire contract + `FrameDecoder` + `constant_time_eq` |
| `src/ring.rs` | `SourceRing`, `RingHandle`, `new_handle`, `with_ring` |
| `src/config.rs` | every typed config key + `Config::validate` |

  *Amended with owner sign-off for the stop-command plan (unpublished; see §6 row 25): `proto.rs` gained
  `Control::Stop` and `config.rs` gained `capture.stop_phrases`, `kws.stop_keywords_file`,
  `kws.stop_threshold` and `pub fn normalise_phrase`.*

**Lane-owned seams — public signatures are frozen; their bodies were `todo!()` at contract
time and are now implemented.** Implement the
bodies and add **private** items only. If a signature blocks you, message the orchestrator
with the concrete mismatch; do not rename or re-shape a public item, and do not add a
public item another lane will also need (that is a contract change).

| File | Owner (plan) | What lands there |
|---|---|---|
| `src/ingest.rs` | Lane A · S1.4, S5.3, S7.1 | accept loop, registration, ring write, KWS/broadcast fan-out, gap/jitter, hello auth |
| `src/kws.rs` | Lane A · S2.1–S2.4 | `SherpaEngine`, KWS thread, `SampleCounters` spans, `bench` |
| `src/coordinator.rs` | Lane A · S4.4, S5, S6 | transition table, `snr`, `shrink_noise_window`, suppression |
| `src/capture.rs` | Lane A · S3.1, S3.2, S6.1 | `SileroVad`, `backward_bounds`, `ForwardEndpointer` |
| `src/verify.rs` | Lane A · S4.2 | token set, word boundary, filler strip |
| `src/parakeet.rs` | Lane A · S3.3 (U1) | Wyoming framing + `transcribe` |
| `src/ha.rs` | Lane A · S4.1, S4.3, S4.5 (U2) | WS auth, pipeline run, play_media, media_stop, mute subscribe |
| `src/clips.rs` | Lane A · S8.1, S8.2 (+S2 wav dump) | WAV + sidecar writer, missed-wake dump, retention sweep |
| `src/replay.rs` | integration owner · S3.4 | `replay` with the counting no-op HA sink |
| `firmware/components/luna_stream/*`, `config/satellite.example.yaml` | Lane B · S1.5 | C++ component + YAML — contract tests against `proto` |
| `deploy/*`, `config/listenrr.toml.example` (plan path), `docs/protocol.md`, `README` | Lane C · S0.1, S1.6, S8.3 | example config, unit, protocol doc |
| `src/main.rs` | orchestrator | CLI + task wiring; lanes do not edit it — extend `dispatch`'s arms by request |
| `tests/` | everyone | fixtures + integration tests; `docs/design/TEST-NOTES.md` for tuning records |

`src/lib.rs` exists so the `tests/` integration suites (S7 / AC10's scripted source client)
compile against the same module graph as the binary; the binary is still the `listenrr`
target.

## 3. Seam signatures (frozen)

`src/ingest.rs`
- `pub enum IngestEvent { Event { source: String, event: proto::Event }, SourceGone { source: String } }`
- `pub struct SourceStats { frames, seq_gaps, gap_samples, last_seq, jitter_p50_ms, jitter_p95_ms, kws_drops, reconnects, rssi_dbm, connected_at }`
- `pub struct SourceHandle { source_id, room, group: Option<String>, caps: proto::Caps, ring: RingHandle, stats: Mutex<SourceStats> }`
- `impl SourceRegistry`: `new(&Config)`, `register(&Hello, Option<String>, usize) -> Arc<SourceHandle>`, `get(&str)`, `live_sources()`, `record_frame(&str, u32, &[i16]) -> Option<u64>`, `record_kws_drop(&str)`, `record_rssi(&str, f32)`, `unregister(&str)`, `maybe_log_metrics(Instant)`, `warn_throttle()`
- `impl WarnThrottle`: `new()`, `should_warn(IpAddr, Instant) -> bool` (1/s per peer IP)
- `pub enum AuthFailure { MissingToken, TokenMismatch }`; `pub fn check_hello_auth(&Hello, Option<&str>) -> Result<(), AuthFailure>`
- `pub async fn serve(&Config, Arc<SourceRegistry>, crossbeam_channel::Sender<(String, Arc<[i16]>)>, tokio::sync::broadcast::Sender<(String, Arc<[i16]>)>, tokio::sync::mpsc::Sender<IngestEvent>, tokio::sync::watch::Receiver<HashSet<String>>) -> anyhow::Result<()>`
- consts: `KWS_CHANNEL_CAPACITY = 4096`, `FRAME_BROADCAST_CAPACITY = 512`, `EVENT_QUEUE_CAPACITY = 256`, `METRIC_LOG_INTERVAL = 60 s`, `WARN_THROTTLE_INTERVAL = 1 s`

`src/kws.rs`
- `pub struct WakeEvent { source, kw_span: Range<u64>, arrived: Instant }`; `pub struct Detected { source, keyword, first_token_ts_s: Option<f32> }`
- `pub struct StopEvent { source: String, arrived: Instant }` (`Debug + Clone + PartialEq`); `pub struct StopLane { /* private: engine, listen, active, tx */ }` + `StopLane::new(Option<Box<dyn KwsEngine>>, watch::Receiver<HashSet<String>>, mpsc::Sender<StopEvent>)` — the stop spotter bundled as one value so `run_kws_thread` gains one parameter (added for the stop-command plan; see §6 row 26)
- `pub trait KwsEngine: Send { accept(&mut self, &str, &[f32]); decode_ready(&mut self) -> Vec<Detected>; reset(&mut self, &str) }`
- `pub struct SampleCounters`: `new`, `note(&str, usize) -> u64`, `accepted(&str) -> u64`, `samples_at_reset(&str)`, `note_reset(&str)`, `span(&str, u64, u64, Option<f32>) -> Range<u64>`
- `pub fn run_kws_thread(crossbeam_channel::Receiver<(String, Arc<[i16]>)>, mpsc::Sender<WakeEvent>, watch::Receiver<HashSet<String>>, Box<dyn KwsEngine>, StopLane) -> u64`
- `pub struct SherpaEngine` + `SherpaEngine::new(&config::Kws) -> anyhow::Result<Self>` (implements `KwsEngine`)
- `pub fn bench(&Path, &Path) -> anyhow::Result<BenchReport>`; serde `BenchReport { model, wav, audio_seconds, wall_seconds, realtime_factor, streams, detections, passes_ac3 }`
- consts: `SAMPLE_RATE_HZ = 16000`, `SPAN_REFINE_WINDOW_S = 3.0`, `BENCH_MIN_REALTIME_FACTOR = 20.0`

`src/coordinator.rs`
- `pub enum Session { Idle, Capturing { winner, start }, Processing { winner, conversation_id }, Speaking { winner, conversation_id, until }, FollowUp { winner, conversation_id, until } }`
- `pub trait Clock: Send { fn now(&self) -> Instant }` + `pub struct SystemClock`
- `pub enum CoordIn { Wake(WakeEvent), Ingest(IngestEvent), EndpointReached { source, utterance_start, endpoint }, Transcript { source, conversation_id, text }, Verified { source, conversation_id, text }, VerifyRejected { source, transcript }, PipelineFinished { source, conversation_id, tts_url, mime_type: Option<String> }, MuteChanged { entity, muted }, Stop(crate::kws::StopEvent), Tick }`
- `pub enum CoordOut { Control { source, control }, Endpoint { source, kw_span, conversation_id }, Transcribe { source, conversation_id, utterance_start, endpoint }, Verify { source, conversation_id, transcript, wake_initiated }, RunPipeline { source, conversation_id, text }, PlayMedia { source, entity_id, url, conversation_id, mime_type: Option<String> }, StopListenChanged { sources: Vec<String> }, StopPlayback { source, entity }, FalseAcceptClip { source }, MissedWakeClip { source }, SuppressionChanged { suppressed }, Latency { stage, conversation_id, ms } }`
  (`mime_type` on both was added at integration: `PipelineOutcome.mime_type` had nowhere to go, so `play_media` was hardcoding a content type — see §6 row 16.)
  (`StopListenChanged` / `StopPlayback` were added with owner sign-off for the stop-command plan — see §6 row 26. `StopPlayback.entity` is resolved with the same `output_entity` helper `PlayMedia` uses, empty string = nothing to stop. `set_outs()` (private) returns `[StopListenChanged{stop_listen()}, SuppressionChanged{suppressed()}]` **in that order**, and `stop_listen()` — members of every group whose session is Processing or Speaking, minus `muted`, sorted — is the only publisher of the stop-listen set.)
- `impl Coordinator`: `new(&Config, Box<dyn Clock>)`, `session(&str) -> &Session`, `handle(CoordIn) -> Vec<CoordOut>`, `is_suppressed(&str)`, `suppressed() -> Vec<String>`, `release(&str)`
- `pub fn snr(&[i16], &Range<u64>, &[i16]) -> f64`; `pub fn shrink_noise_window(Range<u64>, &mut dyn VadEngine, u64) -> Range<u64>`
- decisions stay synchronous + clock-injected: AC1's arbitration/transition tests need no tokio.

`src/capture.rs`
- `pub trait VadEngine: Send { process(&mut self, &[f32]) -> bool; reset(&mut self) }`; `SileroVad::new(&Path)` implements it
- `pub fn backward_bounds(&[i16], &Range<u64>, &mut dyn VadEngine) -> Range<u64>` (S3.1, 300 ms fallback) and `backward_bounds_with_preroll(..., preroll_samples: u64)` for a non-default `capture.preroll_ms`
- `pub struct ForwardEndpointer`: `new(&Capture, Box<dyn VadEngine>, EndpointMode)`, `push(u64, &[i16]) -> Option<Endpoint>`, `speech_started()`, `speech_start()`, `finish(EndpointReason)`
- `pub struct Endpoint { start, end, reason, last_speech }`, `enum EndpointReason { TrailingSilence, HardCap, SourceGone }`, `enum EndpointMode { Wake { kw_span }, FollowUp }`, `struct Utterance { source, conversation_id, pcm, kw_span, endpoint }`
- consts: `BACKWARD_LOOKBACK_SAMPLES = 160_000` (10 s), `DEFAULT_PREROLL_MS = 300`

`src/verify.rs` — `verify_utterance(&str, wake_initiated: bool, wake_words: &[String]) -> Result<String, FalseAccept>`; `FalseAccept { transcript }`; consts `FILLER_TOKENS = ["hey"]`, `STRIP_CHARS`. The wake words were originally a frozen const set (`luna`, `lunar`, `loona`, `lunas`, `luna's`); they are now the `kws.wake_words` config key with the same default.

`src/parakeet.rs` — `pub struct WyomingClient { pub addr: String, pub fallbacks: Vec<String> }` + `new(addr)` (one tier) / `with_fallbacks(addr, fallbacks)` (ordered tiers; holds cooldown state, so build it once and pass `&client`); `pub async fn transcribe(&WyomingClient, &[i16]) -> anyhow::Result<String>` tries `addr` first, then `fallbacks` in order, and names every endpoint it tried in the error when none answered; consts `PROBE_TIMEOUT_MS` (budget for a non-final candidate) and `ENDPOINT_COOLDOWN` (how long a failed tier is left alone); the **last** candidate gets `framing::REQUEST_TIMEOUT_MS`, not the probe budget, because a slow CPU tier doing real work is not a fault; `pub mod framing` holds **every** wire constant (U1 pins them against the live service; the current values are captured bytes, see the module docs).

`src/ha.rs` — `pub struct HaClient { pub url, pub token, pub pipeline_id }` + `new(url, token, pipeline_id)`; `run_pipeline(&PipelineRequest) -> PipelineOutcome`, `play_media(&str, &str, Option<&str> /*mime*/)`, `media_stop(&str)` (same async-ness and `Result` as `play_media` without the url/mime/announce arguments; added with owner sign-off for the stop-command plan — see §6 row 26), `subscribe_mute_states(&[String], mpsc::Sender<(String, bool)>)` (all `&self`, async); `PipelineRequest { text, device_id, conversation_id }`, `PipelineOutcome { intent_summary, tts_url, mime_type }`; consts `START_STAGE_INTENT`, `END_STAGE_TTS`, `PLAY_ANNOUNCE`, backoff bounds. Tolerant reader: missing fields → `None` + warn, never panic. (`pipeline_id` and the `mime` argument were added at integration — see §6 row 16.)

`src/clips.rs` — `write_trigger_clip(&Path, &str, &[i16], &TriggerRecord) -> PathBuf`, `save_buffer_dump(&RingHandle, &str, &Path) -> PathBuf` (last 10 s), `retention_sweep(&Path, u32) -> usize`; serde `TriggerRecord` (incl. `candidates[].noise_window_used` and `ended_by: Option<EndedBy>`), `EndedBy { PlaybackDone, Timeout }`, `StageLatencies`, `TriggerReason`; consts `FALSE_ACCEPT_DIR`, `MISSED_DIR`, `MISSED_SECONDS = 10`, `CLIP_PADDING_SECONDS = 1`, `SIDECAR_SUFFIX`.

`src/replay.rs` — `run(&Path /*model*/, &Path /*wav*/, &Config) -> anyhow::Result<ReplayReport>`; serde `ReplayReport` with bounds, `last_speech`, `utterance_end_margin_samples/ms`, transcript, verify outcome, `ha_calls`, `pipeline_requests`.

`src/main.rs` — `listenrr run --config`, `listenrr replay <wav> --config [--model]`,
`listenrr bench-kws --model --wav`. `dispatch` executes `CoordOut`s; suppression and
latency lines are wired for real, the I/O arms are stubs the integration owner completes
as each seam lands.

## 4. Working rules for lanes

1. **Never edit a contract file.** If `proto`/`ring`/`config`/`lib` or a frozen signature
   is wrong for your slice, write the mismatch in your report (file, item, what you need,
   why) — the orchestrator changes it once, so eight lanes cannot drift.
2. Contract tests are the spec. `cargo test proto::` asserts the literal bytes
   (`0x01`/`0x02`/`0x03`, `u32`/`u16` LE, the 2052-byte frame, `save_buffer` vs
   `button` + `night_mode`). A test that contradicts the plan is a bug report, not an
   invitation to edit the test. A failing check is never weakened to pass (plan §7).
3. Sample-index time (`u64`) for all buffer/span arithmetic; `Instant` only in the
   coordinator for cross-source comparison and latency; wall time only in logs and
   sidecars. Do not add `SystemTime` to a hot path.
4. Ingest never blocks on DSP or network calls. KWS stays on its own thread
   (`run_kws_thread`), Parakeet/HA on async tasks.
5. Suppressed sources keep filling their rings. Reads never consume the ring; a lagged
   broadcast subscriber recovers by re-reading by index.
6. No new dependencies without asking. The plan's list is closed ("no others without
   cause"); fuzzy matching is a ~10-line function.
7. Do not run `cargo fmt` or `cargo clippy`; the orchestrator owns those gates. No
   commits from lanes.

## 5. Running tests

```sh
export SHERPA_ONNX_ARCHIVE_DIR="$HOME/.cache/sherpa-onnx-archive"
export CARGO_TARGET_DIR="$HOME/.cache/listenrr-target"   # shared by all worktrees
cargo build                       # every module, 0 warnings
cargo test                        # the whole suite, fully offline (current count below)
cargo test proto::                # wire contract incl. the 2052-byte frame + save_buffer rows
cargo test ring::                 # wraparound, boundary, read-backwards, zero-fill retention
cargo test config::               # valid fixture + one test per validation failure
cargo test --lib                  # unit only
```

Lane tests belong in the module they test (`#[cfg(test)] mod tests`) plus `tests/` for
integration (S7's scripted source client, AC10). Fixtures are 16 kHz mono PCM WAVs under
`tests/fixtures` (`.gitignore` already allows that path and rejects other `*.wav`).

**Shared `CARGO_TARGET_DIR` caveat.** With one target dir across worktrees, cargo's
per-worktree fingerprint can report `Fresh` while `deps/listenrr-<hash>` holds a
*different* worktree's compiled code — a lane then quotes a green run it never executed
(this fooled two lanes). Prefix any test command whose result you intend to report with
`touch src/*.rs &&`, and only believe a run whose `Compiling listenrr v0.1.0 (<your path>)`
line names your tree and whose test names are yours.

**Clippy caveat — the obvious invocation checks almost nothing here.** `cargo clippy` is not
installed for the pinned toolchain (`error: 'cargo-clippy' is not installed for the toolchain
'1.99.0'`), and the bare `cargo-clippy` that *is* on `PATH` consumes its first argument as its own
subcommand name, so `cargo-clippy --all-targets -- -D warnings` silently drops `--all-targets` and
lints **lib and bin only** — it exits 0 on a tree containing deliberate `assertions_on_constants`
and `needless_range_loop` violations in an integration test. The form that actually checks the
workspace is:

    cargo-clippy clippy --all-targets -- -D warnings

Because `RUSTC_WRAPPER=kache` replays cached diagnostics, `touch` is not enough to make that a
result rather than a recital: precede it with `cargo clean -p listenrr` and only believe the run
that prints `Checking listenrr v0.1.0 (<your path>)`. Any earlier "clippy `--all-targets` green"
in this file predates that discovery and was checking two targets of six.

Current state: `cargo build` clean (0 errors, 0 warnings), the clippy form above exit 0 with zero
diagnostics on a freshly checked tree (`cargo clean -p listenrr` first — `RUSTC_WRAPPER=kache`
replays cached diagnostics, so a run that does not print `Checking listenrr v0.1.0` is not a
result), `cargo test` **235 passed / 0 failed** — lib 189 (22 `proto`, 23 `coordinator`,
20 `ha`, 18 `parakeet`, 18 `ingest`, 17 `config`, 16 `kws`, 16 `capture`, 14 `ring`,
11 `clips`, 6 each `verify`/`replay`, 2 crate-level helpers), bin 12 (`src/main.rs`),
18 in `tests/listener.rs` (socket-level admission and supersession), 5 in
`tests/scripted_source.rs` (AC10, hand-written client), and 11 in
`tests/wire_contract.rs` (firmware↔backend differential: the ESPHome codec compiled with
`g++` and driven byte-for-byte against `proto` + serde in both directions). Counts are from the
runner's own output, not from a tally of names; the module split above is `cargo test --lib
-- --list` tallied by module. The additions since the previous figure (215) are, by commit:
`4d82a9c`/`36128df` (ordered ASR tiers — `parakeet` 6→18 and the client-construction cases in
the binary), `d123e9b` (`ha`: a `state_changed` outside the mute set must produce no warning,
reddened by swapping the membership check and the parse back), and the two `ha` cases added with
U2 — `accessor_finds_the_captured_intent_speech` and
`run_pipeline_parses_the_captured_live_envelope`, which feeds Home Assistant's own captured
event bytes to the tolerant reader and was red twice before the reader was fixed (its value over
a mock-fed test is exactly that: the mock speaks a dialect the real service does not). The
`coordinator` case added since, `a_turn_that_asks_nothing_never_enters_the_pipeline`, is a
product of that fix rather than of a spec: the first `ha: intent-end` line the fix produced
showed Home Assistant's agent replying "Sorry, I had a problem talking to OpenAI: … No user
message found in messages" to a bare wake word, because AC1 passes those forms with an empty
command and the empty string was being sent as `input.text`. It asserts at the choke point
(`on_verified`) that an empty request yields neither a `RunPipeline` nor a false-accept clip and
that the group returns to `Idle`.
Earlier rounds, recorded here when they were the delta and still present: the connection-identity
and registry-atomicity cases in `ingest`, socket case 18 (a superseded connection whose *next*
write is only part of a frame — reddened by hand, by deleting the ownership gate and watching it
hang instead of close), and four differential cases closing gaps the first harness left open:
split `0x03` feeds decoded incrementally, the `MAX_CONTROL_JSON` total-payload cap at 1024/1025,
both tokenless hello shapes, and the `-0.0` RSSI divergence.
Field-boundary cases are now built by
deriving each string from a prefix and asserting its length, because two earlier test comments
described lengths their own strings did not have (344 and 500 for strings of 340 and 496). No
`todo!()` or `unimplemented!()` remains in `src/` (re-grepped, not inherited).

## 6. Where the plan was ambiguous, and what this contract chose

The plan is unchanged; each of these is a local decision with a test behind it.

| # | Ambiguity | Choice | Why |
|---|---|---|---|
| 1 | "frame total 2052 B" vs the plan's byte list `tag + seq u32 + 2048 B PCM`, which sums to 2053 | `AUDIO_FRAME_LEN = 2052` (seq + PCM, the contract's frame) **and** `AUDIO_RECORD_LEN = 2053` (tag + frame, what the socket consumes) | Both numbers are then explicit; the tag is record framing. **Lane B must match the record: 2053 bytes per frame on the wire.** |
| 2 | Ring reads for samples older than the retained window | zero-fill, not an error; `retained_start()` is exported so a caller can detect the hole | Keeps capture/clip paths free of error plumbing for a bounded read; tested both ways. |
| 3 | `range()` misuse | `end` clamps to `written()`, `start >= end` → empty vec, never a panic | A voice service must not die on a bad span; `last(n)` clamps to what exists (a 2-s-old source asked for 10 s gets 2 s). |
| 4 | `[[groups]]` appears in the plan's file list but not in §4's key list | optional table; when present every `sources[].group` must name a declared group, when absent group names come from the sources | Catches a typo'd room without forcing single-room configs to declare a table. |
| 5 | `ha.internal_url` required? | typed `String`, may be empty = "satellites can reach `ha.url`" | A minimal config must load; the field only matters for satellite-fetchable TTS. |
| 6 | Silero VAD model has no config key, but §4 names a VAD component | optional `capture.vad_model`; `None` ⇒ use `kws.model` (where the sherpa bundle carries it) | Optional, so plan-shaped configs stay valid; `SileroVad::new(&Path)` has one honest input. |
| 7 | `S6.2 min_utterance_ms` isn't in §4's list | `capture.min_utterance_ms`, default `0` (check disabled), validated `≤ hard_cap_s * 1000` | S6.2 names the knob; 0 keeps the plan's default behavior. |
| 8 | Graceful shutdown needs `tokio::signal`, which is not in §4's feature list | feature added, with the reason recorded | S1.1 requires graceful shutdown; the plan restricts *dependencies*, not features. Revert only by also removing the shutdown path. |
| 9 | Hello line with trailing blank line | extra trailing `\r`/`\n` stripped; a second **non-empty** line is a JSON error | `serde_json` tolerates trailing whitespace, so strictness there only produced false rejections. |
| 10 | `FrameDecoder` error handling | first error latched: `feed` yields nothing while held; `take_error` consumes and re-arms | Ingest closes the connection on the first error; the semantics are stated and tested either way. |
| 11 | `Control` tag arriving **upstream** | `ProtoError::BadTag` | `0x03` is downlink-only; a source sending it is a firmware bug, and the plan wants drift caught (R10). |
| 12 | `snr(ring_range, kw_span, noise_range)` — how `kw_span` relates to the passed slice | `ring_range` is the `kw_span ± 300 ms` window aligned so `[0]` is `kw_span.start − 300 ms`; `kw_span` sizes the numerator and rejects a degenerate span; silent denominator → large finite ratio, never NaN | The frozen signature has no offset parameter, so the alignment is documented instead of invented per lane. |
| 13 | `Config` needs a source lookup for ingest | `Config::source(&str)` / `Config::group(&str)` helpers | Eight lanes should not each write their own linear search. |
| 14 | **What domain a sample index is in.** Two clocks count the same audio: `SourceRing`'s write cursor and the KWS thread's received-frame count (`seq × 1024` looks like a third). | **The ring write cursor is canonical** — `SourceRing::written()` / `record_frame`'s return value. `seq` is for gap *detection* and dropped-frame accounting only; `seq × 1024` is never a sample index. Registration therefore also sends the KWS thread a zero-length `(source, [])` **stream-restart marker** (not audio: not counted in the AC3 frame total, filtered out of every frame-counting test), which resets that source's spotter stream and rebases its counter to 0 via `SampleCounters::rebase`. Unregistering sends one too, so a departed source's spotter stream is evicted (KWS-5), and a registration whose marker cannot be sent is unwound and its connection closed rather than left registered on an unrebased clock (I1). | The ring is append-only, so a sequence gap *compresses* rather than leaving a hole, and `seq × 1024` would then disagree with the ring while `written()` stays right. On a reconnect `register()` replaces the ring (cursor back to 0) and the firmware resets `seq`, so the marker is the only thing that can move the spotter's origin; without it every span after a reconnect is offset by the previous connection's samples and `ring.range(span)` reads an empty window. Consequence, stated rather than hidden: the thread's counter equals the ring cursor except for frames the *producer* drops because the channel was full (R9) — a `kws_drops` change since a source's last wake drifts its spans by 1024 samples per drop, so the wake path `warn!`s the drift in samples. Frames the thread skips for suppression, and frames it sheds from a deep backlog, still advance the counter (KWS-1, I2), so neither drifts it; consumer-side sheds are logged but not counted in `kws_drops`, because the frozen seam gives the thread no registry handle. Tested: `a_registration_sends_the_kws_stream_restart_marker`, `a_restart_marker_rebases_the_wake_span`, `a_sequence_gap_is_counted_without_leaving_a_hole_in_the_ring`, `suppressed_frames_advance_the_clock_and_the_resume_rebases_the_origin`, `a_shed_backlog_keeps_the_clock_even_with_the_ring`, `a_registration_with_a_full_kws_channel_is_refused_not_left_unrebased`. |
| 15 | `backward_bounds(recent, kw_span, vad)` — what window base is a caller supposed to pass, and is the end inclusive? | Callers pass `recent = ring.range(kw_span.end.saturating_sub(BACKWARD_LOOKBACK_SAMPLES)..kw_span.end)`: the window's last sample is **exclusive** at `kw_span.end`, and `kw_span` is in the absolute (ring-cursor) domain of row 14. Inside, `base = kw_span.end − recent.len()` converts to the window's own domain. | `backward_bounds` has no ring and no offset parameter, so the base can only come from the caller. Passing a window that ends anywhere else silently shifts every utterance start, and `ring.range` zero-fills below `retained_start()`, so a wrong base can look like a legitimate quiet window. Enforced at both production call sites — `main.rs`'s `run_capture` (the S3.1 pass whose result becomes `CoordIn::EndpointReached::utterance_start`) and `replay::run` — and asserted by `replay`'s bounds output and `capture`'s absolute-domain test. |

| 16 | Two config values had no path to the code that needs them: `ha.pipeline_id` is required and validated but nothing carried it to `run_pipeline` (which sent `null`), and `PipelineOutcome.mime_type` was dropped at `CoordOut::PlayMedia` so `play_media` hardcoded `"music"`. | `HaClient` gained `pub pipeline_id` (third `new` argument), `play_media` gained an `Option<&str> mime` parameter, and `mime_type: Option<String>` was added to `CoordIn::PipelineFinished` / `CoordOut::PlayMedia` so the pipeline's actual type reaches playback. `"music"` survives only as the fallback when the pipeline reported none. | A required config key that nothing reads is a silent misconfiguration: HA would have used its own default pipeline while the operator believed they had set one. Threading mime through the coordinator (rather than re-deriving it at the call site) keeps the coordinator free of HA knowledge. The alternative — a `pipeline_id` argument on `run_pipeline` — just moves the seam problem to every caller. |
| 17 | `parakeet::framing`'s stub described a 1-byte-tag / `u32` LE-length header with `TYPE_ASR_*` tags, which is not the Wyoming framing the plan itself specifies. | Re-shaped to 4-byte BE header length + header JSON + header-described payload, all constants in that one `pub mod framing`. Grep-verified that nothing outside `parakeet.rs` referenced the old constants. | The stub's shape was a placeholder, not the contract; keeping it would have produced a client that could never talk to the ASR host. **This does not close U1** — the values are still spec-derived and must be re-pinned against the live endpoint. |
| 18 | `SileroVad::new(&Path)` takes no tuning input and no config key exists, yet the VAD needs a threshold, min-silence, min-speech, window and max-speech. | The five constants are hardcoded in `capture.rs` (0.5 / 0.25 s / 0.10 s / 512 / 20 s, mutually consistent: min-silence < `trailing_silence_ms`, min-speech < `preroll_ms`, max-speech > `hard_cap_s` so the detector can never truncate a legal utterance). **No config keys were invented for them.** | Plan Principle 4 says thresholds belong in config, but S3.4 is where those values get *tuned*, and tuning needs the model plus real household clips — hardware we do not have. Adding knobs now would advertise tunability that nothing has evidence for. This is a **known gap, not a done item**: the keys land during S3.4 with measured values. |
| 19 | Nothing in the contract said **which** connection may remove a `source_id`'s entry, and `record_frame` resolved purely by id with no concept of connection identity. So a satellite that reconnected could be killed by the reap of its own dead socket: the epilogue unregistered unconditionally, and the replaced connection's first frame hit a bare `return Ok(())` that skipped both the R9 count and `SourceGone` — a satellite could go dark with no log, no counter and no event. | Ownership-checked newest-wins: ownership is decided in one place, by `Arc::ptr_eq` against the registry's current entry, and gates the epilogue (row 21 records how that decision came to be *held* across the work it authorises, which is where the original form fell short); `record_frame` takes `&Arc<SourceHandle>` and refuses unless its caller holds the registration; all exits route through one epilogue via `break 'read`, counting the held frame *before* the ownership branch; a superseded close is logged with peer, `source_id` and whether a newer connection holds the id. Separately, `READ_IDLE_DEADLINE` (300 s, `tokio::time::timeout` on the read) bounds a reap that was otherwise unbounded: a peer that vanished without a FIN held a task and a socket forever. | §12 already said the backend **replaces** the entry (newest-wins) and that a disconnect emits `SourceGone`; the code contradicted both, so this conforms rather than invents. Plain first-wins is unsafe here because "the old socket is reaped eventually" is false — tokio exposes no `set_keepalive` in any version and there was no read timeout, so a ghost could hold the identity permanently and lock out a live satellite. The harm is asymmetric: a stale socket reaped late costs nothing, a refused satellite is a missed wake. The deadline is 10× the mandatory 30 s `rssi` heartbeat, which `luna_stream.cpp` emits from `loop()` whenever the state is Connected with no mute gate, so a live peer cannot reach it. |
| 20 | Both clip writers resolve their source **when they write**, not when the audio arrives — `write_missed_wake_clip` / `write_false_accept_clip` call `registry.get(source)` after the event has crossed a channel, and the owning connection's epilogue removes the entry whenever it gets there first. A satellite that dropped offline in between lost the clip: the samples were still in memory, the log said "unknown source", and nothing reached disk. Found while writing the §12 sentence that promised the opposite. | `unregister` retires the registration its owner just removed; `SourceRegistry::recently_gone` serves it for `RECENTLY_GONE_WINDOW` (5 s) and **only while the id is unregistered**; both clip writers resolve through one `handle_for_already_received` helper. The retired list is capped at `RECENTLY_GONE_MAX` (8) and pruned on insert, and the expiry is a parameter of the private `…_at` (as `maybe_log_metrics` takes `now`) so the window is tested without sleeping. | This is the only place where "the source is gone" and "the audio is gone" are different facts, so it is the only place the grace applies: presence, attribution and frame writes still go through `get`, and retirement lives in a **separate map** so no other reader can be misled by it. 5 s is three orders of magnitude above the scheduling gap it has to cover; 8 exceeds any plausible number of satellites reconnecting at once, so the memory is bounded by construction rather than by a timer. Chosen over putting a `RingHandle` on `IngestEvent`, which would leak ingest internals into a protocol event type and every test literal to fix a problem two call sites have. |
| 21 | §12 promises a replaced connection is "closed when it next sends anything" and that only the owning connection may act for a source. The frame path honoured that; **the `UpMessage::Event` arm had no gate at all**, and both ownership decisions were check-then-act: `owns_registration` and then a mutation, `owns_registration` and then `unregister`. Found by an independent review of code I had written and documented an hour earlier — not by the tests, which had no case that sent an *event* on a superseded connection. | One primitive instead of two: `hold_ownership`, which confirms `Arc::ptr_eq` **and keeps the registry locked** while the caller acts. The frame path decides once and holds across the ring write, the seq-gap and jitter accounting, `push_kws` and the capture broadcast; the event path decides once and holds across the rssi record and the forward; `unregister_if_owner` tests and removes in one critical section. `record_rssi` is now attributed to the connection that measured it rather than re-resolved by id; `record_kws_drop` keeps its id-keyed form for the epilogue's R9 count, with an `as_owner` variant for when the lock is already held. The lock-free predicate was deleted rather than left beside it. | The unguarded event arm was not cosmetic: a ghost's stale `rssi` overwrote the live source's signal reading, a ghost `playback_done` could end the live session's Speaking state, and because **any inbound byte resets the read deadline**, a peer that sends only its 30 s heartbeat was never reaped — the bound §12 had just justified could not fire for exactly the peer most likely to need it. Two ways to ask the same ownership question is how a caller ends up asking the unsafe one, so only the safe form exists. Case 17 in `tests/listener.rs` is the regression test and was shown red with the gate removed (the ghost hangs; a hang is not a close). |

| 22 | Row 21 made the frame and event paths decide ownership once; **the same class survived one level up**, in the registry's own mutators and the metric loop. `unregister`, `unregister_if_owner` and `register` all released `inner` between changing the entry and touching the per-connection state beside it (`arrivals`, `last_metric`, `retired`), and `update_jitter` re-resolved the handle by `source_id` while `maybe_log_metrics` was already iterating a specific handle. | `inner` is held across the whole mutation in all three — remove→forget→retire, and insert→forget — and `update_jitter` takes the `&Arc<SourceHandle>` the loop is already holding, writing its percentiles into **that** handle's stats. New nesting `inner` → `last_metric`/`retired`, audited acyclic: nothing in `{stats, ring, arrivals, last_metric, retired}` acquires another lock while held, and `recently_gone_at` takes `inner`, releases it, then takes `retired`. | The gap is not cosmetic. A satellite reconnecting inside it has *its* arrival samples and metric cadence deleted by the connection that is leaving; if it also drops before that connection reaches `retire`, its handle is pushed **behind** the one retired there, so `recently_gone` (row 20) serves the older ring — the wrong audio, silently. On the metric side a departed connection's jitter percentiles could overwrite a live connection's "no samples yet" sentinel, and one log line could splice one connection's room and uptime to another's counters. Found by the independent review that closed the earlier three findings, in the pass asked to hunt this class rather than re-check the fix. |

| 23 | A per-connection artifact — a source's ring, a capture cursor, a wake span, a connection's stats — is chosen while the connection is live and then **re-resolved by `source_id`** at the moment it is used. An independent hunt for this class, after the earlier fixes had closed its frame and event instances, found five more: `run_capture` decided liveness with `registry.get(&source).is_none()` while polling one specific handle's ring; `start_transcription` re-resolved the ring by id after the endpoint had crossed a channel; `write_false_accept_clip` sliced whatever ring the id named *after* a network round-trip to ASR and the verify gate; `check_wake_domain` only `warn!`ed that a wake's span could not exist in the current ring and then started the capture anyway; and the coordinator's completion handlers acted on a group by `source` alone. | **The handle chosen at the moment of use is the capability.** `Wiring` carries `(conversation_id, Arc<SourceHandle>)` for the capturing connection, written where capture starts and swept with the turn; the endpoint → transcribe → false-accept path reads that handle and never calls `registry.get(source)` for turn audio. Liveness becomes ownership: `SourceRegistry::is_current(&handle)` is a *decision*, not a claim — which is why it is not a reintroduction of the `owns_registration` predicate that row 21 deleted — and a capture whose own handle has been superseded ends at once instead of polling a frozen ring to `hard_cap_s`. The coordinator stays data-only: no handle enters `CoordIn`/`CoordOut` (they are `Clone + PartialEq` and hand-built throughout its tests); completions carry the `conversation_id` of the turn they belong to — `VerifyRejected` was the one that lacked it — and `serve_turn` refuses anything that does not match the live session. A winner that disconnects **mid-turn** marks the group `gone`: the turn stands, because its audio was already delivered and is still worth transcribing, but `on_pipeline_finished` will not enter `Speaking` with no endpoint to play on. A wake that the current ring cannot satisfy is refused, not warned about. | Every one of these was silent. A reconnect mid-utterance meant twenty seconds of dead air and then "nothing to transcribe", because the poll was watching a superseded ring that could never advance while `get(&source)` stayed `Some`; a disconnect mid-utterance dropped an utterance the satellite had spoken in full, and the endpoint arriving one poll later was discarded as "not capturing"; a false-accept clip could be sliced from the *next* connection's buffer at the *old* connection's offsets; a late `VerifyRejected` from an abandoned turn tore down the LED and capture of the turn that followed it, and a late `PipelineFinished` resurrected an `Idle` group into `Speaking` — deaf, group-suppressed, for `speaking_timeout_s`. Each is now covered by a test that was shown red with its guard removed. Two sites are left deliberately: the missed-wake `save_buffer` keeps `handle_for_already_received`, because a missed wake has no turn to bind a ring and span to, and the follow-up capture path is untouched because the coordinator never arms `CoordOut::Endpoint` with `kw_span: None`. |

| 24 | The differential harness compares **bytes**, and on two values the two sides genuinely do not agree — a comparison that is silent about its own blind spots is not an instrument. (a) `build_hello` writes the `"token":` key unconditionally (`luna_stream.h`, `nullptr` → `null`), so the shipped satellite can never emit the key-absent hello that §3.1 recommends; (b) serde formats `f64` through ryu, which preserves sign, so `Event::Rssi { dbm: -0.0 }` prints `-0.0`, while the firmware's `tenths` emitter does integer division and a remainder and has no way to print a negative zero. | Both are **stated and pinned rather than harmonised**. (a) The two spellings are proven equivalent where it matters (`Option<String>` + `#[serde(default)]` → both decode to `None`, and not to `Some("")`, which §4 would treat as a mismatch) and the *difference* is asserted to be exactly the token key: removing `"token":null,` from the firmware's own bytes must reproduce §3.1's payload. The firmware has no hello parser — hello is uplink-only and `build_hello` is its sole entry point — so the comparison is the builder's output decoded back by the backend, which is also where a builder that turned an absent token into an empty string would show. (b) `-0.0` is deliberately **not** added to the byte-comparison table, where it would be red by construction and would teach nothing; it is pinned by `negative_zero_rssi_is_a_documented_hazard`, which asserts serde's current text and that the value parses back equal to `0.0`. | Neither can be reached from the firmware side today: the component computes an integer number of tenths, and `-0` and `0` are the same integer. So neither is an intentional divergence to excuse, and neither is a case to force through a comparison that cannot express it. What each test buys is the failure: if serde stops emitting `-0.0`, or the RSSI value ever comes from a float computation that can produce negative zero, that assertion says so here instead of surfacing as an unexplained byte mismatch in `uplink_bytes_match_backend_encoders` — and if anyone "fixes" the hello by making the encoder skip `None` fields, the byte-diff assertion is what points at the firmware's frozen shape as the real constraint. |

### Rows 25–26: owner-approved amendments, not plan ambiguities

Rows 1–24 are this contract's own local decisions. Rows 25–26 are different in kind: they are
frozen surfaces amended **with owner sign-off** under the stop-command plan (unpublished; its section 5,
plus an explicit **Start execution**), recorded here so every later diff references an existing
record.

| # | Ambiguity | Choice | Why |
|---|---|---|---|
| 25 | The stop-command plan (unpublished) needs a spoken "stop" on the wire and operator-tunable phrases, but both live in contract files lanes MUST NOT edit (§2, working rule 1). | **`src/proto.rs`:** `Control::Stop` (unit variant, serde tag `stop`, wire JSON `{"type":"stop"}`), documented tablet-only. **`src/config.rs`:** `capture.stop_phrases: Vec<String>` (serde default `["stop"]`), `kws.stop_keywords_file: Option<String>` and `kws.stop_threshold: Option<f32>` (default `None` = use `threshold`), and `pub fn normalise_phrase(&str) -> String` (lowercase, trim, strip leading/trailing non-alphanumeric-non-apostrophe, collapse internal whitespace), used by `Config::validate` and by the coordinator's phrase match from one definition. | A1: no existing control can express stop — reusing `Play` with an empty url would hit the null-url path and emit a spurious `playback_done` (`Player.java:65-68`). A2: the phrase list and the keyword file must be operator config, and one normaliser shared by validation and matching is the only way `"Stop"` fails loudly at load instead of silently never matching. |
| 26 | The same plan needs a second spotter, a coordinator effect, and a Home Assistant stop call, and all three are frozen seam signatures ("public signatures are frozen; add private items only"). | **`src/kws.rs`:** `run_kws_thread` gains one trailing parameter (`stop: StopLane`); new `pub StopEvent { source, arrived }` and `pub StopLane` (private fields) so the extra state crosses as one argument. **`src/coordinator.rs`:** `CoordIn::Stop(StopEvent)`, `CoordOut::StopListenChanged { sources }`, `CoordOut::StopPlayback { source, entity }` (entity via the same `output_entity` `PlayMedia` uses; empty = nothing to stop). **`src/ha.rs`:** `HaClient::media_stop(&str)` with `play_media`'s async-ness and `Result`, minus url/mime/announce. | A3: the spotters are reachable only through `kws_rx`, a **single-consumer** channel — a separate thread or an ingest-side route would change `serve`'s frozen signature instead. A4: the coordinator is the only stateful component, so every effect must be a `CoordOut` (I1). A5: no stop service call existed anywhere (`call_service` appeared only in `play_media`). No other frozen signature changes: `serve`/`serve_on`, `verify_utterance`, capture, ring, the wake path and `lib.rs` are untouched; `stop_listen()`/`set_outs()` are private; `main.rs` is orchestrator-owned. |

## 7. Still open by design (not contract gaps)

**What is finished.** No `todo!()`/`unimplemented!()` is left in `src/`: `run`, `replay`
and `bench-kws` are real, and `main.rs`'s I/O arms (`Transcribe`, `RunPipeline`,
`PlayMedia`, capture start/stop, both clip arms, the retention sweep) call the lanes. The
CLI is exactly the plan's S1.6 surface plus `health`; there is no `--test-wake` or
`--record` — the stub doc comments mentioned them and the plan's CLI contract never had
them, so they were not carried forward.

**What is outstanding, stated as outstanding.** None of this is a green check:

- **AC2, AC5 (multi-source part), AC6, AC7, AC8, AC9 are not evaluated.** They need
  2–4 satellites streaming real audio, a live HA Assist run, a real
  80 dB speaker, an A/B reboot, and CI. The state machine, suppression, replay, clips and
  the protocol client are implemented and unit-tested; their acceptance is not measured.
- **AC11 is half measured.** The backend half ran on the deployment host on 2026-11-02: a `save_buffer`
  event from a remote client produced a `clips/missed/` clip of exactly 10.0 s carrying the
  bytes that were sent. What is not evaluated is the device half — the physical "for you"
  button and the firmware mapping that emits the event, which exist only as a host-side
  self-test of the codec. See `docs/design/TEST-NOTES.md` for the evidence.
- **The ESPHome component now compiles for real** — for the devkit configuration as it then was, which plan Revision 7 retired and which `config/satellite.example.yaml` has since replaced in place with the Waveshare ESP32-S3-AUDIO configuration (the devkit file is in git history before that change). `esphome compile
  config/satellite.example.yaml` exits 0 on a clean build (1425 ninja progress markers
  reaching `[1291/1294]`, `luna_stream.cpp.obj` and `main.cpp.obj` both present in the log,
  0 errors, 0 warnings from the component; 1 126 499 B flash = 61.4 %, 117 559 B RAM =
  34.4 %), verified from an empty build directory rather than an incremental one. **These are
  devkit numbers:** the component's own cost is reusable, but the reference board is a
  different binary. Its figures (1 154 803 B flash, 118 939 B RAM) are recorded in the header
  of the new `config/satellite.example.yaml` as a compile record only: it has not been
  booted, and it was built in a reused rather than an emptied build directory. Re-measured
  2026-11-02 after the codec's escape handling landed; the ~1 KB of flash and ~270 B of RAM
  since the previous figure are that change, not drift. Toolchain: `nix shell nixpkgs#esphome`
  (ESPHome 2026.9.1) under a
  32 GiB `systemd-run --user --scope` memory cap; the config needs a local
  `config/secrets.yaml` (gitignored) because this version has no `--secret` flag.
  Running the gate was the whole point: it invalidated six things the host-side harnesses
  could not see —
  1. the pins were classic-ESP32 numbers on an S3 board: GPIO22/25 **do not exist**, and
     GPIO26 is `SPICS1`; re-pinned to GPIO14/15/16 (mic) and 17/18/7 (amp), with the
     excluded ranges documented in the file so they are not re-picked;
  2. `media_player: platform: i2s_audio` **has been removed** — announcements now go
     through a `speaker` platform `announcement_pipeline`, and the amp's pins live only in
     the `speaker` block;
  3. `MicrophoneSource` has no `set_microphone()` / `set_audio_stream_info()`; the real
     path is `microphone.microphone_source_schema(...)` + `microphone_source_to_code`,
     which is strictly better — a non-16 kHz mono mic is now rejected at *config* time;
  4. `MediaPlayer::add_on_state_callback` takes exactly one argument, so the SFINAE hedge
     over two shapes was dead weight and is gone;
  5. `esphome/components/wifi/wifi.h` does not exist (it is `wifi_component.h`, and
     `wifi_rssi()` returns `int8_t`) — the local stub had invented it, which is why the
     earlier syntax check was falsely green;
  6. `gai_strerror` is gated behind an lwIP define the ESPHome build does not set, and
     lwIP's `EAI_*` values are positive, so a POSIX-shaped table would have been wrong.
  Two warnings survive, neither in our code: ESPHome's advisory that an `ota` password
  costs ~3.5 KB of flash (its recommended `encryption: psk` needs a secret this example
  does not carry — a deployment decision), and one in the vendored opus sources that
  `format: NONE` pulls in. The codec is 515/515 on the host — rebuilt and run here with
  `g++ -std=c++17 -Wall -Wextra -DLUNA_STREAM_HOST_TEST=1` (488 before the wire-contract
  fixes, which added 27 differential-parity checks whose verdict is computed by serde rather
  than asserted by hand. Compiling this component is a gate that has to be *made* to bite: two
  runs after the differential harness finished mutating and restoring `luna_stream.h` exited 0
  while ninja recompiled `firmware.elf` and left `luna_stream.cpp.obj` forty-odd minutes older
  than the header, reporting size figures read from a stale map. Deleting that one object and
  rebuilding is what makes the number mean something — `[4/10] Building CXX object
  …/luna_stream/luna_stream.cpp.obj`, then link, then 117 559 B RAM / 1 126 499 B flash,
  byte-identical to the clean build above, which is also the check that the restored header is
  the file that was measured. Trust `Building CXX object … luna_stream.cpp.obj` in the log, never
  an exit code.
- **AC2 is now partly covered, and in software.** "Satellites connect" turned out not to need
  satellites: `tests/listener.rs` drives `ingest::serve_on` over real TCP with 18 cases — auth and
  admission in both directions, three concurrent sources with per-source attribution, one departure
  producing exactly one `SourceGone`, a frame arriving on a replaced connection refused, the held
  frame counted whichever exit path is taken, and a superseded connection closed when it sends
  *anything* — including a write that is only the first two bytes of a frame, which is the case a
  message-completion-only check would pass by never reading it at all. There are no spare devices,
  so this is the only route that path had. What no socket can stand in for is RF and silicon: real
  I2S capture, Wi-Fi behaviour, 80 dB playback, and AC8's 100-reconnect soak with real audio stay
  device work.
- **AC8's reconnect budget is tighter than the plan assumes.** Read from the shipped backoff:
  `on_connected_()` resets the attempt counter on a *successful TCP connect*
  (`luna_stream.cpp:378`), before the hello — so a source that is refused after connecting retries
  once at `reconnect_delay_ms(0)` = 1 s and is then admitted, which makes the supersession path
  cost about a second, not a ladder walk. The ladder only advances when `connect()` itself fails,
  i.e. the backend is down, and there the cumulative waits are 1, 3, 7, 15, 31, 61 s: AC8's
  single-satellite ≤ 30 s is exceeded at the **fifth** retry and the fleet ≤ 60 s at the **sixth**.
  A restart long enough to reach the sixth retry cannot be met with the caps as set. This is a
  prediction from the constants, not a measured pass or failure — it closes with the device soak,
  and if the soak confirms it the fix is the cap, not the check.
- **AC3 is met with headroom but not in the plan's literal form.** Measured on the pinned
  bundle in a *debug* build: 57–69× realtime against the 20× floor, 60/60 detections on
  the bundle's own keyword list. The literal AC3 (1 h of continuous satellite audio,
  release profile) is still owed. Models and the 8-minute concatenated fixture are left at
  `/tmp/lt-kws-models` and `/tmp/lt-kws-long.wav` so that run costs one command.
- **AC4 has no fixtures.** The replay chain is implemented and its pure parts are tested,
  but there are no real "luna" clips, no ground-truth `kw_span`, no negative set, no
  100-prompt false-accept battery and no 20-command battery. U3 is resolved (token
  timestamps are stream-relative, recorded in `docs/design/TEST-NOTES.md`), which tells the fixture
  author that each clip must carry its own explicit ground-truth span — the engine cannot
  supply it.
- **U1 and U2 are unpinned, and now for a reason that is measured rather than assumed.**
  The service did reach Home Assistant on the deployment host — the journal shows `auth rejected by
  Home Assistant: Invalid access token or password`, so the WebSocket path works and only the
  placeholder credential stops it, which is U2's remaining gap (a long-lived token plus a real
  `pipeline_id`, then capturing the actual `assist_pipeline/run` envelope). U1 cannot be closed
  yet: **there is no ASR service to capture from.** Ports 10200–10500 are closed on the deployment
  host *and* on the HA VM, so `parakeet::framing` stays spec-derived. The plan's assumption
  that the Parakeet Wyoming service runs on the deployment host is **not met**; the design plan,
  Revision 6, corrects that assumption in place (and its O1/R2/R8 dependents), and
  `docs/deployment.md` carries the measurements behind it. Do not close U1 by guessing again,
  and do not treat any document in this repository as claiming an ASR service exists.
- **S0 is partly measured.** KWS on the deployment hardware is 32× realtime on a single
  stream from a release build, and the Plex Quick Sync pass-through that S0 asked to confirm is
  present in the plex container's config (`/dev/dri`, `renderD128`, `devices.allow: c 226:*`).
  The Parakeet RTF gate cannot be run until an ASR endpoint exists, and the eight-stream
  KWS case has not been run either. The latency table still holds design targets, not
  measurements — the one latency this run produced (`WakeToEndpoint 5936 ms`) is from a
  long-form bundle clip with no TTS and is explicitly not an AC5 number.
- **Deployed on one host, and only that host.** the backend host `listenrr.example.lan`
  (container `<CTID>` on `pve.example.lan`) runs `/opt/listenrr/listenrr`
  under `deploy/listenrr.service`, `enabled` and active on `0.0.0.0:10700`, with the KWS bundle
  and Silero VAD fetched by `scripts/get_models.sh`. No satellite is flashed, no firewall
  change was needed beyond nothing at all (the container is on `vmbr0`, LAN-reachable), and
  no second host has ever run it. `config/listenrr.toml.example` and `docs/protocol.md` are
  verified against the loader and `proto.rs`, and the example's `ha.url` is known **wrong** for
  the original deployment — HA is a VM at `homeassistant.example.lan`, not `listenrr.example.lan`,
  which does not resolve; the runbook carries the working values.
- `replay` is not a CI test: the KWS stage needs a spotter model on the host, and the
  plan runs AC4 on the deployment host against the live Parakeet endpoint. With a
  `<stem>.transcript.json` sidecar its ASR stage is offline and no audio leaves the
  machine; without one it calls the real Wyoming client.
- The `Control` downlink has no seam to reach: an LED or earcon command is validated and
  logged, not delivered, because nothing in `ingest`'s frozen API hands a frame back to a
  live connection. Same for a per-source mute *intent* beyond HA's own entity.
- A false-accept clip's `candidates`/SNR columns stay empty: the candidate list is the
  coordinator's private state and its frozen API does not expose it (S8.1 records what
  exists rather than inventing the rest).
- The Silero VAD's five tuning constants are hardcoded (row 18); their config keys arrive
  with measured values during S3.4, not before.
