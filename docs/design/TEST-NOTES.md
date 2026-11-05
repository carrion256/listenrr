# TEST-NOTES — tuning and uncertainty records

_Historical engineering notes from the original development. Host names and addresses have been generalised; references to "the plan" point to an unpublished design document._

Per plan §7: **a failing check is never weakened to pass.** Thresholds move in config, and
every such change is recorded here with the evidence that motivated it. Per plan §3, the
bounded unknowns U1–U3 are resolved by measurement during their step, not assumed.

Status vocabulary — an entry is **RESOLVED** only with a reproducible measurement and the
command that produced it. Everything else is **PENDING**: no number, no pass, no inferred
value. An entry whose instrument has not yet run says so.

| Entry | Topic | Status |
|---|---|---|
| U1 / R2 | Wyoming framing shape | **RESOLVED** (measured): the request/response shapes are captured bytes held as literals in `src/parakeet.rs`, and a live exchange answered on 2026-11-02 — 2.0 s of audio in 0.119 s (rtf 16.9), one `transcript` frame, `data` keys `["text"]`. Command and evidence below |
| U2 / R3 | HA `assist_pipeline/run` event envelope | **RESOLVED** (measured): the six real events are pinned at `fixtures/ha-run-2026-11-02.json` and fed to the reader by `run_pipeline_parses_the_captured_live_envelope`. The stage is the event's own `type`; the reader's paths were already right, and the dump caught a live defect — HA carries a `tts_output` in `run-start`, which the reader used as its "this is the TTS stage" test, so `intent-end` was never read. Section below |
| U3 / R4 | `KeywordResult` timestamp semantics | **RESOLVED** (measured) |
| AC1 | Verify-gate keyword set and stripping | **MEASURED, and unchanged** — the AC1 table still passes as written, including its deliberate **bare forms** block (`"luna"`, `"loona"`, `"luna's?"` are accepted and yield an empty command). What was broken was downstream of the gate, not in it: that empty command reached Home Assistant as `input.text: ""` and the household's agent answered with a 400 apology. Fixed at the coordinator choke point (`on_verified`), deliberately not in the gate — new test `a_turn_that_asks_nothing_never_enters_the_pipeline`. The runbook's "What the first logged answer turned out to be" records why the gate is the wrong place for it, including the false-accept-corruption reason |
| AC2 | Source connect / admission / multi-source | **PARTLY MEASURED** — the socket half is covered by 18 cases in `tests/listener.rs` (`cargo test --test listener` → 18 passed), plus one real-network session on the Intel N100 test host (2026-11-02) where registration, admission, capability flags and room/group attribution were correct from a remote client; the device half is PENDING |
| AC3 | KWS realtime factor | **PARTLY MEASURED** — 32.3× and 31.8× on the two clips that ship in the model bundle, release build, one core of the Intel N100 test host, `passes_ac3: true` (command and output below). The plan's gate is KWS across **8 concurrent** streams; that has not been run, so this is not a pass. Supersedes the earlier debug-build figure |
| AC4 | Fixture battery | **PARTLY MEASURED** — a synthesised set exists and has already paid for itself: 10 positives + 8 near-miss negatives (+ padded variants), a 4-set × 2-threshold matrix through `replay`, and it located the `▁HE` token defect. It is recorded in `docs/deployment.md`, "The wake words". What AC4 actually asks for is still missing: real "luna" audio captured in a room, and ground-truth `kw_span` for it |
| AC5 | Idle-house p50 wake→playback_start | PENDING |
| AC8 | Restart resilience / scripted-fixture `kw_span` ground truth | PENDING — plus a backoff-budget prediction below that the caps may not meet it |
| AC11 | "for you" button → `clips/missed/` clip with previous 10 s | **PARTLY MEASURED** — the backend half ran on the Intel N100 test host: a `save_buffer` event over the wire produced `…-probe-missed.wav` of exactly 10.0 s, 16 kHz mono 16-bit, `listenrr:listenrr` 0640, containing the bytes the client sent. The physical button and the firmware's mapping of it to that event remain PENDING (host-side codec self-test only) |
| INSTR | The clippy gate as previously reported | **RESOLVED** (measured): the reported form lints two targets of six; corrected command below |
| — | Config tuning log | **one entry**: 2026-11-02, `kws.keywords_file` → the measured two-word set, forced by the matrix in the section below. `kws.threshold` and `kws.boosts` were checked and deliberately left alone |

---

## U3 / R4 — `KeywordResult` timestamps are STREAM-RELATIVE — RESOLVED

**Question (plan §3 U3, R4):** are `KeywordResult` token timestamps usable as absolute
sample positions, and what happens across a stream reset?

**Answer (measured, 2026-11-01, by the KWS lane):** `KeywordResult.timestamps` is a
`Vec<f32>` in **seconds, relative to the start of the current stream** — not absolute
positions in the audio. Observed on the pinned sherpa-onnx bundle with that lane's own
`keywords.txt`, via the spotter-hit log fields `kws.rs` emits
(`keyword`, `token_count`, `first_token_ts_s`, `start_time`):

| hit | `first_token_ts_s` | `token_count` | `start_time` |
|---|---|---|---|
| 1 | 5.44 | 5 | 0.0 |
| 2 | 4.48 | 3 | 0.0 |

`start_time` was `0.0` on both hits while the token timestamps were large: the only
consistent reading is that the timestamps count from the stream origin, and the KWS thread
resets every stream at each detection.

**Decision this settles:** `kw_span` MUST be derived by **adding the segment start to the
relative timestamp**, exactly as `SampleCounters::span` in `src/kws.rs` does
(`segment_start + seconds_to_samples(ts)`), and never by using the token timestamp as an
absolute position — that would be wrong by the whole segment offset, which is exactly the
failure R4 warns about. The count-derived start stays primary; the timestamp only ever
refines it, and only when the refined value lands inside the last `SPAN_REFINE_WINDOW_S`
(3 s) of the span, is finite, non-negative, and not before the last
`samples_at_reset` for that source. Anything else leaves the count-derived start standing.

**Consequence for fixtures (AC8, still PENDING):** a scripted fixture must carry its own
ground-truth `kw_span` **explicitly**. It cannot be recovered from engine timestamps,
because a relative timestamp has no absolute meaning without the segment origin the harness
would itself have to invent. Recorded as pending: no scripted fixture may claim a span
correctness result until that ground truth is present in the fixture.

**Open note on the measurement's model (not resolved here).** The bundle named in the
report reaching this file is `sherpa-onnx-streaming-zipformer-small-ctc-zh-int8-2025-06-02`,
whereas `scripts/get_models.sh` pins the English streaming-KWS bundle
`sherpa-onnx-kws-zipformer-gigaspeech-3.3M-2024-01-01`, and neither worktree contains a
downloaded bundle for either name. The timestamp semantics above are a property of the
sherpa-onnx streaming-KWS C API and hold regardless; the *model-specific* claims in this
file (AC3 headroom, full-precision preference) concern the **pinned 3.3 M bundle**, and the
AC3 number must be produced on it. Anyone re-running this measurement should record the
model directory they used.

---

## INSTR — the clippy gate was checking two targets of six — RESOLVED

**Question:** is the `clippy --all-targets -D warnings` exit 0 recorded in `CONTRACT-NOTES.md` §5
a result, or a recital?

**Answer (measured 2026-11-02):** not as written. `cargo clippy` is not installed for the pinned
toolchain at all — `cargo clippy --version` → `error: 'cargo-clippy' is not installed for the
toolchain '1.99.0-x86_64-unknown-linux-gnu'`. The `cargo-clippy` that *is* on `PATH` consumes its
first argument as its own subcommand name, so `--all-targets` is silently discarded and only lib
and bin are linted. Reproduced with a throwaway `tests/` file containing one
`assertions_on_constants` and one `needless_range_loop`:

| command | exit | lints caught |
|---|---|---|
| `cargo-clippy --all-targets -- -D warnings` | **0** | none |
| `cargo-clippy clippy --all-targets -- -D warnings` | **101** | both |

**Second trap on the same instrument:** `RUSTC_WRAPPER=kache` replays cached diagnostics, so
`touch` does not force a re-check — `cargo clean -p listenrr` does. A run counts only if it prints
`Checking listenrr v0.1.0 (<your path>)`.

**Consequence.** AC11's CI must use the `clippy`-subcommand form, and every "clippy green" in this
repo predating this entry was checking two of six targets. The tree is clean under the correct
command today: verified 2026-11-02, exit 0 with zero diagnostics after `cargo clean -p listenrr`.
The general rule this is an instance of — an instrument nobody has watched fail is an assumption —
is why every gate added since carries a case that makes it go red.

---

## INSTR — the build-cap proof read a file that does not exist — RESOLVED

**Question:** heavy commands here log a `CAPPROOF` line read from `/sys/fs/cgroup/memory.max`. Is
that evidence the 32 GiB cap held, or a line that prints a failure nobody reads?

**Answer (measured 2026-11-02):** the latter. This host is cgroup v2 unified, so
`/sys/fs/cgroup/memory.max` does not exist and `/proc/self/cgroup` is a single `0::/user.slice/…`
line — the naive path can only ever print an error. The read that works targets the process's
**own** cgroup:

```sh
CG=/sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)
echo "memory.max=$(cat $CG/memory.max) memory.high=$(cat $CG/memory.high) memory.peak=$(cat $CG/memory.peak)"
```

Run inside `systemd-run --user --scope -p MemoryHigh=28G -p MemoryMax=32G`, it prints
`memory.max=34359738368` (32 GiB exactly) and `memory.high=30064771072` (28 GiB exactly): the
kernel confirming the limit, which is the claim being made. `memory.peak` is the high-water mark
to compare against those two. Two details are load-bearing — the read must happen **inside** the
scope, and `awk -F:` does not survive the quoting through `systemd-run` → `bash -c` → `awk` (it
took its program as a filename); `cut -d: -f3` does.

**Consequence.** The cap was always *applied* — `systemd-run` enforces it whether or not anything
can read it — but no earlier build in this session *evidenced* it. Every capped build from here
prints the line above; a cap absent from the log is a cap not proven. Same class as the row above:
an instrument that cannot fail is not a check.

---

## Model selection and keyword discovery — documented choices (KWS lane)

- **Full precision wins over int8.** A release ships `{stem}-*.onnx` and
  `{stem}-*.int8.onnx`; `find_model_file` prefers the full-precision file and takes an
  int8 one only when that is all the directory has. Rationale (the lane's, and plan
  principle 1): recall is sacred, and the 3.3 M spotter has AC3 headroom either way, so
  there is no accuracy worth trading. Candidates are sorted so two hosts resolve to the
  same file rather than depending on `read_dir` order.
- **`bench-kws` discovers `keywords.txt` inside the model directory.** The frozen
  `kws::bench(&Path, &Path)` signature carries no keywords argument (contract seam), so
  `find_keywords_file` prefers `keywords.txt` next to the model and falls back to another
  `*.txt` list there. This is why the bench runs self-contained against a downloaded
  bundle: it decodes with the bundle's own keyword list, not the deployment's
  `kws.keywords_file`. A bench number is therefore a property of the *bundle*, and any
  claim about the configured keywords needs the deployment path, not the bench path.

---

## PENDING entries

Each lists the measurement that closes it. Until then: no value, no pass.

### U1 / R2 — Wyoming framing — **RESOLVED** (captured 2026-11-02)

Closed by bytes, in two layers, not by inference.

**The bytes are in the tree.** `src/parakeet.rs` ("U1 is pinned, not assumed") carries the
request and response shapes captured from `lmoe/wyoming-onnx-stt` (Parakeet TDT 0.6b-v3,
wyoming 1.8.0) on the GPU box, and the tests hold them as **literals on purpose** — a fixture
built with our own encoder could only ever agree with itself, which is the failure mode this
pins. `fixtures_are_the_captured_bytes_and_their_data_lengths_agree`,
`one_full_chunk_is_byte_identical_to_the_captured_request`,
`audio_chunks_announce_sample_width_in_bytes_not_bits`, and
`transcribe_sends_the_captured_bytes_and_reads_the_transcript_blob` are the witnesses.

**The two things the spec-based constants got wrong were each caught against the running
service**, which is what those tests now pin: the `data` JSON blob is a *separate* read after
the `\n`-terminated line, never inline in it; and `width` is **bytes** per sample (2), not
bits — send 16 and the server dies inside `audioop.lin2lin`.

**Reproduce** (a second implementation, deliberately mirroring `src/parakeet.rs` rather than
sharing it — agreement between the two against a real service is the proof, where either alone
could be self-consistent and wrong):

```sh
python3 ~/.cache/wyoming_rtf.py --host 192.168.1.40 --port 10300 \
    --wav /tmp/luna-pad/pos_hey_a.wav
```

Measured 2026-11-02 against the GPU tier: 2.0 s of audio answered in **0.119 s**
(**rtf 16.9**, `would_pass_listenrr_10s_timeout: true`), request **69 020 bytes**, transcript
`Hey Luna`, and exactly one reply frame — `{"type": "transcript", …}` with a `data` blob whose
keys are `["text"]` and no payload. That frame list is the "response byte shape" this row asked
for; `headers_seen` in the tool's JSON output prints it for any clip.

### U2 / R3 — HA event envelope — **RESOLVED** (captured 2026-11-02)

The six events Home Assistant 2026.9.2 sent for `assist_pipeline/run`
(`start_stage: "intent"`, `end_stage: "tts"`) are stored verbatim at
[`../../tests/fixtures/ha-run-2026-11-02.json`](../../tests/fixtures/ha-run-2026-11-02.json), and
`ha::tests::run_pipeline_parses_the_captured_live_envelope` feeds them to the tolerant reader:
the agent's text, the TTS url and `audio/mpeg` are asserted against the real bytes rather than
against the mock's dialect. Reproduce with (the token comes from the deployed config; the third
argument is where the raw event objects are written):

```sh
HA_TOKEN="$(sed -n 's|^token = "\(.*\)"|\1|p' /etc/listenrr/listenrr.toml)" \
  python3 ~/.cache/ha_ws.py run <pipeline_id> "what is the weather" \
  tests/fixtures/ha-run-2026-11-02.json
```

**The envelope.** Every event is `{type, timestamp, data}` and the **stage is the event's own
`type`**: `run-start, intent-start, intent-end, tts-start, tts-end, run-end`. The paths the
reader needs:

| Field | Real path | Reader |
|---|---|---|
| the agent's answer | `data/intent_output/response/speech/plain/speech` | `INTENT_PATHS[0]` — was already right |
| TTS media | `data/tts_output/url` | `TTS_URL_PATHS[0]` — was already right |
| TTS mime | `data/tts_output/mime_type` = **`audio/mpeg`** | `TTS_MIME_PATHS[0]`; R-C's `media_content_type` now carries a measured value instead of an assumed `audio/flac` |

**What the dump caught, and why 26 green run tests had not.** `run-start` itself carries a
`data.tts_output`. The reader's test for "is this a TTS event" had been *does it carry a
`tts_output`*, so the **first event of every run** satisfied it: the loop returned there,
`intent-end` was never read, `intent_summary` was always `None`, and the url handed to the
satellite came from the run's opening announcement instead of the stage that produced it (the
same string in this household — luck, not behaviour). The test for this was red twice before it
was green. The trigger is now that an event **names** the stage (`tts_named`: `type: "tts-end"`,
or the older `data.stage: "tts"`), and a stage-less `tts_output` is held for the degraded paths
only, with a warning. The mock in `src/ha.rs` speaks the spec-derived shape
(`{"type":"stage","data":{"stage":"tts",…}}`) — which is precisely why nothing else could see
this: a corpus that only speaks your mock's dialect tests your dialect.

### AC3 — KWS realtime factor — PENDING
Gate: ≥ 20× realtime (`BENCH_MIN_REALTIME_FACTOR`), i.e. a 1 h wav decoded in under 3 min
single-stream. Command:

```sh
export SHERPA_ONNX_ARCHIVE_DIR="$HOME/.cache/sherpa-onnx-archive"
cargo run --release -- bench-kws --model <pinned 3.3 M bundle dir> --wav <1 h wav>
```

**Preliminary only — NOT a pass:** one run on a **debug build** (no release profile), a
**single clip**, reported as 16.7 s of audio decoded in 0.28 s wall ≈ **60× realtime**,
`passes_ac3: true`. Preliminary, debug build, single clip: it exercises the code path and
shows headroom, and it proves nothing about the gate. Unknown and still required before
this entry moves: release-profile numbers, the 20-clip battery, per-clip variance, the
`realtime_factor`, `detections` and `passes_ac3` fields for the battery, the `streams`
count each run used (`BenchReport.streams` is 1 for the AC3 gate and 8 for the S0 ×8
budget check — a ×8 number is not an AC3 number), and the model directory actually used
(see the open note above). Do not quote 60× without this paragraph attached.

### AC4 — Fixture battery — PENDING
≥ 10 clips recorded on a real satellite (wake at start/middle/end, one-breath command,
trailing "…, luna", negatives synthetic where possible), each through `listenrr replay
<wav> --config …`, asserting transcript correctness and an endpoint within ±400 ms of
speech end, hand-checked once against the waveform. Closing evidence: a per-clip table of
transcript match + utterance-end margin, the `trailing_silence_ms` value it was measured
at, and the AC4 headline (≥ 9/10 positives, 0 HA calls on negatives).

### AC5 — Idle-house p50 wake→playback_start — PENDING
Record the five stage latencies (wake→endpoint→transcript→tts-end→playback_start) and the
idle-house p50 target ≤ 2.5 s. Target is tune-tracked, not a hard gate — record the
measured distribution, never a single lucky run.

**Instrument gap found while measuring this (2026-11-02).** The false-accept sidecar's
`stages` block is always `null`, including for a turn whose latencies were measured and
logged moments earlier: the journal carries `WakeToEndpoint 2036 ms` and
`EndpointToTranscript 139 ms` for the same conversation while the sidecar for it holds four
nulls. Checked against a second sidecar written before today's changes, so this predates them
rather than regressing with them. The consequence is AC5-shaped: rejected turns are the entire
tuning corpus, and they carry no latency record, which is exactly the set most likely to be
slow. Filling it is small — `write_false_accept_clip` has the turn, and the turn has its
`StageLatencies` — so it is recorded here rather than only in a commit message.

### AC8 — Restart resilience / scripted-fixture ground truth — PENDING
Backend restart → all sources reconnected ≤ 60 s with no reflash or config change; plus the
scripted-fixture `kw_span` ground-truth requirement recorded under U3. Closing evidence: the
reconnect timing and the fixture carrying an explicit expected span.

---

### AC2 — device half — PENDING

The socket half is **measured**, and it needed no satellites: `cargo test --test listener` → 18
passed, covering connect and admission and auth in both directions, three concurrent sources with
per-source attribution, one departure producing exactly one `IngestEvent::SourceGone`, a frame
**or an event** arriving on a replaced connection being refused, and the held frame counted on
every exit path. Case 17 is the one that found the defect it guards: a superseded peer that sends
only its 30 s heartbeat used to overwrite the live source's signal reading, could be attributed
any uplink event, and was never reaped — because every inbound byte resets the read deadline.
Shown red by deleting the gate (the peer hangs; a hang is not a close), then green with it.
Case 18 is its sibling and closes the hole §12 had promised closed without the code covering it:
a superseded peer that sends **two bytes** of a 2052-byte audio frame — bytes that can never
complete a message — is now closed on the first read rather than kept alive forever by resetting
the deadline, and the live source is asserted to keep streaming afterwards.
What still closes this entry is the literal AC2 text — 2–4 real satellites on the LAN — which needs
RF, I2S and a household. No number, no pass.

### AC8 — backoff budget vs the ≤ 30 s / ≤ 60 s targets — PENDING

A prediction from constants, explicitly **not** a measurement. The firmware used to reset the
attempt counter on a successful TCP connect, before the hello; it now resets only after a
connection has survived 5 s (`reconnect_settled()`), so a source that is accepted and then
refused climbs the ladder instead of retrying every second. For the backend-down case — where
`connect()` itself fails and the ladder advances either way — the cumulative waits are
**1, 3, 7, 15, 31, 61 s**. That exceeds AC8's single-satellite ≤ 30 s at the **fifth** retry and the
fleet ≤ 60 s at the **sixth**, so a restart long enough to reach the sixth retry cannot be met with
the caps as configured. Closing evidence: a backend-restart soak recording per-source
time-to-reconnect. If it confirms, the fix is the cap, not the gate.

---

## AC3 / AC11 on the deployment host — measured 2026-11-02

First time any of this ran on the hardware that is meant to run it permanently: the Intel N100
test host (`pve.example.lan`, Proxmox VE 8.3.1, 4 cores / 15 GiB) as **LXC CT `<CTID>`**, Debian 12, unprivileged,
`--cores 1 --cpuunits 2000 --memory 2048 --swap 512`, `192.168.1.30`. Full provisioning
record in [`../deployment.md`](../deployment.md). Release build from the tree as
committed (`rust-toolchain.toml` pinned 1.99.0), 3 m 20 s on that one core.

**AC3 — KWS speed.** Command, run inside the container against the pinned bundle:

```sh
cd /opt/listenrr/models/kws/sherpa-onnx-kws-zipformer-gigaspeech-3.3M-2024-01-01
/opt/listenrr/listenrr bench-kws --model . --wav test_wavs/0.wav
/opt/listenrr/listenrr bench-kws --model . --wav test_wavs/1.wav
```

Observed, verbatim JSON fields:

| clip | audio_seconds | wall_seconds | realtime_factor | passes_ac3 |
|---|---|---|---|---|
| `test_wavs/0.wav` | 6.625 | 0.205 | **32.296** | true |
| `test_wavs/1.wav` | 16.715 | 0.525 | **31.822** | true |

What this does **not** establish:

- `streams: 1`. The plan's S0/AC3 gate is KWS ×8 concurrent streams staying well under one
  core. At 32× a single stream consumes ~3 % of a core, so eight would sit near a quarter —
  but that arithmetic is mine, not a measurement, and it is filed here as the prediction it
  is. Re-run with eight streams before anyone calls AC3 passed.
- The audio is `test_wavs/0.wav` and `1.wav` from inside the model bundle — Gigaspeech
  sample speech, not household audio, not "luna", not 16 kHz-mic-from-an-ESP32 conditions.
- `detections: 0` in both runs is expected and is not a failure of the detector: `bench-kws`
  uses the bundle's own `keywords.txt` (its sample list), which does not contain the phrases
  in those clips. The service, configured with `test_keywords.txt`, did detect one — see
  below. Do not read `detections: 0` as a regression signal.
- The earlier debug-build figure that appeared in this file is superseded by these two
  numbers, which come from a release build on the real target. Quote either only with this
  paragraph.

**AC11 — `save_buffer` → `clips/missed/`.** A protocol client on the GPU box (`hello` line, then
`0x01` + seq `u32 LE` + 1024 `i16 LE` records, then `0x02` + `u16 LE` + event JSON) sent a
`rssi` event and a `save_buffer` event from the source `probe` (room/group `utility`,
`has_speaker=false`), authenticated with `server.auth_token`. Service log:

```
source registered        source=probe room=utility group=Some("utility") caps=… false
missed-wake clip written clip=/var/lib/listenrr/clips/missed/…-probe-missed.wav
```

The file was **10.0 s** of 16 kHz mono 16-bit PCM, owned `listenrr:listenrr` mode 0640, and
its samples were the bytes the client had sent. Two things worth recording so nobody
re-litigates them: the clip correctly has **no `.wav.json` sidecar** (sidecars belong to
`write_trigger_clip`, which has a transcript and a verify verdict to record; a missed wake
has neither, and the retention sweep tolerates its absence), and the 10 s length is the
configured ring-buffer depth, not a property of what the client happened to send.

**Wake path, for completeness** (not an AC on its own, but it is the first real evidence for
the KWS→capture seam on target). Feeding `0.wav` — whose transcript contains "…THE YELLOW
LAMPS WOULD **LIGHT UP** HERE AND THERE…" — against a `keywords_file` containing "light up":

```
wake accepted: capturing  source=probe conversation_id=listenrr-1 kw_span=53760..59392
stage latency             stage=WakeToEndpoint conversation_id=listenrr-1 ms=5936
transcription failed      connecting to the Parakeet Wyoming service at 127.0.0.1:10300
```

`kw_span` starts at sample 53760 = **3.36 s**, which is where "light up" actually falls in
that clip — the detector located the keyword in real speech at the right offset, and the
endpointer ran and measured itself. `WakeToEndpoint 5936 ms` is **not** an AC5 number: AC5 is
p50 wake→playback_start in an idle house with satellites and TTS, none of which were present,
and this clip is long-form speech. The stop at `127.0.0.1:10300` was the honest outcome **at
that hour**: no Wyoming ASR service existed in this household (ports 10200–10500 were closed
on the test host and on the HA VM), and that is what kept U1 and the S0 Parakeet RTF gate PENDING
rather than deferred.

Later the same day that stopped being true. Two tiers were stood up — a GPU endpoint at
`192.168.1.40:10300` and a CPU fallback at `192.168.1.121:10300` — and this failure line is
now a transcript: the same client path answers in 94–143 ms on the GPU tier. U1 is closed
above on captured bytes, the S0 RTF half is measured (`docs/deployment.md`, "The ASR"), and
what remains open here is only what the paragraph above already excluded: AC5, which needs a
microphone and a speaker.

---

## Config tuning log

The rule this table exists to enforce: a failing check is never relaxed by editing the check —
the value moves in config, and this table records what forced it.

| Date | Key | From → To | Failing evidence | Re-measured result |
|---|---|---|---|---|
| 2026-11-02 | `kws.keywords_file` | ad-hoc file on the target → the two-line measured set, version-controlled at [`../../config/keywords.luna.txt`](../../config/keywords.luna.txt) (sha256 `db41b81b…`, byte-identical to `/opt/listenrr/keywords.txt`) | The obvious spelling `▁ HE Y ▁ LU N A` fired on **0 of 10** "hey luna" clips while looking entirely correct, and isolated "luna" clips fired on **0 of 3** — silent misses, no error anywhere | `▁HE Y ▁ LU N A` fires **10/10** (the model emits `▁HE` as one token); one-word clips fire **4/4** once the clip has 400 ms of leading silence, which is what a person actually produces; **0 false accepts** across 18 near-miss negatives. Matrix: 4 sets × 2 thresholds × 18 clips through `replay --config`, no restarts |

**Deliberately not in that table.** `kws.threshold` stays 0.25 and `kws.boosts` stays empty.
0.25 and 0.10 gave **identical** outcomes in every cell — threshold is not the lever here, and
moving it would be tuning with no observable effect. No boost has been measured against 60 dB of
extractor noise in a room, and an unverifiable boost is worse than none.

Method, matrices and the limits of the instrument (clean TTS, no room, no microphone — proof the
file is well-formed and a way to rank sets, not an AC5 acceptance test):
`docs/deployment.md`, "The wake words".
