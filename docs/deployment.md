# Deploying listenrr

This guide takes listenrr from nothing to a running voice backend on a small Debian host. It
was written from a real deployment into a Proxmox LXC container, and every number in it was
measured on real hardware. Where something is an inference rather than a measurement, it says
so.

What you are building:

```
satellites (ESP32 / tablets / any client speaking docs/protocol.md)
      |  audio over TCP :10700, authenticated with a shared hello token
      v
listenrr  (this repo: wake-word spotting, capture, endpointing, verify gate)
      |                                   |
      | Wyoming TCP                       | WebSocket + REST
      v                                   v
ASR (Parakeet behind a Wyoming server)    Home Assistant (Assist pipeline, TTS)
```

Two facts shape everything below:

- **Satellites never contact the ASR.** They stream to listenrr, listenrr transcribes. The only
  network path that must exist for transcription is listenrr to the ASR.
- **The ASR is the expensive part.** The backend itself is small (1 core and 2 GiB were enough);
  the speech recognizer is not. Read [The ASR: where it runs, and what it costs](#the-asr-where-it-runs-and-what-it-costs)
  before choosing hardware.

## What you need

| Component | Notes |
|---|---|
| A Debian 12 host | Container or VM. 1 core and 2 GiB (plus 512 MiB swap) was the ceiling used and the service used far less. 16 GB of disk leaves room for the build tree and clips |
| Rust toolchain | `rust-toolchain.toml` pins the version; `rustup` fetches it |
| sherpa-onnx static library archive | The build links it statically. Fetch it once and point `SHERPA_ONNX_ARCHIVE_DIR` at it (see below) |
| An ASR with a Wyoming interface | Parakeet via [`lmoe/wyoming-onnx-stt`](https://hub.docker.com/r/lmoe/wyoming-onnx-stt) was the one measured. See the ASR section |
| Home Assistant | With a long-lived access token and an Assist pipeline |
| Satellites | See `firmware/`, `tablet/` and `docs/protocol.md`. Not covered here |

The design notes behind the wire format and the build commands are in
[`docs/design/CONTRACT-NOTES.md`](design/CONTRACT-NOTES.md). Measured acceptance results are in
[`docs/design/TEST-NOTES.md`](design/TEST-NOTES.md).

## Provision the host

On Proxmox VE 8, the container used here was created like this (run on the Proxmox host; replace
`<CTID>` with a free id and adjust storage names):

```sh
pct create <CTID> local:vztmpl/debian-12-standard_12.7-1_amd64.tar.zst \
  --hostname listenrr --unprivileged 1 \
  --cores 1 --cpuunits 2000 --memory 2048 --swap 512 \
  --rootfs local-lvm:16 \
  --net0 name=eth0,bridge=vmbr0,ip=dhcp,type=veth \
  --onboot 1 --storage local \
  --description "listenrr voice backend"
pct start <CTID>
```

Notes that each cost a round trip:

- `--rootfs` takes `storage:GB` (`local-lvm:16`), not `local-lvm,size=16G`. The latter fails
  with `unable to parse volume ID`.
- `pct push` accepts no `--permission` or `--ownership` option in PVE 8.3. Set modes afterwards
  with `pct exec <CTID> -- chmod ...`.
- The container was capped (1 core, `cpuunits=2000`, 2 GiB) rather than sized, because the host
  was already over-committed. The 2 GiB is a ceiling, not a measured need.
- **Pin the address.** Use a router reservation or a static `--net0 ...,ip=<addr>/<mask>,gw=<gw>`.
  Satellites are configured with a `backend_host`, and a renewed DHCP lease silently moves it.
  Everything below assumes the backend is reachable as `listenrr.example.lan`
  (`192.168.1.30`).
- Measure before you promise. `nproc; free -h; pct list; qm list` on the Proxmox host tells you
  whether there is room. Containers that need transcoding or other heavy work on the same host
  should be checked for CPU contention first.

A plain VM or bare-metal Debian 12 install works the same way from the next section on.
Satellites are ESP32 devices: they cannot join a VPN or overlay network and stay on the LAN, so
the shared hello token (`server.auth_token`, compared in constant time) is the trust control on
the satellite path. A VPN for your own operator access does not replace it.

## Build inside the target

Build on the machine that will run the binary. Do not build elsewhere and copy the binary over,
even when both are `x86_64-unknown-linux-gnu`.

```sh
# inside the container, as root
apt-get install -y --no-install-recommends build-essential pkg-config ca-certificates curl xz-utils
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --no-modify-path

mkdir -p /opt/listenrr/src
# get the source into /opt/listenrr/src: git clone, or tar + scp + pct push
cd /opt/listenrr/src

export PATH=/root/.cargo/bin:/usr/bin:/bin
export SHERPA_ONNX_ARCHIVE_DIR=/root/sherpa-cache   # directory holding the sherpa-onnx static-lib .tar.bz2
export CARGO_TARGET_DIR=/root/ctarget
cargo build --release                               # rust-toolchain.toml pins the version
cp -a /opt/listenrr/listenrr /opt/listenrr/listenrr.prev 2>/dev/null || true   # keep the last good binary
install -m 0755 /root/ctarget/release/listenrr /opt/listenrr/listenrr
```

Measured: `Finished release profile in 3m 20s` on one Intel N100 core, a 41 MB binary. The static
sherpa-onnx archive links cleanly against Debian 12 with no glibc mismatch to work around.

Practical points:

- If the container cannot reach your git server (for example, SSH is only open on the machine
  hosting it), ship a tarball excluding `.git`, `target`, `.esphome`, `config/secrets.yaml` and
  `__pycache__`, and place the sherpa-onnx archive in `SHERPA_ONNX_ARCHIVE_DIR` by hand.
- If you script the build, use `set -eux`, not `set -ux`. Without `-e`, a failed stage still
  prints `ok` and the script ends "ALL DONE" having built nothing.
- sherpa-onnx ships a glibc static archive, so a musl target is not an option.

### Why not build on another machine

A binary built on NixOS will not start on Debian. It looks like it should, and then systemd
reports:

```
ExecStartPre=... health ... (code=exited, status=203/EXEC)
Failed at step EXEC spawning /opt/listenrr/listenrr: No such file or directory
```

`203/EXEC` with the file plainly present means the ELF interpreter is missing:

```sh
readelf -l listenrr | sed -n 's/.*\[Requesting program interpreter: \(.*\)\]/\1/p'
# /nix/store/<hash>-glibc-<version>/lib/ld-linux-x86-64.so.2
```

A NixOS-built binary names a loader path that does not exist on Debian, so `execve` fails with
`ENOENT` for the interpreter and systemd reports it as if the binary were gone. Building inside
the target is the fix. Cross-building is possible in principle, but this was not tested.

## Models

```sh
cd /opt/listenrr/src && bash scripts/get_models.sh --dest /opt/listenrr/models
```

This fetches the pinned KWS bundle (`sherpa-onnx-kws-zipformer-gigaspeech-3.3M-2024-01-01`) and
`vad/silero_vad.onnx`, records digests under `models/.sha256/`, and prints the `[kws]` and
`[capture]` snippet to paste into your config. The bundle also contains `test_wavs/` and
`test_wavs/test_keywords.txt`, which are useful for the first smoke test because they are real
speech with a keyword list known to match.

**Your own wake word is still your job.** `keywords_file` has to be produced with sherpa-onnx's
`text2token` tool against the bundle's `tokens.txt`. A wrong token sequence does not fail
loudly; it simply never wakes. See [The wake words](#the-wake-words).

## Configuration and secrets

Create the service account, then the config:

```sh
useradd --system --no-create-home --shell /usr/sbin/nologin listenrr
install -d -m 0755 /etc/listenrr
install -m 0640 -o root -g listenrr /opt/listenrr/src/config/listenrr.toml.example /etc/listenrr/listenrr.toml
```

`/etc/listenrr/listenrr.toml` carries the Home Assistant credential, so keep it `root:listenrr
0640`. [`config/listenrr.toml.example`](../config/listenrr.toml.example) documents every key,
marks the REQUIRED ones and states every default. The values that matter:

| Key | Example | Why |
|---|---|---|
| `server.listen` | `0.0.0.0:10700` | Satellites are on the LAN |
| `server.auth_token` | output of `openssl rand -hex 16` | The trust boundary. Every satellite gets the same value. **Never leave the example placeholder in place.** A placeholder token is not a trust control, and nothing fails to tell you. After setting it, probe both directions: the old public value must be refused, the new one admitted |
| `ha.url` | `ws://homeassistant.example.lan:8123` | The bare origin is correct: listenrr appends `/api/websocket` itself. Home Assistant is often a separate VM and is not necessarily the voice host |
| `[ha] internal_url` | `http://192.168.1.20:8123` | The address satellites fetch `/api/tts_proxy/...` from. Must be reachable by the devices |
| `ha.token`, `ha.pipeline_id` | from Home Assistant | See [Home Assistant](#home-assistant-credentials-and-the-pipeline) |
| `parakeet.addr` | `asr.example.lan:10300` (`192.168.1.40`) | The preferred ASR tier, the GPU box |
| `parakeet.fallback_addrs` | `["asr2.example.lan:10300"]` (`192.168.1.41`) | The CPU box, tried only when the preferred tier fails. Omit it and you have one tier: a busy GPU becomes a failed turn instead of a slow one |
| `clips.dir` | `/var/lib/listenrr/clips` | Must match `StateDirectory=` in the unit |

`listenrr health` parses the config, validates it (all problems at once) and creates the clip
tree. Run it as the service user so ownership comes out right:

```sh
runuser -u listenrr -- /opt/listenrr/listenrr health --config /etc/listenrr/listenrr.toml
# ok: config valid, 1 source(s), 0 group(s), listening on 0.0.0.0:10700
```

## Install the systemd unit

```sh
install -m 0644 deploy/listenrr.service /etc/systemd/system/listenrr.service
systemd-analyze verify /etc/systemd/system/listenrr.service   # exit 0, no diagnostics
systemctl daemon-reload && systemctl enable --now listenrr
```

Two directives in [`deploy/listenrr.service`](../deploy/listenrr.service) are load-bearing:

- The `[Install]` section with `WantedBy=multi-user.target`. Without it `systemctl enable`
  refuses the unit ("no installation config") and a reboot leaves voice down until someone
  starts it by hand.
- `KillSignal=SIGINT`. The graceful drain in `run_service` is wired to Ctrl-C, not SIGTERM. With
  systemd's default the process would exit through the default disposition and never enter the
  arm that lets in-flight sources finish.

`ProtectSystem=strict` is safe because the only writes are under `StateDirectory=`. If the
service ever dies while loading the KWS model, that is the first directive to test removing,
since the vendored ONNX Runtime is outside this repository.

## Verify on the host

From another machine, use any client that speaks the protocol in
[`docs/protocol.md`](protocol.md): a `hello` line, then `0x01` audio frames (sequence `u32` LE
followed by 1024 `i16` LE samples), then `0x02` event frames (`u16` LE length plus event JSON).
Stream one of the bundle's `test_wavs/*.wav` clips as the source, with a keyword list that is
known to match it.

A working backend logs lines like these:

```
source registered            source=probe room=kitchen group=Some("kitchen") caps=... false
source stream health         frames=... seq_gaps=0 jitter_p50_ms=... kws_drops=0 rssi_dbm=...
wake accepted: capturing     source=probe conversation_id=listenrr-1 kw_span=53760..59392
stage latency                stage=WakeToEndpoint conversation_id=listenrr-1 ms=5936
missed-wake clip written     clip=/var/lib/listenrr/clips/missed/...-probe-missed.wav
```

`kw_span` is in samples at 16 kHz: `53760..59392` is 3.36 s to 3.71 s into the clip, which is where
the keyword was in the real speech. If no ASR is reachable yet, the expected stop one stage later
is `transcription failed: connecting to the Parakeet Wyoming service at <addr>`.

Missed-wake clips (`save_buffer` events) are written without a `.wav.json` sidecar. Sidecars
carry a transcript and a verify result, which a missed wake does not have, so their absence is
not a fault and the retention sweep tolerates it.

Useful for tuning: `listenrr replay <clip-or-dir> --config <toml>` runs the real spotter over
16 kHz mono WAV files and prints a JSON report per clip (the `woke` field is the verdict). It
takes `--config`, so changing keywords or thresholds needs no service restart. `listenrr
bench-kws --model <dir> --wav <file>` measures spotting speed.

### Readiness: a port that answers is not a service

When the ASR container was first started it crash-looped 34 times because it could not download
its model, and yet a TCP probe to its published port succeeded:

```sh
(exec 3<>/dev/tcp/asr.example.lan/10300) && echo READY   # said READY while the app was dying
```

Docker's userland proxy accepts the connection whether or not anything is behind it, so that
probe measured the port map, not the service. Ask for readiness in the protocol the service
speaks: one real transcription. Use two instruments and run them in that order:

1. A contract-level check that drives the Wyoming vendor library's own client against the ASR.
   A pass means the service works.
2. A client-level check that drives the framing listenrr's own Rust client uses. A pass means
   listenrr works.

They disagree whenever listenrr's framing has drifted from the protocol, and that disagreement is
the measurement. It is how two real defects were found (see
[Two wire-format defects](#two-wire-format-defects-no-unit-test-could-see)).

Two related facts:

- Containers on some hosts start with no usable resolver (for example when the host's
  `resolv.conf` points at a VPN-only address). Pass `--dns 1.1.1.1` (or your resolver), or the
  model never downloads and the container crash-loops.
- A connection from the satellite network to a published port that returns `REFUSED` in 0 s means
  nothing is listening; a timeout means something is blocking you. Reading that wrong sends you
  editing firewall rules for no reason.

Gating a container restart on `docker logs <name> | grep "Server ready"` is not a readiness
check: the log still holds the previous start's line, so the gate returns at once. Use
`docker logs --since <window>`, or better, the protocol-level probe above.

## The ASR: where it runs, and what it costs

A Parakeet Wyoming ASR cannot live on a small shared host. The deployment host here was an Intel
N100 (4 weak cores) already running other guests, with swap in use, so it was never a candidate.
The placement below is what was measured, in the order it should be tried. Hardware is described
by class; your numbers will differ with core count and load.

| Tier | Endpoint | Measured RTF | Verdict |
|---|---|---|---|
| 1 | A consumer NVIDIA GPU (RTX 5090 class) on another box, shared with other workloads | **0.058** warm, 0.086 on the first request after a reboot (with warm-up) | The only tier that met an RTF target of under 0.1. Used 1.5 GB of 13 GB free VRAM |
| 2 | 8 CPU cores of a many-core server | 0.48 to 0.66 (0.67 at load average 35) | The fallback. About 5x to 8x off the target, but clears the client timeout on short utterances |
| none | The 4-core N100 itself | not run | Weak cores, no spare capacity. The image used is CUDA-only, so an iGPU would need an OpenVINO build |

RTF (real-time factor) is processing time divided by audio duration, as logged by the server for
a 6.625 s clip. Lower is faster; 1.0 is real time.

### What happens when the GPU is busy

A GPU shared with other jobs is sometimes busy, so "the preferred tier is unavailable" is the
normal case, not the exception. `src/parakeet.rs` tries `parakeet.addr` first, then
`parakeet.fallback_addrs` in the order written:

- **A refused connection costs nothing.** A stopped container is not a wait; the next tier is
  dialed immediately.
- **A tier that accepts the audio and says nothing is abandoned at 2.5 s**
  (`PROBE_TIMEOUT_MS`). That number is measured: a healthy preferred tier answers a 6.6 s clip in
  0.4 s and the worst legitimate first request after a reboot was 0.57 s, so 2.5 s is about 4x the
  slowest healthy behaviour while staying far under the 10 s a caller waits. It exists for the
  failure a caller cannot otherwise tell from a dead service: an ASR that takes the audio and
  never answers.
- **The last tier that is not in cooldown is exempt from that budget** and gets the full 10 s. A
  CPU tier taking seconds on a long utterance is doing real work, and abandoning the only
  working tier because it is slow would turn a slow night into a failed one. A tier sitting in
  cooldown is still a candidate behind the working one, so "last in the list" is not the same as
  "the attempt that has to work". Confusing the two was a real bug (see below).
- **A tier that just failed is left alone for 60 s** (`ENDPOINT_COOLDOWN`). Without this, a busy
  GPU costs 2.5 s of dead air on every command; with it, that cost is paid once a minute. Cooled
  tiers are demoted to last, never skipped, so after a total outage the service recovers by
  itself without a restart.

When a turn is served by anything other than the preferred tier, the log says so:
`transcribed by a fallback tier`, with `asr`, `preferred` and `tried` fields. If commands feel
slow, that is the line to look for. A total outage produces one error naming every endpoint tried.

Tests in `src/parakeet.rs` pin this behaviour, including that a recovered tier is preferred
again, and that tier health is shared (not copied) across the clones each transcription task
gets.

### Deployment proof, and the two bugs only running it found

The rules above were exercised against the real service by stopping the GPU container under live
turns. Each row is a log line, not an inference:

| Situation | Result |
|---|---|
| Both tiers up | Preferred tier served, `EndpointToTranscript` **232 ms** |
| Preferred tier stopped | Refused instantly, next tier served, no added wait |
| Dead tier, second turn within 60 s | Served, and the dead tier was not contacted at all |
| Preferred tier back, cooldown over | Served again in **195 ms / 237 ms**, no warning. Preference returns by itself |
| Working tier behind a cooled one | Served in **4379 ms** (3943 ms after the fix), over the 2.5 s probe budget, which is the point |

1. **A cooled tier demoted the working tier to a probe.** With the GPU in cooldown the CPU tier
   was tried first, but the cooled GPU still sat behind it in the list, so the CPU tier was no
   longer "the last candidate". It was abandoned at 2.5 s mid-transcription (`no transcript
   within 2500 ms`) from the only ASR that was working, and the turn failed. The exempt attempt
   is now the last tier that is not in cooldown.
2. **The degraded-state log went silent exactly when needed.** The warning was keyed on `rank >
   0`, and a cooled preferred tier puts the fallback at rank 0, so the CPU tier served 4.4 s and
   2.8 s turns with nothing in the log saying the house was on the fallback. It is now keyed on
   whether the preferred tier answered, and carries `rank` separately.

**The CPU tier has no headroom.** On a loaded many-core server (load average 17 to 35) a roughly
7 s utterance took 9752 ms and 9992 ms through the real path, both within 250 ms of the client's
10 s budget. A slightly longer command or a busier box turns the fallback into a failed turn.
Treat tier 2 as a last resort that keeps the house talking. If the GPU is frequently busy, the
answer is a second GPU or a smaller streaming model (the sherpa-onnx route, which runs real-time
on one N100 core), not a longer timeout.

### Run cards

The two ASR hosts are placeholders used throughout: `asr.example.lan` is `192.168.1.40` (the GPU
box, tier 1) and `asr2.example.lan` is `192.168.1.41` (the CPU box, tier 2).

GPU tier (Docker with the NVIDIA container runtime):

```sh
docker run -d --name listenrr-asr --restart unless-stopped \
  --gpus all --cpus 8 --memory 8g \
  -p 192.168.1.40:10300:10300 \
  --dns 1.1.1.1 \
  -v "$HOME/wyoming-asr-data:/data" \
  -e STT_MODEL=nemo-parakeet-tdt-0.6b-v3 -e STT_DEVICE=cuda \
  -e STT_QUANTIZATION=int8 -e STT_LANGUAGES=en \
  -e STT_VAD_ENABLED=false -e STT_LOG_LEVEL=INFO -e STT_WARMUP=true \
  lmoe/wyoming-onnx-stt@sha256:1797ba20d198b048f0f21e2f1644b0cafd7ece7d33aa4339beed72943d4c0056
```

CPU fallback: the same image and digest with `-e STT_DEVICE=cpu`, no `--gpus`, `--cpus 8
--memory 6g`, no `STT_WARMUP`, published on `192.168.1.41`, and its own bind mount. The only
difference between tier 1 and tier 2 is `STT_DEVICE`.

What each choice is for, each learned by getting it wrong first:

- **Pin by digest, not `:latest`.** `:latest` is a silent upgrade of a component whose wire format
  you do not control. The digest above is the one every number in this guide was measured against.
- **`--restart unless-stopped`.** Docker's default is `no`, so a reboot or a crash left voice
  silently dead.
- **`STT_WARMUP=true` is not cosmetic.** Without it the first transcription after a start took
  **32.4 s** (RTF 4.9) while CUDA built its graphs, which would time out against the client's 10 s
  budget and look like a dead ASR. With it, `Warmup complete` is logged before `Server ready` and
  the first real request took 0.57 s.
- **Bind-mount `/data`.** The model is cached there, so a cold CPU start reaches `Server ready on
  tcp://0.0.0.0:10300` in 7 s (about 18 s on GPU including warm-up, image pull excluded).
- **`--dns`.** See Readiness above.

Two notes on the GPU tier. onnxruntime logs `742 Memcpy nodes are added to the graph` for the
CUDA execution provider: a warning about headroom, not a failure. And accuracy improved on GPU:
the same int8 model that returned "brothers" for the word "brothels" on CPU got it right here, so
that error was a property of the CPU execution path, not the quantised model.

### Two wire-format defects no unit test could see

Both were found by talking to a real server. Neither is visible to a test that asserts an
encoder against its own decoder, which is exactly the kind of test that let both ship.

| listenrr assumed | Wyoming actually does |
|---|---|
| 4-byte big-endian header length, `data` inline in the header | A **JSON line terminated by `\n`**, then a separate `data` JSON blob of `data_length` bytes, then a payload of `payload_length` bytes. The first write was never newline-terminated, so the server's `readline()` blocked forever and returned no error: the client just timed out |
| `width: 16` (bits) | `width` is **bytes per sample**, so `2`. The server died in `audioop.lin2lin` with `Size should be 1, 2, 3 or 4` |

The second was less a protocol mistake than a constant name that invited it, which is why the
Rust side now calls it `SAMPLE_WIDTH_BYTES`. Canonical request bytes, captured from the vendor
library and asserted byte-for-byte in `src/parakeet.rs`:

```
transcribe  : {"type": "transcribe", "version": "1.8.0", "data_length": 18}\n{"language": "en"}
audio-start : {"type": "audio-start", "version": "1.8.0", "data_length": 61}\n{"rate": 16000, "width": 2, "channels": 1, "timestamp": null}
audio-chunk : {"type": "audio-chunk", "version": "1.8.0", "data_length": 61, "payload_length": 2048}\n{...same...}\n<2048 PCM bytes>
audio-stop  : {"type": "audio-stop", "version": "1.8.0", "data_length": 19}\n{"timestamp": null}
```

### Throughput on CPU

The same 6.625 s clip every time; `rtf` is the server's own measurement.

| Cores | Server-measured RTF | A 15 s utterance would take |
|---|---|---|
| 3 | 1.19 to 1.66 | 18 to 25 s |
| 8 | 0.48 to 0.66 | 7 to 10 s |
| 12 | 0.56 | 8 s |
| 24 | 0.35 | 5 s |

It is thread-limited and sublinear: 8x the cores (3 to 24) bought about 4x the speed, and 4x the
cores (3 to 12) only 2.1x, so reaching an RTF of 0.1 is not a matter of being generous with
cores. The first request after a start is also markedly slower than steady state (0.98 against
0.48 at 8 cores) because onnxruntime optimises the graph on first use, so a warm-up call or a
patient timeout is not optional.

What that means in practice:

- **Fine on CPU:** offline work such as fixture batteries, verify-gate tuning and replay.
- **Fine on CPU as a fallback:** short utterances at 8 cores clear the 10 s client timeout.
- **Not fine on CPU:** a live conversation. A typical interactive latency target is not reachable
  this way, and a long utterance does not clear the timeout at all.

Accuracy on the one clip tested was good, with a caveat. On the full 6.6 s clip the transcript was
word-for-word right except "brothels" came out as "brothers" under int8 on CPU. On the
VAD-trimmed utterance from the same audio the error did not occur, so the failure tracked the
window handed to the model, not the sentence: trimming helped accuracy as well as latency. "light
up" was right in both. This is one clip of one public test set, not household speech.

## Home Assistant: credentials and the pipeline

`[ha]` needs a long-lived access token and an Assist pipeline id. The secret lives only in
`/etc/listenrr/listenrr.toml` (`root:listenrr 0640`), never in git.

**Finding the pipeline id.** Current Home Assistant serves the Assist pipeline registry over
WebSocket only: `/api/assist/pipeline/` answers 404 while `/api/` and `/api/states` answer 200.
Read it with the `assist_pipeline/pipeline/list` WebSocket command, or from Settings, Voice
assistants. With one pipeline in the house there is nothing to choose. With several, ask which
conversation agent should own the voice path instead of taking the default.

**What listenrr does.** It starts a run at the intent stage (`start_stage: "intent"`,
`end_stage: "tts"`, `input.text`), because transcription is listenrr's own and Home Assistant's STT
should not be paid for. A working run streams `run-start`, `intent-start`, `intent-end`,
`tts-start`, `tts-end`, `run-end`; the `/api/tts_proxy/...` URL in `tts-end` returns audio bytes.

**Re-checking it without listenrr.** Write a small dependency-free WebSocket script (or use
`websocat`) and test four things:

1. Authenticate with the token: expect `auth_ok` and the HA version.
2. Authenticate with a mangled token: expect `auth_invalid` ("Invalid access token"). This
   negative control is what makes the first check mean something.
3. List pipelines with `assist_pipeline/pipeline/list`.
4. Run a pipeline with text input and read stage events to `run-end`, then fetch the TTS URL.

### Gotchas, so the socket is not re-debugged next time

- `/api` is the REST root; the socket is `/api/websocket`. A 404 on the handshake is this, not a
  bad token.
- Home Assistant sends `auth_required` before it judges the token. Reading that as the verdict
  makes every token fail. Only `auth_ok` passes; `auth_invalid`, or a socket closed without one,
  is the rejection.
- For `assist_pipeline/run`, the `result` frame arrives before the stage events. Reading to
  `run-end` is what proves a pipeline ran; `result: success` only proves the command was
  accepted.
- Field names are `input.text` and `pipeline`; the media is at `data.tts_output.url` and
  `data.tts_output.mime_type`. A reader that guesses wrong fails silently: it reports a completed
  run with no TTS, which looks like a Home Assistant fault.
- Home Assistant 2026.9.2 puts a `tts_output` inside `run-start`. A reader that decides "this event
  is the TTS stage" by the presence of `tts_output` stops at the first event of every run, never
  reads `intent-end`, and never logs the agent's answer. listenrr now tests whether an event names
  the stage (`type: "tts-end"`, or the older `data.stage: "tts"`). A mock that speaks only the
  spec-derived shape cannot catch this; `tests/fixtures/ha-run-2026-11-02.json` holds the real
  bytes.
- `subscribe_events` asks Home Assistant for every state change in the house. With no mute
  entities configured, listenrr does not subscribe at all (`ha: no mute entities configured; not
  subscribing`). Mute entities are per source (`[[sources]] mute_entity = "switch...."`).

### An empty request is not a question

A turn consisting of only the wake word strips to an empty command. Sent to
`assist_pipeline/run` as `input.text: ""`, one conversation agent answered "Sorry, I had a problem
talking to OpenAI" (HTTP 400, no user message). The guard lives in `Coordinator::on_verified`, the
single point every pipeline request passes through: an empty request ends the session like a
rejection (LED cancelled, group back to idle), emits no pipeline run, writes no false-accept clip,
and logs `verified turn asked nothing; not entering the pipeline`. It is not in `verify.rs`,
because the verify gate's contract is "was this addressed to us, and what is the command?", for
which the honest answer to a bare wake word is "yes, and nothing". Whether a bare wake word should
instead be answered (a tone, a "yes?", an open follow-up window) is a product decision this does
not make.

### What a satellite must accept for playback

An Assist satellite entity in Home Assistant advertises which `media_player` features it supports
(`supported_features`). One real device advertised `play_announcement` and volume and mute, but
not `play_media`. Such a device expects the announcement form (`media_content_type:
"announcement"`), which is what Home Assistant's own Assist satellite path uses and which gives
ducking and wake-word suspension. listenrr currently sends the TTS `mime_type` (`audio/mpeg`) and,
when missing, the frozen fallback `"music"`. When you bring a device onto this protocol, read what
it accepts and pin the exact shape with a test, rather than guessing.

## The wake words

The default words are **`luna`** and **`hey luna`**. The keyword file is not spelled by hand. The
spotter matches the exact token sequence the model emits, so a plausible spelling that does not
correspond to emitted tokens simply never fires, silently, with no error.

### Deriving the keyword file

If your sherpa-onnx install has the `sherpa-onnx-cli text2token` tool, use it against the bundle's
`tokens.txt`. The bundle used here ships `tokens.txt` (a 500-piece BPE vocabulary with `▁` as the
word-boundary marker) and no `text2token`, so the candidate segmentations were enumerated from
the vocabulary and each one was measured. There are exactly three legal segmentations of "LUNA"
and three of "HEY". The result is not the one that looks right:

| Keyword line | Outcome |
|---|---|
| `▁ LU N A` | Fires: the bare word |
| `▁HE Y ▁ LU N A` | Fires on every "hey luna" clip |
| `▁ HE Y ▁ LU N A` | **Fires on none of 10 wake clips.** The model emits `▁HE` as one word-initial token. Splitting it into `▁` and `HE` produces a sequence it never emits |

That is the `▁HE` token defect: the intuitive spelling is wrong. The smallest set the data supports
is [`config/keywords.luna.txt`](../config/keywords.luna.txt):

```
▁ LU N A
▁HE Y ▁ LU N A
```

The selection rule that reproduced the measured result is greedy longest-match over the
vocabulary: prefix the word with `▁` and repeatedly take the longest piece matching the start of
what is left. Run on `LUNA` it yields `▁ LU N A`, which is the measured line. Always confirm the
segmentation with `listenrr replay`, because a vocabulary can say what is legal, but only the
model can say what it emits.

### The measurement matrix

18 clips were synthesised from the Assist pipeline's own TTS (`start_stage: "tts"`, so nothing is
spoken aloud): 10 positives (three takes each of "luna" and "hey luna", plus four in a sentence)
and 8 negatives including deliberate near-misses (`lunar`, `banana`, `louise`, `the tuna is
delicious`, `can you turn on the kitchen light`). Four keyword sets, two thresholds (0.25 and
0.10) and 18 clips were run against the real spotter through `replay`.

- **Threshold is not the lever.** 0.25 and 0.10 produced identical outcomes: detections are nowhere
  near the boundary in either direction. Lowering the threshold to chase misses, or raising it to
  chase false accepts, would have been wasted tuning.
- The `▁ HE Y`-only set: 0 of 10. The all-variants set and the minimal two-line set: **7 of 10
  unpadded, and 0 false accepts out of 8 negatives** at either threshold.
- All three misses were the isolated one-word clip (0 of 3), with perfect consistency, which made
  it look like a property of the word rather than the clip.

**Why the bare word "failed", and why it does not.** Those clips began at the first sample of
speech. A `chunk-16, left-64` streaming model needs leading context, and a recording that starts
on the word gives it none. Adding 400 ms of silence before and after, which is what a person
saying a wake word produces, turned all of them into detections: 4 of 4 positives, 0 false
accepts. The keyword file was not the problem; the fixture was. Make test clips with leading
silence.

### What the measurement exposed: the utterance boundary

The padded bare-word clip was detected (`wake accepted`, `kw_span 16640..22528`, 1.04 s to 1.41 s)
while the word itself occupied 0.40 s to 1.28 s of the clip. The ASR was fed the back half of
"luna" plus 300 ms of silence and answered "Okay.". The verify gate then rejected it, which is the
gate doing its job.

The same boundary cut a request in half. A clip of "hey luna, what is the weather" (400 ms leading
pad, 2.72 s of speech, 1.2 s trailing silence) fired at `kw_span 15360..22528` (0.96 s to 1.41 s),
the utterance was handed to the ASR from there, and the transcript was "Uh what is the weather?".
The request half came out correct; only the wake word was missing, because the audio carrying it
was never sent. So it is not the ASR and not the keyword file: it is the boundary. The gate
rejected the turn, kept the clip and the transcript, and Home Assistant was never asked.

The cause is a clamp (`start.max(utterance_start)` in `coordinator.rs`) that discards the ring
buffer's pre-roll on wake turns. It was left in place deliberately, because removing it changes
what every wake turn feeds the ASR (a latency and accuracy change, not a side effect). It is
recorded in a comment at the clamp as an owner decision.

### The false-accept clip is the audio the ASR heard

Two rules once cut one event: the ASR was fed `kw_span.start .. endpoint`, while the false-accept
clip was sliced `kw_span +/- 1 s` from the same ring. The sidecar transcript and the `.wav` beside
it disagreed (the clip was 2.28 s and contained the wake text the transcript was missing). That
mattered because a threshold-tuning workflow depends on an operator listening to the clip to see
what the microphone actually heard. A `Turn` now records `transcribed`, the exact range handed to
the ASR, and `false_accept_window` slices the clip from it. After the fix, the sidecar and an
independent transcription of the archived clip were the same string.

### Stop keywords

A second spotter with its own file runs only while a group is speaking, so the wake word can
interrupt a reply. The shipped [`config/keywords.stop.txt`](../config/keywords.stop.txt) holds
`luna stop` and `hey luna stop`:

```
▁ LU N A ▁ST O P :1.0 #0.5
▁HE Y ▁ LU N A ▁ST O P :1.0 #0.5
```

The vocabulary has no `▁STOP` and no plain `STOP` token: `▁ST`, `▁S` and `▁` exist; plain `ST`,
`TO` and `OP` do not. So "stop" has three legal segmentations, in order of preference: `▁ST O P`
(chosen, and what greedy longest-match yields), then `▁S T O P`, then `▁ S T O P`.

Per-line syntax is `TOKENS :score #threshold`. `:1.0` is a neutral score. `#0.5` is an
unmeasured starting point, deliberately high (twice the global default `kws.threshold` of 0.25),
so that a reply or room audio that merely contains the words does not stop a wanted conversation.
The format has no comment syntax (`#` begins the threshold field), so the caveat lives here.

Install the file where `kws.stop_keywords_file` points (`/etc/listenrr/keywords.stop.txt`, mode
0644, there is no secret in it). **Measure before enabling it:**

1. Record positives: several takes of a person saying "luna stop", each with about 400 ms of
   leading silence. Record negatives: the same voice saying "luna what time is it", and TTS reply
   audio of an ordinary answer.
2. Score them with a scratch copy of the deployed TOML whose `[kws] keywords_file` names the stop
   file: `listenrr replay <clip-or-directory> --config /path/to/stop-gate.toml`, and read each
   report's `woke` field.
3. If positives come back 0 of N, the segmentation is wrong, not the speaker: substitute
   `▁S T O P`, then `▁ S T O P`, and re-run. Raise `#0.5` if negatives fire; lower it only against
   measured misses, never on theory.

### Honesty about this instrument

All of the above is clean synthesised speech played through no room and no microphone. It is
enough to prove a keyword file is well formed, to rank keyword sets, and to show that threshold
tuning is not the lever. It is not an acceptance test: that needs a person saying the word to a
board across a room with the real noise floor.

## Measured numbers

| | |
|---|---|
| Release build, 1 core, N100 | 3 m 20 s |
| KWS speed, `bench-kws`, 6.625 s clip | 0.205 s, **32.3x** real time |
| KWS speed, `bench-kws`, 16.715 s clip | 0.525 s, **31.8x** real time |
| Wake path | Wake detected in real speech at the correct offset; capture and endpointer ran; `WakeToEndpoint` 5936 ms (a long clip, not a latency figure) |
| `save_buffer` | Produced a `...-probe-missed.wav` of exactly 10.0 s, 16 kHz mono 16-bit, owned `listenrr:listenrr` 0640, containing the bytes the client sent |

ASR throughput and live transcription latencies are in
[The ASR: where it runs, and what it costs](#the-asr-where-it-runs-and-what-it-costs) rather than
repeated here.

Limits of this table: `bench-kws` measures one stream, so eight streams fitting in a fraction of
a core is an inference, not a measurement. The audio is the model bundle's own clips, not household
speech. And end-to-end wake to playback latency needs real satellites, Home Assistant and TTS,
none of which were involved.

## What this does not prove

- No satellite hardware on this protocol was exercised in this deployment: no RF, no I2S, no
  speaker, no soak. Every clip here was a file pushed over the ingest protocol.
- Nobody heard the answer. The Assist pipeline runs and produces TTS audio that listenrr can
  fetch, but a probe client has no speaker path (`has_speaker = false`), so playback needs a
  satellite.
- Transcription is proven: wake, capture, endpointing, transcription via listenrr's own Wyoming
  client and framing, and an Assist pipeline run all worked end to end.

## Rollback and retirement

Rolling back is binary plus config. Keep the previous `/opt/listenrr/listenrr.prev` and
`/etc/listenrr/listenrr.toml`, then:

```sh
install -m 0755 /opt/listenrr/listenrr.prev /opt/listenrr/listenrr
systemctl restart listenrr && systemctl is-active listenrr
```

Satellites reconnect on their own backoff and need no reflash. While the backend is down, Home
Assistant stays healthy with voice simply unavailable, which is why `Restart=on-failure` with
`RestartSec=2` is enough.

Retiring the feature means putting satellites back on a stock ESPHome config with
`voice_assistant`, which is a reflash per device.

## Backups

Clips under `/var/lib/listenrr/clips` are household speech, and nothing here backs them up.
Decide explicitly whether they are backed up: 30 days of continuous audio from many sources is
not small, and it is the one dataset that cannot be regenerated. Treat committed test fixtures
that contain real speech the same way.

## Pitfalls checklist

| Symptom | Cause |
|---|---|
| `203/EXEC` with the binary present | Binary built on a different libc (for example NixOS); build in the target |
| `systemctl enable` says "no installation config" | Unit lacks `[Install]` |
| `systemctl stop` does not drain in-flight sources | Missing `KillSignal=SIGINT` |
| Wake word never fires, no error | Keyword tokens not what the model emits; derive them, then `replay` |
| Bare wake word never fires in a test clip | Clip has no leading silence |
| ASR port "ready" but nothing transcribes | Docker proxy accepts connections regardless; probe with a real transcription |
| ASR container crash-loops at start | No resolver in the container; pass `--dns` |
| First command after reboot times out | ASR run without `STT_WARMUP=true` |
| Every satellite refused | `server.auth_token` mismatch (or still the example placeholder everywhere) |
| WebSocket handshake 404 | `ha.url` points at `/api` instead of the origin |
| Satellites cannot play the answer | `[ha] internal_url` is not reachable from the devices |
| Slow turns, no explanation | Check for `transcribed by a fallback tier` in the journal |
