# listenrr — common commands. `just` lists them; `just <recipe>` runs one.
#
# Rules this file encodes (each one cost somebody a bad afternoon):
#   * Heavy builds run under a 32 GiB cgroup cap because a box that also serves inference models
#     must not be swapped out by a build.
#     The cap is enforced, not promised; every capped run prints a CAPPROOF line.
#   * There is deliberately NO repo-wide `cargo fmt` recipe. Format single files you authored
#     (`just fmt-file <path>`), never the tree.
#   * `just clippy` is the one clippy invocation that really lints all targets here; plain
#     `cargo clippy` is not installed for the pinned toolchain (docs/design/TEST-NOTES.md, "clippy gate").
#   * Deploys ship `git archive HEAD`, never the working tree, and build INSIDE the container:
#     a binary built on NixOS names a /nix/store loader that Debian lacks (203/EXEC).
#   * No secrets live here. config/secrets.yaml and the backend's own config are never read or copied.

set shell := ["bash", "-euo", "pipefail", "-c"]

# --- locations -----------------------------------------------------------------------------

pve := env_var_or_default("LISTENRR_PVE_HOST", "root@pve.example.lan")
ct := env_var_or_default("LISTENRR_CTID", "200")
inst := "/opt/listenrr"
cfg := "/etc/listenrr/listenrr.toml"
port := "10700"

# sherpa-onnx-sys otherwise tries to download a 22 MB archive from GitHub.
export SHERPA_ONNX_ARCHIVE_DIR := env("SHERPA_ONNX_ARCHIVE_DIR", home_dir() / ".cache/sherpa-onnx-archive")

# `capped <cmd…>`: run under the cgroup cap and print what the kernel actually enforces.
capped := "systemd-run --user --scope -q -p MemoryHigh=28G -p MemoryMax=32G bash -c 'echo CAPPROOF memory.max=$(cat /sys/fs/cgroup$(cut -d: -f3 /proc/self/cgroup)/memory.max); exec \"$@\"' capped"

esphome := env("ESPHOME", "nix shell nixpkgs#esphome --command esphome")

[private]
default:
    @just --list --unsorted

# --- rust: build and test -------------------------------------------------------------------

# Debug build of every target.
[group('rust')]
build:
    {{ capped }} cargo build

# Release build (the AC3 numbers and anything perf-related are release-only).
[group('rust')]
build-release:
    {{ capped }} cargo build --release

# Whole suite, offline. `touch` first so a shared target dir cannot replay another tree's result.
[group('rust')]
test:
    touch src/*.rs tests/*.rs
    {{ capped }} cargo test

# Unit tests only.
[group('rust')]
test-lib:
    {{ capped }} cargo test --lib

# One integration suite, e.g. `just test-suite listener` (suites: tests/*.rs).
[group('rust')]
test-suite suite:
    {{ capped }} cargo test --test {{ suite }}

# Tests whose name contains FILTER, e.g. `just test-one coordinator::`.
[group('rust')]
test-one filter:
    {{ capped }} cargo test {{ filter }}

# `cargo clean -p` first: the kache wrapper replays cached diagnostics, so a run only counts
# if it prints `Checking listenrr v0.1.0 (<this path>)`.

# Clippy over lib, bins and tests with warnings denied (the one invocation that lints all targets).
[group('rust')]
clippy:
    cargo clean -p listenrr
    {{ capped }} cargo-clippy clippy --all-targets -- -D warnings

# rustfmt --check on ONE file you edited. Never the whole tree.
[group('rust')]
fmt-file file:
    rustfmt --edition 2021 --check {{ file }}

# The pre-commit gate: clippy, then tests (which include the firmware wire differential).
[group('rust')]
gate: clippy test

# --- rust: run the service locally ----------------------------------------------------------

# Validate a config and exit (the same `health` the systemd unit runs as ExecStartPre).
[group('run')]
health config="config/listenrr.toml":
    cargo run -q -- health --config {{ config }}

# Run the backend in the foreground against a local config.
[group('run')]
serve config="config/listenrr.toml":
    RUST_LOG=${RUST_LOG:-info} cargo run -q --release -- run --config {{ config }}

# Replay a 16 kHz mono wav or dir: `just replay clip.wav --config config/listenrr.toml --model <dir>`
[group('run')]
replay *args:
    cargo run -q --release -- replay {{ args }}

# KWS realtime factor on one clip: `just bench-kws <model-dir> <wav>`.
[group('run')]
bench-kws model wav:
    cargo run -q --release -- bench-kws --model {{ model }} --wav {{ wav }}

# Fetch the pinned KWS + VAD models (idempotent; sha256-checked).
[group('run')]
models dest="models":
    ./scripts/get_models.sh --dest {{ dest }}

# --- firmware: luna_stream ESPHome component --------------------------------------------------

# N rises with every added case; it is not a pin.

# Pure-codec host self-test (rerun after any luna_stream.h edit); expect `ALL PASS (N checks)`.
[group('firmware')]
fw-codec-test:
    g++ -std=c++17 -Wall -Wextra -Werror -Wno-pragma-once-outside-header -O2 \
      -DLUNA_STREAM_HOST_TEST -x c++ firmware/components/luna_stream/luna_stream.h \
      -o /tmp/luna_framing_test
    /tmp/luna_framing_test

# Component-side policy with no Rust counterpart: reconnect ladder, wrap-safe settle, hello cap.
[group('firmware')]
fw-backoff-test:
    g++ -std=c++17 -Wall -Wextra -Werror -Wno-pragma-once-outside-header -O2 \
      -DLUNA_STREAM_HOST_TEST -DLUNA_STREAM_NO_TEST_MAIN \
      firmware/components/luna_stream/tests/wire_diff.cpp -o /tmp/wire_diff
    /tmp/wire_diff backoff

# Needs config/secrets.yaml (gitignored): wifi_ssid, wifi_password, api_key, ota_password, and a
# luna_token that is not a placeholder. Incremental: use fw-compile-clean before claiming the gate.

# Real ESPHome compile of config/satellite.example.yaml (incremental).
[group('firmware')]
fw-compile:
    #!/usr/bin/env bash
    set -euo pipefail
    [ -f config/secrets.yaml ] || { echo "config/secrets.yaml is missing (see the recipe comment)" >&2; exit 1; }
    log=/tmp/esphome-gate.log
    rc=0
    {{ capped }} {{ esphome }} compile config/satellite.example.yaml >"$log" 2>&1 || rc=$?
    grep -E 'Successfully compiled|^(Flash|RAM):|ERROR' "$log" | tail -8 || true
    echo "esphome exit=$rc  log=$log"
    exit "$rc"

# An unchanged tree otherwise "passes" by relinking in seconds, which proves nothing.

# Forced from-scratch compile, then prove it compiled (ninja step count + luna_stream object).
[group('firmware')]
fw-compile-clean:
    #!/usr/bin/env bash
    set -euo pipefail
    rm -rf config/.esphome/build
    just fw-compile
    python3 - <<'PY'
    import re
    log = open('/tmp/esphome-gate.log', errors='replace').read()
    steps = re.findall(r'\[(\d+)/(\d+)\]', log)
    n = len(steps)
    objs = log.count('luna_stream.cpp.obj')
    print(f"ninja markers={n} luna_stream.cpp.obj={objs}")
    if n < 1000 or objs < 1:
        raise SystemExit("NOT a clean build: nothing (or not luna_stream) was recompiled")
    PY

# Every firmware check that needs no board.
[group('firmware')]
fw-gate: fw-codec-test fw-backoff-test fw-compile-clean

# --- OTA: push luna_stream firmware to satellites over Wi-Fi -------------------------------------
#
# The fleet list is config/satellites.toml — a local file: copy config/satellites.example.toml to
# that path and edit it. The FIRST flash of a new board is over USB and stays
# manual (see that file's header); a board still running the vendor package is marked `usb_first`
# there and is not an OTA target until that first flash. Builds run one at a time under the memory
# cap, because each board has its own name substitution and therefore its own build directory.

# Shell assignments (SAT_*, BACKEND_*) for one satellite. Private: used by the OTA recipes via eval.
[private]
_sat-env sat:
    #!/usr/bin/env python3
    import shlex, sys, tomllib
    cfg = tomllib.load(open("config/satellites.toml", "rb"))
    sats = {s["name"]: s for s in cfg["satellites"]}
    want = "{{ sat }}"
    if want not in sats:
        sys.exit(f"unknown satellite {want!r}; known: {', '.join(sorted(sats))}")
    s = sats[want]
    for k in ("name", "friendly_name", "source_id", "room", "host"):
        print(f"SAT_{k.upper()}={shlex.quote(s[k])}")
    print(f"BACKEND_HOST={shlex.quote(cfg['backend_host'])}")
    print(f"BACKEND_PORT={shlex.quote(cfg['backend_port'])}")
    print(f"SAT_USB_FIRST={int(bool(s.get('usb_first', False)))}")

# Satellite names in file order, one per line. Private: the fleet recipe iterates it.
[private]
_sat-names:
    #!/usr/bin/env python3
    import tomllib
    for s in tomllib.load(open("config/satellites.toml", "rb"))["satellites"]:
        if not s.get("usb_first", False):  # vendor boards are not OTA targets until flashed over USB
            print(s["name"])

# The satellites OTA knows about.
[group('firmware')]
fw-sats:
    #!/usr/bin/env python3
    import tomllib
    cfg = tomllib.load(open("config/satellites.toml", "rb"))
    print(f"backend {cfg['backend_host']}:{cfg['backend_port']}")
    for s in cfg["satellites"]:
        print(f"  {s['name']:<16} source={s['source_id']:<10} room={s['room']:<10} host={s['host']}")

# Done means the backend logged a NEW `source registered` for the board after the upload started: it
# rebooted into the new image AND got through hello. An upload that merely finished proves
# neither. Dirty firmware/config is allowed here (a canary or a dev loop) but is printed, so the
# log says what was flashed.

# Build config/satellite.example.yaml for one satellite, push it over Wi-Fi, wait for it to re-register.
[group('firmware')]
fw-ota sat:
    #!/usr/bin/env bash
    set -euo pipefail
    [ -f config/secrets.yaml ] || { echo "config/secrets.yaml is missing" >&2; exit 1; }
    eval "$({{ just_executable() }} _sat-env {{ sat }})"
    [ "$SAT_USB_FIRST" = 0 ] || { echo "FAIL: $SAT_NAME still runs the vendor firmware (usb_first in config/satellites.toml): flash it with 'just fw-usb $SAT_NAME' first, then remove the flag" >&2; exit 1; }
    dirty=""
    [ -z "$(git status --porcelain -- firmware config/satellite.example.yaml)" ] || dirty=" +DIRTY (uncommitted firmware/config)"
    echo "== OTA $SAT_NAME -> $SAT_HOST   git $(git rev-parse --short HEAD)$dirty"
    # Fail in a second, not after a build: the ESPHome OTA listener is TCP 3232.
    timeout 4 bash -c "echo > /dev/tcp/$SAT_HOST/3232" 2>/dev/null \
      || { echo "FAIL: $SAT_NAME ($SAT_HOST) does not answer on 3232: off, asleep, or the address moved" >&2; exit 1; }
    # The backend host's clock, not ours: the journal filter below must not depend on the two agreeing.
    since=$(ssh {{ pve }} 'date -u "+%F %T"')
    log=/tmp/esphome-ota-$SAT_NAME.log
    {{ capped }} {{ esphome }} \
      -s name "$SAT_NAME" -s friendly_name "$SAT_FRIENDLY_NAME" \
      -s backend_host "$BACKEND_HOST" -s backend_port "$BACKEND_PORT" \
      -s source_id "$SAT_SOURCE_ID" -s room "$SAT_ROOM" \
      run config/satellite.example.yaml --device "$SAT_HOST" --no-logs >"$log" 2>&1 \
      || { tail -25 "$log"; echo "FAIL: build/upload for $SAT_NAME (full log $log)" >&2; exit 1; }
    grep -E '^(Flash|RAM):|OTA successful' "$log" | tail -3
    # Bounded wait on a condition, not a guess: a reboot plus Wi-Fi plus hello takes ~15 s.
    deadline=$((SECONDS + 90))
    until ssh {{ pve }} "pct exec {{ ct }} -- journalctl -u listenrr --since '$since' --no-pager" \
          | grep -q "\"message\":\"source registered\",\"source\":\"$SAT_SOURCE_ID\""; do
      [ "$SECONDS" -lt "$deadline" ] || { echo "FAIL: $SAT_NAME uploaded but never re-registered on the backend within 90 s" >&2; exit 1; }
      sleep 3
    done
    echo "OK: $SAT_NAME re-registered on the backend as '$SAT_SOURCE_ID' after the update"

# The first flash of a board is over USB: a vendor board has no luna_stream to receive an update
# from, and a replaced image changes the API encryption key, so Home Assistant must be told. This
# builds with the board's substitutions, uploads over the one serial port that is present, and
# then waits for the backend to log a NEW `source registered` for it, which proves it joined Wi-Fi and
# got through hello. The source must already be in the backend's listenrr.toml or hello is refused.

# First flash of one satellite over USB (plug exactly one board in), then wait for it to register on the backend.
[group('firmware')]
fw-usb sat:
    #!/usr/bin/env bash
    set -euo pipefail
    [ -f config/secrets.yaml ] || { echo "config/secrets.yaml is missing" >&2; exit 1; }
    eval "$({{ just_executable() }} _sat-env {{ sat }})"
    ports=$(ls /dev/ttyACM* /dev/ttyUSB* 2>/dev/null || true)
    [ -n "$ports" ] || { echo "FAIL: no serial port: plug the board in (hold BOOT while plugging if it does not appear)" >&2; exit 1; }
    [ "$(echo "$ports" | wc -l)" -eq 1 ] || { echo "FAIL: more than one serial port ($(echo $ports)); unplug the others so the wrong board cannot be flashed" >&2; exit 1; }
    port=$ports
    { [ -r "$port" ] && [ -w "$port" ]; } || { echo "FAIL: no access to $port. Run: sudo setfacl -m u:$USER:rw $port" >&2; exit 1; }
    since=$(ssh {{ pve }} 'date -u "+%F %T"')
    dirty=""
    [ -z "$(git status --porcelain -- firmware config/satellite.example.yaml)" ] || dirty=" +DIRTY (uncommitted firmware/config)"
    echo "== USB $SAT_NAME via $port   git $(git rev-parse --short HEAD)$dirty"
    log=/tmp/esphome-usb-$SAT_NAME.log
    {{ capped }} {{ esphome }} \
      -s name "$SAT_NAME" -s friendly_name "$SAT_FRIENDLY_NAME" \
      -s backend_host "$BACKEND_HOST" -s backend_port "$BACKEND_PORT" \
      -s source_id "$SAT_SOURCE_ID" -s room "$SAT_ROOM" \
      run config/satellite.example.yaml --device "$port" --no-logs >"$log" 2>&1 \
      || { tail -25 "$log"; echo "FAIL: build/upload for $SAT_NAME (full log $log)" >&2; exit 1; }
    grep -E '^(Flash|RAM):|Hash of data verified|Successfully uploaded' "$log" | tail -4
    deadline=$((SECONDS + 120))
    until ssh {{ pve }} "pct exec {{ ct }} -- journalctl -u listenrr --since '$since' --no-pager" \
          | grep -q "\"message\":\"source registered\",\"source\":\"$SAT_SOURCE_ID\""; do
      [ "$SECONDS" -lt "$deadline" ] || { echo "FAIL: $SAT_NAME flashed but never registered on the backend within 120 s (wrong Wi-Fi? source '$SAT_SOURCE_ID' missing from the backend config?)" >&2; exit 1; }
      sleep 3
    done
    echo "OK: $SAT_NAME registered on the backend as '$SAT_SOURCE_ID'. Next: re-add it in Home Assistant (new API key), then set its ha_entity/device_id."

# Serial and fail-fast: a bad image reaches one board, not all of them. Refuses a dirty tree, so a
# fleet push is reproducible from a commit.

# Push the committed firmware to every satellite in config/satellites.toml, stopping at the first failure.
[group('firmware')]
fw-ota-all:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "$(git status --porcelain -- firmware config/satellite.example.yaml)" ]; then
      echo "refusing a fleet push from uncommitted firmware/config changes: commit first, or use 'just fw-ota <name>' for one board" >&2
      exit 1
    fi
    done_list=""
    for s in $({{ just_executable() }} _sat-names); do
      {{ just_executable() }} fw-ota "$s" || { echo "STOPPED at $s. updated so far:${done_list:- none}" >&2; exit 1; }
      done_list="$done_list $s"
    done
    echo "fleet updated:$done_list"

# --- tablets: the Android voice-source app (tablet/) ----------------------------------------------
#
# The tablets run FreeKiosk and have no Home Assistant media_player, so they are not ESPHome boards
# and are NOT in [[satellites]] (fw-ota / fw-ota-all would try to flash them). They are listed in
# [[tablets]] in config/satellites.toml. `host` is the tablet's address; the adb port changes every
# time Wireless debugging is switched on, so the live adb serial is found by asking each connected
# device for its IP, and a tablet that is connected twice (an IP entry and an mDNS entry) is one device.

# Shell assignments (TABLET_*) for one tablet. Private: used by the tablet recipes via eval.
[private]
_tablet-env name:
    #!/usr/bin/env python3
    import shlex, sys, tomllib
    cfg = tomllib.load(open("config/satellites.toml", "rb"))
    tablets = {t["name"]: t for t in cfg.get("tablets", [])}
    want = "{{ name }}"
    if want not in tablets:
        sys.exit(f"unknown tablet {want!r}; known: {', '.join(sorted(tablets)) or 'none'}")
    t = tablets[want]
    for k in ("name", "source_id", "room", "host"):
        print(f"TABLET_{k.upper()}={shlex.quote(t[k])}")

# The adb serial of the one connected device whose Wi-Fi address is the tablet's `host`. Private.
[private]
_tablet-serial name:
    #!/usr/bin/env python3
    import re, subprocess, sys, tomllib
    cfg = tomllib.load(open("config/satellites.toml", "rb"))
    host = {t["name"]: t["host"] for t in cfg.get("tablets", [])}["{{ name }}"]
    def adb(*args):
        return subprocess.run(["adb", *args], capture_output=True, text=True, timeout=20).stdout.replace("\r", "")
    serials = [l.split()[0] for l in adb("devices").splitlines()[1:] if l.strip().endswith("device")]
    found = {}  # hardware serialno -> first adb serial that answered for this host
    for s in serials:
        ip = re.search(r"inet (\d+\.\d+\.\d+\.\d+)", adb("-s", s, "shell", "ip", "-4", "addr", "show", "wlan0"))
        if ip and ip.group(1) == host:
            hw = adb("-s", s, "shell", "getprop", "ro.serialno").strip() or s
            found.setdefault(hw, s)
    if not found:
        sys.exit(f"no connected adb device has the address {host}: is Wireless debugging on, and did 'adb connect' succeed? (adb devices: {serials or 'none'})")
    if len(found) > 1:
        sys.exit(f"{len(found)} different devices claim {host}: {sorted(found)}")
    print(next(iter(found.values())))

# Host-side check of the tablet app's wire codec against docs/protocol.md section 9 (no device, no secrets).
[group('tablet')]
tablet-test:
    bash tablet/build.sh --test

# Build the signed APK for one tablet from [[tablets]] (identity) and secrets.yaml (hello token).
[group('tablet')]
tablet-build name:
    bash tablet/build.sh {{ name }}

# Install the APK on one tablet over adb, grant the microphone, exempt it from battery limits, start it.
[group('tablet')]
tablet-install name:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v adb >/dev/null || { echo "adb is not on PATH: enter the devshell (direnv allow, or 'nix develop --no-pure-eval')" >&2; exit 1; }
    eval "$({{ just_executable() }} _tablet-env {{ name }})"
    apk="tablet/build/$TABLET_NAME/listenrr-tablet-$TABLET_NAME.apk"
    [ -f "$apk" ] || { echo "no APK at $apk: run 'just tablet-build $TABLET_NAME' first" >&2; exit 1; }
    serial=$({{ just_executable() }} _tablet-serial {{ name }})
    pkg=dev.listenrr.tablet
    echo "== install $TABLET_NAME ($TABLET_HOST) via adb serial $serial"
    adb -s "$serial" install -r "$apk"
    adb -s "$serial" shell pm grant "$pkg" android.permission.RECORD_AUDIO
    # Android 13+ only; older releases reject it, which is harmless.
    adb -s "$serial" shell pm grant "$pkg" android.permission.POST_NOTIFICATIONS || true
    # Samsung's battery management otherwise kills a background service on an idle tablet.
    adb -s "$serial" shell dumpsys deviceidle whitelist "+$pkg" >/dev/null
    # Lets the boot receiver open the activity from the background (see BootReceiver.java).
    adb -s "$serial" shell appops set "$pkg" SYSTEM_ALERT_WINDOW allow
    # Android forbids starting a microphone service from the background, so it is started by
    # opening the activity; the activity starts the service and closes itself.
    adb -s "$serial" shell am start -n "$pkg/.MainActivity" >/dev/null
    echo "started $pkg on $TABLET_NAME. Check it with: just tablet-status $TABLET_NAME"

# Is the app running as a foreground microphone service, and is the backend hearing the tablet? (read-only)
[group('tablet')]
tablet-status name:
    #!/usr/bin/env bash
    set -euo pipefail
    eval "$({{ just_executable() }} _tablet-env {{ name }})"
    serial=$({{ just_executable() }} _tablet-serial {{ name }})
    pkg=dev.listenrr.tablet
    echo "== $TABLET_NAME ($TABLET_HOST, adb $serial)"
    adb -s "$serial" shell dumpsys activity services "$pkg" | tr -d '\r' | grep -E "ServiceRecord|isForeground|foregroundServiceType" | head -4 || echo "service not running"
    echo "-- backend, source '$TABLET_SOURCE_ID':"
    ssh {{ pve }} "pct exec {{ ct }} -- journalctl -u listenrr --since '-3min' --no-pager" \
      | grep "\"source\":\"$TABLET_SOURCE_ID\"" | grep -E 'stream health|source registered' | tail -2 | cut -c1-420 || echo "the backend has not heard this source in 3 minutes"

# Live app log from one tablet (Ctrl-C to stop).
[group('tablet')]
tablet-logs name:
    #!/usr/bin/env bash
    set -euo pipefail
    serial=$({{ just_executable() }} _tablet-serial {{ name }})
    exec adb -s "$serial" logcat -v time -s listenrr:V

# --- deploy: the backend's LXC container on the Proxmox host -------------------------------------
#
# Host and container id come from LISTENRR_PVE_HOST (default root@pve.example.lan) and
# LISTENRR_CTID (default 200); `pct` runs on the Proxmox host, so every recipe sshes there.

# One-shot routine redeploy: ship HEAD, build in the container, install, restart, verify.
[group('deploy')]
deploy: deploy-ship deploy-build deploy-install deploy-verify

# Extracts fresh (so deleted files vanish) and with `tar -m`: archive mtimes are commit times and
# can predate the last build, which would make cargo skip a changed file.

# Ship `git archive HEAD` (never the working tree) into the container.
[group('deploy')]
deploy-ship:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! git diff --quiet HEAD --; then
      echo "NOTE: tracked files differ from HEAD; deploying HEAD, not the working tree." >&2
    fi
    echo "shipping $(git rev-parse --short HEAD): $(git log -1 --format=%s)"
    git archive --format=tar.gz -o /tmp/listenrr-src.tgz HEAD
    scp -q /tmp/listenrr-src.tgz {{ pve }}:/root/listenrr-src.tgz
    ssh {{ pve }} "pct push {{ ct }} /root/listenrr-src.tgz /root/listenrr-src.tgz"
    ssh {{ pve }} "pct exec {{ ct }} -- bash -s" <<'REMOTE'
    set -euo pipefail
    rm -rf {{ inst }}/src.new && mkdir -p {{ inst }}/src.new
    tar xzmf /root/listenrr-src.tgz -C {{ inst }}/src.new
    rm -rf {{ inst }}/src.old
    [ -d {{ inst }}/src ] && mv {{ inst }}/src {{ inst }}/src.old
    mv {{ inst }}/src.new {{ inst }}/src
    echo "extracted $(find {{ inst }}/src -type f | wc -l) files"
    REMOTE

# The exit code is the recipe's; the full log stays in /root/build.log inside the container.

# Build --release INSIDE the container (~3.5 min on the N100's one core).
[group('deploy')]
deploy-build:
    #!/usr/bin/env bash
    set -euo pipefail
    ssh {{ pve }} "pct exec {{ ct }} -- bash -s" <<'REMOTE'
    set -euo pipefail
    export PATH=/root/.cargo/bin:/usr/bin:/bin SHERPA_ONNX_ARCHIVE_DIR=/root/sherpa-cache CARGO_TARGET_DIR=/root/ctarget
    cd {{ inst }}/src
    rc=0
    cargo build --release >/root/build.log 2>&1 || rc=$?
    tail -5 /root/build.log
    echo "BUILD_EXIT=$rc"
    exit "$rc"
    REMOTE

# Only copies to listenrr.prev when the binary differs, so a repeat deploy cannot overwrite the
# rollback target with itself.

# Install the built binary, keep the previous one as listenrr.prev, restart.
[group('deploy')]
deploy-install:
    #!/usr/bin/env bash
    set -euo pipefail
    ssh {{ pve }} "pct exec {{ ct }} -- bash -s" <<'REMOTE'
    set -euo pipefail
    new=/root/ctarget/release/listenrr
    cur={{ inst }}/listenrr
    if cmp -s "$new" "$cur"; then
      echo "installed binary already equals the build; restarting anyway"
    else
      cp -a "$cur" {{ inst }}/listenrr.prev
      install -m 0755 "$new" "$cur"
      echo "installed; previous binary kept as {{ inst }}/listenrr.prev"
    fi
    before=$(systemctl show -p MainPID --value listenrr)
    systemctl restart listenrr
    after=$(systemctl show -p MainPID --value listenrr)
    echo "MainPID $before -> $after"
    [ "$before" != "$after" ] || { echo "process did not restart" >&2; exit 1; }
    REMOTE

# Is it really the new code, up, and listening? Fails loudly rather than printing "ok".
[group('deploy')]
deploy-verify:
    #!/usr/bin/env bash
    set -euo pipefail
    ssh {{ pve }} "pct exec {{ ct }} -- bash -s" <<'REMOTE'
    set -euo pipefail
    echo "sha256 installed: $(sha256sum {{ inst }}/listenrr | cut -c1-16)  built: $(sha256sum /root/ctarget/release/listenrr | cut -c1-16)"
    [ "$(systemctl is-active listenrr)" = active ] || { systemctl status listenrr --no-pager | tail -20; exit 1; }
    runuser -u listenrr -- {{ inst }}/listenrr health --config {{ cfg }}
    timeout 30 bash -c 'until ss -ltn | grep -q ":{{ port }} "; do sleep 1; done' \
      || { echo "nothing listening on :{{ port }} after 30 s" >&2; exit 1; }
    echo "listening on :{{ port }}"
    since=$(systemctl show -p ActiveEnterTimestamp --value listenrr)
    journalctl -u listenrr --since "$since" --no-pager -o cat | cut -c1-220 | tail -15
    REMOTE

# Does not restart; run `just deploy-install` (or `systemctl restart`) when you want it live.

# Install deploy/listenrr.service from the shipped tree, verify and daemon-reload.
[group('deploy')]
deploy-unit:
    #!/usr/bin/env bash
    set -euo pipefail
    ssh {{ pve }} "pct exec {{ ct }} -- bash -s" <<'REMOTE'
    set -euo pipefail
    install -m 0644 {{ inst }}/src/deploy/listenrr.service /etc/systemd/system/listenrr.service
    systemd-analyze verify /etc/systemd/system/listenrr.service
    systemctl daemon-reload
    systemctl is-enabled listenrr
    REMOTE

# Put listenrr.prev back and restart. Satellites reconnect on their own; no reflash.
[group('deploy')]
deploy-rollback:
    ssh {{ pve }} "pct exec {{ ct }} -- bash -c 'set -euo pipefail; install -m 0755 {{ inst }}/listenrr.prev {{ inst }}/listenrr && systemctl restart listenrr && systemctl is-active listenrr'"

# What is running right now: service state, binary hash, listener.
[group('deploy')]
status:
    ssh {{ pve }} "pct exec {{ ct }} -- bash -c 'systemctl status listenrr --no-pager | head -8; sha256sum {{ inst }}/listenrr | cut -c1-16; ss -ltn | grep :{{ port }} || true'"

# Last N journal lines (default 40), JSON trimmed to one line each.
[group('deploy')]
logs n="40":
    ssh {{ pve }} "pct exec {{ ct }} -- journalctl -u listenrr -n {{ n }} --no-pager -o cat" | cut -c1-240

# Follow the journal live (Ctrl-C to stop).
[group('deploy')]
logs-follow:
    ssh {{ pve }} "pct exec {{ ct }} -- journalctl -u listenrr -f -o cat" | cut -c1-240

# Recent clips (missed-wake and false-accept) written by the service.
[group('deploy')]
clips:
    ssh {{ pve }} "pct exec {{ ct }} -- bash -c 'ls -lt /var/lib/listenrr/clips/*/ | head -20'"
