#!/usr/bin/env bash
# Build the listenrr tablet app without Gradle, without Maven and without network.
#
#   tablet/build.sh --test          host-side wire conformance (no Android SDK needed)
#   tablet/build.sh <tablet-name>   APK for the [[tablets]] entry with that name
#
# Inputs (override with env for a dry run; the real files are never written):
#   LISTENRR_SATELLITES_TOML  default <repo>/config/satellites.toml
#   LISTENRR_SECRETS_YAML     default <repo>/config/secrets.yaml
# The signing key lives outside the repo so a rebuild installs over the old APK:
#   ${XDG_DATA_HOME:-$HOME/.local/share}/listenrr/tablet.keystore
set -euo pipefail

TABLET_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$TABLET_DIR/.." && pwd)"
PKG=dev.listenrr.tablet
PKG_PATH=dev/listenrr/tablet
CHIME_SRC="$REPO_DIR/config/sounds/ack.wav"
SATELLITES_TOML="${LISTENRR_SATELLITES_TOML:-$REPO_DIR/config/satellites.toml}"
SECRETS_YAML="${LISTENRR_SECRETS_YAML:-$REPO_DIR/config/secrets.yaml}"

SDK="${ANDROID_HOME:-/nix/store/7387av15s3098jg79jc402955d0ap4v7-androidsdk/libexec/android-sdk}"
BUILD_TOOLS="$SDK/build-tools/35.0.0"
PLATFORM="$SDK/platforms/android-34/android.jar"
KEYSTORE="${XDG_DATA_HOME:-$HOME/.local/share}/listenrr/tablet.keystore"

fail() {
  echo "build.sh: $*" >&2
  exit 1
}

# javac/java come from the devshell; outside it, borrow the pinned JDK from nixpkgs.
# The re-exec wraps the whole script so the SDK's java-based tools (d8, apksigner) work too.
if ! command -v javac >/dev/null 2>&1 && [ -z "${LISTENRR_JDK_READY:-}" ]; then
  command -v nix >/dev/null 2>&1 || fail "no javac on PATH and no nix to provide one"
  echo "build.sh: no javac on PATH, re-running under nixpkgs#jdk17" >&2
  exec nix shell nixpkgs#jdk17 --command bash "$0" "$@"
fi

need() {
  [ -x "$1" ] || fail "missing tool: $1 (set ANDROID_HOME to a working Android SDK)"
}

# ------------------------------------------------------------------- --test
if [ "${1:-}" = "--test" ]; then
  OUT="$TABLET_DIR/build/test"
  rm -rf "$OUT"
  mkdir -p "$OUT/classes"
  echo "== javac (host JVM: Wire + WireTest)"
  javac -Xlint:all -nowarn -d "$OUT/classes" \
    "$TABLET_DIR/src/$PKG_PATH/Wire.java" "$TABLET_DIR/test/WireTest.java"
  echo "== java WireTest"
  exec java -cp "$OUT/classes" WireTest
fi

# --------------------------------------------------------------------- APK
NAME="${1:-}"
[ -n "$NAME" ] || {
  echo "usage: tablet/build.sh --test | tablet/build.sh <tablet-name>" >&2
  exit 2
}

FW_VERSION="$(tr -d '[:space:]' <"$TABLET_DIR/VERSION")"
[ -n "$FW_VERSION" ] || fail "tablet/VERSION is empty"
case "$FW_VERSION" in
[0-9]*.[0-9]*.[0-9]*) ;;
*) fail "tablet/VERSION must be major.minor.patch, got '$FW_VERSION'" ;;
esac
# versionCode = major*10000 + minor*100 + patch (three components, each < 100).
VERSION_CODE=$(( ${FW_VERSION%%.*} * 10000 + $(echo "$FW_VERSION" | cut -d. -f2) * 100 + $(echo "$FW_VERSION" | cut -d. -f3) ))

[ -f "$SATELLITES_TOML" ] || fail "no satellites config at $SATELLITES_TOML"
[ -f "$SECRETS_YAML" ] || fail "no secrets file at $SECRETS_YAML"
[ -f "$CHIME_SRC" ] || fail "no ack chime at $CHIME_SRC"
need "$BUILD_TOOLS/aapt2"
need "$BUILD_TOOLS/aapt"
need "$BUILD_TOOLS/d8"
need "$BUILD_TOOLS/zipalign"
need "$BUILD_TOOLS/apksigner"
[ -f "$PLATFORM" ] || fail "no android.jar at $PLATFORM"

WORK="$TABLET_DIR/build/$NAME"
GEN="$WORK/gen"
CLASSES="$WORK/classes"
DEXDIR="$WORK/dex"
STAGE="$WORK/stage"
APK="$WORK/listenrr-tablet-$NAME.apk"
rm -rf "$WORK"
mkdir -p "$GEN/$PKG_PATH" "$CLASSES" "$DEXDIR" "$STAGE/assets"

echo "== config for tablet '$NAME' (from $SATELLITES_TOML)"
# Reads the two config files and writes Config.java. The token is never printed.
python3 - "$SATELLITES_TOML" "$SECRETS_YAML" "$NAME" "$FW_VERSION" "$GEN/$PKG_PATH/Config.java" <<'PY'
import re, sys, tomllib

toml_path, secrets_path, name, fw, out_path = sys.argv[1:6]

with open(toml_path, "rb") as fh:
    cfg = tomllib.load(fh)

for key in ("backend_host", "backend_port"):
    if key not in cfg:
        sys.exit(f"build.sh: {toml_path} has no top-level {key}")
host, port = str(cfg["backend_host"]), str(cfg["backend_port"])
if not re.fullmatch(r"[0-9]{1,5}", port):
    sys.exit(f"build.sh: backend_port in {toml_path} is not a number")

tablets = cfg.get("tablets") or []
entry = next((t for t in tablets if t.get("name") == name), None)
if entry is None:
    names = ", ".join(sorted(str(t.get("name")) for t in tablets)) or "none"
    sys.exit(f"build.sh: no [[tablets]] entry named {name!r} in {toml_path} (known: {names})")
for key in ("source_id", "room"):
    if not entry.get(key):
        sys.exit(f"build.sh: [[tablets]] {name!r} in {toml_path} has no {key}")
source_id, room = str(entry["source_id"]), str(entry["room"])

token = None
with open(secrets_path, encoding="utf-8") as fh:
    for line in fh:
        m = re.match(r'^\s*luna_token:\s*"?([^"\r\n]*)"?\s*$', line)
        if m and m.group(1).strip():
            token = m.group(1).strip()
            break
if token is None:
    sys.exit(f"build.sh: no non-empty luna_token in {secrets_path}")


def jlit(s):
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'


with open(out_path, "w", encoding="utf-8") as fh:
    fh.write("// Generated by tablet/build.sh -- do not edit, do not commit.\n")
    fh.write("package dev.listenrr.tablet;\n\n")
    fh.write("public final class Config {\n")
    fh.write("    private Config() {}\n\n")
    fh.write(f"    public static final String BACKEND_HOST = {jlit(host)};\n")
    fh.write(f"    public static final int BACKEND_PORT = {port};\n")
    fh.write(f"    public static final String SOURCE_ID = {jlit(source_id)};\n")
    fh.write(f"    public static final String ROOM = {jlit(room)};\n")
    fh.write(f"    public static final String TOKEN = {jlit(token)};\n")
    fh.write(f"    public static final String FW_VERSION = {jlit(fw)};\n")
    fh.write("}\n")

print(f"build.sh: host={host} port={port} source_id={source_id} room={room} "
      f"fw={fw} token=<{len(token)} chars, hidden>")
PY

echo "== javac -source 8 -target 8 -bootclasspath android.jar"
javac -source 8 -target 8 -nowarn -bootclasspath "$PLATFORM" -d "$CLASSES" \
  "$GEN/$PKG_PATH/Config.java" \
  "$TABLET_DIR/src/$PKG_PATH/Wire.java" \
  "$TABLET_DIR/src/$PKG_PATH/Player.java" \
  "$TABLET_DIR/src/$PKG_PATH/SatelliteService.java" \
  "$TABLET_DIR/src/$PKG_PATH/MainActivity.java"

echo "== d8"
"$BUILD_TOOLS/d8" --lib "$PLATFORM" --min-api 26 --output "$DEXDIR" \
  $(find "$CLASSES" -name '*.class')

echo "== aapt2 link"
"$BUILD_TOOLS/aapt2" link -o "$WORK/base.apk" -I "$PLATFORM" \
  --manifest "$TABLET_DIR/AndroidManifest.xml" \
  --min-sdk-version 26 --target-sdk-version 34 \
  --version-code "$VERSION_CODE" --version-name "$FW_VERSION"
cp "$WORK/base.apk" "$WORK/withdex.apk"

echo "== aapt add classes.dex + assets/ack.wav"
(cd "$DEXDIR" && "$BUILD_TOOLS/aapt" add "$WORK/withdex.apk" classes.dex)
cp "$CHIME_SRC" "$STAGE/assets/ack.wav"
(cd "$STAGE" && "$BUILD_TOOLS/aapt" add "$WORK/withdex.apk" assets/ack.wav)

echo "== zipalign"
"$BUILD_TOOLS/zipalign" -f -p 4 "$WORK/withdex.apk" "$WORK/aligned.apk"

echo "== apksigner sign"
if [ ! -f "$KEYSTORE" ]; then
  mkdir -p "$(dirname "$KEYSTORE")"
  echo "build.sh: creating signing key at $KEYSTORE"
  keytool -genkeypair -v -keystore "$KEYSTORE" -storepass listenrr-tablet \
    -keypass listenrr-tablet -alias listenrr -keyalg RSA -keysize 2048 \
    -validity 10000 -dname "CN=listenrr tablet, OU=listenrr, O=listenrr, L=home, C=NO" >&2
fi
"$BUILD_TOOLS/apksigner" sign --ks "$KEYSTORE" \
  --ks-key-alias listenrr --ks-pass pass:listenrr-tablet --key-pass pass:listenrr-tablet \
  --min-sdk-version 26 --max-sdk-version 36 \
  --out "$APK" "$WORK/aligned.apk"

echo "== apksigner verify"
"$BUILD_TOOLS/apksigner" verify --verbose "$APK"

echo "== aapt2 dump badging"
"$BUILD_TOOLS/aapt2" dump badging "$APK" | head -n 20

echo "$APK"
