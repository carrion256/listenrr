#!/usr/bin/env bash
# Fetch the models listenrr needs, pinned (plan S2.1, S2.4).
#
#   models/kws/<kws bundle>/      sherpa-onnx streaming KWS zipformer (English)
#   models/vad/silero_vad.onnx    Silero VAD ([capture] VAD, docs/design/CONTRACT-NOTES.md §6.6)
#
# Idempotent: what is already on disk is kept when its recorded sha256 still matches,
# so a re-run is a no-op. Nothing here touches the test suite — tests are hermetic and
# never download.
set -euo pipefail

KWS_TAG="kws-models"
KWS_NAME="sherpa-onnx-kws-zipformer-gigaspeech-3.3M-2024-01-01"
KWS_URL="https://github.com/k2-fsa/sherpa-onnx/releases/download/${KWS_TAG}/${KWS_NAME}.tar.bz2"
VAD_NAME="silero_vad.onnx"
VAD_URL="https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/${VAD_NAME}"

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEST="${LISTENRR_MODELS_DIR:-$script_dir/../models}"
FORCE=0

usage() {
  cat <<EOF
usage: get_models.sh [--dest DIR] [--force] [--help]

Download the pinned models into DIR (default: <repo>/models; also honoured as
\$LISTENRR_MODELS_DIR):

  kws/$KWS_NAME/   streaming KWS zipformer (English, 3.3M)
  vad/$VAD_NAME        Silero VAD for [capture]

Idempotent: an existing file is kept when its sha256 matches the digest recorded in
DIR/.sha256/ on the first download; a mismatch or --force re-downloads. Prints the
resulting [kws]/[capture] config snippet at the end.
EOF
}

while (($#)); do
  case "$1" in
    -h | --help)
      usage
      exit 0
      ;;
    -f | --force)
      FORCE=1
      ;;
    --dest)
      [[ $# -ge 2 ]] || {
        echo "get_models.sh: --dest needs a directory" >&2
        exit 2
      }
      DEST="$2"
      shift
      ;;
    *)
      echo "get_models.sh: unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

mkdir -p "$DEST"
DEST=$(cd -- "$DEST" && pwd)
DIGEST_DIR="$DEST/.sha256"
mkdir -p "$DIGEST_DIR"

command -v curl >/dev/null 2>&1 || {
  echo "get_models.sh: curl is required" >&2
  exit 1
}

digest_of() { sha256sum -- "$1" | cut -d' ' -f1; }

# fetch URL TARGET — download unless TARGET exists and matches its recorded digest.
fetch() {
  local url=$1 target=$2 name record actual want tmp
  name=$(basename -- "$target")
  record="$DIGEST_DIR/$name.sha256"

  if [[ $FORCE -eq 0 && -s $target && -f $record ]]; then
    want=$(<"$record")
    want=${want%% *}
    actual=$(digest_of "$target")
    if [[ $actual == "$want" ]]; then
      echo "ok       $target"
      return 0
    fi
    echo "stale    $target (digest $actual != $want; redownloading)"
  fi

  mkdir -p "$(dirname -- "$target")"
  tmp="$target.part.$$"
  curl -fsSL --retry 3 -o "$tmp" "$url" || {
    rm -f "$tmp"
    echo "get_models.sh: download failed: $url" >&2
    return 1
  }
  mv -- "$tmp" "$target"
  digest_of "$target" >"$record"
  echo "fetched  $target ($(digest_of "$target"))"
}

# unpack ARCHIVE OUT — extract the single top-level directory of the release tarball.
unpack() {
  local archive=$1 out=$2
  if [[ $FORCE -eq 0 && -d $out ]] && [[ -n $(ls -A -- "$out" 2>/dev/null) ]]; then
    echo "ok       $out"
    return 0
  fi
  rm -rf -- "$out"
  mkdir -p -- "$out"
  tar -xjf "$archive" -C "$out" --strip-components=1
  echo "unpacked $out"
}

fetch "$KWS_URL" "$DEST/kws/$KWS_NAME.tar.bz2"
unpack "$DEST/kws/$KWS_NAME.tar.bz2" "$DEST/kws/$KWS_NAME"
fetch "$VAD_URL" "$DEST/vad/$VAD_NAME"

MODEL_DIR="$DEST/kws/$KWS_NAME"
for required in encoder decoder joiner; do
  compgen -G "$MODEL_DIR/${required}*.onnx" >/dev/null ||
    echo "get_models.sh: warning: no ${required}*.onnx in $MODEL_DIR" >&2
done
[[ -f $MODEL_DIR/tokens.txt ]] ||
  echo "get_models.sh: warning: no tokens.txt in $MODEL_DIR" >&2

KEYWORDS="$MODEL_DIR/keywords.txt"
if [[ ! -f $KEYWORDS ]]; then
  KEYWORDS=$(find "$MODEL_DIR" -maxdepth 1 -name '*keyword*.txt' -print -quit 2>/dev/null || true)
  KEYWORDS=${KEYWORDS:-$MODEL_DIR/keywords.txt}
  echo "get_models.sh: note: no keyword list shipped; create $KEYWORDS with" >&2
  echo "               sherpa-onnx-text2token --tokens $MODEL_DIR/tokens.txt 'luna'" >&2
fi

cat <<EOF

config/listenrr.toml:

  [kws]
  model = "$MODEL_DIR"
  keywords_file = "$KEYWORDS"
  threshold = 0.25

  [capture]
  vad_model = "$DEST/vad/$VAD_NAME"
EOF
