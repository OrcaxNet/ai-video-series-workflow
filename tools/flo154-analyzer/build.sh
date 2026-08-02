#!/bin/sh
set -eu

if [ "$#" -ne 2 ]; then
  echo 'usage: build.sh OUTPUT_ROOT FASTER_WHISPER_SNAPSHOT' >&2
  exit 2
fi

OUTPUT_ROOT=$1
MODEL_SNAPSHOT=$2
SOURCE_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)

case "$OUTPUT_ROOT" in
  /*) ;;
  *) echo 'OUTPUT_ROOT must be absolute' >&2; exit 2 ;;
esac
if [ -e "$OUTPUT_ROOT" ]; then
  echo 'OUTPUT_ROOT must not already exist' >&2
  exit 2
fi
for required in model.bin config.json tokenizer.json vocabulary.json preprocessor_config.json; do
  test -f "$MODEL_SNAPSHOT/$required" || {
    echo "model snapshot is missing $required" >&2
    exit 2
  }
done

mkdir -p "$OUTPUT_ROOT/bin" "$OUTPUT_ROOT/config" "$OUTPUT_ROOT/lib" \
  "$OUTPUT_ROOT/licenses" "$OUTPUT_ROOT/models/faster-whisper-large-v3-turbo"
uv venv --python 3.12 "$OUTPUT_ROOT/venv"
uv pip sync --python "$OUTPUT_ROOT/venv/bin/python" "$SOURCE_ROOT/requirements.lock"

cp "$SOURCE_ROOT/launcher.sh" "$OUTPUT_ROOT/bin/flo154-analyzer"
chmod 0755 "$OUTPUT_ROOT/bin/flo154-analyzer"
cp "$SOURCE_ROOT/analyzer.py" "$OUTPUT_ROOT/lib/analyzer.py"
cp "$SOURCE_ROOT/analyzer.json" "$OUTPUT_ROOT/config/analyzer.json"
cp "$SOURCE_ROOT/normalizer.json" "$OUTPUT_ROOT/config/normalizer.json"
cp "$SOURCE_ROOT/requirements.lock" "$OUTPUT_ROOT/config/requirements.lock"
cp "$SOURCE_ROOT/av-sync-correlation-v1.json" "$OUTPUT_ROOT/models/av-sync-correlation-v1.json"
cp "$SOURCE_ROOT/license-snapshot.json" "$OUTPUT_ROOT/licenses/snapshot.json"

for model_file in model.bin config.json tokenizer.json vocabulary.json preprocessor_config.json; do
  cp "$MODEL_SNAPSHOT/$model_file" "$OUTPUT_ROOT/models/faster-whisper-large-v3-turbo/$model_file"
done
FACE_CASCADE=$(
  "$OUTPUT_ROOT/venv/bin/python" -c 'import cv2; print(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")'
)
VAD_MODEL=$(
  "$OUTPUT_ROOT/venv/bin/python" -c 'import pathlib, faster_whisper; print(pathlib.Path(faster_whisper.__file__).parent / "assets" / "silero_vad_v6.onnx")'
)
cp "$FACE_CASCADE" "$OUTPUT_ROOT/models/haarcascade_frontalface_default.xml"
cp "$VAD_MODEL" "$OUTPUT_ROOT/models/silero_vad_v6.onnx"
cp "$(command -v ffmpeg)" "$OUTPUT_ROOT/bin/ffmpeg"
cp "$(command -v ffprobe)" "$OUTPUT_ROOT/bin/ffprobe"
chmod 0755 "$OUTPUT_ROOT/bin/ffmpeg" "$OUTPUT_ROOT/bin/ffprobe"

FLO154_FFMPEG_VERSION=$(ffmpeg -version | sed -n '1s/^ffmpeg version \([^ ]*\).*/\1/p')
FLO154_FFPROBE_VERSION=$(ffprobe -version | sed -n '1s/^ffprobe version \([^ ]*\).*/\1/p')
export FLO154_FFMPEG_VERSION FLO154_FFPROBE_VERSION
"$OUTPUT_ROOT/venv/bin/python" "$SOURCE_ROOT/build.py" "$OUTPUT_ROOT"
