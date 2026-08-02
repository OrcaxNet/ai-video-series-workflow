#!/bin/sh
set -eu

ANALYZER_ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
export FLO154_ANALYZER_ROOT="$ANALYZER_ROOT"
export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1
export HF_DATASETS_OFFLINE=1
export NO_PROXY='*'
exec "$ANALYZER_ROOT/venv/bin/python" -I "$ANALYZER_ROOT/lib/analyzer.py" "$@"
