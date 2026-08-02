#!/usr/bin/env python3
"""Create the deterministic inventory and analyzer seal for one local build."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sys


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            value.update(block)
    return value.hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def component(
    root: Path,
    name: str,
    kind: str,
    relative: str,
    version: str,
    license_id: str,
    source: str,
) -> dict[str, object]:
    return {
        "name": name,
        "kind": kind,
        "path": relative,
        "sha256": digest(root / relative),
        "version": version,
        "spdxLicense": license_id,
        "commercialUse": True,
        "source": source,
    }


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: build.py ANALYZER_ROOT", file=sys.stderr)
        return 2
    root = Path(sys.argv[1]).resolve()
    if not root.is_dir():
        raise ValueError("analyzer root must exist")

    inventory_files: list[dict[str, str]] = []
    for directory in (root / "venv/lib").glob("python*/site-packages"):
        for path in sorted(directory.rglob("*")):
            # Wheel RECORD rows for generated console scripts contain hashes
            # of shebangs with the absolute venv path. The scripts themselves
            # are outside site-packages and are not used by this analyzer, so
            # excluding RECORD removes build-directory identity without
            # weakening the inventory of every executable package file.
            if (
                path.is_file()
                and not path.is_symlink()
                and path.name != "RECORD"
                and path.suffix not in {".pyc", ".pyo"}
            ):
                inventory_files.append(
                    {"path": path.relative_to(root).as_posix(), "sha256": digest(path)}
                )
    inventory = root / "config/python-environment.json"
    write_json(
        inventory,
        {"schemaVersion": "flo154.python-environment.v1", "files": inventory_files},
    )

    components = [
        component(root, "analyzer-source", "analyzer_source", "lib/analyzer.py", "1.0.0", "LicenseRef-Project-Internal", "repo://tools/flo154-analyzer/analyzer.py"),
        component(root, "whisper-model", "asr_model", "models/faster-whisper-large-v3-turbo/model.bin", "0a363e9161cbc7ed1431c9597a8ceaf0c4f78fcf", "MIT", "https://huggingface.co/mobiuslabsgmbh/faster-whisper-large-v3-turbo"),
        component(root, "whisper-model-config", "asr_model", "models/faster-whisper-large-v3-turbo/config.json", "0a363e9161cbc7ed1431c9597a8ceaf0c4f78fcf", "MIT", "https://huggingface.co/mobiuslabsgmbh/faster-whisper-large-v3-turbo"),
        component(root, "whisper-tokenizer", "tokenizer", "models/faster-whisper-large-v3-turbo/tokenizer.json", "0a363e9161cbc7ed1431c9597a8ceaf0c4f78fcf", "MIT", "https://github.com/openai/whisper"),
        component(root, "whisper-vocabulary", "tokenizer", "models/faster-whisper-large-v3-turbo/vocabulary.json", "0a363e9161cbc7ed1431c9597a8ceaf0c4f78fcf", "MIT", "https://github.com/openai/whisper"),
        component(root, "flo154-normalizer", "normalizer", "config/normalizer.json", "1.0.0", "LicenseRef-Project-Internal", "repo://tools/flo154-analyzer/normalizer.json"),
        component(root, "silero-vad", "vad", "models/silero_vad_v6.onnx", "6", "MIT", "https://github.com/snakers4/silero-vad"),
        component(root, "opencv-haar-face", "face_mouth", "models/haarcascade_frontalface_default.xml", "4.12.0.88", "Apache-2.0", "https://github.com/opencv/opencv/tree/4.x/data/haarcascades"),
        component(root, "flo154-av-sync", "av_sync", "models/av-sync-correlation-v1.json", "1.0.0", "LicenseRef-Project-Internal", "repo://tools/flo154-analyzer/av-sync-correlation-v1.json"),
        component(root, "ffmpeg", "ffmpeg", "bin/ffmpeg", os.environ["FLO154_FFMPEG_VERSION"], "GPL-3.0-or-later", "https://ffmpeg.org/legal.html"),
        component(root, "ffprobe", "ffprobe", "bin/ffprobe", os.environ["FLO154_FFPROBE_VERSION"], "GPL-3.0-or-later", "https://ffmpeg.org/legal.html"),
        component(root, "license-snapshot", "license_snapshot", "licenses/snapshot.json", "1.0.0", "LicenseRef-Project-Internal", "repo://tools/flo154-analyzer/license-snapshot.json"),
        component(root, "dependency-lock", "dependency_lock", "config/requirements.lock", "1.0.0", "LicenseRef-Project-Internal", "repo://tools/flo154-analyzer/requirements.lock"),
        component(root, "python-environment", "python_environment", "config/python-environment.json", "1.0.0", "LicenseRef-Project-Internal", "repo://tools/flo154-analyzer/build.py"),
    ]
    seal = {
        "schemaVersion": "flo154.analyzer-seal.v1",
        "analyzer": {
            "path": "bin/flo154-analyzer",
            "sha256": digest(root / "bin/flo154-analyzer"),
            "version": "1.0.0",
            "executable": True,
        },
        "config": {
            "path": "config/analyzer.json",
            "sha256": digest(root / "config/analyzer.json"),
            "version": "1.0.0",
        },
        "components": components,
        "offline": {
            "network": "disabled",
            "commandSchema": "flo154.audio-analyzer-command.v1",
            "referenceTextProvided": False,
        },
    }
    write_json(root / "analyzer-seal.json", seal)
    print(digest(root / "analyzer-seal.json"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
