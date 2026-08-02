#!/usr/bin/env python3
"""Build and run a local, Provider-free analyzer fixture on macOS."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess
import sys


ASR = {
    "modelId": "mobiuslabsgmbh/faster-whisper-large-v3-turbo",
    "modelRevision": "0a363e9161cbc7ed1431c9597a8ceaf0c4f78fcf",
    "fasterWhisperVersion": "1.2.1",
    "device": "cpu",
    "computeType": "int8",
    "language": "zh",
    "beamSize": 5,
    "vadFilter": True,
    "referencePrompt": None,
    "projectDictionary": False,
    "postHocAllowlist": False,
}


def run(args: list[str]) -> None:
    subprocess.run(args, check=True)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            value.update(block)
    return value.hexdigest()


def media(path: Path, duration: int) -> dict[str, object]:
    return {"path": str(path.resolve()), "sha256": digest(path), "durationMillis": duration}


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: fixture.py ANALYZER_ROOT OUTPUT_DIR", file=sys.stderr)
        return 2
    root, output = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()
    if output.exists():
        raise ValueError("fixture output directory must not exist")
    output.mkdir(parents=True)
    ffmpeg = str(root / "bin/ffmpeg")
    analyzer = str(root / "bin/flo154-analyzer")
    first_aiff, second_aiff = output / "first.aiff", output / "second.aiff"
    run(["/usr/bin/say", "-v", "Tingting", "第一场风吹过旧城", "-o", str(first_aiff)])
    run(["/usr/bin/say", "-v", "Tingting", "第二场雨落在窗前", "-o", str(second_aiff)])
    native_paths = [output / "native-001.wav", output / "native-002.wav"]
    for source, target in zip((first_aiff, second_aiff), native_paths, strict=True):
        run(
            [
                ffmpeg,
                "-nostdin",
                "-v",
                "error",
                "-i",
                str(source),
                "-af",
                "loudnorm=I=-16:TP=-1:LRA=11,apad=pad_dur=4,atrim=duration=4",
                "-ar",
                "48000",
                "-ac",
                "2",
                str(target),
            ]
        )
    final_mix = output / "final-mix.wav"
    run(
        [
            ffmpeg,
            "-nostdin",
            "-v",
            "error",
            "-i",
            str(native_paths[0]),
            "-i",
            str(native_paths[1]),
            "-filter_complex",
            "[0:a][1:a]concat=n=2:v=0:a=1,loudnorm=I=-16:TP=-1:LRA=11[a]",
            "-map",
            "[a]",
            "-ar",
            "48000",
            "-ac",
            "2",
            str(final_mix),
        ]
    )
    final_video = output / "final-video.mp4"
    run(
        [
            ffmpeg,
            "-nostdin",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "color=c=0x203040:s=1280x720:r=24:d=8",
            "-i",
            str(final_mix),
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-t",
            "8",
            "-movflags",
            "+faststart",
            str(final_video),
        ]
    )
    input_value = {
        "schemaVersion": "flo154.audio-analyzer-command.v1",
        "evidence": "offline_fixture",
        "asr": ASR,
        "finalMix": media(final_mix, 8000),
        "finalVideo": media(final_video, 8000),
        "nativeMixes": [media(path, 4000) for path in native_paths],
        "cueWindows": [
            {
                "cueId": "fixture-cue-1",
                "startMillis": 0,
                "endMillis": 4000,
                "lipSyncRunId": "fixture-run-1",
                "lipSyncStartMillis": 0,
                "lipSyncEndMillis": 4000,
                "lipSyncRequired": False,
            },
            {
                "cueId": "fixture-cue-2",
                "startMillis": 4000,
                "endMillis": 8000,
                "lipSyncRunId": "fixture-run-2",
                "lipSyncStartMillis": 4000,
                "lipSyncEndMillis": 8000,
                "lipSyncRequired": False,
            },
        ],
        "runWindows": [
            {
                "runId": "fixture-run-1",
                "startMillis": 0,
                "endMillis": 4000,
                "contextSnapshotId": "fixture-context-1",
                "contextSnapshotHash": "1" * 64,
                "ambienceIdentity": "fixture-old-city-wind",
                "ambienceVersion": "v1",
                "continuityIntoNext": True,
                "lipSyncRequired": False,
            },
            {
                "runId": "fixture-run-2",
                "startMillis": 4000,
                "endMillis": 8000,
                "contextSnapshotId": "fixture-context-2",
                "contextSnapshotHash": "2" * 64,
                "ambienceIdentity": "fixture-window-rain",
                "ambienceVersion": "v1",
                "continuityIntoNext": False,
                "lipSyncRequired": False,
            },
        ],
    }
    input_path, analysis_path = output / "input.json", output / "analysis.json"
    input_path.write_text(
        json.dumps(input_value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    run([analyzer, str(input_path), str(analysis_path)])
    report = json.loads(analysis_path.read_text(encoding="utf-8"))
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
