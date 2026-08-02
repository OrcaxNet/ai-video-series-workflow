#!/usr/bin/env python3
"""Frozen, local-only FLO-154 native-audio analyzer.

The command accepts exactly ``input.json output.json``.  It uses the frozen
faster-whisper model for ASR/VAD, OpenCV's production-licensed face detector
for mouth-region motion, a sealed cross-correlation model for AV sync, and
FFmpeg/FFprobe for media and EBU R128 measurements.  Reference subtitle text
and network access are deliberately absent.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
from typing import Any

os.environ.update(
    {
        "HF_HUB_OFFLINE": "1",
        "TRANSFORMERS_OFFLINE": "1",
        "HF_DATASETS_OFFLINE": "1",
        "NO_PROXY": "*",
    }
)


def _network_disabled(*_args: Any, **_kwargs: Any) -> Any:
    raise RuntimeError("FLO-154 analyzer network is disabled")


import cv2  # type: ignore[import-not-found]  # noqa: E402
from faster_whisper import WhisperModel  # type: ignore[import-not-found]  # noqa: E402
import numpy as np  # type: ignore[import-not-found]  # noqa: E402

# Dependencies are imported under explicit offline environment variables;
# disable every subsequent Python network connection before any model loads.
socket.socket = _network_disabled  # type: ignore[assignment]
socket.create_connection = _network_disabled  # type: ignore[assignment]


SCHEMA = "flo154.audio-analyzer-command.v1"
OUTPUT_SCHEMA = "flo154.audio-analysis.v1"
CONFIG_SCHEMA = "flo154.audio-analyzer-config.v1"
EXPECTED_ASR = {
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

MEDIA_KEYS = {"path", "sha256", "durationMillis"}
CUE_KEYS = {
    "cueId",
    "startMillis",
    "endMillis",
    "lipSyncRunId",
    "lipSyncStartMillis",
    "lipSyncEndMillis",
    "lipSyncRequired",
}
RUN_KEYS = {
    "runId",
    "startMillis",
    "endMillis",
    "contextSnapshotId",
    "contextSnapshotHash",
    "ambienceIdentity",
    "ambienceVersion",
    "continuityIntoNext",
    "lipSyncRequired",
}
INPUT_KEYS = {
    "schemaVersion",
    "evidence",
    "asr",
    "finalMix",
    "finalVideo",
    "nativeMixes",
    "dialogue",
    "cueWindows",
    "runWindows",
}


def fail(message: str) -> None:
    raise ValueError(message)


def require_keys(value: dict[str, Any], allowed: set[str], required: set[str], name: str) -> None:
    unknown = set(value) - allowed
    missing = required - set(value)
    if unknown or missing:
        fail(f"{name} keys invalid: missing={sorted(missing)} unknown={sorted(unknown)}")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def validate_media(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{name} must be an object")
    require_keys(value, MEDIA_KEYS, MEDIA_KEYS, name)
    path = Path(value["path"])
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        fail(f"{name} path must be an absolute regular non-symlink file")
    if not isinstance(value["durationMillis"], int) or value["durationMillis"] <= 0:
        fail(f"{name} durationMillis must be positive")
    if not re.fullmatch(r"[0-9a-f]{64}", str(value["sha256"])):
        fail(f"{name} SHA-256 is invalid")
    if sha256_file(path) != value["sha256"]:
        fail(f"{name} SHA-256 drifted")
    return value


def load_input(path: Path) -> tuple[dict[str, Any], bytes]:
    raw = path.read_bytes()
    if not raw or len(raw) > 4 * 1024 * 1024:
        fail("input size is invalid")
    value = json.loads(raw)
    if not isinstance(value, dict):
        fail("input must be one JSON object")
    require_keys(value, INPUT_KEYS, INPUT_KEYS - {"dialogue"}, "input")
    if value["schemaVersion"] != SCHEMA or not str(value["evidence"]).strip():
        fail("input schemaVersion or evidence is invalid")
    if value["asr"] != EXPECTED_ASR:
        fail("ASR configuration differs from the frozen FLO-104 protocol")
    validate_media(value["finalMix"], "finalMix")
    validate_media(value["finalVideo"], "finalVideo")
    if not isinstance(value["nativeMixes"], list) or not value["nativeMixes"]:
        fail("nativeMixes must be non-empty")
    for index, media in enumerate(value["nativeMixes"]):
        validate_media(media, f"nativeMixes[{index}]")
    if value.get("dialogue") is not None:
        validate_media(value["dialogue"], "dialogue")
    if not isinstance(value["cueWindows"], list) or not value["cueWindows"]:
        fail("cueWindows must be non-empty")
    cue_ids: set[str] = set()
    for index, cue in enumerate(value["cueWindows"]):
        if not isinstance(cue, dict):
            fail(f"cueWindows[{index}] must be an object")
        require_keys(cue, CUE_KEYS, CUE_KEYS, f"cueWindows[{index}]")
        if not str(cue["cueId"]).strip() or cue["cueId"] in cue_ids:
            fail("cue IDs must be unique and non-empty")
        cue_ids.add(cue["cueId"])
        if not (0 <= cue["startMillis"] < cue["endMillis"]):
            fail("cue timeline is invalid")
        if not (
            cue["startMillis"] <= cue["lipSyncStartMillis"]
            < cue["lipSyncEndMillis"] <= cue["endMillis"]
        ):
            fail("cue lip-sync window is invalid")
    if not isinstance(value["runWindows"], list) or len(value["runWindows"]) != len(value["nativeMixes"]):
        fail("runWindows must match nativeMixes")
    previous_end = 0
    run_ids: set[str] = set()
    for index, run in enumerate(value["runWindows"]):
        if not isinstance(run, dict):
            fail(f"runWindows[{index}] must be an object")
        require_keys(run, RUN_KEYS, RUN_KEYS, f"runWindows[{index}]")
        if run["runId"] in run_ids or run["startMillis"] != previous_end or run["endMillis"] <= previous_end:
            fail("run timeline must be unique, ordered, and contiguous")
        run_ids.add(run["runId"])
        previous_end = run["endMillis"]
    if previous_end != value["finalMix"]["durationMillis"] or previous_end != value["finalVideo"]["durationMillis"]:
        fail("run timeline differs from final media duration")
    return value, raw


def root_path() -> Path:
    configured = os.environ.get("FLO154_ANALYZER_ROOT", "")
    return Path(configured).resolve() if configured else Path(__file__).resolve().parent.parent


def load_config(root: Path) -> dict[str, Any]:
    config = json.loads((root / "config/analyzer.json").read_text(encoding="utf-8"))
    expected = {
        "schemaVersion",
        "name",
        "version",
        "modelPath",
        "faceCascadePath",
        "avSyncModelPath",
        "ffmpegPath",
        "ffprobePath",
        "environmentInventoryPath",
        "mouthSampleFps",
        "mouthMotionThreshold",
        "silenceDbfs",
    }
    require_keys(config, expected, expected, "config")
    if config["schemaVersion"] != CONFIG_SCHEMA:
        fail("analyzer config schemaVersion is invalid")
    for key in ("modelPath", "faceCascadePath", "avSyncModelPath", "ffmpegPath", "ffprobePath", "environmentInventoryPath"):
        candidate = (root / config[key]).resolve()
        expected_type = candidate.is_dir() if key == "modelPath" else candidate.is_file()
        if root not in candidate.parents or not expected_type:
            fail(f"config {key} escapes or is missing")
        config[key] = str(candidate)
    return config


def verify_environment(root: Path, inventory_path: str) -> None:
    inventory = json.loads(Path(inventory_path).read_text(encoding="utf-8"))
    if inventory.get("schemaVersion") != "flo154.python-environment.v1":
        fail("Python environment inventory schemaVersion is invalid")
    files = inventory.get("files")
    if not isinstance(files, list) or not files:
        fail("Python environment inventory is empty")
    for item in files:
        if not isinstance(item, dict) or set(item) != {"path", "sha256"}:
            fail("Python environment inventory entry is invalid")
        candidate = (root / str(item["path"])).resolve()
        if root not in candidate.parents or candidate.is_symlink() or not candidate.is_file():
            fail("Python environment inventory path escapes, is linked, or is missing")
        if sha256_file(candidate) != item["sha256"]:
            fail(f"Python environment file drifted: {item['path']}")


def run_tool(program: str, args: list[str]) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run([program, *args], check=False, capture_output=True)
    if result.returncode != 0:
        fail(f"tool {Path(program).name} failed: {result.stderr[-2000:].decode(errors='replace')}")
    return result


def decode_audio(config: dict[str, Any], path: str) -> tuple[np.ndarray, int]:
    sample_rate = 16_000
    result = run_tool(
        config["ffmpegPath"],
        ["-nostdin", "-v", "error", "-i", path, "-ac", "1", "-ar", str(sample_rate), "-f", "f32le", "-"],
    )
    samples = np.frombuffer(result.stdout, dtype="<f4").copy()
    if samples.size == 0 or not np.all(np.isfinite(samples)):
        fail("decoded final audio is empty or invalid")
    return samples, sample_rate


def transcribe(config: dict[str, Any], input_value: dict[str, Any]) -> tuple[str, list[tuple[int, int, str]]]:
    model = WhisperModel(
        config["modelPath"],
        device=input_value["asr"]["device"],
        compute_type=input_value["asr"]["computeType"],
        local_files_only=True,
    )
    segments, _info = model.transcribe(
        input_value["finalMix"]["path"],
        language=input_value["asr"]["language"],
        beam_size=input_value["asr"]["beamSize"],
        vad_filter=input_value["asr"]["vadFilter"],
        word_timestamps=True,
        condition_on_previous_text=False,
        initial_prompt=None,
    )
    words: list[tuple[int, int, str]] = []
    transcript: list[str] = []
    for segment in segments:
        transcript.append(segment.text.strip())
        for word in segment.words or []:
            text = word.word.strip()
            if text and word.start is not None and word.end is not None and word.end > word.start:
                words.append((round(word.start * 1000), round(word.end * 1000), text))
    joined = "".join(transcript).strip()
    if not joined or not words:
        fail("frozen ASR produced no transcript or word timestamps")
    return joined, words


def cue_speech(cue: dict[str, Any], words: list[tuple[int, int, str]]) -> tuple[int, int]:
    start_bound, end_bound = cue["lipSyncStartMillis"], cue["lipSyncEndMillis"]
    selected = [(start, end) for start, end, _text in words if min(end, end_bound) > max(start, start_bound)]
    if not selected:
        fail(f"ASR/VAD found no speech in cue {cue['cueId']}")
    start = max(start_bound, min(item[0] for item in selected))
    end = min(end_bound, max(item[1] for item in selected))
    if end <= start:
        fail(f"ASR/VAD speech interval is invalid for cue {cue['cueId']}")
    return start, end


def mouth_envelope(
    config: dict[str, Any],
    video_path: str,
    start_ms: int,
    end_ms: int,
) -> tuple[np.ndarray, float]:
    cascade = cv2.CascadeClassifier(config["faceCascadePath"])
    if cascade.empty():
        fail("sealed face cascade could not be loaded")
    capture = cv2.VideoCapture(video_path)
    if not capture.isOpened():
        fail("final video could not be opened")
    fps = float(capture.get(cv2.CAP_PROP_FPS))
    if not math.isfinite(fps) or fps <= 0:
        capture.release()
        fail("final video FPS is invalid")
    sample_fps = min(float(config["mouthSampleFps"]), fps)
    step_ms = 1000.0 / sample_fps
    values: list[float] = []
    previous: np.ndarray | None = None
    last_face: tuple[int, int, int, int] | None = None
    timestamp = float(start_ms)
    while timestamp <= end_ms:
        capture.set(cv2.CAP_PROP_POS_MSEC, timestamp)
        ok, frame = capture.read()
        if not ok:
            break
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        faces = cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=4, minSize=(48, 48))
        if len(faces):
            last_face = max((tuple(map(int, face)) for face in faces), key=lambda item: item[2] * item[3])
        if last_face is None:
            values.append(0.0)
            timestamp += step_ms
            continue
        x, y, width, height = last_face
        mouth = gray[y + height * 3 // 5 : y + height, x + width // 5 : x + width * 4 // 5]
        if mouth.size == 0:
            values.append(0.0)
        else:
            mouth = cv2.resize(mouth, (96, 48), interpolation=cv2.INTER_AREA)
            mouth = cv2.GaussianBlur(mouth, (5, 5), 0)
            if previous is None:
                values.append(0.0)
            else:
                values.append(float(np.mean(cv2.absdiff(mouth, previous))) / 255.0)
            previous = mouth
        timestamp += step_ms
    capture.release()
    envelope = np.asarray(values, dtype=np.float64)
    if envelope.size < 3 or not np.any(envelope > float(config["mouthMotionThreshold"])):
        fail("required face/mouth motion was not detected")
    return envelope, step_ms


def audio_envelope(samples: np.ndarray, sample_rate: int, start_ms: int, frames: int, step_ms: float) -> np.ndarray:
    values: list[float] = []
    for index in range(frames):
        left = round((start_ms + index * step_ms) * sample_rate / 1000)
        right = round((start_ms + (index + 1) * step_ms) * sample_rate / 1000)
        window = samples[max(0, left) : max(left + 1, right)]
        values.append(float(np.sqrt(np.mean(np.square(window)))) if window.size else 0.0)
    return np.asarray(values, dtype=np.float64)


def av_lag_millis(audio: np.ndarray, mouth: np.ndarray, step_ms: float, model_path: str) -> int:
    model = json.loads(Path(model_path).read_text(encoding="utf-8"))
    if model.get("schemaVersion") != "flo154.av-sync-correlation.v1":
        fail("sealed AV-sync model schemaVersion is invalid")
    maximum = int(round(float(model["maximumLagMillis"]) / step_ms))
    audio = (audio - audio.mean()) / (audio.std() + 1e-9)
    mouth = (mouth - mouth.mean()) / (mouth.std() + 1e-9)
    best_lag, best_score = 0, -math.inf
    for lag in range(-maximum, maximum + 1):
        if lag < 0:
            left, right = audio[-lag:], mouth[: len(mouth) + lag]
        elif lag > 0:
            left, right = audio[: len(audio) - lag], mouth[lag:]
        else:
            left, right = audio, mouth
        if len(left) < 3:
            continue
        score = float(np.dot(left, right) / len(left))
        if score > best_score:
            best_lag, best_score = lag, score
    if not math.isfinite(best_score) or best_score < float(model["minimumCorrelation"]):
        fail("mouth/audio correlation is below the sealed AV-sync confidence threshold")
    return round(best_lag * step_ms)


def longest_silence_ms(samples: np.ndarray, sample_rate: int, center_ms: int, threshold_dbfs: float) -> int:
    half_window = 300
    start = max(0, round((center_ms - half_window) * sample_rate / 1000))
    end = min(len(samples), round((center_ms + half_window) * sample_rate / 1000))
    frame = max(1, sample_rate // 100)
    longest = current = 0
    threshold = 10.0 ** (threshold_dbfs / 20.0)
    for offset in range(start, end, frame):
        window = samples[offset : min(offset + frame, end)]
        rms = float(np.sqrt(np.mean(np.square(window)))) if window.size else 0.0
        if rms <= threshold:
            current += round(1000 * len(window) / sample_rate)
            longest = max(longest, current)
        else:
            current = 0
    return longest


def window_db(samples: np.ndarray, sample_rate: int, start_ms: int, end_ms: int) -> float:
    start = max(0, round(start_ms * sample_rate / 1000))
    end = min(len(samples), round(end_ms * sample_rate / 1000))
    window = samples[start:end]
    if window.size == 0:
        fail("ambience loudness window is empty")
    rms = max(float(np.sqrt(np.mean(np.square(window)))), 1e-12)
    return 20.0 * math.log10(rms)


def loudness(config: dict[str, Any], path: str) -> tuple[float, float]:
    result = run_tool(
        config["ffmpegPath"],
        ["-nostdin", "-hide_banner", "-i", path, "-af", "loudnorm=I=-16:TP=-1:LRA=11:print_format=json", "-f", "null", "-"],
    )
    stderr = result.stderr.decode(errors="replace")
    matches = re.findall(r"\{\s*\"input_i\".*?\}", stderr, flags=re.DOTALL)
    if not matches:
        fail("FFmpeg did not emit EBU R128 measurements")
    report = json.loads(matches[-1])
    integrated, peak = float(report["input_i"]), float(report["input_tp"])
    if not math.isfinite(integrated) or not math.isfinite(peak):
        fail("FFmpeg loudness measurements are invalid")
    return integrated, peak


def stream_start_ms(config: dict[str, Any], path: str, selector: str) -> int:
    result = run_tool(
        config["ffprobePath"],
        ["-v", "error", "-select_streams", selector, "-show_entries", "stream=start_time", "-of", "json", path],
    )
    value = json.loads(result.stdout)
    streams = value.get("streams", [])
    if not streams:
        fail(f"media lacks {selector} stream")
    return round(float(streams[0].get("start_time", 0)) * 1000)


def analyze(input_value: dict[str, Any], input_raw: bytes, config: dict[str, Any]) -> dict[str, Any]:
    transcript, words = transcribe(config, input_value)
    samples, sample_rate = decode_audio(config, input_value["finalMix"]["path"])
    cue_timings: list[dict[str, Any]] = []
    lip_sync: list[dict[str, Any]] = []
    for cue in input_value["cueWindows"]:
        audio_start, audio_end = cue_speech(cue, words)
        mouth_start, mouth_end = audio_start, audio_end
        if cue["lipSyncRequired"]:
            mouth, step_ms = mouth_envelope(
                config,
                input_value["finalVideo"]["path"],
                cue["lipSyncStartMillis"],
                cue["lipSyncEndMillis"],
            )
            audio = audio_envelope(samples, sample_rate, cue["lipSyncStartMillis"], len(mouth), step_ms)
            lag = av_lag_millis(audio, mouth, step_ms, config["avSyncModelPath"])
            mouth_start = max(cue["lipSyncStartMillis"], min(audio_start + lag, cue["lipSyncEndMillis"] - 1))
            mouth_end = max(mouth_start + 1, min(audio_end + lag, cue["lipSyncEndMillis"]))
        cue_timings.append(
            {"cueId": cue["cueId"], "speechStartMillis": audio_start, "speechEndMillis": audio_end}
        )
        lip_sync.append(
            {
                "runId": cue["lipSyncRunId"],
                "cueId": cue["cueId"],
                "required": cue["lipSyncRequired"],
                "audioStartMillis": audio_start,
                "audioEndMillis": audio_end,
                "mouthStartMillis": mouth_start,
                "mouthEndMillis": mouth_end,
            }
        )

    transitions: list[dict[str, Any]] = []
    runs = input_value["runWindows"]
    for index in range(len(runs) - 1):
        before, after = runs[index], runs[index + 1]
        window = 400
        before_db = window_db(samples, sample_rate, max(before["startMillis"], before["endMillis"] - window), before["endMillis"])
        after_db = window_db(samples, sample_rate, after["startMillis"], min(after["endMillis"], after["startMillis"] + window))
        transitions.append(
            {
                "fromRunId": before["runId"],
                "toRunId": after["runId"],
                "continuityRequired": before["continuityIntoNext"],
                "fromIdentity": before["ambienceIdentity"],
                "fromVersion": before["ambienceVersion"],
                "toIdentity": after["ambienceIdentity"],
                "toVersion": after["ambienceVersion"],
                "hardSilenceMillis": longest_silence_ms(
                    samples, sample_rate, before["endMillis"], float(config["silenceDbfs"])
                ),
                "loudnessDeltaLufs": abs(before_db - after_db),
            }
        )

    final_video_start = stream_start_ms(config, input_value["finalVideo"]["path"], "v:0")
    av_starts = [
        stream_start_ms(config, media["path"], "a:0") - final_video_start
        for media in input_value["nativeMixes"]
    ]
    integrated, peak = loudness(config, input_value["finalMix"]["path"])
    sources = [input_value["finalMix"]["sha256"], input_value["finalVideo"]["sha256"]]
    sources.extend(media["sha256"] for media in input_value["nativeMixes"])
    if input_value.get("dialogue") is not None:
        sources.append(input_value["dialogue"]["sha256"])
    identity = hashlib.sha256(input_raw + config["version"].encode()).hexdigest()[:24]
    return {
        "schemaVersion": OUTPUT_SCHEMA,
        "analysisId": f"flo154-{identity}",
        "analyzer": config["name"],
        "analyzerVersion": config["version"],
        "evidence": input_value["evidence"],
        "asr": input_value["asr"],
        "transcript": transcript,
        "sourceHashes": sources,
        "cueTimings": cue_timings,
        "lipSync": lip_sync,
        "ambienceTransitions": transitions,
        "audioVideoStartMillis": av_starts,
        "integratedLufs": integrated,
        "truePeakDbtp": peak,
        "contentHash": "",
    }


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: flo154-analyzer input.json output.json", file=sys.stderr)
        return 2
    input_path, output_path = Path(sys.argv[1]), Path(sys.argv[2])
    if not input_path.is_absolute() or not output_path.is_absolute() or output_path.exists():
        fail("input/output paths must be absolute and output must not exist")
    input_value, input_raw = load_input(input_path)
    root = root_path()
    config = load_config(root)
    verify_environment(root, config["environmentInventoryPath"])
    output = analyze(input_value, input_raw, config)
    encoded = json.dumps(output, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8") + b"\n"
    descriptor = os.open(output_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as target:
        target.write(encoded)
        target.flush()
        os.fsync(target.fileno())
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # fail closed without leaking environment or secrets
        print(f"FLO-154 analyzer failed: {error}", file=sys.stderr)
        raise SystemExit(1)
