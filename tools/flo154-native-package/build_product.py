#!/usr/bin/env python3
"""Derive the independent FLO-154 native-only product/safety package.

The original source and visual board remain immutable reusable inputs. All
database identities, safety evidence, shot/run identities, native-audio
settings, Scene Context ambience, and no-TTS authorization facts are new.
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import sys
from typing import Any


ISSUE_ID = "0e489bcd-01c0-4ec8-8429-7d04e4c7f40d"
BATCH_ID = "flo154-native-sample-1"
DECISION_COMMENT = "8ddff8b5-f3c1-46ec-84e8-66e3ef6592ad"


def canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def replace_namespace(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("10400000-", "15400000-")
    if isinstance(value, list):
        return [replace_namespace(item) for item in value]
    if isinstance(value, dict):
        return {key: replace_namespace(item) for key, item in value.items()}
    return value


def write_json(path: Path, value: Any) -> bytes:
    data = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    path.write_bytes(data)
    return data


def build_safety(template: dict[str, Any]) -> dict[str, Any]:
    safety = copy.deepcopy(template)
    safety.update(
        {
            "schemaVersion": "flo154.native-content-safety-evidence.v1",
            "issueId": ISSUE_ID,
            "batchId": BATCH_ID,
            "policyVersion": "flo154-native-sample-safety-v1",
            "reviewState": "APPROVED_FOR_NO_COST_MATERIALIZATION",
        }
    )
    safety["requiredPromptRestrictions"] = [
        "仅呈现成年虚构角色林晚和拟人化机械萤灯，不得生成其他可辨识人物",
        "不得生成文字、字幕、标识、品牌、水印或签名；字幕由后期统一烧录",
        "不得生成武器、伤害、血腥、危险模仿动作或现实人物相貌",
        "不得引入现有影视、游戏、动漫或文学作品的受保护角色与视觉标识",
        "只允许 Provider 原生音轨，禁用 Speech/TTS route、VOICE asset 与 fallback",
    ]
    safety["conditions"] = [
        "审批绑定本证据文件、全新的 FLO-154 episode revision 与 10 个 shot spec revision",
        "真实 Provider 授权已撤销；本阶段 materialize/preflight 的 Provider/TTS submit 必须为 0",
        "未来 Provider submit 前必须由 QA 与深度研究Agent重新绑定 code/package/analyzer hashes",
    ]
    safety["visualAssetEvidence"]["generatedForThisIssue"] = False
    safety["visualAssetEvidence"]["reusedFromIssue"] = "FLO-104"
    safety["conclusion"] = "APPROVE_NO_COST_MATERIALIZATION_ONLY"
    return safety


def build_product(template: dict[str, Any], safety_bytes: bytes) -> dict[str, Any]:
    product = replace_namespace(copy.deepcopy(template))
    product.update(
        {
            "schemaVersion": "flo154.native-sample.v1",
            "issueId": ISSUE_ID,
            "batchId": BATCH_ID,
            "recordState": "FROZEN_FOR_NO_COST_QA",
        }
    )
    reserved = product["reservedIds"]
    for key in (
        "voiceAssetId",
        "voiceAssetVersionId",
        "voiceLicenseSnapshotId",
        "speechBudgetApprovalId",
        "speechProviderProfileId",
    ):
        reserved.pop(key, None)
    product["generationProfile"].pop("speechRoute", None)
    product["generationProfile"]["videoRoute"]["routeVersion"] = "agent-plan-large-flo154-native-v1"
    product["reusableAssets"] = [
        asset for asset in product["reusableAssets"] if asset["assetType"] == "IMAGE"
    ]
    product["reusableAssets"][0]["source"]["generatedForThisIssue"] = False
    product["reusableAssets"][0]["source"]["reusedFromIssue"] = "FLO-104"
    product["licenseSnapshots"] = [
        license_ for license_ in product["licenseSnapshots"] if license_["subjectType"] == "ASSET"
    ]
    product["sharedPrompt"]["assetVersionRefs"] = [reserved["visualAssetVersionId"]]
    product["sharedPrompt"]["output"].update(
        {
            "generateAudio": True,
            "audioStrategy": "native_preferred",
            "audioDelivery": "native_mix",
        }
    )
    product["generationPlan"].pop("speechBudgetApprovalId", None)
    product["generationPlan"]["contentSafetyPolicyVersion"] = "flo154-native-sample-safety-v1"
    post = product["postProduction"]
    for key in (
        "speechProviderProfileId",
        "speechBudgetApprovalId",
        "speechBudgetMaximumMicros",
        "speechBudgetCurrency",
        "speaker",
        "voiceAssetVersionId",
    ):
        post.pop(key, None)
    post.update(
        {
            "traceId": "flo154-native-sample1-preflight-v1",
            "independentDialogueTrack": False,
            "audioStrategy": "native_preferred",
            "audioDelivery": "native_mix",
            "generateAudio": True,
        }
    )
    post["qualityThresholds"].update(
        {
            "lipSyncP95MillisMaximum": 80,
            "ambienceHardSilenceMillisMaximum": 250,
            "integratedLufs": -16,
            "integratedLufsTolerance": 1,
            "truePeakDbtpMaximum": -1,
        }
    )
    product["contentSafetyEvidence"].update(
        {
            "filename": "flo154-native-sample1-safety-evidence-v1.json",
            "sha256": digest(safety_bytes),
            "bytes": len(safety_bytes),
            "artifactUriAfterIngest": "cas://sha256/" + digest(safety_bytes),
            "policyVersion": "flo154-native-sample-safety-v1",
        }
    )
    approvals = []
    for approval in product["approvalsToMaterialize"]:
        if approval.get("budgetScope") in {"SPEECH", "VIDEO"}:
            continue
        if approval.get("gate") == "SAFETY":
            approval["policyVersion"] = "flo154-native-sample-safety-v1"
        approvals.append(approval)
    product["approvalsToMaterialize"] = approvals
    product["authorizationBoundary"].update(
        {
            "decisionCommentId": DECISION_COMMENT,
            "liveAuthorizationState": "REVOKED_PENDING_NEW_QA_AND_AUTHORIZATION",
            "authorizedProviderSubmitsNow": 0,
            "authorizedTtsSubmitsNow": 0,
            "maximumTtsAfpMilli": 0,
        }
    )
    product["dialogueSummary"]["ttsAfpMilliEstimate"] = 0
    product["requiredNextChecks"] = [
        "后端使用独立 FLO-154 native materializer 落库/CAS，不构造 Provider client",
        "后端封印 analyzer executable/model/config/license hashes 并运行无参考字幕离线 fixture",
        "预检证明 reservation/provider_job/cost/provider/TTS calls 全部为 0",
        "QA 仅离线复验新 HEAD/package/analyzer hashes；通过后返回深度研究Agent重新授权",
    ]
    product["materializationRule"] = (
        "全新 FLO-154 UUID、shot/run/package/trace 原样进入独立 namespace；复用源文与视觉资产仅按 SHA-256 回链。"
        "本阶段不得创建 Provider job、budget reservation、cost ledger 或 Speech/TTS 事实。"
    )

    lip_shots = {2, 4, 6, 9}
    first_ambience = {"identity": "rainy-stone-bridge", "version": "ambience-rain-bridge-v1"}
    second_ambience = {"identity": "navigation-tower-machinery", "version": "ambience-tower-gears-v1"}
    for index, shot in enumerate(product["shots"], start=1):
        ambience = first_ambience if index <= 5 else second_ambience
        shot["ambience"] = {
            **ambience,
            "continuityIntoNext": index in {1, 2, 3, 4, 6, 7, 8, 9},
        }
        for dialogue in shot["narrative"]["dialogue"]:
            dialogue.pop("voiceRef", None)
            dialogue["speaker"] = "lin-wan" if index in lip_shots else "river-narrator"
        if index in lip_shots:
            shot["cinematography"].update(
                {
                    "shotSize": "frontal-close-up",
                    "framing": "frontal",
                    "lipSyncRequired": True,
                }
            )
            shot["positivePrompt"] += " 正面近景保持林晚面部与嘴部清晰可见，她在画面内自然说出本镜台词，音画同生并保持准确口型。"
        else:
            shot["cinematography"]["lipSyncRequired"] = False
        if index == 8:
            shot["narrative"]["dialogue"][0]["speaker"] = "mechanical-firefly"
        if index == 9:
            shot["narrative"]["dialogue"] = [
                {
                    "id": "cue-009",
                    "speaker": "lin-wan",
                    "text": "今夜，雨会停。",
                    "startMillis": 900,
                    "endMillis": 2800,
                }
            ]
        if index == 10:
            shot["narrative"]["dialogue"] = []
        shot_material = {
            key: shot[key]
            for key in ("timeline", "narrative", "cinematography", "continuity", "ambience")
        }
        shot["shotSpecContentHash"] = digest(canonical(shot_material))
        shot["promptSnapshotContentHash"] = digest(
            canonical(
                {
                    "positivePrompt": shot["positivePrompt"],
                    "sharedPrompt": product["sharedPrompt"],
                }
            )
        )
        shot["runSpecDigest"] = digest(
            canonical(
                {
                    "runId": shot["runId"],
                    "shotSpecContentHash": shot["shotSpecContentHash"],
                    "promptSnapshotContentHash": shot["promptSnapshotContentHash"],
                    "generationProfileRevisionId": reserved["generationProfileRevisionId"],
                }
            )
        )

    dialogue = [
        cue
        for shot in product["shots"]
        for cue in shot["narrative"]["dialogue"]
    ]
    product["dialogueSummary"].update(
        {
            "cueCount": len(dialogue),
            "text": "".join(cue["text"] for cue in dialogue),
            "unicodeCharacterCount": len("".join(cue["text"] for cue in dialogue)),
        }
    )
    return product


def main() -> int:
    if len(sys.argv) != 4:
        print("usage: build_product.py FLO104_PRODUCT FLO104_SAFETY OUTPUT_DIR", file=sys.stderr)
        return 2
    product_template = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    safety_template = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
    output = Path(sys.argv[3]).resolve()
    output.mkdir(parents=True, exist_ok=False)
    safety = build_safety(safety_template)
    safety_bytes = write_json(output / "flo154-native-sample1-safety-evidence-v1.json", safety)
    product = build_product(product_template, safety_bytes)
    write_json(output / "flo154-native-sample1-product-input-v1.json", product)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
