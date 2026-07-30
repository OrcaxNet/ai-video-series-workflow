import {
  AlertTriangle,
  Check,
  CheckCircle2,
  CircleDashed,
  Clock3,
  LockKeyhole,
  RefreshCw,
  RotateCcw,
  ShieldAlert,
  XCircle,
} from "lucide-react";
import type { GateState, JobState } from "../domain";

type Tone = "neutral" | "info" | "success" | "warning" | "danger";

const toneByGate: Record<GateState, Tone> = {
  PENDING: "warning",
  APPROVED: "success",
  RETURNED: "danger",
  BLOCKED: "neutral",
};

const gateLabels: Record<GateState, string> = {
  PENDING: "等待人工审核",
  APPROVED: "已批准并锁定",
  RETURNED: "已退回修改",
  BLOCKED: "上游未通过",
};

const toneByJob: Record<JobState, Tone> = {
  QUEUED: "neutral",
  RUNNING: "info",
  RETRYING: "warning",
  CANCEL_REQUESTED: "warning",
  CANCELLED: "neutral",
  FAILED: "danger",
  SUCCEEDED: "success",
  UNKNOWN: "warning",
  REQUIRES_ACTION: "danger",
};

const jobLabels: Record<JobState, string> = {
  QUEUED: "排队中",
  RUNNING: "生成中",
  RETRYING: "重试中",
  CANCEL_REQUESTED: "取消确认中",
  CANCELLED: "已取消",
  FAILED: "失败",
  SUCCEEDED: "已完成",
  UNKNOWN: "状态未知",
  REQUIRES_ACTION: "需要处理",
};

function StateIcon({ state }: { state: GateState | JobState }) {
  const props = { size: 13, strokeWidth: 2.2, "aria-hidden": true };
  if (state === "APPROVED" || state === "SUCCEEDED") return <CheckCircle2 {...props} />;
  if (state === "RETURNED" || state === "FAILED" || state === "REQUIRES_ACTION")
    return <XCircle {...props} />;
  if (state === "BLOCKED") return <LockKeyhole {...props} />;
  if (state === "RUNNING") return <RefreshCw className="spin-slow" {...props} />;
  if (state === "RETRYING" || state === "CANCEL_REQUESTED") return <RotateCcw {...props} />;
  if (state === "UNKNOWN") return <ShieldAlert {...props} />;
  if (state === "PENDING") return <Clock3 {...props} />;
  return <CircleDashed {...props} />;
}

export function GateBadge({ state }: { state: GateState }) {
  return (
    <span className={`badge badge-${toneByGate[state]}`}>
      <StateIcon state={state} />
      {gateLabels[state]}
    </span>
  );
}

export function JobBadge({ state }: { state: JobState }) {
  return (
    <span className={`badge badge-${toneByJob[state]}`}>
      <StateIcon state={state} />
      {jobLabels[state]}
    </span>
  );
}

export function EvidenceBadge({ evidence }: { evidence: "mock_only" | "pending_key" | "live_provider_call" }) {
  if (evidence === "mock_only") {
    return (
      <span className="evidence evidence-mock">
        <CircleDashed size={12} aria-hidden="true" />
        Mock 证据
      </span>
    );
  }
  if (evidence === "pending_key") {
    return (
      <span className="evidence evidence-pending">
        <AlertTriangle size={12} aria-hidden="true" />
        等待 Key 验证
      </span>
    );
  }
  return (
    <span className="evidence evidence-live">
      <Check size={12} aria-hidden="true" />
      真实调用证据
    </span>
  );
}

export function ProgressBar({ value, label }: { value: number; label?: string }) {
  return (
    <div className="progress-wrap">
      <div
        className="progress-track"
        role="progressbar"
        aria-valuenow={value}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-label={label ?? `进度 ${value}%`}
      >
        <span style={{ width: `${Math.max(0, Math.min(100, value))}%` }} />
      </div>
      <span className="progress-value">{value}%</span>
    </div>
  );
}

export function EmptyPanel({
  title,
  description,
  action,
}: {
  title: string;
  description: string;
  action?: React.ReactNode;
}) {
  return (
    <div className="empty-panel">
      <CircleDashed size={24} aria-hidden="true" />
      <strong>{title}</strong>
      <p>{description}</p>
      {action}
    </div>
  );
}
