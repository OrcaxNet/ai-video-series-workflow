import { ChevronDown, History, LockKeyhole, MessageSquareText, RefreshCw } from "lucide-react";
import { useId, useState } from "react";
import type { Decision, Gate } from "../domain";
import { useStudio } from "../studio-store";
import { GateBadge } from "./ui";

export function GateCard({ gate, compact = false }: { gate: Gate; compact?: boolean }) {
  const { state, actions } = useStudio();
  const [expanded, setExpanded] = useState(false);
  const [explanation, setExplanation] = useState("");
  const noteId = useId();
  const upstreamBlocked = gate.state === "BLOCKED";
  const staleDependency =
    gate.id === "G2" && !upstreamBlocked && state.shots.some((shot) => shot.state === "STALE");
  const blocked = upstreamBlocked || staleDependency;
  const terminal = gate.state === "APPROVED";
  const conflictRevision = state.lastProblem?.affectedObjects.find(
    (object) => object.objectType === "GATE" && object.objectId === gate.id,
  )?.currentRevision;

  const decide = (decision: Decision) => {
    void actions.decideGate(gate.id, decision, explanation.trim());
  };

  return (
    <article className={`gate-card gate-${gate.state.toLowerCase()} ${compact ? "gate-compact" : ""}`}>
      <div className="gate-index" aria-hidden="true">
        {gate.id}
      </div>
      <div className="gate-body">
        <div className="gate-heading">
          <div>
            <span className="eyebrow">{gate.eyebrow}</span>
            <h3>{gate.title}</h3>
          </div>
          <GateBadge state={gate.state} />
        </div>
        <p className="gate-description">{gate.description}</p>

        <div className="binding-strip" aria-label={`${gate.id} 锁定内容`}>
          {gate.bindings.map((item) => (
            <span className="binding-pill" key={item.revisionId} title={item.contentHash}>
              <LockKeyhole size={12} aria-hidden="true" />
              {item.label}
            </span>
          ))}
        </div>

        {!compact && (
          <div className="gate-meta">
            <span>revision {gate.revision}</span>
            <span>ETag “{gate.etag}”</span>
            <span>{gate.bindings.length} 个精确绑定</span>
          </div>
        )}

        {state.lastProblem?.errorCode === "REVISION_CONFLICT" && (
          <div className="gate-conflict-recovery" role="alert">
            <div>
              <strong>本地 ETag 已过期</strong>
              <p>
                {state.lastProblem.message}{" "}
                {conflictRevision
                  ? `冻结错误契约返回 currentRevision=${conflictRevision}。`
                  : "响应缺少 currentRevision，请重新加载工作区。"}{" "}
                <span className="mono">{state.lastProblem.traceId}</span>
              </p>
            </div>
            <button
              className="button button-secondary"
              type="button"
              onClick={() => actions.synchronizeGate(gate.id)}
              disabled={state.busy || !conflictRevision}
            >
              <RefreshCw className={state.busy ? "spin-slow" : ""} size={15} aria-hidden="true" />
              {conflictRevision ? `同步最新 revision ${conflictRevision}` : "重新加载后同步"}
            </button>
          </div>
        )}

        {staleDependency && (
          <div className="gate-stale-dependency" role="status">
            <strong>资产引用已变化，此 G2 revision 不可继续批准</strong>
            <p>先“创建新 revision”吸收 STALE 镜头和 Prompt 影响；旧 G2 审核记录保持只读。</p>
          </div>
        )}

        {expanded && (
          <div className="review-box">
            <label htmlFor={noteId}>审核意见</label>
            <textarea
              id={noteId}
              value={explanation}
              onChange={(event) => setExplanation(event.target.value)}
              placeholder="批准时可记录关注点；退回时请写清如何修改。"
              rows={3}
            />
            <div className="review-actions">
              <button
                className="button button-secondary button-danger"
                type="button"
                disabled={state.busy || blocked || terminal}
                onClick={() => decide("RETURNED")}
              >
                退回修改
              </button>
              <button
                className="button button-primary"
                type="button"
                disabled={state.busy || blocked || terminal}
                onClick={() => decide("APPROVED")}
              >
                {state.busy ? <RefreshCw className="spin-slow" size={15} aria-hidden="true" /> : null}
                批准并锁定 r{gate.revision}
              </button>
            </div>
          </div>
        )}

        <div className="gate-footer">
          <button
            className="text-button"
            type="button"
            disabled={blocked || terminal}
            onClick={() => setExpanded((value) => !value)}
            aria-expanded={expanded}
          >
            <MessageSquareText size={15} aria-hidden="true" />
            {expanded
              ? "收起审核"
              : terminal
                ? "已完成审核"
                : staleDependency
                  ? "依赖已变更"
                  : blocked
                    ? "等待上游"
                    : "开始审核"}
            <ChevronDown className={expanded ? "turn-180" : ""} size={14} aria-hidden="true" />
          </button>
          <div className="gate-utility">
            <button
              className="text-button muted"
              type="button"
              onClick={() => actions.simulateConcurrentUpdate(gate.id)}
              disabled={state.busy}
              title="仅 Mock：让服务端 ETag 前进，以验证 409 并发保护"
            >
              并发测试
            </button>
            <button
              className="text-button"
              type="button"
              onClick={() => actions.regenerateGate(gate.id)}
              disabled={state.busy || upstreamBlocked}
            >
              <History size={15} aria-hidden="true" />
              创建新 revision
            </button>
          </div>
        </div>
      </div>
    </article>
  );
}
