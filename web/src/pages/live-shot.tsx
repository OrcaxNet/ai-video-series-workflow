import {
  AlertTriangle,
  ArrowDownToLine,
  Check,
  CircleDot,
  Clock3,
  Film,
  Fingerprint,
  LoaderCircle,
  Play,
  RefreshCw,
  ShieldCheck,
  Sparkles,
} from "lucide-react";
import { type FormEvent, useMemo, useState } from "react";
import type {
  CreateLiveShotPlanInput,
  CreatorLiveShotRun,
  CreatorLiveShotRunState,
  LiveShotPhase,
} from "../domain";
import { useStudio } from "../studio-store";

const initialDraft: CreateLiveShotPlanInput = {
  title: "雨夜重逢",
  sceneText: "深夜的旧火车站，雨水沿玻璃顶棚倾泻。两位多年未见的朋友隔着空站台认出彼此，谁都没有先开口。镜头缓慢推近，停在他们同时松开的手上。",
  aspectRatio: "16:9",
  rightsAccepted: false,
};

const terminalStates = new Set<CreatorLiveShotRunState>([
  "SUCCEEDED",
  "FAILED",
  "CANCELLED",
  "REQUIRES_ACTION",
]);

const runLabels: Record<CreatorLiveShotRunState, string> = {
  QUEUED: "已排队",
  RUNNING: "生成中",
  UNKNOWN: "状态待对账",
  RECONCILING: "正在恢复同一任务",
  SUCCEEDED: "真实产物已归档",
  FAILED: "生成失败",
  CANCELLED: "已取消",
  REQUIRES_ACTION: "需要处理",
};

const phaseOrder: LiveShotPhase[] = [
  "DRAFT",
  "AWAITING_CONFIRMATION",
  "TRACKING",
  "TERMINAL",
];

const phaseIndex = (phase: LiveShotPhase) => {
  if (phase === "PLANNING") return 0;
  if (phase === "CONFIRMING") return 1;
  if (phase === "RECOVERING") return 2;
  return phaseOrder.indexOf(phase);
};

const formatNumber = (value: number | null | undefined) =>
  value == null ? "未知" : new Intl.NumberFormat("zh-CN").format(value);

const formatBytes = (value: number) => {
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / 1024 / 1024).toFixed(1)} MB`;
};

const shortHash = (value: string) => `${value.slice(0, 12)}…`;

export function LiveShotPage() {
  const { state, actions } = useStudio();
  const [draft, setDraft] = useState(initialDraft);
  const [validation, setValidation] = useState<Record<string, string>>({});
  const { liveShot } = state;
  const videoCapability = state.capabilities.find((item) => item.alias === "video.primary");
  const liveReady = videoCapability?.liveConfigured && videoCapability.liveCallsEnabled;
  const locked = liveShot.phase !== "DRAFT" && liveShot.phase !== "AWAITING_CONFIRMATION";
  const currentStep = phaseIndex(liveShot.phase);

  const counts = useMemo(
    () => ({
      title: Array.from(draft.title.trim()).length,
      scene: Array.from(draft.sceneText.trim()).length,
    }),
    [draft.sceneText, draft.title],
  );

  const updateDraft = <Key extends keyof CreateLiveShotPlanInput>(
    key: Key,
    value: CreateLiveShotPlanInput[Key],
  ) => {
    if (liveShot.plan && !liveShot.run) actions.resetLiveShot();
    setValidation((current) => ({ ...current, [key]: "" }));
    setDraft((current) => ({ ...current, [key]: value }));
  };

  const submitPlan = (event: FormEvent) => {
    event.preventDefault();
    const errors: Record<string, string> = {};
    if (counts.title < 1 || counts.title > 80) errors.title = "项目名需为 1–80 个字符。";
    if (counts.scene < 1 || counts.scene > 800) errors.sceneText = "场景描述需为 1–800 个字符。";
    if (!draft.rightsAccepted) errors.rightsAccepted = "确认权利声明后才能生成计划。";
    setValidation(errors);
    if (Object.keys(errors).length > 0) return;
    void actions.createLiveShotPlan({
      ...draft,
      title: draft.title.trim(),
      sceneText: draft.sceneText.trim(),
    });
  };

  return (
    <div className="page page-live-shot">
      <header className="live-shot-hero">
        <div>
          <span className="kicker live-kicker">
            <CircleDot size={11} aria-hidden="true" /> LIVE · VIDEO.PRIMARY
          </span>
          <h1>让一个镜头，<em>真正发生。</em></h1>
          <p>先锁定一份零提交计划，再确认唯一一次 Agent Plan 任务。刷新只恢复原任务，不会重新生成。</p>
        </div>
        <div className={`live-readiness ${liveReady ? "ready" : "blocked"}`} role="status">
          {liveReady ? <ShieldCheck size={22} aria-hidden="true" /> : <AlertTriangle size={22} aria-hidden="true" />}
          <span>
            <strong>{liveReady ? "真实调用已就绪" : "真实调用未启用"}</strong>
            <small>{liveReady ? `${videoCapability.defaultProvider} · Agent Plan` : "确认按钮保持关闭"}</small>
          </span>
        </div>
      </header>

      <FilmTrack currentStep={currentStep} phase={liveShot.phase} />

      <div className="live-shot-workspace">
        {liveShot.run && liveShot.plan ? (
          <LockedShotSummary />
        ) : (
          <form className="live-input-panel" onSubmit={submitPlan} aria-label="真实单镜输入">
          <div className="panel-title-row live-panel-heading">
            <div>
              <span className="eyebrow">镜头单</span>
              <h2>描述这 5 秒</h2>
            </div>
            <span className="badge badge-info">固定规格</span>
          </div>

          <label className="field-label" htmlFor="live-title">
            项目名 <span>{counts.title}/80</span>
          </label>
          <input
            id="live-title"
            value={draft.title}
            disabled={locked}
            aria-invalid={Boolean(validation.title)}
            aria-describedby={validation.title ? "live-title-error" : undefined}
            onChange={(event) => updateDraft("title", event.target.value)}
          />
          {validation.title && <small className="field-error" id="live-title-error">{validation.title}</small>}

          <label className="field-label" htmlFor="live-scene">
            场景描述 <span>{counts.scene}/800</span>
          </label>
          <textarea
            id="live-scene"
            rows={8}
            value={draft.sceneText}
            disabled={locked}
            aria-invalid={Boolean(validation.sceneText)}
            aria-describedby={validation.sceneText ? "live-scene-error" : "live-scene-hint"}
            onChange={(event) => updateDraft("sceneText", event.target.value)}
          />
          <small id="live-scene-hint">写清主体、动作、环境与镜头意图；内容不会写入浏览器存储。</small>
          {validation.sceneText && <small className="field-error" id="live-scene-error">{validation.sceneText}</small>}

          <fieldset className="aspect-choice" disabled={locked}>
            <legend>画幅</legend>
            <label className={draft.aspectRatio === "16:9" ? "selected" : ""}>
              <input
                type="radio"
                name="aspect"
                value="16:9"
                checked={draft.aspectRatio === "16:9"}
                onChange={() => updateDraft("aspectRatio", "16:9")}
              />
              <span className="aspect-frame landscape" aria-hidden="true" />
              <strong>16:9</strong>
              <small>横屏</small>
            </label>
            <label className={draft.aspectRatio === "9:16" ? "selected" : ""}>
              <input
                type="radio"
                name="aspect"
                value="9:16"
                checked={draft.aspectRatio === "9:16"}
                onChange={() => updateDraft("aspectRatio", "9:16")}
              />
              <span className="aspect-frame portrait" aria-hidden="true" />
              <strong>9:16</strong>
              <small>竖屏</small>
            </label>
          </fieldset>

          <div className="fixed-spec" aria-label="固定生成规格">
            <span><strong>1</strong> 候选</span>
            <span><strong>5s</strong> 时长</span>
            <span><strong>720p</strong> 画质</span>
            <span><strong>OFF</strong> 音频</span>
          </div>

          <label className="rights-check">
            <input
              type="checkbox"
              checked={draft.rightsAccepted}
              disabled={locked}
              onChange={(event) => updateDraft("rightsAccepted", event.target.checked)}
            />
            <span>我确认拥有或已获授权，可将以上内容提交给第三方生成服务。</span>
          </label>
          {validation.rightsAccepted && <small className="field-error">{validation.rightsAccepted}</small>}

          <button
            className="button button-primary live-plan-button"
            type="submit"
            disabled={!liveReady || liveShot.phase !== "DRAFT"}
          >
            {liveShot.phase === "PLANNING" ? <LoaderCircle className="spin-slow" size={16} /> : <Sparkles size={16} />}
            {liveShot.phase === "PLANNING" ? "正在生成零提交计划" : "生成计划（不扣费）"}
          </button>
          <p className="zero-submit-note"><ShieldCheck size={13} /> 此步骤的 Provider submit 必须为 0。</p>
          </form>
        )}

        <section className="live-truth-panel" aria-live="polite">
          {liveShot.phase === "DRAFT" && !liveShot.problem && (
            <DraftTruth
              liveReady={Boolean(liveReady)}
              onRefresh={() => void actions.refreshProviderStatus()}
            />
          )}
          {liveShot.phase === "PLANNING" && <PlanningState />}
          {liveShot.plan && !liveShot.run && liveShot.phase !== "PLANNING" && (
            <PlanTruth onConfirm={() => void actions.confirmLiveShotPlan()} phase={liveShot.phase} />
          )}
          {liveShot.run && <RunTruth run={liveShot.run} />}
          {liveShot.problem && <ProblemTruth />}
        </section>
      </div>

      <section className="live-security-strip" aria-label="浏览器安全边界">
        <span><ShieldCheck size={15} /> 只访问同源 Control Plane</span>
        <span>Agent Plan Key / Authorization 不进入浏览器</span>
        <span>Provider 临时地址不进入 DOM 或存储</span>
        <span className="deferred-capabilities">text · image · speech 暂未接入</span>
      </section>
    </div>
  );
}

function LockedShotSummary() {
  const { state } = useStudio();
  const plan = state.liveShot.plan!;
  return (
    <aside className="live-input-panel locked-shot-summary" aria-label="已冻结镜头单">
      <div className="panel-title-row live-panel-heading">
        <div>
          <span className="eyebrow">已冻结镜头单</span>
          <h2>{plan.title}</h2>
        </div>
        <span className="badge badge-success"><Check size={11} /> 已绑定</span>
      </div>
      <div className="locked-shot-frame" aria-hidden="true">
        <ClapperboardMark />
        <span>{plan.spec.durationSeconds}.00 SEC</span>
      </div>
      <dl className="locked-shot-ledger">
        <div><dt>场景内容</dt><dd>已由控制面冻结为 hash；刷新不把原文写入浏览器存储。</dd></div>
        <div><dt>Scene SHA-256</dt><dd className="mono" title={plan.sceneTextHash}>{shortHash(plan.sceneTextHash)}</dd></div>
        <div><dt>画幅</dt><dd>{plan.spec.aspectRatio} · {plan.spec.resolution}</dd></div>
        <div><dt>输出</dt><dd>1 候选 · 5 秒 · 无音频</dd></div>
      </dl>
      <div className="locked-rights"><ShieldCheck size={17} /><span><strong>权利声明已绑定</strong><small>USER_ATTESTED_OWNED_OR_AUTHORIZED</small></span></div>
      <p className="locked-shot-note">镜头单已只读。状态恢复只查询本项目的原 operation / run。</p>
    </aside>
  );
}

function ClapperboardMark() {
  return (
    <span className="clapperboard-mark">
      <i /><i /><i />
    </span>
  );
}

function FilmTrack({ currentStep, phase }: { currentStep: number; phase: LiveShotPhase }) {
  const steps = [
    ["01", "写镜头"],
    ["02", "看计划"],
    ["03", "盯任务"],
    ["04", "收原片"],
  ];
  return (
    <ol className="film-track" aria-label="真实单镜进度">
      {steps.map(([number, label], index) => (
        <li className={index < currentStep ? "complete" : index === currentStep ? "active" : ""} key={number}>
          <span>{index < currentStep ? <Check size={13} /> : number}</span>
          <strong>{label}</strong>
          {index === currentStep && <small>{phase === "RECOVERING" ? "恢复中" : "当前"}</small>}
        </li>
      ))}
    </ol>
  );
}

function DraftTruth({ liveReady, onRefresh }: { liveReady: boolean; onRefresh(): void }) {
  return (
    <div className="live-empty-state">
      <div className="live-empty-frame"><Film size={34} aria-hidden="true" /></div>
      <span className="eyebrow">等待镜头单</span>
      <h2>{liveReady ? "先看清计划，再让它开机。" : "机器没有开机，输入仍留在本地。"}</h2>
      <p>{liveReady ? "计划会锁定精确模型、预算和 planHash，但不会创建 Provider 任务。" : "video.primary 未同时通过能力快照与运维开关；页面打开和编辑不会触发真实调用。"}</p>
      {!liveReady && (
        <button className="button button-secondary" type="button" onClick={onRefresh}>
          <RefreshCw size={14} /> 重新读取能力
        </button>
      )}
      <div className="empty-contract">
        <span>PLAN</span><strong>0 submit</strong>
        <span>CONFIRM</span><strong>最多 1 task</strong>
        <span>RECOVERY</span><strong>同一 operation</strong>
      </div>
    </div>
  );
}

function PlanningState() {
  return (
    <div className="live-loading-state" role="status">
      <LoaderCircle className="spin-slow" size={28} aria-hidden="true" />
      <span className="eyebrow">PLAN / ZERO SUBMIT</span>
      <h2>正在锁定模型与预算</h2>
      <p>控制面正在物化权利、Prompt、route 与预算快照；此时不会启动 Provider 任务。</p>
    </div>
  );
}

function PlanTruth({ onConfirm, phase }: { onConfirm(): void; phase: LiveShotPhase }) {
  const { state } = useStudio();
  const plan = state.liveShot.plan!;
  const blocked = !plan.confirmable || plan.blockers.length > 0;
  return (
    <div className="plan-truth">
      <div className="truth-heading">
        <div>
          <span className="eyebrow">已冻结的生成计划</span>
          <h2>确认前，没有任务被提交</h2>
        </div>
        <span className="zero-submit-seal"><Check size={13} /> submit {plan.providerSubmitCount}</span>
      </div>
      <dl className="plan-ledger">
        <div><dt>模型</dt><dd>{plan.route.modelId}</dd></div>
        <div><dt>Provider</dt><dd>{plan.route.provider}</dd></div>
        <div><dt>本次上限</dt><dd>{plan.budget.maxTasksThisConfirmation} task / {formatNumber(plan.budget.maxVideoTokensThisConfirmation)} token</dd></div>
        <div><dt>项目累计</dt><dd>{plan.budget.projectTasksUsed}/{plan.budget.projectTaskLimit} task · {formatNumber(plan.budget.projectTokensUsed)}/{formatNumber(plan.budget.projectTokenLimit)} token</dd></div>
        <div><dt>结算口径</dt><dd>Agent Plan subscription</dd></div>
        <div><dt>现金金额</dt><dd>未提供 · null · 未核验</dd></div>
        <div><dt>计划有效期</dt><dd>{new Date(plan.expiresAt).toLocaleString("zh-CN", { hour12: false })}</dd></div>
        <div><dt>planHash</dt><dd className="mono" title={plan.planHash}>{shortHash(plan.planHash)}</dd></div>
      </dl>
      {blocked && (
        <div className="plan-blockers" role="alert">
          <AlertTriangle size={16} />
          <span><strong>计划不可确认</strong>{plan.blockers.join(" · ") || "控制面未允许确认"}</span>
        </div>
      )}
      <div className="confirm-once">
        <div className="confirm-copy">
          <Fingerprint size={24} aria-hidden="true" />
          <span>
            <strong>最后确认</strong>
            <small>将提交 1 个真实 Agent Plan 视频任务，并绑定上方同一 planHash。</small>
          </span>
        </div>
        <button
          type="button"
          className="button button-primary"
          disabled={blocked || phase === "CONFIRMING" || phase === "RECOVERING"}
          onClick={onConfirm}
        >
          {phase === "CONFIRMING" || phase === "RECOVERING" ? <LoaderCircle className="spin-slow" size={15} /> : <Play size={15} />}
          {phase === "CONFIRMING" ? "正在确认唯一任务" : phase === "RECOVERING" ? "正在恢复原操作" : "确认并生成一次"}
        </button>
      </div>
    </div>
  );
}

function RunTruth({ run }: { run: CreatorLiveShotRun }) {
  const { state, actions } = useStudio();
  const artifactUrl = actions.liveShotArtifactUrl(run.runId);
  const manifestUrl = actions.liveShotManifestUrl(run.runId);
  const terminal = terminalStates.has(run.state);
  const recovering = run.state === "UNKNOWN" || run.state === "RECONCILING" || state.liveShot.phase === "RECOVERING";
  return (
    <div className="run-truth">
      <div className={`run-state-banner state-${run.state.toLowerCase()}`}>
        {terminal ? <Check size={18} aria-hidden="true" /> : <LoaderCircle className={recovering ? "spin-slow" : ""} size={18} aria-hidden="true" />}
        <span><small>CONTROL PLANE PROJECTION</small><strong>{runLabels[run.state]}</strong></span>
        <em>{run.progress == null ? "—" : `${run.progress}%`}</em>
      </div>
      {recovering && <p className="reconcile-note">系统只查询同一 Provider task，不会 submit 新任务。</p>}
      <dl className="run-ledger">
        <div><dt>Provider Job</dt><dd className="mono">{run.providerJobId}</dd></div>
        <div><dt>Provider task</dt><dd className="mono">{run.providerTaskId ?? "等待 Provider 返回"}</dd></div>
        <div><dt>Request</dt><dd className="mono">{run.providerRequestId ?? "未知"}</dd></div>
        <div><dt>submit count</dt><dd>{run.submitCount} / 1</dd></div>
        <div><dt>模型</dt><dd>{run.route.modelId}</dd></div>
        <div><dt>Trace</dt><dd className="mono">{run.traceId}</dd></div>
      </dl>

      {!terminal && (
        <button className="button button-secondary" type="button" onClick={() => void actions.refreshLiveShot()}>
          <RefreshCw size={14} /> 刷新真实状态
        </button>
      )}

      {run.failure && (
        <div className="run-failure" role="alert">
          <AlertTriangle size={18} />
          <span><strong>{run.failure.errorCode}</strong><p>{run.failure.suggestedAction}</p><small>retryable={String(run.failure.retryable)} · trace {run.traceId}</small></span>
        </div>
      )}

      <div className="usage-board">
        <span><small>Prompt video token</small><strong>{formatNumber(run.usage.promptVideoTokens)}</strong></span>
        <span><small>Completion video token</small><strong>{formatNumber(run.usage.completionVideoTokens)}</strong></span>
        <span><small>Total video token</small><strong>{formatNumber(run.usage.totalVideoTokens)}</strong></span>
        <span><small>生成时长</small><strong>{run.usage.generatedDurationMs == null ? "未知" : `${run.usage.generatedDurationMs} ms`}</strong></span>
      </div>
      <p className="cash-line">Agent Plan subscription · 现金金额 {run.cost.amountMicros == null ? "null（未核验）" : `${run.cost.amountMicros} ${run.cost.currency ?? ""}`}</p>

      {run.state === "SUCCEEDED" && run.artifact && run.manifest && (
        <div className="artifact-stage">
          <div className={`video-frame ${state.liveShot.plan?.spec.aspectRatio === "9:16" ? "portrait" : ""}`}>
            <video controls preload="metadata" src={artifactUrl} aria-label="真实生成的单镜视频" />
          </div>
          <div className="artifact-ledger">
            <span className="eyebrow">CAS / LIVE PROVIDER CALL</span>
            <h3>原片与证据已落盘</h3>
            <dl>
              <div><dt>MP4</dt><dd>{run.artifact.width ?? "—"}×{run.artifact.height ?? "—"} · {formatBytes(run.artifact.sizeBytes)}</dd></div>
              <div><dt>CAS SHA-256</dt><dd className="mono" title={run.artifact.sha256}>{shortHash(run.artifact.sha256)}</dd></div>
              <div><dt>Manifest</dt><dd className="mono" title={run.manifest.hash}>{shortHash(run.manifest.hash)}</dd></div>
              <div><dt>证据</dt><dd>{run.manifest.evidence}</dd></div>
            </dl>
            <div className="artifact-actions">
              <a className="button button-primary" href={artifactUrl} download={`live-shot-${run.runId}.mp4`}>
                <ArrowDownToLine size={15} /> 下载 MP4
              </a>
              <a className="button button-secondary" href={manifestUrl} download={`live-shot-${run.runId}-manifest.json`}>
                下载 Manifest
              </a>
            </div>
          </div>
        </div>
      )}

      {terminal && (
        <button className="text-button new-live-shot" type="button" onClick={() => actions.resetLiveShot()}>
          新建另一个单镜项目
        </button>
      )}
    </div>
  );
}

function ProblemTruth() {
  const { state, actions } = useStudio();
  const problem = state.liveShot.problem!;
  const recovery = state.liveShot.phase === "RECOVERING";
  const stale = ["PLAN_EXPIRED", "PLAN_STALE", "PLAN_HASH_MISMATCH"].includes(problem.errorCode);
  const retryProjection = problem.retryable && Boolean(state.liveShot.run);
  return (
    <div className="live-problem" role="alert">
      <AlertTriangle size={20} />
      <div>
        <span className="eyebrow">{problem.errorCode}</span>
        <h3>{problem.message}</h3>
        <p>{problem.suggestedAction}</p>
        <small className="mono">trace {problem.traceId} · retryable={String(problem.retryable)}</small>
        <div className="problem-actions">
          {(recovery || stale || retryProjection) && (
            <button
              className="button button-secondary"
              type="button"
              onClick={() => {
                if (stale) actions.resetLiveShot();
                else if (retryProjection) void actions.refreshLiveShot();
                else void actions.recoverLiveShot();
              }}
            >
              <RefreshCw size={14} /> {stale ? "回到输入并重新计划" : retryProjection ? "重读同一 run" : "恢复原 operation"}
            </button>
          )}
          {recovery && (
            <button className="text-button" type="button" onClick={() => actions.resetLiveShot()}>
              清除本地恢复记录
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
