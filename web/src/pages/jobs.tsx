import {
  Ban,
  CircleDollarSign,
  CloudOff,
  FlaskConical,
  Gauge,
  KeyRound,
  RotateCcw,
  ShieldAlert,
  TimerReset,
  Unplug,
  Webhook,
  XOctagon,
} from "lucide-react";
import { EvidenceBadge, JobBadge, ProgressBar } from "../components/ui";
import { jobErrorLabel, type FailureScenario, useStudio } from "../studio-store";

const scenarios: Array<{
  id: FailureScenario;
  label: string;
  detail: string;
  icon: typeof KeyRound;
}> = [
  { id: "unauthorized", label: "401", detail: "凭据未配置", icon: KeyRound },
  { id: "forbidden", label: "403", detail: "模型无权限", icon: Ban },
  { id: "rate_limited", label: "429", detail: "限流等待", icon: Gauge },
  { id: "provider_unavailable", label: "5xx", detail: "同任务重试", icon: Unplug },
  { id: "timeout", label: "超时", detail: "进入 UNKNOWN", icon: TimerReset },
  { id: "duplicate_callback", label: "重复", detail: "回调去重", icon: Webhook },
  { id: "out_of_order_callback", label: "乱序", detail: "禁止回退", icon: RotateCcw },
  { id: "cancel_race", label: "竞态", detail: "终态优先", icon: ShieldAlert },
];

export function JobsPage() {
  const { state, actions } = useStudio();
  const estimated = state.jobs.reduce((sum, job) => sum + (job.costMicros ?? 0), 0) / 1_000_000;
  const completeEnabled = state.gates.G2.state === "APPROVED";

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <span className="eyebrow">Provider 操作中心</span>
          <h1>每个远程任务都有凭据</h1>
          <p>submit、poll、webhook、cancel、retry 和 timeout 都回到同一个 Job ID；浏览器只看脱敏投影。</p>
        </div>
        <button
          className="button button-primary"
          type="button"
          onClick={() => actions.completeMockRun()}
          disabled={!completeEnabled}
          title={completeEnabled ? "将 fixture 任务推进到成功终态" : "G2 批准后才能提交生成计划"}
        >
          <FlaskConical size={16} aria-hidden="true" />
          完成 Mock 排练
        </button>
      </header>

      <section className="provider-banner">
        <div className="provider-banner-icon">
          <CloudOff size={22} aria-hidden="true" />
        </div>
        <div>
          <span className="eyebrow">执行模式</span>
          <h2>Mock only · 真实火山调用 pending_key</h2>
          <p>四种能力均可 Dry-run；liveConfigured=false，任何 Secret 都不会进入浏览器、日志、trace 或 fixture。</p>
        </div>
        <div className="provider-caps">
          {state.capabilities.map((capability) => (
            <span key={capability.alias}>{capability.alias.split(".")[0]}</span>
          ))}
        </div>
      </section>

      <section className="job-metrics">
        <article>
          <span>当前批次</span>
          <strong>{state.jobs.length} 个任务</strong>
          <small>plan hash · 7d3a…e910</small>
        </article>
        <article>
          <span>预算预占上界</span>
          <strong>¥{estimated.toFixed(2)}</strong>
          <small>Mock 估算 · verified=false</small>
        </article>
        <article>
          <span>基础设施重试</span>
          <strong>{state.jobs.reduce((sum, job) => sum + job.retryCount, 0)} / 3</strong>
          <small>原 Job ID，不新建 attempt</small>
        </article>
        <article>
          <span>外部调用</span>
          <strong>{completeEnabled ? "已获 G2" : "0 次"}</strong>
          <small>{completeEnabled ? "可运行 Mock 计划" : "G2 阻断中"}</small>
        </article>
      </section>

      <section className="scenario-lab">
        <div className="subheading-row">
          <div>
            <span className="eyebrow">异常注入台</span>
            <h2>验证明确反馈，不伪造顺利路径</h2>
          </div>
          <small>注入目标：S03-02 · 仅 Mock</small>
        </div>
        <div className="scenario-grid">
          {scenarios.map((scenario) => {
            const Icon = scenario.icon;
            return (
              <button type="button" key={scenario.id} onClick={() => actions.injectScenario(scenario.id)}>
                <Icon size={16} aria-hidden="true" />
                <span>
                  <strong>{scenario.label}</strong>
                  <small>{scenario.detail}</small>
                </span>
              </button>
            );
          })}
        </div>
      </section>

      <section className="jobs-panel">
        <div className="subheading-row jobs-heading">
          <div>
            <span className="eyebrow">批次 EP03-R02</span>
            <h2>Provider jobs</h2>
          </div>
          <div className="job-state-legend" aria-label="支持的任务状态">
            {(["QUEUED", "RUNNING", "RETRYING", "CANCEL_REQUESTED", "CANCELLED", "FAILED", "SUCCEEDED"] as const).map(
              (status) => (
                <JobBadge key={status} state={status} />
              ),
            )}
          </div>
        </div>
        <div className="job-table-wrap">
          <table className="job-table">
            <thead>
              <tr>
                <th scope="col">镜头 / 任务</th>
                <th scope="col">状态</th>
                <th scope="col">Provider / 模型</th>
                <th scope="col">进度</th>
                <th scope="col">费用证据</th>
                <th scope="col">操作</th>
              </tr>
            </thead>
            <tbody>
              {state.jobs.map((job) => (
                <tr key={job.id}>
                  <td>
                    <strong>{job.shot}</strong>
                    <small className="mono">{job.id}</small>
                    {job.failure && (
                      <span className="job-error-inline">
                        <XOctagon size={13} aria-hidden="true" />
                        {job.failure.httpStatus ? `${job.failure.httpStatus} · ` : ""}
                        {jobErrorLabel(job.failure.errorCode)}
                      </span>
                    )}
                  </td>
                  <td>
                    <JobBadge state={job.state} />
                    <small>attempt {job.attempt} · retry {job.retryCount}</small>
                  </td>
                  <td>
                    <strong>{job.provider}</strong>
                    <small>{job.model}</small>
                    <EvidenceBadge evidence={job.evidence} />
                  </td>
                  <td>
                    <ProgressBar value={job.progress} label={`${job.shot} 任务进度`} />
                    <small>{job.updatedAt} 更新</small>
                  </td>
                  <td>
                    <strong>{job.costMicros ? `¥${(job.costMicros / 1_000_000).toFixed(2)}` : "金额未知"}</strong>
                    <small>{job.costVerified ? "账单已核实" : "估算 · 未核实"}</small>
                  </td>
                  <td>
                    <div className="table-actions">
                      {job.failure?.retryable && job.retryCount < 3 && (
                        <button className="text-button" type="button" onClick={() => actions.retryJob(job.id)}>
                          重试
                        </button>
                      )}
                      {!["SUCCEEDED", "FAILED", "CANCELLED", "CANCEL_REQUESTED"].includes(job.state) && (
                        <button className="text-button muted" type="button" onClick={() => actions.cancelJob(job.id)}>
                          取消
                        </button>
                      )}
                      {job.state === "SUCCEEDED" && <span className="table-done">已归档 CAS</span>}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="trace-boundary">
        <CircleDollarSign size={18} aria-hidden="true" />
        <div>
          <strong>可追溯不等于可泄密</strong>
          <p>Manifest 保存 provider profile、model、request/task ID、用量、费用和 hash；不保存凭据、原始错误体或临时签名地址。</p>
        </div>
      </section>
    </div>
  );
}
