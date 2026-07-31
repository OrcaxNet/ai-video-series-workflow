import { ArrowRight, Check, Clock3, Film, LockKeyhole, Play, ShieldCheck, Sparkles } from "lucide-react";
import { GateCard } from "../components/gate-card";
import { useStudio } from "../studio-store";

export function OverviewPage() {
  const { state, actions } = useStudio();
  const approved = Object.values(state.gates).filter((gate) => gate.state === "APPROVED").length;
  const finishedJobs = state.jobs.filter((job) => job.state === "SUCCEEDED").length;

  return (
    <div className="page page-overview">
      <section className="hero-workboard">
        <div className="hero-copy">
          <span className="kicker">
            <span className="record-dot" aria-hidden="true" />
            当前工作台 · {state.project.episode}
          </span>
          <h1>
            把每一次决定，
            <br />
            <em>留在成片里。</em>
          </h1>
          <p>
            《{state.project.title}》第 03 集已进入生产。审核绑定 revision；生成只使用已锁定的内容、资产和上下文。
          </p>
          <div className="hero-actions">
            <button className="button button-primary" type="button" onClick={() => actions.navigate("assets")}>
              继续 G1 审核
              <ArrowRight size={16} aria-hidden="true" />
            </button>
            <button className="button button-secondary" type="button" onClick={() => actions.navigate("storyboard")}>
              打开分镜台
            </button>
          </div>
        </div>
        <div className="timecode-board" aria-label="本集生产摘要">
          <div className="timecode-top">
            <span>EPISODE CUT</span>
            <span className="mono">E03 / R02</span>
          </div>
          <div className="timecode-main">
            <span className="mono">{state.project.targetDuration}</span>
            <small>目标成片时长</small>
          </div>
          <div className="timecode-grid">
            <div>
              <strong>{approved}/3</strong>
              <span>人工闸门</span>
            </div>
            <div>
              <strong>{finishedJobs}/5</strong>
              <span>镜头就绪</span>
            </div>
            <div>
              <strong>¥2.20</strong>
              <span>估算上界</span>
            </div>
            <div>
              <strong>{state.project.aspectRatio}</strong>
              <span>画幅锁定</span>
            </div>
          </div>
          <div className="film-perf" aria-hidden="true">
            {Array.from({ length: 12 }, (_, index) => (
              <i key={index} />
            ))}
          </div>
        </div>
      </section>

      <section className="mock-boundary" aria-label="当前执行边界">
        <div className="mock-boundary-icon">
          <Sparkles size={18} aria-hidden="true" />
        </div>
        <div>
          <strong>当前是无 Key 的 Mock 排练</strong>
          <p>可验证编排、异常和审核；不能把结果当作真实生成质量、时延、成功率或费用。</p>
        </div>
        <button className="text-button" type="button" onClick={() => actions.navigate("jobs")}>
          查看证据边界
          <ArrowRight size={14} aria-hidden="true" />
        </button>
      </section>

      <section className="section-block">
        <div className="section-heading">
          <div>
            <span className="eyebrow">本集通行链</span>
            <h2>三道闸门，按顺序发生</h2>
          </div>
          <div className="legend-inline">
            <span>
              <Check size={13} aria-hidden="true" /> 已锁定
            </span>
            <span>
              <Clock3 size={13} aria-hidden="true" /> 待人工
            </span>
            <span>
              <LockKeyhole size={13} aria-hidden="true" /> 被阻断
            </span>
          </div>
        </div>
        <div className="gate-stack">
          {Object.values(state.gates).map((gate) => (
            <GateCard gate={gate} key={gate.id} />
          ))}
        </div>
      </section>

      <section className="overview-bottom-grid">
        <article className="panel next-action-panel">
          <span className="eyebrow">下一步</span>
          <div className="next-action-icon">
            <ShieldCheck size={22} aria-hidden="true" />
          </div>
          <h2>先确认角色与场景可复用</h2>
          <p>G1 会锁定 14 项资产和权利快照；一旦批准，剧本和 Prompt 才能引用这些 revision。</p>
          <button className="text-button strong" type="button" onClick={() => actions.navigate("assets")}>
            检查内容与资产 <ArrowRight size={15} aria-hidden="true" />
          </button>
        </article>
        <article className="panel episode-lineup">
          <div className="panel-title-row">
            <div>
              <span className="eyebrow">剧集排期</span>
              <h2>本季 8 集</h2>
            </div>
            <Film size={20} aria-hidden="true" />
          </div>
          <div className="episode-bars">
            {[
              ["E01", "锁版", 100],
              ["E02", "锁版", 100],
              ["E03", "生产中", 62],
              ["E04", "剧本", 38],
              ["E05", "结构稿", 18],
            ].map(([label, status, progress]) => (
              <div className="episode-row" key={label}>
                <span className="mono">{label}</span>
                <div>
                  <i style={{ width: `${progress}%` }} />
                </div>
                <small>{status}</small>
              </div>
            ))}
          </div>
        </article>
        <article className="panel budget-panel">
          <div className="panel-title-row">
            <div>
              <span className="eyebrow">本集预算</span>
              <h2>付费前先确认</h2>
            </div>
            <Play size={20} aria-hidden="true" />
          </div>
          <div className="budget-figure">
            <strong>¥1.48–2.20</strong>
            <span>Mock 估算 · 未核实</span>
          </div>
          <div className="budget-meter">
            <i style={{ width: "44%" }} />
          </div>
          <div className="budget-labels">
            <span>计划上界 ¥5.00</span>
            <span>预计 25 秒视频</span>
          </div>
        </article>
      </section>
    </div>
  );
}
