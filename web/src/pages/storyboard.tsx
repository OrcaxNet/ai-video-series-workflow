import { Camera, ChevronRight, Layers3, LockKeyhole, MessageSquareQuote, Timer, TriangleAlert } from "lucide-react";
import { GateCard } from "../components/gate-card";
import { useStudio } from "../studio-store";

export function StoryboardPage() {
  const { state } = useStudio();

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <span className="eyebrow">剧本 / 分镜 · G2</span>
          <h1>把文字切成可生成的镜头</h1>
          <p>每个镜头保持 3–5 秒，携带冻结的资产和四层上下文；变更会显式标记 stale。</p>
        </div>
        <div className="header-stat">
          <strong>21s</strong>
          <span>5 个镜头</span>
        </div>
      </header>

      <section className="screenplay-strip">
        <div className="scene-marker">
          <span>SC 12</span>
          <strong>北岬灯塔 · 内/外 · 夜</strong>
        </div>
        <blockquote>
          灯塔的光束第三次扫过海面时，潮水没有继续上涨。它停在那里，像有人按住了一帧旧胶片。
        </blockquote>
        <span className="script-revision">
          <LockKeyhole size={13} aria-hidden="true" /> 剧本 r6
        </span>
      </section>

      <section className="storyboard-workspace">
        <div className="shot-sequence">
          <div className="subheading-row">
            <div>
              <span className="eyebrow">镜头轨</span>
              <h2>SC-12 · 连续动作</h2>
            </div>
            <span className="mono">00:42:18 — 01:03:18</span>
          </div>

          <div className="shot-timeline">
            {state.shots.map((shot, index) => (
              <article className={`shot-card shot-${shot.state.toLowerCase()}`} key={shot.id}>
                <div className="shot-frame">
                  <span className="shot-number">{String(index + 1).padStart(2, "0")}</span>
                  <div className="frame-lines" aria-hidden="true">
                    <i />
                    <i />
                    <i />
                  </div>
                  <span className="shot-code">{shot.code}</span>
                </div>
                <div className="shot-copy">
                  <div className="shot-title-row">
                    <h3>{shot.title}</h3>
                    <span className={`tiny-state tiny-${shot.state.toLowerCase()}`}>
                      {shot.state === "STALE" ? "上下文已变" : shot.state === "READY" ? "可生成" : shot.state === "GENERATING" ? "生成中" : "已有产物"}
                    </span>
                  </div>
                  <p>{shot.action}</p>
                  {shot.dialogue && (
                    <div className="dialogue-line">
                      <MessageSquareQuote size={14} aria-hidden="true" />
                      “{shot.dialogue}”
                    </div>
                  )}
                  <div className="shot-specs">
                    <span>
                      <Timer size={13} aria-hidden="true" /> {shot.duration}s
                    </span>
                    <span>
                      <Camera size={13} aria-hidden="true" /> {shot.camera}
                    </span>
                    <span>
                      <Layers3 size={13} aria-hidden="true" /> Prompt r{shot.promptRevision}
                    </span>
                  </div>
                </div>
                <button className="icon-button shot-open" type="button" aria-label={`打开 ${shot.code}`}>
                  <ChevronRight size={17} aria-hidden="true" />
                </button>
              </article>
            ))}
          </div>
        </div>

        <aside className="context-rail" aria-label="四层上下文解析">
          <div className="subheading-row">
            <div>
              <span className="eyebrow">有效上下文</span>
              <h2>4 层合并</h2>
            </div>
            <span className="resolved-chip">已解析</span>
          </div>
          <p className="context-note">当前预览：S03-03。下层同名字段显式覆盖，上层仍保留来源。</p>
          <div className="context-layer-list">
            {state.contextLayers.map((layer, index) => (
              <div className="context-layer" key={layer.revisionId}>
                <div className="context-spine" aria-hidden="true">
                  <span>{index + 1}</span>
                  {index < state.contextLayers.length - 1 && <i />}
                </div>
                <div>
                  <div className="context-layer-title">
                    <strong>{layer.label}</strong>
                    <small>r{layer.revision}</small>
                  </div>
                  {layer.entries.map((entry) => (
                    <p key={entry.key}>
                      <span>{entry.key}</span>
                      {entry.value}
                      {entry.override && <em>覆盖</em>}
                    </p>
                  ))}
                </div>
              </div>
            ))}
          </div>
          <div className="context-hash">
            <span>effective context hash</span>
            <code>{state.shots[2].contextHash.slice(0, 20)}…</code>
          </div>
        </aside>
      </section>

      <div className="stale-warning" role="status">
        <TriangleAlert size={18} aria-hidden="true" />
        <div>
          <strong>S03-03 使用的道具草稿发生变化</strong>
          <p>已锁定的潮汐钟 v1 不受影响；只有选择 v2 并创建新 Prompt revision 后才会进入生成。</p>
        </div>
        <button className="button button-secondary" type="button">检查差异</button>
      </div>

      <section className="gate-section">
        <div className="section-heading">
          <div>
            <span className="eyebrow">提交人工闸门</span>
            <h2>锁定剧本、分镜与 Prompt 集</h2>
          </div>
        </div>
        <GateCard gate={state.gates.G2} />
      </section>
    </div>
  );
}
