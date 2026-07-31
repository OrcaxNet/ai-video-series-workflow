import {
  Captions,
  Check,
  ChevronRight,
  Download,
  Film,
  Pause,
  Play,
  ShieldCheck,
  Volume2,
} from "lucide-react";
import { useState } from "react";
import { GateCard } from "../components/gate-card";
import { useStudio } from "../studio-store";

export function DeliveryPage() {
  const { state } = useStudio();
  const [playing, setPlaying] = useState(false);
  const complete = state.jobs.every((job) => job.state === "SUCCEEDED");

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <span className="eyebrow">最终成片 · G3</span>
          <h1>看片、核对、锁版</h1>
          <p>批准会冻结成片、字幕、音轨、AI 标识和 Manifest；后续修改只能产生新的 cut revision。</p>
        </div>
        <button className="button button-secondary" type="button" disabled={!complete}>
          <Download size={16} aria-hidden="true" />
          导出审片包
        </button>
      </header>

      <section className="delivery-workspace">
        <div className="player-column">
          <div className="review-player">
            <div className="player-visual">
              <div className="lighthouse-beam" aria-hidden="true" />
              <div className="player-horizon" aria-hidden="true" />
              <span className="player-slate">E03 · CUT R02 · MOCK PREVIEW</span>
              {!complete && (
                <div className="player-blocked">
                  <Film size={24} aria-hidden="true" />
                  <strong>尚未形成完整审片版本</strong>
                  <p>还有 {state.jobs.filter((job) => job.state !== "SUCCEEDED").length} 个镜头未完成。</p>
                </div>
              )}
              {complete && (
                <button
                  className="player-play"
                  type="button"
                  onClick={() => setPlaying((value) => !value)}
                  aria-label={playing ? "暂停预览" : "播放预览"}
                >
                  {playing ? <Pause size={24} aria-hidden="true" /> : <Play size={24} aria-hidden="true" />}
                </button>
              )}
              <div className="subtitle-preview">不是海在涨，是时间在退。</div>
            </div>
            <div className="player-controls">
              <button className="icon-button" type="button" aria-label={playing ? "暂停" : "播放"}>
                {playing ? <Pause size={16} /> : <Play size={16} />}
              </button>
              <span className="mono">00:00:42:18</span>
              <div className="player-scrubber">
                <i style={{ width: "61%" }} />
              </div>
              <span className="mono">00:01:03:18</span>
              <Volume2 size={16} aria-label="音量" />
              <Captions size={16} aria-label="字幕已开启" />
            </div>
          </div>

          <div className="cut-timeline" aria-label="成片镜头时间线">
            <div className="timeline-ruler">
              <span>00:42</span><span>00:47</span><span>00:52</span><span>00:57</span><span>01:03</span>
            </div>
            <div className="video-track">
              {state.shots.map((shot, index) => (
                <div
                  className={`clip clip-${shot.state.toLowerCase()}`}
                  key={shot.id}
                  style={{ flex: shot.duration }}
                >
                  <span>{shot.code}</span>
                  <i style={{ opacity: 0.22 + index * 0.12 }} />
                </div>
              ))}
            </div>
            <div className="audio-track">
              {Array.from({ length: 45 }, (_, index) => (
                <i key={index} style={{ height: `${4 + ((index * 13) % 16)}px` }} />
              ))}
            </div>
            <div className="subtitle-track">
              <span style={{ left: "18%", width: "22%" }}>何津，你听得到吗？</span>
              <span style={{ left: "62%", width: "28%" }}>不是海在涨，是时间在退。</span>
            </div>
          </div>
        </div>

        <aside className="qc-column">
          <div className="subheading-row">
            <div>
              <span className="eyebrow">锁版检查</span>
              <h2>7 / 8 通过</h2>
            </div>
            <ShieldCheck size={21} aria-hidden="true" />
          </div>
          <div className="qc-list">
            {[
              ["画幅与帧率", "1920×1080 · 24fps", true],
              ["镜头连续性", "角色 / 道具 revision 一致", true],
              ["字幕时间轴", "SRT / VTT 均通过", true],
              ["音轨峰值", "-1.2 dBTP", true],
              ["内容安全", "结构校验通过 · Mock", true],
              ["许可与同意", "13/14 权利清晰", false],
              ["AI 内容标识", "片尾和 Manifest 均包含", true],
              ["谱系 Manifest", "hash 已生成", true],
            ].map(([label, detail, passed]) => (
              <div className={passed ? "qc-pass" : "qc-review"} key={String(label)}>
                <span>{passed ? <Check size={14} aria-hidden="true" /> : "!"}</span>
                <div>
                  <strong>{label}</strong>
                  <small>{detail}</small>
                </div>
                <ChevronRight size={14} aria-hidden="true" />
              </div>
            ))}
          </div>
          <div className="qc-warning">
            <strong>潮汐钟 v2 的外部纹样需复核</strong>
            <p>当前成片仍引用已批准的 v1，因此不阻断本 revision；换用 v2 会触发新审查。</p>
          </div>
        </aside>
      </section>

      <section className="gate-section">
        <div className="section-heading">
          <div>
            <span className="eyebrow">最终人工闸门</span>
            <h2>批准成片与谱系清单</h2>
          </div>
        </div>
        <GateCard gate={state.gates.G3} />
      </section>
    </div>
  );
}
