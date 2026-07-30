import {
  ArrowDown,
  Check,
  Copy,
  CornerDownLeft,
  FileJson2,
  GitCompareArrows,
  Layers3,
  LockKeyhole,
  Network,
  RotateCcw,
} from "lucide-react";
import { useStudio } from "../studio-store";

const scopeLabel = {
  SERIES: "剧集",
  EPISODE: "单集",
  SCENE: "场景",
  SHOT: "镜头",
};

export function LineagePage() {
  const { state, actions } = useStudio();
  const current = state.promptRevisions.find((item) => item.version === state.selectedPromptVersion)!;
  const locked = state.promptRevisions.find((item) => item.state === "LOCKED")!;

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <span className="eyebrow">上下文 / 版本 / Manifest</span>
          <h1>从一句 Prompt 追溯到每个决定</h1>
          <p>切换、差异和回滚都只改变业务引用；已批准 revision、产物与成本记录保持不可变。</p>
        </div>
        <button className="button button-secondary" type="button">
          <FileJson2 size={16} aria-hidden="true" />
          查看 Manifest JSON
        </button>
      </header>

      <section className="lineage-graph" aria-label="S03-03 生成谱系">
        <div className="subheading-row">
          <div>
            <span className="eyebrow">生成谱系</span>
            <h2>S03-03 · 断针回转</h2>
          </div>
          <span className="manifest-hash mono">manifest · b4e9…81d2</span>
        </div>
        <div className="lineage-columns">
          <div className="lineage-column">
            <span className="lineage-label">输入</span>
            <div className="lineage-node node-source">
              <span>小说原文</span>
              <strong>source r3</strong>
              <small>sha256 · 4ea1…f903</small>
            </div>
            <div className="lineage-node node-asset">
              <span>资产绑定</span>
              <strong>潮汐钟 v1</strong>
              <small>G1 approved</small>
            </div>
          </div>
          <ArrowDown className="lineage-arrow" size={20} aria-hidden="true" />
          <div className="lineage-column">
            <span className="lineage-label">解析</span>
            <div className="lineage-node node-context">
              <span>有效上下文</span>
              <strong>4 层 snapshot</strong>
              <small>{state.shots[2].contextHash.slice(0, 14)}…</small>
            </div>
            <div className="lineage-node node-prompt">
              <span>Prompt</span>
              <strong>revision 4</strong>
              <small>501 tokens</small>
            </div>
          </div>
          <ArrowDown className="lineage-arrow" size={20} aria-hidden="true" />
          <div className="lineage-column">
            <span className="lineage-label">执行</span>
            <div className="lineage-node node-provider">
              <span>Provider Job</span>
              <strong>job-v-033</strong>
              <small>route-v4 · mock_only</small>
            </div>
            <div className="lineage-node node-cost">
              <span>成本记录</span>
              <strong>¥0.42 估算</strong>
              <small>verified=false</small>
            </div>
          </div>
          <ArrowDown className="lineage-arrow" size={20} aria-hidden="true" />
          <div className="lineage-column">
            <span className="lineage-label">产物</span>
            <div className="lineage-node node-output">
              <span>镜头产物</span>
              <strong>CAS artifact</strong>
              <small>sha256 · 68cf…0bd1</small>
            </div>
            <div className="lineage-node node-manifest">
              <LockKeyhole size={13} aria-hidden="true" />
              <strong>等待 G3</strong>
              <small>episode manifest</small>
            </div>
          </div>
        </div>
      </section>

      <section className="version-workspace">
        <div className="context-version-panel">
          <div className="subheading-row">
            <div>
              <span className="eyebrow">四级上下文</span>
              <h2>继承与覆盖</h2>
            </div>
            <Layers3 size={20} aria-hidden="true" />
          </div>
          <div className="inheritance-stack">
            {state.contextLayers.map((layer, index) => (
              <article className="inheritance-card" key={layer.revisionId}>
                <div className="inheritance-head">
                  <span>{scopeLabel[layer.scope]}</span>
                  <strong>{layer.label.split(" · ")[1]}</strong>
                  <small>r{layer.revision}</small>
                </div>
                <div className="inheritance-items">
                  {layer.entries.map((entry) => (
                    <p key={entry.key}>
                      <span>{entry.key}</span>
                      <strong>{entry.value}</strong>
                      {entry.override && <em>覆盖上层</em>}
                    </p>
                  ))}
                </div>
                {index < state.contextLayers.length - 1 && (
                  <span className="inheritance-connector" aria-hidden="true">
                    <CornerDownLeft size={14} /> 继承
                  </span>
                )}
              </article>
            ))}
          </div>
        </div>

        <div className="prompt-version-panel">
          <div className="subheading-row">
            <div>
              <span className="eyebrow">Prompt revision</span>
              <h2>选择与差异</h2>
            </div>
            <GitCompareArrows size={20} aria-hidden="true" />
          </div>
          <div className="version-tabs" role="tablist" aria-label="Prompt 版本">
            {state.promptRevisions.map((revision) => (
              <button
                type="button"
                role="tab"
                aria-selected={revision.version === current.version}
                className={revision.version === current.version ? "active" : ""}
                key={revision.revisionId}
                onClick={() => actions.selectPrompt(revision.version)}
              >
                <span>r{revision.version}</span>
                <small>{revision.state}</small>
              </button>
            ))}
          </div>
          <div className="prompt-meta">
            <div>
              <span>revision</span>
              <strong className="mono">{current.revisionId}</strong>
            </div>
            <div>
              <span>状态</span>
              <strong>{current.state}</strong>
            </div>
            <div>
              <span>长度</span>
              <strong>{current.tokens} tokens</strong>
            </div>
          </div>
          <div className="prompt-content">
            <p>{current.content}</p>
            <button className="icon-button" type="button" aria-label="复制 Prompt">
              <Copy size={15} aria-hidden="true" />
            </button>
          </div>
          <div className="diff-block">
            <div className="diff-heading">
              <strong>与 G2 锁定版 r{locked.version} 比较</strong>
              <span>{current.changes.length} 项变化</span>
            </div>
            {current.changes.length === 0 ? (
              <p className="no-diff">
                <Check size={14} aria-hidden="true" /> 当前就是锁定版，没有差异。
              </p>
            ) : (
              current.changes.map((change, index) => (
                <p className={`diff-line diff-${change.type}`} key={`${change.type}-${index}`}>
                  <span>{change.type === "add" ? "+" : "−"}</span>
                  {change.text}
                </p>
              ))
            )}
          </div>
          <div className="rollback-row">
            <div>
              <strong>回滚不会删除 r{current.version}</strong>
              <p>仅把下一次生成的引用改回 r{locked.version}，历史 Manifest 仍保持原样。</p>
            </div>
            <button className="button button-secondary" type="button" onClick={() => actions.selectPrompt(locked.version)}>
              <RotateCcw size={15} aria-hidden="true" />
              选择 r{locked.version}
            </button>
          </div>
        </div>
      </section>

      <section className="manifest-summary">
        <div>
          <Network size={20} aria-hidden="true" />
          <span>
            <small>Manifest 状态</small>
            <strong>{state.gates.G3.state === "APPROVED" ? "LOCKED" : "DRAFT · 等待 G3"}</strong>
          </span>
        </div>
        <div>
          <span>
            <small>输入 / 输出</small>
            <strong>7 / 4 个 CAS hash</strong>
          </span>
        </div>
        <div>
          <span>
            <small>Provider 证据</small>
            <strong>5 个 mock_only</strong>
          </span>
        </div>
        <div>
          <span>
            <small>Secret 字段</small>
            <strong>0 项</strong>
          </span>
        </div>
      </section>
    </div>
  );
}
