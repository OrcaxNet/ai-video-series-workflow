import { Archive, Box, CheckCircle2, Fingerprint, Image, Link2, Palette, ShieldCheck, Users } from "lucide-react";
import { GateCard } from "../components/gate-card";
import { useStudio } from "../studio-store";

const kindLabel = {
  CHARACTER: "角色",
  LOCATION: "场景",
  PROP: "道具",
};

export function AssetsPage() {
  const { state, actions } = useStudio();
  const selected = state.assets.find((asset) => asset.id === state.selectedAssetId) ?? state.assets[0];

  return (
    <div className="page">
      <header className="page-header">
        <div>
          <span className="eyebrow">内容基座 · G1</span>
          <h1>内容与资产</h1>
          <p>原画是可复用的 production asset；每次引用都指向明确 revision，不追随“最新版”。</p>
        </div>
        <div className="header-stat">
          <strong>14</strong>
          <span>待锁定资产</span>
        </div>
      </header>

      <section className="asset-summary-row" aria-label="资产摘要">
        <div>
          <Users size={18} aria-hidden="true" />
          <strong>6</strong>
          <span>主要角色</span>
        </div>
        <div>
          <Image size={18} aria-hidden="true" />
          <strong>4</strong>
          <span>核心场景</span>
        </div>
        <div>
          <Box size={18} aria-hidden="true" />
          <strong>4</strong>
          <span>叙事道具</span>
        </div>
        <div>
          <ShieldCheck size={18} aria-hidden="true" />
          <strong>13/14</strong>
          <span>权利清晰</span>
        </div>
      </section>

      <section className="asset-workspace">
        <div className="asset-library">
          <div className="subheading-row">
            <div>
              <span className="eyebrow">复用资产库</span>
              <h2>当前 G1 候选</h2>
            </div>
            <div className="segmented" aria-label="资产过滤">
              <button className="active" type="button">全部</button>
              <button type="button">角色</button>
              <button type="button">场景</button>
              <button type="button">道具</button>
            </div>
          </div>
          <div className="asset-grid">
            {state.assets.map((asset, index) => (
              <button
                className={`asset-card ${selected.id === asset.id ? "selected" : ""}`}
                key={asset.id}
                type="button"
                onClick={() => actions.selectAsset(asset.id)}
                aria-pressed={selected.id === asset.id}
              >
                <span
                  className="asset-art"
                  style={{
                    background: `linear-gradient(${135 + index * 11}deg, ${asset.palette[0]}, ${asset.palette[1]} 58%, ${asset.palette[2]})`,
                  }}
                >
                  <span className="asset-contour" aria-hidden="true" />
                  <em>{kindLabel[asset.kind]}</em>
                </span>
                <span className="asset-card-copy">
                  <span>
                    <strong>{asset.name}</strong>
                    <small>v{asset.version}</small>
                  </span>
                  <small>{asset.subtitle}</small>
                  <span className="asset-card-meta">
                    <span className={`tiny-state tiny-${asset.state.toLowerCase()}`}>{asset.state === "APPROVED" ? "已锁定版本可用" : asset.state === "DRAFT" ? "有新草稿" : "需重验"}</span>
                    <span>{asset.usages} 次引用</span>
                  </span>
                </span>
              </button>
            ))}
          </div>
        </div>

        <aside className="asset-detail" aria-label={`${selected.name} 版本详情`}>
          <div className="detail-hero">
            <span
              className="detail-swatch"
              style={{ background: `linear-gradient(145deg, ${selected.palette[0]}, ${selected.palette[1]})` }}
            />
            <div>
              <span className="eyebrow">{kindLabel[selected.kind]}资产</span>
              <h2>{selected.name}</h2>
              <p>{selected.subtitle}</p>
            </div>
          </div>
          <div className="integrity-list">
            <div>
              <Fingerprint size={16} aria-hidden="true" />
              <span>
                <small>当前引用</small>
                <strong className="mono">{selected.revisionId}</strong>
              </span>
            </div>
            <div>
              <Link2 size={16} aria-hidden="true" />
              <span>
                <small>下游使用</small>
                <strong>{selected.usages} 个镜头 / Prompt</strong>
              </span>
            </div>
            <div>
              <ShieldCheck size={16} aria-hidden="true" />
              <span>
                <small>权利状态</small>
                <strong>{selected.rights === "cleared" ? "已绑定许可快照" : "1 项需人工复核"}</strong>
              </span>
            </div>
          </div>
          <div className="revision-list">
            <div className="subheading-row">
              <h3>版本链</h3>
              <span>{selected.revisions.length} 个 revision</span>
            </div>
            {selected.revisions
              .slice()
              .reverse()
              .map((revision) => (
                <div className="revision-item" key={revision.revisionId}>
                  <span className={`revision-dot state-${revision.state.toLowerCase()}`} />
                  <div>
                    <strong>v{revision.version} · {revision.state}</strong>
                    <p>{revision.note}</p>
                    <small>{revision.createdAt} · {revision.revisionId}</small>
                  </div>
                </div>
              ))}
          </div>
          <button className="button button-secondary full-width" type="button">
            <Archive size={15} aria-hidden="true" />
            查看影响与回滚
          </button>
        </aside>
      </section>

      <section className="palette-note">
        <Palette size={18} aria-hidden="true" />
        <div>
          <strong>风格锚点来自剧集上下文 r4</strong>
          <p>青灰主调，钠灯琥珀只作为叙事线索；新资产 v5 不会自动替换已批准镜头中的 v4。</p>
        </div>
        <CheckCircle2 size={18} aria-label="继承校验通过" />
      </section>

      <section className="gate-section">
        <div className="section-heading">
          <div>
            <span className="eyebrow">提交人工闸门</span>
            <h2>锁定内容和资产 revision</h2>
          </div>
        </div>
        <GateCard gate={state.gates.G1} />
      </section>
    </div>
  );
}
