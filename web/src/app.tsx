import {
  Activity,
  Aperture,
  Boxes,
  ChevronDown,
  CircleHelp,
  CloudOff,
  Eye,
  Film,
  LayoutDashboard,
  Menu,
  Network,
  PanelRightClose,
  PanelRightOpen,
  Search,
  Settings2,
  ShieldCheck,
  X,
} from "lucide-react";
import {
  type ComponentType,
  type RefObject,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import type { CreateProjectInput, ViewId } from "./domain";
import { useStudio } from "./studio-store";
import { AssetsPage } from "./pages/assets";
import { DeliveryPage } from "./pages/delivery";
import { JobsPage } from "./pages/jobs";
import { LineagePage } from "./pages/lineage";
import { OverviewPage } from "./pages/overview";
import { StoryboardPage } from "./pages/storyboard";

const navigation: Array<{
  id: ViewId;
  label: string;
  shortLabel: string;
  icon: typeof LayoutDashboard;
  gate?: string;
}> = [
  { id: "overview", label: "片场总览", shortLabel: "总览", icon: LayoutDashboard },
  { id: "assets", label: "内容与资产", shortLabel: "资产", icon: Boxes, gate: "G1" },
  { id: "storyboard", label: "剧本与分镜", shortLabel: "分镜", icon: Aperture, gate: "G2" },
  { id: "jobs", label: "任务中心", shortLabel: "任务", icon: Activity },
  { id: "delivery", label: "成片审核", shortLabel: "成片", icon: Film, gate: "G3" },
  { id: "lineage", label: "版本与谱系", shortLabel: "谱系", icon: Network },
];

const pages: Record<ViewId, ComponentType> = {
  overview: OverviewPage,
  assets: AssetsPage,
  storyboard: StoryboardPage,
  jobs: JobsPage,
  delivery: DeliveryPage,
  lineage: LineagePage,
};

export function App() {
  const { state, actions } = useStudio();
  const [projectDialogOpen, setProjectDialogOpen] = useState(false);
  const projectSwitcherRef = useRef<HTMLButtonElement>(null);
  const closeProjectDialog = useCallback(() => setProjectDialogOpen(false), []);
  const CurrentPage = pages[state.view];
  const agentPlanConfigured = state.capabilities.some((capability) => capability.liveConfigured);

  return (
    <div className={`studio-shell ${state.inspectorOpen ? "with-inspector" : ""}`}>
      <a className="skip-link" href="#studio-main">
        跳到主要内容
      </a>
      <header className="studio-topbar">
        <div className="brand-lockup">
          <button className="mobile-menu-button" type="button" aria-label="打开导航">
            <Menu size={19} />
          </button>
          <span className="brand-mark" aria-hidden="true">
            <i />
            <i />
          </span>
          <strong>场记</strong>
          <span>AI 剧集创作操作台</span>
        </div>
        <button
          ref={projectSwitcherRef}
          className="project-switcher"
          type="button"
          aria-label="切换或新建项目"
          onClick={() => setProjectDialogOpen(true)}
        >
          <span className="project-thumb">潮</span>
          <span>
            <strong>{state.project.title}</strong>
            <small>{state.project.episode} · {state.project.episodeTitle}</small>
          </span>
          <ChevronDown size={14} aria-hidden="true" />
        </button>
        <div className="topbar-actions">
          <div
            className="mode-pill"
            title={
              agentPlanConfigured
                ? "Agent Plan 凭据仅注入后端；当前界面操作仍是可复现演练"
                : "当前不进行真实 Provider 调用"
            }
          >
            {agentPlanConfigured ? <ShieldCheck size={14} aria-hidden="true" /> : <CloudOff size={14} aria-hidden="true" />}
            <span>
              <strong>{agentPlanConfigured ? "Agent Plan 已配置" : "Mock only"}</strong>
              <small>{agentPlanConfigured ? "交互仍为安全演练" : "live pending_key"}</small>
            </span>
          </div>
          <button className="icon-button" type="button" aria-label="搜索">
            <Search size={17} />
          </button>
          <button className="icon-button" type="button" aria-label="帮助">
            <CircleHelp size={17} />
          </button>
          <button
            className={`icon-button ${state.inspectorOpen ? "active" : ""}`}
            type="button"
            aria-label={state.inspectorOpen ? "关闭证据检查器" : "打开证据检查器"}
            aria-pressed={state.inspectorOpen}
            onClick={() => actions.toggleInspector()}
          >
            {state.inspectorOpen ? <PanelRightClose size={17} /> : <PanelRightOpen size={17} />}
          </button>
          <span className="avatar" title="本地导演">岚</span>
        </div>
      </header>

      <aside className="studio-sidebar">
        <nav aria-label="生产工作区">
          <span className="nav-section-label">制作流程</span>
          {navigation.map((item) => {
            const Icon = item.icon;
            const gate = item.gate ? state.gates[item.gate as "G1" | "G2" | "G3"] : undefined;
            return (
              <button
                type="button"
                className={state.view === item.id ? "active" : ""}
                onClick={() => actions.navigate(item.id)}
                aria-current={state.view === item.id ? "page" : undefined}
                key={item.id}
              >
                <Icon size={17} aria-hidden="true" />
                <span>{item.label}</span>
                {item.gate && <small className={`nav-gate nav-gate-${gate?.state.toLowerCase()}`}>{item.gate}</small>}
              </button>
            );
          })}
        </nav>
        <div className="sidebar-footer">
          <button type="button">
            <Settings2 size={17} aria-hidden="true" />
            <span>项目设置</span>
          </button>
          <div className="production-baseline">
            <ShieldCheck size={15} aria-hidden="true" />
            <span>
              <strong>API-first 基线</strong>
              <small>无需本地 GPU</small>
            </span>
          </div>
        </div>
      </aside>

      <main id="studio-main" className="studio-main" tabIndex={-1}>
        <CurrentPage />
      </main>

      {state.inspectorOpen && <EvidenceInspector />}

      <nav className="mobile-bottom-nav" aria-label="移动端生产工作区">
        {navigation.map((item) => {
          const Icon = item.icon;
          return (
            <button
              key={item.id}
              type="button"
              onClick={() => actions.navigate(item.id)}
              className={state.view === item.id ? "active" : ""}
              aria-current={state.view === item.id ? "page" : undefined}
            >
              <Icon size={17} aria-hidden="true" />
              <span>{item.shortLabel}</span>
            </button>
          );
        })}
      </nav>

      <div className="toast-region" aria-live="polite" aria-label="操作通知">
        {state.toasts.map((toast) => (
          <div className={`toast toast-${toast.tone}`} key={toast.id}>
            <div>
              <strong>{toast.title}</strong>
              <p>{toast.description}</p>
            </div>
            <button type="button" onClick={() => actions.dismissToast(toast.id)} aria-label="关闭通知">
              <X size={14} />
            </button>
          </div>
        ))}
      </div>
      {projectDialogOpen && (
        <ProjectDialog onClose={closeProjectDialog} returnFocusRef={projectSwitcherRef} />
      )}
    </div>
  );
}

function ProjectDialog({
  onClose,
  returnFocusRef,
}: {
  onClose(): void;
  returnFocusRef: RefObject<HTMLButtonElement | null>;
}) {
  const { state, actions } = useStudio();
  const dialogRef = useRef<HTMLElement>(null);
  const firstInputRef = useRef<HTMLInputElement>(null);
  const [draft, setDraft] = useState<CreateProjectInput>({
    title: "潮汐失语者",
    sourceText: "台风登陆前，观测员在一座停止报时的灯塔里收到来自三年前的求救信号。",
    aspectRatio: "16:9",
    targetDuration: "03:20",
    targetAudience: "16–35 岁悬疑剧观众",
    visualStyle: "近未来沿海现实主义",
  });

  const update = <Key extends keyof CreateProjectInput>(key: Key, value: CreateProjectInput[Key]) =>
    setDraft((current) => ({ ...current, [key]: value }));

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!dialog) return;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    firstInputRef.current?.focus();

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        onClose();
        return;
      }
      if (event.key !== "Tab") return;
      const focusable = Array.from(
        dialog.querySelectorAll<HTMLElement>(
          'button:not([disabled]), input:not([disabled]), textarea:not([disabled]), select:not([disabled]), [href], [tabindex]:not([tabindex="-1"])',
        ),
      ).filter((element) => !element.hasAttribute("hidden"));
      if (focusable.length === 0) {
        event.preventDefault();
        dialog.focus();
        return;
      }
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };

    dialog.addEventListener("keydown", handleKeyDown);
    return () => {
      dialog.removeEventListener("keydown", handleKeyDown);
      document.body.style.overflow = previousOverflow;
      returnFocusRef.current?.focus();
    };
  }, [onClose, returnFocusRef]);

  return (
    <div className="dialog-backdrop" role="presentation">
      <section
        ref={dialogRef}
        className="project-dialog"
        role="dialog"
        aria-modal="true"
        aria-labelledby="create-project-title"
        tabIndex={-1}
      >
        <div className="dialog-header">
          <div>
            <span className="eyebrow">新建制作项目</span>
            <h2 id="create-project-title">从原作建立可追溯剧集</h2>
            <p>先记录创作意图；下一步仍需在 G1 人工确认提取出的内容和资产。</p>
          </div>
          <button className="icon-button" type="button" onClick={onClose} aria-label="关闭新建项目">
            <X size={17} />
          </button>
        </div>
        <form
          onSubmit={(event) => {
            event.preventDefault();
            void actions.createProject(draft).then(onClose);
          }}
        >
          <div className="dialog-form-grid">
            <label className="field field-wide">
              <span>项目名称</span>
              <input
                ref={firstInputRef}
                required
                value={draft.title}
                onChange={(event) => update("title", event.target.value)}
                placeholder="例如：潮汐失语者"
              />
            </label>
            <label className="field field-wide">
              <span>原作输入</span>
              <textarea
                required
                rows={5}
                value={draft.sourceText}
                onChange={(event) => update("sourceText", event.target.value)}
                placeholder="粘贴小说全文或本次 PoC 使用的章节摘要"
              />
              <small>PoC 仅保存在当前内存中；正式接入由 source revision API 归档至 CAS。</small>
            </label>
            <label className="field">
              <span>视觉风格</span>
              <input value={draft.visualStyle} onChange={(event) => update("visualStyle", event.target.value)} />
            </label>
            <label className="field">
              <span>目标观众</span>
              <input value={draft.targetAudience} onChange={(event) => update("targetAudience", event.target.value)} />
            </label>
            <label className="field">
              <span>屏幕比例</span>
              <select value={draft.aspectRatio} onChange={(event) => update("aspectRatio", event.target.value)}>
                <option>16:9</option>
                <option>9:16</option>
                <option>1:1</option>
                <option>2.39:1</option>
              </select>
            </label>
            <label className="field">
              <span>单集目标时长</span>
              <input
                value={draft.targetDuration}
                onChange={(event) => update("targetDuration", event.target.value)}
                pattern="[0-9]{2}:[0-9]{2}"
                aria-describedby="duration-hint"
              />
              <small id="duration-hint">格式 mm:ss</small>
            </label>
          </div>
          <div className="rights-declaration">
            <ShieldCheck size={17} aria-hidden="true" />
            <p>
              <strong>权利声明会绑定 source revision</strong>
              我确认有权将以上内容用于本项目的 AI 辅助改编与生成排练。
            </p>
            <input type="checkbox" required aria-label="确认原作权利声明" defaultChecked />
          </div>
          <div className="dialog-actions">
            <button className="button button-secondary" type="button" onClick={onClose}>取消</button>
            <button className="button button-primary" type="submit" disabled={state.busy}>
              {state.busy ? "创建中…" : "创建并生成结构草稿"}
            </button>
          </div>
        </form>
      </section>
    </div>
  );
}

function EvidenceInspector() {
  const { state, actions } = useStudio();
  const currentGate =
    state.view === "assets"
      ? state.gates.G1
      : state.view === "storyboard"
        ? state.gates.G2
        : state.view === "delivery"
          ? state.gates.G3
          : undefined;

  return (
    <aside className="evidence-inspector" aria-label="证据与上下文检查器">
      <div className="inspector-header">
        <div>
          <span className="eyebrow">只读检查器</span>
          <h2>证据与上下文</h2>
        </div>
        <button className="icon-button" type="button" onClick={() => actions.toggleInspector()} aria-label="关闭证据检查器">
          <X size={16} />
        </button>
      </div>

      {state.lastProblem && (
        <section className="inspector-problem" aria-label="最后一个 API 错误">
          <span>{state.lastProblem.status}</span>
          <div>
            <strong>{state.lastProblem.errorCode}</strong>
            <p>{state.lastProblem.message}</p>
            <small>trace · {state.lastProblem.traceId}</small>
          </div>
        </section>
      )}

      <section className="inspector-section">
        <div className="inspector-title">
          <CloudOff size={15} aria-hidden="true" />
          <strong>Provider 边界</strong>
        </div>
        <div className="fact-list">
          <div><span>执行模式</span><strong>dry-run / Mock</strong></div>
          <div><span>真实调用</span><strong className="text-warning">pending_key</strong></div>
          <div><span>浏览器 Secret</span><strong className="text-success">0</strong></div>
          <div><span>证据级别</span><strong>mock_only</strong></div>
        </div>
        <p className="inspector-help">Mock 结果不用于声明真实质量、时延、成功率或费用。</p>
      </section>

      {currentGate && (
        <section className="inspector-section">
          <div className="inspector-title">
            <Eye size={15} aria-hidden="true" />
            <strong>{currentGate.id} 精确绑定</strong>
          </div>
          <div className="inspector-bindings">
            {currentGate.bindings.map((binding) => (
              <div key={binding.revisionId}>
                <span>{binding.label}</span>
                <code>{binding.revisionId}</code>
                <small>{binding.contentHash.slice(0, 12)}…</small>
              </div>
            ))}
          </div>
        </section>
      )}

      <section className="inspector-section">
        <div className="inspector-title">
          <Aperture size={15} aria-hidden="true" />
          <strong>当前上下文</strong>
        </div>
        <div className="scope-chips">
          <span>剧集 r4</span>
          <span>单集 r6</span>
          <span>场景 r3</span>
          <span>镜头 r2</span>
        </div>
        <div className="fact-list">
          <div><span>解析器</span><strong>context-resolver/v1</strong></div>
          <div><span>Prompt</span><strong>r{state.selectedPromptVersion}</strong></div>
          <div><span>资产</span><strong>4 项锁定</strong></div>
        </div>
      </section>

      <section className="inspector-section inspector-activity">
        <div className="inspector-title">
          <Activity size={15} aria-hidden="true" />
          <strong>最近审计</strong>
        </div>
        {state.activity.slice(0, 5).map((item) => (
          <div className="activity-item" key={item.id}>
            <span className="mono">{item.at}</span>
            <div>
              <strong>{item.action}</strong>
              <p>{item.detail}</p>
              <small>{item.actor}</small>
            </div>
          </div>
        ))}
      </section>
    </aside>
  );
}
