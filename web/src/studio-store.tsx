import {
  createContext,
  type PropsWithChildren,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useReducer,
  useRef,
} from "react";
import { createControlPlaneApi, type ControlPlaneApi } from "./api/control-plane";
import {
  ApiProblem,
  type ApprovalResult,
  type CreateProjectInput,
  type CreateProjectResult,
  type Decision,
  type GateId,
  type JobErrorCode,
  type JobsViewState,
  type ProviderJob,
  type RegenerationResult,
  type StudioState,
  type ToastMessage,
  type ViewId,
} from "./domain";
import { createInitialState } from "./mock-data";

type Action =
  | { type: "NAVIGATE"; view: ViewId }
  | { type: "TOGGLE_INSPECTOR" }
  | { type: "CAPABILITIES_LOADED"; capabilities: StudioState["capabilities"] }
  | { type: "PROJECT_CREATED"; input: CreateProjectInput; result: CreateProjectResult }
  | { type: "BUSY"; busy: boolean }
  | { type: "GATE_DECIDED"; result: ApprovalResult; explanation: string }
  | { type: "GATE_REGENERATED"; result: RegenerationResult }
  | { type: "GATE_SYNCHRONIZED"; gateId: GateId; gate: StudioState["gates"][GateId] }
  | { type: "COMPLETE_MOCK_RUN" }
  | { type: "INJECT_SCENARIO"; scenario: FailureScenario }
  | { type: "CANCEL_JOB"; jobId: string }
  | { type: "CONFIRM_CANCEL_JOB"; jobId: string }
  | { type: "RETRY_JOB"; jobId: string }
  | { type: "SET_JOBS_VIEW_STATE"; viewState: JobsViewState }
  | { type: "SELECT_PROMPT"; version: number }
  | { type: "SELECT_ASSET"; assetId: string }
  | { type: "LOCK_ASSET_REVISION"; assetId: string; version: number }
  | { type: "PROBLEM"; problem?: ApiProblem }
  | { type: "TOAST"; toast: Omit<ToastMessage, "id"> }
  | { type: "DISMISS_TOAST"; id: number };

export type FailureScenario =
  | "unauthorized"
  | "forbidden"
  | "rate_limited"
  | "quota_exceeded"
  | "budget_exceeded"
  | "provider_unavailable"
  | "timeout"
  | "terminal_failure"
  | "cancelled"
  | "duplicate_callback"
  | "out_of_order_callback"
  | "cancel_race";

const scenarioFailure: Partial<Record<FailureScenario, ProviderJob["failure"]>> = {
  unauthorized: {
    errorCode: "unauthenticated",
    retryable: false,
    requiresAction: true,
    httpStatus: 401,
    suggestedAction: "在服务端 Secret Store 配置凭据；浏览器不会接收或保存 Key。",
  },
  forbidden: {
    errorCode: "forbidden",
    retryable: false,
    requiresAction: true,
    httpStatus: 403,
    suggestedAction: "确认模型、Endpoint 与区域权限后创建新 route snapshot。",
  },
  rate_limited: {
    errorCode: "rate_limited",
    retryable: true,
    requiresAction: false,
    httpStatus: 429,
    retryAfterSeconds: 30,
    suggestedAction: "保留相同 Job ID，等待 Retry-After 后重试，最多 3 次。",
  },
  quota_exceeded: {
    errorCode: "quota_exceeded",
    retryable: false,
    requiresAction: true,
    httpStatus: 429,
    suggestedAction: "检查 Provider 账户配额并调整路由；不要在配额恢复前重复提交。",
  },
  budget_exceeded: {
    errorCode: "budget_exceeded",
    retryable: false,
    requiresAction: true,
    httpStatus: 402,
    suggestedAction: "由制作负责人提高批次预算上限，或降低镜头成本后重新批准。",
  },
  provider_unavailable: {
    errorCode: "provider_unavailable",
    retryable: true,
    requiresAction: false,
    httpStatus: 503,
    suggestedAction: "保留相同 Job ID 退避重试；不创建第二个付费任务。",
  },
  timeout: {
    errorCode: "timeout",
    retryable: false,
    requiresAction: true,
    httpStatus: 504,
    suggestedAction: "任务状态未知，使用上游 task ID 对账，不要盲目重新提交。",
  },
  terminal_failure: {
    errorCode: "invalid_request",
    retryable: false,
    requiresAction: true,
    httpStatus: 400,
    suggestedAction: "修正请求参数并创建新的创作 attempt；当前任务保持失败终态。",
  },
};

const toast = (state: StudioState, value: Omit<ToastMessage, "id">) => ({
  ...state,
  toasts: [...state.toasts.slice(-2), { ...value, id: Date.now() + state.toasts.length }],
});

const nowLabel = () =>
  new Intl.DateTimeFormat("zh-CN", { hour: "2-digit", minute: "2-digit", hour12: false }).format(new Date());

function reducer(state: StudioState, action: Action): StudioState {
  switch (action.type) {
    case "NAVIGATE":
      return { ...state, view: action.view };
    case "TOGGLE_INSPECTOR":
      return { ...state, inspectorOpen: !state.inspectorOpen };
    case "CAPABILITIES_LOADED":
      return { ...state, capabilities: action.capabilities };
    case "PROJECT_CREATED": {
      const seed = createInitialState();
      return toast(
        {
          ...seed,
          inspectorOpen: state.inspectorOpen,
          capabilities: state.capabilities,
          busy: false,
          project: {
            ...seed.project,
            id: action.result.seriesId,
            title: action.input.title,
            aspectRatio: action.input.aspectRatio,
            targetDuration: action.input.targetDuration,
            targetAudience: action.input.targetAudience,
            visualStyle: action.input.visualStyle,
          },
          activity: [
            {
              id: action.result.operationId,
              at: nowLabel(),
              actor: "本地创作者",
              action: "创建项目",
              detail: `${action.input.title} · 原作已进入结构化 Mock 排练`,
            },
            ...seed.activity,
          ],
        },
        {
          tone: "success",
          title: `已创建《${action.input.title}》`,
          description: "原作、补充信息和权利声明已形成创建操作；现在从 G1 开始核对。",
        },
      );
    }
    case "BUSY":
      return { ...state, busy: action.busy };
    case "PROBLEM":
      return { ...state, lastProblem: action.problem };
    case "TOAST":
      return toast(state, action.toast);
    case "DISMISS_TOAST":
      return { ...state, toasts: state.toasts.filter((item) => item.id !== action.id) };
    case "GATE_DECIDED": {
      const { gate: gateId, decision, decidedAt, expectedRevision, decisionId } = action.result;
      const gate = state.gates[gateId];
      const nextGates = {
        ...state.gates,
        [gateId]: {
          ...gate,
          state: decision,
          etag: expectedRevision,
          decidedBy: "本地导演",
          decidedAt,
          explanation: action.explanation,
          history: gate.history.map((record) =>
            record.revision === gate.revision
              ? { ...record, state: decision === "APPROVED" ? ("APPROVED" as const) : ("RETURNED" as const) }
              : record,
          ),
        },
      };

      if (decision === "APPROVED" && gateId === "G1") {
        nextGates.G2 = { ...nextGates.G2, state: "PENDING" };
      }
      if (decision === "APPROVED" && gateId === "G2") {
        const allFinished = state.jobs.every((job) => job.state === "SUCCEEDED");
        nextGates.G3 = { ...nextGates.G3, state: allFinished ? "PENDING" : "BLOCKED" };
      }
      if (decision === "RETURNED" && gateId === "G1") {
        nextGates.G2 = { ...nextGates.G2, state: "BLOCKED" };
        nextGates.G3 = { ...nextGates.G3, state: "BLOCKED" };
      }
      if (decision === "RETURNED" && gateId === "G2") {
        nextGates.G3 = { ...nextGates.G3, state: "BLOCKED" };
      }

      const next = {
        ...state,
        busy: false,
        gates: nextGates,
        lastProblem: undefined,
        activity: [
          {
            id: decisionId,
            at: nowLabel(),
            actor: "本地导演",
            action: decision === "APPROVED" ? `批准 ${gateId}` : `退回 ${gateId}`,
            detail: `${gate.revisionId} · ${action.explanation || "未填写补充说明"}`,
          },
          ...state.activity,
        ],
      };
      return toast(next, {
        tone: decision === "APPROVED" ? "success" : "warning",
        title: decision === "APPROVED" ? `${gateId} 已批准并锁定` : `${gateId} 已退回`,
        description:
          decision === "APPROVED"
            ? "决策只绑定当前 revision；后续修改会创建新版本。"
            : "历史 revision 已保留，可根据意见重新生成。",
      });
    }
    case "GATE_REGENERATED": {
      const gate = state.gates[action.result.gate];
      const oldHistory = gate.history.map((record) =>
        record.revision === gate.revision && record.state === "CURRENT"
          ? { ...record, state: "SUPERSEDED" as const }
          : record,
      );
      const nextGates = {
        ...state.gates,
        [action.result.gate]: {
          ...gate,
          revision: action.result.revision,
          revisionId: action.result.revisionId,
          etag: action.result.etag,
          state: "PENDING" as const,
          decidedAt: undefined,
          decidedBy: undefined,
          explanation: undefined,
          history: [
            ...oldHistory,
            {
              revision: action.result.revision,
              revisionId: action.result.revisionId,
              state: "CURRENT" as const,
              author: "创作编排",
              createdAt: nowLabel(),
              note: "根据审核意见创建，不覆盖上一版",
            },
          ],
        },
      };
      if (action.result.gate === "G1") {
        nextGates.G2 = { ...nextGates.G2, state: "BLOCKED" };
        nextGates.G3 = { ...nextGates.G3, state: "BLOCKED" };
      } else if (action.result.gate === "G2") {
        nextGates.G3 = { ...nextGates.G3, state: "BLOCKED" };
      }
      return toast(
        { ...state, busy: false, gates: nextGates, lastProblem: undefined },
        {
          tone: "info",
          title: `已创建 ${action.result.gate} revision ${action.result.revision}`,
          description: "被批准或退回的旧版本保持只读，新的 revision 等待审核。",
        },
      );
    }
    case "GATE_SYNCHRONIZED":
      return toast(
        {
          ...state,
          busy: false,
          lastProblem: undefined,
          gates: { ...state.gates, [action.gateId]: action.gate },
        },
        {
          tone: "success",
          title: `${action.gateId} 已同步到 ETag ${action.gate.etag}`,
          description: "请检查最新 revision 与绑定差异，再重新提交审核或创建新版本。",
        },
      );
    case "COMPLETE_MOCK_RUN": {
      const jobs = state.jobs.map((job) => ({
        ...job,
        state: "SUCCEEDED" as const,
        progress: 100,
        failure: undefined,
        updatedAt: nowLabel(),
      }));
      const nextGates = {
        ...state.gates,
        G3: {
          ...state.gates.G3,
          state: state.gates.G2.state === "APPROVED" ? ("PENDING" as const) : ("BLOCKED" as const),
        },
      };
      return toast(
        {
          ...state,
          jobs,
          jobsViewState: "READY",
          gates: nextGates,
          activity: [
            {
              id: `act-run-${Date.now()}`,
              at: nowLabel(),
              actor: "Provider Mock",
              action: "完成排练",
              detail: "5 个镜头均生成固定 fixture；证据仍标记 mock_only",
            },
            ...state.activity,
          ],
        },
        {
          tone: "success",
          title: "Mock 排练已完成",
          description: "这只验证编排与界面，不代表真实质量、时延、成功率或费用。",
        },
      );
    }
    case "INJECT_SCENARIO": {
      const current = state.jobs[1];
      const jobs = state.jobs.map((job, index) => {
        if (index !== 1) return job;
        if (action.scenario === "duplicate_callback") {
          return { ...job, state: current.state, updatedAt: nowLabel() };
        }
        if (action.scenario === "out_of_order_callback") {
          return { ...job, state: "RUNNING" as const, progress: Math.max(job.progress, 64), updatedAt: nowLabel() };
        }
        if (action.scenario === "cancel_race") {
          return {
            ...job,
            state: "SUCCEEDED" as const,
            progress: 100,
            failure: undefined,
            updatedAt: nowLabel(),
          };
        }
        if (action.scenario === "cancelled") {
          return {
            ...job,
            state: "CANCELLED" as const,
            progress: Math.min(job.progress, 64),
            failure: undefined,
            updatedAt: nowLabel(),
          };
        }
        const failure = scenarioFailure[action.scenario];
        const stateByScenario: Partial<Record<FailureScenario, ProviderJob["state"]>> = {
          unauthorized: "REQUIRES_ACTION",
          forbidden: "REQUIRES_ACTION",
          rate_limited: "RETRYING",
          quota_exceeded: "REQUIRES_ACTION",
          budget_exceeded: "REQUIRES_ACTION",
          provider_unavailable: "RETRYING",
          timeout: "UNKNOWN",
          terminal_failure: "FAILED",
        };
        return {
          ...job,
          state: stateByScenario[action.scenario] ?? "FAILED",
          failure,
          retryCount:
            action.scenario === "rate_limited" || action.scenario === "provider_unavailable"
              ? Math.min(job.retryCount + 1, 3)
              : job.retryCount,
          updatedAt: nowLabel(),
        };
      });
      const descriptions: Record<FailureScenario, string> = {
        unauthorized: "401 已映射为需要配置服务端凭据。",
        forbidden: "403 已映射为模型或区域权限问题。",
        rate_limited: "429 遵循 Retry-After，同 Job ID 重试。",
        quota_exceeded: "Provider 配额不足，任务等待人工调整路由。",
        budget_exceeded: "批次预算上限已阻断新的付费提交。",
        provider_unavailable: "5xx 进入退避重试，不重复计费提交。",
        timeout: "超时进入 UNKNOWN，等待对账而不是盲目重提。",
        terminal_failure: "无效请求进入 FAILED 终态，不自动创建付费重试。",
        cancelled: "Provider 已确认取消，任务进入 CANCELLED 终态。",
        duplicate_callback: "重复 callback 已去重，任务状态未变化。",
        out_of_order_callback: "旧序列 callback 被忽略，状态没有回退。",
        cancel_race: "成功终态先到达，取消响应没有覆盖成功。",
      };
      return toast(
        { ...state, jobs },
        {
          tone:
            action.scenario === "duplicate_callback" ||
            action.scenario === "out_of_order_callback" ||
            action.scenario === "cancel_race"
              ? "info"
              : "warning",
          title: "异常场景已注入",
          description: descriptions[action.scenario],
        },
      );
    }
    case "CANCEL_JOB": {
      const jobs = state.jobs.map((job) =>
        job.id === action.jobId && !["SUCCEEDED", "FAILED", "CANCELLED"].includes(job.state)
          ? { ...job, state: "CANCEL_REQUESTED" as const, updatedAt: nowLabel() }
          : job,
      );
      return toast(
        { ...state, jobs },
        {
          tone: "warning",
          title: "取消请求已登记",
          description: "在 Provider 确认前显示 cancel-requested，费用可能仍需对账。",
        },
      );
    }
    case "CONFIRM_CANCEL_JOB": {
      const jobs = state.jobs.map((job) =>
        job.id === action.jobId && job.state === "CANCEL_REQUESTED"
          ? { ...job, state: "CANCELLED" as const, updatedAt: nowLabel() }
          : job,
      );
      return toast(
        { ...state, jobs },
        {
          tone: "info",
          title: "Provider 已确认取消",
          description: "任务已进入 CANCELLED 终态；实际费用仍以账单证据为准。",
        },
      );
    }
    case "RETRY_JOB": {
      const jobs = state.jobs.map((job) =>
        job.id === action.jobId && job.failure?.retryable && job.retryCount < 3
          ? {
              ...job,
              state: "RETRYING" as const,
              retryCount: job.retryCount + 1,
              updatedAt: nowLabel(),
            }
          : job,
      );
      return toast(
        { ...state, jobs },
        {
          tone: "info",
          title: "已安排同任务重试",
          description: "沿用原 Job ID 与上游映射，不创建新的创作 attempt。",
        },
      );
    }
    case "SELECT_PROMPT":
      return { ...state, selectedPromptVersion: action.version };
    case "SET_JOBS_VIEW_STATE":
      return { ...state, jobsViewState: action.viewState };
    case "SELECT_ASSET":
      return { ...state, selectedAssetId: action.assetId };
    case "LOCK_ASSET_REVISION": {
      const asset = state.assets.find((item) => item.id === action.assetId);
      const revision = asset?.revisions.find((item) => item.version === action.version);
      if (!asset || !revision) return state;
      const affectedShots = state.shots.filter((shot) => shot.assetIds.includes(action.assetId));
      const assets = state.assets.map((item) =>
        item.id === action.assetId
          ? {
              ...item,
              version: revision.version,
              revisionId: revision.revisionId,
              state: revision.state,
            }
          : item,
      );
      const shots = state.shots.map((shot) =>
        shot.assetIds.includes(action.assetId) && shot.state !== "SUCCEEDED"
          ? { ...shot, state: "STALE" as const }
          : shot,
      );
      return toast(
        {
          ...state,
          assets,
          shots,
          activity: [
            {
              id: `act-asset-${action.assetId}-${revision.version}-${Date.now()}`,
              at: nowLabel(),
              actor: "本地导演",
              action: revision.version < asset.version ? "回滚资产引用" : "切换资产引用",
              detail: `${asset.name} → ${revision.revisionId} · ${affectedShots.length} 个镜头待重验 · 历史 revision 保留`,
            },
            ...state.activity,
          ],
        },
        {
          tone: revision.version < asset.version ? "warning" : "success",
          title: `${asset.name} 当前引用已设为 v${revision.version}`,
          description: `${affectedShots.length} 个下游镜头已标记待重验；版本链与已批准历史均未覆盖。`,
        },
      );
    }
    default:
      return state;
  }
}

interface StudioActions {
  createProject(input: CreateProjectInput): Promise<void>;
  navigate(view: ViewId): void;
  toggleInspector(): void;
  decideGate(gateId: GateId, decision: Decision, explanation: string): Promise<void>;
  regenerateGate(gateId: GateId): Promise<void>;
  simulateConcurrentUpdate(gateId: GateId): Promise<void>;
  synchronizeGate(gateId: GateId): Promise<void>;
  completeMockRun(): void;
  injectScenario(scenario: FailureScenario): void;
  cancelJob(jobId: string): void;
  confirmCancelJob(jobId: string): void;
  retryJob(jobId: string): void;
  setJobsViewState(viewState: JobsViewState): void;
  selectPrompt(version: number): void;
  selectAsset(assetId: string): void;
  lockAssetRevision(assetId: string, version: number): void;
  dismissToast(id: number): void;
}

const StudioContext = createContext<{ state: StudioState; actions: StudioActions } | undefined>(undefined);

export function StudioProvider({
  children,
  api,
  initialState,
}: PropsWithChildren<{ api?: ControlPlaneApi; initialState?: StudioState }>) {
  const [state, dispatch] = useReducer(reducer, initialState ?? createInitialState());
  const apiRef = useRef(api ?? createControlPlaneApi());

  const handleProblem = useCallback((error: unknown) => {
    const value =
      error instanceof ApiProblem
        ? error
        : new ApiProblem({
            status: 500,
            errorCode: "INTERNAL_ERROR",
            title: "Unexpected client error",
            detail: error instanceof Error ? error.message : "发生未知错误。",
            retryable: false,
            traceId: "client-error",
            suggestedAction: "刷新页面后重试。",
          });
    dispatch({ type: "BUSY", busy: false });
    dispatch({ type: "PROBLEM", problem: value });
    dispatch({
      type: "TOAST",
      toast: {
        tone: "error",
        title: `${value.status} · ${value.errorCode}`,
        description: `${value.message} ${value.suggestedAction}`,
      },
    });
  }, []);

  const decideGate = useCallback(
    async (gateId: GateId, decision: Decision, explanation: string) => {
      dispatch({ type: "BUSY", busy: true });
      try {
        const gate = state.gates[gateId];
        const result = await apiRef.current.createApproval({
          gateId,
          decision,
          explanation,
          expectedRevision: gate.etag,
          idempotencyKey: crypto.randomUUID(),
          bindings: gate.bindings,
        });
        dispatch({ type: "GATE_DECIDED", result, explanation });
      } catch (error) {
        handleProblem(error);
      }
    },
    [handleProblem, state.gates],
  );

  const createProject = useCallback(
    async (input: CreateProjectInput) => {
      dispatch({ type: "BUSY", busy: true });
      try {
        const result = await apiRef.current.createSeries(input, crypto.randomUUID());
        dispatch({ type: "PROJECT_CREATED", input, result });
      } catch (error) {
        handleProblem(error);
      }
    },
    [handleProblem],
  );

  const regenerateGate = useCallback(
    async (gateId: GateId) => {
      dispatch({ type: "BUSY", busy: true });
      try {
        const result = await apiRef.current.regenerateGate(gateId, state.gates[gateId].etag);
        dispatch({ type: "GATE_REGENERATED", result });
      } catch (error) {
        handleProblem(error);
      }
    },
    [handleProblem, state.gates],
  );

  const simulateConcurrentUpdate = useCallback(
    async (gateId: GateId) => {
      try {
        await apiRef.current.simulateConcurrentUpdate(gateId);
        dispatch({
          type: "TOAST",
          toast: {
            tone: "warning",
            title: "已模拟协作者更新",
            description: "本地仍持有旧 ETag；下一次审核将返回 409，且不会覆盖新版本。",
          },
        });
      } catch (error) {
        handleProblem(error);
      }
    },
    [handleProblem],
  );

  const synchronizeGate = useCallback(
    async (gateId: GateId) => {
      dispatch({ type: "BUSY", busy: true });
      try {
        const gate = await apiRef.current.synchronizeGate(gateId);
        dispatch({ type: "GATE_SYNCHRONIZED", gateId, gate });
      } catch (error) {
        handleProblem(error);
      }
    },
    [handleProblem],
  );

  useEffect(() => {
    let active = true;
    void apiRef.current
      .getProviderStatus()
      .then((capabilities) => {
        if (active) dispatch({ type: "CAPABILITIES_LOADED", capabilities });
      })
      .catch((error: unknown) => {
        if (active) handleProblem(error);
      });
    return () => {
      active = false;
    };
  }, [handleProblem]);

  const actions = useMemo<StudioActions>(
    () => ({
      createProject,
      navigate: (view) => dispatch({ type: "NAVIGATE", view }),
      toggleInspector: () => dispatch({ type: "TOGGLE_INSPECTOR" }),
      decideGate,
      regenerateGate,
      simulateConcurrentUpdate,
      synchronizeGate,
      completeMockRun: () => dispatch({ type: "COMPLETE_MOCK_RUN" }),
      injectScenario: (scenario) => dispatch({ type: "INJECT_SCENARIO", scenario }),
      cancelJob: (jobId) => dispatch({ type: "CANCEL_JOB", jobId }),
      confirmCancelJob: (jobId) => dispatch({ type: "CONFIRM_CANCEL_JOB", jobId }),
      retryJob: (jobId) => dispatch({ type: "RETRY_JOB", jobId }),
      setJobsViewState: (viewState) => dispatch({ type: "SET_JOBS_VIEW_STATE", viewState }),
      selectPrompt: (version) => dispatch({ type: "SELECT_PROMPT", version }),
      selectAsset: (assetId) => dispatch({ type: "SELECT_ASSET", assetId }),
      lockAssetRevision: (assetId, version) => dispatch({ type: "LOCK_ASSET_REVISION", assetId, version }),
      dismissToast: (id) => dispatch({ type: "DISMISS_TOAST", id }),
    }),
    [createProject, decideGate, regenerateGate, simulateConcurrentUpdate, synchronizeGate],
  );

  return <StudioContext.Provider value={{ state, actions }}>{children}</StudioContext.Provider>;
}

export function useStudio() {
  const value = useContext(StudioContext);
  if (!value) throw new Error("useStudio must be used inside StudioProvider");
  return value;
}

export const jobErrorLabel = (code?: JobErrorCode) => {
  const labels: Partial<Record<JobErrorCode, string>> = {
    unauthenticated: "凭据未配置",
    forbidden: "模型或区域无权限",
    rate_limited: "请求过快",
    quota_exceeded: "配额不足",
    budget_exceeded: "超出预算",
    invalid_request: "请求参数无效",
    content_blocked: "内容安全阻断",
    provider_unavailable: "Provider 暂时不可用",
    timeout: "状态未知，等待对账",
  };
  return code ? labels[code] ?? code : "";
};
