import {
  ApiProblem,
  type ApprovalInput,
  type ApprovalResult,
  type CreateJobAttemptInput,
  type CreateJobAttemptResult,
  type CreateProjectInput,
  type CreateProjectResult,
  type Gate,
  type GateId,
  type ProviderCapability,
  type ProviderJob,
  type RegenerationResult,
} from "../domain";
import {
  capabilities as fixtureCapabilities,
  gates as fixtureGates,
  jobs as fixtureJobs,
} from "../mock-data";

export interface ControlPlaneApi {
  getProviderStatus(): Promise<ProviderCapability[]>;
  createSeries(input: CreateProjectInput, idempotencyKey: string): Promise<CreateProjectResult>;
  createApproval(input: ApprovalInput): Promise<ApprovalResult>;
  createJobAttempt(input: CreateJobAttemptInput): Promise<CreateJobAttemptResult>;
  completeMockRun(): Promise<void>;
  regenerateGate(gateId: GateId, expectedRevision: number): Promise<RegenerationResult>;
  simulateConcurrentUpdate(gateId: GateId): Promise<number>;
}

const problem = (
  status: number,
  errorCode: string,
  detail: string,
  suggestedAction: string,
  retryable = false,
  affectedObjects: ApiProblem["affectedObjects"] = [],
) =>
  new ApiProblem({
    status,
    errorCode,
    title: "Control plane rejected the command",
    detail,
    retryable,
    traceId: `trc_mock_${Math.random().toString(16).slice(2, 10)}`,
    suggestedAction,
    affectedObjects,
  });

const payloadFingerprint = (input: ApprovalInput) =>
  JSON.stringify({
    gate: input.gateId,
    decision: input.decision,
    explanation: input.explanation,
    expectedRevision: input.expectedRevision,
    bindings: input.bindings.map((item) => [item.objectType, item.revisionId, item.contentHash]),
  });

const jobAttemptFingerprint = (input: CreateJobAttemptInput) =>
  JSON.stringify({
    sourceJobId: input.sourceJob.id,
    shotId: input.sourceJob.shotId,
    nextAttempt: input.nextAttempt,
    generationAttemptId: input.generationAttemptId,
    capability: input.sourceJob.capability,
    model: input.sourceJob.model,
  });

const requiredG3BindingTypes = [
  "EPISODE_REVISION",
  "QC_REPORT",
  "MANIFEST",
  "ARTIFACT",
] as const;

const bindingFingerprint = (bindings: ApprovalInput["bindings"]) =>
  bindings
    .map((binding) => [binding.objectType, binding.revisionId, binding.contentHash].join(":"))
    .sort();

const liveMutationBlocked = (operation: string) =>
  problem(
    501,
    "LIVE_PROJECTION_BINDINGS_REQUIRED",
    `${operation} 仍绑定 Mock projection，真实写操作已在网络请求前阻断。`,
    "先从控制面加载并校验真实 series/episode、rights/profile、immutable revision、route、budget 与 policy 绑定，再开放对应 mutation。",
  );

export class MockControlPlaneApi implements ControlPlaneApi {
  private gates: Record<GateId, Gate>;
  private idempotency = new Map<string, { fingerprint: string; result: ApprovalResult }>();
  private jobAttempts = new Map<string, { fingerprint: string; result: CreateJobAttemptResult }>();
  private jobAttemptIntents = new Map<
    string,
    { idempotencyKey: string; generationAttemptId: string; result: CreateJobAttemptResult }
  >();
  private currentJobByShot = new Map<string, string>();
  private attemptByJobId = new Map<string, number>();
  private jobStateById = new Map<string, ProviderJob["state"]>();
  private generationAttemptOwners = new Map<string, string>();
  private staleGates = new Set<GateId>();

  constructor(
    seed: Record<GateId, Gate> = fixtureGates,
    jobSeed: ProviderJob[] = fixtureJobs,
  ) {
    this.gates = structuredClone(seed);
    jobSeed.forEach((job) => {
      if (job.isCurrentAttempt) this.currentJobByShot.set(job.shotId, job.id);
      this.attemptByJobId.set(job.id, job.attempt);
      this.jobStateById.set(job.id, job.state);
    });
  }

  async getProviderStatus() {
    return structuredClone(fixtureCapabilities);
  }

  async createSeries(input: CreateProjectInput, idempotencyKey: string): Promise<CreateProjectResult> {
    await Promise.resolve();
    if (!input.title.trim() || !input.sourceText.trim()) {
      throw problem(400, "VALIDATION_ERROR", "项目名称和原作输入不能为空。", "补全必填字段后重新创建。");
    }
    return {
      operationId: `operation-series-${idempotencyKey.slice(0, 8)}`,
      seriesId: `series-${input.title.trim().toLowerCase().replaceAll(/\s+/g, "-").slice(0, 24)}`,
      state: "ACCEPTED",
      traceId: `trc_series_${idempotencyKey.slice(0, 8)}`,
    };
  }

  async createApproval(input: ApprovalInput): Promise<ApprovalResult> {
    await Promise.resolve();
    const existing = this.idempotency.get(input.idempotencyKey);
    const fingerprint = payloadFingerprint(input);
    if (existing) {
      if (existing.fingerprint !== fingerprint) {
        throw problem(
          409,
          "conflict",
          "同一 Idempotency-Key 被用于不同的审核内容。",
          "生成新的幂等键后重试，不要修改已提交命令。",
        );
      }
      return structuredClone(existing.result);
    }

    const gate = this.gates[input.gateId];
    if (input.expectedRevision !== gate.etag) {
      throw problem(
        409,
        "REVISION_CONFLICT",
        `${input.gateId} 已被其他协作者更新（当前 ETag ${gate.etag}）。`,
        "同步最新 revision，检查差异后重新提交审核。",
        false,
        [{ objectType: "GATE", objectId: input.gateId, currentRevision: gate.etag }],
      );
    }

    if (input.gateId === "G2" && this.gates.G1.state !== "APPROVED") {
      throw problem(422, "GATE_REQUIRED", "G1 尚未批准，不能审核剧本与分镜。", "先完成内容与资产审核。");
    }
    if (
      input.gateId === "G3" &&
      (this.gates.G1.state !== "APPROVED" || this.gates.G2.state !== "APPROVED")
    ) {
      throw problem(422, "GATE_REQUIRED", "上游审核尚未完成，不能锁定成片。", "先完成 G1 和 G2。");
    }
    if (input.gateId === "G3" && input.decision === "APPROVED") {
      this.assertG3ApprovalTruth(input);
    }
    if (gate.state === "BLOCKED") {
      throw problem(422, "GATE_REQUIRED", `${input.gateId} 仍被上游条件阻断。`, "检查审核链与任务终态。");
    }
    if (this.staleGates.has(input.gateId)) {
      throw problem(
        422,
        "STALE_DEPENDENCY",
        `${input.gateId} 当前 revision 仍绑定旧的上游 snapshot。`,
        `先创建新的 ${input.gateId} revision，吸收变更后重新审核。`,
      );
    }
    if (gate.state === "APPROVED" && input.decision === "APPROVED") {
      throw problem(
        409,
        "RUN_TERMINAL",
        `${input.gateId} 当前 revision 已批准，重复批准不会创建新决策。`,
        "如需修改，请先创建新的 revision。",
      );
    }

    gate.state = input.decision;
    gate.etag += 1;
    gate.explanation = input.explanation;
    gate.bindings = structuredClone(input.bindings);
    if (input.decision === "APPROVED" && input.gateId === "G1") {
      this.gates.G2.state = "PENDING";
    }
    if (input.decision === "APPROVED" && input.gateId === "G2") {
      this.gates.G3.state = this.isG3TruthReady() ? "PENDING" : "BLOCKED";
    }
    if (input.decision === "RETURNED" && input.gateId === "G1") {
      this.gates.G2.state = "BLOCKED";
      this.gates.G3.state = "BLOCKED";
    }
    if (input.decision === "RETURNED" && input.gateId === "G2") {
      this.gates.G3.state = "BLOCKED";
    }
    const result: ApprovalResult = {
      decisionId: `decision-${input.gateId.toLowerCase()}-${gate.etag}`,
      gate: input.gateId,
      decision: input.decision,
      decidedAt: new Date().toISOString(),
      traceId: `trc_gate_${input.gateId.toLowerCase()}_${gate.etag}`,
      expectedRevision: gate.etag,
    };
    this.idempotency.set(input.idempotencyKey, { fingerprint, result });
    return structuredClone(result);
  }

  async createJobAttempt(input: CreateJobAttemptInput): Promise<CreateJobAttemptResult> {
    await Promise.resolve();
    const fingerprint = jobAttemptFingerprint(input);
    const existing = this.jobAttempts.get(input.idempotencyKey);
    if (existing) {
      if (existing.fingerprint !== fingerprint) {
        throw problem(
          409,
          "conflict",
          "同一 Idempotency-Key 被用于不同的创作 attempt。",
          "生成新的幂等键后重新提交，不要修改已持久化的 attempt。",
        );
      }
      return structuredClone(existing.result);
    }
    const intentKey = `${input.sourceJob.id}:${input.nextAttempt}`;
    const existingIntent = this.jobAttemptIntents.get(intentKey);
    if (existingIntent) {
      throw problem(
        409,
        "ATTEMPT_ALREADY_EXISTS",
        `${intentKey} 已绑定 ${existingIntent.result.providerJobId}，不能用新命令重复提交。`,
        "复用原 Idempotency-Key 获取已持久化的 Job；不要创建第二个外部任务。",
      );
    }
    const currentJobId = this.currentJobByShot.get(input.sourceJob.shotId);
    if (currentJobId !== input.sourceJob.id) {
      throw problem(
        409,
        "ATTEMPT_SOURCE_SUPERSEDED",
        `${input.sourceJob.id} 已不是 ${input.sourceJob.shot} 的当前 Job。`,
        `刷新任务投影并使用当前 Job ${currentJobId ?? "unknown"}；不得从历史 source 重放。`,
      );
    }
    if (input.sourceJob.state !== "FAILED" && input.sourceJob.state !== "CANCELLED") {
      throw problem(
        422,
        "RUN_TERMINAL_REQUIRED",
        "只有当前 FAILED 或 CANCELLED Job 可以创建替代 attempt。",
        "选择当前终态任务，或对可重试故障沿用原 Job ID。",
      );
    }
    const currentAttempt = this.attemptByJobId.get(input.sourceJob.id);
    if (currentAttempt === undefined || input.nextAttempt !== currentAttempt + 1) {
      throw problem(
        409,
        "REVISION_CONFLICT",
        "新 attempt 序号必须严格递增。",
        "刷新任务投影后从最新 attempt 创建替代任务。",
      );
    }
    const generationAttemptOwner = this.generationAttemptOwners.get(input.generationAttemptId);
    if (generationAttemptOwner && generationAttemptOwner !== intentKey) {
      throw problem(
        409,
        "GENERATION_ATTEMPT_CONFLICT",
        `${input.generationAttemptId} 已绑定其他创作意图。`,
        "为不同创作意图使用不同 generationAttemptId。",
      );
    }

    const suffix = input.sourceJob.shotId.replace(/^shot-/, "");
    const result: CreateJobAttemptResult = {
      providerJobId: `job-v-${suffix}-a${input.nextAttempt}`,
      generationAttemptId: input.generationAttemptId,
      state: "QUEUED",
      traceId: `trc_attempt_${suffix}_a${input.nextAttempt}`,
      createdAt: new Date().toISOString(),
    };
    this.jobAttempts.set(input.idempotencyKey, { fingerprint, result });
    this.jobAttemptIntents.set(intentKey, {
      idempotencyKey: input.idempotencyKey,
      generationAttemptId: input.generationAttemptId,
      result,
    });
    this.currentJobByShot.set(input.sourceJob.shotId, result.providerJobId);
    this.attemptByJobId.set(result.providerJobId, input.nextAttempt);
    this.jobStateById.set(input.sourceJob.id, input.sourceJob.state);
    this.jobStateById.set(result.providerJobId, result.state);
    this.generationAttemptOwners.set(input.generationAttemptId, intentKey);
    return structuredClone(result);
  }

  async completeMockRun(): Promise<void> {
    await Promise.resolve();
    for (const jobId of this.currentJobByShot.values()) {
      const state = this.jobStateById.get(jobId);
      if (state !== "FAILED" && state !== "CANCELLED") {
        this.jobStateById.set(jobId, "SUCCEEDED");
      }
    }
    this.gates.G3.state =
      this.gates.G2.state === "APPROVED" && this.isG3TruthReady() ? "PENDING" : "BLOCKED";
  }

  async regenerateGate(gateId: GateId, expectedRevision: number): Promise<RegenerationResult> {
    await Promise.resolve();
    const gate = this.gates[gateId];
    if (expectedRevision !== gate.etag) {
      throw problem(
        409,
        "REVISION_CONFLICT",
        `${gateId} 的版本已变化，不能覆盖当前 revision。`,
        "同步后基于最新 revision 重新生成。",
        false,
        [{ objectType: "GATE", objectId: gateId, currentRevision: gate.etag }],
      );
    }
    gate.revision += 1;
    gate.etag += 1;
    gate.revisionId = `gate-${gateId.toLowerCase()}-r${gate.revision}`;
    gate.state = "PENDING";
    if (gateId === "G1") {
      this.staleGates.add("G2");
      this.staleGates.add("G3");
      this.gates.G2.state = "BLOCKED";
      this.gates.G3.state = "BLOCKED";
    } else if (gateId === "G2") {
      this.staleGates.delete("G2");
      this.staleGates.add("G3");
      this.gates.G3.state = "BLOCKED";
    } else {
      this.staleGates.delete("G3");
    }
    return {
      gate: gateId,
      revision: gate.revision,
      revisionId: gate.revisionId,
      etag: gate.etag,
      createdAt: new Date().toISOString(),
    };
  }

  async simulateConcurrentUpdate(gateId: GateId): Promise<number> {
    await Promise.resolve();
    this.gates[gateId].etag += 1;
    return this.gates[gateId].etag;
  }

  private currentJobStates() {
    return Array.from(this.currentJobByShot.values(), (jobId) => ({
      jobId,
      state: this.jobStateById.get(jobId),
    }));
  }

  private hasCompleteG3Bindings(bindings: ApprovalInput["bindings"]) {
    return requiredG3BindingTypes.every(
      (objectType) => bindings.filter((binding) => binding.objectType === objectType).length === 1,
    );
  }

  private isG3TruthReady() {
    const currentJobs = this.currentJobStates();
    return (
      currentJobs.length > 0 &&
      currentJobs.every((job) => job.state === "SUCCEEDED") &&
      this.hasCompleteG3Bindings(this.gates.G3.bindings)
    );
  }

  private assertG3ApprovalTruth(input: ApprovalInput) {
    const currentJobs = this.currentJobStates();
    const terminalBlockers = currentJobs.filter(
      (job) => job.state === "FAILED" || job.state === "CANCELLED",
    );
    if (terminalBlockers.length > 0) {
      throw problem(
        422,
        "G3_TERMINAL_ATTEMPT_BLOCKED",
        `当前批次仍有 ${terminalBlockers.length} 个 FAILED/CANCELLED attempt。`,
        "为每个失败或取消的当前任务创建并完成新 attempt；历史终态保持只读。",
      );
    }
    if (currentJobs.length === 0 || currentJobs.some((job) => job.state !== "SUCCEEDED")) {
      throw problem(
        422,
        "G3_RUNS_INCOMPLETE",
        "当前批次的全部 current attempts 尚未成功，不能批准 G3。",
        "等待所有当前 GenerationRun/ProviderJob 达到 SUCCEEDED 后重新审核。",
      );
    }
    if (
      !this.hasCompleteG3Bindings(this.gates.G3.bindings) ||
      !this.hasCompleteG3Bindings(input.bindings)
    ) {
      throw problem(
        422,
        "G3_BINDING_REQUIRED",
        "G3 必须精确绑定 episode revision、QC report、Manifest 与 artifact。",
        "重新加载当前成片投影，并提交四类不可变 binding。",
      );
    }
    if (
      bindingFingerprint(input.bindings).join("|") !==
      bindingFingerprint(this.gates.G3.bindings).join("|")
    ) {
      throw problem(
        409,
        "G3_BINDING_CONFLICT",
        "G3 请求绑定与控制面当前 episode/QC/Manifest/artifact 真相不一致。",
        "刷新成片谱系，核对精确 revision 与 content hash 后重新提交。",
      );
    }
  }
}

interface ProviderStatusResponse {
  capabilities: ProviderCapability[];
}

export class HttpControlPlaneApi implements ControlPlaneApi {
  constructor(private readonly baseUrl = "/api/v1") {}

  async getProviderStatus(): Promise<ProviderCapability[]> {
    const response = await fetch(`${this.baseUrl}/providers/status`, {
      method: "GET",
      credentials: "omit",
      headers: { Accept: "application/json" },
    });
    if (!response.ok) {
      throw await this.readProblem(response);
    }
    const body = (await response.json()) as ProviderStatusResponse;
    return body.capabilities;
  }

  async createSeries(input: CreateProjectInput, idempotencyKey: string): Promise<CreateProjectResult> {
    void input;
    void idempotencyKey;
    throw liveMutationBlocked("创建 Series");
  }

  async createApproval(input: ApprovalInput): Promise<ApprovalResult> {
    void input;
    throw liveMutationBlocked("提交 Gate 审核");
  }

  async createJobAttempt(input: CreateJobAttemptInput): Promise<CreateJobAttemptResult> {
    void input;
    throw liveMutationBlocked("创建 GenerationRun attempt");
  }

  async completeMockRun(): Promise<void> {
    throw liveMutationBlocked("完成 Mock 排练");
  }

  async regenerateGate(gateId: GateId, expectedRevision: number): Promise<RegenerationResult> {
    void gateId;
    void expectedRevision;
    throw liveMutationBlocked("重生成 Gate revision");
  }

  async simulateConcurrentUpdate(gateId: GateId): Promise<number> {
    void gateId;
    throw liveMutationBlocked("注入并发更新");
  }

  private async readProblem(response: Response) {
    const fallback = {
      status: response.status,
      errorCode: "INTERNAL_ERROR",
      title: response.statusText,
      detail: "控制面返回了无法解析的错误。",
      retryable: response.status >= 500,
      traceId: "missing-trace",
      suggestedAction: "稍后重试并向运维提供 trace ID。",
    };
    try {
      return new ApiProblem({ ...fallback, ...(await response.json()) });
    } catch {
      return new ApiProblem(fallback);
    }
  }
}

export const createControlPlaneApi = (): ControlPlaneApi =>
  import.meta.env.VITE_STUDIO_MODE === "live"
    ? new HttpControlPlaneApi(import.meta.env.VITE_API_BASE || "/api/v1")
    : new MockControlPlaneApi();
