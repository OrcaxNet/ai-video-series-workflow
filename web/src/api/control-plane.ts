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

const sha256 = async (seed: string) => {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(seed));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
};

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
  private generationAttemptOwners = new Map<string, string>();
  private staleGates = new Set<GateId>();

  constructor(seed: Record<GateId, Gate> = fixtureGates) {
    this.gates = structuredClone(seed);
    fixtureJobs.forEach((job) => {
      if (job.isCurrentAttempt) this.currentJobByShot.set(job.shotId, job.id);
      this.attemptByJobId.set(job.id, job.attempt);
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
      this.gates.G3.state = "PENDING";
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
    this.generationAttemptOwners.set(input.generationAttemptId, intentKey);
    return structuredClone(result);
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
    const response = await fetch(`${this.baseUrl}/series`, {
      method: "POST",
      credentials: "omit",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        "Idempotency-Key": idempotencyKey,
      },
      body: JSON.stringify({
        schemaVersion: "v1",
        title: input.title,
        generationProfileRevisionId: "00000000-0000-4000-8000-000000000101",
        rightsDeclaration: {
          basis: "creator_declared",
          evidenceArtifactHash: "0".repeat(64),
        },
        actor: { actorId: "local-creator", role: "CREATOR" },
      }),
    });
    if (!response.ok) {
      throw await this.readProblem(response);
    }
    const operation = (await response.json()) as {
      operationId: string;
      aggregateId: string;
      state: "ACCEPTED";
      traceId: string;
    };
    return {
      operationId: operation.operationId,
      seriesId: operation.aggregateId,
      state: operation.state,
      traceId: operation.traceId,
    };
  }

  async createApproval(input: ApprovalInput): Promise<ApprovalResult> {
    const response = await fetch(`${this.baseUrl}/approvals`, {
      method: "POST",
      credentials: "omit",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        "Idempotency-Key": input.idempotencyKey,
      },
      body: JSON.stringify({
        schemaVersion: "v1",
        seriesId: "series-tide-001",
        gate: input.gateId,
        decision: input.decision,
        reasonCode: input.decision === "APPROVED" ? "creator_approved" : "creator_returned",
        explanation: input.explanation,
        bindings: input.bindings,
        actor: { actorId: "local-creator", role: "DIRECTOR" },
      }),
    });
    if (!response.ok) {
      throw await this.readProblem(response);
    }
    return (await response.json()) as ApprovalResult;
  }

  async createJobAttempt(input: CreateJobAttemptInput): Promise<CreateJobAttemptResult> {
    const [inputHash, capabilityHash] = await Promise.all([
      sha256(`${input.sourceJob.shotId}:${input.nextAttempt}:${input.generationAttemptId}`),
      sha256(`${input.sourceJob.capability}:${input.sourceJob.model}`),
    ]);
    const response = await fetch(`${this.baseUrl}/provider-jobs`, {
      method: "POST",
      credentials: "omit",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        "Idempotency-Key": input.idempotencyKey,
      },
      body: JSON.stringify({
        schemaVersion: "v1",
        generationAttemptId: input.generationAttemptId,
        generationPlanId: "00000000-0000-4000-8000-000000000301",
        capability: input.sourceJob.capability,
        inputHash,
        routeSnapshot: {
          capabilityAlias: input.sourceJob.capability,
          providerProfileId: "00000000-0000-4000-8000-000000000401",
          provider: input.sourceJob.provider,
          modelId: input.sourceJob.model,
          routeVersion: "route-v4",
          capabilityHash,
        },
        requestSnapshot: {
          shotId: input.sourceJob.shotId,
          creativeAttempt: input.nextAttempt,
          creativeIntentKey: `${input.sourceJob.id}:${input.nextAttempt}`,
          supersedesProviderJobId: input.sourceJob.id,
          evidence: input.sourceJob.evidence,
        },
        actor: { actorId: "local-creator", role: "CREATOR" },
      }),
    });
    if (!response.ok) {
      throw await this.readProblem(response);
    }
    const job = (await response.json()) as {
      providerJobId: string;
      generationAttemptId: string;
      state: string;
      traceId: string;
      createdAt: string;
      upstreamTaskId?: string;
    };
    if (job.state !== "QUEUED") {
      throw problem(
        502,
        "PROJECTION_STATE_UNSUPPORTED",
        `新 Provider Job 返回了未就绪状态 ${job.state}。`,
        "重新加载任务投影，确认控制面已完成持久化与提交。",
      );
    }
    return {
      providerJobId: job.providerJobId,
      generationAttemptId: job.generationAttemptId,
      state: "QUEUED",
      traceId: job.traceId,
      createdAt: job.createdAt,
      taskId: job.upstreamTaskId,
    };
  }

  async regenerateGate(): Promise<RegenerationResult> {
    throw problem(
      501,
      "CAPABILITY_UNAVAILABLE",
      "当前冻结 OpenAPI 尚未提供通用 Gate 重生成端点。",
      "由对应内容 revision API 创建新版本后重新绑定 Gate。",
    );
  }

  async simulateConcurrentUpdate(): Promise<number> {
    throw problem(501, "CAPABILITY_UNAVAILABLE", "真实控制面不提供测试注入。", "仅在 Mock 场景中使用此操作。");
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
