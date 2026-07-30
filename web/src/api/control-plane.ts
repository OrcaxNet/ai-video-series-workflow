import {
  ApiProblem,
  type ApprovalInput,
  type ApprovalResult,
  type CreateProjectInput,
  type CreateProjectResult,
  type Gate,
  type GateId,
  type ProviderCapability,
  type RegenerationResult,
} from "../domain";
import { capabilities as fixtureCapabilities, gates as fixtureGates } from "../mock-data";

export interface ControlPlaneApi {
  getProviderStatus(): Promise<ProviderCapability[]>;
  createSeries(input: CreateProjectInput, idempotencyKey: string): Promise<CreateProjectResult>;
  createApproval(input: ApprovalInput): Promise<ApprovalResult>;
  regenerateGate(gateId: GateId, expectedRevision: number): Promise<RegenerationResult>;
  simulateConcurrentUpdate(gateId: GateId): Promise<number>;
}

const problem = (
  status: number,
  errorCode: string,
  detail: string,
  suggestedAction: string,
  retryable = false,
) =>
  new ApiProblem({
    status,
    errorCode,
    title: "Control plane rejected the command",
    detail,
    retryable,
    traceId: `trc_mock_${Math.random().toString(16).slice(2, 10)}`,
    suggestedAction,
  });

const payloadFingerprint = (input: ApprovalInput) =>
  JSON.stringify({
    gate: input.gateId,
    decision: input.decision,
    explanation: input.explanation,
    expectedRevision: input.expectedRevision,
    bindings: input.bindings.map((item) => [item.objectType, item.revisionId, item.contentHash]),
  });

export class MockControlPlaneApi implements ControlPlaneApi {
  private gates: Record<GateId, Gate>;
  private idempotency = new Map<string, { fingerprint: string; result: ApprovalResult }>();

  constructor(seed: Record<GateId, Gate> = fixtureGates) {
    this.gates = structuredClone(seed);
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

  async regenerateGate(gateId: GateId, expectedRevision: number): Promise<RegenerationResult> {
    await Promise.resolve();
    const gate = this.gates[gateId];
    if (expectedRevision !== gate.etag) {
      throw problem(
        409,
        "REVISION_CONFLICT",
        `${gateId} 的版本已变化，不能覆盖当前 revision。`,
        "同步后基于最新 revision 重新生成。",
      );
    }
    gate.revision += 1;
    gate.etag += 1;
    gate.revisionId = `gate-${gateId.toLowerCase()}-r${gate.revision}`;
    gate.state = "PENDING";
    if (gateId === "G1") {
      this.gates.G2.state = "BLOCKED";
      this.gates.G3.state = "BLOCKED";
    } else if (gateId === "G2") {
      this.gates.G3.state = "BLOCKED";
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
