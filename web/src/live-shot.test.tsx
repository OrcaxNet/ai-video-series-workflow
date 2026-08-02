import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it } from "vitest";
import { MockControlPlaneApi } from "./api/control-plane";
import { App } from "./app";
import type {
  CreateLiveShotPlanInput,
  CreatorLiveShotManifest,
  CreatorLiveShotPlan,
  CreatorLiveShotProject,
  CreatorLiveShotRun,
  ProviderCapability,
} from "./domain";
import { StudioProvider } from "./studio-store";

const ids = {
  series: "11111111-1111-4111-8111-111111111111",
  plan: "22222222-2222-4222-8222-222222222222",
  run: "33333333-3333-4333-8333-333333333333",
  operation: "44444444-4444-4444-8444-444444444444",
  job: "55555555-5555-4555-8555-555555555555",
  manifest: "66666666-6666-4666-8666-666666666666",
};

const route = {
  capabilityAlias: "video.primary" as const,
  provider: "volcengine_ark",
  modelId: "doubao-seedance-1-0-pro-250528",
  routeVersion: "agent-plan-v1",
  capabilityHash: "c".repeat(64),
  verification: "authenticated_adapter_snapshot",
  billingMode: "subscription" as const,
};

const liveCapability = (enabled = true): ProviderCapability => ({
  alias: "video.primary",
  liveConfigured: enabled,
  liveCallsEnabled: enabled,
  dryRunAvailable: true,
  mockAvailable: true,
  defaultProvider: "volcengine_ark",
  liveEvidence: enabled ? "authenticated_adapter_snapshot" : "not_enabled_for_creator_v1",
  mockEvidence: "mock_only",
});

const planFixture = (overrides: Partial<CreatorLiveShotPlan> = {}): CreatorLiveShotPlan => ({
  schemaVersion: "v1",
  planId: ids.plan,
  seriesId: ids.series,
  state: "AWAITING_CONFIRMATION",
  confirmable: true,
  blockers: [],
  title: "雨夜重逢",
  sceneTextHash: "b".repeat(64),
  aspectRatio: "16:9",
  providerCallCount: 1,
  providerSubmitCount: 0,
  planHash: "a".repeat(64),
  spec: { candidates: 1, durationSeconds: 5, resolution: "720p", audio: false, aspectRatio: "16:9" },
  route,
  budget: {
    maxTasksThisConfirmation: 1,
    maxVideoTokensThisConfirmation: 1_000_000,
    projectTaskLimit: 3,
    projectTokenLimit: 3_000_000,
    projectTasksUsed: 0,
    projectTokensUsed: 0,
    cashAmountMaximum: null,
    currency: null,
    verified: false,
  },
  bindings: { shotSpecRevisionId: "shot-spec-r1" },
  executionPolicy: { mode: "single-submit" },
  traceId: "trace-plan-safe",
  expiresAt: "2030-08-03T03:00:00Z",
  createdAt: "2026-08-03T02:00:00Z",
  ...overrides,
});

const runFixture = (overrides: Partial<CreatorLiveShotRun> = {}): CreatorLiveShotRun => ({
  schemaVersion: "v1",
  runId: ids.run,
  planId: ids.plan,
  seriesId: ids.series,
  operationId: ids.operation,
  providerJobId: ids.job,
  state: "QUEUED",
  progress: null,
  planHash: "a".repeat(64),
  route,
  providerTaskId: null,
  providerRequestId: null,
  submitCount: 1,
  replayed: false,
  failure: null,
  artifact: null,
  usage: {
    promptVideoTokens: null,
    completionVideoTokens: null,
    totalVideoTokens: null,
    generatedDurationMs: null,
  },
  cost: { amountMicros: null, currency: null, verified: false, billingMode: "subscription" },
  manifest: null,
  traceId: "trace-run-safe",
  createdAt: "2026-08-03T02:01:00Z",
  updatedAt: "2026-08-03T02:01:01Z",
  ...overrides,
});

const manifestFixture: CreatorLiveShotManifest = {
  schemaVersion: "creator-live-shot-manifest.v1",
  manifestId: ids.manifest,
  evidence: "live_provider_call",
  runId: ids.run,
  planId: ids.plan,
  planHash: "a".repeat(64),
  provider: route,
  providerRegion: "cn-beijing",
  providerJobId: ids.job,
  upstreamTaskId: "task-live-redacted-safe",
  requestId: "request-live-redacted-safe",
  inputHash: "b".repeat(64),
  outputHash: "d".repeat(64),
  media: {
    sha256: "d".repeat(64),
    mediaType: "video/mp4",
    sizeBytes: 2_400_000,
    width: 1280,
    height: 720,
    durationMs: 5000,
    downloadUrl: `/api/v1/creator/live-shot-runs/${ids.run}/artifact`,
  },
  usage: {
    promptVideoTokens: 1200,
    completionVideoTokens: 8800,
    totalVideoTokens: 10000,
    generatedDurationMs: 5000,
  },
  budget: {
    budgetApprovalId: "budget-safe",
    reservationId: "reservation-safe",
    reservedTasks: 1,
    reservedVideoTokens: 1_000_000,
    settledVideoTokens: 10000,
    settlement: "CONSERVATIVE_SUBSCRIPTION_RESERVATION",
  },
  cost: { amountMicros: null, currency: null, verified: false, billingMode: "subscription" },
  createdAt: "2026-08-03T02:03:00Z",
};

class LiveApi extends MockControlPlaneApi {
  readonly planInputs: Array<{ input: CreateLiveShotPlanInput; intent: string }> = [];
  readonly confirmIntents: string[] = [];
  plan = planFixture();
  run = runFixture();

  override async getProviderStatus() {
    return [liveCapability()];
  }

  override async createLiveShotPlan(input: CreateLiveShotPlanInput, intent: string) {
    this.planInputs.push({ input: structuredClone(input), intent });
    return structuredClone(this.plan);
  }

  override async confirmLiveShotPlan(_planId: string, _planHash: string, intent: string) {
    this.confirmIntents.push(intent);
    return structuredClone(this.run);
  }

  override async getLiveShotProject(): Promise<CreatorLiveShotProject> {
    return { schemaVersion: "v1", seriesId: ids.series, plan: this.plan, runs: this.run ? [this.run] : [] };
  }

  override async getLiveShotRun() {
    return { notModified: false, run: structuredClone(this.run), etag: '"run-etag"' };
  }

  override async getLiveShotManifest() {
    return structuredClone(manifestFixture);
  }
}

class DeferredConfirmApi extends LiveApi {
  private resolveRun?: (run: CreatorLiveShotRun) => void;

  override async confirmLiveShotPlan(_planId: string, _planHash: string, intent: string) {
    this.confirmIntents.push(intent);
    return new Promise<CreatorLiveShotRun>((resolve) => {
      this.resolveRun = resolve;
    });
  }

  finish() {
    this.resolveRun?.(runFixture());
  }
}

class LostResponseApi extends LiveApi {
  private first = true;

  override async confirmLiveShotPlan(_planId: string, _planHash: string, intent: string) {
    this.confirmIntents.push(intent);
    if (this.first) {
      this.first = false;
      throw new Error("accepted response was lost");
    }
    return runFixture({ replayed: true });
  }

  override async getLiveShotProject(): Promise<CreatorLiveShotProject> {
    return { schemaVersion: "v1", seriesId: ids.series, plan: this.plan, runs: [] };
  }
}

const renderLive = async (api: LiveApi) => {
  render(
    <StudioProvider api={api}>
      <App />
    </StudioProvider>,
  );
  await waitFor(() => expect(screen.getByLabelText("可真实调用")).toBeInTheDocument());
  fireEvent.click(screen.getByRole("button", { name: /真实单镜预览/ }));
  await waitFor(() => expect(screen.getByText("Live 已就绪")).toBeInTheDocument());
};

const createPlan = async () => {
  fireEvent.click(screen.getByRole("checkbox"));
  fireEvent.click(screen.getByRole("button", { name: "生成计划（不扣费）" }));
  await waitFor(() => expect(screen.getByText(/submit 0/)).toBeInTheDocument());
};

describe("live single-shot studio", () => {
  beforeEach(() => window.localStorage.clear());

  it("creates a zero-submit plan and synchronously guards the paid confirmation", async () => {
    const api = new DeferredConfirmApi();
    await renderLive(api);
    await createPlan();

    expect(api.planInputs).toHaveLength(1);
    expect(api.confirmIntents).toHaveLength(0);
    expect(screen.getByText("未提供 · null · 未核验")).toBeInTheDocument();
    const confirm = screen.getByRole("button", { name: "确认并生成一次" });
    fireEvent.click(confirm);
    fireEvent.click(confirm);

    expect(api.confirmIntents).toHaveLength(1);
    api.finish();
    await waitFor(() => expect(screen.getByText("已排队")).toBeInTheDocument());
    expect(JSON.parse(window.localStorage.getItem("studio.live-shot.resume.v1") ?? "{}"))
      .toMatchObject({ seriesId: ids.series, planId: ids.plan, runId: ids.run, confirmationPending: false });
  });

  it("reuses the confirmation intent after a lost response", async () => {
    const api = new LostResponseApi();
    await renderLive(api);
    await createPlan();
    fireEvent.click(screen.getByRole("button", { name: "确认并生成一次" }));

    await waitFor(() => expect(api.confirmIntents).toHaveLength(2));
    expect(api.confirmIntents[1]).toBe(api.confirmIntents[0]);
    await waitFor(() => expect(screen.getByText("已排队")).toBeInTheDocument());
  });

  it("blocks confirmation when the project budget projection is not confirmable", async () => {
    const api = new LiveApi();
    api.plan = planFixture({ confirmable: false, blockers: ["PROJECT_BUDGET_EXCEEDED"] });
    await renderLive(api);
    await createPlan();

    expect(screen.getByText("PROJECT_BUDGET_EXCEEDED")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "确认并生成一次" })).toBeDisabled();
  });

  it("shows UNKNOWN without inventing progress and then renders a stable failure", async () => {
    const api = new LiveApi();
    api.run = runFixture({ state: "UNKNOWN", providerTaskId: "task-unknown-safe" });
    await renderLive(api);
    await createPlan();
    fireEvent.click(screen.getByRole("button", { name: "确认并生成一次" }));
    await waitFor(() => expect(screen.getByText("状态待对账")).toBeInTheDocument());
    expect(screen.getByText("系统只查询同一 Provider task，不会 submit 新任务。")).toBeInTheDocument();

    api.run = runFixture({
      state: "FAILED",
      providerTaskId: "task-unknown-safe",
      failure: { errorCode: "CONTENT_BLOCKED", retryable: false, suggestedAction: "修改场景描述后新建项目。" },
    });
    fireEvent.click(screen.getByRole("button", { name: "刷新真实状态" }));
    await waitFor(() => expect(screen.getByText("CONTENT_BLOCKED")).toBeInTheDocument());
    expect(screen.getByText("retryable=false · trace trace-run-safe")).toBeInTheDocument();
  });

  it("plays and downloads only same-origin committed MP4 and Manifest artifacts", async () => {
    const api = new LiveApi();
    api.run = runFixture({
      state: "SUCCEEDED",
      progress: 100,
      providerTaskId: "task-live-redacted-safe",
      providerRequestId: "request-live-redacted-safe",
      artifact: manifestFixture.media,
      usage: manifestFixture.usage,
      manifest: { id: ids.manifest, hash: "e".repeat(64), url: "https://provider.invalid/signed", evidence: "live_provider_call" },
    });
    await renderLive(api);
    await createPlan();
    fireEvent.click(screen.getByRole("button", { name: "确认并生成一次" }));

    await waitFor(() => expect(screen.getByText("真实产物已归档")).toBeInTheDocument());
    const video = screen.getByLabelText("真实生成的单镜视频");
    expect(video).toHaveAttribute("src", `/api/v1/creator/live-shot-runs/${ids.run}/artifact`);
    expect(screen.getByRole("link", { name: "下载 MP4" })).toHaveAttribute(
      "href",
      `/api/v1/creator/live-shot-runs/${ids.run}/artifact`,
    );
    expect(screen.getByRole("link", { name: "下载 Manifest" })).toHaveAttribute(
      "href",
      `/api/v1/creator/live-shot-runs/${ids.run}/manifest`,
    );
    expect(document.body.textContent).not.toContain("provider.invalid");
  });

  it("keeps planning disabled when video.primary is not live-enabled", async () => {
    const api = new LiveApi();
    api.getProviderStatus = async () => [liveCapability(false)];
    render(
      <StudioProvider api={api}>
        <App />
      </StudioProvider>,
    );
    fireEvent.click(screen.getByRole("button", { name: /真实单镜预览/ }));
    await waitFor(() => expect(screen.getByText("Live 未启用")).toBeInTheDocument());
    expect(screen.getByRole("button", { name: "生成计划（不扣费）" })).toBeDisabled();
    expect(screen.getByText("确认按钮保持关闭")).toBeInTheDocument();
  });
});
