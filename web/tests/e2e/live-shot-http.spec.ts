import { expect, test, type Page, type Route } from "@playwright/test";

const ids = {
  series: "11111111-1111-4111-8111-111111111111",
  plan: "22222222-2222-4222-8222-222222222222",
  run: "33333333-3333-4333-8333-333333333333",
  operation: "44444444-4444-4444-8444-444444444444",
  job: "55555555-5555-4555-8555-555555555555",
  manifest: "66666666-6666-4666-8666-666666666666",
};

const hash = "a".repeat(64);
const outputHash = "d".repeat(64);
const routeSnapshot = {
  capabilityAlias: "video.primary",
  provider: "volcengine_ark",
  modelId: "doubao-seedance-1-0-pro-250528",
  routeVersion: "agent-plan-v1",
  capabilityHash: "c".repeat(64),
  verification: "authenticated_adapter_snapshot",
  billingMode: "subscription",
};

const providerStatus = (enabled = true) => ({
  schemaVersion: "v1",
  mode: enabled ? "live" : "disabled",
  capabilities: [
    {
      alias: "video.primary",
      liveConfigured: enabled,
      liveCallsEnabled: enabled,
      dryRunAvailable: true,
      mockAvailable: true,
      defaultProvider: "volcengine_ark",
      liveEvidence: enabled ? "authenticated_adapter_snapshot" : "not_enabled_for_creator_v1",
      mockEvidence: "mock_only",
    },
  ],
  secretPolicy: { browserReceivesCredential: false },
});

const planProjection = (confirmable = true) => ({
  schemaVersion: "v1",
  planId: ids.plan,
  seriesId: ids.series,
  state: "AWAITING_CONFIRMATION",
  confirmable,
  blockers: confirmable ? [] : ["PROJECT_BUDGET_EXCEEDED"],
  title: "雨夜重逢",
  sceneTextHash: "b".repeat(64),
  aspectRatio: "16:9",
  providerCallCount: 1,
  providerSubmitCount: 0,
  planHash: hash,
  spec: { candidates: 1, durationSeconds: 5, resolution: "720p", audio: false, aspectRatio: "16:9" },
  route: routeSnapshot,
  budget: {
    maxTasksThisConfirmation: 1,
    maxVideoTokensThisConfirmation: 1_000_000,
    projectTaskLimit: 3,
    projectTokenLimit: 3_000_000,
    projectTasksUsed: confirmable ? 0 : 3,
    projectTokensUsed: confirmable ? 0 : 3_000_000,
    cashAmountMaximum: null,
    currency: null,
    verified: false,
  },
  bindings: { shotSpecRevisionId: "77777777-7777-4777-8777-777777777777" },
  executionPolicy: { maxProviderSubmits: 1 },
  traceId: "trace-plan-http",
  expiresAt: "2030-08-03T03:00:00Z",
  createdAt: "2026-08-03T02:00:00Z",
});

const artifact = {
  sha256: outputHash,
  mediaType: "video/mp4",
  sizeBytes: 2_400_000,
  width: 1280,
  height: 720,
  durationMs: 5000,
  downloadUrl: `/api/v1/creator/live-shot-runs/${ids.run}/artifact`,
};

const runProjection = (state: string) => ({
  schemaVersion: "v1",
  runId: ids.run,
  planId: ids.plan,
  seriesId: ids.series,
  operationId: ids.operation,
  providerJobId: ids.job,
  state,
  progress: state === "RUNNING" ? 42 : state === "SUCCEEDED" ? 100 : null,
  planHash: hash,
  route: routeSnapshot,
  providerTaskId: "provider-task-redacted-http",
  providerRequestId: "provider-request-redacted-http",
  submitCount: 1,
  replayed: false,
  failure: state === "FAILED"
    ? { errorCode: "CONTENT_BLOCKED", retryable: false, suggestedAction: "修改场景描述后新建项目。" }
    : null,
  artifact: state === "SUCCEEDED" ? artifact : null,
  usage: state === "SUCCEEDED"
    ? { promptVideoTokens: 1200, completionVideoTokens: 8800, totalVideoTokens: 10000, generatedDurationMs: 5000 }
    : { promptVideoTokens: null, completionVideoTokens: null, totalVideoTokens: null, generatedDurationMs: null },
  cost: { amountMicros: null, currency: null, verified: false, billingMode: "subscription" },
  manifest: state === "SUCCEEDED"
    ? { id: ids.manifest, hash: "e".repeat(64), url: `/api/v1/creator/live-shot-runs/${ids.run}/manifest`, evidence: "live_provider_call" }
    : null,
  traceId: "trace-run-http",
  createdAt: "2026-08-03T02:01:00Z",
  updatedAt: "2026-08-03T02:02:00Z",
});

const manifestProjection = {
  schemaVersion: "creator-live-shot-manifest.v1",
  manifestId: ids.manifest,
  evidence: "live_provider_call",
  runId: ids.run,
  planId: ids.plan,
  planHash: hash,
  provider: routeSnapshot,
  providerRegion: "cn-beijing",
  providerJobId: ids.job,
  upstreamTaskId: "provider-task-redacted-http",
  requestId: "provider-request-redacted-http",
  inputHash: "b".repeat(64),
  outputHash,
  media: artifact,
  usage: { promptVideoTokens: 1200, completionVideoTokens: 8800, totalVideoTokens: 10000, generatedDurationMs: 5000 },
  budget: {
    budgetApprovalId: "budget-redacted-http",
    reservationId: "reservation-redacted-http",
    reservedTasks: 1,
    reservedVideoTokens: 1_000_000,
    settledVideoTokens: 10000,
    settlement: "CONSERVATIVE_SUBSCRIPTION_RESERVATION",
  },
  cost: { amountMicros: null, currency: null, verified: false, billingMode: "subscription" },
  createdAt: "2026-08-03T02:03:00Z",
};

const json = (route: Route, body: unknown, status = 200, headers: Record<string, string> = {}) =>
  route.fulfill({
    status,
    contentType: "application/json",
    headers,
    body: JSON.stringify(body),
  });

async function openLiveShot(page: Page) {
  await page.goto("/");
  await page.getByRole("button", { name: /真实单镜预览/ }).click();
  await expect(page.getByText("Live 已就绪")).toBeVisible();
}

test("real HTTP contract plans with zero submit, confirms once, recovers, and exposes same-origin artifacts", async ({ page, context }) => {
  let planCalls = 0;
  let confirmCalls = 0;
  let projectionState = "QUEUED";
  const confirmIntentIds: string[] = [];
  const browserRequests: Array<{ url: string; headers: Record<string, string> }> = [];
  const consoleMessages: string[] = [];
  page.on("request", (request) => browserRequests.push({ url: request.url(), headers: request.headers() }));
  page.on("console", (message) => consoleMessages.push(message.text()));

  await page.route("**/api/v1/providers/status", (requestRoute) => json(requestRoute, providerStatus()));
  await page.route("**/api/v1/creator/live-shot-plans", async (requestRoute) => {
    planCalls += 1;
    const request = requestRoute.request();
    const body = request.postDataJSON();
    expect(body).toMatchObject({ schemaVersion: "v1", aspectRatio: "16:9", rightsAccepted: true });
    expect(request.headers()["idempotency-key"]).toMatch(/^[0-9a-f-]{36}$/);
    expect(request.headers()).not.toHaveProperty("authorization");
    await json(requestRoute, planProjection(), 201, { ETag: `"${hash}"` });
  });
  await page.route(`**/api/v1/creator/live-shot-plans/${ids.plan}/confirm`, async (requestRoute) => {
    confirmCalls += 1;
    const request = requestRoute.request();
    confirmIntentIds.push(request.headers()["idempotency-key"] ?? "");
    expect(request.headers()["if-match"]).toBe(`"${hash}"`);
    expect(request.postDataJSON()).toEqual({ schemaVersion: "v1", planHash: hash, confirmed: true });
    projectionState = "QUEUED";
    await json(requestRoute, runProjection(projectionState), 202, { ETag: '"run-queued"' });
  });
  await page.route(`**/api/v1/creator/live-shots/${ids.series}`, (requestRoute) =>
    json(requestRoute, {
      schemaVersion: "v1",
      seriesId: ids.series,
      plan: planProjection(),
      runs: [runProjection(projectionState)],
    }),
  );
  await page.route(`**/api/v1/creator/live-shot-runs/${ids.run}`, (requestRoute) =>
    json(requestRoute, runProjection(projectionState), 200, { ETag: `"run-${projectionState.toLowerCase()}"` }),
  );
  await page.route(`**/api/v1/creator/live-shot-runs/${ids.run}/manifest`, (requestRoute) =>
    json(requestRoute, manifestProjection),
  );
  await page.route(`**/api/v1/creator/live-shot-runs/${ids.run}/artifact`, (requestRoute) =>
    requestRoute.fulfill({ status: 206, contentType: "video/mp4", headers: { "Accept-Ranges": "bytes", "Content-Range": "bytes 0-31/32" }, body: "00000018ftypmp4200000000mp42isom" }),
  );

  await openLiveShot(page);
  await page.getByRole("checkbox").check();
  await page.getByRole("button", { name: "生成计划（不扣费）" }).click();
  await expect(page.getByText(/submit 0/)).toBeVisible();
  expect(planCalls).toBe(1);
  expect(confirmCalls).toBe(0);

  const confirm = page.getByRole("button", { name: "确认并生成一次" });
  await confirm.dblclick();
  await expect(page.getByText("已排队")).toBeVisible();
  expect(confirmCalls).toBe(1);
  expect(confirmIntentIds).toHaveLength(1);

  projectionState = "UNKNOWN";
  await page.getByRole("button", { name: "刷新真实状态" }).click();
  await expect(page.getByText("状态待对账")).toBeVisible();
  await expect(page.getByText("系统只查询同一 Provider task，不会 submit 新任务。")).toBeVisible();

  projectionState = "RECONCILING";
  await page.getByRole("button", { name: "刷新真实状态" }).click();
  await expect(page.getByText("正在恢复同一任务")).toBeVisible();

  await page.reload();
  await page.getByRole("button", { name: /真实单镜预览/ }).click();
  await expect(page.getByText("正在恢复同一任务")).toBeVisible();
  expect(confirmCalls).toBe(1);

  projectionState = "SUCCEEDED";
  await page.getByRole("button", { name: "刷新真实状态" }).click();
  await expect(page.getByText("真实产物已归档")).toBeVisible();
  await expect(page.getByLabel("真实生成的单镜视频")).toHaveAttribute(
    "src",
    `/api/v1/creator/live-shot-runs/${ids.run}/artifact`,
  );
  await expect(page.getByRole("link", { name: "下载 Manifest" })).toHaveAttribute(
    "href",
    `/api/v1/creator/live-shot-runs/${ids.run}/manifest`,
  );

  const storage = await page.evaluate(() => ({ ...localStorage }));
  const serializedStorage = JSON.stringify(storage);
  expect(serializedStorage).not.toContain("深夜的旧火车站");
  expect(serializedStorage).not.toMatch(/authorization|signed|provider\.invalid|ark[_-]?api/i);
  expect(await page.locator("body").innerText()).not.toMatch(
    /provider\.invalid|x-amz-signature|authorization:\s*bearer|bearer\s+[a-z0-9._-]{16,}/i,
  );
  expect(browserRequests.every((request) => new URL(request.url).origin === "http://127.0.0.1:4174")).toBe(true);
  expect(browserRequests.every((request) => !request.headers.authorization && !request.headers.cookie)).toBe(true);
  expect(JSON.stringify(browserRequests)).not.toMatch(/x-amz-signature|signed_url|provider\.invalid/i);
  expect(JSON.stringify(await context.cookies())).not.toMatch(/authorization|ark[_-]?api|bearer|signed/i);
  expect(JSON.stringify(consoleMessages)).not.toMatch(/authorization:\s*bearer|x-amz-signature|ark[_-]?api/i);
});

test("budget-blocked HTTP plan never enables the paid confirmation", async ({ page }) => {
  let confirmCalls = 0;
  await page.route("**/api/v1/providers/status", (requestRoute) => json(requestRoute, providerStatus()));
  await page.route("**/api/v1/creator/live-shot-plans", (requestRoute) => json(requestRoute, planProjection(false), 201));
  await page.route("**/api/v1/creator/live-shot-plans/**/confirm", (requestRoute) => {
    confirmCalls += 1;
    return json(requestRoute, runProjection("QUEUED"), 202);
  });

  await openLiveShot(page);
  await page.getByRole("checkbox").check();
  await page.getByRole("button", { name: "生成计划（不扣费）" }).click();
  await expect(page.getByText("PROJECT_BUDGET_EXCEEDED")).toBeVisible();
  await expect(page.getByRole("button", { name: "确认并生成一次" })).toBeDisabled();
  expect(confirmCalls).toBe(0);
});

test("disabled live capability keeps the creator path fail-closed", async ({ page }) => {
  await page.route("**/api/v1/providers/status", (requestRoute) => json(requestRoute, providerStatus(false)));
  await page.goto("/");
  await page.getByRole("button", { name: /真实单镜预览/ }).click();
  await expect(page.getByText("Live 未启用")).toBeVisible();
  await expect(page.getByRole("button", { name: "生成计划（不扣费）" })).toBeDisabled();
  await expect(page.getByText("确认按钮保持关闭")).toBeVisible();
});
