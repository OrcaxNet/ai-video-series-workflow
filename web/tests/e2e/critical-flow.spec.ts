import { expect, test, type Page } from "@playwright/test";

async function approveG1AndG2(page: Page) {
  await page.getByRole("button", { name: "内容与资产 G1" }).click();
  await page.getByRole("button", { name: "开始审核" }).click();
  await page.getByRole("button", { name: "批准并锁定 r7" }).click();
  await page.getByRole("button", { name: "剧本与分镜 G2" }).click();
  await page.getByRole("button", { name: "开始审核" }).click();
  await page.getByRole("button", { name: "批准并锁定 r9" }).click();
}

test("creator completes the three non-bypassable gates in order", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByRole("heading", { name: /把每一次决定/ })).toBeVisible();
  await expect(page.getByText("Mock only", { exact: true }).first()).toBeVisible();

  await page.getByRole("button", { name: "切换或新建项目" }).click();
  await expect(page.getByRole("dialog", { name: "从原作建立可追溯剧集" })).toBeVisible();
  await page.getByLabel("项目名称").fill("潮汐失语者 PoC");
  await page.getByRole("button", { name: "创建并生成结构草稿" }).click();
  await expect(page.getByText("已创建《潮汐失语者 PoC》")).toBeVisible();

  await page.getByRole("button", { name: "内容与资产 G1" }).click();
  await page.getByRole("button", { name: "开始审核" }).click();
  await page.getByLabel("审核意见").fill("资产、许可与引用已核对");
  await page.getByRole("button", { name: "批准并锁定 r7" }).click();
  await expect(page.getByText("G1 已批准并锁定")).toBeVisible();

  await page.getByRole("button", { name: "剧本与分镜 G2" }).click();
  await page.getByRole("button", { name: "开始审核" }).click();
  await page.getByLabel("审核意见").fill("节奏、镜头时长与上下文已核对");
  await page.getByRole("button", { name: "批准并锁定 r9" }).click();
  await expect(page.getByText("G2 已批准并锁定")).toBeVisible();

  await page.getByRole("button", { name: "任务中心" }).click();
  await expect(page.getByRole("button", { name: "完成 Mock 排练" })).toBeEnabled();
  await page.getByRole("button", { name: "完成 Mock 排练" }).click();
  await expect(page.getByText("Mock 排练已完成")).toBeVisible();

  await page.getByRole("button", { name: "成片审核 G3" }).click();
  await page.getByRole("button", { name: "开始审核" }).click();
  await page.getByLabel("审核意见").fill("画面、声音、字幕、标识与 Manifest 已核对");
  await page.getByRole("button", { name: "批准并锁定 r2" }).click();
  await expect(page.getByText("G3 已批准并锁定")).toBeVisible();
  await expect(page.getByText("已批准并锁定", { exact: true })).toBeVisible();
});

test("provider failures and callback races have explicit, stable feedback", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "任务中心" }).click();

  await page.getByRole("button", { name: /429.*限流等待/ }).click();
  await expect(page.getByText(/429 遵循 Retry-After/)).toBeVisible();
  await expect(page.getByText("重试中", { exact: true }).first()).toBeVisible();

  await page.getByRole("button", { name: /超时.*进入 UNKNOWN/ }).click();
  await expect(page.getByText("超时进入 UNKNOWN，等待对账而不是盲目重提。")).toBeVisible();
  await expect(page.getByText("状态未知", { exact: true }).first()).toBeVisible();

  await page.getByRole("button", { name: /乱序.*禁止回退/ }).click();
  await expect(page.getByText(/旧序列 callback 被忽略/)).toBeVisible();
  await expect(page.getByText("状态未知", { exact: true }).first()).toBeVisible();
});

test("asset revisions can be compared, locked, and rolled back without deleting history", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "内容与资产 G1" }).click();
  await page.getByRole("button", { name: "查看影响与回滚" }).click();

  const dialog = page.getByRole("dialog", { name: /季岚 · 影响分析与回滚/ });
  await expect(dialog).toBeVisible();
  await dialog.getByRole("radio", { name: /v2 · APPROVED/ }).click();
  await expect(dialog.locator("ins").filter({ hasText: "asset-jilan-r2" })).toBeVisible();
  await expect(dialog.getByText(/下游影响 · 3 个镜头/)).toBeVisible();
  await expect(dialog.getByText("G1 审核历史、旧 Prompt 和 CAS 产物保持只读可追溯。")).toBeVisible();
  await dialog.getByRole("button", { name: "回滚业务引用至 v2" }).click();

  await expect(page.getByText("季岚 已切换；G1 r8 等待重审")).toBeVisible();
  await expect(page.getByText("asset-jilan-r2").first()).toBeVisible();
  await expect(page.getByText("v4 · DRAFT")).toBeVisible();
  await expect(page.getByText("revision 8")).toBeVisible();
  await expect(page.getByText("基础原画 · 14 项 · 季岚 v2")).toBeVisible();
});

test("quota, budget, failed, and cancelled provider outcomes are reachable", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "任务中心" }).click();

  await page.getByRole("button", { name: /配额.*阻断并换路由/ }).click();
  await expect(page.getByText(/配额不足/).first()).toBeVisible();
  await expect(page.getByText("需要处理", { exact: true }).first()).toBeVisible();

  await page.getByRole("button", { name: /预算.*等待负责人批准/ }).click();
  await expect(page.getByText(/超出预算/).first()).toBeVisible();

  await page.getByRole("button", { name: /失败.*FAILED 终态/ }).click();
  await expect(page.getByText("失败", { exact: true }).first()).toBeVisible();
  await expect(page.getByText("需新建 attempt", { exact: true })).toBeVisible();

  await page.reload();
  await page.getByRole("button", { name: "任务中心" }).click();
  await page.getByRole("button", { name: /取消.*CANCELLED 终态/ }).click();
  await expect(page.getByText("已取消", { exact: true }).first()).toBeVisible();
  await expect(page.getByText("已确认取消", { exact: true })).toBeVisible();
});

test("a 409 conflict exposes synchronization and then accepts a fresh decision", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "内容与资产 G1" }).click();
  await page.getByRole("button", { name: "开始审核" }).click();
  await page.getByLabel("审核意见").fill("确认冲突恢复路径");
  await page.getByRole("button", { name: "并发测试" }).click();
  await page.getByRole("button", { name: "批准并锁定 r7" }).click();

  await expect(page.getByText("本地 ETag 已过期")).toBeVisible();
  await page.getByRole("button", { name: "同步最新 revision 8" }).click();
  await expect(page.getByText("G1 已同步到 ETag 8")).toBeVisible();
  await page.getByRole("button", { name: "批准并锁定 r7" }).click();
  await expect(page.getByText("G1 已批准并锁定")).toBeVisible();
});

test("asset reference changes create a new G1 snapshot and invalidate the old G2 revision", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "内容与资产 G1" }).click();
  await page.getByRole("button", { name: "开始审核" }).click();
  await page.getByRole("button", { name: "批准并锁定 r7" }).click();
  await expect(page.getByText("G1 已批准并锁定")).toBeVisible();

  await page.getByRole("button", { name: "查看影响与回滚" }).click();
  const dialog = page.getByRole("dialog", { name: /季岚 · 影响分析与回滚/ });
  await dialog.getByRole("radio", { name: /v2 · APPROVED/ }).click();
  await dialog.getByRole("button", { name: "回滚业务引用至 v2" }).click();
  await expect(page.getByText("季岚 已切换；G1 r8 等待重审")).toBeVisible();
  await expect(page.getByText("等待人工审核", { exact: true })).toBeVisible();
  await expect(page.getByText("基础原画 · 14 项 · 季岚 v2")).toBeVisible();

  await page.getByRole("button", { name: "剧本与分镜 G2" }).click();
  await expect(page.getByRole("button", { name: "等待上游" })).toBeDisabled();
  await expect(page.getByText(/S03-01、S03-02、S03-04 的资产引用发生变化/)).toBeVisible();

  await page.getByRole("button", { name: "内容与资产 G1" }).click();
  await page.getByRole("button", { name: "开始审核" }).click();
  await page.getByRole("button", { name: "批准并锁定 r8" }).click();
  await page.getByRole("button", { name: "剧本与分镜 G2" }).click();
  await expect(page.getByText("资产引用已变化，此 G2 revision 不可继续批准")).toBeVisible();
  await expect(page.getByRole("button", { name: "依赖已变更" })).toBeDisabled();
  await page.getByRole("button", { name: "创建新 revision" }).click();
  await expect(page.getByText("已创建 G2 revision 10")).toBeVisible();
  await expect(page.getByText("资产引用已变化，此 G2 revision 不可继续批准")).toHaveCount(0);
});

test("terminal Provider states survive later callbacks and batch completion", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "任务中心" }).click();
  const cancelledRow = page.getByRole("row").filter({ hasText: "S03-02" });
  await page.getByRole("button", { name: /取消.*CANCELLED 终态/ }).click();
  await expect(cancelledRow.getByText("已取消", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: /乱序.*禁止回退/ }).click();
  await expect(page.getByText("CANCELLED 终态保持不变")).toBeVisible();
  await expect(cancelledRow.getByText("已取消", { exact: true })).toBeVisible();

  await page.reload();
  await page.getByRole("button", { name: "内容与资产 G1" }).click();
  await page.getByRole("button", { name: "开始审核" }).click();
  await page.getByRole("button", { name: "批准并锁定 r7" }).click();
  await page.getByRole("button", { name: "剧本与分镜 G2" }).click();
  await page.getByRole("button", { name: "开始审核" }).click();
  await page.getByRole("button", { name: "批准并锁定 r9" }).click();
  await page.getByRole("button", { name: "任务中心" }).click();
  const failedRow = page.getByRole("row").filter({ hasText: "S03-02" });
  await page.getByRole("button", { name: /失败.*FAILED 终态/ }).click();
  await expect(failedRow.getByText("失败", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "完成 Mock 排练" }).click();
  await expect(page.getByText("Mock 排练未解锁 G3")).toBeVisible();
  await expect(failedRow.getByText("失败", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "成片审核 G3" }).click();
  await expect(page.getByRole("button", { name: "等待上游" })).toBeDisabled();
});

test("FAILED creates a new Job attempt, preserves the old terminal row, and unlocks G3 only after success", async ({
  page,
}) => {
  await page.goto("/");
  await approveG1AndG2(page);
  await page.getByRole("button", { name: "任务中心" }).click();

  await page.getByRole("button", { name: /失败.*FAILED 终态/ }).click();
  const oldJob = page.getByRole("row").filter({ has: page.getByText("job-v-032", { exact: true }) });
  await expect(oldJob.getByText("失败", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "为 S03-02 创建新 attempt" }).click();

  await expect(page.getByText("S03-02 attempt 2 已创建")).toBeVisible();
  await expect(oldJob.getByText("失败", { exact: true })).toBeVisible();
  await expect(oldJob.getByText("历史 attempt", { exact: true })).toBeVisible();
  await expect(oldJob.getByText("历史终态已保留", { exact: true })).toBeVisible();
  const newJob = page.getByRole("row").filter({ has: page.getByText("job-v-032-a2", { exact: true }) });
  await expect(newJob.getByText("当前 attempt", { exact: true })).toBeVisible();
  await expect(newJob.getByText("排队中", { exact: true })).toBeVisible();
  await expect(newJob.getByText("attempt 2 · retry 0", { exact: true })).toBeVisible();

  await page.getByRole("button", { name: "成片审核 G3" }).click();
  await expect(page.getByRole("button", { name: "等待上游" })).toBeDisabled();
  await page.getByRole("button", { name: "任务中心" }).click();
  await page.getByRole("button", { name: "完成 Mock 排练" }).click();

  await expect(oldJob.getByText("失败", { exact: true })).toBeVisible();
  await expect(newJob.getByText("已完成", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "成片审核 G3" }).click();
  await expect(page.getByRole("button", { name: "开始审核" })).toBeEnabled();
});

test("CANCELLED creates a new Job attempt while its old evidence remains immutable", async ({ page }) => {
  await page.goto("/");
  await approveG1AndG2(page);
  await page.getByRole("button", { name: "任务中心" }).click();

  await page.getByRole("button", { name: /取消.*CANCELLED 终态/ }).click();
  const oldJob = page.getByRole("row").filter({ has: page.getByText("job-v-032", { exact: true }) });
  await expect(oldJob.getByText("已取消", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "为 S03-02 创建新 attempt" }).click();

  await expect(oldJob.getByText("已取消", { exact: true })).toBeVisible();
  await expect(oldJob.getByText("历史终态已保留", { exact: true })).toBeVisible();
  const newJob = page.getByRole("row").filter({ has: page.getByText("job-v-032-a2", { exact: true }) });
  await expect(newJob.getByText("attempt 2 · retry 0", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "完成 Mock 排练" }).click();
  await expect(newJob.getByText("已完成", { exact: true })).toBeVisible();
  await expect(oldJob.getByText("已取消", { exact: true })).toBeVisible();

  await page.getByRole("button", { name: "成片审核 G3" }).click();
  await expect(page.getByRole("button", { name: "开始审核" })).toBeEnabled();
});

test("429 infrastructure retry keeps the same Job ID and creative attempt", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "任务中心" }).click();
  await page.getByRole("button", { name: /429.*限流等待/ }).click();

  const job = page.getByRole("row").filter({ has: page.getByText("job-v-032", { exact: true }) });
  await job.getByRole("button", { name: "重试" }).click();
  await expect(job.getByText("job-v-032", { exact: true })).toBeVisible();
  await expect(job.getByText("attempt 1 · retry 2", { exact: true })).toBeVisible();
  await expect(page.getByText("job-v-032-a2", { exact: true })).toHaveCount(0);
  await expect(page.getByText("5 个当前任务", { exact: true })).toBeVisible();
});

test("project dialog traps focus, closes on Escape, and restores the trigger", async ({ page }) => {
  await page.goto("/");
  const trigger = page.getByRole("button", { name: "切换或新建项目" });
  await trigger.click();

  await expect(page.getByLabel("项目名称")).toBeFocused();
  const submit = page.getByRole("button", { name: "创建并生成结构草稿" });
  await submit.focus();
  await page.keyboard.press("Tab");
  await expect(page.getByRole("button", { name: "关闭新建项目" })).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(trigger).toBeFocused();
});

test("job projection has loading, empty, and explicit unrecoverable states", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "任务中心" }).click();

  await page.getByRole("button", { name: "加载中" }).click();
  await expect(page.getByLabel("正在加载 Provider jobs")).toHaveAttribute("aria-busy", "true");
  await page.getByRole("button", { name: "空列表" }).click();
  await expect(page.getByText("还没有 Provider 任务")).toBeVisible();
  await page.getByRole("button", { name: "不可恢复" }).click();
  await expect(page.getByRole("alert")).toContainText("ARTIFACT_STORE_UNAVAILABLE");
  await expect(page.getByRole("alert")).toContainText("retryable=false");
  await page.getByRole("button", { name: "重新加载投影" }).click();
  await expect(page.getByRole("table")).toBeVisible();
});

test("390px mobile flow keeps the critical Mock action reachable through G3", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");

  await page.getByRole("button", { name: "资产", exact: true }).click();
  await page.getByRole("button", { name: "开始审核" }).click();
  await page.getByRole("button", { name: "批准并锁定 r7" }).click();
  await page.getByRole("button", { name: "分镜", exact: true }).click();
  await page.getByRole("button", { name: "开始审核" }).click();
  await page.getByRole("button", { name: "批准并锁定 r9" }).click();
  await page.getByRole("button", { name: "任务", exact: true }).click();

  const complete = page.getByRole("button", { name: "完成 Mock 排练" });
  await expect(complete).toBeVisible();
  await expect(complete).toBeEnabled();
  await complete.click();
  await page.getByRole("button", { name: "成片", exact: true }).click();
  await page.getByRole("button", { name: "开始审核" }).click();
  await page.getByRole("button", { name: "批准并锁定 r2" }).click();
  await expect(page.getByText("G3 已批准并锁定")).toBeVisible();
});

test("prompt rollback changes only the selected reference", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "版本与谱系" }).click();
  await expect(page.getByText("prompt-s033-r4")).toBeVisible();
  await page.getByRole("button", { name: "选择 r2" }).click();
  await expect(page.getByText("prompt-s033-r2")).toBeVisible();
  await expect(page.getByText("当前就是锁定版，没有差异。")).toBeVisible();
});
