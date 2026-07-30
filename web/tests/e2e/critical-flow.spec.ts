import { expect, test } from "@playwright/test";

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
  await expect(page.getByText("生成中", { exact: true }).first()).toBeVisible();
});

test("prompt rollback changes only the selected reference", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "版本与谱系" }).click();
  await expect(page.getByText("prompt-s033-r4")).toBeVisible();
  await page.getByRole("button", { name: "选择 r2" }).click();
  await expect(page.getByText("prompt-s033-r2")).toBeVisible();
  await expect(page.getByText("当前就是锁定版，没有差异。")).toBeVisible();
});
