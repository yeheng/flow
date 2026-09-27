import { expect, test } from "@playwright/test";
import { seedWorkflow, uniq, waitTerminal } from "./helpers";

test("运行链路：点运行 → RunPanel 终态 → 列表来源=手动 → 详情页时间线", async ({ page }) => {
  const name = uniq("e2e-运行");
  const wf = await seedWorkflow(name, "return 7;");
  await page.goto(`/workflows/${wf.id}`);

  // 点运行：RunPanel 出现 run 条目与阶段徽章
  await page.getByRole("button", { name: "运行" }).click();
  await expect(page.locator(".run-monitor .run-id")).toBeVisible();
  const runIdText = await page.locator(".run-monitor .run-id").innerText();
  const runPrefix = runIdText.replace(/^run /, "").replace(/…$/, "");
  expect(runPrefix.length).toBeGreaterThan(0);

  // 终态：徽章变绿（成功）。运行面板订阅实时事件，poll 等待
  await expect(page.locator(".run-monitor .run-phase")).toHaveText("成功", { timeout: 15_000 });

  // 全局运行记录：该 run 出现在列表，来源=手动。
  // 按工作流名定位行（uuid v7 的 run id 前 8 位在同毫秒窗口内会撞）
  await page.goto("/runs");
  const row = page.locator(".data-table tr", { hasText: name });
  await expect(row).toBeVisible();
  await expect(row).toContainText(runPrefix);
  await expect(row).toContainText("手动");
  await expect(row.getByRole("link", { name: "详情" })).toBeVisible();

  // 详情页：状态徽章 + 时间线有 3 个节点行
  await row.getByRole("link", { name: "详情" }).click();
  await expect(page).toHaveURL(/\/runs\/[0-9a-f-]{36}/);
  await expect(page.locator(".run-header .badge").first()).toContainText("成功");
  await expect(page.locator("table.timeline tbody tr")).toHaveCount(3);

  // 后端确认已终结（双保险，防 UI 假绿）
  // runPrefix 是 run_id 前 8 位，waitTerminal 需要完整 id，从 URL 取
  const runId = page.url().split("/runs/")[1];
  expect(await waitTerminal(runId)).toBe("succeeded");
});
