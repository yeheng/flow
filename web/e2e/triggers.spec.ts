import { expect, test } from "@playwright/test";
import { seedWorkflow, uniq, waitTerminal } from "./helpers";

test("触发器页：webhook 创建/复制/POST 触发/归因；非法 cron 报错", async ({
  page,
  request,
  context,
}) => {
  const wf = await seedWorkflow(uniq("e2e-触发器"), "return 1;");
  await page.goto(`/workflows/${wf.id}/triggers`);
  await expect(page.getByRole("heading", { name: /触发器/ })).toBeVisible();

  // 新建 webhook → 列表出现完整 hook URL
  await page.getByRole("button", { name: "新建 webhook" }).click();
  const urlCell = page.locator(".hook-url").first();
  await expect(urlCell).toContainText("/hook/");
  const hookUrl = (await urlCell.innerText()).trim();

  // 复制：授权剪贴板后内容与 URL 一致
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await page.getByRole("button", { name: "复制" }).first().click();
  await expect(page.locator(".toast-success", { hasText: "已复制" })).toBeVisible();
  const copied = await page.evaluate(() => navigator.clipboard.readText());
  expect(copied).toBe(hookUrl);

  // 直接 POST hook URL（绕过页面，验证 HTTP 入口真实可用）→ 200 + run_id
  const resp = await request.post(hookUrl, { data: { src: "e2e" } });
  expect(resp.status()).toBe(200);
  const { run_id } = (await resp.json()) as { run_id: string };
  expect(await waitTerminal(run_id)).toBe("succeeded");

  // 运行记录出现来源=Webhook 的行
  await page.goto(`/workflows/${wf.id}/runs`);
  await expect(page.locator(".data-table tr", { hasText: "Webhook" })).toBeVisible();

  // 非法 cron：后端 -32010 以错误 toast 呈现
  await page.goto(`/workflows/${wf.id}/triggers`);
  await page.locator(".cron-input").fill("not a cron");
  await page.getByRole("button", { name: "新建调度" }).click();
  await expect(page.locator(".toast-error")).toContainText("cron");
});
