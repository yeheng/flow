import { expect, test } from "@playwright/test";
import { seedWorkflow, uniq } from "./helpers";

test("编辑器：画布渲染、顶部条、改参数保存无错误", async ({ page }) => {
  const name = uniq("e2e-编辑器");
  const wf = await seedWorkflow(name, "return 1;");
  await page.goto(`/workflows/${wf.id}`);

  // vue-flow 画布渲染出 3 个节点（start/script/end）
  await expect(page.locator(".vue-flow__node")).toHaveCount(3);

  // 顶部条：工作流名 + 版本入口
  await expect(page.locator(".editor-title")).toContainText(name);
  await expect(page.locator(".editor-title")).toContainText("v1");
  await expect(page.locator(".editor-actions").getByRole("link", { name: "版本" })).toBeVisible();

  // 点 script 节点（定义顺序第二）→ 参数面板出现代码编辑框
  await page.locator(".vue-flow__node").nth(1).click();
  const codeArea = page.locator("aside.right textarea.code").first();
  await expect(codeArea).toBeVisible();
  await codeArea.fill("return 42;");

  // 保存：成功 toast，无错误 toast，「未保存」标记消失
  await page.getByRole("button", { name: "保存" }).click();
  await expect(page.locator(".toast-success")).toContainText("已保存");
  await expect(page.locator(".toast-error")).toHaveCount(0);
  await expect(page.locator(".wf-dirty")).toHaveCount(0);
});
