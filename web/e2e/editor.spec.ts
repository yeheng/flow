import { expect, test } from "@playwright/test";
import { rpc, seedWorkflow, uniq } from "./helpers";

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

test("可复用节点模板：存为模板 → 另一流程插入 → 刷新仍在", async ({ page }) => {
  const name = uniq("e2e-模板");
  const wf = await seedWorkflow(name, "return 1;");

  // 一个供对照的空工作流（只有 start），验证模板插入的是片段而非整图
  const target = await seedWorkflow(uniq("e2e-模板目标"), "return 0;");
  await rpc("workflow.update", {
    workflow_id: target.id,
    definition: {
      nodes: [
        { id: "s", type: "start" },
        { id: "e", type: "end" },
      ],
      edges: [{ from: "s", to: "e" }],
    },
  });

  // 打开源流程：选中 script 节点（模板保存的是选中集 + 内部连线）
  await page.goto(`/workflows/${wf.id}`);
  await expect(page.locator(".vue-flow__node")).toHaveCount(3);
  await page.locator(".vue-flow__node").nth(1).click();

  // 存为模板：面板按钮 → prompt 弹窗输入名称
  await page.locator(".palette-save").click();
  const promptInput = page.locator(".modal input");
  await expect(promptInput).toBeVisible();
  const tplName = uniq("模板-取数");
  await promptInput.fill(tplName);
  await page.locator(".modal-actions button", { hasText: "确定" }).click();

  // 面板「模板」区出现新模板
  const tplItem = page.locator(".palette-template", { hasText: tplName });
  await expect(tplItem).toBeVisible();

  // 另一流程：点击模板插入
  await page.goto(`/workflows/${target.id}`);
  await expect(page.locator(".vue-flow__node")).toHaveCount(2);
  await page.locator(".palette-template", { hasText: tplName }).click();
  await expect(page.locator(".vue-flow__node")).toHaveCount(3, { timeout: 10_000 });
  // 插入的是模板片段（孤立未连线是预期，用户自行接线）；params 随模板带入
  // 由 state/templates 单测覆盖，这里断言节点渲染即可
  await expect(page.locator(".vue-flow__node").nth(2)).toContainText("脚本");

  // 重载后模板仍在（服务端持久化）
  await page.reload();
  await expect(page.locator(".palette-template", { hasText: tplName })).toBeVisible();

  // 清理：删除模板
  await page.locator(".palette-template", { hasText: tplName }).hover();
  await page.locator(".palette-template .tpl-btn.danger").click();
  await page.locator(".modal-actions button", { hasText: "确定" }).click();
  await expect(page.locator(".palette-template", { hasText: tplName })).toHaveCount(0);
});
