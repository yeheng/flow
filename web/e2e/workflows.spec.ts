import { expect, test } from "@playwright/test";
import { uniq } from "./helpers";

test("工作流列表：渲染、modal 新建、跳转编辑器、列表出现新行", async ({ page }) => {
  await page.goto("/workflows");
  await expect(page.getByRole("heading", { name: "工作流" })).toBeVisible();

  const name = uniq("e2e-列表");
  await page.getByRole("button", { name: "新建" }).click();
  await page.locator(".modal input").fill(name);
  await page.locator(".modal").getByRole("button", { name: "确定" }).click();

  // 新建后跳转编辑器（URL 带 workflow id），顶部条显示名称
  await expect(page).toHaveURL(/\/workflows\/[0-9a-f-]{36}/);
  await expect(page.locator(".editor-title")).toContainText(name);

  // 回到列表：新行出现
  await page.goto("/workflows");
  await expect(page.locator(".data-table tr", { hasText: name })).toBeVisible();
});

test("导入工作流 JSON：文件选择 → 名字弹窗 → 列表出现且已发布", async ({ page }) => {
  await page.goto("/workflows");
  const name = uniq("e2e-导入");

  // 信封格式（examples/*.workflow.json 同款）；buffer 直接喂文件选择器
  const envelope = JSON.stringify({
    name,
    description: "e2e 导入用例",
    definition: {
      nodes: [
        { id: "s", type: "start", position: { x: 0, y: 0 } },
        { id: "n", type: "script", params: { code: "return 1;" }, position: { x: 120, y: 0 } },
        { id: "e", type: "end", position: { x: 240, y: 0 } },
      ],
      edges: [
        { from: "s", to: "n" },
        { from: "n", to: "e" },
      ],
    },
  });
  await page.locator(".page-header button", { hasText: "导入" }).click();
  await page
    .locator("input[type=file]")
    .setInputFiles({ name: "import-case.json", mimeType: "application/json", buffer: Buffer.from(envelope) });

  // 名字弹窗预填信封 name，确定后导入并发布
  await expect(page.locator(".modal input")).toHaveValue(name);
  await page.locator(".modal").getByRole("button", { name: "确定" }).click();
  await expect(page.locator(".toast-success")).toContainText("已导入");

  // 列表出现新行且已有发布版本
  const row = page.locator(".data-table tr", { hasText: name });
  await expect(row).toBeVisible();
  await expect(row.locator(".published")).toHaveText(/v\d+/);

  // 打开编辑器：3 个节点齐
  await row.getByRole("link", { name: "打开" }).click();
  await expect(page.locator(".vue-flow__node")).toHaveCount(3);

  // 同名再次导入（定义有变化）：追加版本 v2；完全相同的定义会被服务端
  // checksum 去重复用版本号，不会刷版本
  await page.goto("/workflows");
  const changed = JSON.stringify({
    name,
    definition: {
      nodes: [
        { id: "s", type: "start", position: { x: 0, y: 0 } },
        { id: "n", type: "script", params: { code: "return 2;" }, position: { x: 120, y: 0 } },
        { id: "e", type: "end", position: { x: 240, y: 0 } },
      ],
      edges: [
        { from: "s", to: "n" },
        { from: "n", to: "e" },
      ],
    },
  });
  await page.locator(".page-header button", { hasText: "导入" }).click();
  await page
    .locator("input[type=file]")
    .setInputFiles({ name: "import-case.json", mimeType: "application/json", buffer: Buffer.from(changed) });
  await page.locator(".modal").getByRole("button", { name: "确定" }).click();
  await expect(page.locator(".toast-success")).toContainText("追加版本");
  await expect(page.locator(".data-table tr", { hasText: name }).locator("td").nth(1)).toHaveText("v2");
});
