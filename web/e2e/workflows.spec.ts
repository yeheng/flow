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
