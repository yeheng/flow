import { expect, test } from "@playwright/test";
import { rpc, seedWorkflow, uniq, waitTerminal } from "./helpers";

/** 发布一个新版本并返回版本号（seedWorkflow 只发布 v1）。 */
async function publishVersion(workflowId: string, code: string): Promise<number> {
  const { version } = await rpc<{ version: number }>("workflow.update", {
    workflow_id: workflowId,
    definition: {
      nodes: [
        { id: "s", type: "start" },
        { id: "n", type: "script", params: { code } },
        { id: "e", type: "end" },
      ],
      edges: [
        { from: "s", to: "n" },
        { from: "n", to: "e" },
      ],
    },
  });
  await rpc("workflow.publish", { workflow_id: workflowId, version });
  return version;
}

test("回归：同版本复现基线，自动对比通过", async ({ page }) => {
  const name = uniq("e2e-回归复现");
  const wf = await seedWorkflow(name, "return {time: Date.now(), random: Math.random()};");
  const { run_id: baseline } = await rpc<{ run_id: string }>("run.start", {
    workflow_id: wf.id,
  });
  expect(await waitTerminal(baseline)).toBe("succeeded");

  await page.goto(`/runs/${baseline}`);
  await expect(page.locator(".run-header .badge").first()).toContainText("成功");

  // 候选默认取最新已发布版本（这里只有 v1），提交后跳到回归候选 run
  await page.getByRole("button", { name: "回归", exact: true }).click();
  await expect(page.locator("#regress-version")).toHaveValue(String(wf.version));
  await page.getByRole("button", { name: "开始回归" }).click();
  await expect(page.locator(".regress-modal")).toHaveCount(0);
  await expect(page).not.toHaveURL(new RegExp(baseline));

  const candidate = page.url().split("/runs/")[1];
  expect(candidate).toBeTruthy();
  expect(await waitTerminal(candidate)).toBe("succeeded");
  await expect(page.locator(".run-header .badge").first()).toContainText("成功", {
    timeout: 15_000,
  });
  // 回归候选终结后自动打开对比：同版本复现 → 一致
  await expect(page.locator(".compare-modal")).toBeVisible({ timeout: 15_000 });
  await expect(page.locator(".compare-modal")).toContainText("与基线一致");
  await expect(page.locator(".run-header-meta")).toContainText("回归自基线");
});

test("回归：候选版本输出变化，对比给出差异", async ({ page }) => {
  const name = uniq("e2e-回归差异");
  const wf = await seedWorkflow(name, "return {version: 1};");
  const { run_id: baseline } = await rpc<{ run_id: string }>("run.start", {
    workflow_id: wf.id,
  });
  expect(await waitTerminal(baseline)).toBe("succeeded");

  const v2 = await publishVersion(wf.id, "return {version: 2};");
  await page.goto(`/runs/${baseline}`);
  await expect(page.locator(".run-header .badge").first()).toContainText("成功");

  await page.getByRole("button", { name: "回归", exact: true }).click();
  await expect(page.locator("#regress-version")).toHaveValue(String(v2));
  await page.getByRole("button", { name: "开始回归" }).click();
  await expect(page).not.toHaveURL(new RegExp(baseline));
  const candidate = page.url().split("/runs/")[1];
  expect(candidate).toBeTruthy();
  expect(await waitTerminal(candidate)).toBe("succeeded");

  await expect(page.locator(".compare-modal")).toBeVisible({ timeout: 15_000 });
  await expect(page.locator(".compare-modal")).toContainText(/处与基线不同/);
  await expect(page.locator(".compare-modal")).toContainText("最终输出");

  // 运行记录列表把候选标为回归，并可回链基线（uuid v7 前 8 位会撞，按详情链接定位）
  await page.goto("/runs");
  const row = page
    .locator(".data-table tbody tr")
    .filter({ has: page.locator(`a[href="/runs/${candidate}"]`) });
  await expect(row).toHaveCount(1);
  await expect(row).toContainText("手动");
  await expect(row.getByRole("link", { name: "回归" })).toBeVisible();
});
