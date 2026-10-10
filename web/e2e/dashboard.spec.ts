import { expect, test } from "@playwright/test";
import { rpc, runStats, seedWorkflow, startRun, uniq, waitTerminal } from "./helpers";

/** start → human_task → end：run 停在等待信号，造「进行中」 */
async function seedHumanWorkflow(name: string): Promise<string> {
  const { workflow_id } = await rpc<{ workflow_id: string }>("workflow.create", { name });
  const { version } = await rpc<{ version: number }>("workflow.update", {
    workflow_id,
    definition: {
      nodes: [
        { id: "s", type: "start" },
        { id: "h", type: "human_task" },
        { id: "e", type: "end" },
      ],
      edges: [
        { from: "s", to: "h" },
        { from: "h", to: "e" },
      ],
    },
  });
  await rpc("workflow.publish", { workflow_id, version });
  return workflow_id;
}

test("仪表盘：卡片数字与 run.stats 精确一致，按工作流分组表正确", async ({ page }) => {
  // 种子：2 成功 + 1 失败 + 1 进行中（workers=1，本用例独占写入窗口）
  const okName = uniq("e2e-成功");
  const failName = uniq("e2e-失败");
  const wfOk = await seedWorkflow(okName, "return 1;");
  const wfFail = await seedWorkflow(failName, "throw new Error('boom');");
  const wfHuman = await seedHumanWorkflow(uniq("e2e-人工"));

  expect(await waitTerminal(await startRun(wfOk.id))).toBe("succeeded");
  expect(await waitTerminal(await startRun(wfOk.id))).toBe("succeeded");
  expect(await waitTerminal(await startRun(wfFail.id))).toBe("failed");
  await startRun(wfHuman); // 停在 human_task，不终结
  await expect.poll(async () => (await runStats(wfHuman)).total, { timeout: 10_000 }).toBe(1);

  // 期望值直接从 RPC 拿，与页面卡片对比
  const stats = await runStats();
  const succeeded = stats.by_status["succeeded"] ?? 0;
  const failed = stats.by_status["failed"] ?? 0;
  const cancelled = stats.by_status["cancelled"] ?? 0;
  const terminal = succeeded + failed + cancelled;
  const expectRate = terminal > 0 ? `${Math.round((succeeded / terminal) * 100)}%` : "—";
  const active =
    (stats.by_status["running"] ?? 0) + (stats.by_status["awaiting_resume"] ?? 0);

  await page.goto("/");
  const card = (label: string) => page.locator(".card", { hasText: label }).locator(".card-value");
  await expect(card("运行总数")).toHaveText(String(stats.total));
  await expect(card("成功率")).toHaveText(expectRate);
  await expect(card("进行中")).toHaveText(String(active));

  // 按工作流成功率表：名称在种子里唯一（uniq），按名称定位行
  const table = page.locator("section", { hasText: "按工作流成功率" });
  const okRow = table.locator("tr", { hasText: okName });
  await expect(okRow).toContainText("2");
  await expect(okRow).toContainText("100%");
  await expect(table.locator("tr", { hasText: failName })).toContainText("0%");

  // 最近运行表有内容
  await expect(
    page.locator("section", { hasText: "最近运行" }).locator("tbody tr").first(),
  ).toBeVisible();
});
