import { expect, test } from "@playwright/test";
import { rpc, seedWorkflow, uniq, waitTerminal } from "./helpers";

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

test("可观察性：日志页签实时显示 console 输出，节点检查器展示输入面/输出/脱敏", async ({
  page,
}) => {
  const name = uniq("e2e-可观察");
  const { workflow_id } = await rpc<{ workflow_id: string }>("workflow.create", { name });
  const { version } = await rpc<{ version: number }>("workflow.update", {
    workflow_id,
    definition: {
      nodes: [
        { id: "s", type: "start" },
        {
          id: "n",
          type: "script",
          name: "脚本节点",
          params: {
            code: "console.log('hello e2e', input);\nconsole.error('boom e2e');\nreturn { ok: input.n };",
            note: "n=${input.n}",
            token: "sk-should-be-redacted",
          },
        },
        { id: "e", type: "end" },
      ],
      edges: [
        { from: "s", to: "n" },
        { from: "n", to: "e" },
      ],
    },
  });
  await rpc("workflow.publish", { workflow_id, version });
  const { run_id: runId } = await rpc<{ run_id: string }>("run.start", {
    workflow_id,
    input: { n: 7 },
  });
  await page.goto(`/runs/${runId}`);
  await expect(page.locator(".run-header .badge").first()).toContainText("成功", {
    timeout: 15_000,
  });

  // 日志页签：历史日志来自订阅回放（seq=1 起完整重放），终态 run 也能看到
  await page.locator(".aside-tab", { hasText: "日志" }).click();
  await expect(page.locator(".log-list")).toContainText("hello e2e");
  await expect(page.locator(".log-list")).toContainText("boom e2e");
  await expect(page.locator(".log-list .log-error")).toHaveCount(1);
  // stdout/stderr 来源徽章
  await expect(page.locator(".log-list .log-row", { hasText: "hello e2e" })).toContainText(
    "stdout",
  );

  // 节点检查器：点日志行的节点标签 → 输入面（模板展开 + 脱敏）与输出
  await page.locator(".log-list .log-row", { hasText: "hello e2e" }).locator(".log-node").click();
  const inspector = page.locator(".inspector");
  await expect(inspector).toBeVisible();
  await expect(inspector).toContainText("输入面（模板展开后）");
  await expect(inspector).toContainText("n=7");
  await expect(inspector).not.toContainText("sk-should-be-redacted");
  await expect(inspector).toContainText("***");
  await expect(inspector).toContainText("输出");
  await expect(inspector).toContainText("ok");

  // 日志的节点过滤：选择节点后只剩该节点的行
  await page.locator(".log-node-select").selectOption("n");
  await expect(page.locator(".log-list .log-row")).toHaveCount(2);
});

/**
 * 实时路径的 output 脱敏（回归「只脱 timeline 不脱订阅」的绕过）。
 *
 * 关键在于**attach 时 run 还在跑**：node_completed 事件到达时 seq 大于
 * timeline 快照的 last_seq，前端会把它应用到投影上。老代码下这个值来自
 * 原始事件，于是节点检查器显示未脱敏的 output；run 终态后再进来反而
 * 看到脱敏值（回放段全被 seqAction 跳过，只剩 timeline 的展示值）。
 */
test("可观察性：运行中收到的 node_completed 也走展示脱敏", async ({ page }) => {
  const name = uniq("e2e-实时脱敏");
  const { workflow_id } = await rpc<{ workflow_id: string }>("workflow.create", { name });
  const { version } = await rpc<{ version: number }>("workflow.update", {
    workflow_id,
    definition: {
      nodes: [
        { id: "s", type: "start" },
        // delay 2s 给页面留出「运行中 attach」的窗口
        { id: "d", type: "delay", params: { ms: 2000 } },
        {
          id: "n",
          type: "script",
          params: {
            code: "console.log('late node done');\nreturn { ok: 1, token: ['sk','live','secret'].join('-') };",
          },
        },
        { id: "e", type: "end" },
      ],
      edges: [
        { from: "s", to: "d" },
        { from: "d", to: "n" },
        { from: "n", to: "e" },
      ],
    },
  });
  await rpc("workflow.publish", { workflow_id, version });
  const { run_id: runId } = await rpc<{ run_id: string }>("run.start", {
    workflow_id,
    input: {},
  });

  // 立刻进详情页：确认此刻 run 仍在跑（这是本用例的前提——node_completed
  // 必须是在页面 attach **之后**实时到达的，而不是回放段里被 seqAction 跳过的）
  await page.goto(`/runs/${runId}`);
  await expect(page.locator(".run-header .badge").first()).toContainText("运行中");
  // 等后端到终态（头部徽章是 run.get 的一次性快照，不随订阅更新；等它没用）
  expect(await waitTerminal(runId)).toBe("succeeded");

  // 从日志行点节点标签打开检查器（这条路径不依赖画布选中的工作流）
  await page.locator(".aside-tab", { hasText: "日志" }).click();
  await page
    .locator(".log-list .log-row", { hasText: "late node done" })
    .locator(".log-node")
    .click();
  const inspector = page.locator(".inspector");
  await expect(inspector).toBeVisible();
  await expect(inspector).toContainText("输出");
  await expect(inspector).toContainText("ok");
  await expect(inspector).not.toContainText("sk-live-secret");
  await expect(inspector).toContainText("***");
});
