import { expect, test } from "@playwright/test";
import { rpc, seedWorkflow, uniq, waitTerminal } from "./helpers";
import { E2E_TOKEN } from "./config";

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

  // 底部运行抽屉：可手动收起，顶栏「控制台」再展开
  await page.getByRole("button", { name: "收起 ▾" }).click();
  await expect(page.locator(".run-drawer")).toHaveCount(0);
  await page.getByRole("button", { name: "控制台 ▴" }).click();
  await expect(page.locator(".run-drawer")).toBeVisible();

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

test("可观察性：Journal 观测日志与节点检查器展示输入面/输出/脱敏", async ({ page }) => {
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
  expect(await waitTerminal(runId)).toBe("succeeded");
  await page.goto(`/runs/${runId}`);
  await expect(page.locator(".run-header .badge").first()).toContainText("成功", {
    timeout: 15_000,
  });

  // V2 将 console 输出写入独立观测存储，节点检查器从运行画布打开。
  await page.locator('.vue-flow__node[data-id="n"]').click();
  const inspector = page.locator(".inspector");
  await expect(inspector).toBeVisible();
  await expect(inspector).toContainText("输入面（模板展开后）");
  await expect(inspector).toContainText("n=7");
  await expect(inspector).not.toContainText("sk-should-be-redacted");
  await expect(inspector).toContainText("***");
  await expect(inspector).toContainText("输出");
  await expect(inspector).toContainText("ok");

  // 真实 /journal 客户端以原生 V2 格式订阅，并显示独立观测日志。
  await page.goto("/journal");
  const inputs = page.locator(".connect-bar input");
  await inputs.nth(0).fill("ws://127.0.0.1:19311");
  await inputs.nth(1).fill("http://127.0.0.1:19312");
  await inputs.nth(2).fill(E2E_TOKEN);
  await page.getByRole("button", { name: "连接", exact: true }).click();
  const journalName = uniq("e2e-Journal回执");
  await page.getByPlaceholder("新工作流名称").fill(journalName);
  await page.getByRole("button", { name: "新建", exact: true }).click();
  await expect(page.locator(".journal-status")).toHaveText("已提交并可见");
  await expect(page.locator(".journal-side select option:checked")).toHaveText(journalName);
  const runRow = page.locator("tr").filter({ has: page.locator(`td[title="${runId}"]`) });
  await runRow.getByRole("button", { name: "监控", exact: true }).click();
  const observations = page.locator(".journal-detail .journal-log").last();
  await expect(observations).toContainText("hello e2e");
  await expect(observations).toContainText("boom e2e");
  await expect(observations.locator(".log-error")).toHaveCount(1);
  await observations.locator(".log-search").fill("hello e2e");
  await expect(observations.locator(".log-row")).toHaveCount(1);
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

  // 画布节点打开检查器，保留实时 node_completed 的脱敏断言。
  await page.locator('.vue-flow__node[data-id="n"]').click();
  const inspector = page.locator(".inspector");
  await expect(inspector).toBeVisible();
  await expect(inspector).toContainText("输出");
  await expect(inspector).toContainText("ok");
  await expect(inspector).not.toContainText("sk-live-secret");
  await expect(inspector).toContainText("***");
});
