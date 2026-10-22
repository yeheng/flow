import { expect, test } from "@playwright/test";
import { rpc, seedWorkflow, startRun, uniq, waitTerminal } from "./helpers";

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
  await page.locator("input[type=file]").setInputFiles({
    name: "import-case.json",
    mimeType: "application/json",
    buffer: Buffer.from(envelope),
  });

  // 名字弹窗预填信封 name，确定后导入并发布
  await expect(page.locator(".modal input")).toHaveValue(name);
  await page.locator(".modal").getByRole("button", { name: "确定" }).click();
  await expect(page.locator(".toast-success")).toContainText("已导入");

  await expect(page).toHaveURL(/\/workflows\/[0-9a-f-]{36}/);
  await page.goto("/workflows");
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
  await page.locator("input[type=file]").setInputFiles({
    name: "import-case.json",
    mimeType: "application/json",
    buffer: Buffer.from(changed),
  });
  await page.locator(".modal").getByRole("button", { name: "确定" }).click();
  await expect(page.locator(".toast-success")).toContainText("追加版本");
  await expect(page).toHaveURL(/\/workflows\/[0-9a-f-]{36}/);
  await page.goto("/workflows");
  await expect(page.locator(".data-table tr", { hasText: name }).locator("td").nth(1)).toHaveText(
    "v2",
  );
});

const simpleDefinition = {
  nodes: [
    { id: "s", type: "start" },
    { id: "e", type: "end" },
  ],
  edges: [{ from: "s", to: "e" }],
};

test("无效导入和取消补填都不产生空记录", async ({ page }) => {
  await page.goto("/workflows");
  const name = uniq("e2e-非法导入");
  await page.locator("input[type=file]").setInputFiles({
    name: "invalid.json",
    mimeType: "application/json",
    buffer: Buffer.from(JSON.stringify({ name, definition: { nodes: [], edges: [] } })),
  });
  await page.locator(".modal").getByRole("button", { name: "确定" }).click();
  await expect(page.locator(".toast-error")).toBeVisible();
  let listed = await rpc<{ workflows: { name: string }[] }>("workflow.list.view");
  expect(listed.workflows.some((w) => w.name === name)).toBe(false);

  await page.locator("input[type=file]").setInputFiles({
    name: "missing.json",
    mimeType: "application/json",
    buffer: Buffer.from(
      JSON.stringify({
        name,
        definition: {
          nodes: [
            { id: "s", type: "start" },
            { id: "n", type: "script", params: {} },
            { id: "e", type: "end" },
          ],
          edges: [
            { from: "s", to: "n" },
            { from: "n", to: "e" },
          ],
        },
      }),
    ),
  });
  await page.locator(".modal").getByRole("button", { name: "确定" }).click();
  await expect(page.locator(".modal-message")).toHaveText("补全工作流配置");
  await page.locator(".modal").getByRole("button", { name: "取消" }).click();
  listed = await rpc("workflow.list.view");
  expect(listed.workflows.some((w) => w.name === name)).toBe(false);
});

test("导入缺失密钥：密码表单补齐后再校验、发布", async ({ page }) => {
  const name = uniq("e2e-密钥导入");
  const secret = `IMPORT_${Date.now()}`;
  await page.goto("/workflows");
  await page.locator("input[type=file]").setInputFiles({
    name: "secret.json",
    mimeType: "application/json",
    buffer: Buffer.from(
      JSON.stringify({
        name,
        definition: {
          nodes: [
            { id: "s", type: "start" },
            {
              id: "n",
              type: "harness",
              params: { mode: "agent", prompt: "test", api_key: secret },
            },
            { id: "e", type: "end" },
          ],
          edges: [
            { from: "s", to: "n" },
            { from: "n", to: "e" },
          ],
        },
      }),
    ),
  });
  await page.locator(".modal").getByRole("button", { name: "确定" }).click();
  await expect(page.locator(".modal-message")).toHaveText("配置工作流需要的密钥");
  await page.locator(".modal input[type=password]").fill("test-import-secret-value");
  await page.locator(".modal").getByRole("button", { name: "保存并校验" }).click();
  await expect(page.locator(".toast-success")).toContainText("已导入");
  await expect(page).toHaveURL(/\/workflows\/[0-9a-f-]{36}/);
  const listed = await rpc<{ workflows: { name: string; published_version: number }[] }>(
    "workflow.list.view",
  );
  expect(listed.workflows.find((w) => w.name === name)?.published_version).toBe(1);
});

test("运行按发布版本的必填合同弹表单，取消不创建 run", async ({ page }) => {
  const name = uniq("e2e-必填输入");
  const workflow = await rpc<{ workflow_id: string }>("workflow.import", {
    name,
    definition: {
      ...simpleDefinition,
      input_schema: {
        type: "object",
        required: ["project"],
        properties: { project: { type: "string", title: "项目名称", minLength: 1 } },
        additionalProperties: false,
      },
    },
  });
  await page.goto(`/workflows/${workflow.workflow_id}`);
  await page.getByRole("button", { name: "运行", exact: true }).click();
  await expect(page.locator(".modal-message")).toHaveText("填写工作流运行输入");
  await page.locator(".modal").getByRole("button", { name: "运行", exact: true }).click();
  await expect(page.locator(".modal [role=alert]")).toContainText("项目名称");
  await page.locator(".modal").getByRole("button", { name: "取消" }).click();
  const runs = await rpc<{ runs: unknown[] }>("run.list.view", {
    workflow_id: workflow.workflow_id,
  });
  expect(runs.runs).toHaveLength(0);
  await page.getByRole("button", { name: "运行", exact: true }).click();
  await page.locator(".modal").getByLabel("项目名称").fill("00123");
  await page.locator(".modal").getByRole("button", { name: "运行", exact: true }).click();
  await expect(page.locator(".run-monitor .run-phase")).toHaveText("成功", { timeout: 15000 });
  const records = await rpc<{ runs: { input: unknown }[] }>("run.list.view", {
    workflow_id: workflow.workflow_id,
  });
  expect(records.runs[0].input).toEqual({ project: "00123" });
});

test("执行过的工作流可以删除：列表隐藏且运行历史可读", async ({ page }) => {
  const name = uniq("e2e-逻辑删除");
  const workflow = await seedWorkflow(name, "return 1;");
  const runId = await startRun(workflow.id);
  await waitTerminal(runId);
  await page.goto("/workflows");
  const row = page.locator("tbody tr", { hasText: name });
  await row.getByRole("button", { name: "删除" }).click();
  await expect(page.locator(".modal-message")).toContainText("已有运行记录会保留");
  await page.locator(".modal").getByRole("button", { name: "确定" }).click();
  await expect(row).toHaveCount(0);
  const history = await rpc<{ run: { id: string } }>("run.get.view", { run_id: runId });
  expect(history.run.id).toBe(runId);
  await page.goto(`/runs/${runId}`);
  await expect(page.locator(".run-header .badge").first()).toContainText("成功");
});

test("运行中等待 input 节点时自动弹表单，提交后继续执行", async ({ page }) => {
  const workflow = await rpc<{ workflow_id: string }>("workflow.import", {
    name: uniq("e2e-人工输入"),
    definition: {
      nodes: [
        { id: "s", type: "start" },
        {
          id: "input",
          type: "input",
          name: "人工确认",
          params: { fields: [{ key: "approval", label: "审批编号" }] },
        },
        { id: "e", type: "end" },
      ],
      edges: [
        { from: "s", to: "input" },
        { from: "input", to: "e" },
      ],
    },
  });
  const runId = await startRun(workflow.workflow_id);
  await page.goto(`/runs/${runId}`);
  await expect(page.locator(".modal-message")).toHaveText("填写「人工确认」");
  await page.locator(".modal").getByRole("button", { name: "取消" }).click();
  await page.getByRole("button", { name: "填写表单" }).click();
  await page.locator(".modal").getByLabel("审批编号").fill("approved");
  await page.locator(".modal").getByRole("button", { name: "提交并继续" }).click();
  expect(await waitTerminal(runId)).toBe("succeeded");
});

test("运行表单回填、字符串类型保持和跨工作流输入隔离", async ({ page }) => {
  const contract = {
    type: "object",
    required: ["id", "count"],
    properties: { id: { type: "string" }, count: { type: "integer" } },
    additionalProperties: false,
  };
  const a = await rpc<{ workflow_id: string }>("workflow.import", {
    name: uniq("e2e-输入A"),
    definition: { ...simpleDefinition, input_schema: contract },
  });
  const nameB = uniq("e2e-输入B");
  const b = await rpc<{ workflow_id: string }>("workflow.import", {
    name: nameB,
    definition: { ...simpleDefinition, input_schema: contract },
  });
  await page.goto(`/workflows/${a.workflow_id}`);
  await expect(page.locator(".vue-flow__node")).toHaveCount(2);
  await expect(page.locator(".wf-dirty")).toHaveCount(0);
  await page.getByRole("button", { name: "控制台 ▴" }).click();
  const panel = page.locator(".run-panel");
  await expect(panel.getByLabel("id", { exact: true })).toHaveValue("");
  await page.getByRole("button", { name: "运行", exact: true }).click();
  await page.locator(".modal").getByLabel("id", { exact: false }).fill("true");
  await page.locator(".modal").getByLabel("count", { exact: false }).fill("1");
  await page.locator(".modal").getByRole("button", { name: "运行", exact: true }).click();
  await expect(page.locator(".run-monitor .run-phase")).toHaveText("成功");
  await expect(panel.getByLabel("id", { exact: true })).toHaveValue("true");
  await panel.getByLabel("count", { exact: true }).fill("2");
  await page.getByRole("button", { name: "运行", exact: true }).click();
  await expect
    .poll(
      async () =>
        (await rpc<{ runs: { input: unknown }[] }>("run.list.view", { workflow_id: a.workflow_id }))
          .runs.length,
    )
    .toBe(2);
  await expect(page.locator(".modal")).toHaveCount(0);
  const runs = await rpc<{ runs: { input: unknown }[] }>("run.list.view", {
    workflow_id: a.workflow_id,
  });
  expect(runs.runs[0].input).toEqual({ id: "true", count: 2 });
  await page.getByRole("link", { name: "← 工作流列表" }).click();
  await page
    .locator("tbody tr", { hasText: nameB })
    .getByRole("link", { name: "打开", exact: true })
    .click();
  await expect(page).toHaveURL(`/workflows/${b.workflow_id}`);
  await expect(page.locator(".editor-title")).toContainText(nameB);
  await expect(panel.getByLabel("id", { exact: true })).toHaveValue("");
});

test("直接访问版本页也可以加载版本到编辑器", async ({ page }) => {
  const workflow = await seedWorkflow(uniq("e2e-版本直达"), "return 1;");
  await page.goto(`/workflows/${workflow.id}/versions`);
  await page.getByRole("button", { name: "加载到编辑器" }).click();
  await expect(page).toHaveURL(`/workflows/${workflow.id}`);
  await expect(page.locator(".vue-flow__node")).toHaveCount(3);
  await expect(page.locator(".toast-error")).toHaveCount(0);
});

test("浏览器返回离开编辑器时关闭待输入弹窗，不启动运行", async ({ page }) => {
  const name = uniq("e2e-取消待输入");
  const workflow = await rpc<{ workflow_id: string }>("workflow.import", {
    name,
    definition: {
      ...simpleDefinition,
      input_schema: { type: "object", required: ["id"], properties: { id: { type: "string" } } },
    },
  });
  await page.goto("/workflows");
  await page
    .locator("tbody tr", { hasText: name })
    .getByRole("link", { name: "打开", exact: true })
    .click();
  await expect(page).toHaveURL(`/workflows/${workflow.workflow_id}`);
  await page.getByRole("button", { name: "运行", exact: true }).click();
  await expect(page.locator(".modal-message")).toHaveText("填写工作流运行输入");
  await page.goBack();
  await expect(page).toHaveURL("/workflows");
  await expect(page.locator(".modal")).toHaveCount(0);
  const runs = await rpc<{ runs: unknown[] }>("run.list.view", {
    workflow_id: workflow.workflow_id,
  });
  expect(runs.runs).toHaveLength(0);
});
