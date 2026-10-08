import { expect, test, type Page } from "@playwright/test";
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

// ---- 右键交互：原地松开弹菜单，按住拖动节点上连线 / 空白处平移 ----

/** 在画布里找一块空白点（避开节点/边/控件面板/面包屑），右键平移与空白菜单用 */
async function blankSpot(page: Page): Promise<{ x: number; y: number }> {
  return page.evaluate(() => {
    const wrap = document.querySelector(".canvas-wrap");
    if (!wrap) throw new Error("canvas-wrap 不存在");
    const r = wrap.getBoundingClientRect();
    for (let fy = 0.15; fy <= 0.85; fy += 0.1) {
      for (let fx = 0.35; fx <= 0.92; fx += 0.05) {
        const x = r.left + r.width * fx;
        const y = r.top + r.height * fy;
        const el = document.elementFromPoint(x, y);
        if (
          el &&
          !el.closest(".vue-flow__node") &&
          !el.closest(".vue-flow__edge") &&
          !el.closest(".vue-flow__panel") &&
          !el.closest(".breadcrumb")
        ) {
          return { x, y };
        }
      }
    }
    throw new Error("画布上找不到空白点");
  });
}

test("右键节点：菜单编辑参数与删除", async ({ page }) => {
  const name = uniq("e2e-右键菜单");
  const wf = await seedWorkflow(name, "return 1;");
  await page.goto(`/workflows/${wf.id}`);
  await expect(page.locator(".vue-flow__node")).toHaveCount(3);

  const box = (await page.locator('.vue-flow__node[data-id="n"]').boundingBox())!;
  // 原地按下即松开右键 → 弹自定义菜单（浏览器原生菜单被抑制）
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await page.mouse.down({ button: "right" });
  await page.mouse.up({ button: "right" });
  const menu = page.locator(".ctx-menu");
  await expect(menu).toBeVisible();

  // 编辑参数：直达右侧面板并聚焦名称输入框
  await menu.locator(".ctx-item", { hasText: "编辑参数" }).click();
  await expect(page.locator("#node-name")).toBeFocused();

  // 再次右键同一节点 → 删除 → 3 节点变 2，菜单关闭
  await page.mouse.down({ button: "right" });
  await page.mouse.up({ button: "right" });
  await page.locator(".ctx-item", { hasText: "删除" }).click();
  await expect(page.locator(".vue-flow__node")).toHaveCount(2);
  await expect(page.locator(".ctx-menu")).toHaveCount(0);
});

test("右键空白：添加节点子菜单与粘贴", async ({ page }) => {
  const name = uniq("e2e-空白菜单");
  const wf = await seedWorkflow(name, "return 1;");
  await page.goto(`/workflows/${wf.id}`);
  await expect(page.locator(".vue-flow__node")).toHaveCount(3);

  const spot = await blankSpot(page);
  await page.mouse.move(spot.x, spot.y);
  await page.mouse.down({ button: "right" });
  await page.mouse.up({ button: "right" });
  const menu = page.locator(".ctx-menu");
  await expect(menu).toBeVisible();

  // hover「添加节点」展开分组子菜单 → 点「脚本」
  await menu.locator(".ctx-item", { hasText: "添加节点" }).hover();
  const sub = menu.locator(".ctx-sub");
  await expect(sub).toBeVisible();
  await sub.locator(".ctx-item", { hasText: "脚本" }).click();
  await expect(page.locator(".vue-flow__node")).toHaveCount(4);
  await expect(page.locator(".ctx-menu")).toHaveCount(0);

  // 复制新节点 → 空白处右键粘贴（落点为菜单弹出位置）
  await page.locator(".vue-flow__node").nth(3).click();
  await page.keyboard.press("ControlOrMeta+c");
  const spot2 = await blankSpot(page);
  await page.mouse.move(spot2.x, spot2.y);
  await page.mouse.down({ button: "right" });
  await page.mouse.up({ button: "right" });
  await page.locator(".ctx-item", { hasText: "粘贴" }).click();
  await expect(page.locator(".vue-flow__node")).toHaveCount(5);
});

test("右键拖拽：节点上拖出连线，空白处拖动平移画布", async ({ page }) => {
  const name = uniq("e2e-右键拖拽");
  const wf = await seedWorkflow(name, "return 1;");
  await page.goto(`/workflows/${wf.id}`);
  await expect(page.locator(".vue-flow__node")).toHaveCount(3);
  await expect(page.locator(".vue-flow__edge")).toHaveCount(2);

  // 用空白菜单加一个孤立节点作连线目标（服务端校验禁止无入边节点，不能走 RPC 种子）
  const spot0 = await blankSpot(page);
  await page.mouse.move(spot0.x, spot0.y);
  await page.mouse.down({ button: "right" });
  await page.mouse.up({ button: "right" });
  const menu = page.locator(".ctx-menu");
  await expect(menu).toBeVisible();
  await menu.locator(".ctx-item", { hasText: "添加节点" }).hover();
  await menu.locator(".ctx-sub .ctx-item", { hasText: "脚本" }).click();
  await expect(page.locator(".vue-flow__node")).toHaveCount(4);
  const iso = page.locator('.vue-flow__node[data-id="script_1"]');

  // 从 start 按住右键拖到孤立节点 → 拖拽连线模式，落点成边
  const src = (await page.locator('.vue-flow__node[data-id="s"]').boundingBox())!;
  const dst = (await iso.boundingBox())!;
  await page.mouse.move(src.x + src.width / 2, src.y + src.height / 2);
  await page.mouse.down({ button: "right" });
  await page.mouse.move(dst.x + dst.width / 2, dst.y + dst.height / 2, { steps: 15 });
  await page.mouse.up({ button: "right" });
  await expect(page.locator(".vue-flow__edge")).toHaveCount(3);

  // 空白处按住右键拖动 → 平移视口（transform 变化），原地语义不弹菜单
  const vp = page.locator(".vue-flow__transformationpane");
  const before = await vp.evaluate((el) => getComputedStyle(el).transform);
  const spot = await blankSpot(page);
  await page.mouse.move(spot.x, spot.y);
  await page.mouse.down({ button: "right" });
  await page.mouse.move(spot.x + 90, spot.y + 60, { steps: 10 });
  await page.mouse.up({ button: "right" });
  const after = await vp.evaluate((el) => getComputedStyle(el).transform);
  expect(after).not.toBe(before);
  await expect(page.locator(".ctx-menu")).toHaveCount(0);
});
