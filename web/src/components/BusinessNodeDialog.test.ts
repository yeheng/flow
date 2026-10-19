import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../api/flow", () => ({
  listTemplates: vi.fn(async () => [
    { id: "t0", name: "已有", category: "财务", node_count: 1, created_at: "", updated_at: "" },
  ]),
  getTemplate: vi.fn(async () => {
    throw new Error("not used");
  }),
  createTemplate: vi.fn(async (name: string) => ({ id: "new", name })),
  deleteTemplate: vi.fn(async () => {}),
  updateTemplate: vi.fn(async () => {}),
}));

import { mount, flushPromises } from "@vue/test-utils";
import BusinessNodeDialog from "./BusinessNodeDialog.vue";
import * as api from "../api/flow";
import { editor } from "../state/editor";
import { closeAllOverlays, overlaysOf, topOverlay } from "../state/overlays";
import { confirmDialog } from "../state/modal";
import { ensureTemplates, openTemplateSave } from "../state/templates";

const wrappers: ReturnType<typeof mount>[] = [];
function create() {
  // attachTo：焦点断言需要真实挂到 document（jsdom 不给游离元素焦点）
  const w = mount(BusinessNodeDialog, {
    attachTo: document.body,
    global: { stubs: { AppButton: false } },
  });
  wrappers.push(w);
  return w;
}

/** keydown 派发到 window：Esc 由全局弹层栈关栈顶 */
function key(k: string): void {
  window.dispatchEvent(new KeyboardEvent("keydown", { key: k, bubbles: true, cancelable: true }));
}

function selectOneNode(): void {
  editor.nodes = [
    {
      id: "n1",
      type: "flow",
      position: { x: 0, y: 0 },
      selected: true,
      data: {
        name: "校验",
        nodeType: {
          type: "script",
          label: "脚本",
          category: "compute",
          ports: [],
          params_schema: { type: "object" },
        },
        params: { code: "return 1;" },
      },
    },
  ];
}

beforeEach(() => {
  vi.clearAllMocks();
  closeAllOverlays();
  editor.nodes = [];
});

afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount());
  closeAllOverlays();
});

describe("BusinessNodeDialog：存为业务节点", () => {
  it("打开预填名称并聚焦；分类 datalist 含已有分类与默认项", async () => {
    await ensureTemplates(true);
    openTemplateSave("预填名");
    const w = create();
    await flushPromises();

    const nameInput = w.get("#biz-name").element as HTMLInputElement;
    expect(nameInput.value).toBe("预填名");
    expect(document.activeElement).toBe(nameInput);

    const options = w.findAll("#biz-category-options option").map((o) => o.attributes("value"));
    expect(options).toContain("业务");
    expect(options).toContain("子流程");
    expect(options).toContain("财务");
    expect((w.get("#biz-category").element as HTMLInputElement).value).toBe("业务");
  });

  it("保存语义：提交落库（名称/分类/选中片段）后条目关闭", async () => {
    selectOneNode();
    openTemplateSave();
    const w = create();
    await flushPromises();
    expect(w.text()).toContain("将保存 1 个选中节点");

    const saveBtn = w.get('button[type="submit"]');
    expect(saveBtn.attributes("disabled")).toBeDefined();

    await w.get("#biz-name").setValue("发票校验");
    await w.get("#biz-category").setValue("财务");
    await w.get("form").trigger("submit");
    await flushPromises();

    expect(vi.mocked(api.createTemplate)).toHaveBeenCalledWith(
      "发票校验",
      [
        expect.objectContaining({
          id: "n1",
          type: "script",
          params: { code: "return 1;" },
        }),
      ],
      [],
      "财务",
    );
    expect(overlaysOf("biz-save")).toEqual([]);
  });

  it("取消语义：Esc/取消按钮收场且不落库", async () => {
    selectOneNode();
    openTemplateSave("预填名");
    const w = create();
    await flushPromises();

    key("Escape");
    await flushPromises();
    expect(overlaysOf("biz-save")).toEqual([]);
    expect(vi.mocked(api.createTemplate)).not.toHaveBeenCalled();

    openTemplateSave("再开");
    await flushPromises();
    await w.get(".biz-footer button").trigger("click"); // 取消按钮
    await flushPromises();
    expect(overlaysOf("biz-save")).toEqual([]);
    expect(vi.mocked(api.createTemplate)).not.toHaveBeenCalled();
  });

  it("空白名称不能提交；保存中禁止取消", async () => {
    selectOneNode();
    openTemplateSave("  ");
    const w = create();
    await flushPromises();
    // 前后空白被 trim 后为空 → 禁用
    expect(w.get('button[type="submit"]').attributes("disabled")).toBeDefined();

    await w.get("#biz-name").setValue("有效名");
    // 保存挂起期间取消入口全部禁用，对话框保持打开
    let release!: (v: unknown) => void;
    vi.mocked(api.createTemplate).mockReturnValueOnce(
      new Promise((r) => (release = r)) as unknown as ReturnType<typeof api.createTemplate>,
    );
    await w.get("form").trigger("submit");
    await w.get(".biz-close").trigger("click");
    await w.get(".biz-overlay").trigger("click");
    expect(overlaysOf("biz-save")).toHaveLength(1);

    release({ id: "new", name: "有效名" });
    await flushPromises();
    expect(overlaysOf("biz-save")).toEqual([]);
  });

  it("Esc 叠层只关一层：confirm 压在对话框上时先关 confirm", async () => {
    selectOneNode();
    openTemplateSave("预填名");
    const w = create();
    await flushPromises();
    const p = confirmDialog("继续？");
    await flushPromises();
    expect(topOverlay.value?.kind).toBe("modal");

    key("Escape");
    await expect(p).resolves.toBe(false);
    // 只关栈顶：对话框仍在，且仍是栈上唯一弹层
    expect(w.text()).toContain("存为业务节点");
    expect(overlaysOf("biz-save")).toHaveLength(1);
    expect(topOverlay.value?.kind).toBe("biz-save");

    key("Escape");
    await flushPromises();
    expect(overlaysOf("biz-save")).toEqual([]);
  });
});
