import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mount } from "@vue/test-utils";

vi.mock("../state/editor", async () => {
  const { reactive: r, computed } = await import("vue");
  const editor = r({
    workflowId: "w1",
    nodes: [] as unknown[],
    nodeTypes: [
      {
        type: "start",
        label: "开始",
        category: "control",
        max_instances: 1,
        ports: [],
        params_schema: { type: "object" },
      },
      {
        type: "script",
        label: "脚本",
        category: "compute",
        ports: [],
        params_schema: { type: "object" },
      },
      {
        type: "http_call",
        label: "HTTP 请求",
        category: "integration",
        ports: [],
        params_schema: { type: "object" },
      },
    ],
  });
  return {
    editor,
    addNode: vi.fn(),
    nodeTypeGroups: computed(() => {
      const order: string[] = [];
      const byCat = new Map<string, unknown[]>();
      for (const nt of editor.nodeTypes as { category: string }[]) {
        if (!byCat.has(nt.category)) {
          byCat.set(nt.category, []);
          order.push(nt.category);
        }
        byCat.get(nt.category)!.push(nt);
      }
      return order.map((cat) => ({ cat, label: cat, items: byCat.get(cat)! }));
    }),
  };
});
vi.mock("../state/templates", () => {
  const templates = { list: [] as unknown[], loaded: true, loading: false };
  return {
    templates,
    ensureTemplates: vi.fn(async () => true),
    insertTemplate: vi.fn(async () => true),
    openTemplateSave: vi.fn(),
    removeTemplate: vi.fn(async () => true),
    renameTemplate: vi.fn(async () => true),
  };
});
vi.mock("../state/modal", () => ({
  confirmDialog: vi.fn(async () => true),
  promptDialog: vi.fn(async () => "新名字"),
}));

import NodePalette from "./NodePalette.vue";
import { addNode, editor } from "../state/editor";
import {
  insertTemplate,
  openTemplateSave,
  removeTemplate,
  renameTemplate,
  templates,
} from "../state/templates";
import { confirmDialog } from "../state/modal";

function tpl(id: string, name: string, category: string | null) {
  return {
    id,
    name,
    category,
    node_count: 2,
    created_at: "",
    updated_at: "",
  };
}

const wrappers: ReturnType<typeof mount>[] = [];
function create() {
  const w = mount(NodePalette);
  wrappers.push(w);
  return w;
}

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  editor.workflowId = "w1";
  editor.nodes = [];
  templates.list = [];
});

afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount());
});

describe("NodePalette：drawio 式分组与搜索", () => {
  it("按功能分组渲染组头（计数 + aria-expanded）", () => {
    const w = create();
    const heads = w.findAll(".group-head");
    expect(heads.length).toBe(3); // control / compute / integration
    expect(heads[0].text()).toContain("control");
    expect(heads[0].text()).toContain("1");
    expect(heads[0].attributes("aria-expanded")).toBe("true");
    expect(w.findAll(".palette-item[data-node-type]").length).toBe(3);
  });

  it("组头点击折叠并持久化到 localStorage，搜索时强制展开", async () => {
    const w = create();
    const head = w.findAll(".group-head")[1]; // compute（脚本）
    await head.trigger("click");
    expect(head.attributes("aria-expanded")).toBe("false");
    expect(JSON.parse(localStorage.getItem("flow.palette-groups") ?? "{}")).toMatchObject({
      compute: true,
    });
    // 折叠后该组条目不渲染
    expect(w.find('[data-node-type="script"]').exists()).toBe(false);

    // 搜索强制展开（折叠态保留）
    await w.find(".palette-search-input").setValue("脚本");
    expect(w.find('[data-node-type="script"]').exists()).toBe(true);
    await w.find(".palette-search-input").setValue("");
    expect(w.find('[data-node-type="script"]').exists()).toBe(false);
  });

  it("折叠偏好持久化损坏值回退默认（全部展开），不炸 setup 且写回合法 JSON", async () => {
    localStorage.setItem("flow.palette-groups", "{这不是 JSON");
    const w = create(); // 损坏值不得让组件 setup 抛错
    const heads = w.findAll(".group-head");
    expect(heads.length).toBe(3);
    expect(heads[1].attributes("aria-expanded")).toBe("true"); // 回退 = 全部展开

    await heads[1].trigger("click");
    expect(JSON.parse(localStorage.getItem("flow.palette-groups") ?? "{}")).toMatchObject({
      compute: true,
    });
  });

  it("搜索态点组头照样记偏好：搜索结束回到该偏好，aria-expanded 搜索态仍展开", async () => {
    const w = create();
    await w.find(".palette-search-input").setValue("脚本");
    const head = w.findAll(".group-head")[0]; // 搜索只剩 compute 组
    expect(head.text()).toContain("compute");
    expect(head.attributes("aria-expanded")).toBe("true"); // 搜索态仍显示展开

    await head.trigger("click");
    // 偏好已记（compute 折叠），但搜索态依旧展开可见
    expect(JSON.parse(localStorage.getItem("flow.palette-groups") ?? "{}")).toMatchObject({
      compute: true,
    });
    expect(head.attributes("aria-expanded")).toBe("true");
    expect(w.find('[data-node-type="script"]').exists()).toBe(true);

    await w.find(".palette-search-input").setValue("");
    expect(w.find('[data-node-type="script"]').exists()).toBe(false); // 搜索结束回到偏好
  });

  it("搜索按 label/type 过滤节点与业务节点，无结果显示空态", async () => {
    templates.list = [tpl("t1", "发票校验", "业务"), tpl("t2", "订单同步", "子流程")];
    const w = create();
    await w.find(".palette-search-input").setValue("发票");
    expect(w.findAll(".palette-template").length).toBe(1);
    expect(w.find(".palette-template").text()).toContain("发票校验");
    expect(w.findAll(".palette-item[data-node-type]").length).toBe(0);

    await w.find(".palette-search-input").setValue("script");
    expect(w.findAll(".palette-template").length).toBe(0);
    expect(w.find('[data-node-type="script"]').exists()).toBe(true);

    await w.find(".palette-search-input").setValue("不存在的词");
    expect(w.text()).toContain("无匹配节点");
    expect(w.findAll(".palette-item").length).toBe(0);
  });
});

describe("NodePalette：插入与业务节点动作", () => {
  it("点击节点类型插入（无工作流时禁用）", async () => {
    const w = create();
    await w.find('[data-node-type="script"]').trigger("click");
    expect(addNode).toHaveBeenCalledWith("script", expect.anything());

    editor.workflowId = null;
    await w.vm.$nextTick();
    expect(w.find('[data-node-type="script"]').attributes("aria-disabled")).toBe("true");
  });

  it("业务节点按分类分组；点击插入、重命名、删除（确认后）", async () => {
    templates.list = [
      tpl("t1", "发票校验", "财务"),
      tpl("t2", "订单同步", "财务"),
      tpl("t3", "子图", null),
    ];
    const w = create();
    // 两个分类（财务/业务）各一个小节标题
    expect(w.findAll(".palette-cat").map((e) => e.text())).toEqual(["财务", "业务"]);
    expect(w.findAll(".palette-template").length).toBe(3);

    await w.find(".palette-template").trigger("click");
    expect(insertTemplate).toHaveBeenCalledWith("t1", expect.anything());

    await w.findAll(".palette-template .tpl-btn")[0].trigger("click");
    expect(renameTemplate).toHaveBeenCalledWith("t1", "新名字");

    await w.findAll(".palette-template .tpl-btn.danger")[0].trigger("click");
    expect(confirmDialog).toHaveBeenCalled();
    expect(removeTemplate).toHaveBeenCalledWith("t1");
  });

  it("业务节点项键盘可达：Enter 插入；重命名/删除按钮的按键不冒泡成插入", async () => {
    templates.list = [tpl("t1", "发票校验", "财务")];
    const w = create();
    const item = w.find(".palette-template");
    expect(item.attributes("tabindex")).toBe("0");

    await item.trigger("keydown", { key: "enter" });
    expect(insertTemplate).toHaveBeenCalledWith("t1", expect.anything());

    // 按键落在重命名/删除按钮上时不触发插入（与 @click.stop 一致）
    vi.mocked(insertTemplate).mockClear();
    await w.find(".palette-template .tpl-btn").trigger("keydown", { key: "enter" });
    await w.find(".palette-template .tpl-btn.danger").trigger("keydown", { key: " " });
    expect(insertTemplate).not.toHaveBeenCalled();
  });

  it("「存为业务节点」按选中数启用并打开保存对话框", async () => {
    const w = create();
    const save = w.find(".palette-save");
    expect(save.attributes("disabled")).toBeDefined();
    (editor.nodes as unknown[]).push({
      id: "n1",
      selected: true,
      data: { nodeType: { type: "script" } },
      position: { x: 0, y: 0 },
    });
    await w.vm.$nextTick();
    expect(save.attributes("disabled")).toBeUndefined();
    await save.trigger("click");
    expect(openTemplateSave).toHaveBeenCalled();
  });
});
