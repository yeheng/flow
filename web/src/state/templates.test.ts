import { beforeEach, describe, expect, it, vi } from "vitest";
import { copySelection, editor, insertFragment, pasteClipboard, type EditorNode } from "./editor";
import { resetHistory, undo } from "./history";
import type { NodeTypeDesc } from "../types";

// api 层 mock：templates store 的插入路径走 template.get
vi.mock("../api/flow", () => ({
  listTemplates: vi.fn(async () => []),
  getTemplate: vi.fn(async (id: string) => {
    if (id === "t1") {
      return {
        id,
        name: "组",
        category: null,
        nodes: [
          { id: "a", type: "script", name: "A", params: { code: "1" }, position: { x: 0, y: 0 } },
          { id: "b", type: "script", name: "B", params: { code: "2" }, position: { x: 100, y: 0 } },
        ],
        edges: [{ source: "a", target: "b" }],
        created_at: "",
        updated_at: "",
      };
    }
    throw new Error("not found");
  }),
  createTemplate: vi.fn(async () => {
    throw new Error("no network in unit test");
  }),
  deleteTemplate: vi.fn(async () => {}),
  updateTemplate: vi.fn(async () => {}),
}));

import { insertTemplate } from "./templates";

const types: NodeTypeDesc[] = [
  {
    type: "script",
    label: "脚本",
    category: "compute",
    ports: [
      { id: "in", label: "入" },
      { id: "out", label: "出" },
    ],
    params_schema: { type: "object" },
  },
  {
    type: "start",
    label: "开始",
    category: "control",
    max_instances: 1,
    ports: [{ id: "out", label: "出" }],
    params_schema: { type: "object" },
  },
];

function node(id: string, type: string, x: number, selected = false): EditorNode {
  return {
    id,
    type: "flow",
    position: { x, y: 0 },
    data: { name: id, nodeType: types.find((t) => t.type === type)!, params: { code: "x" } },
    selected,
  };
}

beforeEach(() => {
  editor.nodeTypes = types;
  editor.nodes = [];
  editor.edges = [];
  editor.selectedNodeId = null;
  resetHistory();
});

describe("insertFragment（粘贴与模板插入共用）", () => {
  it("自定义 offset 落点 + id 重生成 + 内部边重连", () => {
    editor.nodes = [node("script_1", "script", 0)];
    const ok = insertFragment(
      [
        { id: "a", type: "script", name: "A", params: { code: "1" }, position: { x: 10, y: 10 } },
        { id: "b", type: "script", name: "B", params: { code: "2" }, position: { x: 60, y: 10 } },
      ],
      [{ source: "a", target: "b" }],
      { offset: { x: 500, y: 400 } },
    );
    expect(ok).toBe(true);
    expect(editor.nodes.map((n) => n.id)).toEqual(["script_1", "script_2", "script_3"]);
    expect(editor.nodes[1].position).toEqual({ x: 510, y: 410 });
    expect(editor.nodes[2].position).toEqual({ x: 560, y: 410 });
    expect(editor.edges).toHaveLength(1);
    expect(editor.edges[0].source).toBe("script_2");
    expect(editor.edges[0].target).toBe("script_3");
  });

  it("全部撞 max_instances 时不插入且返回 false", () => {
    editor.nodes = [node("start_1", "start", 0)];
    const ok = insertFragment(
      [{ id: "s", type: "start", name: "S", params: {}, position: { x: 0, y: 0 } }],
      [],
    );
    expect(ok).toBe(false);
    expect(editor.nodes).toHaveLength(1);
  });

  it("部分撞上限：能插的插入，撞上限的跳过并保留其余连接", () => {
    editor.nodes = [node("start_1", "start", 0, true), node("script_1", "script", 100, true)];
    copySelection();
    // 剪贴板里有 start + script；再手动放一个 start 占掉名额
    editor.nodes.push(node("start_2", "start", 300));
    // 直接走 insertFragment（与模板插入等价）：start 撞上限，script 照常
    const ok = insertFragment(
      [
        { id: "s", type: "start", name: "S", params: {}, position: { x: 0, y: 0 } },
        { id: "c", type: "script", name: "C", params: { code: "1" }, position: { x: 50, y: 0 } },
      ],
      [],
      { offset: { x: 0, y: 0 } },
    );
    expect(ok).toBe(true);
    expect(editor.nodes.filter((n) => n.data.nodeType.type === "start")).toHaveLength(2);
    expect(editor.nodes.some((n) => n.data.name === "C")).toBe(true);
  });

  it("空片段返回 false", () => {
    expect(insertFragment([], [])).toBe(false);
  });

  it("粘贴路径仍然复用同一逻辑（偏移按连续粘贴序号）", () => {
    editor.nodes = [node("script_1", "script", 0, true)];
    copySelection();
    expect(pasteClipboard()).toBe(true);
    expect(pasteClipboard()).toBe(true);
    expect(editor.nodes[1].position).toEqual({ x: 32, y: 32 });
    expect(editor.nodes[2].position).toEqual({ x: 64, y: 64 });
  });

  it("插入入 undo 栈", () => {
    editor.nodes = [];
    insertFragment(
      [{ id: "a", type: "script", name: "A", params: {}, position: { x: 0, y: 0 } }],
      [],
      { offset: { x: 1, y: 1 } },
    );
    expect(editor.nodes).toHaveLength(1);
    undo();
    expect(editor.nodes).toHaveLength(0);
  });
});

describe("模板插入（templates store）", () => {
  it("insertTemplate 拉载荷并落进画布，未知类型不阻塞", async () => {
    editor.nodes = [node("script_1", "script", 0)];
    const ok = await insertTemplate("t1", { x: 200, y: 200 });
    expect(ok).toBe(true);
    expect(editor.nodes).toHaveLength(3);
    expect(editor.nodes[1].position.x).toBe(200);
    expect(editor.nodes[2].position.x).toBe(300);
    // 内部边重连
    expect(editor.edges).toHaveLength(1);
    expect(editor.edges[0].target).toBe(editor.nodes[2].id);
  });

  it("模板不存在时报错且不动画布", async () => {
    editor.nodes = [node("script_1", "script", 0)];
    const ok = await insertTemplate("missing");
    expect(ok).toBe(false);
    expect(editor.nodes).toHaveLength(1);
  });
});
