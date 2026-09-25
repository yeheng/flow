import { beforeEach, describe, expect, it } from "vitest";
import { copySelection, editor, pasteClipboard, type EditorEdge, type EditorNode } from "./editor";
import { resetHistory, undo } from "./history";
import type { NodeTypeDesc } from "../types";

const types: NodeTypeDesc[] = [
  {
    type: "start",
    label: "开始",
    category: "control",
    max_instances: 1,
    ports: [{ id: "out", label: "出" }],
    params_schema: { type: "object" },
  },
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
];

function node(id: string, type: string, x: number, y: number, selected = false): EditorNode {
  return {
    id,
    type: "flow",
    position: { x, y },
    data: { name: id, nodeType: types.find((t) => t.type === type)!, params: { code: "x" } },
    selected,
  };
}

function edge(source: string, target: string): EditorEdge {
  return { id: `e_${source}_out_${target}`, source, target, sourceHandle: "out" };
}

beforeEach(() => {
  editor.nodeTypes = types;
  editor.nodes = [];
  editor.edges = [];
  editor.selectedNodeId = null;
  resetHistory();
});

describe("复制/粘贴", () => {
  it("无选中时 copy 返回 false（不拦截浏览器默认行为）", () => {
    editor.nodes = [node("script_1", "script", 0, 0)];
    expect(copySelection()).toBe(false);
  });

  it("粘贴重新生成 id、偏移 +32、内部边重连、外部边不复制", () => {
    editor.nodes = [
      node("script_1", "script", 100, 100, true),
      node("script_2", "script", 300, 100, true),
      node("script_3", "script", 500, 100), // 未选中
    ];
    editor.edges = [
      edge("script_1", "script_2"), // 内部边
      edge("script_2", "script_3"), // 外部边
      edge("script_3", "script_1"), // 外部边
    ];
    expect(copySelection()).toBe(true);
    expect(pasteClipboard()).toBe(true);

    const ids = editor.nodes.map((n) => n.id);
    expect(ids).toEqual(["script_1", "script_2", "script_3", "script_4", "script_5"]);
    const pasted = editor.nodes.slice(3);
    expect(pasted.map((n) => n.position)).toEqual([
      { x: 132, y: 132 },
      { x: 332, y: 132 },
    ]);
    // params 深拷贝：改新节点不影响原节点
    pasted[0].data.params.code = "y";
    expect(editor.nodes[0].data.params.code).toBe("x");
    // 只有内部边被复制并重连到新 id
    expect(editor.edges).toHaveLength(4);
    const copied = editor.edges[3];
    expect(copied.source).toBe("script_4");
    expect(copied.target).toBe("script_5");
  });

  it("重复粘贴继续偏移（+64、+96…），新复制重置偏移", () => {
    editor.nodes = [node("script_1", "script", 100, 100, true)];
    copySelection();
    pasteClipboard();
    pasteClipboard();
    expect(editor.nodes[2].position).toEqual({ x: 164, y: 164 });

    // 新复制重置偏移序号
    editor.nodes[0].selected = true;
    copySelection();
    pasteClipboard();
    expect(editor.nodes[3].position).toEqual({ x: 132, y: 132 });
  });

  it("max_instances 上限：第二个 start 不粘贴", () => {
    editor.nodes = [node("start_1", "start", 0, 0, true)];
    copySelection();
    expect(pasteClipboard()).toBe(false);
    expect(editor.nodes).toHaveLength(1);
  });

  it("粘贴入 undo 栈", () => {
    editor.nodes = [node("script_1", "script", 0, 0, true)];
    copySelection();
    pasteClipboard();
    expect(editor.nodes).toHaveLength(2);
    undo();
    expect(editor.nodes).toHaveLength(1);
  });
});
