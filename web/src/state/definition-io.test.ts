import { beforeEach, describe, expect, it } from "vitest";
import { definitionToFlow, editor, flowToDefinition } from "./editor";
import type { Definition, NodeTypeDesc } from "../types";

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
    type: "end",
    label: "结束",
    category: "control",
    ports: [{ id: "in", label: "入" }],
    params_schema: { type: "object" },
  },
];

beforeEach(() => {
  editor.nodeTypes = types;
  editor.nodes = [];
  editor.edges = [];
  editor.selectedNodeId = null;
});

describe("definitionToFlow 未知节点类型", () => {
  const defWithUnknown: Definition = {
    nodes: [
      { id: "start_1", type: "start", position: { x: 0, y: 0 }, params: {} },
      {
        id: "script_v2_1",
        type: "script_v2",
        name: "新脚本",
        position: { x: 220, y: 0 },
        params: { code: "echo 1", future_field: { nested: true } },
      },
      { id: "end_1", type: "end", position: { x: 440, y: 0 }, params: {} },
    ],
    edges: [
      { from: "start_1", to: "script_v2_1" },
      { from: "script_v2_1", to: "end_1" },
    ],
  };

  it("未知类型不白屏：降级为占位描述（入/出端口、空 schema）", () => {
    const flow = definitionToFlow(defWithUnknown);
    const unknown = flow.nodes.find((n) => n.id === "script_v2_1")!;
    expect(unknown.data.nodeType).toBeDefined();
    expect(unknown.data.nodeType.type).toBe("script_v2");
    expect(unknown.data.nodeType.ports.some((p) => p.id === "in")).toBe(true);
    expect(unknown.data.nodeType.ports.some((p) => p.id !== "in")).toBe(true);
    expect(unknown.data.nodeType.params_schema.properties ?? {}).toEqual({});
  });

  it("编辑保存往返不丢失未知节点的 params 原文", () => {
    const flow = definitionToFlow(defWithUnknown);
    editor.nodes = flow.nodes;
    editor.edges = flow.edges;
    const out = flowToDefinition();
    const unknown = out.nodes.find((n) => n.id === "script_v2_1")!;
    expect(unknown.type).toBe("script_v2");
    expect(unknown.name).toBe("新脚本");
    expect(unknown.params).toEqual({ code: "echo 1", future_field: { nested: true } });
  });
});
