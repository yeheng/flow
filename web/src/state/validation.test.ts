import { describe, expect, it } from "vitest";
import { validateDefinition } from "./validation";
import type { EditorEdge, EditorNode } from "./editor";
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
    type: "end",
    label: "结束",
    category: "control",
    ports: [{ id: "in", label: "入" }],
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
    params_schema: {
      type: "object",
      required: ["code"],
      properties: { code: { type: "string", "x-widget": "code" } },
    },
  },
  {
    type: "condition",
    label: "条件",
    category: "control",
    ports: [
      { id: "in", label: "入" },
      { id: "true", label: "真" },
      { id: "false", label: "假" },
    ],
    params_schema: { type: "object" },
  },
];

function node(id: string, type: string, params: Record<string, unknown> = {}): EditorNode {
  return {
    id,
    type: "flow",
    position: { x: 0, y: 0 },
    data: { name: id, nodeType: types.find((t) => t.type === type)!, params },
  };
}

function edge(source: string, target: string, sourceHandle = "out"): EditorEdge {
  return { id: `e_${source}_${sourceHandle}_${target}`, source, target, sourceHandle };
}

// start → script → end 的合法最小图
function validGraph(): { nodes: EditorNode[]; edges: EditorEdge[] } {
  return {
    nodes: [
      node("start_1", "start"),
      node("script_1", "script", { code: "return 1" }),
      node("end_1", "end"),
    ],
    edges: [edge("start_1", "script_1"), edge("script_1", "end_1")],
  };
}

describe("validateDefinition", () => {
  it("合法图无错误", () => {
    const { nodes, edges } = validGraph();
    expect(validateDefinition(nodes, edges, types)).toEqual([]);
  });

  it("空图报错", () => {
    const errs = validateDefinition([], [], types);
    expect(errs).toHaveLength(1);
    expect(errs[0].message).toContain("没有任何节点");
  });

  it("恰好一个 start：0 个与 2 个都报错", () => {
    const { nodes, edges } = validGraph();
    const noStart = validateDefinition(
      nodes.filter((n) => n.id !== "start_1"),
      edges.filter((e) => e.source !== "start_1"),
      types,
    );
    expect(noStart.some((e) => e.message.includes("start"))).toBe(true);

    const twoStart = validateDefinition([...nodes, node("start_2", "start")], edges, types);
    expect(twoStart.some((e) => e.message.includes("必须且只能有一个 start"))).toBe(true);
    // max_instances=1 的 start 超限也应报
    expect(twoStart.some((e) => e.message.includes("最多 1 个"))).toBe(true);
  });

  it("没有 end 报错", () => {
    const { nodes, edges } = validGraph();
    const errs = validateDefinition(
      nodes.filter((n) => n.id !== "end_1"),
      edges.filter((e) => e.target !== "end_1"),
      types,
    );
    expect(errs.some((e) => e.message.includes("至少需要一个 end"))).toBe(true);
  });

  it("有环报错", () => {
    const { nodes, edges } = validGraph();
    nodes.push(node("script_2", "script", { code: "1" }));
    edges.push(edge("script_1", "script_2"), edge("script_2", "script_1"));
    const errs = validateDefinition(nodes, edges, types);
    expect(errs.some((e) => e.message.includes("必须是 DAG"))).toBe(true);
  });

  it("从 start 不可达报错", () => {
    const { nodes, edges } = validGraph();
    nodes.push(node("script_9", "script", { code: "1" }), node("end_9", "end"));
    edges.push(edge("script_9", "end_9"));
    const errs = validateDefinition(nodes, edges, types);
    expect(errs.some((e) => e.message.includes("不可达"))).toBe(true);
  });

  it("condition 出边必须带 true/false 端口", () => {
    const { nodes, edges } = validGraph();
    nodes.push(node("condition_1", "condition"), node("end_2", "end"));
    edges.push(
      edge("script_1", "condition_1"),
      edge("condition_1", "end_1", "true"),
      edge("condition_1", "end_2", "out"), // 非法端口
    );
    const errs = validateDefinition(nodes, edges, types);
    expect(errs.some((e) => e.message.includes("true/false"))).toBe(true);
  });

  it("非 start/end 节点无入边报错；start 有入边、end 有出边报错", () => {
    const orphan = validateDefinition(
      [node("start_1", "start"), node("script_1", "script", { code: "1" }), node("end_1", "end")],
      [edge("start_1", "end_1")],
      types,
    );
    expect(orphan.some((e) => e.message.includes("没有入边"))).toBe(true);

    const startIn = validateDefinition(
      validGraph().nodes,
      [...validGraph().edges, edge("script_1", "start_1")],
      types,
    );
    expect(startIn.some((e) => e.message.includes("start 节点不能有入边"))).toBe(true);

    const endOut = validateDefinition(
      validGraph().nodes,
      [...validGraph().edges, edge("end_1", "script_1")],
      types,
    );
    expect(endOut.some((e) => e.message.includes("end 节点不能有出边"))).toBe(true);
  });

  it("required 参数缺失/空串报错，填了不报", () => {
    const { nodes, edges } = validGraph();
    nodes.find((n) => n.id === "script_1")!.data.params = {};
    const errs = validateDefinition(nodes, edges, types);
    expect(errs.some((e) => e.nodeId === "script_1" && e.message.includes("必填参数"))).toBe(true);

    nodes.find((n) => n.id === "script_1")!.data.params = { code: "  " };
    // "  " 非空串：前端 required 只挡空值，内容合法性留给服务端
    expect(
      validateDefinition(nodes, edges, types).filter((e) => e.message.includes("必填参数")),
    ).toEqual([]);
  });

  it("自环与重复边报错", () => {
    const { nodes, edges } = validGraph();
    edges.push(edge("script_1", "script_1"));
    expect(validateDefinition(nodes, edges, types).some((e) => e.message.includes("自环"))).toBe(
      true,
    );

    const g2 = validGraph();
    g2.edges.push(edge("start_1", "script_1"));
    expect(
      validateDefinition(g2.nodes, g2.edges, types).some((e) => e.message.includes("重复的边")),
    ).toBe(true);
  });
});
