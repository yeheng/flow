import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../api/flow", () => ({
  subscribeRun: vi.fn(async () => async () => {}),
  runTimeline: vi.fn(async () => {
    throw new Error("probe: 不拉 timeline");
  }),
  startRun: vi.fn(),
  runCancel: vi.fn(),
  runSignal: vi.fn(),
  runObservations: vi.fn(async () => ({ records: [], loss: {} })),
}));
vi.mock("../rpc/client", () => ({
  client: { onReconnect: vi.fn() },
  errText: (e: unknown) => String(e),
}));
vi.mock("../state/editor", () => {
  const state = {
    workflowId: "w1",
    selectedNodeId: null,
    highlightNodeId: null,
    nodes: [],
    nodeTypes: [] as { type: string }[],
  };
  return {
    editor: state,
    nodeTypeDesc: (type: string) => state.nodeTypes.find((t) => t.type === type),
  };
});

import { mount } from "@vue/test-utils";
import NodeInspector from "./NodeInspector.vue";
import { monitor } from "../state/monitor";
import { editor } from "../state/editor";
import type { NodeTypeDesc, TimelineNode } from "../types";

function node(over: Partial<TimelineNode> & Pick<TimelineNode, "id">): TimelineNode {
  return {
    name: over.id,
    type: "script",
    state: "failed",
    attempts: 1,
    started_at: null,
    ended_at: null,
    duration_ms: null,
    output: null,
    error: null,
    ...over,
  };
}

function desc(type: string, over: Partial<NodeTypeDesc> = {}): NodeTypeDesc {
  return {
    type,
    label: type,
    category: "action",
    ports: [],
    params_schema: { type: "object" },
    ...over,
  };
}

describe("NodeInspector：领域词汇走 labels.ts", () => {
  beforeEach(() => {
    monitor.runId = "A";
    monitor.workflowId = "w1";
    monitor.status = "failed";
    monitor.nodes = [];
    monitor.logs = [];
    monitor.logsByNode = {};
    editor.nodeTypes = [];
  });

  it("节点类型/跳过原因/耗时使用统一词汇与友好格式", () => {
    monitor.nodes = [
      node({ id: "n1", type: "script", reason: "upstream_failed", duration_ms: 1234 }),
    ];
    const w = mount(NodeInspector, { props: { nodeId: "n1" } });
    expect(w.find(".inspector-type").text()).toBe("脚本");
    expect(w.find(".inspector-reason").text()).toContain("上游节点失败，本步未执行");
    expect(w.find(".inspector-meta").text()).toContain("耗时 1.2s");
    w.unmount();
  });

  it("未知节点类型/跳过原因原样显示（词汇表兜底）", () => {
    monitor.nodes = [node({ id: "n2", type: "custom_thing", reason: "weird_reason" })];
    const w = mount(NodeInspector, { props: { nodeId: "n2" } });
    expect(w.find(".inspector-type").text()).toBe("custom_thing");
    expect(w.find(".inspector-reason").text()).toContain("weird_reason");
    w.unmount();
  });
});

describe("NodeInspector：证据粒度（trace_granularity）", () => {
  beforeEach(() => {
    monitor.runId = "A";
    monitor.workflowId = "w1";
    monitor.status = "succeeded";
    monitor.nodes = [];
    monitor.logs = [];
    monitor.logsByNode = {};
    editor.nodeTypes = [];
  });

  it.each([
    ["node", "节点级证据"],
    ["external_call", "外部调用证据"],
    ["process", "进程级证据"],
    ["model_call", "模型调用证据"],
  ])("trace_granularity=%s 显示「%s」", (granularity, label) => {
    editor.nodeTypes = [desc("llm", { trace_granularity: granularity })];
    monitor.nodes = [node({ id: "n1", type: "llm", state: "completed" })];
    const w = mount(NodeInspector, { props: { nodeId: "n1" } });
    expect(w.find(".inspector-trace").text()).toContain(label);
    w.unmount();
  });

  it("llm 节点类型标签为「模型调用」", () => {
    editor.nodeTypes = [desc("llm", { trace_granularity: "model_call" })];
    monitor.nodes = [node({ id: "n2", type: "llm", state: "completed" })];
    const w = mount(NodeInspector, { props: { nodeId: "n2" } });
    expect(w.find(".inspector-type").text()).toBe("模型调用");
    w.unmount();
  });

  it("未知粒度原样兜底；未下发 trace_granularity 不渲染提示", () => {
    editor.nodeTypes = [
      desc("custom_thing", { trace_granularity: "weird_gran" }),
      desc("script", {}),
    ];
    monitor.nodes = [node({ id: "n3", type: "custom_thing" }), node({ id: "n4", type: "script" })];
    const w3 = mount(NodeInspector, { props: { nodeId: "n3" } });
    expect(w3.find(".inspector-trace").text()).toContain("weird_gran");
    w3.unmount();
    const w4 = mount(NodeInspector, { props: { nodeId: "n4" } });
    expect(w4.find(".inspector-trace").exists()).toBe(false);
    w4.unmount();
  });
});
