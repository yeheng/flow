import { beforeEach, describe, expect, it, vi } from "vitest";
import { nextTick } from "vue";

vi.mock("../api/flow", () => ({
  subscribeRun: vi.fn(async () => async () => {}),
  runTimeline: vi.fn(async () => {
    throw new Error("probe: 不拉 timeline");
  }),
  startRun: vi.fn(),
  runCancel: vi.fn(),
  runSignal: vi.fn(),
  adjudicateRun: vi.fn(async () => {}),
  runObservations: vi.fn(async () => ({ records: [], loss: {} })),
}));
vi.mock("../rpc/client", () => ({
  client: { onReconnect: vi.fn() },
  errText: (e: unknown) => String(e),
}));
vi.mock("../state/editor", () => ({
  editor: {
    workflowId: "w1",
    selectedNodeId: null,
    highlightNodeId: null,
    nodes: [],
    nodeTypes: [],
  },
}));

import { mount } from "@vue/test-utils";
import RunMonitor from "./RunMonitor.vue";
import * as api from "../api/flow";
import { monitor } from "../state/monitor";
import type { TimelineNode } from "../types";

function node(over: Partial<TimelineNode> & Pick<TimelineNode, "id">): TimelineNode {
  return {
    name: over.id,
    type: "script",
    state: "running",
    attempts: 1,
    started_at: null,
    ended_at: null,
    duration_ms: null,
    output: null,
    error: null,
    ...over,
  };
}

function inputValue(w: ReturnType<typeof mount>): string {
  return (w.find(".signal-box input").element as HTMLInputElement).value;
}

function mountMonitor() {
  return mount(RunMonitor, { global: { stubs: { RouterLink: true } } });
}

describe("RunMonitor：跨 run 状态不残留", () => {
  beforeEach(() => {
    monitor.runId = "A";
    monitor.workflowId = "w1";
    monitor.status = "running";
    monitor.output = undefined;
    monitor.nodes = [];
    monitor.logs = [];
    monitor.logsByNode = {};
    monitor.breadcrumb = [];
  });

  it("切换 runId 后清空 human_task 信号输入，不把 A 的 payload 带给 B", async () => {
    monitor.nodes = [node({ id: "n1", type: "human_task", wait: { kind: "signal" } })];
    const w = mountMonitor();
    await w.find(".signal-box input").setValue('{"approved":true}');
    expect(inputValue(w)).toBe('{"approved":true}');

    // B run 里出现同一个 node id：旧实现沿用 A 的输入
    monitor.runId = "B";
    monitor.nodes = [node({ id: "n1", type: "human_task", wait: { kind: "signal" } })];
    await nextTick();
    expect(inputValue(w)).toBe("");
    w.unmount();
  });

  it("切换 runId 后重置 input 节点表单字段值", async () => {
    monitor.nodes = [
      node({ id: "in1", type: "input", wait: { kind: "signal" }, input: { fields: [{ key: "x" }] } }),
    ];
    const w = mountMonitor();
    await w.find(".signal-box input").setValue("a-value");
    expect(inputValue(w)).toBe("a-value");

    monitor.runId = "B";
    monitor.nodes = [
      node({ id: "in1", type: "input", wait: { kind: "signal" }, input: { fields: [{ key: "x" }] } }),
    ];
    await nextTick();
    expect(inputValue(w)).toBe("");
    expect(w.find(".signal-box").text()).toContain("等待输入");
    w.unmount();
  });

  it("切换 runId 后收起输出全文", async () => {
    monitor.output = { big: "x".repeat(400) };
    const w = mountMonitor();
    await w.findAll(".run-output-btn").find((b) => b.text() === "展开全文")!.trigger("click");
    expect(w.find(".run-output-full").exists()).toBe(true);

    monitor.runId = "B";
    await nextTick();
    expect(w.find(".run-output-full").exists()).toBe(false);
    w.unmount();
  });

  it("run 状态徽章沿用 runBadgeClass（awaiting_resume → run-awaiting）", () => {
    monitor.status = "awaiting_resume";
    const w = mountMonitor();
    expect(w.find(".run-phase").classes()).toContain("run-awaiting");
    expect(w.find(".run-phase").text()).toBe("挂起待恢复");
    w.unmount();
  });
});

describe("RunMonitor：uncertain 外部操作人工裁决", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    monitor.runId = "A";
    monitor.workflowId = "w1";
    monitor.status = "awaiting_resume";
    monitor.output = undefined;
    monitor.nodes = [];
    monitor.logs = [];
    monitor.logsByNode = {};
    monitor.breadcrumb = [];
    monitor.adjudicating = false;
  });

  function uncertain(): TimelineNode {
    return node({
      id: "op1",
      name: "扣款",
      type: "http_call",
      state: "running",
      wait: { kind: "uncertain" },
      operation_id: "operation-1",
    });
  }

  it("理由必填：空理由禁用提交，提交后按契约发 run.adjudicate", async () => {
    monitor.nodes = [uncertain()];
    const w = mountMonitor();
    const box = w.find(".signal-uncertain");
    expect(box.text()).toContain("外部操作结果待确认");

    const submit = box.findAll("button").find((b) => b.text() === "提交裁决")!;
    expect(submit.attributes("disabled")).toBeDefined();

    await box.find('input[placeholder="裁决理由（必填）"]').setValue("  已核对对端，确认未扣款  ");
    expect(submit.attributes("disabled")).toBeUndefined();
    await submit.trigger("click");

    expect(vi.mocked(api.adjudicateRun)).toHaveBeenCalledWith(
      "A",
      "op1",
      "operation-1",
      "accept_output",
      "已核对对端，确认未扣款",
      null,
    );
    w.unmount();
  });

  it("accept_output 的产出按 JSON 解析后提交", async () => {
    monitor.nodes = [uncertain()];
    const w = mountMonitor();
    const box = w.find(".signal-uncertain");
    await box.find('input[placeholder="裁决理由（必填）"]').setValue("人工确认");
    await box.find('input[placeholder*="人工认定的产出"]').setValue('{"charged": false}');
    await box.findAll("button").find((b) => b.text() === "提交裁决")!.trigger("click");
    expect(vi.mocked(api.adjudicateRun)).toHaveBeenCalledWith(
      "A",
      "op1",
      "operation-1",
      "accept_output",
      "人工确认",
      { charged: false },
    );
    w.unmount();
  });

  it("retry：隐藏产出输入、显示重复副作用警告，且不要求产出", async () => {
    monitor.nodes = [uncertain()];
    const w = mountMonitor();
    const box = w.find(".signal-uncertain");
    await box.find('select[aria-label="裁决决定"]').setValue("retry");
    expect(box.find('input[placeholder*="人工认定的产出"]').exists()).toBe(false);
    expect(box.text()).toContain("可能重复扣款/发信");

    await box.find('input[placeholder="裁决理由（必填）"]').setValue("对端确认未入账");
    await box.findAll("button").find((b) => b.text() === "提交裁决")!.trigger("click");
    expect(vi.mocked(api.adjudicateRun)).toHaveBeenCalledWith(
      "A",
      "op1",
      "operation-1",
      "retry",
      "对端确认未入账",
      null,
    );
    w.unmount();
  });

  it("切换 runId 后清空裁决草稿", async () => {
    monitor.nodes = [uncertain()];
    const w = mountMonitor();
    await w
      .find(".signal-uncertain")
      .find('input[placeholder="裁决理由（必填）"]')
      .setValue("A run 的裁决理由");

    monitor.runId = "B";
    monitor.nodes = [uncertain()];
    await nextTick();
    const reason = w
      .find(".signal-uncertain")
      .find('input[placeholder="裁决理由（必填）"]')
      .element as HTMLInputElement;
    expect(reason.value).toBe("");
    w.unmount();
  });
});
