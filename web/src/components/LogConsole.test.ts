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

import LogConsole from "../components/LogConsole.vue";
import { monitor } from "../state/monitor";
import { mount } from "@vue/test-utils";
import type { RunEvent } from "../types";

function logLine(seq: number, nodeId = "n1", message = `line ${seq}`): RunEvent {
  return {
    seq,
    ts: "2026-01-01T00:00:00Z",
    run_id: "r-probe",
    type: "node_log",
    node_id: nodeId,
    attempt: 1,
    level: "info",
    stream: "stdout",
    message,
  };
}

/**
 * 刷屏时节流的回归：推送 N 行不该触发 N 次重渲染。
 *
 * 曾经的写法里 `filtered` computed 直接依赖 `monitor.logs`，而模板又读
 * `monitor.logs.length`——Vue 3.4+ 的 computed 失效传播会在每次 push 时把
 * 渲染标脏，每行一次全量过滤 + DOM diff（200 行 = 200 次渲染，实测）。
 * 修复：模板只读帧快照 ref（LogConsole 的 `frame`）。
 */
describe("LogConsole：刷屏时节流", () => {
  beforeEach(() => {
    // monitor 是模块级单例：用例之间必须清干净，否则行数互相污染
    monitor.runId = "r-probe";
    monitor.nodes = [];
    monitor.logs = [];
    monitor.logsByNode = {};
    monitor.lastLogSeq = 0;
  });

  it("逐 tick 推送多行只渲染一次（帧合并）", async () => {
    let renders = 0;
    const counting = {
      beforeUpdate() {
        renders += 1;
      },
    };
    const wrapper = mount(LogConsole, {
      props: { "onSelect-node": () => {} },
      global: { mixins: [counting] },
    });

    const N = 60;
    for (let i = 1; i <= N; i++) {
      monitor.logs.push(logLine(i) as never);
      await nextTick();
    }
    // jsdom 的 rAF 在宏任务里：这一轮 push 全部落进同一帧
    expect(renders).toBeLessThan(N / 2);
    wrapper.unmount();
  });

  it("首帧就展示已有日志（切 tab 不闪空态）", async () => {
    monitor.logs.push(logLine(1) as never, logLine(2) as never);
    const wrapper = mount(LogConsole, { props: { "on-select-node": () => {} } });
    expect(wrapper.find(".log-empty").exists()).toBe(false);
    expect(wrapper.findAll(".log-row")).toHaveLength(2);
    wrapper.unmount();
  });

  it("级别过滤生效：默认不渲染 debug 行", async () => {
    monitor.logs.push(
      { ...logLine(1), level: "info" } as never,
      { ...logLine(2), level: "debug" } as never,
    );
    const wrapper = mount(LogConsole, { props: { "on-select-node": () => {} } });
    expect(wrapper.findAll(".log-row")).toHaveLength(1);
    wrapper.unmount();
  });
});
