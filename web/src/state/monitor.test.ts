import { beforeEach, describe, expect, it, vi } from "vitest";
import type { RunEvent, Timeline } from "../types";

// monitor 的 attach 竞态语义：局部构建 + 令牌比对 + 原子换入。
// mock api 层，验证「被取代的 attach 不得写回 monitor、不得泄漏订阅」。
vi.mock("../api/flow", () => ({
  subscribeRun: vi.fn(),
  runTimeline: vi.fn(),
  startRun: vi.fn(),
  runCancel: vi.fn(),
  runSignal: vi.fn(),
}));

import * as api from "../api/flow";
import { attachRun, detachRun, monitor } from "./monitor";

function timeline(runId: string): Timeline {
  return {
    run_id: runId,
    status: "running",
    phase: "running",
    workflow_id: "wf1",
    workflow_version: 1,
    started_at: null,
    ended_at: null,
    output: undefined,
    fatal_error: null,
    last_seq: 0,
    nodes: [],
  };
}

function deferred<T>(): { promise: Promise<T>; resolve: (v: T) => void } {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

const unsubs: Record<string, ReturnType<typeof vi.fn>> = {};

beforeEach(() => {
  vi.clearAllMocks();
  for (const k of Object.keys(unsubs)) delete unsubs[k];
  vi.mocked(api.subscribeRun).mockImplementation(async (runId: string) => {
    unsubs[runId] = vi.fn(async () => {});
    return unsubs[runId] as unknown as () => Promise<void>;
  });
  monitor.runId = null;
  monitor.workflowId = null;
  monitor.nodes = [];
  monitor.breadcrumb = [];
});

describe("attach 原子换入", () => {
  it("慢的旧 attach 后完成也不得覆盖新的 run，且其订阅被退订", async () => {
    const slow = deferred<Timeline>();
    vi.mocked(api.runTimeline).mockImplementation(async (runId: string) => {
      if (runId === "run1") return slow.promise;
      return timeline(runId);
    });

    const a = attachRun("run1"); // 卡在 timeline 上
    const b = await attachRun("run2"); // 后发起，先完成
    expect(b).toBe(true);
    expect(monitor.runId).toBe("run2");

    slow.resolve(timeline("run1"));
    expect(await a).toBe(false);
    expect(monitor.runId).toBe("run2"); // 未被旧 attach 覆盖
    expect(unsubs["run1"]).toHaveBeenCalled(); // 旧订阅已退订，无泄漏
  });

  it("detach 使进行中的 attach 自动放弃", async () => {
    const slow = deferred<Timeline>();
    vi.mocked(api.runTimeline).mockReturnValue(slow.promise);

    const a = attachRun("run1");
    await detachRun();
    slow.resolve(timeline("run1"));
    expect(await a).toBe(false);
    expect(monitor.runId).toBeNull();
    expect(unsubs["run1"]).toHaveBeenCalled();
  });

  it("订阅建立与 timeline 对齐之间到达的事件按 seq 补放", async () => {
    let handler: ((e: RunEvent) => void) | null = null;
    vi.mocked(api.subscribeRun).mockImplementation(async (_runId: string, onEvent) => {
      handler = onEvent;
      return async () => {};
    });
    vi.mocked(api.runTimeline).mockImplementation(async (runId: string) => {
      // 对齐完成前推入两条缓冲事件
      handler?.({ seq: 1, ts: "", run_id: runId, type: "run_started" });
      handler?.({ seq: 2, ts: "", run_id: runId, type: "run_completed", output: { ok: 1 } });
      return timeline(runId);
    });

    expect(await attachRun("run1")).toBe(true);
    expect(monitor.lastSeq).toBe(2);
    expect(monitor.phase).toBe("succeeded");
    expect(monitor.output).toEqual({ ok: 1 });
  });
});
