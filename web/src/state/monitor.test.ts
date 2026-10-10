import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Timeline } from "../types";

// monitor 的 attach 竞态语义：局部构建 + 令牌比对 + 原子换入。
// mock api 层，验证「被取代的 attach 不得写回 monitor、不得泄漏订阅」。
vi.mock("../api/flow", () => ({
  subscribeRun: vi.fn(),
  runTimeline: vi.fn(),
  runObservations: vi.fn(),
  startRun: vi.fn(),
  runCancel: vi.fn(),
  runSignal: vi.fn(),
}));

import * as api from "../api/flow";
import { attachRun, canCancelRun, detachRun, monitor, needReattach } from "./monitor";

function timeline(runId: string): Timeline {
  return {
    run_id: runId,
    status: "running",
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
  vi.mocked(api.runObservations).mockResolvedValue({ records: [], loss: {} });
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

  it("订阅建立与 timeline 对齐之间到达的事件置脏，随后刷新拉最新快照收敛", async () => {
    let handler: ((e: unknown) => void) | null = null;
    vi.mocked(api.subscribeRun).mockImplementation(async (_runId: string, onEvent) => {
      handler = onEvent as (e: unknown) => void;
      return async () => {};
    });
    let pulls = 0;
    vi.mocked(api.runTimeline).mockImplementation(async (runId: string) => {
      pulls += 1;
      // 对齐完成前推入两条订阅事件：订阅流只负责置脏，投影一律来自 timeline 快照
      handler?.({ event: { run_id: runId, kind: "RunStarted", run_seq: 1 } });
      handler?.({ event: { run_id: runId, kind: "RunCompleted", run_seq: 2 } });
      if (pulls === 1) return timeline(runId);
      return { ...timeline(runId), status: "succeeded", output: { ok: 1 }, last_seq: 2 };
    });

    expect(await attachRun("run1")).toBe(true);
    // attach 内的首次 refresh 已按脏标记拉到最新快照：终态不丢
    expect(pulls).toBeGreaterThanOrEqual(2);
    expect(monitor.lastSeq).toBe(2);
    expect(monitor.status).toBe("succeeded");
    expect(monitor.output).toEqual({ ok: 1 });
  });
});

describe("timeline 拉取失败的降级路径", () => {
  it("status 保持 null（不知道≠running）：取消与重连判据都不依赖它是否 running", async () => {
    const runId = "run-degraded";
    vi.mocked(api.runTimeline).mockRejectedValue(new Error("timeline 拉取失败（模拟）"));
    const ok = await attachRun(runId);
    expect(ok).toBe(true);
    expect(monitor.runId).toBe(runId);
    // 不撒谎成 running：判据是「没拿到终态」而不是「确定还在跑」
    expect(monitor.status).toBeNull();
    expect(canCancelRun.value).toBe(true);
    expect(needReattach.value).toBe(true);
    await detachRun();
  });

  it("终态 run 不显示取消、不触发重连重建", async () => {
    vi.mocked(api.runTimeline).mockResolvedValue({
      ...timeline("run-done"),
      status: "succeeded",
    });
    expect(await attachRun("run-done")).toBe(true);
    expect(canCancelRun.value).toBe(false);
    expect(needReattach.value).toBe(false);
    await detachRun();
  });

  it("awaiting_resume 算非终态：挂起的 run 仍可取消、仍会在重连后重建投影", async () => {
    // 回归：此前 UI 用 status 判文案、用 phase 判配色，挂起态两者不一致
    // （status=awaiting_resume / phase=running），渲染成「运行中配色 +
    // 挂起待恢复文案」。判据统一到 status 之后，挂起态按非终态处理。
    vi.mocked(api.runTimeline).mockResolvedValue({
      ...timeline("run-hang"),
      status: "awaiting_resume",
    });
    expect(await attachRun("run-hang")).toBe(true);
    expect(monitor.status).toBe("awaiting_resume");
    expect(canCancelRun.value).toBe(true);
    expect(needReattach.value).toBe(true);
    await detachRun();
  });
});
