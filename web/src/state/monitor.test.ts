import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Timeline, WorkflowDetail } from "../types";

// monitor 的 attach 竞态语义：局部构建 + 令牌比对 + 原子换入。
// mock api 层，验证「被取代的 attach 不得写回 monitor、不得泄漏订阅」。
vi.mock("../api/flow", () => ({
  subscribeRun: vi.fn(),
  runTimeline: vi.fn(),
  runObservations: vi.fn(),
  startRun: vi.fn(),
  getWorkflow: vi.fn(),
  runCancel: vi.fn(),
  runSignal: vi.fn(),
}));

import * as api from "../api/flow";
import { editor } from "./editor";
import { toast } from "./toast";
import {
  attachRun,
  backToParentRun,
  cancelRunStart,
  canCancelRun,
  detachRun,
  monitor,
  needReattach,
  openChildRun,
  startRun,
} from "./monitor";
import { closeAllOverlays } from "./overlays";
import { modalEntries } from "./modal";
import { flushPromises } from "@vue/test-utils";

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
  cancelRunStart();
  closeAllOverlays();
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

describe("startRun 前置校验", () => {
  it("运行已换入后离开编辑器，仍继续观测该运行直到终态", async () => {
    const observations = deferred<Awaited<ReturnType<typeof api.runObservations>>>();
    let markDirty!: () => void;
    vi.mocked(api.subscribeRun).mockImplementationOnce(async (_id, onEvent) => {
      markDirty = () =>
        onEvent({
          lsn: "1",
          event_index: 0,
          event: { run_id: "observed", run_seq: "2", kind: "RunCompleted", payload: null },
        });
      return async () => {};
    });
    editor.workflowId = "wf-observed";
    editor.publishedVersion = 1;
    monitor.inputText = "";
    vi.mocked(api.getWorkflow).mockResolvedValueOnce({
      workflow_id: "wf-observed",
      version: 1,
      status: "published",
      published_version: 1,
      definition: { nodes: [], edges: [] },
    });
    vi.mocked(api.startRun).mockResolvedValueOnce({ run_id: "observed", workflow_version: 1 });
    vi.mocked(api.runTimeline).mockResolvedValue(timeline("observed"));
    vi.mocked(api.runObservations).mockReturnValueOnce(observations.promise);
    const started = startRun();
    await flushPromises();
    expect(monitor.runId).toBe("observed");
    cancelRunStart();
    observations.resolve({ records: [], loss: {} });
    await started;
    vi.mocked(api.runTimeline).mockResolvedValue({ ...timeline("observed"), status: "succeeded" });
    markDirty();
    try {
      await new Promise((resolve) => setTimeout(resolve, 300));
      expect(monitor.status).toBe("succeeded");
    } finally {
      await detachRun();
    }
  });
  it("查看子运行再返回时恢复父运行输入", async () => {
    monitor.runId = "parent";
    monitor.workflowId = "wf-parent";
    monitor.runInput = { project: "parent-input" };
    vi.mocked(api.runTimeline).mockImplementation(async (id) => ({
      ...timeline(id),
      status: "succeeded",
    }));
    await openChildRun("child");
    expect(monitor.runInput).toBeUndefined();
    await backToParentRun();
    expect(monitor.runInput).toEqual({ project: "parent-input" });
    await detachRun();
  });
  it("切换工作流清空旧输入，旧定义迟到时不在新页面打开表单", async () => {
    const pending = deferred<WorkflowDetail>();
    editor.workflowId = "wf-old";
    editor.publishedVersion = 1;
    monitor.inputText = '{"token":"old-secret"}';
    vi.mocked(api.getWorkflow).mockReturnValueOnce(pending.promise);
    const started = startRun();
    editor.workflowId = "wf-new";
    expect(monitor.inputText).toBe("");
    expect(monitor.starting).toBe(false);
    pending.resolve({
      workflow_id: "wf-old",
      version: 1,
      status: "published",
      published_version: 1,
      definition: { nodes: [], edges: [], input_schema: { type: "object", required: ["name"] } },
    });
    await started;
    expect(modalEntries()).toHaveLength(0);
    expect(api.startRun).not.toHaveBeenCalled();
  });

  it("离开编辑器时取消等待输入的启动，并关闭对应表单", async () => {
    editor.workflowId = "wf-close";
    editor.publishedVersion = 1;
    monitor.inputText = "";
    vi.mocked(api.getWorkflow).mockResolvedValueOnce({
      workflow_id: "wf-close",
      version: 1,
      status: "published",
      published_version: 1,
      definition: { nodes: [], edges: [], input_schema: { type: "object", required: ["name"] } },
    });
    const started = startRun();
    await flushPromises();
    expect(modalEntries()).toHaveLength(1);
    cancelRunStart();
    await started;
    expect(modalEntries()).toHaveLength(0);
    expect(api.startRun).not.toHaveBeenCalled();
  });

  it("旧启动取消后迟到的 finally 不解除新启动的忙碌状态", async () => {
    const old = deferred<WorkflowDetail>();
    const next = deferred<WorkflowDetail>();
    editor.workflowId = "wf-old";
    editor.publishedVersion = 1;
    vi.mocked(api.getWorkflow).mockReturnValueOnce(old.promise).mockReturnValueOnce(next.promise);
    const a = startRun();
    editor.workflowId = "wf-new";
    const b = startRun();
    old.resolve({
      workflow_id: "wf-old",
      version: 1,
      status: "published",
      published_version: 1,
      definition: { nodes: [], edges: [] },
    });
    await a;
    expect(monitor.starting).toBe(true);
    cancelRunStart();
    next.resolve({
      workflow_id: "wf-new",
      version: 1,
      status: "published",
      published_version: 1,
      definition: { nodes: [], edges: [] },
    });
    await b;
    expect(api.startRun).not.toHaveBeenCalled();
  });

  it("修改下一次运行的草稿不改变已启动运行的输入", async () => {
    editor.workflowId = "wf-snapshot";
    editor.publishedVersion = 1;
    monitor.inputText = '{"project":"original"}';
    vi.mocked(api.getWorkflow).mockResolvedValueOnce({
      workflow_id: "wf-snapshot",
      version: 1,
      status: "published",
      published_version: 1,
      definition: { nodes: [], edges: [] },
    });
    vi.mocked(api.startRun).mockResolvedValueOnce({ run_id: "snapshot", workflow_version: 1 });
    vi.mocked(api.runTimeline).mockResolvedValue({ ...timeline("snapshot"), status: "succeeded" });
    await startRun();
    monitor.inputText = '{"project":"next"}';
    expect(monitor.runInput).toEqual({ project: "original" });
    await detachRun();
  });
  it("读取固定发布版本的必填合同，取消表单不创建运行", async () => {
    editor.workflowId = "wf-contract";
    editor.publishedVersion = 2;
    monitor.inputText = "";
    vi.mocked(api.getWorkflow).mockResolvedValue({
      workflow_id: "wf-contract",
      version: 2,
      status: "published",
      published_version: 2,
      definition: {
        nodes: [],
        edges: [],
        input_schema: { type: "object", required: ["id"], properties: { id: { type: "string" } } },
      },
    });
    const pending = startRun();
    await flushPromises();
    expect(api.getWorkflow).toHaveBeenCalledWith("wf-contract", 2);
    expect(modalEntries()[0]?.payload.kind).toBe("form");
    expect(api.startRun).not.toHaveBeenCalled();
    closeAllOverlays();
    await pending;
    expect(api.startRun).not.toHaveBeenCalled();
    expect(monitor.starting).toBe(false);
  });
  it("未发布工作流点运行：不发 run.start（避免服务端英文 -32602）", async () => {
    editor.workflowId = "wf-unpublished";
    editor.publishedVersion = null;
    monitor.inputText = "";
    await startRun();
    expect(api.startRun).not.toHaveBeenCalled();
  });
});

describe("monitor 生命周期（终态收尾/降级）", () => {
  it("终态后停止时间线轮询，不再重复拉取", async () => {
    vi.useFakeTimers();
    try {
      vi.mocked(api.runTimeline).mockResolvedValue({
        ...timeline("run-done"),
        status: "succeeded",
      });
      vi.mocked(api.runObservations).mockResolvedValue({ records: [], loss: {} });
      expect(await attachRun("run-done")).toBe(true);
      expect(api.runTimeline).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(2000);
      // 终态后 250ms 轮询必须停掉（只允许 800ms 收尾补拉不触发 timeline）
      expect(api.runTimeline).toHaveBeenCalledTimes(1);
      await detachRun();
    } finally {
      vi.useRealTimers();
    }
  });

  it("运行中→终态跳变发一次完成通知", async () => {
    vi.useFakeTimers();
    try {
      let onEvent: ((e: unknown) => void) | null = null;
      vi.mocked(api.subscribeRun).mockImplementation(async (_id: string, cb: unknown) => {
        onEvent = cb as (e: unknown) => void;
        return async () => {};
      });
      vi.mocked(api.runObservations).mockResolvedValue({ records: [], loss: {} });
      vi.mocked(api.runTimeline).mockResolvedValue(timeline("run-finish"));
      expect(await attachRun("run-finish")).toBe(true);

      const success = vi.spyOn(toast, "success").mockImplementation(() => {});
      vi.mocked(api.runTimeline).mockResolvedValue({
        ...timeline("run-finish"),
        status: "succeeded",
      });
      onEvent!({ event: { run_id: "run-finish" } });
      await vi.advanceTimersByTimeAsync(250);
      expect(success).toHaveBeenCalledTimes(1);
      await detachRun();
      success.mockRestore();
    } finally {
      vi.useRealTimers();
    }
  });

  it("timeline 拉取失败：标记降级、停止重试，detach 后复位", async () => {
    vi.useFakeTimers();
    try {
      vi.mocked(api.runTimeline).mockRejectedValue(new Error("RESPONSE_TOO_LARGE"));
      vi.mocked(api.runObservations).mockResolvedValue({ records: [], loss: {} });
      expect(await attachRun("run-big")).toBe(true);
      expect(monitor.timelineUnavailable).toBe(true);
      expect(api.runTimeline).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(2000);
      expect(api.runTimeline).toHaveBeenCalledTimes(1);
      await detachRun();
      expect(monitor.timelineUnavailable).toBe(false);
    } finally {
      vi.useRealTimers();
    }
  });
});
