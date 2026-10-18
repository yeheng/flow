import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import JournalView from "./JournalView.vue";

/**
 * 观测增量拉取：订阅事件节流（250ms）拉 run.observations.page，
 * from_seq=已展示最大 seq（闭区间）逐页递进；「加载更多」继续同一条分页链。
 */

const h = vi.hoisted(() => {
  interface ObsRec {
    seq: string;
    [k: string]: unknown;
  }
  const obsStore: ObsRec[] = [];
  /** run.observations.page 的 from_seq 调用序列（十进制字符串水位） */
  const obsCalls: string[] = [];
  const monitorCbs: {
    snapshot?: (v: unknown) => void;
    event?: (e: unknown) => void;
    loss?: (n: number) => void;
  } = {};

  function snapshot(runId: string) {
    return { run_id: runId, status: "running", last_run_seq: "0" };
  }

  class FakeEventWindow {
    events: unknown[] = [];
    dropped = 0;
    push(e: unknown): void {
      this.events.push(e);
    }
  }

  class FakeJournalClient {
    constructor(
      _url: string,
      _token: string,
      _http: string,
    ) {}
    close(): void {}
    async call<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
      if (method === "workflow.list") return { values: [] } as T;
      if (method === "run.list") return { values: [snapshot("run-0001")] } as T;
      if (method === "run.get")
        return { value: snapshot(String(params.run_id ?? "run-0001")) } as T;
      if (method === "run.observations.page") {
        const from = String(params.from_seq ?? "0");
        obsCalls.push(from);
        const limit = Number(params.limit ?? 100);
        return {
          records: obsStore.filter((r) => BigInt(r.seq) >= BigInt(from)).slice(0, limit),
          loss: {},
        } as T;
      }
      return {} as T;
    }
    async page() {
      return { events: [], next_cursor: null };
    }
    async monitor(
      _run: string,
      snapshotCb: (v: unknown) => void,
      eventCb: (e: unknown) => void,
      lossCb: (n: number) => void,
    ) {
      monitorCbs.snapshot = snapshotCb;
      monitorCbs.event = eventCb;
      monitorCbs.loss = lossCb;
      snapshotCb(snapshot(_run));
      return async () => {};
    }
    async download(): Promise<void> {}
  }

  return { obsStore, obsCalls, monitorCbs, FakeJournalClient, FakeEventWindow };
});

vi.mock("../api/journal", () => ({
  JournalClient: h.FakeJournalClient,
  EventWindow: h.FakeEventWindow,
}));
vi.mock("../platform", () => ({ isDesktop: () => false }));
// CodeEditor 顶层 import monaco（jsdom 下不可用）：用空组件替掉，避免拉起 monaco
vi.mock("../components/CodeEditor.vue", () => ({ default: { template: "<div />" } }));
vi.mock("../state/modal", () => ({ confirmDialog: vi.fn(async () => true) }));
vi.mock("../state/toast", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));

function obs(seq: string, message: string) {
  return {
    seq,
    run_id: "run-0001",
    dispatch_id: "d1",
    ts: "2026-10-18T00:00:00Z",
    line: { node_id: "n1", attempt: 1, level: "info", stream: "stdout", message },
  };
}

function event(i: number) {
  return {
    lsn: `lsn-${i}`,
    event_index: i,
    event: {
      run_id: "run-0001",
      run_seq: String(100 + i),
      kind: "node_started",
      payload: null,
    },
  };
}

const wrappers: ReturnType<typeof mount>[] = [];
function create() {
  const wrapper = mount(JournalView);
  wrappers.push(wrapper);
  return wrapper;
}

function buttonByText(w: ReturnType<typeof mount>, text: string) {
  const btn = w.findAll("button").find((b) => b.text().includes(text));
  if (!btn) throw new Error(`找不到按钮：${text}`);
  return btn;
}

/** 连接工作区并监控 run-0001：watchRun 尾部会以 from_seq=0 拉一页观测 */
async function openRun(w: ReturnType<typeof mount>) {
  await w.find("form.connect-bar").trigger("submit");
  await flushPromises();
  await buttonByText(w, "监控").trigger("click");
  await flushPromises();
}

/** 等过 250ms 节流窗口并冲刷微任务 */
async function waitThrottle() {
  await new Promise((resolve) => setTimeout(resolve, 320));
  await flushPromises();
}

beforeEach(() => {
  vi.clearAllMocks();
  h.obsStore.splice(0);
  h.obsCalls.splice(0);
  delete h.monitorCbs.snapshot;
  delete h.monitorCbs.event;
  delete h.monitorCbs.loss;
  for (const seq of ["1", "2", "3", "4", "5"]) h.obsStore.push(obs(seq, `line-${seq}`));
});

afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount());
});

describe("observation incremental pull", () => {
  it("订阅事件节流拉取观测，from_seq 按水位递进；加载更多继续分页", async () => {
    const w = create();
    await openRun(w);

    // 打开 run 即以 from_seq=0 起拉一页（水位 → 5）
    expect(h.obsCalls).toEqual(["0"]);
    expect(w.text()).toContain("line-1");

    // 新观测落库后连发 3 个事件：250ms 节流合并成一次增量拉取
    h.obsStore.push(obs("6", "line-6"), obs("7", "line-7"));
    for (let i = 0; i < 3; i++) h.monitorCbs.event!(event(i));
    await waitThrottle();
    expect(h.obsCalls).toEqual(["0", "5"]); // from_seq 递进：闭区间起点 = 已展示最大 seq
    expect(w.text()).toContain("line-6");
    expect(w.text()).toContain("line-7");

    // 「加载更多」：from_seq=当前水位继续分页
    h.obsStore.push(obs("8", "line-8"));
    await buttonByText(w, "加载更多").trigger("click");
    await flushPromises();
    expect(h.obsCalls).toEqual(["0", "5", "7"]);
    expect(w.text()).toContain("line-8");
  });

  it("闭区间重读的水位记录不重复追加", async () => {
    const w = create();
    await openRun(w);

    await buttonByText(w, "加载更多").trigger("click");
    await flushPromises();

    expect(h.obsCalls).toEqual(["0", "5"]);
    // from_seq=5 会重读 seq=5 那条，列表仍只有 5 条
    expect(w.findAll(".log-item").length).toBe(5);
    expect(w.text()).toContain("line-5");
  });
});
