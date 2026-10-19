import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import JournalView from "./JournalView.vue";

/**
 * 观测增量拉取：选择 run 即拉首页（from_seq=0），「加载更多」从水位继续
 * （run.observations.page 的 from_seq 是闭区间，追加时跳过水位上的那条）。
 */

const h = vi.hoisted(() => {
  interface ObsRec {
    seq: string;
    [k: string]: unknown;
  }
  const obsStore: ObsRec[] = [];
  /** run.observations.page 的 from_seq 调用序列（十进制字符串水位） */
  const obsCalls: string[] = [];

  function snapshot(runId: string) {
    return { run_id: runId, status: "running", last_run_seq: "0" };
  }

  class FakeJournalClient {
    constructor(
      _url: string,
      _token: string,
      _http: string,
    ) {}
    close(): void {}
    async call<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
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
    async download(): Promise<void> {}
  }

  return { obsStore, obsCalls, FakeJournalClient };
});

vi.mock("../api/journal", () => ({ JournalClient: h.FakeJournalClient }));
vi.mock("../platform", () => ({ isDesktop: () => false }));
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

/** 连接工作区并选择 run-0001：selectRun 尾部会以 from_seq=0 拉一页观测 */
async function openRun(w: ReturnType<typeof mount>) {
  await w.find("form.connect-bar").trigger("submit");
  await flushPromises();
  await buttonByText(w, "查看").trigger("click");
  await flushPromises();
}

beforeEach(() => {
  vi.clearAllMocks();
  h.obsStore.splice(0);
  h.obsCalls.splice(0);
  for (const seq of ["1", "2", "3", "4", "5"]) h.obsStore.push(obs(seq, `line-${seq}`));
});

afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount());
});

describe("observation incremental pull", () => {
  it("选择 run 拉首页观测；加载更多从水位继续分页", async () => {
    const w = create();
    await openRun(w);

    // 打开 run 即以 from_seq=0 起拉一页（水位 → 5）
    expect(h.obsCalls).toEqual(["0"]);
    expect(w.text()).toContain("line-1");

    // 新观测落库后点「加载更多」：from_seq=当前水位继续分页
    h.obsStore.push(obs("6", "line-6"), obs("7", "line-7"));
    await buttonByText(w, "加载更多").trigger("click");
    await flushPromises();
    expect(h.obsCalls).toEqual(["0", "5"]);
    expect(w.text()).toContain("line-6");
    expect(w.text()).toContain("line-7");

    // 再次「加载更多」：水位已递进到 7
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
