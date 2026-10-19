import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import * as api from "../api/flow";
import { toast } from "../state/toast";
import type { RunRecord, RunStats } from "../types";
import DashboardView from "./DashboardView.vue";

vi.mock("../api/flow", () => ({ runStats: vi.fn(), listRuns: vi.fn() }));
vi.mock("../rpc/client", () => ({
  client: { onReconnect: vi.fn(() => () => {}) },
  errText: (e: unknown) => (e instanceof Error ? e.message : String(e)),
}));
vi.mock("../state/editor", () => ({
  editor: { workflows: [] },
  refreshWorkflows: vi.fn(async () => {}),
}));
vi.mock("../state/toast", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));

const stats: RunStats = {
  total: 3,
  by_status: { running: 1, succeeded: 2 },
  by_workflow: [],
};

function run(id: string, status: string): RunRecord {
  return {
    id,
    workflow_id: "wf-1",
    workflow_version: 1,
    status,
    input: null,
    output: null,
    error: null,
    source: "manual",
    source_detail: null,
    started_at: "2026-10-18T00:00:00Z",
    ended_at: null,
  };
}

const wrappers: ReturnType<typeof mount>[] = [];
function create() {
  const wrapper = mount(DashboardView, {
    global: { stubs: { RouterLink: { props: ["to"], template: "<a><slot /></a>" } } },
  });
  wrappers.push(wrapper);
  return wrapper;
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.runStats).mockResolvedValue(stats);
  vi.mocked(api.listRuns).mockResolvedValue([]);
  vi.mocked(toast.error).mockClear();
});

afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount());
});

describe("dashboard refresh failures", () => {
  it("刷新失败显示错误条并保留上一次成功的统计", async () => {
    const w = create();
    await flushPromises();
    expect(w.find(".cards").text()).toContain("3");

    vi.mocked(api.runStats).mockRejectedValue(new Error("backend down"));
    vi.mocked(api.listRuns).mockRejectedValue(new Error("backend down"));
    await w.find(".page-header button").trigger("click");
    await flushPromises();

    const alert = w.find('[role="alert"]');
    expect(alert.exists()).toBe(true);
    expect(alert.text()).toContain("backend down");
    expect(alert.text()).toContain("当前显示上一次成功刷新的数据");
    // 旧统计未被清零/覆盖
    expect(w.find(".cards").text()).toContain("3");
    // 失败不得伪装成空态
    expect(w.text()).not.toContain("暂无运行记录");
  });

  it("首拉失败不显示空态，重试成功后恢复空态", async () => {
    vi.mocked(api.runStats).mockRejectedValueOnce(new Error("offline"));
    vi.mocked(api.listRuns).mockRejectedValueOnce(new Error("offline"));
    const w = create();
    await flushPromises();

    expect(w.find('[role="alert"]').exists()).toBe(true);
    expect(w.text()).not.toContain("暂无运行记录");
    expect(w.text()).not.toContain("暂无数据");
    expect(toast.error).toHaveBeenCalledTimes(1);

    await w.find('[role="alert"] button').trigger("click");
    await flushPromises();

    expect(w.find('[role="alert"]').exists()).toBe(false);
    expect(w.text()).toContain("暂无运行记录");
    expect(w.text()).toContain("暂无数据");
  });

  it("状态词汇走 labels.ts（awaiting_resume 文案与徽章颜色）", async () => {
    vi.mocked(api.listRuns).mockResolvedValue([run("run-00000001", "awaiting_resume")]);
    const w = create();
    await flushPromises();

    expect(w.text()).toContain("挂起待恢复");
    expect(w.find(".badge").classes()).toContain("run-awaiting");
  });
});

describe("capacity display", () => {
  function withStats(byStatus: Record<string, number>) {
    vi.mocked(api.runStats).mockResolvedValue({
      total: Object.values(byStatus).reduce((a, b) => a + b, 0),
      by_status: byStatus,
      by_workflow: [],
    });
  }

  it("非终态 = running + awaiting_resume，容量/派发上限口径来自 OPS.md", async () => {
    withStats({ running: 500, awaiting_resume: 300, succeeded: 5 });
    const w = create();
    await flushPromises();

    const cap = w.find(".capacity");
    expect(cap.text()).toContain("非终态 800 / 1000");
    expect(cap.text()).toContain("在飞派发上限 16");
    // 运维口径收进 tooltip，不占首屏正文
    expect(cap.text()).not.toContain("OPS.md");
    expect(cap.attributes("title")).toContain("OPS.md");
  });

  it("≥80% 用 warn 配色提示接近平台上限", async () => {
    withStats({ running: 799, awaiting_resume: 1 });
    const w = create();
    await flushPromises();

    expect(w.find(".capacity").text()).toContain("非终态 800 / 1000");
    expect(w.find(".capacity").classes()).toContain("warn");
  });

  it("低于 80% 不告警", async () => {
    withStats({ running: 798, awaiting_resume: 1 });
    const w = create();
    await flushPromises();

    expect(w.find(".capacity").text()).toContain("非终态 799 / 1000");
    expect(w.find(".capacity").classes()).not.toContain("warn");
  });
});

describe("copy-cell", () => {
  it("run id 单元格是 button.copy-id，点击复制并保留 title", async () => {
    vi.mocked(api.listRuns).mockResolvedValue([run("run-00000001", "succeeded")]);
    const writeText = vi.fn(async () => {});
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText },
      configurable: true,
    });
    const w = create();
    await flushPromises();

    const cell = w.find("button.copy-id");
    expect(cell.exists()).toBe(true);
    expect(cell.attributes("title")).toBe("run-00000001");
    await cell.trigger("click");
    expect(writeText).toHaveBeenCalledWith("run-00000001");
  });
});
