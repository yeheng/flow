import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import * as api from "../api/flow";
import { RpcError } from "../rpc/errors";
import { editor } from "../state/editor";
import RunListView from "./RunListView.vue";
import type { RunRecord, WorkflowSummary } from "../types";

vi.mock("../api/flow", async (original) => ({
  ...(await original<typeof import("../api/flow")>()),
  listRuns: vi.fn(),
  listRunsLite: vi.fn(),
  runCancel: vi.fn(),
}));
vi.mock("vue-router", () => {
  const route = { query: {} as Record<string, string> };
  return {
    useRoute: () => route,
    useRouter: () => ({ replace: vi.fn() }),
    __route: route,
  };
});
vi.mock("../state/editor", () => ({
  editor: { workflows: [] },
  refreshWorkflows: vi.fn(async () => {}),
}));
vi.mock("../state/modal", () => ({ confirmDialog: vi.fn(async () => true) }));
vi.mock("../state/toast", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));

/** RouterLink 桩渲染 href，互跳路由可断言 */
const RouterLinkStub = { props: ["to"], template: '<a :href="to"><slot /></a>' };

function workflow(workflowId: string, name: string): WorkflowSummary {
  return {
    workflow_id: workflowId,
    name,
    latest_version: 1,
    published_version: 1,
    created_at: "2026-10-18T00:00:00Z",
  };
}

function run(id: string, workflowId: string): RunRecord {
  return {
    id,
    workflow_id: workflowId,
    workflow_version: 1,
    status: "succeeded",
    input: null,
    output: null,
    error: null,
    source: "manual",
    source_detail: null,
    started_at: "2026-10-18T00:00:00Z",
    ended_at: null,
  };
}

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

const wrappers: ReturnType<typeof mount>[] = [];
function create(props: { workflowId?: string } = {}) {
  const wrapper = mount(RunListView, {
    props,
    global: {
      stubs: {
        RouterLink: RouterLinkStub,
      },
    },
  });
  wrappers.push(wrapper);
  return wrapper;
}

beforeEach(() => {
  vi.clearAllMocks();
  editor.workflows.splice(0);
});

afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount());
});

describe("run list refresh", () => {
  it("筛选切换丢弃在途旧响应，旧筛选数据不残留", async () => {
    const first = deferred<RunRecord[]>();
    const second = deferred<RunRecord[]>();
    vi.mocked(api.listRuns)
      .mockImplementationOnce(() => first.promise)
      .mockImplementationOnce(() => second.promise);

    const w = create();
    await flushPromises();
    expect(api.listRuns).toHaveBeenCalledTimes(1);

    // 首拉在途时切换筛选：新请求被挂起，等旧请求收尾后用新条件补拉
    await w.find('select[aria-label="状态筛选"]').setValue("failed");
    first.resolve([run("old-run-1", "old-workflow")]);
    await flushPromises();

    expect(w.text()).not.toContain("old-workflow");
    expect(api.listRuns).toHaveBeenCalledTimes(2);
    expect(vi.mocked(api.listRuns).mock.calls[1][0]).toMatchObject({ status: "failed" });

    second.resolve([run("new-run-1", "new-workflow")]);
    await flushPromises();
    expect(w.text()).toContain("new-workflow");
    expect(w.text()).not.toContain("old-workflow");
  });

  it("工作流切换同样重置列表并丢弃旧响应", async () => {
    const first = deferred<RunRecord[]>();
    const second = deferred<RunRecord[]>();
    vi.mocked(api.listRuns)
      .mockImplementationOnce(() => first.promise)
      .mockImplementationOnce(() => second.promise);

    const w = create({ workflowId: "wf-a" });
    await flushPromises();
    expect(api.listRuns).toHaveBeenCalledTimes(1);

    await w.setProps({ workflowId: "wf-b" });
    first.resolve([run("old-run-1", "wf-a")]);
    await flushPromises();
    expect(api.listRuns).toHaveBeenCalledTimes(2);
    expect(vi.mocked(api.listRuns).mock.calls[1][0]).toMatchObject({ workflowId: "wf-b" });
    expect(w.text()).not.toContain("old-run-1".slice(0, 8));

    second.resolve([run("new-run-2", "wf-b")]);
    await flushPromises();
    expect(w.text()).toContain("new-run".slice(0, 8));
  });
});

describe("run list 大值降级", () => {
  it("物化列表超预算 → 切原始快照扫描，展示降级提示", async () => {
    vi.mocked(api.listRuns).mockRejectedValueOnce(new RpcError(-32021, "RESPONSE_TOO_LARGE"));
    vi.mocked(api.listRunsLite).mockResolvedValueOnce({
      runs: [run("lite-run-1", "lite-workflow")],
      hasMore: false,
      scanned: true,
    });
    const w = create();
    await flushPromises();
    expect(api.listRunsLite).toHaveBeenCalledTimes(1);
    expect(w.text()).toContain("lite-workflow");
    expect(w.text()).toContain("已降级为原始快照扫描");
  });
});

describe("run list workflow 模式页头与互跳", () => {
  it("页头显示工作流名（缺失回退 id），并给出编辑器/版本/触发器互跳", async () => {
    editor.workflows.push(workflow("wf-a", "订单流程"));
    vi.mocked(api.listRuns).mockResolvedValue([]);
    const w = create({ workflowId: "wf-a" });
    await flushPromises();

    expect(w.find(".page-header h2").text()).toContain("订单流程");
    expect(
      w
        .findAll(".wf-links a")
        .map((a) => a.attributes("href")),
    ).toEqual(["/workflows/wf-a", "/workflows/wf-a/versions", "/workflows/wf-a/triggers"]);
  });

  it("工作流不在清单里时回退显示 id", async () => {
    vi.mocked(api.listRuns).mockResolvedValue([]);
    const w = create({ workflowId: "wf-unknown" });
    await flushPromises();

    expect(w.find(".page-header h2").text()).toContain("wf-unknown");
    expect(w.find(".wf-links").exists()).toBe(true);
  });

  it("run id 复制单元格是 button.copy-id，点击复制并保留 title", async () => {
    const writeText = vi.fn(async () => {});
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText },
      configurable: true,
    });
    vi.mocked(api.listRuns).mockResolvedValue([run("run-00000001", "wf-1")]);
    const w = create();
    await flushPromises();

    const cell = w.find("button.copy-id");
    expect(cell.exists()).toBe(true);
    expect(cell.attributes("title")).toBe("run-00000001");
    await cell.trigger("click");
    expect(writeText).toHaveBeenCalledWith("run-00000001");
    // 全局模式没有工作流互跳区
    expect(w.find(".wf-links").exists()).toBe(false);
  });
});
