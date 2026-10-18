import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import * as api from "../api/flow";
import { editor } from "../state/editor";
import type { WorkflowSummary } from "../types";
import WorkflowListView from "./WorkflowListView.vue";

vi.mock("vue-router", () => ({ useRouter: () => ({ push: vi.fn() }) }));
vi.mock("../api/flow", () => ({ listWorkflows: vi.fn() }));
vi.mock("../state/editor", async () => {
  const { reactive } = await import("vue");
  return {
    editor: reactive({ workflows: [] as WorkflowSummary[] }),
    createWorkflow: vi.fn(),
    removeWorkflow: vi.fn(),
    refreshWorkflows: vi.fn(async () => {}),
  };
});
vi.mock("../state/workflow-io", () => ({ importWorkflow: vi.fn(), parseImportDocument: vi.fn() }));
vi.mock("../state/modal", () => ({
  confirmDialog: vi.fn(async () => true),
  promptDialog: vi.fn(async () => null),
}));
vi.mock("../state/toast", () => ({ toast: { success: vi.fn(), error: vi.fn() } }));
vi.mock("../rpc/client", () => ({
  errText: (e: unknown) => (e instanceof Error ? e.message : String(e)),
}));

function workflow(id: string, name: string): WorkflowSummary {
  return {
    workflow_id: id,
    name,
    latest_version: 1,
    published_version: null,
    created_at: "2026-10-18T00:00:00Z",
  };
}

const wrappers: ReturnType<typeof mount>[] = [];
function create() {
  const wrapper = mount(WorkflowListView, {
    global: { stubs: { RouterLink: { props: ["to"], template: "<a><slot /></a>" } } },
  });
  wrappers.push(wrapper);
  return wrapper;
}

beforeEach(() => {
  vi.clearAllMocks();
  editor.workflows = [];
});

afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount());
});

describe("workflow list first load", () => {
  it("首拉失败显示错误条而不是「暂无工作流」，重试成功后恢复", async () => {
    vi.mocked(api.listWorkflows).mockRejectedValueOnce(new Error("offline"));
    const w = create();
    await flushPromises();

    expect(w.find('[role="alert"]').text()).toContain("offline");
    expect(w.text()).not.toContain("暂无工作流");

    vi.mocked(api.listWorkflows).mockResolvedValue([]);
    await w.find('[role="alert"] button').trigger("click");
    await flushPromises();

    expect(w.find('[role="alert"]').exists()).toBe(false);
    expect(w.text()).toContain("暂无工作流");
  });

  it("成功拉取后渲染列表；长名称单元格允许折行（wf-cell）", async () => {
    const longName = "一个特别长的工作流名称".repeat(8);
    vi.mocked(api.listWorkflows).mockResolvedValue([workflow("wf-1", longName)]);
    const w = create();
    await flushPromises();

    expect(w.text()).toContain(longName);
    const nameCell = w.find("tbody td.wf-cell");
    expect(nameCell.exists()).toBe(true);
    expect(nameCell.text()).toContain(longName);
  });
});
