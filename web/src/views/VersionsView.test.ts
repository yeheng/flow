import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createMemoryHistory, createRouter } from "vue-router";
import * as api from "../api/flow";
import type { VersionMeta } from "../types";
import VersionsView from "./VersionsView.vue";
import { confirmDialog } from "../state/modal";
import { ensureNodeTypes, selectWorkflow } from "../state/editor";

vi.mock("../api/flow", () => ({
  listVersions: vi.fn(),
  getWorkflow: vi.fn(),
  updateWorkflow: vi.fn(),
  publishWorkflow: vi.fn(),
}));
vi.mock("../rpc/client", () => ({
  errText: (e: unknown) => (e instanceof Error ? e.message : String(e)),
}));
vi.mock("../state/editor", () => ({
  editor: { workflows: [] },
  refreshWorkflows: vi.fn(async () => {}),
  selectWorkflow: vi.fn(async () => {}),
  confirmDiscardIfDirty: vi.fn(async () => true),
  ensureNodeTypes: vi.fn(async () => true),
}));
vi.mock("../state/modal", () => ({ confirmDialog: vi.fn(async () => true) }));
vi.mock("../state/toast", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() },
}));

function version(v: number, checksum: string, status = "published"): VersionMeta {
  return {
    workflow_id: "wf-1",
    version: v,
    status,
    checksum,
    created_at: "2026-10-18T00:00:00Z",
  };
}

const wrappers: ReturnType<typeof mount>[] = [];
function create() {
  const wrapper = mount(VersionsView, {
    props: { workflowId: "wf-1" },
    global: {
      plugins: [createRouter({ history: createMemoryHistory(), routes: [] })],
      stubs: { RouterLink: { props: ["to"], template: "<a><slot /></a>" } },
    },
  });
  wrappers.push(wrapper);
  return wrapper;
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(confirmDialog).mockResolvedValue(true);
  vi.mocked(api.listVersions).mockResolvedValue([version(2, "b".repeat(32))]);
  vi.mocked(api.getWorkflow).mockResolvedValue({
    workflow_id: "wf-1",
    version: 2,
    status: "published",
    definition: { nodes: [], edges: [] },
    published_version: 2,
  });
});

afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount());
});

describe("versions copy-cell", () => {
  it("旧定义迟到后不能污染新工作流的版本缓存或发布内容", async () => {
    let resolve!: (value: Awaited<ReturnType<typeof api.getWorkflow>>) => void;
    const pending = new Promise<Awaited<ReturnType<typeof api.getWorkflow>>>((r) => {
      resolve = r;
    });
    const current = { nodes: [{ id: "new", type: "start", params: {} }], edges: [] };
    vi.mocked(api.getWorkflow).mockImplementation((id, version) =>
      id === "wf-1"
        ? pending
        : Promise.resolve({
            workflow_id: id,
            version: version ?? 2,
            status: "published",
            published_version: 2,
            definition: current,
          }),
    );
    vi.mocked(api.updateWorkflow).mockResolvedValue(3);
    const w = create();
    await flushPromises();
    await w.setProps({ workflowId: "wf-2" });
    await flushPromises();
    resolve({
      workflow_id: "wf-1",
      version: 2,
      status: "published",
      published_version: 2,
      definition: { nodes: [{ id: "old", type: "start", params: {} }], edges: [] },
    });
    await flushPromises();
    await w
      .findAll("button")
      .find((b) => b.text() === "以此版本发布")!
      .trigger("click");
    await flushPromises();
    expect(api.updateWorkflow).toHaveBeenCalledWith("wf-2", current);
    expect(api.publishWorkflow).toHaveBeenCalledWith("wf-2", 3);
  });
  it("切换工作流后丢弃旧版本列表的迟到响应", async () => {
    let resolve!: (value: VersionMeta[]) => void;
    vi.mocked(api.listVersions).mockImplementation((id) =>
      id === "wf-1"
        ? new Promise((r) => {
            resolve = r;
          })
        : Promise.resolve([version(9, "new")]),
    );
    const w = create();
    await w.setProps({ workflowId: "wf-2" });
    await flushPromises();
    resolve([version(1, "old")]);
    await flushPromises();
    expect(w.get("tbody").text()).toContain("v9");
    expect(w.get("tbody").text()).not.toContain("v1");
  });

  it("确认发布期间切换工作流，不向新工作流发布旧内容", async () => {
    let resolve!: (value: boolean) => void;
    vi.mocked(confirmDialog).mockImplementationOnce(
      () =>
        new Promise((r) => {
          resolve = r;
        }),
    );
    const w = create();
    await flushPromises();
    await w
      .findAll("button")
      .find((b) => b.text() === "以此版本发布")!
      .trigger("click");
    await w.setProps({ workflowId: "wf-2" });
    resolve(true);
    await flushPromises();
    expect(api.updateWorkflow).not.toHaveBeenCalled();
    expect(api.publishWorkflow).not.toHaveBeenCalled();
  });

  it("版本页直接加载到编辑器前先加载节点类型", async () => {
    vi.mocked(selectWorkflow).mockResolvedValue(true);
    const w = create();
    await flushPromises();
    await w
      .findAll("button")
      .find((b) => b.text() === "加载到编辑器")!
      .trigger("click");
    await flushPromises();
    expect(ensureNodeTypes).toHaveBeenCalled();
    expect(selectWorkflow).toHaveBeenCalledWith("wf-1", 2);
  });
  it("校验和单元格是 button.copy-id，点击复制并保留 title", async () => {
    const writeText = vi.fn(async () => {});
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText },
      configurable: true,
    });
    vi.mocked(api.listVersions).mockResolvedValue([version(2, "abcdef0123456789abcdef")]);
    const w = create();
    await flushPromises();

    const cell = w.find("button.copy-id");
    expect(cell.exists()).toBe(true);
    expect(cell.attributes("title")).toBe("abcdef0123456789abcdef");
    await cell.trigger("click");
    expect(writeText).toHaveBeenCalledWith("abcdef0123456789abcdef");
  });

  it("当前发布版本行标注不变", async () => {
    vi.mocked(api.listVersions).mockResolvedValue([
      version(2, "b".repeat(32)),
      version(1, "a".repeat(32), "draft"),
    ]);
    const w = create();
    await flushPromises();

    expect(w.find(".row-published").exists()).toBe(true);
    expect(w.text()).toContain("（当前发布）");
  });
});
