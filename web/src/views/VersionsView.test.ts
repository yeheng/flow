import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import { createMemoryHistory, createRouter } from "vue-router";
import * as api from "../api/flow";
import type { VersionMeta } from "../types";
import VersionsView from "./VersionsView.vue";

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
