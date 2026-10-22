import { beforeEach, expect, it, vi } from "vitest";
import { dirty, editor, markSaved, publish, save, selectWorkflow } from "./editor";
import * as api from "../api/flow";
import { RpcError } from "../rpc/client";

vi.mock("../api/flow", () => ({
  getWorkflow: vi.fn(),
  updateWorkflow: vi.fn(),
  publishWorkflow: vi.fn(),
  listWorkflows: vi.fn(),
}));

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

beforeEach(() => {
  vi.resetAllMocks();
  editor.workflowId = "a";
  editor.version = 0;
  editor.publishedVersion = null;
  editor.nodeTypes = ["start", "end"].map((type) => ({
    type,
    label: type,
    category: "control",
    params_schema: { type: "object" },
    ports: type === "start" ? [{ id: "out", label: "out" }] : [{ id: "in", label: "in" }],
  }));
  editor.nodes = editor.nodeTypes.map((nodeType) => ({
    id: nodeType.type,
    type: "flow",
    position: { x: 0, y: 0 },
    data: { name: nodeType.type, nodeType, params: {} },
  }));
  editor.edges = [{ id: "edge", source: "start", target: "end", sourceHandle: "out" }];
  markSaved();
  vi.mocked(api.listWorkflows).mockResolvedValue([]);
  vi.mocked(api.updateWorkflow).mockResolvedValue(1);
  vi.mocked(api.publishWorkflow).mockResolvedValue();
});

it("saves and publishes the first version without reading a nonexistent version", async () => {
  vi.mocked(api.getWorkflow).mockRejectedValue(new RpcError(-32011, "no version"));
  await publish();
  expect(api.getWorkflow).not.toHaveBeenCalled();
  expect(api.publishWorkflow).toHaveBeenCalledWith("a", 1);
  expect(editor.publishedVersion).toBe(1);
});

it("invalidates the computed dirty flag when the saved snapshot changes", () => {
  expect(dirty.value).toBe(false);
  editor.nodes[0]!.data.name = "changed";
  expect(dirty.value).toBe(true);
  markSaved();
  expect(dirty.value).toBe(false);
});

it("画布自动网格吸附不产生未保存标记，实际移动到其他网格仍标记为修改", () => {
  editor.nodes[0]!.position = { x: 300, y: 200 };
  markSaved();
  editor.nodes[0]!.position = { x: 304, y: 208 };
  expect(dirty.value).toBe(false);
  editor.nodes[0]!.position.x = 320;
  expect(dirty.value).toBe(true);
});

it("does not hide read failures for an existing version", async () => {
  editor.version = 1;
  vi.mocked(api.getWorkflow).mockRejectedValue(new RpcError(-32011, "deleted"));
  expect(await save()).toBe(false);
  expect(api.updateWorkflow).not.toHaveBeenCalled();
});

it("keeps edits made during a save dirty and sends an immutable snapshot", async () => {
  const pending = deferred<number>();
  vi.mocked(api.updateWorkflow).mockReturnValue(pending.promise);
  const saving = save();
  editor.nodes[0]!.data.name = "changed during save";
  expect(api.updateWorkflow).toHaveBeenCalledWith(
    "a",
    expect.objectContaining({
      nodes: expect.arrayContaining([expect.objectContaining({ id: "start", name: "start" })]),
    }),
  );
  pending.resolve(1);
  expect(await saving).toBe(true);
  expect(dirty.value).toBe(true);
});

it("saves the captured canvas even when a different workflow opens during the read", async () => {
  editor.version = 1;
  const pending = deferred<Awaited<ReturnType<typeof api.getWorkflow>>>();
  vi.mocked(api.getWorkflow).mockReturnValue(pending.promise);
  const saving = save();
  editor.workflowId = "b";
  editor.version = 9;
  editor.nodes[0]!.data.name = "b canvas";
  markSaved();
  pending.resolve({ version: 1 } as Awaited<ReturnType<typeof api.getWorkflow>>);
  expect(await saving).toBe(true);
  expect(api.updateWorkflow).toHaveBeenCalledWith(
    "a",
    expect.objectContaining({
      nodes: expect.arrayContaining([expect.objectContaining({ name: "start" })]),
    }),
  );
  expect(editor.version).toBe(9);
  expect(dirty.value).toBe(false);
});

it("publishes the saved workflow and version after navigation", async () => {
  const pending = deferred<number>();
  vi.mocked(api.updateWorkflow).mockReturnValue(pending.promise);
  const publishing = publish();
  editor.workflowId = "b";
  editor.version = 9;
  editor.publishedVersion = 8;
  pending.resolve(2);
  await publishing;
  expect(api.publishWorkflow).toHaveBeenCalledWith("a", 2);
  expect(editor.version).toBe(9);
  expect(editor.publishedVersion).toBe(8);
});

it("ignores out-of-order workflow loads", async () => {
  const pending = deferred<Awaited<ReturnType<typeof api.getWorkflow>>>();
  const detail = (version: number) =>
    ({
      workflow_id: "test",
      status: "draft",
      version,
      published_version: null,
      definition: { nodes: [], edges: [] },
    }) as Awaited<ReturnType<typeof api.getWorkflow>>;
  vi.mocked(api.getWorkflow).mockReturnValueOnce(pending.promise).mockResolvedValueOnce(detail(9));
  const first = selectWorkflow("a");
  await selectWorkflow("b");
  pending.resolve(detail(2));
  await first;
  expect(editor.workflowId).toBe("b");
  expect(editor.version).toBe(9);
});
