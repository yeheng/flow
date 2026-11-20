import { beforeEach, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import RawWorkflowEditor from "./RawWorkflowEditor.vue";
import { editor, flowToDefinition } from "../state/editor";
import { resetHistory, undo, redo } from "../state/history";
import * as api from "../api/flow";
vi.mock("../api/flow", () => ({
  validateWorkflow: vi.fn(),
  createWorkflow: vi.fn(),
  updateWorkflow: vi.fn(),
  publishWorkflow: vi.fn(),
  startRun: vi.fn(),
  getRun: vi.fn(),
  runCancel: vi.fn(),
}));
vi.mock("./CodeEditor.vue", () => ({
  default: {
    props: ["modelValue"],
    emits: ["update:modelValue"],
    template:
      '<textarea class="json" :value="modelValue" @input="$emit(\'update:modelValue\', $event.target.value)" />',
  },
}));
beforeEach(() => {
  vi.clearAllMocks();
  editor.workflowId = "wf";
  editor.workflowName = "Test";
  editor.nodes = [];
  editor.edges = [];
  editor.definitionOptions = {};
  editor.nodeTypes = [];
  editor.secrets = ["KEY"];
  resetHistory();
  vi.mocked(api.validateWorkflow).mockResolvedValue({ valid: true, missing_secrets: [] });
});
function render() {
  return mount(RawWorkflowEditor, { global: { stubs: { RouterLink: true } } });
}
it("invalid JSON stays in draft and never mutates the canvas", async () => {
  const w = render();
  await w.get(".json").setValue("{");
  await (w.vm as unknown as { apply(): Promise<boolean> }).apply();
  expect(w.get('[role="alert"]').text()).toBeTruthy();
  expect(editor.nodes).toEqual([]);
  expect(api.validateWorkflow).not.toHaveBeenCalled();
  w.unmount();
});
it("applies validated top-level options with undo and redo", async () => {
  const w = render();
  const definition = {
    nodes: [],
    edges: [],
    permissions: { version: 1, mode: "bypass", allow: [] },
    input_schema: { type: "object" },
  };
  await w.get(".json").setValue(JSON.stringify({ name: "imported", definition }));
  expect(await (w.vm as unknown as { apply(): Promise<boolean> }).apply()).toBe(true);
  expect(flowToDefinition()).toEqual(definition);
  undo();
  expect(editor.definitionOptions).toEqual({});
  redo();
  expect(flowToDefinition()).toEqual(definition);
  w.unmount();
});
it("shows the node and secret when validation reports a missing credential", async () => {
  vi.mocked(api.validateWorkflow).mockResolvedValue({
    valid: false,
    missing_secrets: [{ node_id: "recommend", name: "HARNESS_AGENT_API_KEY" }],
  });
  const w = render();
  await w.get(".json").setValue('{"nodes":[],"edges":[],"semantics":2}');
  expect(await (w.vm as unknown as { apply(): Promise<boolean> }).apply()).toBe(false);
  expect(w.get('[role="alert"]').text()).toBe("recommend 缺少密钥 HARNESS_AGENT_API_KEY");
  expect(editor.definitionOptions).toEqual({});
  w.unmount();
});
it("does not apply a stale validation response after the draft changes", async () => {
  let resolve!: (value: { valid: boolean; missing_secrets: [] }) => void;
  vi.mocked(api.validateWorkflow).mockReturnValue(
    new Promise((r) => {
      resolve = r;
    }),
  );
  const w = render();
  await w.get(".json").setValue('{"nodes":[],"edges":[],"semantics":2}');
  const apply = (w.vm as unknown as { apply(): Promise<boolean> }).apply();
  await w.get(".json").setValue("{");
  resolve({ valid: true, missing_secrets: [] });
  expect(await apply).toBe(false);
  expect(editor.definitionOptions).toEqual({});
  w.unmount();
});
it("publishes a dedicated harness workflow and passes literal JSON through run input", async () => {
  vi.mocked(api.createWorkflow).mockResolvedValue("helper");
  vi.mocked(api.updateWorkflow).mockResolvedValue(1);
  vi.mocked(api.startRun).mockResolvedValue({ run_id: "run", workflow_version: 1 });
  vi.mocked(api.getRun).mockResolvedValue({
    run: {
      status: "succeeded",
      output: { stdout: '```json\n{"nodes":[],"edges":[]}\n```' },
    } as never,
    live: false,
  });
  const w = render();
  await w.get("input").setValue("KEY");
  await w.get(".chat-pane textarea").setValue("如何修改 ${input.name}？");
  const send = w.findAll("button").find((b) => b.text() === "发送")!;
  await send.trigger("click");
  await flushPromises();
  const definition = vi.mocked(api.updateWorkflow).mock.calls[0]![1];
  expect(definition.nodes.map((n) => n.type)).toEqual(["start", "harness", "end"]);
  expect(definition.nodes[1]!.params.prompt).toBe("${input.prompt}");
  expect(definition.edges).toHaveLength(2);
  expect(api.publishWorkflow).toHaveBeenCalledWith("helper", 1);
  expect(api.startRun).toHaveBeenCalledWith(
    "helper",
    { prompt: expect.stringContaining("${input.name}") },
    1,
  );
  expect(w.text()).toContain("将 JSON 放入编辑区");
  expect(editor.nodes).toEqual([]);
  w.unmount();
});
