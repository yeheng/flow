import { flushPromises, shallowMount } from "@vue/test-utils";
import { expect, it, vi } from "vitest";
import EditorView from "./EditorView.vue";
import RawWorkflowEditor from "../components/RawWorkflowEditor.vue";
import AppButton from "../components/AppButton.vue";
import { editor } from "../state/editor";
import * as api from "../api/flow";

vi.mock("vue-router", () => ({
  useRoute: () => ({ params: { id: "raw-save" }, query: {} }),
  useRouter: () => ({}),
  onBeforeRouteLeave: vi.fn(),
  onBeforeRouteUpdate: vi.fn(),
}));
vi.mock("../api/flow", () => ({
  listSecrets: vi.fn(),
  listWorkflows: vi.fn(),
  updateWorkflow: vi.fn(),
  validateWorkflow: vi.fn(),
}));
vi.mock("../components/CodeEditor.vue", () => ({
  default: {
    props: ["modelValue"],
    emits: ["update:modelValue"],
    template: '<textarea :value="modelValue" @input="$emit(\'update:modelValue\', $event.target.value)" />',
  },
}));

it.each(["toolbar", "shortcut"])("saves a pending Raw JSON draft through the %s", async (entry) => {
  vi.clearAllMocks();
  editor.workflowId = "raw-save";
  editor.version = 0;
  editor.nodes = [];
  editor.edges = [];
  editor.definitionOptions = {};
  editor.nodeTypes = ["start", "end"].map((type) => ({
    type,
    label: type,
    category: "control",
    params_schema: { type: "object" },
    ports: type === "start" ? [{ id: "out", label: "out" }] : [{ id: "in", label: "in" }],
  }));
  vi.mocked(api.listWorkflows).mockResolvedValue([]);
  vi.mocked(api.listSecrets).mockResolvedValue([]);
  vi.mocked(api.validateWorkflow).mockResolvedValue({ valid: true, missing_secrets: [] });
  vi.mocked(api.updateWorkflow).mockResolvedValue(1);

  const w = shallowMount(EditorView, {
    global: { stubs: { RawWorkflowEditor, AppButton, CodeEditor: false, RouterLink: true } },
  });
  try {
    await w.findAll("button").find((button) => button.text() === "Raw JSON")!.trigger("click");
    await flushPromises();
    const definition = {
      nodes: [{ id: "start", type: "start", name: "Raw edit" }, { id: "end", type: "end" }],
      edges: [{ from: "start", to: "end" }],
    };
    await w.get(".json-pane textarea").setValue(JSON.stringify(definition));
    if (entry === "toolbar") {
      const button = w.get('button[title="保存 (⌘S)"]');
      expect(button.attributes("disabled")).toBeUndefined();
      await button.trigger("click");
    } else {
      window.dispatchEvent(new KeyboardEvent("keydown", { key: "s", metaKey: true }));
    }
    await flushPromises();
    expect(api.validateWorkflow).toHaveBeenCalledWith(definition);
    expect(api.updateWorkflow).toHaveBeenCalledWith("raw-save", expect.objectContaining({
      nodes: expect.arrayContaining([expect.objectContaining({ id: "start", name: "Raw edit" })]),
    }));
    expect(editor.version).toBe(1);
  } finally {
    w.unmount();
  }
});
