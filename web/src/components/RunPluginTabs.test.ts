import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import RunPluginTabs from "./RunPluginTabs.vue";
import { client } from "../rpc/client";
vi.mock("../rpc/client", () => ({
  client: { call: vi.fn(), subscribe: vi.fn(async () => async () => {}) },
  errText: String,
}));
vi.mock("../config", () => ({ httpUrl: () => "http://localhost:9803" }));
beforeEach(() => {
  vi.useFakeTimers();
  vi.mocked(client.call).mockResolvedValue({
    services: [
      {
        plugin_id: "plugin-a",
        instance: "a",
        alive: true,
        ui: "a-ui",
        binding: { run_id: "run-a", node_id: "q" },
      },
      {
        plugin_id: "plugin-b",
        instance: "b",
        alive: true,
        ui: "b-ui",
        binding: { run_id: "run-b", node_id: "q" },
      },
    ],
  });
});
afterEach(() => {
  vi.useRealTimers();
});
it("renders only the bound run's plugin and switches back to the host view", async () => {
  const w = mount(RunPluginTabs, { props: { runId: "run-a" } });
  await flushPromises();
  expect(w.text()).toContain("plugin-a");
  expect(w.text()).not.toContain("plugin-b");
  await w.findAll('[role="tab"]')[1]!.trigger("click");
  expect(w.emitted("active")?.slice(-1)[0]).toEqual([true]);
  expect(w.find("iframe").attributes("src")).toContain("a-ui");
  await w.findAll('[role="tab"]')[0]!.trigger("click");
  expect(w.find("iframe").exists()).toBe(false);
  expect(w.emitted("active")?.slice(-1)[0]).toEqual([false]);
  w.unmount();
});
it("drops the previous frame when navigating to another run", async () => {
  const w = mount(RunPluginTabs, { props: { runId: "run-a" } });
  await flushPromises();
  await w.findAll('[role="tab"]')[1]!.trigger("click");
  await w.setProps({ runId: "run-b" });
  await flushPromises();
  expect(w.find("iframe").exists()).toBe(false);
  expect(w.text()).not.toContain("plugin-a");
  expect(w.text()).toContain("plugin-b");
  expect(w.emitted("active")?.slice(-1)[0]).toEqual([false]);
  w.unmount();
});
