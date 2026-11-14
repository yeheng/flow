import { beforeEach, afterEach, expect, it, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import ResidentServicesPanel from "./ResidentServicesPanel.vue";
import { client } from "../rpc/client";
vi.mock("../rpc/client", () => ({
  client: { call: vi.fn(), subscribe: vi.fn(async () => async () => {}) },
  errText: (e: unknown) => String(e),
}));
vi.mock("../config", () => ({ httpUrl: () => "http://localhost:9803" }));
class Port {
  onmessage: ((e: MessageEvent) => void) | null = null;
  postMessage = vi.fn();
  close = vi.fn();
}
let ports: Port[] = [];
beforeEach(() => {
  vi.stubGlobal(
    "MessageChannel",
    class {
      port1 = new Port();
      port2 = new Port();
      constructor() {
        ports.push(this.port1);
      }
    },
  );
  ports = [];
  vi.mocked(client.call).mockReset();
  vi.mocked(client.call).mockImplementation(async (method) => {
    if (method === "services.list")
      return {
        services: [{ plugin_id: "echo", instance: "instance-1", alive: true, ui: "ui-id" }],
      } as never;
    return { value: { ok: true } } as never;
  });
});
afterEach(() => {
  vi.unstubAllGlobals();
});
it("binds calls to the selected instance and destroys the bridge when closed", async () => {
  const w = mount(ResidentServicesPanel, { attachTo: document.body });
  await flushPromises();
  await w
    .findAll("button")
    .find((b) => b.text() === "打开界面")!
    .trigger("click");
  const iframe = w.find("iframe");
  expect(iframe.attributes("sandbox")).toBe("allow-scripts");
  await iframe.trigger("load");
  ports[0]!.onmessage!({
    data: {
      id: "q",
      method: "echo",
      plugin_id: "other",
      instance: "forged",
      params: { text: "hi" },
    },
  } as MessageEvent);
  await flushPromises();
  expect(client.call).toHaveBeenCalledWith("services.call", {
    plugin_id: "echo",
    instance: "instance-1",
    method: "echo",
    params: { text: "hi" },
  });
  expect(ports[0]!.postMessage).toHaveBeenCalledWith({ id: "q", result: { ok: true } });
  await w
    .findAll("button")
    .find((b) => b.text() === "关闭界面")!
    .trigger("click");
  expect(ports[0]!.close).toHaveBeenCalled();
  vi.mocked(client.call).mockClear();
  ports[0]!.onmessage!({ data: { id: "late", method: "echo" } } as MessageEvent);
  await flushPromises();
  expect(client.call).not.toHaveBeenCalled();
  w.unmount();
});
it("rejects oversized page requests without calling the plugin", async () => {
  const w = mount(ResidentServicesPanel, { attachTo: document.body });
  await flushPromises();
  await w
    .findAll("button")
    .find((b) => b.text() === "打开界面")!
    .trigger("click");
  await w.find("iframe").trigger("load");
  vi.mocked(client.call).mockClear();
  ports[0]!.onmessage!({
    data: { id: "big", method: "echo", params: "x".repeat(65536) },
  } as MessageEvent);
  await flushPromises();
  expect(client.call).not.toHaveBeenCalled();
  expect(ports[0]!.postMessage).toHaveBeenCalledWith(
    expect.objectContaining({ id: "big", error: expect.any(String) }),
  );
  w.unmount();
});
