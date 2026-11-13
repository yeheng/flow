import { beforeEach, expect, it, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import EnvironmentSettings from "./EnvironmentSettings.vue";
import { environments, saveEnvironment } from "../state/environments";
vi.mock("../state/environments", async () => {
  const { ref } = await import("vue");
  return {
    environments: ref([]),
    selectedEnvironment: ref("default"),
    refreshEnvironments: vi.fn(async () => {}),
    saveEnvironment: vi.fn(async () => {}),
  };
});
vi.mock("../rpc/client", () => ({ client: { call: vi.fn() }, errText: (e: unknown) => String(e) }));
vi.mock("../state/modal", () => ({ confirmDialog: vi.fn(async () => true) }));
beforeEach(() => {
  vi.clearAllMocks();
  environments.value = [
    { id: "dev", name: "开发", revision: 2, config: { url: "dev" }, secrets: ["KEY"] },
  ];
});
it("copies config without copying secret values or names", async () => {
  const w = mount(EnvironmentSettings);
  await flushPromises();
  await w
    .findAll("button")
    .find((b) => b.text().includes("开发"))!
    .trigger("click");
  await w
    .findAll("button")
    .find((b) => b.text() === "复制配置")!
    .trigger("click");
  expect((w.findAll("input")[0]!.element as HTMLInputElement).value).toBe("");
  expect((w.find("textarea").element as HTMLTextAreaElement).value).toContain('"dev"');
  expect(w.text()).toContain("已配置密钥：无");
});
it("rejects non-object configuration before sending", async () => {
  const w = mount(EnvironmentSettings);
  await flushPromises();
  await w.find("textarea").setValue("[]");
  await w.find("form").trigger("submit");
  await flushPromises();
  expect(saveEnvironment).not.toHaveBeenCalled();
  expect(w.find('[role="alert"]').text()).toContain("JSON 对象");
});
it("sends expected revision and clears password after a failed save", async () => {
  vi.mocked(saveEnvironment).mockRejectedValueOnce(new Error("revision conflict"));
  const w = mount(EnvironmentSettings);
  await flushPromises();
  await w
    .findAll("button")
    .find((b) => b.text().includes("开发"))!
    .trigger("click");
  const inputs = w.findAll("input");
  await inputs[2]!.setValue("KEY");
  await inputs[3]!.setValue("private-value");
  await w.find("form").trigger("submit");
  await flushPromises();
  expect(saveEnvironment).toHaveBeenCalledWith(
    expect.objectContaining({
      id: "dev",
      revision: 2,
      secret: { name: "KEY", value: "private-value" },
    }),
  );
  expect((inputs[3]!.element as HTMLInputElement).value).toBe("");
  expect(w.find('[role="alert"]').text()).toContain("revision conflict");
});
