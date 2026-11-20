import { beforeEach, expect, it, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import EnvironmentSettings from "./EnvironmentSettings.vue";
import { environments, saveEnvironment } from "../state/environments";
import { formDialog } from "../state/modal";
import * as api from "../api/flow";

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
vi.mock("../state/modal", () => ({ confirmDialog: vi.fn(async () => true), formDialog: vi.fn() }));
vi.mock("../state/editor", () => ({ refreshSecrets: vi.fn() }));
vi.mock("../state/toast", () => ({ toast: { success: vi.fn() } }));
vi.mock("../api/flow", () => ({ listSecrets: vi.fn(), setSecret: vi.fn(), deleteSecret: vi.fn() }));

beforeEach(() => {
  vi.clearAllMocks();
  environments.value = [
    { id: "dev", name: "开发", revision: 2, config: { url: "dev", retries: 2 }, secrets: ["KEY"] },
  ];
  vi.mocked(api.listSecrets).mockResolvedValue([{ name: "PROCESS_KEY", source: "env" }]);
});

it("passes only configuration and creates a new revision-zero environment on copy", async () => {
  vi.mocked(formDialog).mockResolvedValue({ id: "copy", name: "开发副本" });
  const w = mount(EnvironmentSettings, { props: { mode: "environments" } });
  await flushPromises();
  await w
    .findAll("button")
    .find((b) => b.text() === "复制配置")!
    .trigger("click");
  await flushPromises();
  expect(saveEnvironment).toHaveBeenCalledWith({
    id: "copy",
    revision: 0,
    name: "开发副本",
    config: { url: "dev", retries: 2 },
  });
  w.unmount();
});

it("edits a variable with the loaded revision and preserves the neighboring entries", async () => {
  vi.mocked(formDialog).mockResolvedValue({ value: "7" });
  const w = mount(EnvironmentSettings, { props: { mode: "variables" } });
  await flushPromises();
  const row = w.findAll("tbody tr").find((row) => row.text().includes("retries"))!;
  await row
    .findAll("button")
    .find((b) => b.text() === "编辑")!
    .trigger("click");
  await flushPromises();
  expect(saveEnvironment).toHaveBeenCalledWith({
    id: "dev",
    revision: 2,
    name: "开发",
    config: { url: "dev", retries: 7 },
  });
  w.unmount();
});

it("keeps deployment secrets read-only and rotates custom secrets without prefilled values", async () => {
  vi.mocked(formDialog).mockResolvedValue({ value: "test-rotation-value" });
  vi.mocked(saveEnvironment).mockRejectedValueOnce(new Error("environment revision conflict"));
  const w = mount(EnvironmentSettings, { props: { mode: "secrets" } });
  await flushPromises();
  expect(w.get("tbody tr").text()).toContain("由部署管理");
  expect(w.get("tbody tr").find("button").exists()).toBe(false);
  await w.get("select").setValue("dev");
  await w.get("tbody tr button").trigger("click");
  await flushPromises();
  expect(formDialog).toHaveBeenCalledWith(
    "更新密钥「KEY」",
    [expect.objectContaining({ key: "value", secret: true })],
    expect.not.objectContaining({ values: expect.anything() }),
  );
  expect(saveEnvironment).toHaveBeenCalledWith({
    id: "dev",
    revision: 2,
    name: "开发",
    config: { url: "dev", retries: 2 },
    secret: { name: "KEY", value: "test-rotation-value" },
  });
  expect(w.text()).not.toContain("test-rotation-value");
  expect(w.get('[role="alert"]').text()).toContain("revision conflict");
  w.unmount();
});
