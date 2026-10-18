import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import * as api from "../api/flow";
import type { ConfigView, FlowConfig } from "../types";
import SettingsView from "./SettingsView.vue";

vi.mock("../api/flow", () => ({
  getConfig: vi.fn(),
  updateConfig: vi.fn(),
  listSecrets: vi.fn(async () => []),
  setSecret: vi.fn(),
  deleteSecret: vi.fn(),
}));
vi.mock("../rpc/client", () => ({
  errText: (e: unknown) => (e instanceof Error ? e.message : String(e)),
}));
vi.mock("../platform", () => ({ isDesktop: () => false }));
vi.mock("../config", () => ({
  apiToken: () => "",
  setApiToken: vi.fn(),
  rpcUrl: () => "http://127.0.0.1:9802",
}));
vi.mock("../state/editor", () => ({ refreshSecrets: vi.fn(async () => {}) }));
vi.mock("../state/modal", () => ({ confirmDialog: vi.fn(async () => true) }));
vi.mock("../state/toast", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() },
}));

function config(): FlowConfig {
  return {
    server: { scheduler_enabled: true, journal_trigger_tick_secs: 30 },
    storage: { backend: "journal", data_dir: "/data" },
    execution: {
      mode: "ipc",
      executor_bin: null,
      x_max: 4,
      prewarm: false,
      remote: {
        control_addr: "127.0.0.1:9811",
        ca_cert: "/ca.pem",
        cert: "/cert.pem",
        key: "/key.pem",
        attach_timeout_ms: 5000,
        data_addr: "legacy-addr",
        wss_addr: null,
        agents: [{ id: "a1", token: "t1" }],
      },
    },
    agent: {
      control_addr: null,
      agent_id: null,
      ca_cert: null,
      cert: null,
      key: null,
      executor_bin: null,
      slots: 2,
      data_addr: null,
      wss_url: null,
      token: null,
    },
    journal: {
      addr: "127.0.0.1:9802",
      http_addr: "127.0.0.1:9803",
      data_dir: null,
      cluster: null,
    },
  };
}

const wrappers: ReturnType<typeof mount>[] = [];
function create() {
  const wrapper = mount(SettingsView);
  wrappers.push(wrapper);
  return wrapper;
}

function field(w: ReturnType<typeof mount>, label: string) {
  const f = w.findAll(".field").find((el) => el.find("label").exists() && el.text().includes(label));
  if (!f) throw new Error(`找不到字段：${label}`);
  return f;
}

function buttonByText(w: ReturnType<typeof mount>, text: string) {
  const btn = w.findAll("button").find((b) => b.text().includes(text));
  if (!btn) throw new Error(`找不到按钮：${text}`);
  return btn;
}

function mockConfigView(cfg: FlowConfig): ConfigView {
  return { config: cfg, config_path: "/flow.toml", env_overrides: [] };
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.getConfig).mockResolvedValue(mockConfigView(config()));
  // 保存回显服务端写回的同一视图（这里直接以 patch 当写回结果）
  vi.mocked(api.updateConfig).mockImplementation(async (patch) =>
    mockConfigView(patch as FlowConfig),
  );
});

afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount());
});

describe("settings execution/agent fields", () => {
  it("prewarm 开关 round-trip：整包 config.update 带回开关值且不丢未编辑字段", async () => {
    const w = create();
    await flushPromises();

    const checkbox = field(w, "预热执行器").find("input[type=checkbox]");
    expect((checkbox.element as HTMLInputElement).checked).toBe(false);
    await checkbox.setValue(true);
    expect(field(w, "预热执行器").text()).toContain("开启");

    await buttonByText(w, "保存（重启后生效）").trigger("click");
    await flushPromises();

    expect(api.updateConfig).toHaveBeenCalledTimes(1);
    const patch = vi.mocked(api.updateConfig).mock.calls[0][0];
    expect(patch.execution?.prewarm).toBe(true);
    // patch 分区整节替换，必须整包带回：未编辑字段保持原值
    expect(patch.execution?.x_max).toBe(4);
    expect(patch.execution?.executor_bin).toBeNull();
    expect(patch.execution?.remote?.attach_timeout_ms).toBe(5000);
    expect(patch.execution?.remote?.agents).toEqual([{ id: "a1", token: "t1" }]);
    expect(patch.agent?.slots).toBe(2);
    expect(patch.journal?.addr).toBe("127.0.0.1:9802");
  });

  it("execution.remote.wss_addr 与 agents（id/token 行编辑 + 增删）随整包保存", async () => {
    const w = create();
    await flushPromises();

    await field(w, "WSS 接入地址").find("input").setValue("wss://127.0.0.1:9443");
    expect(w.findAll(".agent-row").length).toBe(1);
    await w.findAll(".agent-row")[0].findAll("input")[1].setValue("t1-new");

    await buttonByText(w, "添加凭据").trigger("click");
    expect(w.findAll(".agent-row").length).toBe(2);
    const added = w.findAll(".agent-row")[1].findAll("input");
    await added[0].setValue("a2");
    await added[1].setValue("t2");

    await w.findAll(".agent-row")[0].find("button").trigger("click");
    expect(w.findAll(".agent-row").length).toBe(1);

    await buttonByText(w, "保存（重启后生效）").trigger("click");
    await flushPromises();

    const patch = vi.mocked(api.updateConfig).mock.calls[0][0];
    expect(patch.execution?.remote?.wss_addr).toBe("wss://127.0.0.1:9443");
    expect(patch.execution?.remote?.agents).toEqual([{ id: "a2", token: "t2" }]);
  });

  it("agent.wss_url / token 可编辑；data_addr 标已废弃且只读（存量值不丢）", async () => {
    const w = create();
    await flushPromises();

    await field(w, "WSS 上联地址").find("input").setValue("wss://agent-up");
    await field(w, "WSS 接入令牌").find("input").setValue("agent-token");

    // remote.data_addr 与 agent.data_addr 两处只读提示
    const deprecated = w.findAll("input.deprecated");
    expect(deprecated.length).toBe(2);
    for (const el of deprecated) expect(el.attributes("disabled")).toBeDefined();
    expect(w.text()).toContain("已废弃，仅存量兼容");

    await buttonByText(w, "保存（重启后生效）").trigger("click");
    await flushPromises();

    const patch = vi.mocked(api.updateConfig).mock.calls[0][0];
    expect(patch.agent?.wss_url).toBe("wss://agent-up");
    expect(patch.agent?.token).toBe("agent-token");
    // 已废弃字段只读回显，随整包原样保留
    expect(patch.agent?.data_addr).toBeNull();
    expect(patch.execution?.remote?.data_addr).toBe("legacy-addr");
  });
});
