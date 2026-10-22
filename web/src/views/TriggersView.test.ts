import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";
import * as api from "../api/flow";
import { sha256Hex } from "../state/invocation";
import { confirmDialog } from "../state/modal";
import { toast } from "../state/toast";
import type { Schedule, VersionMeta, Webhook } from "../types";
import TriggersView from "./TriggersView.vue";
import { ensureNodeTypes } from "../state/editor";
import { discoverInputKeys } from "../state/run-input";

vi.mock("../api/flow", () => ({
  listSchedules: vi.fn(),
  listWebhooks: vi.fn(),
  getWorkflow: vi.fn(),
  createSchedule: vi.fn(),
  updateSchedule: vi.fn(),
  deleteSchedule: vi.fn(),
  createWebhook: vi.fn(),
  setWebhookEnabled: vi.fn(),
  deleteWebhook: vi.fn(),
  listVersions: vi.fn(),
  updateWebhookInvoke: vi.fn(),
  webhookBase: () => "http://127.0.0.1:9801",
}));
vi.mock("../rpc/client", () => ({
  client: { onReconnect: vi.fn(() => () => {}) },
  errText: (e: unknown) => (e instanceof Error ? e.message : String(e)),
}));
vi.mock("../state/editor", () => ({
  editor: { workflows: [], nodeTypes: [] },
  refreshWorkflows: vi.fn(async () => {}),
  ensureNodeTypes: vi.fn(async () => false),
}));
vi.mock("../state/run-input", () => ({ discoverInputKeys: vi.fn(() => []) }));
vi.mock("../state/modal", () => ({ confirmDialog: vi.fn(async () => true) }));
vi.mock("../state/toast", () => ({ toast: { info: vi.fn(), success: vi.fn(), error: vi.fn() } }));
vi.mock("../components/WorkflowInputForm.vue", () => ({ default: { template: "<div />" } }));

function schedule(id: string, enabled = true, input: unknown = null): Schedule {
  return {
    id,
    workflow_id: "wf-1",
    cron_expr: `*/5 * * * * #${id}`,
    input,
    enabled,
    created_at: "2026-10-18T00:00:00Z",
    next_fire_at: null,
  };
}

function webhook(token: string, extra: Partial<Webhook> = {}): Webhook {
  return {
    token,
    workflow_id: "wf-1",
    enabled: true,
    created_at: "2026-10-18T00:00:00Z",
    ...extra,
  };
}

function version(n: number, status: string): VersionMeta {
  return {
    workflow_id: "wf-1",
    version: n,
    status,
    checksum: "c".repeat(64),
    created_at: "2026-10-18T00:00:00Z",
  };
}

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

const wrappers: ReturnType<typeof mount>[] = [];
function create() {
  const wrapper = mount(TriggersView, {
    props: { workflowId: "wf-1" },
    global: { stubs: { RouterLink: { props: ["to"], template: "<a><slot /></a>" } } },
  });
  wrappers.push(wrapper);
  return wrapper;
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(ensureNodeTypes).mockResolvedValue(false);
  vi.mocked(api.listSchedules).mockResolvedValue([schedule("s1")]);
  vi.mocked(api.listWebhooks).mockResolvedValue([]);
  vi.mocked(api.listVersions).mockResolvedValue([]);
  vi.mocked(api.updateWebhookInvoke).mockResolvedValue({});
  vi.mocked(confirmDialog).mockResolvedValue(true);
});

afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount());
});

describe("triggers first load", () => {
  it("旧工作流仍在刷新时立即加载新工作流，迟到响应不能覆盖新列表", async () => {
    const old = deferred<Schedule[]>();
    vi.mocked(api.listSchedules).mockImplementation((id) =>
      id === "wf-1" ? old.promise : Promise.resolve([schedule("new")]),
    );
    const w = create();
    await w.setProps({ workflowId: "wf-2" });
    await flushPromises();
    expect(api.listSchedules).toHaveBeenCalledWith("wf-2");
    expect(w.text()).toContain("#new");
    old.resolve([schedule("old")]);
    await flushPromises();
    expect(w.text()).toContain("#new");
    expect(w.text()).not.toContain("#old");
  });

  it("创建旧工作流的调度完成后不清空新工作流的表单", async () => {
    const pending = deferred<Schedule>();
    vi.mocked(api.createSchedule).mockReturnValueOnce(pending.promise);
    const w = create();
    await flushPromises();
    await w.get(".cron-input").setValue("*/5 * * * *");
    await w.get(".create-form").trigger("submit");
    await w.setProps({ workflowId: "wf-2" });
    await w.get(".cron-input").setValue("*/10 * * * *");
    pending.resolve(schedule("created"));
    await flushPromises();
    expect((w.get(".cron-input").element as HTMLInputElement).value).toBe("*/10 * * * *");
  });

  it("调度输入字段取已发布定义，避免读到尚未发布的草稿", async () => {
    vi.mocked(ensureNodeTypes).mockResolvedValue(true);
    const draft = {
      nodes: [],
      edges: [],
      input_schema: { properties: { draft: { type: "string" } } },
    };
    const published = {
      nodes: [],
      edges: [],
      input_schema: { properties: { live: { type: "string" } } },
    };
    vi.mocked(api.getWorkflow).mockImplementation(async (_id, version) => ({
      workflow_id: "wf-1",
      version: version ?? 2,
      status: "draft",
      published_version: 1,
      definition: version === 1 ? published : draft,
    }));
    const w = create();
    await flushPromises();
    expect(api.getWorkflow).toHaveBeenCalledWith("wf-1", 1);
    expect(discoverInputKeys).toHaveBeenLastCalledWith(published, []);
    expect(w.exists()).toBe(true);
  });
  it("首拉失败显示错误条而不是「暂无调度」，重试成功后恢复空态", async () => {
    vi.mocked(api.listSchedules).mockRejectedValueOnce(new Error("offline"));
    const w = create();
    await flushPromises();

    expect(w.find('[role="alert"]').text()).toContain("offline");
    expect(w.text()).not.toContain("暂无调度");
    expect(w.text()).not.toContain("暂无 webhook");
    expect(toast.error).toHaveBeenCalledTimes(1);

    vi.mocked(api.listSchedules).mockResolvedValue([]);
    await w.find('[role="alert"] button').trigger("click");
    await flushPromises();

    expect(w.find('[role="alert"]').exists()).toBe(false);
    expect(w.text()).toContain("暂无调度");
    expect(w.text()).toContain("暂无 webhook");
  });
});

describe("triggers 10s polling", () => {
  it("按 10s 轮询；失败只在 ok→error 跳变时 toast，且保留上一次数据", async () => {
    const intervalSpy = vi.spyOn(globalThis, "setInterval");
    const w = create();
    await flushPromises();
    expect(w.find('[role="alert"]').exists()).toBe(false);

    const timer = intervalSpy.mock.calls.find((c) => c[1] === 10_000);
    expect(timer).toBeDefined();
    const tick = timer![0] as () => void;

    vi.mocked(api.listSchedules).mockRejectedValue(new Error("offline"));
    tick();
    await flushPromises();
    expect(w.find('[role="alert"]').text()).toContain("offline");
    expect(w.text()).toContain("*/5 * * * * #s1"); // 旧数据保留
    expect(toast.error).toHaveBeenCalledTimes(1);

    tick();
    await flushPromises();
    expect(toast.error).toHaveBeenCalledTimes(1); // 同一错误期不重复弹

    vi.mocked(api.listSchedules).mockResolvedValue([schedule("s1")]);
    tick();
    await flushPromises();
    expect(w.find('[role="alert"]').exists()).toBe(false);

    vi.mocked(api.listSchedules).mockRejectedValue(new Error("offline"));
    tick();
    await flushPromises();
    expect(toast.error).toHaveBeenCalledTimes(2); // 新的 ok→error 跳变再提示一次
    intervalSpy.mockRestore();
  });
});

describe("triggers row guards", () => {
  it("不同行的启停可并发，同一行在途时禁用本行按钮", async () => {
    vi.mocked(api.listSchedules).mockResolvedValue([schedule("s1"), schedule("s2")]);
    const first = deferred<void>();
    const second = deferred<void>();
    vi.mocked(api.updateSchedule)
      .mockImplementationOnce(() => first.promise)
      .mockImplementationOnce(() => second.promise);

    const w = create();
    await flushPromises();
    const toggles = w.findAll("button").filter((b) => b.text() === "停用");
    expect(toggles).toHaveLength(2);

    await toggles[0].trigger("click");
    expect(api.updateSchedule).toHaveBeenCalledTimes(1);
    expect(toggles[0].attributes("disabled")).toBeDefined();
    expect(toggles[1].attributes("disabled")).toBeUndefined();

    // 旧实现会在第一行在途时静默吞掉第二行点击
    await toggles[1].trigger("click");
    expect(api.updateSchedule).toHaveBeenCalledTimes(2);
    expect(vi.mocked(api.updateSchedule).mock.calls[1]).toEqual(["s2", { enabled: false }]);

    first.resolve();
    await flushPromises();
    second.resolve();
    await flushPromises();
  });

  it("删除中禁用本行删除按钮，防重复请求", async () => {
    const pending = deferred<void>();
    vi.mocked(api.deleteSchedule).mockImplementationOnce(() => pending.promise);
    const w = create();
    await flushPromises();

    const deletes = w.findAll("button").filter((b) => b.text() === "删除");
    expect(deletes).toHaveLength(1);
    await deletes[0].trigger("click");
    await flushPromises();
    expect(api.deleteSchedule).toHaveBeenCalledTimes(1);
    expect(
      w
        .findAll("button")
        .filter((b) => b.text() === "删除")[0]!
        .attributes("disabled"),
    ).toBeDefined();

    pending.resolve();
    await flushPromises();
    expect(
      w
        .findAll("button")
        .filter((b) => b.text() === "删除")[0]!
        .attributes("disabled"),
    ).toBeUndefined();
  });
});

function buttonByText(w: ReturnType<typeof mount>, text: string) {
  return w.findAll("button").find((b) => b.text() === text);
}

describe("triggers 调度编辑", () => {
  it("「编辑」保存只传改动项：改 cron 不带 input（undefined=不改），成功后刷新并 toast", async () => {
    const w = create();
    await flushPromises();

    await buttonByText(w, "编辑")!.trigger("click");
    await w.find(".edit-cron").setValue("*/10 * * * *");
    await buttonByText(w, "保存")!.trigger("click");
    await flushPromises();

    expect(api.updateSchedule).toHaveBeenCalledTimes(1);
    expect(vi.mocked(api.updateSchedule).mock.calls[0]).toEqual(["s1", { cron: "*/10 * * * *" }]);
    // input 未改：不传字段即「不改」，绝不能误传 null 清掉存量输入
    expect(vi.mocked(api.updateSchedule).mock.calls[0]![1]).not.toHaveProperty("input");
    expect(toast.success).toHaveBeenCalledWith("调度已更新");
    expect(api.listSchedules).toHaveBeenCalledTimes(2); // 保存成功后刷新列表
    expect(w.find(".edit-row").exists()).toBe(false); // 编辑行已收起
  });

  it("清空 input 发 null（清空语义），改成新 JSON 发替换值", async () => {
    vi.mocked(api.listSchedules).mockResolvedValue([schedule("s1", true, { a: 1 })]);
    const w = create();
    await flushPromises();

    await buttonByText(w, "编辑")!.trigger("click");
    const inputArea = w.find(".edit-input");
    expect((inputArea.element as HTMLTextAreaElement).value).toBe('{"a":1}');
    await inputArea.setValue("");
    await buttonByText(w, "保存")!.trigger("click");
    await flushPromises();
    expect(vi.mocked(api.updateSchedule).mock.calls[0]).toEqual(["s1", { input: null }]);

    vi.mocked(api.updateSchedule).mockClear();
    await buttonByText(w, "编辑")!.trigger("click");
    await w.find(".edit-input").setValue('{"b":2}');
    await buttonByText(w, "保存")!.trigger("click");
    await flushPromises();
    expect(vi.mocked(api.updateSchedule).mock.calls[0]).toEqual(["s1", { input: { b: 2 } }]);
  });

  it("非法 JSON 前端拦截不发请求；没有改动也不发请求", async () => {
    vi.mocked(api.listSchedules).mockResolvedValue([schedule("s1", true, { a: 1 })]);
    const w = create();
    await flushPromises();

    await buttonByText(w, "编辑")!.trigger("click");
    await w.find(".edit-input").setValue("{");
    await buttonByText(w, "保存")!.trigger("click");
    await flushPromises();
    expect(api.updateSchedule).not.toHaveBeenCalled();
    expect(toast.error).toHaveBeenCalledWith("输入不是合法 JSON");
    expect(w.find(".edit-row").exists()).toBe(true); // 保留编辑现场

    await w.find(".edit-input").setValue('{"a":1}'); // 恢复原文再存：无改动
    await buttonByText(w, "保存")!.trigger("click");
    await flushPromises();
    expect(api.updateSchedule).not.toHaveBeenCalled();
    expect(toast.info).toHaveBeenCalledWith("没有改动");
  });
});

describe("triggers webhook 调用合同", () => {
  it("版本下拉只列已发布；生成密钥后保存 invoke_key_hash=sha256Hex(明文)，明文只显示一次", async () => {
    vi.mocked(api.listWebhooks).mockResolvedValue([webhook("t1")]);
    vi.mocked(api.listVersions).mockResolvedValue([
      version(3, "published"),
      version(2, "draft"),
      version(1, "published"),
    ]);
    const w = create();
    await flushPromises();

    const options = w.findAll("select.invoke-version option");
    expect(options.map((o) => o.attributes("value"))).toEqual(["", "3", "1"]);

    await w.find("select.invoke-version").setValue("3");
    await buttonByText(w, "生成密钥")!.trigger("click");
    const plain = w.find(".key-plain").text();
    expect(plain).toMatch(/^[0-9a-f]{64}$/);

    await buttonByText(w, "保存调用合同")!.trigger("click");
    // sha256Hex 走 WebCrypto 异步摘要，flushPromises 不一定等得到，用 waitFor 等落库
    await vi.waitFor(() => expect(api.updateWebhookInvoke).toHaveBeenCalledTimes(1));
    await flushPromises();

    expect(api.updateWebhookInvoke).toHaveBeenCalledTimes(1);
    const [token, config] = vi.mocked(api.updateWebhookInvoke).mock.calls[0]!;
    expect(token).toBe("t1");
    expect(config.invoke_version).toBe(3);
    expect(config.invoke_key_hash).toMatch(/^[0-9a-f]{64}$/);
    expect(config.invoke_key_hash).toBe(await sha256Hex(plain)); // 落库哈希=明文 SHA-256

    // 明文只显示一次：保存后消失，只剩哈希前缀 +「已设置」
    expect(w.find(".key-plain").exists()).toBe(false);
    expect(w.text()).toContain("密钥已设置");
    expect(w.text()).toContain(config.invoke_key_hash.slice(0, 12));
    expect(w.text()).toContain("已固定 v3");
    expect(toast.success).toHaveBeenCalledWith("调用合同已保存，立即生效");
  });

  it("粘贴入口：不足 32 字节拒绝，够长则进草稿明文", async () => {
    vi.mocked(api.listWebhooks).mockResolvedValue([webhook("t1")]);
    vi.mocked(api.listVersions).mockResolvedValue([version(2, "published")]);
    const w = create();
    await flushPromises();

    await w.find(".key-paste").setValue("short");
    await buttonByText(w, "使用")!.trigger("click");
    expect(w.find(".key-plain").exists()).toBe(false);
    expect(toast.error).toHaveBeenCalledWith("调用密钥至少 32 字节");

    const pasted = "p".repeat(40);
    await w.find(".key-paste").setValue(pasted);
    await buttonByText(w, "使用")!.trigger("click");
    expect(w.find(".key-plain").text()).toBe(pasted);
  });

  it("重设密钥覆盖旧键先 confirmDialog（danger），取消则不保存", async () => {
    vi.mocked(api.listWebhooks).mockResolvedValue([
      webhook("t1", { invoke_version: 1, invoke_key_hash: "a".repeat(64) }),
    ]);
    vi.mocked(api.listVersions).mockResolvedValue([version(1, "published")]);
    const w = create();
    await flushPromises();

    // 服务端回显时直接显示「已固定 vN / 密钥已设置」
    expect(w.text()).toContain("已固定 v1");
    expect(w.text()).toContain("密钥已设置");
    expect(w.text()).toContain("a".repeat(12));

    await buttonByText(w, "重设密钥")!.trigger("click");
    const plain = w.find(".key-plain").text();

    vi.mocked(confirmDialog).mockResolvedValueOnce(false);
    await buttonByText(w, "保存调用合同")!.trigger("click");
    await flushPromises();
    expect(confirmDialog).toHaveBeenCalledWith(
      expect.stringContaining("覆盖旧密钥"),
      expect.objectContaining({ danger: true }),
    );
    expect(api.updateWebhookInvoke).not.toHaveBeenCalled();
    expect(w.find(".key-plain").exists()).toBe(true); // 未确认时明文草稿保留

    vi.mocked(confirmDialog).mockResolvedValueOnce(true);
    await buttonByText(w, "保存调用合同")!.trigger("click");
    await vi.waitFor(() => expect(api.updateWebhookInvoke).toHaveBeenCalledTimes(1));
    await flushPromises();
    expect(api.updateWebhookInvoke).toHaveBeenCalledWith("t1", {
      invoke_version: 1,
      invoke_key_hash: await sha256Hex(plain),
    });
  });

  it("列表不回显时提示「保存后立即生效，列表接口暂不回显」，并给出 202/幂等/run_token 合同文案", async () => {
    vi.mocked(api.listWebhooks).mockResolvedValue([webhook("t1")]);
    vi.mocked(api.listVersions).mockResolvedValue([version(2, "published")]);
    const w = create();
    await flushPromises();

    expect(w.text()).toContain("列表接口暂不回显");
    expect(w.text()).toContain("未固定版本");
    expect(w.text()).toContain("密钥未设置");
    expect(w.text()).toContain("202 只表示受理");
    expect(w.text()).toContain("Idempotency-Key");
    expect(w.text()).toContain("run_token 只绑定该次 run");
    expect(w.text()).toContain("/invocations/"); // resultCurl 结果查询示例
  });
});
