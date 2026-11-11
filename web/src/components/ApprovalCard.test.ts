import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flushPromises, mount } from "@vue/test-utils";

vi.mock("../api/flow", () => ({
  runApprove: vi.fn(async () => ({ result: { decision: "allowed" } })),
  runDeny: vi.fn(async () => ({ result: { decision: "denied" } })),
}));
vi.mock("../rpc/client", () => ({
  // 只用到 RpcError 的 code 判别；给一个最小实现
  RpcError: class RpcError extends Error {
    constructor(
      public code: number,
      message: string,
    ) {
      super(message);
    }
  },
}));
vi.mock("../state/toast", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() },
}));

import ApprovalCard from "./ApprovalCard.vue";
import * as api from "../api/flow";
import { RpcError } from "../rpc/client";
import { toast } from "../state/toast";
import type { ApprovalItem } from "../types";

function item(over: Partial<ApprovalItem> = {}): ApprovalItem {
  return {
    run_id: "run-1",
    workflow_id: "wf-1",
    workflow_version: 3,
    node_id: "http_1",
    gate: "tool",
    status: "pending",
    operation_id: "op-1",
    request_hash: "hash-1",
    actor: { kind: "node", run_id: "run-1", node_id: "http_1" },
    specs: [],
    context: {
      run_id: "run-1",
      workflow_id: "wf-1",
      workflow_version: 3,
      node_id: "http_1",
      node_type: "http_call",
      dispatch_id: "d-1",
      attempt: 1,
      request_fingerprint: "fp-1",
    },
    policy_digest: "pd-1",
    matched: ["workflow:net-https"],
    asked_at: Date.now(),
    deadline: Date.now() + 60_000,
    decision: null,
    request_preview: null,
    ...over,
  };
}

const wrappers: ReturnType<typeof mount>[] = [];
function create(props: { item: ApprovalItem }) {
  const w = mount(ApprovalCard, { props });
  wrappers.push(w);
  return w;
}

beforeEach(() => {
  vi.clearAllMocks();
});

afterEach(() => {
  wrappers.splice(0).forEach((w) => w.unmount());
});

describe("ApprovalCard：spec 渲染", () => {
  it("net 渲染 METHOD URL 两段等宽文本（不拼 HTML）", () => {
    const w = create({
      item: item({
        specs: [
          {
            action: "net",
            method: "POST",
            url: "https://api.example.com/pay",
            headers_digest: "aa11",
            body_digest: "bb22",
          },
        ],
      }),
    });
    const codes = w.findAll(".spec-body code").map((c) => c.text());
    expect(codes).toEqual(["POST", "https://api.example.com/pay"]);
    // 摘要字段以小字 meta 展示（截断显示）
    expect(w.find(".spec-meta").text()).toContain("headers_digest");
    expect(w.text()).not.toContain("<");
  });

  it("exec 的 argv 逐项渲染（数组多少项就多少个 code，不拼 shell 字符串）", () => {
    const w = create({
      item: item({
        specs: [
          { action: "exec", executable: "/bin/tar", argv: ["czf", "out.tgz", "dir with space"], cwd: "/tmp", env_digest: "ee" },
        ],
      }),
    });
    const codes = w.findAll(".spec-body code").map((c) => c.text());
    expect(codes).toEqual(["/bin/tar", "czf", "out.tgz", "dir with space"]);
    // 含空格的参数是一个完整项，没有被 shell 引号拼接
    expect(codes[3]).toBe("dir with space");
    expect(w.find(".spec-meta").text()).toContain("cwd");
  });

  it("shell 渲染解释器 + 脚本摘要 sha256:前16位…", () => {
    const w = create({
      item: item({
        specs: [
          { action: "shell", executable: "/bin/bash", script_digest: "a".repeat(64) },
        ],
      }),
    });
    expect(w.findAll(".spec-body code").map((c) => c.text())).toEqual([
      "/bin/bash",
      `sha256:${"a".repeat(16)}…`,
    ]);
  });

  it("secret 只渲染密钥名，绝不出现密钥值", () => {
    const w = create({
      item: item({ specs: [{ action: "secret", secret_name: "PAY_TOKEN" }] }),
    });
    expect(w.find(".spec-body").text()).toContain("PAY_TOKEN");
    expect(w.text()).not.toContain("sk-live-secret");
  });

  it("llm_call / send_email / file_* 各按闭集字段渲染", () => {
    const w = create({
      item: item({
        specs: [
          { action: "llm_call", provider: "openai", model: "gpt-4o", payload_digest: "p1" },
          { action: "send_email", recipients: ["a@x.com", "b@x.com"] },
          { action: "file_write", path: "out/report.md", content_digest: "c1" },
        ],
      }),
    });
    expect(w.text()).toContain("openai");
    expect(w.text()).toContain("gpt-4o");
    expect(w.text()).toContain("a@x.com");
    expect(w.text()).toContain("b@x.com");
    expect(w.text()).toContain("out/report.md");
    // 动作中文标签
    expect(w.text()).toContain("模型调用");
    expect(w.text()).toContain("发邮件");
    expect(w.text()).toContain("写文件");
  });
});

describe("ApprovalCard：request_preview", () => {
  it("有界预览用只读 <details><pre> 展示 JSON", () => {
    const w = create({ item: item({ request_preview: { url: "https://x", body: { k: 1 } } }) });
    expect(w.find("details pre").exists()).toBe(true);
    expect(w.find("details pre").text()).toContain('"k": 1');
  });

  it("truncated 预览只显示字节数提示，不渲染正文", () => {
    const w = create({ item: item({ request_preview: { truncated: true, bytes: 123456 } }) });
    expect(w.find("details pre").exists()).toBe(false);
    expect(w.text()).toContain("123456 字节");
  });
});

describe("ApprovalCard：人工决定", () => {
  it("pending 默认 scope=once 批准；身份字段（operation_id/request_hash）原样回传", async () => {
    const w = create({ item: item() });
    await w.findAll("button").find((b) => b.text() === "批准")!.trigger("click");
    await flushPromises();
    expect(api.runApprove).toHaveBeenCalledWith("run-1", "http_1", "op-1", "hash-1", "once");
    expect(w.emitted("resolved")).toHaveLength(1);
  });

  it("scope 切到 run 后批准携带 run；帮助文案说明 Run 的语义", async () => {
    const w = create({ item: item() });
    expect(w.text()).toContain("Run 仅免除相同操作的再次询问");
    await w.find("select").setValue("run");
    await w.findAll("button").find((b) => b.text() === "批准")!.trigger("click");
    await flushPromises();
    expect(api.runApprove).toHaveBeenCalledWith("run-1", "http_1", "op-1", "hash-1", "run");
  });

  it("拒绝理由必填：空理由按钮禁用，填写后调用 run.deny", async () => {
    const w = create({ item: item() });
    const deny = w.findAll("button").find((b) => b.text() === "拒绝")!;
    expect(deny.attributes("disabled")).toBeDefined();
    await w.find("input").setValue("金额超限");
    await w.findAll("button").find((b) => b.text() === "拒绝")!.trigger("click");
    await flushPromises();
    expect(api.runDeny).toHaveBeenCalledWith("run-1", "http_1", "op-1", "hash-1", "金额超限");
    expect(w.emitted("resolved")).toHaveLength(1);
  });

  it("-32012（已裁决/实例变化）：提示后 emit resolved 刷新收件箱", async () => {
    vi.mocked(api.runApprove).mockRejectedValueOnce(new RpcError(-32012, "already decided"));
    const w = create({ item: item() });
    await w.findAll("button").find((b) => b.text() === "批准")!.trigger("click");
    await flushPromises();
    expect(toast.info).toHaveBeenCalled();
    expect(w.emitted("resolved")).toHaveLength(1);
    expect(toast.error).not.toHaveBeenCalled();
  });

  it("-32020 同样触发刷新（命令未生效也要对齐最新状态）", async () => {
    vi.mocked(api.runDeny).mockRejectedValueOnce(new RpcError(-32020, "permission state changed"));
    const w = create({ item: item() });
    await w.find("input").setValue("no");
    await w.findAll("button").find((b) => b.text() === "拒绝")!.trigger("click");
    await flushPromises();
    expect(w.emitted("resolved")).toHaveLength(1);
  });

  it("非 pending 只读：不渲染操作区，展示既有决定", () => {
    const w = create({
      item: item({
        status: "denied",
        decision: {
          decision: "deny",
          by: { kind: "user", label: "ops" },
          reason: "金额超限",
          matched_capabilities: [],
          evaluated_at: "2026-11-11T00:00:00Z",
        },
      }),
    });
    expect(w.find("select").exists()).toBe(false);
    expect(w.find("input").exists()).toBe(false);
    expect(w.text()).toContain("已拒绝");
    expect(w.text()).toContain("金额超限");
  });
});
