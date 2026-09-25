import { beforeEach, describe, expect, it, vi } from "vitest";

type Handler = ((ev: { data: string }) => void) | null;

class MockWebSocket {
  static OPEN = 1;
  static CLOSED = 3;
  static instances: MockWebSocket[] = [];

  readyState = 0;
  sent: string[] = [];
  onopen: (() => void) | null = null;
  onmessage: Handler = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;

  constructor(public url: string) {
    MockWebSocket.instances.push(this);
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(): void {
    this.readyState = MockWebSocket.CLOSED;
    this.onclose?.();
  }

  open(): void {
    this.readyState = MockWebSocket.OPEN;
    this.onopen?.();
  }

  receive(msg: unknown): void {
    this.onmessage?.({ data: JSON.stringify(msg) });
  }

  lastSent(): { id: number; method: string; params: unknown } {
    return JSON.parse(this.sent[this.sent.length - 1]);
  }
}

async function freshClient() {
  vi.resetModules();
  const mod = await import("./client");
  return mod.client;
}

describe("RpcClient：请求/响应匹配与订阅映射", () => {
  beforeEach(() => {
    MockWebSocket.instances = [];
    vi.stubGlobal("WebSocket", MockWebSocket);
  });

  it("call 等待连接建立后发送，按 id 匹配响应", async () => {
    const client = await freshClient();
    const p = client.call("workflow.list", {});
    const ws = MockWebSocket.instances[0];
    ws.open();
    await vi.waitFor(() => expect(ws.sent).toHaveLength(1));
    const req = ws.lastSent();
    expect(req.method).toBe("workflow.list");
    ws.receive({ jsonrpc: "2.0", id: req.id, result: { workflows: [] } });
    await expect(p).resolves.toEqual({ workflows: [] });
  });

  it("错误响应 reject 为 RpcError", async () => {
    const client = await freshClient();
    const p = client.call("workflow.get", { workflow_id: "nope" });
    const ws = MockWebSocket.instances[0];
    ws.open();
    await vi.waitFor(() => expect(ws.sent).toHaveLength(1));
    ws.receive({
      jsonrpc: "2.0",
      id: ws.lastSent().id,
      error: { code: -32011, message: "工作流不存在" },
    });
    const err = (await p.catch((e: unknown) => e)) as { name: string; code: number };
    // freshClient 走 resetModules 动态导入，RpcError 类与静态导入非同一份，按 name/code 断言
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe("RpcError");
    expect(err.code).toBe(-32011);
  });

  it("并发请求各自按 id 匹配，乱序响应不错位", async () => {
    const client = await freshClient();
    const p1 = client.call("m1", {});
    const p2 = client.call("m2", {});
    const ws = MockWebSocket.instances[0];
    ws.open();
    await vi.waitFor(() => expect(ws.sent).toHaveLength(2));
    const id2 = JSON.parse(ws.sent[1]).id;
    const id1 = JSON.parse(ws.sent[0]).id;
    ws.receive({ jsonrpc: "2.0", id: id2, result: "second" });
    ws.receive({ jsonrpc: "2.0", id: id1, result: "first" });
    await expect(p1).resolves.toBe("first");
    await expect(p2).resolves.toBe("second");
  });

  it("订阅：响应 result 是服务端订阅 id，通知按 subscription 映射回本地回调", async () => {
    const client = await freshClient();
    const events: unknown[] = [];
    const unsubPromise = client.subscribe("run.subscribe", { run_id: "r1" }, (e) => events.push(e));
    const ws = MockWebSocket.instances[0];
    ws.open();
    // 订阅在连接建立前发起：onopen 重建与 subscribe 自身的续发必须去重，只发一条订阅请求
    await vi.waitFor(() => expect(ws.sent).toHaveLength(1));
    ws.receive({ jsonrpc: "2.0", id: ws.lastSent().id, result: "srv-sub-1" });
    const unsubscribe = await unsubPromise;

    ws.receive({
      jsonrpc: "2.0",
      method: "run.event",
      params: { subscription: "srv-sub-1", result: { seq: 1, type: "run_started" } },
    });
    // 其他订阅 id 的通知不串台
    ws.receive({
      jsonrpc: "2.0",
      method: "run.event",
      params: { subscription: "srv-sub-2", result: { seq: 99 } },
    });
    expect(events).toEqual([{ seq: 1, type: "run_started" }]);

    // 退订：jsonrpsee 约定位置参数 [subscription id]，方法名 subscribe → unsubscribe
    const unsubDone = unsubscribe();
    await vi.waitFor(() => expect(ws.sent).toHaveLength(2));
    const unsubReq = ws.lastSent();
    expect(unsubReq.method).toBe("run.unsubscribe");
    expect(unsubReq.params).toEqual(["srv-sub-1"]);
    ws.receive({ jsonrpc: "2.0", id: unsubReq.id, result: true });
    await unsubDone;
  });

  it("断线重连后订阅只重建一次", async () => {
    const client = await freshClient();
    const subPromise = client.subscribe("run.subscribe", { run_id: "r1" }, () => {});
    const ws = MockWebSocket.instances[0];
    ws.open();
    await vi.waitFor(() => expect(ws.sent).toHaveLength(1));
    ws.receive({ jsonrpc: "2.0", id: ws.lastSent().id, result: "srv-sub-1" });
    await subPromise;

    // 断线 → 自动重连：新连接上重建订阅，恰好一条
    ws.close();
    await vi.waitFor(() => expect(MockWebSocket.instances).toHaveLength(2));
    const ws2 = MockWebSocket.instances[1];
    ws2.open();
    await vi.waitFor(() => expect(ws2.sent).toHaveLength(1));
    expect(ws2.lastSent().method).toBe("run.subscribe");
    // 稍等一拍，确认没有叠加的第二条
    await new Promise((r) => setTimeout(r, 20));
    expect(ws2.sent).toHaveLength(1);
  });
});
