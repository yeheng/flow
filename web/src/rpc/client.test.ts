import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

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

  /**
   * 只置状态、不回调 onclose——模拟「旧 socket 的 onclose 在新 socket 就位
   * 之后才姗姗来迟」。真实浏览器会这样；同步触发 onclose 的 close() 测不出来。
   */
  closeSilently(): void {
    this.readyState = MockWebSocket.CLOSED;
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

describe("RpcClient：重连生命周期", () => {
  beforeEach(() => {
    MockWebSocket.instances = [];
    vi.stubGlobal("WebSocket", MockWebSocket);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // 说明：旧实现（丢弃 setTimeout 句柄）的这条用例**不**变红——因为
  // MockWebSocket 的 readyState 变化让 connect() 的早退恰好挡住了多余连接。
  // 真正的失效场景（定时器各自排链、指数退避被重置）由下面的句柄断言覆盖。
  it("至多一个 pending 重连定时器：连续断线不会排出多条重连链", async () => {
    const client = await freshClient();
    client.call("workflow.list", {}).catch(() => {});
    const ws = MockWebSocket.instances[0];
    ws.open();
    await vi.waitFor(() => expect(ws.sent).toHaveLength(1));

    // 第一次断开 → 排 T1
    ws.close();
    expect(MockWebSocket.instances).toHaveLength(1);

    // T1 到期、新连接建立后它立刻再断开：会排 T2
    await vi.advanceTimersByTimeAsync(2000);
    const ws2 = MockWebSocket.instances[1];
    ws2.open();
    ws2.close();
    expect(MockWebSocket.instances).toHaveLength(2);

    // T1 若没被清掉，此刻仍在队列里；两个定时器都推进也只应新建**一个**连接
    await vi.advanceTimersByTimeAsync(60000);
    expect(MockWebSocket.instances, "重连定时器必须互斥，不能每次断开都新排一条链").toHaveLength(3);
  });

  it("重连定时器句柄至多一个：旧实现丢弃句柄时 timer 计数会累积", async () => {
    const client = await freshClient();
    client.call("workflow.list", {}).catch(() => {});
    const ws = MockWebSocket.instances[0];
    ws.open();
    await vi.waitFor(() => expect(ws.sent).toHaveLength(1));

    // 反复断开：每次 onclose 排一个定时器。互斥实现下，
    // 待触发的定时器数量恒为 1。
    for (let i = 0; i < 4; i++) {
      const live = MockWebSocket.instances[MockWebSocket.instances.length - 1];
      live.open();
      live.close();
    }
    const pendingTimers = vi.getTimerCount();
    expect(pendingTimers, `4 次断开后仍有 ${pendingTimers} 个待触发定时器，句柄没被复用`).toBe(1);

    await vi.advanceTimersByTimeAsync(60000);
  });

  it("旧 socket 的迟到 onclose 不影响新 socket：在飞请求不被误 reject", async () => {
    const client = await freshClient();
    const p = client.call("run.get", { run_id: "r1" });
    // `p` 会在下面 ws1.close() 时立刻 reject。断言要等到用例末尾才做，
    // 中间这段它处于「已 reject 但无人接」的状态——Node 会记一条 unhandled
    // rejection 并让 vitest 退出码为 1。断言句柄先挂上（catch 只是把 rejection
    // 标记为已处理，值仍由末尾的 expects 校验），中途不产生未处理拒绝。
    const pSettled = p.then(
      (v) => ({ ok: true as const, v }),
      (e) => ({ ok: false as const, e }),
    );
    const ws1 = MockWebSocket.instances[0];
    ws1.open();
    await vi.waitFor(() => expect(ws1.sent).toHaveLength(1));

    // 旧连接真正断开 → 重连
    ws1.close();
    await vi.advanceTimersByTimeAsync(1000);
    const ws2 = MockWebSocket.instances[1];
    ws2.open();

    // 新连接上发起一个在飞请求
    const p2 = client.call("run.list", {});
    await vi.waitFor(() => expect(ws2.sent).toHaveLength(1));
    const req2 = ws2.lastSent();

    // 旧 socket 的 onclose 姗姗来迟（浏览器里真实存在）
    ws1.closeSilently();
    ws1.onclose?.();

    // 新连接的在飞请求必须仍然可完成
    let settled = false;
    void p2.then(() => {
      settled = true;
    });
    ws2.receive({ jsonrpc: "2.0", id: req2.id, result: { runs: [] } });
    await expect(p2).resolves.toEqual({ runs: [] });
    expect(settled).toBe(true);

    // 旧连接的失败请求照常 reject（它确实死了）
    await expect(p).rejects.toThrow();
    expect(await pSettled).toMatchObject({ ok: false });
  });
});
