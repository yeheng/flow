import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { RpcError } from "./errors";
import { DEFAULT_CALL_TIMEOUT_MS, WebSocketClient } from "./websocket";

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
  onerror: ((ev: unknown) => void) | null = null;

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

  receiveRaw(raw: string): void {
    this.onmessage?.({ data: raw });
  }

  receive(msg: unknown): void {
    this.receiveRaw(JSON.stringify(msg));
  }

  lastSent(): { id: number; method: string; params: unknown } {
    return JSON.parse(this.sent[this.sent.length - 1]);
  }
}

function freshClient(): WebSocketClient {
  MockWebSocket.instances = [];
  return new WebSocketClient("ws://test/rpc");
}

describe("WebSocketClient.call：超时上界与畸形帧可观测", () => {
  beforeEach(() => {
    vi.stubGlobal("WebSocket", MockWebSocket);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("服务端连接活着但不回：call 在超时点 reject，不再挂到断开", async () => {
    const client = freshClient();
    const started = Date.now();
    const p = client.call("workflow.list", {}, 60);
    const ws = MockWebSocket.instances[0];
    ws.open();
    await vi.waitFor(() => expect(ws.sent).toHaveLength(1));
    await expect(p).rejects.toThrow(/超时/);
    expect(Date.now() - started).toBeLessThan(DEFAULT_CALL_TIMEOUT_MS);
    await expect(p).rejects.toBeInstanceOf(RpcError);
    // 超时后清理 pending：迟到的响应被忽略，不再触发 resolve。
    ws.receive({ jsonrpc: "2.0", id: ws.lastSent().id, result: { ok: true } });
    client.close();
  });

  it("连接始终未建立：call 同样在超时点有界", async () => {
    const client = freshClient();
    const p = client.call("workflow.list", {}, 60);
    await expect(p).rejects.toThrow(/超时/);
    client.close();
  });

  it("正常响应在超时前 resolve", async () => {
    const client = freshClient();
    const p = client.call<{ workflows: unknown[] }>("workflow.list", {}, 5_000);
    const ws = MockWebSocket.instances[0];
    ws.open();
    await vi.waitFor(() => expect(ws.sent).toHaveLength(1));
    ws.receive({ jsonrpc: "2.0", id: ws.lastSent().id, result: { workflows: [] } });
    await expect(p).resolves.toEqual({ workflows: [] });
    client.close();
  });

  it("畸形帧触发 warn（带原始帧片段），不静默丢弃", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const client = freshClient();
    const p = client.call("workflow.list", {}, 5_000);
    const ws = MockWebSocket.instances[0];
    ws.open();
    ws.receiveRaw("{not json");
    expect(warn).toHaveBeenCalledTimes(1);
    expect(String(warn.mock.calls[0][1])).toContain("{not json");
    client.close();
    await expect(p).rejects.toThrow();
  });
});
