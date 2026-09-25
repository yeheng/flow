import { ref } from "vue";

export class RpcError extends Error {
  constructor(
    public code: number,
    message: string,
  ) {
    super(message);
    this.name = "RpcError";
  }
}

export function errText(e: unknown): string {
  if (e instanceof RpcError) return e.message;
  if (e instanceof Error) return e.message;
  return String(e);
}

interface PendingCall {
  resolve: (v: never) => void;
  reject: (e: unknown) => void;
  /** 该请求是订阅建立请求：响应 result 是服务端 subscription id */
  localSub?: number;
}

interface Subscription {
  method: string;
  params: Record<string, unknown>;
  onEvent: (event: unknown) => void;
  /** 本连接上是否已发出过订阅请求：防止「连接未建立时发起订阅」与 onopen 重建叠加成重复订阅 */
  active: boolean;
}

/**
 * JSON-RPC 2.0 over WebSocket 客户端，直连 flow-server（协议见 docs/DESIGN.md §9）。
 * 断线自动重连（指数退避）；重连后自动重建订阅，调用方通过 onReconnect 重新拉取状态。
 */
class RpcClient {
  readonly url: string;
  readonly connected = ref(false);

  private ws: WebSocket | null = null;
  private nextId = 1;
  private pending = new Map<number, PendingCall>();
  private openWaiters: Array<{ resolve: () => void; reject: (e: unknown) => void }> = [];
  private subs = new Map<number, Subscription>();
  private nextLocalSub = 1;
  /** 服务端 subscription id → 本地订阅 id（每条连接独立分配） */
  private serverToLocal = new Map<unknown, number>();
  private everConnected = false;
  private reconnectDelay = 500;
  private reconnectCbs: Array<() => void> = [];

  constructor() {
    this.url = (import.meta.env.VITE_FLOW_RPC as string | undefined) ?? "ws://127.0.0.1:9800";
  }

  onReconnect(cb: () => void): void {
    this.reconnectCbs.push(cb);
  }

  async call<T>(method: string, params: Record<string, unknown> | unknown[]): Promise<T> {
    await this.waitOpen();
    const id = this.nextId++;
    return new Promise<T>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.ws!.send(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
    });
  }

  /** 返回退订函数。jsonrpsee 订阅：响应 result 是 subscription id，通知 params 为 {subscription, result}。 */
  async subscribe(
    method: string,
    params: Record<string, unknown>,
    onEvent: (event: unknown) => void,
  ): Promise<() => Promise<void>> {
    const local = this.nextLocalSub++;
    const sub: Subscription = { method, params, onEvent, active: false };
    this.subs.set(local, sub);
    try {
      await this.sendSubscribe(local, sub);
    } catch (e) {
      this.subs.delete(local);
      throw e;
    }
    return async () => {
      this.subs.delete(local);
      let serverId: unknown;
      for (const [sid, lid] of this.serverToLocal) {
        if (lid === local) serverId = sid;
      }
      if (serverId !== undefined) {
        this.serverToLocal.delete(serverId);
        const unsubMethod = method.replace(/subscribe$/, "unsubscribe");
        // jsonrpsee 的退订参数是位置参数 [subscription id]，具名参数会被静默忽略；
        // 退订尽力而为：连接断开时服务端会自行清理
        await this.call(unsubMethod, [serverId]).catch(() => {});
      }
    };
  }

  private async sendSubscribe(local: number, sub: Subscription): Promise<void> {
    await this.waitOpen();
    // 同一连接上只发一次：onopen 的重建与 subscribe 自身等待 waitOpen 后的发送在此去重
    if (sub.active) return;
    sub.active = true;
    const id = this.nextId++;
    await new Promise<void>((resolve, reject) => {
      this.pending.set(id, { resolve, reject, localSub: local });
      this.ws!.send(JSON.stringify({ jsonrpc: "2.0", id, method: sub.method, params: sub.params }));
    });
  }

  private connect(): void {
    if (this.ws && this.ws.readyState !== WebSocket.CLOSED) return;
    const ws = new WebSocket(this.url);
    this.ws = ws;

    ws.onopen = () => {
      this.connected.value = true;
      this.reconnectDelay = 500;
      const reconnected = this.everConnected;
      this.everConnected = true;
      for (const w of this.openWaiters.splice(0)) w.resolve();
      // 重连后旧订阅已随连接消失，逐个重建；状态补齐交给 onReconnect 调用方。
      // 先复位 active：本连接上尚未发送的订阅（连接未建立时发起的）由重建发送，
      // 其自身 waitOpen 后的续发会被 sendSubscribe 的 active 检查去重
      this.serverToLocal.clear();
      for (const sub of this.subs.values()) sub.active = false;
      for (const [local, sub] of this.subs) {
        void this.sendSubscribe(local, sub).catch(() => {});
      }
      if (reconnected) this.reconnectCbs.forEach((cb) => cb());
    };

    ws.onmessage = (e: MessageEvent<string>) => this.onMessage(e.data);

    ws.onclose = () => {
      this.connected.value = false;
      this.ws = null;
      const err = new RpcError(-32000, "与 flow-server 的连接已断开");
      for (const p of this.pending.values()) p.reject(err);
      this.pending.clear();
      for (const w of this.openWaiters.splice(0)) w.reject(err);
      setTimeout(() => this.connect(), this.reconnectDelay);
      this.reconnectDelay = Math.min(this.reconnectDelay * 2, 10000);
    };

    ws.onerror = () => ws.close();
  }

  private waitOpen(): Promise<void> {
    this.connect();
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return Promise.resolve();
    return new Promise((resolve, reject) => this.openWaiters.push({ resolve, reject }));
  }

  private onMessage(raw: string): void {
    let msg: {
      id?: number;
      method?: string;
      params?: { subscription?: unknown; result?: unknown };
      result?: unknown;
      error?: { code: number; message: string };
    };
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }
    if (msg.method) {
      const local = this.serverToLocal.get(msg.params?.subscription);
      if (local !== undefined) this.subs.get(local)?.onEvent(msg.params?.result);
      return;
    }
    const p = this.pending.get(msg.id ?? -1);
    if (!p) return;
    this.pending.delete(msg.id ?? -1);
    if (msg.error) {
      p.reject(new RpcError(msg.error.code, msg.error.message));
      return;
    }
    if (p.localSub !== undefined) {
      this.serverToLocal.set(msg.result, p.localSub);
      p.resolve(undefined as never);
      return;
    }
    p.resolve(msg.result as never);
  }
}

export const client = new RpcClient();
