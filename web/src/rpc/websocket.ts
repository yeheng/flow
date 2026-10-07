import { ref } from "vue";
import { rpcUrl } from "../config";

import { RpcError } from "./errors";

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
export class WebSocketClient {
  constructor(private readonly endpoint?: string) {}
  readonly connected = ref(false);

  /** 惰性解析：首次连接时才读运行时配置（main.ts 启动时拉取 config.json） */
  get url(): string {
    return this.endpoint ?? rpcUrl();
  }

  private disposed = false;
  close(): void {
    this.disposed = true;
    if (this.reconnectTimer !== null) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    const socket = this.ws;
    this.ws = null;
    this.connected.value = false;
    const error = new RpcError(-32000, "RPC client closed");
    for (const pending of this.pending.values()) pending.reject(error);
    this.pending.clear();
    for (const waiter of this.openWaiters.splice(0)) waiter.reject(error);
    this.subs.clear(); this.serverToLocal.clear(); this.reconnectCbs = [];
    socket?.close();
  }

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
  /** pending 的重连定时器句柄：至多一个，connect() 开头清理 */
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectCbs: Array<() => void> = [];

  onReconnect(cb: () => void): () => void {
    this.reconnectCbs.push(cb);
    return () => { this.reconnectCbs = this.reconnectCbs.filter(item => item !== cb); };
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
        if (this.disposed || !this.connected.value) return;
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
    if (this.disposed) return;
    if (this.ws && this.ws.readyState !== WebSocket.CLOSED) return;
    // 单一 pending 重连定时器：旧实现丢弃 setTimeout 句柄，B 的 onclose 排的
    // T2 与 A 排的 T1 会同时在飞，指数退避也就失效了
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    const ws = new WebSocket(this.url);
    this.ws = ws;

    ws.onopen = () => {
      if (this.disposed || this.ws !== ws) return;
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
      // 归属检查：一个旧 socket 的 onclose 可能在新 socket 已就位之后才触发，
      // 无条件清 this.ws 会把活连接置空、并在飞的请求全 reject 成「连接断开」。
      if (this.ws !== ws) return;
      this.connected.value = false;
      this.ws = null;
      const err = new RpcError(-32000, "与 flow-server 的连接已断开");
      for (const p of this.pending.values()) p.reject(err);
      this.pending.clear();
      for (const w of this.openWaiters.splice(0)) w.reject(err);
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = null;
        this.connect();
      }, this.reconnectDelay);
      // 抖动：服务端重启后所有浏览器会同步重连，齐刷刷砸在刚起来的实例上
      this.reconnectDelay = Math.min(
        Math.round(this.reconnectDelay * 2 * (0.5 + Math.random())),
        10000,
      );
    };

    // 错误信息此前被完全吞掉，转成不可区分的「断开」。至少留一条诊断。
    ws.onerror = (ev) => {
      console.warn("[flow-rpc] WebSocket 错误，随后将关闭重连", ev);
      ws.close();
    };
  }

  private waitOpen(): Promise<void> {
    if (this.disposed) return Promise.reject(new RpcError(-32000, "RPC client closed"));
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
      error?: { code: number; message: string; data?: unknown };
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
      p.reject(new RpcError(msg.error.code, msg.error.message, msg.error.data));
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

