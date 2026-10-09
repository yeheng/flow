import { isDesktop } from "../platform";
import { apiToken } from "../config";
import { NativeClient } from "./native";
import { WebSocketClient } from "./websocket";
export { RpcError, errText } from "./errors";
export type Service = "flow" | "journal";

/**
 * v2 协议包装：JSON-RPC 之下的 journal v2 语义。
 * - 每个请求注入 `_token`（桌面内嵌服务由 Tauri 侧注入，这里跳过）；
 * - 写命令自动携带 `request_id`（幂等键；同键重试不产生双写）；
 * - 写命令回执 `{committed, result, ...}` 解包为 result（v1 门面形状）。
 */
const WRITE_PREFIXES = [
  "workflow.create",
  "workflow.update",
  "workflow.publish",
  "workflow.delete",
  "run.start",
  "run.cancel",
  "run.signal",
  "run.adjudicate",
  "schedule.",
  "webhook.",
  "template.",
  "secrets.set",
  "secrets.delete",
  "config.update",
];

function isWrite(method: string): boolean {
  return WRITE_PREFIXES.some((p) => method === p || method.startsWith(`${p}.`));
}

/** One transport per client: desktop never falls back to a network endpoint. */
export class RpcClient {
  private readonly transport;
  constructor(endpoint?: string, service: Service = "flow") {
    this.transport = isDesktop() ? new NativeClient(service) : new WebSocketClient(endpoint);
  }
  get connected() {
    return this.transport.connected;
  }
  get url() {
    return this.transport.url;
  }
  close(): void {
    this.transport.close();
  }
  onReconnect(cb: () => void): () => void {
    return this.transport.onReconnect(cb);
  }

  async call<T>(method: string, params: Record<string, unknown> | unknown[]): Promise<T> {
    const base: Record<string, unknown> = Array.isArray(params)
      ? ((params[0] as Record<string, unknown>) ?? {})
      : params;
    const enriched: Record<string, unknown> = { ...base };
    if (!isDesktop()) {
      const token = apiToken();
      if (token) enriched["_token"] = token;
    }
    let requestId: string | undefined;
    if (isWrite(method)) {
      requestId = crypto.randomUUID();
      enriched["request_id"] = requestId;
    }
    const reply = await this.transport.call<{
      committed?: boolean;
      result?: unknown;
    }>(method, enriched);
    // 写命令：回执解包为 result（v1 门面形状；COMMITTED_NOT_VISIBLE 由
    // 传输层按错误码放行后同样落在这里的形状上——服务端 data 即回执）
    if (requestId !== undefined) {
      return (reply?.result ?? reply) as T;
    }
    return reply as T;
  }

  subscribe(
    method: string,
    params: Record<string, unknown>,
    onEvent: (event: unknown) => void,
  ): Promise<() => Promise<void>> {
    const enriched: Record<string, unknown> = { ...params };
    if (!isDesktop()) {
      const token = apiToken();
      if (token) enriched["_token"] = token;
    }
    if (method === "run.subscribe") {
      // 主工作台订阅：v1 Envelope 事件形状（服务端物化值），monitor 零适配
      enriched["event_format"] = "envelope";
    }
    return this.transport.subscribe(method, enriched, onEvent);
  }
}
export const client = new RpcClient();
