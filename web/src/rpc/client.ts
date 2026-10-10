import { isDesktop } from "../platform";
import { apiToken } from "../config";
import { NativeClient } from "./native";
import { WebSocketClient } from "./websocket";
import { WRITE_METHODS } from "./methods";
export { RpcError, errText } from "./errors";
export type Service = "flow" | "journal";

/**
 * v2 协议包装：JSON-RPC 之下的 journal v2 语义。
 * - 每个请求注入 `_token`（桌面内嵌服务由 Tauri 侧注入，这里跳过）；
 * - 写命令自动携带 `request_id`（幂等键；同键重试不产生双写）；
 * - 主工作台解包已提交回执；journal 服务保留完整原生协议。
 */
export { WRITE_METHODS } from "./methods";

/** One transport per client: desktop never falls back to a network endpoint. */
export class RpcClient {
  private readonly transport;
  constructor(
    endpoint?: string,
    private readonly service: Service = "flow",
  ) {
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
      if (token && enriched["_token"] === undefined) enriched["_token"] = token;
    }
    let requestId: string | undefined;
    if (WRITE_METHODS.has(method)) {
      if (enriched["request_id"] === undefined) enriched["request_id"] = crypto.randomUUID();
      requestId = enriched["request_id"] as string;
    }
    const reply = await this.transport.call<{
      committed?: boolean;
      result?: unknown;
    }>(method, enriched);
    // 主工作台使用物化结果；JournalClient 消费完整回执并处理 -32020。
    if (this.service === "flow" && requestId !== undefined && reply?.committed === true) {
      return reply.result as T;
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
      if (token && enriched["_token"] === undefined) enriched["_token"] = token;
    }
    if (
      method === "run.subscribe" &&
      this.service === "flow" &&
      enriched["event_format"] === undefined
    ) {
      // 主工作台订阅：v1 Envelope 事件形状（服务端物化值），monitor 零适配
      enriched["event_format"] = "envelope";
    }
    return this.transport.subscribe(method, enriched, onEvent);
  }
}
export const client = new RpcClient();
