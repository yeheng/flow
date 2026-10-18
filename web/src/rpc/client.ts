import { isDesktop } from "../platform";
import { apiToken } from "../config";
import { RpcError } from "./errors";
import { NativeClient } from "./native";
import { WebSocketClient } from "./websocket";
import { WRITE_METHODS } from "./methods";
export { RpcError, errText } from "./errors";
export type Service = "flow" | "journal";

/** 服务端对写命令的准入错误：缺 request_id（命令尚未被接受，补键重试安全） */
function missingRequestId(error: unknown): boolean {
  return (
    error instanceof RpcError &&
    error.code === -32602 &&
    error.message.includes("request_id required for write commands")
  );
}

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
    let reply: {
      committed?: boolean;
      result?: unknown;
      request_id?: string;
      visible?: boolean;
    };
    try {
      reply = await this.transport.call(method, enriched);
    } catch (error) {
      // 浏览器端的写方法清单是手抄的（桌面端由 Tauri 按后端清单兜底注入）：
      // 漏项时服务端在准入阶段就拒绝，命令未被接受，补 request_id 重试一次安全。
      // 已有幂等键仍报缺键说明不是清单问题，直接上抛。
      if (requestId !== undefined || !missingRequestId(error)) throw error;
      requestId = crypto.randomUUID();
      enriched["request_id"] = requestId;
      reply = await this.transport.call(method, enriched);
    }
    // SQLite 单事务提交事实 + 当前状态，写回执的 visible 恒为 true（-32020
    // COMMITTED_NOT_VISIBLE 已无生产者）。主工作台解包回执取物化结果；
    // JournalClient 另行消费完整回执。
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
      // 主工作台订阅：journal v2 事件原形（服务端唯一支持格式）
      enriched["event_format"] = "v2";
    }
    return this.transport.subscribe(method, enriched, onEvent);
  }
}
export const client = new RpcClient();
