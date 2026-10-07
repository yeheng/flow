import { isDesktop } from "../platform";
import { NativeClient } from "./native";
import { WebSocketClient } from "./websocket";
export { RpcError, errText } from "./errors";
export type Service = "flow" | "journal";

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
  call<T>(method: string, params: Record<string, unknown> | unknown[]): Promise<T> {
    return this.transport.call<T>(method, params);
  }
  subscribe(
    method: string,
    params: Record<string, unknown>,
    onEvent: (event: unknown) => void,
  ): Promise<() => Promise<void>> {
    return this.transport.subscribe(method, params, onEvent);
  }
}
export const client = new RpcClient();
