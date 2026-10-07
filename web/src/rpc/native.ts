import { Channel, invoke } from "@tauri-apps/api/core";
import { ref } from "vue";
import { RpcError } from "./errors";
import type { Service } from "./client";

interface Reply<T> {
  result?: T;
  error?: { code: number; message: string; data?: unknown };
}

function result<T>(reply: Reply<T>): T {
  if (reply.error) throw new RpcError(reply.error.code, reply.error.message, reply.error.data);
  return reply.result as T;
}

export class NativeClient {
  readonly connected = ref(false);
  readonly url = "本地服务";
  private disposed = false;
  private session?: Promise<string>;
  private subscriptions = new Set<() => Promise<void>>();

  constructor(private readonly service: Service) {}

  private async open(): Promise<string> {
    if (this.disposed) throw new RpcError(-32000, "RPC client closed");
    this.session ??= invoke<string>("flow_open_client")
      .then((id) => {
        if (!this.disposed) this.connected.value = true;
        return id;
      })
      .catch((error) => {
        this.session = undefined;
        throw error;
      });
    const session = await this.session;
    if (this.disposed) throw new RpcError(-32000, "RPC client closed");
    return session;
  }

  close(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.connected.value = false;
    for (const stop of this.subscriptions) void stop();
    this.subscriptions.clear();
    void this.session?.then((session) => invoke("flow_close_client", { session })).catch(() => {});
  }

  // The embedded service shares the app lifetime; there is no socket to reconnect.
  onReconnect(_cb: () => void): () => void {
    return () => {};
  }

  async call<T>(method: string, params: Record<string, unknown> | unknown[]): Promise<T> {
    const session = await this.open();
    const reply = await invoke<Reply<T>>("flow_call", {
      session,
      service: this.service,
      method,
      params,
    });
    if (this.disposed) throw new RpcError(-32000, "RPC client closed");
    return result(reply);
  }

  async subscribe(
    method: string,
    params: Record<string, unknown>,
    onEvent: (event: unknown) => void,
  ): Promise<() => Promise<void>> {
    const session = await this.open();
    const subscription = crypto.randomUUID();
    const channel = new Channel<unknown>();
    let active = true;
    channel.onmessage = (event) => {
      if (active && !this.disposed) onEvent(event);
    };
    const stop = async () => {
      if (!active) return;
      active = false;
      this.subscriptions.delete(stop);
      await invoke("flow_unsubscribe", { session, subscription }).catch(() => {});
    };
    this.subscriptions.add(stop);
    try {
      const reply = await invoke<Reply<unknown>>("flow_subscribe", {
        session,
        subscription,
        service: this.service,
        method,
        params,
        channel,
      });
      result(reply);
      if (this.disposed) {
        await stop();
        throw new RpcError(-32000, "RPC client closed");
      }
      return stop;
    } catch (error) {
      await stop();
      throw error;
    }
  }
}
