import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
const bridge = vi.hoisted(() => ({ invoke: vi.fn() }));
vi.mock("@tauri-apps/api/core", () => ({
  isTauri: () => true,
  invoke: bridge.invoke,
  Channel: class {
    onmessage: (event: unknown) => void = () => {};
  },
}));
beforeEach(() => {
  vi.resetModules();
  bridge.invoke
    .mockReset()
    .mockImplementation(async (command: string) =>
      command === "flow_open_client" ? "session" : { result: true },
    );
  vi.stubGlobal(
    "WebSocket",
    vi.fn(() => {
      throw new Error("desktop must not use WebSocket");
    }),
  );
});
afterEach(() => vi.unstubAllGlobals());
describe("desktop transport", () => {
  it("selects native calls and preserves structured RPC errors", async () => {
    const { RpcClient, RpcError } = await import("./client");
    const client = new RpcClient("ws://ignored:9800");
    bridge.invoke.mockImplementation(async (command) =>
      command === "flow_open_client"
        ? "session"
        : {
            error: { code: -32020, message: "COMMITTED_NOT_VISIBLE", data: { committed: true } },
          },
    );
    await expect(client.call("workflow.create", { name: "x" })).rejects.toMatchObject({
      code: -32020,
      data: { committed: true },
    });
    await expect(client.call("workflow.list", {})).rejects.toBeInstanceOf(RpcError);
    expect(bridge.invoke).toHaveBeenCalledWith("flow_call", {
      session: "session",
      service: "flow",
      method: "workflow.create",
      params: { name: "x" },
    });
    expect(WebSocket).not.toHaveBeenCalled();
    client.close();
  });
  it("delivers early events and stops them after unsubscribe", async () => {
    const { RpcClient } = await import("./client");
    let channel!: { onmessage: (event: unknown) => void };
    bridge.invoke.mockImplementation(async (command, args) => {
      if (command === "flow_open_client") return "session";
      if (command === "flow_subscribe") {
        channel = args.channel;
        channel.onmessage({ seq: 1 });
      }
      return { result: true };
    });
    const client = new RpcClient();
    const receive = vi.fn();
    const stop = await client.subscribe("run.subscribe", { run_id: "run" }, receive);
    expect(receive).toHaveBeenCalledWith({ seq: 1 });
    await stop();
    await stop();
    channel.onmessage({ seq: 2 });
    expect(receive).toHaveBeenCalledTimes(1);
    expect(bridge.invoke.mock.calls.filter(([cmd]) => cmd === "flow_unsubscribe")).toHaveLength(1);
    client.close();
  });
  it("closes a late session without sending a write after disposal", async () => {
    const { RpcClient } = await import("./client");
    let opened!: (id: string) => void;
    bridge.invoke.mockImplementation((command) =>
      command === "flow_open_client"
        ? new Promise((resolve) => {
            opened = resolve;
          })
        : Promise.resolve(),
    );
    const client = new RpcClient();
    const rejected = expect(client.call("workflow.create", { name: "late" })).rejects.toThrow(
      "closed",
    );
    client.close();
    opened("late-session");
    await rejected;
    await vi.waitFor(() =>
      expect(bridge.invoke).toHaveBeenCalledWith("flow_close_client", { session: "late-session" }),
    );
    expect(bridge.invoke.mock.calls.some(([cmd]) => cmd === "flow_call")).toBe(false);
    expect(client.connected.value).toBe(false);
  });
  it("routes JSONL and downloads to native commands", async () => {
    const { JournalClient } = await import("../api/journal");
    const client = new JournalClient("ws://remote", "browser-token", "http://remote");
    await client.call("workflow.list");
    expect(bridge.invoke).toHaveBeenCalledWith(
      "flow_call",
      expect.objectContaining({ service: "journal" }),
    );
    await client.download("run", "value");
    expect(bridge.invoke).toHaveBeenCalledWith("flow_download", { run: "run", output: "value" });
    expect(WebSocket).not.toHaveBeenCalled();
    client.close();
  });
  it("closes subscriptions being created during disposal", async () => {
    const { RpcClient } = await import("./client");
    let finish!: (value: unknown) => void;
    let channel!: { onmessage: (event: unknown) => void };
    bridge.invoke.mockImplementation((command, args) => {
      if (command === "flow_open_client") return Promise.resolve("session");
      if (command === "flow_subscribe") {
        channel = args.channel;
        return new Promise((resolve) => {
          finish = resolve;
        });
      }
      return Promise.resolve();
    });
    const client = new RpcClient();
    const receive = vi.fn();
    const rejected = expect(client.subscribe("run.subscribe", {}, receive)).rejects.toThrow(
      "closed",
    );
    await vi.waitFor(() => expect(finish).toBeDefined());
    client.close();
    channel.onmessage({ seq: 1 });
    finish({ result: 1 });
    await rejected;
    expect(receive).not.toHaveBeenCalled();
    expect(bridge.invoke).toHaveBeenCalledWith("flow_close_client", { session: "session" });
  });
});
