import { beforeEach, describe, expect, it, vi } from "vitest";
import { ref } from "vue";
const transport = vi.hoisted(() => ({ call: vi.fn(), subscribe: vi.fn() }));
vi.mock("../platform", () => ({ isDesktop: () => false }));
vi.mock("../config", () => ({ apiToken: () => "workbench-token" }));
vi.mock("./websocket", () => ({
  WebSocketClient: class {
    connected = ref(true);
    url = "ws://test";
    call = transport.call;
    subscribe = transport.subscribe;
    close() {}
    onReconnect() {
      return () => {};
    }
  },
}));
import { RpcClient, RpcError, WRITE_METHODS } from "./client";
import { JournalClient } from "../api/journal";
import { getRun } from "../api/flow";

const receipt = {
  committed: true,
  visible: true,
  request_id: "stable",
  commit_cursor: { journal_id: "dataset", lsn: "7" },
  result: { id: "created" },
};
beforeEach(() => {
  transport.call.mockReset().mockResolvedValue(receipt);
  transport.subscribe.mockReset().mockResolvedValue(async () => {});
});

describe("V2 client protocol", () => {
  it("adds identities to every write and leaves reads without write identities", async () => {
    const client = new RpcClient();
    for (const method of WRITE_METHODS) {
      await client.call(method, {});
      const sent = transport.call.mock.calls[transport.call.mock.calls.length - 1]![1];
      expect(sent).toMatchObject({ _token: "workbench-token", request_id: expect.any(String) });
    }
    for (const method of [
      "schedule.list",
      "webhook.list",
      "template.list",
      "template.get",
      "run.get.full",
    ]) {
      await client.call(method, {});
      expect(transport.call.mock.calls[transport.call.mock.calls.length - 1]![1]).toEqual({
        _token: "workbench-token",
      });
    }
  });

  it("preserves journal identity, server credentials and the entire receipt", async () => {
    const client = new JournalClient("ws://other", "other-server-token", "http://other");
    expect(
      await client.command(
        "schedule.create",
        { workflow_id: "w", cron: "* * * * *" },
        "schedule.create",
        "stable",
      ),
    ).toEqual(receipt);
    expect(transport.call).toHaveBeenCalledWith("schedule.create", {
      workflow_id: "w",
      cron: "* * * * *",
      _token: "other-server-token",
      request_id: "stable",
    });
  });

  it("unwraps workbench receipts and preserves explicit subscription formats", async () => {
    const client = new RpcClient();
    expect(await client.call("schedule.create", { request_id: "stable" })).toEqual(receipt.result);
    expect(transport.call.mock.calls[0]![1].request_id).toBe("stable");
    await client.subscribe("run.subscribe", { run_id: "r" }, () => {});
    expect(transport.subscribe.mock.calls[0]![1].event_format).toBe("envelope");
    await client.subscribe(
      "run.subscribe",
      { run_id: "r", event_format: "v2", _token: "explicit" },
      () => {},
    );
    expect(transport.subscribe.mock.calls[1]![1]).toEqual({
      run_id: "r",
      event_format: "v2",
      _token: "explicit",
    });
  });

  it("recovers the original journal command when projection is delayed", async () => {
    transport.call
      .mockRejectedValueOnce(
        new RpcError(-32020, "COMMITTED_NOT_VISIBLE", { ...receipt, visible: false }),
      )
      .mockResolvedValueOnce(receipt);
    const client = new JournalClient("ws://other", "other-token", "http://other");
    expect(await client.command("schedule.create", {}, "schedule.create", "stable")).toEqual(
      receipt,
    );
    expect(transport.call.mock.calls.map(([method]) => method)).toEqual([
      "schedule.create",
      "command.status",
    ]);
    expect(transport.call.mock.calls[1]![1]).toEqual({
      scope: "schedule.create",
      request_id: "stable",
      _token: "other-token",
    });
  });

  it("loads the materialized run shape for the run detail view", async () => {
    const full = { run: { run_id: "r", workflow_id: "w", workflow_version: 1 }, live: true };
    transport.call.mockResolvedValueOnce(full);
    expect(await getRun("r")).toEqual(full);
    expect(transport.call).toHaveBeenCalledWith("run.get.full", {
      run_id: "r",
      _token: "workbench-token",
    });
  });
});
