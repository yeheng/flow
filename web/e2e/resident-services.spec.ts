import { expect, test } from "@playwright/test";
import { rpc } from "./helpers";
import { cpSync, mkdirSync, readFileSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
async function invokeService(params: Record<string, unknown>): Promise<unknown> {
  const result = await rpc<{ value: unknown }>("services.call", params);
  return result.value;
}
let fixtureRoot = "";
test.beforeAll(() => {
  const state = JSON.parse(readFileSync(path.join(tmpdir(), "flow-e2e-server.json"), "utf8")) as {
    dir: string;
  };
  fixtureRoot = state.dir;
  const dir = fixtureRoot;
  const REPO_ROOT = path.resolve(process.cwd(), "..");
  for (const id of ["resident-echo", "resident-target"]) {
    const pluginDir = path.join(dir, "plugins", id);
    cpSync(path.join(REPO_ROOT, "examples/resident-echo"), pluginDir, { recursive: true });
    mkdirSync(path.join(pluginDir, "bin"), { recursive: true });
    cpSync(
      path.join(REPO_ROOT, "target/debug/examples/resident_echo"),
      path.join(pluginDir, "bin/resident_echo"),
    );
    const manifest = readFileSync(path.join(pluginDir, "plugin.toml"), "utf8")
      .replace('id = "resident-echo"', `id = "${id}"`)
      .replace(
        'methods = ["echo"]',
        'methods = ["echo", "relay", "host"]\npeers = ["resident-target"]',
      );
    writeFileSync(path.join(pluginDir, "plugin.toml"), manifest);
  }
});
test.afterAll(() => {
  for (const id of ["resident-echo", "resident-target"])
    rmSync(path.join(fixtureRoot, "plugins", id), { recursive: true, force: true });
});
test("常驻插件：隔离页面双向通信、插件间路由及关闭后回收", async ({ page, request }) => {
  const target = await rpc<{ instance: string }>("services.start", {
    plugin_id: "resident-target",
  });
  const service = await rpc<{ instance: string }>("services.start", { plugin_id: "resident-echo" });
  try {
    expect(
      await invokeService({
        plugin_id: "resident-echo",
        instance: service.instance,
        method: "echo",
        params: { committed: true, result: "plugin-data" },
      }),
    ).toEqual({ committed: true, result: "plugin-data" });
    expect(
      await invokeService({
        plugin_id: "resident-echo",
        instance: service.instance,
        method: "host",
        params: {
          method: "flow/subscribe",
          params: {
            subscription: "peer-events",
            stream: "peer",
            params: { plugin_id: "resident-target", instance: target.instance, after: "0" },
          },
        },
      }),
    ).toEqual({ subscription: "peer-events" });
    expect(
      await invokeService({
        plugin_id: "resident-echo",
        instance: service.instance,
        method: "relay",
        params: {
          plugin_id: "resident-target",
          instance: target.instance,
          method: "echo",
          params: { via: "peer" },
        },
      }),
    ).toEqual({ via: "peer" });
    await expect(
      invokeService({
        plugin_id: "resident-echo",
        instance: "stale",
        method: "echo",
        params: {},
      }),
    ).rejects.toThrow("stale");
    await expect(
      invokeService({
        plugin_id: "resident-echo",
        instance: service.instance,
        method: "relay",
        params: { plugin_id: "forbidden" },
      }),
    ).rejects.toThrow("forbidden");
    await page.goto("/settings");
    const panel = page.locator(".resident-services");
    await panel
      .getByRole("listitem")
      .filter({ hasText: "resident-echo" })
      .getByRole("button", { name: "打开界面" })
      .click();
    const frame = panel.frameLocator("iframe");
    await expect(frame.getByText("已连接常驻插件")).toBeVisible();
    await frame.getByRole("textbox", { name: "消息" }).fill("real-process-message");
    await frame.getByRole("button", { name: "发送" }).click();
    await expect(frame.locator("pre")).toContainText('"result":{"text":"real-process-message"}');
    await expect(frame.locator("pre")).toContainText("echo.received");
    await expect(frame.locator("pre")).toContainText("host.event");
    await panel.screenshot({ path: "/private/tmp/flow-resident-service-ui.png" });
    const isolation = await frame.locator("body").evaluate(() => {
      try {
        void window.parent.localStorage;
        return false;
      } catch {
        return true;
      }
    });
    expect(isolation).toBe(true);
    const src = await panel.locator("iframe").getAttribute("src");
    const resource = await request.get(src!);
    expect(resource.headers()["content-security-policy"]).toContain("sandbox allow-scripts");
    await panel.getByRole("button", { name: "关闭界面" }).click();
    const running = await rpc<{ services: { instance: string; alive: boolean }[] }>(
      "services.list",
    );
    expect(running.services.some((s) => s.instance === service.instance && s.alive)).toBe(true);
    await rpc("services.stop", { plugin_id: "resident-echo", instance: service.instance });
    expect((await request.get(src!)).status()).toBe(404);
  } finally {
    await rpc("services.stop", { plugin_id: "resident-echo", instance: service.instance }).catch(
      () => {},
    );
    await rpc("services.stop", { plugin_id: "resident-target", instance: target.instance }).catch(
      () => {},
    );
  }
});

test("绑定节点：插件读取持久事件并幂等投递回答", async ({ page }) => {
  const wf = await rpc<{ workflow_id: string }>("workflow.import", {
    name: `resident-binding-${Date.now()}`,
    publish: true,
    definition: {
      semantics: 2,
      nodes: [
        { id: "s", type: "start" },
        { id: "q", type: "human_task" },
        { id: "e", type: "end" },
      ],
      edges: [
        { from: "s", to: "q" },
        { from: "q", to: "e" },
      ],
    },
  });
  const { run_id } = await rpc<{ run_id: string }>("run.start", { workflow_id: wf.workflow_id });
  await expect
    .poll(
      async () => {
        const result = await rpc<{ value: { nodes: Record<string, { wait?: unknown }> } }>(
          "run.get",
          { run_id },
        );
        return !!result.value.nodes.q?.wait;
      },
      { timeout: 15000 },
    )
    .toBe(true);
  const service = await rpc<{ instance: string }>("services.start", {
    plugin_id: "resident-echo",
    binding: { run_id, node_id: "q" },
  });
  const call = (method: string, params: Record<string, unknown> = {}) =>
    invokeService({
      plugin_id: "resident-echo",
      instance: service.instance,
      method: "host",
      params: { method, params },
    });
  try {
    await page.goto(`/runs/${run_id}`);
    await page.getByRole("tab", { name: "resident-echo · q", exact: true }).click();
    const pluginFrame = page.frameLocator(".plugin-tabs iframe");
    await expect(pluginFrame.getByText("已连接常驻插件")).toBeVisible();
    await pluginFrame.getByRole("textbox", { name: "消息" }).fill("run-tab-message");
    await pluginFrame.getByRole("button", { name: "发送" }).click();
    await expect(pluginFrame.locator("pre")).toContainText('"result":{"text":"run-tab-message"}');
    expect(
      await call("flow/subscribe", { subscription: "node-events", stream: "node", params: {} }),
    ).toEqual({ subscription: "node-events" });
    const events = (await call("flow/node.events")) as { events: { event: { node_id: string } }[] };
    expect(events.events.length).toBeGreaterThan(0);
    expect(events.events.every((e) => e.event.node_id === "q")).toBe(true);
    const answer = await call("flow/node.signal", {
      request_id: "answer-1",
      payload: { answer: "yes" },
    });
    expect(
      await call("flow/node.signal", { request_id: "answer-1", payload: { answer: "yes" } }),
    ).toEqual(answer);
    await expect
      .poll(
        async () => (await rpc<{ value: { status: string } }>("run.get", { run_id })).value.status,
        { timeout: 15000 },
      )
      .toBe("succeeded");
    await expect
      .poll(async () => {
        const result = await rpc<{
          events: {
            event: {
              topic: string;
              payload: {
                subscription?: string;
                reason?: string;
                event?: { event?: { kind?: string } };
              };
            };
          }[];
        }>("services.events", {
          plugin_id: "resident-echo",
          instance: service.instance,
          after: "0",
        });
        const messages = result.events.filter(
          (e) => e.event.topic === "host.event" && e.event.payload.subscription === "node-events",
        );
        return (
          messages.some((e) => e.event.payload.event?.event?.kind === "wait_resolved") &&
          messages.some((e) => e.event.payload.reason === "stream_ended")
        );
      })
      .toBe(true);
    await expect(
      call("flow/node.signal", { request_id: "late", payload: "late" }),
    ).rejects.toThrow();
  } finally {
    await rpc("services.stop", { plugin_id: "resident-echo", instance: service.instance });
  }
});
