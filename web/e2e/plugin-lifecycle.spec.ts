import { expect, test } from "@playwright/test";
import { cpSync, mkdirSync, readFileSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { rpc } from "./helpers";

test("设置页：安装常驻插件，停止并卸载后撤下服务与页面", async ({ page }) => {
  const state = JSON.parse(readFileSync(path.join(tmpdir(), "flow-e2e-server.json"), "utf8")) as { dir: string };
  const id = "resident-install-test";
  const source = path.join(state.dir, "install-source");
  const repo = path.resolve(process.cwd(), "..");
  cpSync(path.join(repo, "examples/resident-echo"), source, { recursive: true });
  mkdirSync(path.join(source, "bin"), { recursive: true });
  cpSync(path.join(repo, "target/debug/examples/resident_echo"), path.join(source, "bin/resident_echo"));
  const manifest = readFileSync(path.join(source, "plugin.toml"), "utf8").replace('id = "resident-echo"', `id = "${id}"`);
  writeFileSync(path.join(source, "plugin.toml"), manifest);
  try {
    await page.goto("/settings");
    await page.getByLabel("宿主上的插件目录").fill(source);
    await page.getByRole("button", { name: "安装插件", exact: true }).click();
    const row = page.getByRole("row").filter({ hasText: id });
    await expect(row).toContainText("已加载");
    expect(existsSync(path.join(state.dir, "plugins", id, ".flow-install.json"))).toBe(true);
    const service = await rpc<{ instance: string }>("services.start", { plugin_id: id });
    await expect(rpc("plugins.install", { source, replace: true })).rejects.toThrow("-32012");
    await expect(rpc("plugins.uninstall", { plugin_id: id })).rejects.toThrow("service running");
    await page.reload();
    await expect(row).toContainText("运行中");
    await row.getByRole("button", { name: "停止并卸载", exact: true }).click();
    await page.getByRole("dialog").getByRole("button", { name: "停止并卸载", exact: true }).click();
    await expect(row).toHaveCount(0);
    expect(existsSync(path.join(state.dir, "plugins", id))).toBe(false);
    const services = await rpc<{ services: { plugin_id: string }[] }>("services.list");
    expect(services.services.some((s) => s.plugin_id === id)).toBe(false);
    await expect(rpc("services.call", { plugin_id: id, instance: service.instance, method: "echo", params: {} })).rejects.toThrow();
  } finally {
    await rpc("plugins.uninstall", { plugin_id: id, stop_first: true }).catch(() => {});
    rmSync(source, { recursive: true, force: true });
  }
});
