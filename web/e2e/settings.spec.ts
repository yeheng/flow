import { expect, test } from "@playwright/test";
import { E2E_TOKEN } from "./config";
import { rpc } from "./helpers";

test("设置：认证失败后可配置 token，并保存 V2 配置", async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem("flow.token", "invalid-token"));
  await page.goto("/settings");
  await expect(page.locator(".empty").filter({ hasText: "加载配置失败" })).toBeVisible();
  await page.getByLabel("API token").fill(E2E_TOKEN);
  await page.getByRole("button", { name: "保存并连接", exact: true }).click();
  await expect(page.locator(".empty").filter({ hasText: "加载配置失败" })).toHaveCount(0);
  const storage = page
    .locator("section.card")
    .filter({ has: page.getByRole("heading", { name: "存储", exact: true }) });
  await expect(storage.locator("select")).toHaveValue("journal");
  await expect(storage.locator("select option")).toHaveCount(1);
  const tick = page
    .locator(".field")
    .filter({ has: page.locator("label").filter({ hasText: "journal 触发扫描间隔" }) })
    .locator("input");
  await tick.fill("2");
  await page.getByRole("button", { name: "保存（重启后生效）", exact: true }).click();
  await expect(page.getByText("配置已保存，重启 flow 后生效", { exact: true })).toBeVisible();
  const view = await rpc<{ config: { server: { journal_trigger_tick_secs: number } } }>(
    "config.get",
    {},
  );
  expect(view.config.server.journal_trigger_tick_secs).toBe(2);
  expect(await page.evaluate(() => localStorage.getItem("flow.token"))).toBe(E2E_TOKEN);
});
