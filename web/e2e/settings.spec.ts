import { expect, test } from "@playwright/test";
import { E2E_TOKEN } from "./config";
import { rpc } from "./helpers";

test("设置：认证失败后可配置 token，并保存 V2 配置", async ({ page }) => {
  await page.addInitScript(() => {
    if (localStorage.getItem("flow.token") === null) {
      localStorage.setItem("flow.token", "invalid-token");
    }
  });
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

test("设置：分标签管理运行环境、变量和密钥", async ({ page }) => {
  await page.goto("/settings");
  const tab = (name: string) => page.getByRole("tab", { name, exact: true });
  const dialog = page.getByRole("dialog");

  await tab("运行环境").click();
  await expect(page.getByLabel("API token")).toHaveCount(0);
  await page.getByRole("button", { name: "新增环境" }).click();
  await dialog.getByLabel("环境 ID").fill("settings-test-env");
  await dialog.getByLabel("环境名称").fill("设置测试环境");
  await dialog.getByRole("button", { name: "保存环境" }).click();
  const envRow = page.getByRole("row").filter({ hasText: "settings-test-env" });
  await expect(envRow).toContainText("v1");
  await envRow.getByRole("button", { name: "编辑", exact: true }).click();
  await dialog.getByLabel("环境名称").fill("设置测试环境二");
  await dialog.getByRole("button", { name: "保存环境" }).click();
  await expect(envRow).toContainText("设置测试环境二");

  await tab("环境变量").click();
  await page.getByRole("button", { name: "新增变量" }).click();
  await dialog.getByLabel("变量名称").fill("retry_count");
  await dialog.getByLabel("变量值").fill("3");
  await dialog.getByRole("button", { name: "保存变量" }).click();
  const variable = page.getByRole("row").filter({ hasText: "retry_count" });
  await expect(variable).toContainText("3");
  await variable.getByRole("button", { name: "编辑", exact: true }).click();
  await dialog.getByLabel("变量值").fill("5");
  await dialog.getByRole("button", { name: "保存变量" }).click();
  await expect(variable).toContainText("5");

  await tab("密钥").click();
  await expect(page.getByLabel("运行环境")).toHaveValue("settings-test-env");
  const secret = page.getByRole("row").filter({ hasText: "SETTINGS_TEST_KEY" });
  await page.getByRole("button", { name: "新增密钥" }).click();
  await dialog.getByLabel("密钥名称").fill("SETTINGS_TEST_KEY");
  await dialog.getByLabel("新密钥值").fill("sample-value");
  await dialog.getByRole("button", { name: "保存密钥" }).click();
  await expect(secret).toContainText("加密存储");
  await secret.getByRole("button", { name: "更新值" }).click();
  await expect(dialog.getByLabel("新密钥值")).toHaveValue("");
  await dialog.getByLabel("新密钥值").fill("rotated-value");
  await dialog.getByRole("button", { name: "保存密钥" }).click();
  await expect(page.getByText("密钥已保存", { exact: true }).last()).toBeVisible();
  await expect(secret).not.toContainText("rotated-value");
  const listing = await rpc<{ environments: { id: string; config: unknown; secrets: string[] }[] }>(
    "environments.list", {},
  );
  expect(listing.environments.find((item) => item.id === "settings-test-env")).toMatchObject({
    config: { retry_count: 5 },
    secrets: ["SETTINGS_TEST_KEY"],
  });
  expect(JSON.stringify(listing)).not.toContain("rotated-value");
  await secret.getByRole("button", { name: "删除", exact: true }).click();
  await dialog.getByRole("button", { name: "确定", exact: true }).click();
  await expect(secret).toHaveCount(0);

  await page.getByLabel("运行环境").selectOption("default");
  const defaultSecret = page.getByRole("row").filter({ hasText: "SETTINGS_DEFAULT_KEY" });
  await page.getByRole("button", { name: "新增密钥" }).click();
  await dialog.getByLabel("密钥名称").fill("SETTINGS_DEFAULT_KEY");
  await dialog.getByLabel("新密钥值").fill("default-sample-value");
  await dialog.getByRole("button", { name: "保存密钥" }).click();
  await expect(defaultSecret).toContainText("加密存储");
  await defaultSecret.getByRole("button", { name: "删除", exact: true }).click();
  await dialog.getByRole("button", { name: "确定", exact: true }).click();
  await expect(defaultSecret).toHaveCount(0);

  await tab("环境变量").click();
  await variable.getByRole("button", { name: "删除", exact: true }).click();
  await dialog.getByRole("button", { name: "确定", exact: true }).click();
  await expect(variable).toHaveCount(0);
  await tab("运行环境").click();
  await envRow.getByRole("button", { name: "删除", exact: true }).click();
  await dialog.getByRole("button", { name: "确定", exact: true }).click();
  await expect(envRow).toHaveCount(0);
});
