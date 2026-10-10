import { defineConfig } from "@playwright/test";
import { E2E_TOKEN } from "./e2e/config";

// e2e 用独立的 flow-journal-server（独立 Journal/密钥目录，端口 193xx）+ 独立 vite，互不干扰也不碰 产品数据目录。
// flow-journal-server 生命周期在 global-setup/global-teardown（进程 spawn），vite 由 webServer 托管。
export const RPC_URL = "ws://127.0.0.1:19311";
export const HTTP_URL = "http://127.0.0.1:19312";
const WEB_URL = "http://127.0.0.1:19313";

export default defineConfig({
  testDir: "./e2e",
  timeout: 60_000,
  retries: 0,
  // 全部用例共享同一个 e2e flow-journal-server（种子按 workflow 隔离）；
  // 串行执行：dashboard 断言全局 run.stats，并行会互相改数
  workers: 1,
  globalSetup: "./e2e/global-setup.ts",
  globalTeardown: "./e2e/global-teardown.ts",
  reporter: "list",
  use: {
    baseURL: WEB_URL,
  },
  webServer: {
    command: "npm run dev -- --port 19313 --strictPort --host 127.0.0.1",
    url: WEB_URL,
    reuseExistingServer: false,
    timeout: 60_000,
    env: {
      VITE_FLOW_RPC: RPC_URL,
      VITE_FLOW_HTTP: HTTP_URL,
      VITE_FLOW_TOKEN: E2E_TOKEN,
    },
  },
});
