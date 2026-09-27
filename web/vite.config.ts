import { defineConfig } from "vitest/config";
import vue from "@vitejs/plugin-vue";

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
  },
  test: {
    environment: "jsdom",
    globals: true,
    // e2e/ 是 Playwright（test:e2e），不是 vitest 用例
    exclude: ["e2e/**", "node_modules/**", "dist/**"],
  },
});
