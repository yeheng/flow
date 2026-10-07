import { defineConfig } from "vitest/config";
import vue from "@vitejs/plugin-vue";

export default defineConfig({
  plugins: [vue()],
  // Tauri waits for this exact URL; never silently move to another port.
  clearScreen: false,
  server: {
    host: "127.0.0.1",
    port: 5173,
    strictPort: true,
    watch: { ignored: ["**/src-tauri/**"] },
  },
  test: {
    environment: "jsdom",
    globals: true,
    // e2e/ 是 Playwright（test:e2e），不是 vitest 用例
    exclude: ["e2e/**", "node_modules/**", "dist/**"],
  },
});
