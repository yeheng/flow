import { createApp } from "vue";
import "@vue-flow/core/dist/style.css";
import "@vue-flow/core/dist/theme-default.css";
import "@vue-flow/controls/dist/style.css";
import "@vue-flow/minimap/dist/style.css";
import "./style.css";
import App from "./App.vue";
import { router } from "./router";
import { loadRuntimeConfig } from "./config";

// 先拉运行时配置（/config.json，可选，失败静默回落默认值）再挂载；
// RPC 地址在首次连接时惰性解析
void loadRuntimeConfig().finally(() => {
  createApp(App).use(router).mount("#app");
});
