<script setup lang="ts">
import { computed } from "vue";
import { useRoute } from "vue-router";
import { client } from "./rpc/client";
import AppToasts from "./components/AppToasts.vue";
import AppModal from "./components/AppModal.vue";

const route = useRoute();
const connected = computed(() => client.connected.value);

/** 导航高亮：`/` 精确命中，其余按路径段前缀（/workflows/:id 归「工作流」、/runs/:id 归「运行记录」） */
function navActive(path: string, to: string): boolean {
  return to === "/" ? path === "/" : path === to || path.startsWith(`${to}/`);
}
</script>

<template>
  <header>
    <span class="logo">Flow</span>
    <nav class="nav">
      <RouterLink to="/" :class="{ 'nav-active': navActive(route.path, '/') }">仪表盘</RouterLink>
      <RouterLink to="/workflows" :class="{ 'nav-active': navActive(route.path, '/workflows') }">
        工作流
      </RouterLink>
      <RouterLink to="/runs" :class="{ 'nav-active': navActive(route.path, '/runs') }">
        运行记录
      </RouterLink>
      <RouterLink to="/journal" :class="{ 'nav-active': navActive(route.path, '/journal') }">
        事件审计
      </RouterLink>
      <RouterLink to="/settings" :class="{ 'nav-active': navActive(route.path, '/settings') }">
        设置
      </RouterLink>
    </nav>
    <!-- 业务 chrome 不常显 RPC 地址：连接态只留圆点+文案，地址进 tooltip -->
    <span class="conn" :class="{ ok: connected }" :title="client.url">
      <span class="conn-dot" aria-hidden="true" />
      {{ connected ? "已连接" : "未连接" }}
    </span>
  </header>
  <RouterView />
  <AppToasts />
  <AppModal />
</template>

<style scoped>
header {
  display: flex;
  align-items: center;
  gap: 16px;
  height: 48px;
  padding: 0 16px;
  flex-shrink: 0;
  background: var(--surface);
  border-bottom: 1px solid var(--border);
}

.logo {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 600;
  letter-spacing: 0.04em;
  flex-shrink: 0;
}

.logo::before {
  content: "";
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--accent);
  box-shadow: 0 0 8px var(--accent);
}

.nav {
  display: flex;
  gap: 14px;
  min-width: 0;
  overflow-x: auto;
  white-space: nowrap;
}

.nav a {
  color: var(--text2);
  text-decoration: none;
  font-weight: 500;
  padding: 3px 2px;
  border-bottom: 2px solid transparent;
}

.nav a:hover {
  color: var(--text);
}

/* 高亮由 nav-active 判定（useRoute 路径前缀），router-link-active 仅兜底 */
.nav a.router-link-active,
.nav a.nav-active {
  color: var(--text);
  border-bottom-color: var(--accent);
}

.conn {
  margin-left: auto;
  display: inline-flex;
  align-items: center;
  gap: 5px;
  color: var(--danger);
  font-size: 11px;
  font-family: var(--mono);
}

.conn.ok {
  color: var(--ok);
}

.conn-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: currentColor;
}
@media (max-width: 700px) {
  .conn {
    display: none;
  }
}
</style>
