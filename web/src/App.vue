<script setup lang="ts">
import { computed } from "vue";
import { client } from "./rpc/client";
import AppToasts from "./components/AppToasts.vue";
import AppModal from "./components/AppModal.vue";

const connected = computed(() => client.connected.value);
</script>

<template>
  <header>
    <span class="logo">Flow</span>
    <nav class="nav">
      <RouterLink to="/workflows">工作流</RouterLink>
      <RouterLink to="/runs">运行记录</RouterLink>
    </nav>
    <span class="conn" :class="{ ok: connected }">
      {{ connected ? "已连接" : "未连接" }} {{ client.url }}
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

.nav a.router-link-active {
  color: var(--text);
  border-bottom-color: var(--accent);
}

.conn {
  margin-left: auto;
  color: var(--danger);
  font-size: 11px;
  font-family: var(--mono);
}

.conn.ok {
  color: var(--ok);
}
</style>
