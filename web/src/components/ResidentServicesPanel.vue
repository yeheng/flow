<script setup lang="ts">
import { ref, onMounted, onUnmounted } from "vue";
import { client, errText } from "../rpc/client";
import PluginServiceFrame from "./PluginServiceFrame.vue";
import type { ResidentService as Service } from "../types";
import AppButton from "./AppButton.vue";

const services = ref<Service[]>([]),
  pluginId = ref(""),
  error = ref(""),
  busy = ref(false);
const boundRun = ref(""),
  boundNode = ref("");
const selected = ref<Service | null>(null);
let disposed = false;
function close() {
  selected.value = null;
}
async function refresh() {
  try {
    const r = await client.call<{ services: Service[] }>("services.list", {});
    if (disposed) return;
    services.value = r.services;
    if (
      selected.value &&
      !r.services.some((s) => s.instance === selected.value?.instance && s.alive)
    )
      close();
  } catch (e) {
    if (!disposed) error.value = errText(e);
  }
}
async function action(method: "services.start" | "services.stop", service?: Service) {
  busy.value = true;
  error.value = "";
  try {
    await client.call(
      method,
      service
        ? { plugin_id: service.plugin_id, instance: service.instance }
        : {
            plugin_id: pluginId.value,
            ...(boundRun.value && boundNode.value
              ? { binding: { run_id: boundRun.value, node_id: boundNode.value } }
              : {}),
          },
    );
    await refresh();
  } catch (e) {
    error.value = errText(e);
  } finally {
    busy.value = false;
  }
}
function open(service: Service) {
  close();
  selected.value = service;
}
onMounted(() => {
  void refresh();
});
onUnmounted(() => {
  disposed = true;
  close();
});
</script>
<template>
  <section class="resident-services">
    <h3>插件服务</h3>
    <p>服务持续运行，关闭页面不会停止。启动、停止和调用需要管理员权限。</p>
    <form @submit.prevent="action('services.start')">
      <label>插件 ID <input v-model="pluginId" required :disabled="busy" /></label
      ><label>绑定运行（可选）<input v-model="boundRun" :disabled="busy" /></label
      ><label>绑定节点<input v-model="boundNode" :disabled="busy" :required="!!boundRun" /></label
      ><AppButton :disabled="busy">启动服务</AppButton
      ><AppButton type="button" :disabled="busy" @click="refresh">刷新</AppButton>
    </form>
    <ul>
      <li v-for="s in services" :key="s.instance">
        {{ s.plugin_id }} · {{ s.alive ? "运行中" : "已退出" }}
        <AppButton v-if="s.alive && s.ui" :disabled="busy" @click="open(s)">打开界面</AppButton
        ><AppButton :disabled="busy" @click="action('services.stop', s)">{{
          s.alive ? "停止" : "移除"
        }}</AppButton>
      </li>
    </ul>
    <p v-if="error" role="alert">{{ error }}</p>
    <template v-if="selected?.ui"
      ><AppButton @click="close">关闭界面</AppButton
      ><PluginServiceFrame
        :key="selected.instance"
        :service="selected"
        @ended="
          close();
          refresh();
        "
        @error="error = $event"
    /></template>
  </section>
</template>
<style scoped>
.resident-services {
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 16px;
  margin: 16px 0;
}
form,
li {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
  margin: 8px 0;
}

[role="alert"] {
  color: var(--danger);
}
</style>
