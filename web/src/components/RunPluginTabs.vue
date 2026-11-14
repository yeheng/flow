<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch, computed } from "vue";
import { client } from "../rpc/client";
import type { ResidentService } from "../types";
import PluginServiceFrame from "./PluginServiceFrame.vue";
const props = defineProps<{ runId: string }>();
const emit = defineEmits<{ active: [value: boolean] }>();
const services = ref<ResidentService[]>([]),
  selected = ref("");
const error = ref("");
const current = computed(() => services.value.find((s) => s.instance === selected.value));
let disposed = false,
  busy = false,
  timer: ReturnType<typeof setInterval> | undefined;
watch(current, (value) => emit("active", !!value));
watch(
  () => props.runId,
  () => {
    services.value = [];
    selected.value = "";
    void refresh();
  },
);
async function refresh() {
  if (busy) return;
  busy = true;
  const runId = props.runId;
  try {
    const result = await client.call<{ services: ResidentService[] }>("services.list", {});
    if (disposed || props.runId !== runId) return;
    services.value = result.services.filter((s) => s.alive && s.ui && s.binding?.run_id === runId);
    if (!services.value.some((s) => s.instance === selected.value)) selected.value = "";
  } catch {
    if (!disposed) {
      services.value = [];
      selected.value = "";
    }
  } finally {
    busy = false;
  }
}
onMounted(() => {
  void refresh();
  timer = setInterval(() => {
    void refresh();
  }, 1000);
});
onUnmounted(() => {
  disposed = true;
  if (timer) clearInterval(timer);
  emit("active", false);
});
</script>
<template>
  <section v-if="services.length" class="plugin-tabs" :class="{ expanded: !!current }">
    <div role="tablist" aria-label="运行视图">
      <button
        role="tab"
        :aria-selected="!current"
        @click="
          selected = '';
          error = '';
        "
      >
        运行
      </button>
      <button
        v-for="s in services"
        :key="s.instance"
        role="tab"
        :aria-selected="selected === s.instance"
        @click="
          selected = s.instance;
          error = '';
        "
      >
        {{ s.plugin_id }} · {{ s.binding?.node_id }}
      </button>
    </div>
    <p v-if="error" role="alert">{{ error }}</p>
    <PluginServiceFrame
      v-if="current"
      :key="current.instance"
      :service="current"
      @ended="
        selected = '';
        refresh();
      "
      @error="error = $event"
    />
  </section>
</template>
<style scoped>
.plugin-tabs {
  padding: 8px 16px;
  border-bottom: 1px solid var(--border);
}
.expanded {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
}
[role="tablist"] {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}
button {
  padding: 6px 12px;
}
button[aria-selected="true"] {
  border-color: var(--accent);
  color: var(--accent);
}
.expanded :deep(iframe) {
  flex: 1;
  height: 100%;
  min-height: 320px;
}
[role="alert"] {
  color: var(--danger);
}
</style>
