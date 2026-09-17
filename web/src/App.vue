<script setup lang="ts">
import { computed, onMounted } from "vue";
import { client } from "./rpc/client";
import { initEditor, ui } from "./state/editor";
import WorkflowList from "./components/WorkflowList.vue";
import NodePalette from "./components/NodePalette.vue";
import FlowCanvas from "./components/FlowCanvas.vue";
import ParamsPanel from "./components/ParamsPanel.vue";
import RunPanel from "./components/RunPanel.vue";

const connected = computed(() => client.connected.value);

onMounted(() => {
  void initEditor();
});
</script>

<template>
  <header>
    <span class="logo">flow 流程编辑器</span>
    <span class="conn" :class="{ ok: connected }">
      {{ connected ? "已连接" : "未连接" }} {{ client.url }}
    </span>
    <span v-if="ui.error" class="banner error">{{ ui.error }}</span>
    <span v-else-if="ui.info" class="banner info">{{ ui.info }}</span>
  </header>
  <main>
    <aside class="left">
      <WorkflowList />
      <NodePalette />
    </aside>
    <section class="center">
      <FlowCanvas />
    </section>
    <aside class="right">
      <ParamsPanel />
      <RunPanel />
    </aside>
  </main>
</template>
