<script setup lang="ts">
import { monitor } from "../state/monitor";
import RunMonitor from "./RunMonitor.vue";

defineEmits<{ collapse: [] }>();
</script>

<template>
  <div class="run-panel">
    <div class="run-panel-head">
      <h3>运行</h3>
      <textarea
        v-model="monitor.inputText"
        class="code run-input"
        title="运行输入 JSON（可选）"
        aria-label="运行输入 JSON（可选）"
        rows="1"
        spellcheck="false"
        placeholder='运行输入 JSON（可选），如 {"key": "value"}'
      ></textarea>
      <button title="收起运行面板" @click="$emit('collapse')">收起 ▾</button>
    </div>
    <div class="run-panel-body">
      <RunMonitor v-if="monitor.runId" />
      <p v-else class="run-hint">在顶部工具栏点击「运行」启动最新已发布版本，面板随运行自动展开</p>
    </div>
  </div>
</template>

<style scoped>
.run-panel {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
  padding: 10px 12px 8px;
}

.run-panel-head {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-shrink: 0;
}

.run-panel-head h3 {
  margin: 0;
  white-space: nowrap;
}

.run-input {
  flex: 1;
  min-width: 0;
  resize: none;
}

.run-panel-body {
  flex: 1;
  min-height: 0;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  margin-top: 8px;
}
</style>
