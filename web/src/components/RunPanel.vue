<script setup lang="ts">
import { monitor } from "../state/monitor";
import RunMonitor from "./RunMonitor.vue";
import AppButton from "./AppButton.vue";

withDefaults(defineProps<{ layout?: "footer" | "sidebar" }>(), { layout: "footer" });
defineEmits<{ collapse: []; "toggle-layout": [] }>();
</script>

<template>
  <div class="run-panel" :class="layout">
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
      <AppButton
        :title="layout === 'footer' ? '切换为右侧栏' : '切换为底部栏'"
        @click="$emit('toggle-layout')"
      >
        {{ layout === "footer" ? "侧栏 ⇥" : "底部 ⇣" }}
      </AppButton>
      <AppButton title="收起运行面板" @click="$emit('collapse')">收起 ▾</AppButton>
    </div>
    <div class="run-panel-body">
      <RunMonitor v-if="monitor.runId" :layout="layout" />
      <p v-else class="run-hint">在顶部工具栏点击「运行」启动最新已发布版本，面板随运行自动展开</p>
    </div>
  </div>
</template>

<style scoped>
.run-panel {
  flex: 1;
  min-height: 0;
  min-width: 0;
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

/* 侧栏模式宽度有限：输入框独占一行，标题与按钮在第一行 */
.run-panel.sidebar .run-panel-head {
  flex-wrap: wrap;
}

.run-panel.sidebar .run-panel-head h3 {
  flex: 1;
}

.run-panel.sidebar .run-input {
  flex-basis: 100%;
  order: 1;
}

.run-panel-body {
  flex: 1;
  min-height: 0;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  margin-top: 8px;
}
</style>
