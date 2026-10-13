<script setup lang="ts">
import { computed } from "vue";
import { monitor } from "../state/monitor";
import { editor, flowToDefinition } from "../state/editor";
import { discoverInputKeys } from "../state/run-input";
import RunMonitor from "./RunMonitor.vue";
import WorkflowInputForm from "./WorkflowInputForm.vue";
import AppButton from "./AppButton.vue";

withDefaults(defineProps<{ layout?: "footer" | "sidebar" }>(), { layout: "footer" });
defineEmits<{ collapse: []; "toggle-layout": [] }>();

/**
 * 运行输入表单字段：扫当前画布定义的 input 引用。
 * 实际运行的是最新已发布版本，与画布有未发布差异时字段可能不完全对齐，
 * 此时可切 JSON 模式手动补齐。
 */
const inputKeys = computed(() => discoverInputKeys(flowToDefinition(), editor.nodeTypes));

/** 输入节点表单的预填来源：启动草稿里的输入（非法 JSON 时无预填） */
const launchedInput = computed(() => {
  try {
    return JSON.parse(monitor.inputText);
  } catch {
    return undefined;
  }
});
</script>

<template>
  <div class="run-panel" :class="layout">
    <div class="run-panel-head">
      <h3>运行</h3>
      <WorkflowInputForm v-model="monitor.inputText" class="run-input" :input-keys="inputKeys" />
      <AppButton
        :title="layout === 'footer' ? '切换为右侧栏' : '切换为底部栏'"
        @click="$emit('toggle-layout')"
      >
        {{ layout === "footer" ? "侧栏 ⇥" : "底部 ⇣" }}
      </AppButton>
      <AppButton title="收起运行面板" @click="$emit('collapse')">收起 ▾</AppButton>
    </div>
    <div class="run-panel-body">
      <RunMonitor v-if="monitor.runId" :layout="layout" :run-input="launchedInput" />
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
