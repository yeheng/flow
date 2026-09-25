<script setup lang="ts">
import { computed, watch } from "vue";
import { MarkerType, VueFlow, useVueFlow } from "@vue-flow/core";
import type { NodeMouseEvent } from "@vue-flow/core";
import { Background, BackgroundVariant } from "@vue-flow/background";
import { Controls } from "@vue-flow/controls";
import { MiniMap } from "@vue-flow/minimap";
import {
  drillIntoSubWorkflow,
  editor,
  isValidConnection,
  jumpToBreadcrumb,
  onConnect,
  addNode,
} from "../state/editor";
import { beginDrag, commit, endDrag } from "../state/history";
import { monitor } from "../state/monitor";
import FlowNode from "./FlowNode.vue";

const { screenToFlowCoordinate } = useVueFlow();

const defaultEdgeOptions = {
  markerEnd: { type: MarkerType.ArrowClosed, color: "#7c6cff" },
};

function onDrop(event: DragEvent): void {
  const type = event.dataTransfer?.getData("application/flow-node-type");
  if (!type) return;
  addNode(type, screenToFlowCoordinate({ x: event.clientX, y: event.clientY }));
}

function onNodeClick(e: NodeMouseEvent): void {
  editor.selectedNodeId = e.node.id;
}

function onNodeDoubleClick(e: NodeMouseEvent): void {
  void drillIntoSubWorkflow(e.node.id);
}

// 删除键在 vue-flow 处理前先入 undo 栈（canvas-wrap 在冒泡路径上先于 window 监听器执行）
function onKeydown(e: KeyboardEvent): void {
  if (e.key !== "Delete" && e.key !== "Backspace") return;
  if (e.target instanceof HTMLElement && e.target.closest("input, textarea, [contenteditable]"))
    return;
  if (editor.nodes.some((n) => n.selected) || editor.edges.some((edge) => edge.selected)) commit();
}

// 运行中节点的下游边做流动动画
const runningNodeIds = computed(() => {
  if (!monitor.runId || monitor.workflowId !== editor.workflowId) return new Set<string>();
  return new Set(
    monitor.nodes.filter((n) => n.state === "running" || n.state === "retrying").map((n) => n.id),
  );
});
watch(
  runningNodeIds,
  (ids) => {
    for (const e of editor.edges) e.animated = ids.has(e.source);
  },
  { immediate: true },
);
</script>

<template>
  <div class="canvas-wrap" @drop="onDrop" @dragover.prevent @keydown="onKeydown">
    <div v-if="editor.breadcrumb.length" class="breadcrumb">
      <template v-for="(c, i) in editor.breadcrumb" :key="`${i}:${c.workflowId}`">
        <a class="crumb" @click="jumpToBreadcrumb(i)">{{ c.name || c.workflowId }}</a>
        <span class="crumb-sep">/</span>
      </template>
      <span class="crumb-current">{{ editor.workflowName || editor.workflowId }}</span>
    </div>
    <VueFlow
      v-model:nodes="editor.nodes"
      v-model:edges="editor.edges"
      :is-valid-connection="isValidConnection"
      :delete-key-code="['Backspace', 'Delete']"
      :selection-key-code="'Shift'"
      :multi-selection-key-code="['Meta', 'Control']"
      :default-viewport="{ zoom: 1 }"
      :default-edge-options="defaultEdgeOptions"
      :min-zoom="0.2"
      :max-zoom="2"
      :snap-to-grid="true"
      :snap-grid="[16, 16]"
      color-mode="dark"
      fit-view-on-init
      @connect="onConnect"
      @node-click="onNodeClick"
      @node-double-click="onNodeDoubleClick"
      @node-drag-start="beginDrag()"
      @node-drag-stop="endDrag()"
      @pane-click="editor.selectedNodeId = null"
    >
      <template #node-flow="nodeProps">
        <FlowNode v-bind="nodeProps" />
      </template>
      <Background
        :variant="BackgroundVariant.Dots"
        :gap="28"
        :size="1.5"
        color="rgba(255, 255, 255, 0.06)"
      />
      <Controls />
      <MiniMap />
    </VueFlow>
    <div v-if="!editor.workflowId" class="canvas-hint">在左侧新建或选择一个工作流</div>
  </div>
</template>

<style scoped>
.breadcrumb {
  position: absolute;
  top: 12px;
  left: 12px;
  z-index: 10;
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 5px 12px;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: 7px;
  box-shadow: 0 2px 12px var(--shadow);
  font-size: 12px;
}

.crumb {
  color: var(--accent2);
  cursor: pointer;
}

.crumb:hover {
  text-decoration: underline;
}

.crumb-sep {
  color: var(--text3);
}

.crumb-current {
  font-weight: 600;
}
</style>
