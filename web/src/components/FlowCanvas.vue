<script setup lang="ts">
import { computed, watch } from "vue";
import { MarkerType, VueFlow, useVueFlow } from "@vue-flow/core";
import type { EdgeChange, NodeChange, NodeMouseEvent } from "@vue-flow/core";
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

// 只把结构/位置变化计入 dirty，选中高亮不算改动
function onNodesChange(changes: NodeChange[]): void {
  if (changes.some((c) => c.type === "remove" || (c.type === "position" && !c.dragging))) {
    editor.dirty = true;
  }
}

function onEdgesChange(changes: EdgeChange[]): void {
  if (changes.some((c) => c.type === "remove")) editor.dirty = true;
}

// 运行中节点的下游边做流动动画
const runningNodeIds = computed(() => {
  if (!monitor.runId || monitor.workflowId !== editor.workflowId) return new Set<string>();
  return new Set(
    monitor.nodes
      .filter((n) => n.state === "running" || n.state === "retrying")
      .map((n) => n.id),
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
  <div class="canvas-wrap" @drop="onDrop" @dragover.prevent>
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
      @pane-click="editor.selectedNodeId = null"
      @nodes-change="onNodesChange"
      @edges-change="onEdgesChange"
    >
      <template #node-flow="nodeProps">
        <FlowNode v-bind="nodeProps" />
      </template>
      <Background :variant="BackgroundVariant.Dots" :gap="28" :size="1.5" color="rgba(255, 255, 255, 0.06)" />
      <Controls />
      <MiniMap />
    </VueFlow>
    <div v-if="!editor.workflowId" class="canvas-hint">在左侧新建或选择一个工作流</div>
  </div>
</template>
