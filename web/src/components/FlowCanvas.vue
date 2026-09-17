<script setup lang="ts">
import { VueFlow, useVueFlow } from "@vue-flow/core";
import type { EdgeChange, NodeChange, NodeMouseEvent } from "@vue-flow/core";
import { Background } from "@vue-flow/background";
import { Controls } from "@vue-flow/controls";
import { editor, isValidConnection, onConnect, addNode } from "../state/editor";
import FlowNode from "./FlowNode.vue";

const { screenToFlowCoordinate } = useVueFlow();

function onDrop(event: DragEvent): void {
  const type = event.dataTransfer?.getData("application/flow-node-type");
  if (!type) return;
  addNode(type, screenToFlowCoordinate({ x: event.clientX, y: event.clientY }));
}

function onNodeClick(e: NodeMouseEvent): void {
  editor.selectedNodeId = e.node.id;
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
</script>

<template>
  <div class="canvas-wrap" @drop="onDrop" @dragover.prevent>
    <VueFlow
      v-model:nodes="editor.nodes"
      v-model:edges="editor.edges"
      :is-valid-connection="isValidConnection"
      :delete-key-code="['Backspace', 'Delete']"
      :default-viewport="{ zoom: 1 }"
      :min-zoom="0.2"
      :max-zoom="2"
      fit-view-on-init
      @connect="onConnect"
      @node-click="onNodeClick"
      @pane-click="editor.selectedNodeId = null"
      @nodes-change="onNodesChange"
      @edges-change="onEdgesChange"
    >
      <template #node-flow="nodeProps">
        <FlowNode v-bind="nodeProps" />
      </template>
      <Background :gap="16" />
      <Controls />
    </VueFlow>
    <div v-if="!editor.workflowId" class="canvas-hint">在左侧新建或选择一个工作流</div>
  </div>
</template>
