<script setup lang="ts">
import { editor } from "../state/editor";
import type { NodeTypeDesc } from "../types";

function onDragStart(event: DragEvent, nt: NodeTypeDesc): void {
  if (!event.dataTransfer) return;
  event.dataTransfer.setData("application/flow-node-type", nt.type);
  event.dataTransfer.effectAllowed = "move";
}
</script>

<template>
  <div class="palette">
    <h3>节点</h3>
    <div
      v-for="nt in editor.nodeTypes"
      :key="nt.type"
      class="palette-item"
      :class="`cat-${nt.category}`"
      draggable="true"
      @dragstart="onDragStart($event, nt)"
    >
      {{ nt.label }}
    </div>
    <p class="palette-hint">拖拽到画布添加节点</p>
  </div>
</template>
