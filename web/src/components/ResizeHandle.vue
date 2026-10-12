<script setup lang="ts">
defineProps<{ orientation: "vertical" | "horizontal" }>();
const emit = defineEmits<{ dragstart: [PointerEvent] }>();
</script>

<template>
  <div class="resize-handle" :class="orientation" @pointerdown="emit('dragstart', $event)" />
</template>

<style scoped>
.resize-handle {
  position: absolute;
  z-index: 5;
}

/* 贴哪条边由使用方用 fallthrough class 定位（edge-top/left/right） */
.resize-handle.vertical {
  top: 0;
  bottom: 0;
  width: 5px;
  cursor: col-resize;
}

.resize-handle.horizontal {
  left: 0;
  right: 0;
  height: 5px;
  cursor: row-resize;
}

.resize-handle:hover {
  background: rgba(124, 108, 255, 0.5);
}
</style>
