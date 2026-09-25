<script setup lang="ts">
import { dismissToast, toasts } from "../state/toast";
</script>

<template>
  <div class="toasts">
    <TransitionGroup name="toast">
      <div v-for="t in toasts" :key="t.id" class="toast" :class="`toast-${t.kind}`">
        <span class="toast-text">{{ t.text }}</span>
        <button class="toast-close" title="关闭" @click="dismissToast(t.id)">×</button>
      </div>
    </TransitionGroup>
  </div>
</template>

<style scoped>
.toasts {
  position: fixed;
  top: 56px;
  right: 16px;
  z-index: 100;
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-width: 360px;
}

.toast {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 8px 10px;
  border-radius: 8px;
  background: var(--surface2);
  border: 1px solid var(--border2);
  box-shadow: 0 4px 20px var(--shadow-hover);
  font-size: 12px;
}

.toast-info {
  border-left: 3px solid var(--accent);
}

.toast-success {
  border-left: 3px solid var(--ok);
}

.toast-error {
  border-left: 3px solid var(--danger);
}

.toast-text {
  flex: 1;
  word-break: break-all;
}

.toast-close {
  border: none;
  background: none;
  color: var(--text3);
  padding: 0 4px;
}

.toast-close:hover {
  background: none;
  color: var(--text);
}

.toast-enter-active,
.toast-leave-active {
  transition:
    opacity 0.2s,
    transform 0.2s;
}

.toast-enter-from,
.toast-leave-to {
  opacity: 0;
  transform: translateX(12px);
}
</style>
