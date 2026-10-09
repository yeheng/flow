<script setup lang="ts">
import { nextTick, ref, watch } from "vue";
import { modalState, settleModal } from "../state/modal";

const text = ref("");
const inputEl = ref<HTMLInputElement | null>(null);
const okEl = ref<HTMLButtonElement | null>(null);

watch(
  () => modalState.current,
  async (cur) => {
    if (!cur) return;
    text.value = cur.defaultValue;
    await nextTick();
    if (cur.kind === "prompt") {
      inputEl.value?.focus();
      inputEl.value?.select();
    } else {
      okEl.value?.focus();
    }
  },
);

function ok(): void {
  settleModal(modalState.current?.kind === "prompt" ? text.value : "");
}

function cancel(): void {
  settleModal(null);
}

function onKeydown(e: KeyboardEvent): void {
  if (e.key === "Escape") cancel();
  else if (e.key === "Enter" && modalState.current?.kind === "confirm") ok();
}
</script>

<template>
  <div v-if="modalState.current" class="modal-overlay" @click.self="cancel" @keydown="onKeydown">
    <div class="modal" role="dialog" aria-modal="true">
      <p class="modal-message">{{ modalState.current.message }}</p>
      <input
        v-if="modalState.current.kind === 'prompt'"
        ref="inputEl"
        v-model="text"
        type="text"
        @keyup.enter="ok"
        @keyup.esc="cancel"
      />
      <div class="modal-actions">
        <button @click="cancel">取消</button>
        <button
          ref="okEl"
          :class="modalState.current.danger ? 'danger' : 'primary'"
          @click="ok"
        >
          {{ modalState.current.confirmText ?? "确定" }}
        </button>
      </div>
    </div>
  </div>
</template>

<style scoped>
.modal-overlay {
  position: fixed;
  inset: 0;
  z-index: 90;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: flex-start;
  justify-content: center;
  padding-top: 18vh;
}

.modal {
  width: 360px;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: var(--radius);
  box-shadow: 0 8px 32px var(--shadow-hover);
  padding: 16px;
}

.modal-message {
  margin: 0 0 12px;
}

.modal-actions {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  margin-top: 14px;
}
</style>
