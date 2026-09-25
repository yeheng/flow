<script setup lang="ts">
import { nextTick, ref, watch } from "vue";
import { modalState, settleModal } from "../state/modal";

const text = ref("");
const inputEl = ref<HTMLInputElement | null>(null);

watch(
  () => modalState.current,
  async (cur) => {
    if (!cur) return;
    text.value = cur.defaultValue;
    if (cur.kind === "prompt") {
      await nextTick();
      inputEl.value?.focus();
      inputEl.value?.select();
    }
  },
);

function ok(): void {
  settleModal(modalState.current?.kind === "prompt" ? text.value : "");
}

function cancel(): void {
  settleModal(null);
}
</script>

<template>
  <div v-if="modalState.current" class="modal-overlay" @click.self="cancel">
    <div class="modal">
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
        <button class="primary" @click="ok">确定</button>
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
