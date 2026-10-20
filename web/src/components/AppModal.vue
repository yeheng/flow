<script setup lang="ts">
import { computed, nextTick, onMounted, onUnmounted, ref, useId, watch } from "vue";
import { modalEntries, settleModal, type ModalEntry } from "../state/modal";
import { trapTabIn } from "../state/modal-a11y";
import AppButton from "./AppButton.vue";

const text = ref("");
const inputEl = ref<HTMLInputElement | null>(null);
const okEl = ref<InstanceType<typeof AppButton> | null>(null);
const dialogEl = ref<HTMLElement | null>(null);
const titleId = useId();
/** 打开弹窗前的焦点目标：关闭时还原（否则焦点被弹窗吃掉后掉回 body） */
const prevFocus = ref<HTMLElement | null>(null);

/** 打开语义保证同刻至多一条；Esc 由全局弹层栈统一关栈顶，此处不再监听 */
const current = computed<ModalEntry | null>(() => modalEntries()[0] ?? null);

watch(
  () => current.value?.id,
  async (id) => {
    if (id !== undefined) {
      if (!prevFocus.value) prevFocus.value = (document.activeElement as HTMLElement | null) ?? null;
      text.value = current.value?.payload.defaultValue ?? "";
      await nextTick();
      if (current.value?.payload.kind === "prompt") {
        inputEl.value?.focus();
        inputEl.value?.select();
      } else {
        okEl.value?.focus();
      }
    } else {
      const el = prevFocus.value;
      prevFocus.value = null;
      el?.focus();
    }
  },
);

function ok(): void {
  const entry = current.value;
  if (entry) settleModal(entry, entry.payload.kind === "prompt" ? text.value : "");
}

function cancel(): void {
  const entry = current.value;
  if (entry) settleModal(entry, null);
}

// keydown 挂 window：点过消息区等非可聚焦区域后 Enter/Tab 仍响应（Esc 走弹层栈）
function onWindowKeydown(e: KeyboardEvent): void {
  const entry = current.value;
  if (!entry) return;
  if (e.key === "Tab") {
    trapTabIn(dialogEl.value, e);
    return;
  }
  // Enter 确认只对 confirm 生效；焦点在按钮上时交给按钮自身，避免双触发
  if (e.key === "Enter" && entry.payload.kind === "confirm" && !isButton(e.target)) {
    ok();
  }
}

function isButton(t: EventTarget | null): boolean {
  return t instanceof HTMLElement && t.closest("button") !== null;
}


onMounted(() => window.addEventListener("keydown", onWindowKeydown));
onUnmounted(() => window.removeEventListener("keydown", onWindowKeydown));
</script>

<template>
  <div v-if="current" class="modal-overlay" @click.self="cancel">
    <div ref="dialogEl" class="modal" role="dialog" aria-modal="true" :aria-labelledby="titleId">
      <p :id="titleId" class="modal-message">{{ current.payload.message }}</p>
      <input
        v-if="current.payload.kind === 'prompt'"
        ref="inputEl"
        v-model="text"
        type="text"
        @keyup.enter="ok"
      />
      <div class="modal-actions">
        <AppButton @click="cancel">取消</AppButton>
        <AppButton ref="okEl" :variant="current.payload.danger ? 'danger' : 'primary'" @click="ok">
          {{ current.payload.confirmText ?? "确定" }}
        </AppButton>
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
