<script setup lang="ts">
import { computed, nextTick, ref, watch } from "vue";
import { editor } from "../state/editor";
import { overlaysOf, type OverlayEntry } from "../state/overlays";
import { useModalFocusTrap } from "../state/modal-a11y";
import {
  saveSelectionAsTemplate,
  templateCategoryOptions,
  type TemplateSavePayload,
} from "../state/templates";
import AppButton from "./AppButton.vue";

/**
 * 「存为业务节点」对话框（全局弹层栈 kind="biz-save" 的渲染方）：把选中的节点
 * （单个或子图，含参数配置与内部连线）保存为可复用业务节点，插入到面板「业务节点」
 * 区供拖拽/点击复用。Esc 由全局弹层栈关栈顶（=取消），此处不再自己监听。
 */
const name = ref("");
const category = ref("业务");
const saving = ref(false);
const nameEl = ref<HTMLInputElement | null>(null);
let restoreFocus: HTMLElement | null = null;

/** openTemplateSave 是唯一入口，同刻至多一条 */
const entry = computed<OverlayEntry<TemplateSavePayload> | null>(
  () => overlaysOf<TemplateSavePayload>("biz-save")[0] ?? null,
);
const open = computed(() => !!entry.value);

// Tab 焦点圈定在弹层内（此前没有陷阱，Tab 会穿透到背后页面）
const dialogEl = ref<HTMLElement | null>(null);
useModalFocusTrap(dialogEl, open);

const selectedCount = computed(() => editor.nodes.filter((n) => n.selected).length);
const categoryOptions = computed(() => templateCategoryOptions());
const canSave = computed(() => name.value.trim().length > 0 && !saving.value);

watch(
  () => entry.value?.id,
  async (id) => {
    if (id !== undefined) {
      restoreFocus = (document.activeElement as HTMLElement) ?? null;
      name.value = entry.value?.payload.defaultName ?? "";
      category.value = "业务";
      await nextTick();
      nameEl.value?.focus();
      nameEl.value?.select();
    } else {
      restoreFocus?.focus?.();
      restoreFocus = null;
    }
  },
  { immediate: true },
);

/** 取消：保存中禁止取消 */
function cancel(): void {
  if (saving.value) return;
  entry.value?.close(undefined);
}

async function submit(): Promise<void> {
  const target = entry.value;
  if (!target || !canSave.value) return;
  saving.value = true;
  try {
    const created = await saveSelectionAsTemplate(name.value, category.value.trim() || null);
    if (created) target.close(); // 保存成功收场（Esc 先行关闭时 close 幂等，不再回调）
  } finally {
    saving.value = false;
  }
}
</script>

<template>
  <div v-if="open" class="biz-overlay" @click.self="cancel">
    <form
      ref="dialogEl"
      class="biz-modal"
      role="dialog"
      aria-modal="true"
      aria-labelledby="biz-title"
      @submit.prevent="submit"
    >
      <header class="biz-header">
        <h3 id="biz-title">存为业务节点</h3>
        <button
          type="button"
          class="biz-close"
          title="关闭 (Esc)"
          :disabled="saving"
          @click="cancel"
        >
          ×
        </button>
      </header>
      <div class="biz-body">
        <div class="field">
          <label for="biz-name">名称</label>
          <input
            id="biz-name"
            ref="nameEl"
            v-model="name"
            type="text"
            spellcheck="false"
            placeholder="如：发票校验"
          />
        </div>
        <div class="field">
          <label for="biz-category">分类</label>
          <input
            id="biz-category"
            v-model="category"
            type="text"
            list="biz-category-options"
            spellcheck="false"
            placeholder="业务"
          />
          <datalist id="biz-category-options">
            <option v-for="c in categoryOptions" :key="c" :value="c" />
          </datalist>
        </div>
        <p class="muted biz-hint">
          将保存 {{ selectedCount }} 个选中节点及其内部连线（含参数配置）；未选中节点时不会保存。
          保存后可在「业务节点」区拖拽或点击插入任意流程。
        </p>
      </div>
      <footer class="biz-footer">
        <AppButton type="button" :disabled="saving" @click="cancel">取消</AppButton>
        <AppButton type="submit" variant="primary" :disabled="!canSave">
          {{ saving ? "保存中…" : "保存" }}
        </AppButton>
      </footer>
    </form>
  </div>
</template>

<style scoped>
.biz-overlay {
  position: fixed;
  inset: 0;
  z-index: 88;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: flex-start;
  justify-content: center;
  padding-top: 18vh;
}

.biz-modal {
  width: 380px;
  max-width: calc(100vw - 32px);
  display: flex;
  flex-direction: column;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: var(--radius);
  box-shadow: 0 8px 32px var(--shadow-hover);
}

.biz-header {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 16px;
  border-bottom: 1px solid var(--border);
}

.biz-header h3 {
  margin: 0;
  font-size: 14px;
}

.biz-close {
  margin-left: auto;
  padding: 0 8px;
  border: none;
  background: none;
  font-size: 18px;
  line-height: 1.2;
  color: var(--text2);
}

.biz-close:hover:not(:disabled) {
  color: var(--text);
  background: var(--surface2);
}

.biz-body {
  padding: 14px 16px;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.biz-hint {
  margin: 0;
  font-size: 12px;
  line-height: 1.5;
}

.biz-footer {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
  padding: 10px 16px;
  border-top: 1px solid var(--border);
}
</style>
