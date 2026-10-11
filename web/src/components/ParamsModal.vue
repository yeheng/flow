<script setup lang="ts">
import { computed, nextTick, ref, watch } from "vue";
import { editor, selectedNode } from "../state/editor";
import { commitParams } from "../state/history";
import SchemaField from "./SchemaField.vue";

const node = selectedNode;
const data = computed(() => node.value?.data ?? null);
const desc = computed(() => data.value?.nodeType ?? null);
const schema = computed(() => desc.value?.params_schema ?? null);
const nameInput = ref<HTMLInputElement>();

const open = computed(() => editor.paramsOpen && !!node.value);

// 右键「编辑参数」要直达改名：打开弹窗即聚焦名称输入框
watch(
  [open, () => node.value?.id],
  async ([isOpen], [, prevId], onCleanup) => {
    if (!isOpen) return;
    let cancelled = false;
    onCleanup(() => (cancelled = true));
    await nextTick();
    if (cancelled) return;
    nameInput.value?.focus();
    // 换节点重开时全选，便于直接覆盖
    if (prevId !== node.value?.id) nameInput.value?.select();
  },
  { immediate: true },
);

function close(): void {
  editor.paramsOpen = false;
}

function onName(ev: Event): void {
  if (!data.value || !node.value) return;
  // 同字段连续输入合并为一条 undo 历史
  commitParams(`${node.value.id}:__name__`);
  data.value.name = (ev.target as HTMLInputElement).value;
}

function setParam(name: string, value: unknown): void {
  if (!data.value || !node.value) return;
  commitParams(`${node.value.id}:${name}`);
  if (value === undefined) {
    delete data.value.params[name];
  } else {
    data.value.params[name] = value;
  }
}

const retry = computed(
  () => (data.value?.params.retry ?? {}) as { max_attempts?: number; backoff_ms?: number },
);

function setRetry(key: "max_attempts" | "backoff_ms", ev: Event): void {
  const v = (ev.target as HTMLInputElement).valueAsNumber;
  const next: Record<string, unknown> = { ...retry.value, [key]: Number.isNaN(v) ? undefined : v };
  // 两个字段都为空时整体删除 retry（空值语义在写入侧收敛，序列化器不认识具体参数名）
  const clean = Object.fromEntries(Object.entries(next).filter(([, x]) => x !== undefined));
  setParam("retry", Object.keys(clean).length > 0 ? clean : undefined);
}
</script>

<template>
  <div v-if="open" class="params-overlay" @click.self="close">
    <div class="params-modal" role="dialog" aria-modal="true" aria-label="编辑节点参数">
      <header class="params-header">
        <h3>编辑参数</h3>
        <span v-if="node && data" class="params-subtitle">
          {{ data.nodeType.label }} · {{ node.id }}
        </span>
        <button class="params-close" title="关闭 (Esc)" @click="close">×</button>
      </header>
      <div class="params-body">
        <template v-if="node && data && desc">
          <div class="field">
            <label>节点 id</label>
            <div class="field-static">{{ node.id }}</div>
          </div>
          <div class="field">
            <label for="node-name">名称</label>
            <input id="node-name" ref="nameInput" type="text" :value="data.name" @input="onName" />
          </div>
          <!-- key 带节点 id：切换节点时强制重建，清掉 json 等字段的本地编辑状态 -->
          <SchemaField
            v-for="(prop, key) in schema?.properties ?? {}"
            :key="`${node.id}:${key}`"
            :name="key"
            :schema="prop"
            :required="schema?.required?.includes(key)"
            :model-value="data.params[key]"
            @update:model-value="setParam(key, $event)"
          />
          <fieldset v-if="desc.supports_retry" class="retry">
            <legend>重试</legend>
            <div class="field">
              <label>最大尝试次数</label>
              <input
                type="number"
                min="1"
                :value="retry.max_attempts ?? ''"
                placeholder="1"
                @input="setRetry('max_attempts', $event)"
              />
            </div>
            <div class="field">
              <label>退避（毫秒）</label>
              <input
                type="number"
                min="0"
                :value="retry.backoff_ms ?? ''"
                placeholder="0"
                @input="setRetry('backoff_ms', $event)"
              />
            </div>
          </fieldset>
        </template>
      </div>
      <footer class="params-footer">
        <span class="muted params-hint">修改即时生效，⌘Z 可撤销</span>
        <button class="primary" @click="close">完成</button>
      </footer>
    </div>
  </div>
</template>

<style scoped>
.params-overlay {
  position: fixed;
  inset: 0;
  z-index: 80;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
}

.params-modal {
  width: 65vw;
  max-height: 82vh;
  display: flex;
  flex-direction: column;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: var(--radius);
  box-shadow: 0 8px 32px var(--shadow-hover);
}

.params-header {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 16px;
  border-bottom: 1px solid var(--border);
}

.params-header h3 {
  margin: 0;
  font-size: 14px;
}

.params-subtitle {
  color: var(--text2);
  font-size: 12px;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.params-close {
  margin-left: auto;
  padding: 0 8px;
  border: none;
  background: none;
  font-size: 18px;
  line-height: 1.2;
  color: var(--text2);
}

.params-close:hover {
  color: var(--text);
  background: var(--surface2);
}

.params-body {
  padding: 14px 16px;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.params-footer {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 16px;
  border-top: 1px solid var(--border);
}

.params-footer .params-hint {
  margin-right: auto;
  font-size: 12px;
}

fieldset.retry {
  border: 1px solid var(--border);
  border-radius: 6px;
  margin: 0;
  padding: 8px;
}

fieldset.retry legend {
  color: var(--text2);
  font-size: 12px;
}
</style>
