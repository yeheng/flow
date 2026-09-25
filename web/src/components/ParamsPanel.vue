<script setup lang="ts">
import { computed } from "vue";
import { selectedNode } from "../state/editor";
import { commitParams } from "../state/history";
import SchemaField from "./SchemaField.vue";

const node = selectedNode;
const data = computed(() => node.value?.data ?? null);
const desc = computed(() => data.value?.nodeType ?? null);
const schema = computed(() => desc.value?.params_schema ?? null);

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
  setParam("retry", { ...retry.value, [key]: Number.isNaN(v) ? undefined : v });
}
</script>

<template>
  <div class="params-panel">
    <h3>参数</h3>
    <template v-if="node && data && desc">
      <div class="field">
        <label>节点 id</label>
        <div class="field-static">{{ node.id }}</div>
      </div>
      <div class="field">
        <label>名称</label>
        <input type="text" :value="data.name" @input="onName" />
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
    <p v-else class="params-hint">点击画布中的节点编辑参数</p>
  </div>
</template>

<style scoped>
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
