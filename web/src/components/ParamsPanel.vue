<script setup lang="ts">
import { computed, reactive, watch } from "vue";
import { editor, selectedNode } from "../state/editor";
import type { ParamSpec } from "../types";

const node = selectedNode;
const data = computed(() => node.value?.data ?? null);
const desc = computed(() => data.value?.nodeType ?? null);

/** JSON 参数编辑中的解析错误，key = 参数名；切换节点时清空 */
const jsonErrors = reactive<Record<string, string>>({});
watch(
  () => editor.selectedNodeId,
  () => {
    for (const k of Object.keys(jsonErrors)) delete jsonErrors[k];
  },
);

function onName(ev: Event): void {
  if (!data.value) return;
  data.value.name = (ev.target as HTMLInputElement).value;
  editor.dirty = true;
}

function setParam(name: string, value: unknown): void {
  if (!data.value) return;
  data.value.params[name] = value;
  editor.dirty = true;
}

function onText(spec: ParamSpec, ev: Event): void {
  setParam(spec.name, (ev.target as HTMLInputElement).value);
}

function onNumber(spec: ParamSpec, ev: Event): void {
  const v = (ev.target as HTMLInputElement).valueAsNumber;
  setParam(spec.name, Number.isNaN(v) ? undefined : v);
}

function jsonText(v: unknown): string {
  return v === undefined ? "" : JSON.stringify(v, null, 2);
}

function onJson(spec: ParamSpec, ev: Event): void {
  const raw = (ev.target as HTMLTextAreaElement).value.trim();
  if (!raw) {
    setParam(spec.name, undefined);
    delete jsonErrors[spec.name];
    return;
  }
  try {
    setParam(spec.name, JSON.parse(raw));
    delete jsonErrors[spec.name];
  } catch {
    // 解析失败不写回，保留输入框内容等用户修正
    jsonErrors[spec.name] = "JSON 格式错误，未保存此字段";
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
      <div v-for="spec in desc.params" :key="spec.name" class="field">
        <label>
          {{ spec.label }}
          <em v-if="spec.required" class="required">*</em>
        </label>
        <input
          v-if="spec.kind === 'text'"
          type="text"
          :value="(data.params[spec.name] as string) ?? ''"
          @input="onText(spec, $event)"
        />
        <input
          v-else-if="spec.kind === 'number'"
          type="number"
          :value="(data.params[spec.name] as number) ?? ''"
          @input="onNumber(spec, $event)"
        />
        <textarea
          v-else-if="spec.kind === 'code'"
          class="code"
          rows="5"
          spellcheck="false"
          :value="(data.params[spec.name] as string) ?? ''"
          @input="onText(spec, $event)"
        ></textarea>
        <template v-else-if="spec.kind === 'json'">
          <textarea
            class="code"
            rows="4"
            spellcheck="false"
            :value="jsonText(data.params[spec.name])"
            @change="onJson(spec, $event)"
          ></textarea>
          <div v-if="jsonErrors[spec.name]" class="field-error">{{ jsonErrors[spec.name] }}</div>
        </template>
        <select
          v-else-if="spec.kind === 'select'"
          :value="(data.params[spec.name] as string) ?? ''"
          @change="onText(spec, $event)"
        >
          <option v-for="opt in spec.options ?? []" :key="opt" :value="opt">{{ opt }}</option>
        </select>
        <div v-if="spec.help" class="field-help">{{ spec.help }}</div>
      </div>
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
