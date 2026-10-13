<script setup lang="ts">
/**
 * 工作流输入表单：discoverInputKeys 发现的字段各一个输入框，
 * 值组装成 JSON 文本写入 v-model（与原始 JSON 文本域同一数据源，
 * startRun / schedule.create 的解析链路不变）；无字段时退化为纯 JSON 文本域。
 */
import { reactive, ref, watch } from "vue";
import { assembleInput } from "../state/run-input";

const props = withDefaults(
  defineProps<{
    inputKeys: string[];
    modelValue: string;
    rows?: number;
    placeholder?: string;
  }>(),
  { rows: 1, placeholder: '运行输入 JSON（可选），如 {"key": "value"}' },
);
const emit = defineEmits<{ "update:modelValue": [value: string] }>();

const mode = ref<"form" | "json">("form");
const values = reactive<Record<string, string>>({});

function emitForm(): void {
  const assembled = assembleInput(values);
  emit("update:modelValue", assembled === undefined ? "" : JSON.stringify(assembled));
}

/** 字段显示值：字符串原样，其余 JSON 化（回填后再次提交能解析回原类型） */
function toText(v: unknown): string {
  return typeof v === "string" ? v : JSON.stringify(v);
}

/** 从 JSON 文本回填表单（挂载与切回表单模式时） */
function hydrate(): void {
  let obj: unknown;
  try {
    obj = JSON.parse(props.modelValue);
  } catch {
    return;
  }
  if (obj === null || typeof obj !== "object" || Array.isArray(obj)) return;
  for (const k of props.inputKeys) {
    const v = (obj as Record<string, unknown>)[k];
    values[k] = v === undefined ? "" : toText(v);
  }
}

watch(
  () => props.inputKeys,
  (keys) => {
    for (const k of Object.keys(values)) if (!keys.includes(k)) delete values[k];
    for (const k of keys) if (!(k in values)) values[k] = "";
    hydrate();
    emitForm();
  },
  { immediate: true },
);

function switchMode(next: "form" | "json"): void {
  if (next === mode.value) return;
  mode.value = next;
  if (next === "form") {
    hydrate();
    emitForm();
  }
}
</script>

<template>
  <div class="wf-input">
    <template v-if="inputKeys.length > 0">
      <div v-if="mode === 'form'" class="wf-input-fields">
        <label v-for="k in inputKeys" :key="k" class="wf-input-field">
          <span class="wf-input-key mono">{{ k }}</span>
          <input
            v-model="values[k]"
            type="text"
            spellcheck="false"
            placeholder="值"
            title="留空不传入；5、true、{...} 等按 JSON 解析，其余按字符串"
            @input="emitForm"
          />
        </label>
      </div>
      <textarea
        v-else
        class="code wf-input-json"
        title="运行输入 JSON（可选）"
        aria-label="运行输入 JSON（可选）"
        :rows="rows"
        spellcheck="false"
        :placeholder="placeholder"
        :value="modelValue"
        @input="emit('update:modelValue', ($event.target as HTMLTextAreaElement).value)"
      ></textarea>
      <button
        type="button"
        class="wf-input-toggle"
        :title="mode === 'form' ? '切换为原始 JSON' : '切换为表单'"
        @click="switchMode(mode === 'form' ? 'json' : 'form')"
      >
        {{ mode === "form" ? "JSON" : "表单" }}
      </button>
    </template>
    <textarea
      v-else
      class="code wf-input-json"
      title="运行输入 JSON（可选）"
      aria-label="运行输入 JSON（可选）"
      :rows="rows"
      spellcheck="false"
      :placeholder="placeholder"
      :value="modelValue"
      @input="emit('update:modelValue', ($event.target as HTMLTextAreaElement).value)"
    ></textarea>
  </div>
</template>

<style scoped>
.wf-input {
  display: flex;
  gap: 6px;
  align-items: flex-start;
  flex: 1;
  min-width: 0;
}

.wf-input-fields {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-wrap: wrap;
  gap: 6px 10px;
}

.wf-input-field {
  display: flex;
  align-items: center;
  gap: 4px;
}

.wf-input-key {
  font-size: 11px;
  color: var(--text2);
}

.wf-input-field input {
  width: 140px;
  font-family: var(--mono);
  font-size: 12px;
}

.wf-input-json {
  flex: 1;
  min-width: 0;
  resize: none;
}

.wf-input-toggle {
  flex-shrink: 0;
  padding: 1px 8px;
  font-size: 11px;
}
</style>
