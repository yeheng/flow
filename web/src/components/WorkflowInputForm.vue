<script setup lang="ts">
/**
 * 工作流输入表单：discoverInputKeys 发现的字段各一个输入框，
 * 值组装成 JSON 文本写入 v-model（与原始 JSON 文本域同一数据源，
 * startRun / schedule.create 的解析链路不变）；无字段时退化为纯 JSON 文本域。
 */
import { computed, reactive, ref, watch } from "vue";
import { schemaFields } from "../state/form-fields";

const props = withDefaults(
  defineProps<{
    inputKeys: string[];
    modelValue: string;
    inputSchema?: unknown;
    rows?: number;
    placeholder?: string;
  }>(),
  { rows: 1, placeholder: '运行输入 JSON（可选），如 {"key": "value"}' },
);
const emit = defineEmits<{ "update:modelValue": [value: string] }>();

const mode = ref<"form" | "json">("form");

/**
 * 字段编辑缓冲：用户逐键输入的原文（"5.0"/"1e3" 这类输入不能被回写规范化
 * 吃掉字符——所以缓冲不从 modelValue 派生）。已提交真相始终是 modelValue；
 * 两者的关系只有两条规则：syncFields 从 modelValue 补齐显示、emitForm 由用户
 * 输入触发合并写回。
 */
const values = reactive<Record<string, string>>(Object.create(null));
const declaredFields = computed(
  () => new Map(schemaFields(props.inputSchema).map((f) => [f.key, f])),
);
let lastFormEmission: string | undefined;

/** modelValue 解析出的 JSON 对象：空文本 → {}；非对象/非法 JSON → undefined（不可无损合并） */
function parseBase(): Record<string, unknown> | undefined {
  const raw = props.modelValue.trim();
  if (!raw) return {};
  try {
    const v: unknown = JSON.parse(raw);
    if (v === null || typeof v !== "object" || Array.isArray(v)) return undefined;
    return v as Record<string, unknown>;
  } catch {
    return undefined;
  }
}

/** 字段显示值：字符串原样，其余 JSON 化（回填后再次提交能解析回原类型） */
function toText(v: unknown): string {
  return typeof v === "string" ? v : JSON.stringify(v);
}

/**
 * 同步显示缓冲到当前字段清单（只读，不 emit）：删掉已移除字段；缺字段从
 * modelValue 回填。overwrite=true 时已存在字段也覆盖——外部改写（切回表单）
 * 必须以 modelValue 为准；false 只补缺失，保留用户逐键输入的缓冲。
 */
function syncFields(overwrite = false): void {
  const base = parseBase();
  for (const k of Object.keys(values)) if (!props.inputKeys.includes(k)) delete values[k];
  for (const k of props.inputKeys) {
    if (!overwrite && k in values) continue;
    const v = base?.[k];
    values[k] = v === undefined ? "" : toText(v);
  }
}

/** 用户输入才写回：以 modelValue 为基底 merge 声明字段，未声明键保留；基底不可合并时不动原文 */
function emitForm(): void {
  const base = parseBase();
  if (base === undefined) return;
  const next: Record<string, unknown> = { ...base };
  for (const k of props.inputKeys) {
    const raw = values[k] ?? "";
    if (!raw.trim()) {
      delete next[k];
      continue;
    }
    const field = declaredFields.value.get(k);
    if (field?.type === "string" || field?.enum?.some((v) => v === raw)) {
      next[k] = raw;
    } else {
      try {
        next[k] = JSON.parse(raw);
      } catch {
        next[k] = raw;
      }
    }
  }
  lastFormEmission = Object.keys(next).length > 0 ? JSON.stringify(next) : "";
  emit("update:modelValue", lastFormEmission);
}

/** 表单无法表示当前原文（数组/标量/半截 JSON）：明确提示，避免用户以为输入生效了 */
const formBlocked = computed(() => {
  if (mode.value !== "form") return false;
  const raw = props.modelValue.trim();
  if (!raw) return false;
  try {
    const v: unknown = JSON.parse(raw);
    return v === null || typeof v !== "object" || Array.isArray(v);
  } catch {
    return true;
  }
});

watch(
  [() => props.inputKeys, () => props.modelValue],
  ([, model], previous) => {
    const changed = previous && model !== previous[1];
    // 自身输入的回显保留 5.0/1e3 缓冲；外部补填、清空和切换工作流则重新回填。
    syncFields(!!changed && model !== lastFormEmission);
    if (changed) lastFormEmission = undefined;
  },
  { immediate: true },
);

function switchMode(next: "form" | "json"): void {
  if (next === mode.value) return;
  mode.value = next;
  // 切回表单只同步显示（外部改写→覆盖缓冲），不写回：JSON 模式的原文必须原样保留
  if (next === "form") syncFields(true);
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
        <p v-if="formBlocked" class="wf-input-warn">
          当前输入不是 JSON 对象，表单编辑不会写回；请切换到 JSON 模式修改
        </p>
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

.wf-input-warn {
  width: 100%;
  margin: 0;
  font-size: 11px;
  color: var(--warn);
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
