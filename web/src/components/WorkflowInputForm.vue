<script setup lang="ts">
/**
 * 工作流输入表单：discoverInputKeys 发现的字段各一个输入框，
 * 值组装成 JSON 文本写入 v-model（与原始 JSON 文本域同一数据源，
 * startRun / schedule.create 的解析链路不变）；无字段时退化为纯 JSON 文本域。
 */
import { computed, reactive, ref, watch } from "vue";

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

/**
 * modelValue 解析出的 JSON 对象：空文本 → {}；非对象（数组/标量）或非法 JSON → undefined。
 * 后者不能从表单无损重建，只能保持原文不动（见 emitForm/formBlocked）。
 */
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

/** 从 modelValue 回填字段显示值——只读，绝不 emit；避免重写用户刚输入的内容 */
function hydrate(onlyMissing = false): void {
  const base = parseBase();
  if (base === undefined) return;
  for (const k of props.inputKeys) {
    if (onlyMissing && k in values) continue;
    const v = base[k];
    values[k] = v === undefined ? "" : toText(v);
  }
}

/**
 * 只有用户输入事件才写回 modelValue：以当前 JSON 对象为基底 merge 声明字段，
 * 未声明的键原样保留。基底不是 JSON 对象（数组/标量/半截 JSON）时无可无损合并的
 * 结构，保持原文不动——表单编辑被 formBlocked 提示拦下，切 JSON 模式可继续改。
 */
function emitForm(): void {
  const base = parseBase();
  if (base === undefined) return;
  const next: Record<string, unknown> = { ...base };
  for (const k of props.inputKeys) {
    const raw = (values[k] ?? "").trim();
    if (!raw) {
      delete next[k];
      continue;
    }
    try {
      next[k] = JSON.parse(raw);
    } catch {
      next[k] = raw;
    }
  }
  emit("update:modelValue", Object.keys(next).length > 0 ? JSON.stringify(next) : "");
}

/** 表单无法表示当前原文：给出明确提示，避免用户以为输入生效了 */
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

let knownKeys: string[] | null = null;
watch(
  () => props.inputKeys,
  (keys) => {
    const prev = knownKeys;
    knownKeys = [...keys];
    // 字段清单变化只同步表单显示：删掉已移除字段，新增字段从当前 JSON 回填
    for (const k of Object.keys(values)) if (!keys.includes(k)) delete values[k];
    hydrate(true);
    for (const k of keys) if (!(k in values)) values[k] = "";
    // 挂载首次不 emit；只有 form 模式且键集合确实增删时才写回 merge 结果
    if (!prev) return;
    const changed = keys.length !== prev.length || keys.some((k) => !prev.includes(k));
    if (changed && mode.value === "form") emitForm();
  },
  { immediate: true },
);

function switchMode(next: "form" | "json"): void {
  if (next === mode.value) return;
  mode.value = next;
  // 切回表单只回填显示，不写回：JSON 模式的原文（含半截 JSON）必须原样保留
  if (next === "form") hydrate();
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
