<script setup lang="ts">
import { computed, markRaw, ref, useId, watch } from "vue";
import CodeEditor from "./CodeEditor.vue";
import { editor } from "../state/editor";
import type { PropertySchema } from "../types";

const props = defineProps<{
  name: string;
  schema: PropertySchema;
  required?: boolean;
  modelValue: unknown;
}>();
const emit = defineEmits<{ "update:modelValue": [value: unknown] }>();

const label = computed(() => props.schema["x-label"] ?? props.name);
const help = computed(() => props.schema["x-help"]);

type Widget =
  | "text"
  | "number"
  | "boolean"
  | "enum"
  | "secret"
  | "code"
  | "json"
  | "workflow-picker"
  | "key-value"
  | "object";
const widget = computed<Widget>(() => {
  if (props.schema.enum) return "enum";
  if (props.schema["x-secret"]) return "secret";
  const w = props.schema["x-widget"];
  if (w) return w;
  switch (props.schema.type) {
    case "integer":
    case "number":
      return "number";
    case "boolean":
      return "boolean";
    case "object":
      return "object";
    default:
      return "text";
  }
});

function onText(ev: Event): void {
  emit("update:modelValue", (ev.target as HTMLInputElement).value);
}

function onNumber(ev: Event): void {
  const v = (ev.target as HTMLInputElement).valueAsNumber;
  emit("update:modelValue", Number.isNaN(v) ? undefined : v);
}

function onBoolean(ev: Event): void {
  emit("update:modelValue", (ev.target as HTMLInputElement).checked);
}

function onSelect(ev: Event): void {
  const v = (ev.target as HTMLSelectElement).value;
  // 非必填枚举的「（不设置）」空选项映射回 undefined（cleanParams 落库时剥掉）
  emit("update:modelValue", v === "" ? undefined : v);
}

// ---- json widget：本地持有文本，解析成功才写回（非法输入不污染 params） ----
const jsonText = ref(
  props.modelValue === undefined ? "" : JSON.stringify(props.modelValue, null, 2),
);
const jsonError = ref("");
/** 聚焦期间不做外部同步，避免格式化差异打断输入 */
const jsonFocused = ref(false);

// undo/redo 或版本加载从外部改了 params：非聚焦时把文本同步回来
watch(
  () => props.modelValue,
  (v) => {
    if (jsonFocused.value || props.schema["x-widget"] !== "json") return;
    jsonText.value = v === undefined ? "" : JSON.stringify(v, null, 2);
    jsonError.value = "";
  },
);

function onJson(ev: Event): void {
  const raw = (ev.target as HTMLTextAreaElement).value.trim();
  if (!raw) {
    jsonError.value = "";
    emit("update:modelValue", undefined);
    return;
  }
  try {
    jsonError.value = "";
    emit("update:modelValue", JSON.parse(raw));
  } catch {
    jsonError.value = "JSON 格式错误，未保存此字段";
  }
}

// ---- key-value widget：动态键值对行编辑（headers 等 object 参数） ----
// 本地持有行状态，组件按节点 :key 重挂载时从 modelValue 初始化；
// 行输入聚焦期间不做外部同步（undo/redo 等），避免打断输入。
// 重复键取舍：不做 last-wins 静默覆盖——重复行标红（kv-dup）且不写入 params，
// 同名键只保留第一行。
interface KvRow {
  id: number;
  key: string;
  value: string;
}
let kvSeq = 0;
function kvFromModel(v: unknown): KvRow[] {
  if (typeof v !== "object" || v === null || Array.isArray(v)) return [];
  return Object.entries(v as Record<string, unknown>).map(([key, val]) => ({
    id: ++kvSeq,
    key,
    value: typeof val === "string" ? val : String(val ?? ""),
  }));
}
const kvRows = ref<KvRow[]>(kvFromModel(props.modelValue));
/** 聚焦中的行输入数（focus/blur 计数，行间切换不会瞬时清零） */
let kvFocusCount = 0;
const kvFocused = ref(false);

function onKvFocus(): void {
  kvFocusCount += 1;
  kvFocused.value = true;
}
function onKvBlur(): void {
  kvFocusCount = Math.max(0, kvFocusCount - 1);
  kvFocused.value = kvFocusCount > 0;
}

// undo/redo 或版本加载从外部改了 params：非聚焦时把行同步回来。
// 自己 emit 的值按引用跳过——否则删行后焦点离开输入框，resync 会把
// 未完成的空键/重复键行（未写入 params）从本地行状态里抹掉。
let kvSelfEmitted: unknown;
watch(
  () => props.modelValue,
  (v) => {
    if (v === kvSelfEmitted) return;
    kvSelfEmitted = undefined;
    if (kvFocused.value || props.schema["x-widget"] !== "key-value") return;
    kvRows.value = kvFromModel(v);
  },
);

/** 重复的非空键（第二次及以后出现的行不写入 params） */
const kvDupKeys = computed(() => {
  const seen = new Set<string>();
  const dup = new Set<string>();
  for (const row of kvRows.value) {
    const k = row.key.trim();
    if (!k) continue;
    if (seen.has(k)) dup.add(k);
    else seen.add(k);
  }
  return dup;
});

function kvIsDup(row: KvRow): boolean {
  const k = row.key.trim();
  return k !== "" && kvDupKeys.value.has(k) && kvRows.value.find((r) => r.key.trim() === k) !== row;
}

function emitKv(): void {
  const out: Record<string, string> = {};
  for (const row of kvRows.value) {
    const k = row.key.trim();
    if (!k || k in out) continue;
    out[k] = row.value;
  }
  // markRaw：params 存在 reactive 编辑器状态里，读回时保持同一引用，
  // watch 才能按引用识别出这是自己 emit 的值而跳过 resync
  kvSelfEmitted = Object.keys(out).length > 0 ? markRaw(out) : undefined;
  emit("update:modelValue", kvSelfEmitted);
}

function addKvRow(): void {
  kvRows.value.push({ id: ++kvSeq, key: "", value: "" });
}

function removeKvRow(id: number): void {
  kvRows.value = kvRows.value.filter((r) => r.id !== id);
  emitKv();
}

// ---- workflow-picker ----
const targetWorkflow = computed(() => {
  const id = typeof props.modelValue === "string" ? props.modelValue : "";
  return editor.workflows.find((w) => w.workflow_id === id) ?? null;
});

function onPickWorkflow(ev: Event): void {
  const v = (ev.target as HTMLSelectElement).value;
  emit("update:modelValue", v || undefined);
}

const workflowWarning = computed(() => {
  if (typeof props.modelValue !== "string" || !props.modelValue) return "";
  if (!targetWorkflow.value) return "目标工作流不存在或已删除";
  if (!targetWorkflow.value.published_version) return "目标工作流尚未发布，运行时将失败";
  return "";
});

// ---- secret：密钥名称选择器（datalist 既给候选又允许手输，覆盖服务端未配置的空列表场景） ----
const secretListId = useId();

// ---- object 递归 ----
const objValue = computed<Record<string, unknown>>(() =>
  typeof props.modelValue === "object" && props.modelValue !== null
    ? (props.modelValue as Record<string, unknown>)
    : {},
);

function setChild(key: string, value: unknown): void {
  emit("update:modelValue", { ...objValue.value, [key]: value });
}
</script>

<template>
  <fieldset v-if="widget === 'object'" class="schema-object">
    <legend>
      {{ label }}
      <em v-if="required" class="required">*</em>
    </legend>
    <SchemaField
      v-for="(sub, key) in schema.properties ?? {}"
      :key="key"
      :name="key"
      :schema="sub"
      :required="schema.required?.includes(key)"
      :model-value="objValue[key]"
      @update:model-value="setChild(key, $event)"
    />
    <div v-if="help" class="field-help">{{ help }}</div>
  </fieldset>

  <div v-else class="field">
    <label>
      {{ label }}
      <em v-if="required" class="required">*</em>
    </label>

    <input
      v-if="widget === 'text'"
      type="text"
      :value="(modelValue as string) ?? ''"
      @input="onText"
    />
    <input
      v-else-if="widget === 'number'"
      type="number"
      :value="(modelValue as number) ?? ''"
      @input="onNumber"
    />
    <input
      v-else-if="widget === 'boolean'"
      type="checkbox"
      class="checkbox"
      :checked="Boolean(modelValue)"
      @change="onBoolean"
    />
    <select v-else-if="widget === 'enum'" :value="(modelValue as string) ?? ''" @change="onSelect">
      <option v-if="!required" value="">（不设置）</option>
      <option v-for="opt in schema.enum" :key="opt" :value="opt">{{ opt }}</option>
    </select>
    <template v-else-if="widget === 'secret'">
      <input
        type="text"
        :list="secretListId"
        placeholder="密钥名称"
        :value="(modelValue as string) ?? ''"
        @input="onText"
      />
      <datalist :id="secretListId">
        <option v-for="s in editor.secrets" :key="s" :value="s" />
      </datalist>
      <div v-if="editor.secrets.length === 0" class="field-help">
        服务端未配置 FLOW_SECRET_* 环境变量，可手动输入密钥名称
      </div>
    </template>
    <CodeEditor
      v-else-if="widget === 'code'"
      language="javascript"
      height="180px"
      :model-value="(modelValue as string) ?? ''"
      @update:model-value="emit('update:modelValue', $event)"
    />
    <template v-else-if="widget === 'json'">
      <textarea
        v-model="jsonText"
        class="code"
        rows="4"
        spellcheck="false"
        @input="onJson"
        @focus="jsonFocused = true"
        @blur="jsonFocused = false"
      ></textarea>
      <div v-if="jsonError" class="field-error">{{ jsonError }}</div>
    </template>
    <template v-else-if="widget === 'key-value'">
      <div v-for="row in kvRows" :key="row.id" class="kv-row" :class="{ 'kv-dup': kvIsDup(row) }">
        <input
          v-model="row.key"
          type="text"
          class="kv-key"
          placeholder="键"
          spellcheck="false"
          @input="emitKv"
          @focus="onKvFocus"
          @blur="onKvBlur"
        />
        <input
          v-model="row.value"
          type="text"
          class="kv-value"
          placeholder="值"
          spellcheck="false"
          @input="emitKv"
          @focus="onKvFocus"
          @blur="onKvBlur"
        />
        <button
          type="button"
          class="kv-remove"
          title="删除此行"
          @click="removeKvRow(row.id)"
        >
          ×
        </button>
      </div>
      <button type="button" class="kv-add" @click="addKvRow">＋ 添加</button>
      <div v-if="kvDupKeys.size > 0" class="field-error">存在重复的键，重复行未保存</div>
    </template>
    <template v-else-if="widget === 'workflow-picker'">
      <select :value="(modelValue as string) ?? ''" @change="onPickWorkflow">
        <option value="">（选择工作流）</option>
        <option v-for="w in editor.workflows" :key="w.workflow_id" :value="w.workflow_id">
          {{ w.name }}
        </option>
      </select>
      <div v-if="workflowWarning" class="field-error">{{ workflowWarning }}</div>
    </template>

    <div v-if="help" class="field-help">{{ help }}</div>
  </div>
</template>

<style scoped>
input.checkbox {
  width: auto;
}

fieldset.schema-object {
  border: 1px solid var(--border);
  border-radius: 6px;
  margin: 0 0 10px;
  padding: 8px;
}

fieldset.schema-object legend {
  color: var(--text2);
  font-size: 12px;
}

/* key-value 行编辑 */
.kv-row {
  display: flex;
  gap: 6px;
  align-items: center;
  margin-bottom: 6px;
}

.kv-row .kv-key {
  flex: 2;
  min-width: 0;
}

.kv-row .kv-value {
  flex: 3;
  min-width: 0;
}

.kv-row.kv-dup input {
  border-color: var(--danger);
}

.kv-remove {
  flex: none;
  padding: 2px 8px;
  font-size: 13px;
  line-height: 1.4;
}

.kv-add {
  padding: 2px 10px;
  font-size: 12px;
}
</style>
