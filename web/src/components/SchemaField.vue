<script setup lang="ts">
import { computed, ref } from "vue";
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
  "text" | "number" | "boolean" | "enum" | "code" | "json" | "workflow-picker" | "object";
const widget = computed<Widget>(() => {
  if (props.schema.enum) return "enum";
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
  emit("update:modelValue", (ev.target as HTMLSelectElement).value);
}

// ---- json widget：本地持有文本，解析成功才写回（非法输入不污染 params） ----
const jsonText = ref(
  props.modelValue === undefined ? "" : JSON.stringify(props.modelValue, null, 2),
);
const jsonError = ref("");

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
      <option v-for="opt in schema.enum" :key="opt" :value="opt">{{ opt }}</option>
    </select>
    <textarea
      v-else-if="widget === 'code'"
      class="code"
      rows="5"
      spellcheck="false"
      :value="(modelValue as string) ?? ''"
      @input="onText"
    ></textarea>
    <template v-else-if="widget === 'json'">
      <textarea
        v-model="jsonText"
        class="code"
        rows="4"
        spellcheck="false"
        @input="onJson"
      ></textarea>
      <div v-if="jsonError" class="field-error">{{ jsonError }}</div>
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
</style>
