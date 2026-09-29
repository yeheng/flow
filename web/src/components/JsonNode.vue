<script setup lang="ts">
import { computed } from "vue";

/**
 * 递归 JSON 值查看器：对象/数组可折叠（浅层默认展开），标量按类型着色。
 * 节点检查器的输入/输出展示共用。
 */
const props = withDefaults(
  defineProps<{ value: unknown; name?: string; depth?: number; comma?: boolean }>(),
  { name: "", depth: 0, comma: false },
);

const isObject = computed(() => props.value !== null && typeof props.value === "object");

const entries = computed<Array<[string, unknown]>>(() => {
  if (!isObject.value) return [];
  const v = props.value as Record<string, unknown> | unknown[];
  return Array.isArray(v) ? v.map((item, i) => [String(i), item]) : Object.entries(v);
});

const kindClass = computed(() => {
  const v = props.value;
  if (v === null || v === undefined) return "j-null";
  switch (typeof v) {
    case "string":
      return "j-string";
    case "number":
      return "j-number";
    case "boolean":
      return "j-bool";
    default:
      return "";
  }
});

const scalarText = computed(() => {
  const v = props.value;
  if (v === undefined) return "undefined";
  if (v === null) return "null";
  if (typeof v === "string") return JSON.stringify(v);
  return String(v);
});

/** 对象/数组的摘要标签（避免在模板里写嵌套模板字符串） */
const metaText = computed(() =>
  Array.isArray(props.value) ? `Array(${entries.value.length})` : `Object(${entries.value.length})`,
);
</script>

<template>
  <details v-if="isObject" :open="depth < 1" class="json-node">
    <summary class="json-summary">
      <span v-if="name" class="j-key">{{ name }}</span>
      <span class="j-meta">{{ metaText }}</span>
      <span v-if="comma" class="j-punct">,</span>
    </summary>
    <div class="json-children">
      <JsonNode
        v-for="[key, child] in entries"
        :key="key"
        :value="child"
        :name="Array.isArray(value) ? undefined : key"
        :depth="depth + 1"
        :comma="entries.length > 1"
      />
    </div>
  </details>
  <div v-else class="json-leaf">
    <span v-if="name" class="j-key">{{ name }}</span>
    <span :class="kindClass">{{ scalarText }}</span>
    <span v-if="comma" class="j-punct">,</span>
  </div>
</template>

<style scoped>
.json-node,
.json-leaf {
  font-family: var(--mono);
  font-size: 11px;
  line-height: 1.5;
  padding-left: 12px;
}

.json-summary {
  cursor: pointer;
  list-style: none;
  user-select: none;
}

.json-summary::before {
  content: "▸ ";
  color: var(--text3);
}

details[open] > .json-summary::before {
  content: "▾ ";
}

.json-children {
  border-left: 1px dotted var(--border);
  margin-left: 4px;
}

.j-key {
  color: var(--run);
  margin-right: 6px;
}

.j-meta {
  color: var(--text3);
  font-size: 10px;
}

.j-string {
  color: var(--ok);
  word-break: break-all;
}

.j-number {
  color: var(--warn);
}

.j-bool,
.j-null {
  color: var(--accent, #a371f7);
  font-style: italic;
}

.j-punct {
  color: var(--text3);
}
</style>
