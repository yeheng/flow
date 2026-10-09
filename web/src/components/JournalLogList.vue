<script setup lang="ts">
import { computed, reactive, ref } from "vue";
import JsonNode from "./JsonNode.vue";
import { summarizeJournalRecord } from "../state/journal-log";

/**
 * journal 记录列表（实时/历史事件、观测日志共用）：LogConsole 视觉语言
 * （行着色、类型过滤、文本搜索），点击行展开 JsonNode 查看完整结构。
 */
const props = withDefaults(defineProps<{ items: unknown[]; emptyText?: string }>(), {
  emptyText: "暂无记录",
});

const kindFilter = ref("");
const searchText = ref("");
const expanded = reactive(new Set<string>());

/** 单次渲染上限：EventWindow 全量可达 4096 条，超出部分提示用分页/审计读取 */
const RENDER_CAP = 1000;

const rows = computed(() => props.items.map((item, i) => summarizeJournalRecord(item, i)));
const kinds = computed(() => [...new Set(rows.value.map((r) => r.label))]);

const filtered = computed(() => {
  const kw = searchText.value.trim().toLowerCase();
  return rows.value.filter((r) => {
    if (kindFilter.value && r.label !== kindFilter.value) return false;
    if (kw && !r.haystack.includes(kw)) return false;
    return true;
  });
});

const rendered = computed(() => filtered.value.slice(-RENDER_CAP));
const hiddenCount = computed(() => filtered.value.length - rendered.value.length);

function toggle(key: string): void {
  if (expanded.has(key)) expanded.delete(key);
  else expanded.add(key);
}
</script>

<template>
  <div class="journal-log">
    <div class="log-toolbar">
      <select v-model="kindFilter" class="log-kind-select" aria-label="类型筛选">
        <option value="">全部类型</option>
        <option v-for="k in kinds" :key="k" :value="k">{{ k }}</option>
      </select>
      <input v-model="searchText" type="search" class="log-search" placeholder="搜索记录…" />
    </div>

    <div v-if="rows.length === 0" class="log-empty">{{ emptyText }}</div>
    <div v-else-if="filtered.length === 0" class="log-empty">无匹配记录</div>
    <div v-else class="log-list">
      <p v-if="hiddenCount > 0" class="log-cap muted">
        仅展示最近 {{ RENDER_CAP }} 条（共 {{ filtered.length }} 条），更早的请用分页或审计读取
      </p>
      <div v-for="row in rendered" :key="row.key" class="log-item">
        <button
          type="button"
          class="log-row"
          :class="[row.level, { expanded: expanded.has(row.key) }]"
          @click="toggle(row.key)"
        >
          <span class="log-label">{{ row.label }}</span>
          <span v-if="row.meta" class="log-meta">{{ row.meta }}</span>
          <span class="log-text" :title="row.text">{{ row.text }}</span>
        </button>
        <div v-if="expanded.has(row.key)" class="log-detail">
          <JsonNode :value="row.raw" />
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.journal-log {
  display: flex;
  flex-direction: column;
  min-height: 0;
}

.log-toolbar {
  display: flex;
  gap: 6px;
  align-items: center;
  padding: 6px 0;
}

.log-kind-select {
  max-width: 180px;
  font-size: 11px;
  width: auto;
}

.log-search {
  flex: 1;
  min-width: 0;
  font-size: 11px;
}

.log-empty {
  color: var(--text3);
  font-size: 12px;
  border: 1px dashed var(--border);
  border-radius: 6px;
  padding: 16px;
  text-align: center;
}

.log-list {
  max-height: 360px;
  overflow-y: auto;
  border: 1px solid var(--border);
  border-radius: 6px;
  background: var(--surface2);
  padding: 4px 0;
}

.log-list::-webkit-scrollbar {
  width: 4px;
}

.log-list::-webkit-scrollbar-thumb {
  background: var(--surface3);
  border-radius: 2px;
}

.log-cap {
  margin: 2px 8px;
  font-size: 11px;
  text-align: center;
}

.log-row {
  display: flex;
  gap: 8px;
  align-items: baseline;
  width: 100%;
  padding: 2px 8px;
  border: none;
  background: none;
  border-radius: 0;
  font-size: 11px;
  font-family: var(--mono);
  text-align: left;
  cursor: pointer;
  content-visibility: auto;
  contain-intrinsic-size: auto 18px;
}

.log-row:hover {
  background: var(--surface3);
  border-color: transparent;
}

.log-label {
  flex-shrink: 0;
  max-width: 180px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  color: var(--accent2);
}

.log-row.log-warn .log-label {
  color: var(--warn);
}

.log-row.log-error .log-label,
.log-row.log-error .log-text {
  color: var(--danger);
}

.log-row.log-debug .log-text {
  color: var(--text3);
}

.log-meta {
  flex-shrink: 0;
  color: var(--text3);
}

.log-text {
  color: var(--text2);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.log-row.expanded .log-text {
  white-space: pre-wrap;
  word-break: break-all;
}

.log-detail {
  padding: 4px 12px 8px 24px;
  border-bottom: 1px dashed var(--border);
}
</style>
