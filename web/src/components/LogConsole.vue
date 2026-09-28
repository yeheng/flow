<script setup lang="ts">
import { computed, nextTick, ref, watch } from "vue";
import { monitor } from "../state/monitor";
import type { LogLine, LogLevel } from "../types";

/**
 * 日志控制台：一个 run 的时间轴叙事（node_log 事件流）。
 * 历史段来自订阅回放（seq=1 起完整重放），运行中实时追加——同一条流。
 */
const emit = defineEmits<{ "select-node": [nodeId: string] }>();

/** 级别下限过滤：debug 单独开关（默认 info 及以上） */
const showDebug = ref(false);
const nodeFilter = ref("");
const searchText = ref("");
/** 跟随模式：贴底自动滚动；向上滚动即暂停 */
const follow = ref(true);
const listEl = ref<HTMLElement | null>(null);

const LEVEL_ORDER: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };

const nodeName = computed(() => {
  const map = new Map(monitor.nodes.map((n) => [n.id, n.name || n.id]));
  return (id: string) => map.get(id) ?? (id ? id : "引擎");
});

/** 日志中出现过的节点（过滤下拉） */
const nodeOptions = computed(() => {
  const ids = new Set(monitor.logs.map((l) => l.node_id).filter(Boolean));
  return [...ids];
});

const filtered = computed<LogLine[]>(() => {
  const min = showDebug.value ? LEVEL_ORDER.debug : LEVEL_ORDER.info;
  const kw = searchText.value.trim().toLowerCase();
  return monitor.logs.filter((l) => {
    if (LEVEL_ORDER[l.level] < min) return false;
    if (nodeFilter.value && l.node_id !== nodeFilter.value) return false;
    if (kw && !l.message.toLowerCase().includes(kw)) return false;
    return true;
  });
});

/** DOM 行上限：状态存全量（预算封顶），渲染窗口化避免万行节点卡死 */
const RENDER_CAP = 1500;
const rendered = computed(() =>
  filtered.value.length > RENDER_CAP ? filtered.value.slice(-RENDER_CAP) : filtered.value,
);
const hiddenCount = computed(() => filtered.value.length - rendered.value.length);

function fmtTime(ts: string): string {
  const d = new Date(ts);
  return Number.isNaN(d.getTime()) ? ts : d.toLocaleTimeString();
}

function onScroll(): void {
  const el = listEl.value;
  if (!el) return;
  const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
  follow.value = atBottom;
}

function scrollToBottom(): void {
  const el = listEl.value;
  if (el) el.scrollTop = el.scrollHeight;
}

function backToBottom(): void {
  follow.value = true;
  scrollToBottom();
}

watch(
  () => monitor.logs.length,
  async () => {
    if (!follow.value) return;
    await nextTick();
    scrollToBottom();
  },
);

function onSelectNode(id: string): void {
  if (!id) return;
  emit("select-node", id);
}
</script>

<template>
  <div class="log-console">
    <div class="log-toolbar">
      <label class="log-toggle">
        <input v-model="showDebug" type="checkbox" />
        debug
      </label>
      <select v-model="nodeFilter" class="log-node-select">
        <option value="">全部节点</option>
        <option v-for="id in nodeOptions" :key="id" :value="id">
          {{ nodeName(id) }}
        </option>
      </select>
      <input
        v-model="searchText"
        type="search"
        class="log-search"
        placeholder="搜索日志…"
      />
    </div>

    <div v-if="monitor.logs.length === 0" class="log-empty">
      暂无日志<span v-if="monitor.phase === 'running'">（等待节点输出…）</span>
    </div>
    <template v-else>
      <div ref="listEl" class="log-list" @scroll="onScroll">
        <p v-if="hiddenCount > 0" class="log-truncated">
          已省略较早的 {{ hiddenCount }} 条（滚动窗口 {{ RENDER_CAP }} 行）
        </p>
        <div
          v-for="line in rendered"
          :key="line.seq"
          class="log-row"
          :class="`log-${line.level}`"
        >
          <span class="log-ts">{{ fmtTime(line.ts) }}</span>
          <span class="log-level">{{ line.level }}</span>
          <button
            v-if="line.node_id"
            class="log-node link"
            :title="`查看节点 ${nodeName(line.node_id)}`"
            @click="onSelectNode(line.node_id)"
          >
            {{ nodeName(line.node_id) }}<template v-if="line.attempt > 1">×{{ line.attempt }}</template>
          </button>
          <span v-else class="log-node log-node-engine">⚙</span>
          <span v-if="line.stream !== 'engine'" class="log-stream">{{ line.stream }}</span>
          <span class="log-msg">{{ line.message }}</span>
        </div>
      </div>
      <button v-if="!follow" class="log-bottom" @click="backToBottom">↓ 回到底部（跟随中）</button>
    </template>
  </div>
</template>

<style scoped>
.log-console {
  display: flex;
  flex-direction: column;
  min-height: 0;
  flex: 1;
}

.log-toolbar {
  display: flex;
  gap: 6px;
  align-items: center;
  padding: 6px 0;
}

.log-toggle {
  display: flex;
  gap: 4px;
  align-items: center;
  font-size: 11px;
  color: var(--text2);
}

.log-node-select {
  max-width: 130px;
  font-size: 11px;
}

.log-search {
  flex: 1;
  min-width: 0;
  font-size: 11px;
}

.log-list {
  flex: 1;
  min-height: 120px;
  overflow-y: auto;
  border: 1px solid var(--border);
  border-radius: 6px;
  background: var(--surface2);
  padding: 4px 0;
}

.log-truncated {
  color: var(--text3);
  font-size: 11px;
  text-align: center;
  padding: 2px 8px;
}

.log-empty {
  color: var(--text3);
  font-size: 12px;
  border: 1px dashed var(--border);
  border-radius: 6px;
  padding: 16px;
  text-align: center;
}

.log-row {
  display: flex;
  gap: 6px;
  align-items: baseline;
  padding: 1px 8px;
  font-size: 11px;
  font-family: var(--mono);
  content-visibility: auto;
  contain-intrinsic-size: auto 18px;
}

.log-ts {
  color: var(--text3);
  flex-shrink: 0;
}

.log-level {
  flex-shrink: 0;
  width: 34px;
  color: var(--text3);
}

.log-row.log-warn .log-level {
  color: var(--warn);
}

.log-row.log-error .log-level,
.log-row.log-error .log-msg {
  color: var(--danger);
}

.log-row.log-debug .log-msg {
  color: var(--text3);
}

.log-node {
  flex-shrink: 0;
  max-width: 120px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  padding: 0;
  border: none;
  background: none;
  font-size: 11px;
  font-family: var(--mono);
}

.log-node-engine {
  color: var(--text3);
}

.log-stream {
  flex-shrink: 0;
  color: var(--text3);
  font-size: 10px;
  border: 1px solid var(--border);
  border-radius: 3px;
  padding: 0 3px;
}

.log-msg {
  word-break: break-all;
  white-space: pre-wrap;
}

.log-bottom {
  position: relative;
  margin-top: 4px;
  align-self: center;
  font-size: 11px;
  padding: 2px 10px;
}
</style>
