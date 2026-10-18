<script setup lang="ts">
import { computed, nextTick, onUnmounted, ref, watch } from "vue";
import {
  activityEntries,
  friendlyDuration,
  runElapsedMs,
  runProgress,
} from "../state/activity";
import { monitor } from "../state/monitor";
import { nodeStateLabel, nodeTypeLabel, skipReasonLabel } from "../state/labels";
import type { TimelineNode } from "../types";
import JsonNode from "./JsonNode.vue";

/**
 * 活动流：面向普通用户的运行叙事——每个节点一张卡片（状态、耗时、
 * 错误、过程日志、输出摘要），按发生顺序讲「工作流在做什么」。
 * 时间线表格与原始日志控制台在开发者模式里（RunMonitor 的 tab）。
 */
const emit = defineEmits<{
  "select-node": [nodeId: string];
  "hover-node": [nodeId: string | null];
  "open-child": [runId: string];
}>();

const entries = computed(() => activityEntries(monitor.nodes, monitor.logsByNode));
const progress = computed(() => runProgress(monitor.nodes));

/** 运行中每秒走一格的「当前时刻」，让头部时长活起来；挂载期常驻，
 * 每秒一个 ref 赋值远低于任何轮询节奏，不值得按状态拆装 */
const now = ref(Date.now());
const active = computed(
  () => monitor.status === "running" || monitor.status === "awaiting_resume",
);
const timer = setInterval(() => (now.value = Date.now()), 1000);
onUnmounted(() => clearInterval(timer));
const elapsed = computed(() => {
  if (active.value) {
    return runElapsedMs(monitor.nodes, null, now.value);
  }
  // 终态：monitor 不携带 run 级结束时间，用最晚节点的结束时间封口。
  // 节点时间戳目前恒为空（journal 不记录墙钟），此时宁可不显示用时，
  // 也不能拿 now() 让已结束 run 的「用时」一直增长。
  let last: number | null = null;
  for (const n of monitor.nodes) {
    if (n.ended_at === null) continue;
    const at = Date.parse(n.ended_at);
    if (!Number.isNaN(at) && (last === null || at > last)) last = at;
  }
  return last === null ? null : runElapsedMs(monitor.nodes, new Date(last).toISOString(), now.value);
});

/** 观测日志丢失：活动流也是日志读面，静默少行会让叙事不可信 */
const lossText = computed(() => {
  const loss = monitor.observationLoss;
  const total =
    (loss.queue_dropped ?? 0) + (loss.retention_dropped ?? 0) + (loss.storage_dropped ?? 0);
  if (total > 0) return `日志丢弃 ${total} 条`;
  if (loss.history_incomplete) return "日志历史不完整";
  return "";
});

/** 单卡片默认收起的日志行数：够看到「正在做什么」，又不让长脚本淹没流 */
const COLLAPSED_LOG_LINES = 8;
const expandedNodes = ref(new Set<string>());
function nodeLogs(id: string, logs: number): { visible: number; collapsed: boolean } {
  return expandedNodes.value.has(id)
    ? { visible: logs, collapsed: false }
    : { visible: Math.min(logs, COLLAPSED_LOG_LINES), collapsed: logs > COLLAPSED_LOG_LINES };
}
function toggleLogs(id: string): void {
  const next = new Set(expandedNodes.value);
  if (next.has(id)) next.delete(id);
  else next.add(id);
  expandedNodes.value = next;
}

/** 等待语义：running + wait 结构 → 用人话交代节点在等什么 */
function waitText(node: TimelineNode): string | null {
  if (node.state !== "running" && node.state !== "retrying") return null;
  switch (node.wait?.kind) {
    case "signal":
      return "等待人工处理…";
    case "delay":
      return "等待中…";
    case "child":
      return "子工作流运行中…";
    case "uncertain":
      return "外部操作结果待确认…";
    default:
      return null;
  }
}

function outputPreview(v: unknown): string {
  if (v === null || v === undefined) return "";
  const s = JSON.stringify(v);
  return s.length > 160 ? s.slice(0, 160) + "…" : s;
}

/** 状态圆点：运行中转圈，其余静态符号 */
const stateGlyph: Record<string, string> = {
  pending: "○",
  running: "◐",
  retrying: "↻",
  completed: "✓",
  failed: "✗",
  skipped: "⤼",
};

// —— 跟随滚动：贴底自动滚，向上翻历史即暂停 ——
const follow = ref(true);
const listEl = ref<HTMLElement | null>(null);

function onScroll(): void {
  const el = listEl.value;
  if (!el) return;
  follow.value = el.scrollHeight - el.scrollTop - el.clientHeight < 48;
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
  () => [monitor.nodes, monitor.lastLogSeq],
  async () => {
    if (!follow.value) return;
    await nextTick();
    scrollToBottom();
  },
  { deep: false },
);

function onSelectNode(id: string): void {
  if (id) emit("select-node", id);
}
</script>

<template>
  <div class="activity">
    <div class="activity-progress">
      <template v-if="elapsed !== null">
        {{ progress.done }}/{{ progress.total }} 步
        <span class="progress-sep">·</span>
        <span v-if="active">已运行 {{ friendlyDuration(elapsed) }}</span>
        <span v-else>用时 {{ friendlyDuration(elapsed) }}</span>
      </template>
      <template v-else>{{ progress.total }} 个步骤</template>
      <template v-if="lossText">
        <span class="progress-sep">·</span>
        <span class="activity-loss">{{ lossText }}</span>
      </template>
    </div>

    <div ref="listEl" class="activity-list" @scroll="onScroll">
      <div v-if="entries.length === 0" class="activity-empty">
        <template v-if="active">正在准备执行…</template>
        <template v-else>该工作流没有可执行的步骤</template>
      </div>
      <article
        v-for="e in entries"
        :key="e.node.id"
        class="activity-item"
        :class="`st-${e.node.state}`"
        @click="onSelectNode(e.node.id)"
        @mouseenter="emit('hover-node', e.node.id)"
        @mouseleave="emit('hover-node', null)"
      >
        <header class="item-head">
          <span class="item-glyph" :class="`glyph-${e.node.state}`">{{
            stateGlyph[e.node.state] ?? "○"
          }}</span>
          <span class="item-name" :title="e.node.id">{{ e.node.name || e.node.id }}</span>
          <span class="item-type">{{ nodeTypeLabel(e.node.type) }}</span>
          <span v-if="e.node.attempts > 1" class="item-attempts"
            >第 {{ e.node.attempts }} 次尝试</span
          >
          <span class="item-state">{{ nodeStateLabel(e.node.state) }}</span>
          <span v-if="e.node.duration_ms !== null" class="item-duration">{{
            friendlyDuration(e.node.duration_ms)
          }}</span>
        </header>

        <div v-if="e.node.error" class="item-error">{{ e.node.error }}</div>
        <div v-else-if="e.node.reason" class="item-reason">{{ skipReasonLabel(e.node.reason) }}</div>
        <div v-else-if="waitText(e.node)" class="item-wait">{{ waitText(e.node) }}</div>

        <div v-if="nodeLogs(e.node.id, e.logs.length).visible > 0" class="item-logs">
          <div
            v-for="line in e.logs.slice(-nodeLogs(e.node.id, e.logs.length).visible)"
            :key="line.seq"
            class="log-line"
            :class="`lvl-${line.level}`"
          >
            <span v-if="line.attempt > 1" class="log-attempt">×{{ line.attempt }}</span>
            <span v-if="line.stream === 'stdout'" class="log-marker">›</span>
            <span v-else-if="line.stream === 'stderr'" class="log-marker">!</span>
            <span class="log-msg">{{ line.message }}</span>
          </div>
          <button
            v-if="nodeLogs(e.node.id, e.logs.length).collapsed"
            class="log-expand"
            @click.stop="toggleLogs(e.node.id)"
          >
            展开全部 {{ e.logs.length }} 行
          </button>
          <button
            v-else-if="expandedNodes.has(e.node.id)"
            class="log-expand"
            @click.stop="toggleLogs(e.node.id)"
          >
            收起日志
          </button>
        </div>

        <details v-if="e.node.output !== null && e.node.output !== undefined" class="item-output">
          <summary :title="outputPreview(e.node.output)">输出：{{ outputPreview(e.node.output) }}</summary>
          <div class="output-tree"><JsonNode :value="e.node.output" /></div>
        </details>

        <button
          v-if="e.node.child_run_id"
          class="item-child"
          title="查看子工作流运行"
          @click.stop="emit('open-child', e.node.child_run_id!)"
        >
          子工作流运行详情 →
        </button>
      </article>
    </div>
    <button v-if="!follow" class="activity-bottom" @click="backToBottom">↓ 回到最新</button>
  </div>
</template>

<style scoped>
.activity {
  display: flex;
  flex-direction: column;
  min-height: 0;
  flex: 1;
}

.activity-progress {
  padding: 6px 2px;
  font-size: 12px;
  color: var(--text2);
}

.progress-sep {
  margin: 0 4px;
  color: var(--text3);
}

.activity-loss {
  color: var(--warn);
}

.activity-list {
  flex: 1;
  min-height: 120px;
  overflow-y: auto;
  border: 1px solid var(--border);
  border-radius: 6px;
  background: var(--surface2);
  padding: 6px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.activity-empty {
  color: var(--text3);
  font-size: 12px;
  text-align: center;
  padding: 24px 8px;
}

.activity-item {
  border: 1px solid var(--border);
  border-radius: 6px;
  background: var(--surface);
  padding: 6px 10px;
  cursor: pointer;
}

.activity-item:hover {
  border-color: var(--run);
}

.activity-item.st-pending {
  opacity: 0.55;
}

.item-head {
  display: flex;
  align-items: baseline;
  gap: 8px;
  font-size: 12.5px;
  flex-wrap: wrap;
}

.item-glyph {
  flex-shrink: 0;
  width: 16px;
  text-align: center;
}

.glyph-running {
  color: var(--run);
  display: inline-block;
  animation: glyph-spin 1.2s linear infinite;
}

.glyph-retrying {
  color: var(--warn);
}

.glyph-completed {
  color: var(--ok);
}

.glyph-failed {
  color: var(--danger);
}

.glyph-skipped,
.glyph-pending {
  color: var(--text3);
}

@keyframes glyph-spin {
  to {
    transform: rotate(360deg);
  }
}

.item-name {
  font-weight: 600;
  max-width: 200px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.item-type {
  color: var(--text3);
  font-size: 11px;
  border: 1px solid var(--border);
  border-radius: 3px;
  padding: 0 4px;
}

.item-attempts {
  color: var(--warn);
  font-size: 11px;
}

.item-state {
  margin-left: auto;
  color: var(--text2);
  font-size: 11px;
}

.activity-item.st-completed .item-state {
  color: var(--ok);
}

.activity-item.st-failed .item-state {
  color: var(--danger);
}

.activity-item.st-running .item-state,
.activity-item.st-retrying .item-state {
  color: var(--run);
}

.item-duration {
  color: var(--text3);
  font-size: 11px;
}

.item-error {
  margin-top: 4px;
  color: var(--danger);
  font-size: 12px;
  word-break: break-all;
  white-space: pre-wrap;
}

.item-reason {
  margin-top: 4px;
  color: var(--text3);
  font-size: 12px;
}

.item-wait {
  margin-top: 4px;
  color: var(--warn);
  font-size: 12px;
}

.item-logs {
  margin-top: 4px;
  display: flex;
  flex-direction: column;
  gap: 1px;
}

.log-line {
  display: flex;
  gap: 6px;
  font-size: 12px;
  line-height: 1.5;
  align-items: baseline;
}

.log-attempt {
  flex-shrink: 0;
  color: var(--warn);
  font-size: 10px;
}

.log-marker {
  flex-shrink: 0;
  color: var(--text3);
}

.log-msg {
  word-break: break-all;
  white-space: pre-wrap;
  color: var(--text2);
}

.log-line.lvl-warn .log-msg {
  color: var(--warn);
}

.log-line.lvl-error .log-msg {
  color: var(--danger);
}

.log-line.lvl-debug .log-msg {
  color: var(--text3);
}

.log-expand {
  align-self: flex-start;
  border: none;
  background: none;
  padding: 2px 0;
  font-size: 11px;
  color: var(--text3);
  cursor: pointer;
}

.log-expand:hover {
  color: var(--text);
}

.item-output {
  margin-top: 4px;
  font-size: 12px;
}

.item-output summary {
  color: var(--text2);
  cursor: pointer;
  word-break: break-all;
  font-family: var(--mono);
  font-size: 11px;
  overflow-wrap: anywhere;
}

.output-tree {
  margin-top: 4px;
  border-left: 2px solid var(--border);
  padding-left: 8px;
  max-height: 260px;
  overflow: auto;
}

.item-child {
  margin-top: 4px;
  border: none;
  background: none;
  padding: 0;
  font-size: 11px;
  color: var(--run);
  cursor: pointer;
}

.activity-bottom {
  position: relative;
  margin-top: 4px;
  align-self: center;
  font-size: 11px;
  padding: 2px 10px;
}
</style>
