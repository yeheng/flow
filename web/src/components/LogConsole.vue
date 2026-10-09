<script setup lang="ts">
import { computed, nextTick, onUnmounted, ref, watch } from "vue";
import { monitor } from "../state/monitor";
import { logWindow } from "../state/monitor-logic";
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

/**
 * 一帧的日志快照：过滤结果 + 供空态/下拉用的派生量。
 *
 * **模板只读这一个 ref**（及其派生 computed），不直接读 `monitor.logs`。
 * 这是 rAF 节流能成立的前提：只要渲染依赖里还有 `logs` / `logs.length`，
 * Vue 3.4+ 就会在每次 push 时把渲染标脏 → 每追加一行重渲染一次 →
 * `filtered` 每行全量重算一次（1 万行 × 1 万次 = 节流形同虚设，实测
 * 200 行进 200 次重渲染）。把结果搬进 ref、在 rAF 回调里一次性算完，
 * 行数推送就只在 `scheduleFrame` 里留一个脏标记，不再触发渲染。
 */
interface LogFrame {
  /** 过滤后的全量行（窗口化之前） */
  rows: LogLine[];
  /** 是否收到过任何日志（空态判据；不过滤，与旧实现同义） */
  hasLogs: boolean;
  /** 日志里出现过的节点 id（过滤下拉） */
  nodeIds: string[];
}

// 节点过滤走索引；级别/关键词过滤没有索引可走（关键词本质上要全量匹配），
// 但每帧只算一次（见 LogFrame），刷屏节点下不再每行都全量重算
function computeFrame(): LogFrame {
  const min = showDebug.value ? LEVEL_ORDER.debug : LEVEL_ORDER.info;
  const kw = searchText.value.trim().toLowerCase();
  const source = nodeFilter.value ? (monitor.logsByNode[nodeFilter.value] ?? []) : monitor.logs;
  const rows = source.filter((l) => {
    if (LEVEL_ORDER[l.level] < min) return false;
    if (kw && !l.message.toLowerCase().includes(kw)) return false;
    return true;
  });
  const ids = new Set(monitor.logs.map((l) => l.node_id).filter(Boolean));
  return { rows, hasLogs: monitor.logs.length > 0, nodeIds: [...ids] };
}

// 初始值就地算一帧（而非 onMounted 里补）：setup 里的这次求值不进任何
// effect，不建立订阅，但首帧渲染就拿到已有日志——否则切到日志 tab 的
// 第一帧会闪一下「暂无日志」
const frame = ref<LogFrame>(computeFrame());

// 过滤条件或 run 变化后窗口起点重置：新过滤集的"最近 N 行"语义才成立
watch([showDebug, nodeFilter, searchText, () => monitor.runId], () => {
  extraLines.value = 0;
  scheduleFrame();
});

/**
 * 刷屏时的节流：一次 rAF 合并一帧的所有日志追加（`scheduleFrame` 只留脏
 * 标记，不做事）。过滤条件**没有**变化时结果其实不变，所以攒到下一帧算
 * 一次即可，视觉上无差别。
 */
let frameRaf: number | null = null;
let frameDirty = false;

/** 请求重算一帧（同一帧内多次调用只算一次） */
function scheduleFrame(): void {
  frameDirty = true;
  if (frameRaf !== null) return;
  frameRaf = requestAnimationFrame(() => {
    frameRaf = null;
    if (frameDirty) {
      frameDirty = false;
      frame.value = computeFrame();
    }
  });
}

// 日志**增长或缩短**都要重算：环形裁剪会让长度减少，同样要反映到结果
watch(
  () => monitor.logs.length,
  () => {
    scheduleFrame();
  },
);

onUnmounted(() => {
  if (frameRaf !== null) cancelAnimationFrame(frameRaf);
});

/** 起始窗口：DOM 一次只渲染这么多行（状态存全量，预算封顶） */
const RENDER_CAP = 1500;
/** 「加载更早」每次向前扩开的行数 */
const EARLIER_STEP = 1500;
/**
 * 已向前扩开的行数上限。**必须有天花板**：没有它，「加载更早」就是把
 * RENDER_CAP 从安全上限变成累加器——点六次 = 10500 行 DOM，正好是这段代码
 * 存在的理由（避免万行节点卡死）被自己撤销。留 4 倍余量够翻历史。
 */
const MAX_EARLIER = RENDER_CAP * 4;
/** 已向前扩开的行数（过滤/run 变化时重置） */
const extraLines = ref(0);

const windowed = computed(() => logWindow(frame.value.rows.length, RENDER_CAP, extraLines.value));
const rendered = computed(() => frame.value.rows.slice(-windowed.value.rendered));
const hiddenCount = computed(() => windowed.value.hidden);

/** 向前展开一段历史，保持视口停在原内容上（补偿 scrollHeight 增量） */
async function loadEarlier(): Promise<void> {
  if (extraLines.value >= MAX_EARLIER) return;
  const el = listEl.value;
  const prevHeight = el?.scrollHeight ?? 0;
  extraLines.value = Math.min(extraLines.value + EARLIER_STEP, MAX_EARLIER);
  await nextTick();
  if (el) el.scrollTop += el.scrollHeight - prevHeight;
}

function fmtTime(ts: string): string {
  const d = new Date(ts);
  return Number.isNaN(d.getTime()) ? ts : d.toLocaleTimeString();
}

/** 完整日期时间（时间列的 title 悬浮提示用） */
function fmtTimeFull(ts: string): string {
  const d = new Date(ts);
  return Number.isNaN(d.getTime()) ? ts : d.toLocaleString();
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
        <option v-for="id in frame.nodeIds" :key="id" :value="id">
          {{ nodeName(id) }}
        </option>
      </select>
      <input v-model="searchText" type="search" class="log-search" placeholder="搜索日志…" />
    </div>

    <!-- 空态/列表都只读 frame 快照：不直接摸 monitor.logs，见 LogFrame 注释 -->
    <div v-if="!frame.hasLogs" class="log-empty">
      暂无日志<span v-if="monitor.status === 'running'">（等待节点输出…）</span>
    </div>
    <template v-else>
      <div ref="listEl" class="log-list" @scroll="onScroll">
        <button v-if="hiddenCount > 0" class="log-earlier link" @click="loadEarlier">
          ↑ 加载更早（还有 {{ hiddenCount }} 条）
        </button>
        <div v-for="line in rendered" :key="line.seq" class="log-row" :class="`log-${line.level}`">
          <span class="log-ts" :title="fmtTimeFull(line.ts)">{{ fmtTime(line.ts) }}</span>
          <span class="log-level">{{ line.level }}</span>
          <button
            v-if="line.node_id"
            class="log-node link"
            :title="`查看节点 ${nodeName(line.node_id)}`"
            @click="onSelectNode(line.node_id)"
          >
            {{ nodeName(line.node_id)
            }}<template v-if="line.attempt > 1">×{{ line.attempt }}</template>
          </button>
          <span v-else class="log-node log-node-engine">⚙</span>
          <span v-if="line.stream !== 'engine'" class="log-stream">{{ line.stream }}</span>
          <span class="log-msg">{{ line.message }}</span>
        </div>
      </div>
      <button v-if="!follow" class="log-bottom" @click="backToBottom">↓ 回到底部并恢复跟随</button>
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

.log-earlier {
  display: block;
  width: 100%;
  color: var(--text3);
  font-size: 11px;
  text-align: center;
  padding: 2px 8px;
  border-bottom: 1px dashed var(--border);
  margin-bottom: 2px;
  cursor: pointer;
}

.log-earlier:hover {
  color: var(--text);
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
