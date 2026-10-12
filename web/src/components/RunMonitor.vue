<script setup lang="ts">
import { computed, reactive, ref, watch } from "vue";
import { useRouter } from "vue-router";
import {
  backToParentRun,
  cancelRun,
  deliverSignal,
  monitor,
  openChildRun,
  canCancelRun,
  waitingHumanTasks,
} from "../state/monitor";
import { editor } from "../state/editor";
import { nodeStateLabel, runStatusLabel } from "../state/labels";
import { confirmDialog } from "../state/modal";
import { toast } from "../state/toast";
import ActivityFeed from "./ActivityFeed.vue";
import LogConsole from "./LogConsole.vue";
import AppButton from "./AppButton.vue";

const props = withDefaults(
  defineProps<{ childRunNavigate?: boolean; layout?: "footer" | "sidebar" }>(),
  { childRunNavigate: false, layout: "sidebar" },
);

const router = useRouter();

/** 「活动」是默认叙事视图；时间线表格与原始日志是开发者界面 */
const activeTab = ref<"activity" | "timeline" | "logs">("activity");

const DEV_MODE_KEY = "flow.run.devMode";
const devMode = ref(localStorage.getItem(DEV_MODE_KEY) === "1");
watch(devMode, (on) => {
  if (!on && activeTab.value !== "activity") activeTab.value = "activity";
  localStorage.setItem(DEV_MODE_KEY, on ? "1" : "0");
});

/** run 状态文案：单一来源 monitor.status，配色与文案同源 */
const runStateText = computed(() => (monitor.status ? runStatusLabel(monitor.status) : ""));

/** human_task 信号输入框内容，key = node_id */
const signalTexts = reactive<Record<string, string>>({});

function fmtJson(v: unknown): string {
  if (v === undefined || v === null) return "";
  const s = JSON.stringify(v);
  return s.length > 200 ? s.slice(0, 200) + "…" : s;
}

/** 未截断的完整 JSON（截断展示的 title 悬浮全文用） */
function fmtJsonFull(v: unknown): string {
  if (v === undefined || v === null) return "";
  return JSON.stringify(v);
}

async function onCancelRun(): Promise<void> {
  if (!(await confirmDialog("确定取消该运行？", { danger: true }))) return;
  await cancelRun();
}

/** 非空且不是合法 JSON：交付时按字符串处理，提前给用户提示 */
function signalIsNonJson(text: string | undefined): boolean {
  const raw = (text ?? "").trim();
  if (!raw) return false;
  try {
    JSON.parse(raw);
    return false;
  } catch {
    return true;
  }
}

async function copyRunId(): Promise<void> {
  if (!monitor.runId) return;
  try {
    await navigator.clipboard.writeText(monitor.runId);
    toast.info("已复制 run id");
  } catch {
    toast.error("复制失败");
  }
}

function onRowEnter(nodeId: string): void {
  editor.highlightNodeId = nodeId;
}

function onRowLeave(): void {
  editor.highlightNodeId = null;
}

/** 点击行选中画布节点：仅在画布仍展示该 run 所属工作流时有效 */
function onRowClick(nodeId: string): void {
  if (monitor.workflowId === editor.workflowId) editor.selectedNodeId = nodeId;
}

/** 活动流的子 run 钻取：详情页路由跳转，编辑器面板内嵌切换 */
function onOpenChildRun(childRunId: string): void {
  if (props.childRunNavigate) router.push(`/runs/${childRunId}`);
  else void openChildRun(childRunId);
}
</script>

<template>
  <div class="run-monitor" :class="layout">
    <div class="monitor-main">
      <div class="run-status">
        <button
          v-if="monitor.breadcrumb.length"
          class="run-back"
          title="返回父 run"
          @click="backToParentRun()"
        >
          ← 父 run
        </button>
        <span v-if="monitor.breadcrumb.length" class="run-depth">
          子 run（第 {{ monitor.breadcrumb.length }} 层）
        </span>
        <span class="run-id" :title="monitor.runId ?? ''"
          >run {{ monitor.runId?.slice(0, 8) }}…</span
        >
        <button v-if="monitor.runId" class="run-copy" title="复制 run id" @click="copyRunId">
          复制
        </button>
        <span v-if="monitor.status" class="run-phase" :class="`run-${monitor.status}`">
          {{ runStateText }}
        </span>
        <AppButton v-if="canCancelRun" :disabled="monitor.cancelling" @click="onCancelRun"
          >取消</AppButton
        >
      </div>
      <div v-if="monitor.fatalError" class="run-fatal">{{ monitor.fatalError }}</div>
      <div v-if="monitor.output !== undefined" class="run-output">
        输出：<code>{{ fmtJson(monitor.output) }}</code>
      </div>

      <div v-for="n in waitingHumanTasks" :key="n.id" class="signal-box">
        <div>「{{ n.name || n.id }}」等待人工交付</div>
        <div class="signal-row">
          <input
            v-model="signalTexts[n.id]"
            type="text"
            placeholder='信号 payload（JSON），如 {"approved": true}'
          />
          <AppButton
            :disabled="monitor.delivering"
            @click="deliverSignal(n.id, signalTexts[n.id] ?? '')"
          >
            交付
          </AppButton>
        </div>
        <div v-if="signalIsNonJson(signalTexts[n.id])" class="field-help">
          非 JSON，将作为字符串交付
        </div>
      </div>
    </div>

    <div class="monitor-detail">
      <div class="run-tabs">
        <button
          class="run-tab"
          :class="{ active: activeTab === 'activity' }"
          @click="activeTab = 'activity'"
        >
          活动
        </button>
        <template v-if="devMode">
          <button
            class="run-tab"
            :class="{ active: activeTab === 'timeline' }"
            @click="activeTab = 'timeline'"
          >
            时间线
          </button>
          <button
            class="run-tab"
            :class="{ active: activeTab === 'logs' }"
            @click="activeTab = 'logs'"
          >
            日志（{{ monitor.logs.length }}）
          </button>
        </template>
        <label class="dev-toggle" title="显示时间线表格与原始日志控制台">
          <input v-model="devMode" type="checkbox" />
          开发者
        </label>
      </div>
      <ActivityFeed
        v-if="activeTab === 'activity'"
        class="monitor-activity"
        @select-node="onRowClick"
        @hover-node="(id) => (id === null ? onRowLeave() : onRowEnter(id))"
        @open-child="onOpenChildRun"
      />
      <div v-else-if="activeTab === 'timeline'" class="timeline-wrap">
        <table class="timeline">
          <thead>
            <tr>
              <th>节点</th>
              <th>状态</th>
              <th>尝试</th>
              <th>耗时</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="n in monitor.nodes"
              :key="n.id"
              :class="{ highlighted: editor.highlightNodeId === n.id }"
              @mouseenter="onRowEnter(n.id)"
              @mouseleave="onRowLeave"
              @click="onRowClick(n.id)"
            >
              <td>
                {{ n.name || n.id }}
                <RouterLink
                  v-if="n.child_run_id && props.childRunNavigate"
                  class="child-link"
                  title="查看子 run"
                  :to="`/runs/${n.child_run_id}`"
                  @click.stop
                >
                  子 run →
                </RouterLink>
                <button
                  v-else-if="n.child_run_id"
                  class="child-link"
                  title="查看子 run"
                  @click.stop="openChildRun(n.child_run_id)"
                >
                  子 run →
                </button>
              </td>
              <td>
                <span class="run-state" :class="`run-${n.state}`">
                  {{ nodeStateLabel(n.state) }}
                </span>
                <div v-if="n.error" class="node-error">{{ n.error }}</div>
                <div v-else-if="n.reason" class="node-reason">{{ n.reason }}</div>
                <div
                  v-else-if="n.output !== null && n.output !== undefined"
                  class="node-output"
                  :title="fmtJsonFull(n.output)"
                >
                  {{ fmtJson(n.output) }}
                </div>
              </td>
              <td>{{ n.attempts }}</td>
              <td>{{ n.duration_ms !== null ? `${n.duration_ms}ms` : "" }}</td>
            </tr>
          </tbody>
        </table>
      </div>
      <LogConsole v-else class="monitor-log-console" @select-node="onRowClick" />
    </div>
  </div>
</template>

<style scoped>
/* 布局：main = 状态/输出/信号，detail = 时间线/日志 tab。
   footer 模式左右分栏（左输出右明细），sidebar 模式上下堆叠（默认，RunDetailView 同形态） */
.run-monitor {
  flex: 1;
  min-height: 0;
  min-width: 0;
  display: flex;
  flex-direction: column;
}

.run-monitor.footer {
  flex-direction: row;
  gap: 12px;
}

.monitor-main {
  min-width: 0;
}

.run-monitor.footer .monitor-main {
  width: 40%;
  flex-shrink: 0;
  overflow-y: auto;
}

/* 侧栏模式：输出区限高，明细区占剩余空间，各自内部滚动 */
.run-monitor.sidebar .monitor-main {
  max-height: 50%;
  overflow-y: auto;
  flex-shrink: 0;
}

.monitor-detail {
  flex: 1;
  min-height: 0;
  min-width: 0;
  display: flex;
  flex-direction: column;
}

.run-monitor.footer .monitor-detail {
  border-left: 1px solid var(--border);
  padding-left: 12px;
}

.timeline-wrap {
  flex: 1;
  min-height: 0;
  overflow: auto;
}

.run-tabs {
  display: flex;
  gap: 2px;
  border-bottom: 1px solid var(--border);
  margin-top: 6px;
}

.run-tab {
  border: none;
  background: none;
  padding: 4px 10px;
  font-size: 12px;
  color: var(--text2);
  border-bottom: 2px solid transparent;
  cursor: pointer;
}

.run-tab.active {
  color: var(--text);
  border-bottom-color: var(--run);
}

.dev-toggle {
  margin-left: auto;
  display: flex;
  gap: 4px;
  align-items: center;
  font-size: 11px;
  color: var(--text3);
  cursor: pointer;
  user-select: none;
}

.monitor-activity {
  min-height: 140px;
}

.monitor-log-console {
  /* tab 内容区：时间线/日志都在 detail 区内滚动，给头部/状态行留高度 */
  min-height: 140px;
}

.run-status {
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 8px 0;
}

.run-id {
  font-family: var(--mono);
  color: var(--text2);
}

.run-copy {
  padding: 1px 8px;
  font-size: 11px;
}

.run-phase.run-running,
.run-state.run-running {
  color: var(--run);
}

/* 挂起待恢复：不是终态也不是正常推进，用 warn 与 running 区分开 */
.run-phase.run-awaiting_resume {
  color: var(--warn);
}

.run-phase.run-succeeded,
.run-state.run-completed {
  color: var(--ok);
}

.run-phase.run-failed,
.run-phase.run-cancelled,
.run-state.run-failed {
  color: var(--danger);
}

.run-state.run-retrying {
  color: var(--warn);
}

.run-state.run-skipped,
.run-state.run-pending {
  color: var(--text3);
}

.run-fatal {
  color: var(--danger);
  margin-bottom: 8px;
  word-break: break-all;
}

.run-output {
  margin-bottom: 8px;
  word-break: break-all;
}

.signal-box {
  border: 1px solid var(--warn);
  background: rgba(210, 153, 34, 0.1);
  border-radius: 6px;
  padding: 8px;
  margin-bottom: 8px;
}

.signal-row {
  display: flex;
  gap: 6px;
  margin-top: 6px;
}

.timeline {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}

.timeline th,
.timeline td {
  text-align: left;
  padding: 4px 6px;
  border-bottom: 1px solid var(--border);
  vertical-align: top;
}

.timeline th {
  color: var(--text3);
  font-weight: 400;
}

.timeline tbody tr {
  cursor: pointer;
}

.timeline tr.highlighted td {
  background: rgba(124, 108, 255, 0.15);
}

.node-error {
  color: var(--danger);
  word-break: break-all;
}

.node-reason,
.node-output {
  color: var(--text2);
  word-break: break-all;
  font-family: var(--mono);
  font-size: 11px;
}

.run-back {
  padding: 2px 8px;
}

.run-depth {
  color: var(--text3);
  font-size: 11px;
}

.child-link {
  margin-left: 6px;
  padding: 0 6px;
  font-size: 11px;
}
</style>
