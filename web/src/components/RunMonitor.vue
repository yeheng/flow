<script setup lang="ts">
import { computed, reactive, ref } from "vue";
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
import LogConsole from "./LogConsole.vue";

const props = withDefaults(defineProps<{ childRunNavigate?: boolean }>(), {
  childRunNavigate: false,
});

const activeTab = ref<"timeline" | "logs">("timeline");

/** run 状态文案：单一来源 monitor.status，配色与文案同源（此前分家，见提交说明） */
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
</script>

<template>
  <div class="run-monitor">
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
      <span class="run-id" :title="monitor.runId ?? ''">run {{ monitor.runId?.slice(0, 8) }}…</span>
      <button v-if="monitor.runId" class="run-copy" title="复制 run id" @click="copyRunId">
        复制
      </button>
      <span v-if="monitor.status" class="run-phase" :class="`run-${monitor.status}`">
        {{ runStateText }}
      </span>
      <button v-if="canCancelRun" :disabled="monitor.cancelling" @click="onCancelRun">取消</button>
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
        <button
          :disabled="monitor.delivering"
          @click="deliverSignal(n.id, signalTexts[n.id] ?? '')"
        >
          交付
        </button>
      </div>
      <div v-if="signalIsNonJson(signalTexts[n.id])" class="field-help">
        非 JSON，将作为字符串交付
      </div>
    </div>

    <div class="run-tabs">
      <button
        class="run-tab"
        :class="{ active: activeTab === 'timeline' }"
        @click="activeTab = 'timeline'"
      >
        时间线
      </button>
      <button class="run-tab" :class="{ active: activeTab === 'logs' }" @click="activeTab = 'logs'">
        日志（{{ monitor.logs.length }}）
      </button>
    </div>
    <template v-if="activeTab === 'timeline'">
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
              <div v-else-if="n.output !== null && n.output !== undefined" class="node-output" :title="fmtJsonFull(n.output)">
                {{ fmtJson(n.output) }}
              </div>
            </td>
            <td>{{ n.attempts }}</td>
            <td>{{ n.duration_ms !== null ? `${n.duration_ms}ms` : "" }}</td>
          </tr>
        </tbody>
      </table>
    </template>
    <LogConsole v-else class="monitor-log-console" @select-node="onRowClick" />
  </div>
</template>

<style scoped>
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

.monitor-log-console {
  min-height: 260px;
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
.run-phase.run-initializing,
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
