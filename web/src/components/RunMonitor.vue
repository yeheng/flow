<script setup lang="ts">
import { computed, reactive } from "vue";
import {
  backToParentRun,
  cancelRun,
  deliverSignal,
  monitor,
  openChildRun,
  runActive,
  waitingHumanTasks,
} from "../state/monitor";
import { editor } from "../state/editor";

const props = withDefaults(defineProps<{ childRunNavigate?: boolean }>(), {
  childRunNavigate: false,
});

const phaseLabel: Record<string, string> = {
  running: "运行中",
  succeeded: "成功",
  failed: "失败",
  cancelled: "已取消",
};

/** run 投影状态（run.timeline status）的特殊展示；其余回落 fold phase */
const statusLabel: Record<string, string> = {
  initializing: "初始化中",
  awaiting_resume: "挂起待恢复",
};

const runPhaseLabel = computed(() => {
  if (monitor.status && statusLabel[monitor.status]) return statusLabel[monitor.status];
  return phaseLabel[monitor.phase ?? ""] ?? monitor.phase ?? "";
});

const stateLabel: Record<string, string> = {
  pending: "等待",
  running: "运行中",
  retrying: "重试中",
  completed: "完成",
  failed: "失败",
  skipped: "跳过",
};

/** human_task 信号输入框内容，key = node_id */
const signalTexts = reactive<Record<string, string>>({});

function fmtJson(v: unknown): string {
  if (v === undefined || v === null) return "";
  const s = JSON.stringify(v);
  return s.length > 200 ? s.slice(0, 200) + "…" : s;
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
      <span v-if="monitor.phase" class="run-phase" :class="`run-${monitor.phase}`">
        {{ runPhaseLabel }}
      </span>
      <button v-if="runActive" @click="cancelRun()">取消</button>
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
        <button @click="deliverSignal(n.id, signalTexts[n.id] ?? '')">交付</button>
      </div>
    </div>

    <table class="timeline">
      <thead>
        <tr>
          <th>节点</th>
          <th>状态</th>
          <th>次数</th>
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
              {{ stateLabel[n.state] ?? n.state }}
            </span>
            <div v-if="n.error" class="node-error">{{ n.error }}</div>
            <div v-else-if="n.reason" class="node-reason">{{ n.reason }}</div>
            <div v-else-if="n.output !== null && n.output !== undefined" class="node-output">
              {{ fmtJson(n.output) }}
            </div>
          </td>
          <td>{{ n.attempts }}</td>
          <td>{{ n.duration_ms !== null ? `${n.duration_ms}ms` : "" }}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<style scoped>
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

.run-phase.run-running,
.run-state.run-running {
  color: var(--run);
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
  cursor: default;
}

.timeline tr.highlighted td {
  background: rgba(163, 113, 247, 0.15);
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
