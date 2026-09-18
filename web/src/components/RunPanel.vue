<script setup lang="ts">
import { reactive } from "vue";
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

const phaseLabel: Record<string, string> = {
  running: "运行中",
  succeeded: "成功",
  failed: "失败",
  cancelled: "已取消",
};

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
  <div class="run-panel">
    <h3>运行</h3>
    <div class="field">
      <label>输入 JSON（可选）</label>
      <textarea
        v-model="monitor.inputText"
        class="code"
        rows="2"
        spellcheck="false"
        placeholder='{"key": "value"}'
      ></textarea>
    </div>

    <template v-if="monitor.runId">
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
        <span class="run-id" :title="monitor.runId">run {{ monitor.runId.slice(0, 8) }}…</span>
        <span v-if="monitor.phase" class="run-phase" :class="`run-${monitor.phase}`">
          {{ phaseLabel[monitor.phase] ?? monitor.phase }}
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
              <button
                v-if="n.child_run_id"
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
    </template>
    <p v-else class="run-hint">在左侧工具栏点击「运行」启动最新已发布版本</p>
  </div>
</template>
