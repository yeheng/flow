<script setup lang="ts">
import { reactive } from "vue";
import {
  cancelRun,
  deliverSignal,
  monitor,
  runActive,
  waitingHumanTasks,
} from "../state/monitor";

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
          <tr v-for="n in monitor.nodes" :key="n.id">
            <td>{{ n.name || n.id }}</td>
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
