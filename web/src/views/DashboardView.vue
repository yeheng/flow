<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import * as api from "../api/flow";
import { errText } from "../rpc/client";
import { perWorkflowRates, summarizeRuns, type WorkflowRate } from "../state/dashboard";
import { editor, refreshWorkflows } from "../state/editor";
import { toast } from "../state/toast";
import type { RunRecord } from "../types";

/** 仪表盘：全局概览。run 统计基于 run.list 最新 500 条客户端聚合（见 state/dashboard.ts 的局限说明） */
const runs = ref<RunRecord[]>([]);
let timer: ReturnType<typeof setInterval> | null = null;

const workflowNames = computed(() => new Map(editor.workflows.map((w) => [w.workflow_id, w.name])));
const summary = computed(() => summarizeRuns(runs.value));
const rates = computed(() => perWorkflowRates(runs.value));
const recent = computed(() => runs.value.slice(0, 10));

const statusLabel: Record<string, string> = {
  running: "运行中",
  initializing: "初始化中",
  awaiting_resume: "挂起待恢复",
  succeeded: "成功",
  failed: "失败",
  cancelled: "已取消",
};

function badgeClass(status: string): string {
  if (status === "awaiting_resume" || status === "initializing") return "run-awaiting";
  return `run-${status}`;
}

function fmtTime(iso: string): string {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

function fmtRate(rate: number | null): string {
  return rate === null ? "—" : `${(rate * 100).toFixed(0)}%`;
}

function workflowName(r: WorkflowRate): string {
  return workflowNames.value.get(r.workflowId) ?? r.workflowId;
}

async function refresh(): Promise<void> {
  try {
    if (editor.workflows.length === 0) await refreshWorkflows();
    runs.value = await api.listRuns({ limit: 500 });
  } catch (e) {
    toast.error(errText(e));
  }
}

onMounted(async () => {
  await refresh();
  timer = setInterval(() => void refresh(), 5000);
});

onUnmounted(() => {
  if (timer) clearInterval(timer);
});
</script>

<template>
  <div class="page">
    <div class="page-header">
      <h2>仪表盘</h2>
    </div>

    <div class="cards">
      <div class="card">
        <div class="card-value">{{ editor.workflows.length }}</div>
        <div class="card-label">工作流</div>
      </div>
      <div class="card">
        <div class="card-value">{{ summary.total }}</div>
        <div class="card-label">运行总数（近 500 条内）</div>
      </div>
      <div class="card">
        <div class="card-value ok">{{ fmtRate(summary.successRate) }}</div>
        <div class="card-label">成功率</div>
      </div>
      <div class="card">
        <div class="card-value running">{{ summary.active }}</div>
        <div class="card-label">进行中</div>
      </div>
    </div>

    <section class="section">
      <h3>最近运行</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>Run</th>
            <th>工作流</th>
            <th>状态</th>
            <th>开始时间</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="r in recent" :key="r.id">
            <td class="mono">{{ r.id.slice(0, 8) }}…</td>
            <td>
              {{ workflowNames.get(r.workflow_id) ?? r.workflow_id }}
              <span class="muted">v{{ r.workflow_version }}</span>
            </td>
            <td>
              <span class="badge" :class="badgeClass(r.status)">
                {{ statusLabel[r.status] ?? r.status }}
              </span>
            </td>
            <td>{{ fmtTime(r.started_at) }}</td>
            <td class="actions">
              <RouterLink class="link" :to="`/runs/${r.id}`">详情</RouterLink>
            </td>
          </tr>
        </tbody>
      </table>
      <p v-if="recent.length === 0" class="wf-empty">暂无运行记录</p>
    </section>

    <section class="section">
      <h3>按工作流成功率</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>工作流</th>
            <th>运行数</th>
            <th>成功数</th>
            <th>成功率</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="r in rates" :key="r.workflowId">
            <td>
              <RouterLink class="link" :to="`/workflows/${r.workflowId}`">
                {{ workflowName(r) }}
              </RouterLink>
            </td>
            <td>{{ r.total }}</td>
            <td>{{ r.succeeded }}</td>
            <td>{{ fmtRate(r.successRate) }}</td>
          </tr>
        </tbody>
      </table>
      <p v-if="rates.length === 0" class="wf-empty">暂无数据</p>
    </section>
  </div>
</template>

<style scoped>
.cards {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 12px;
  margin-bottom: 24px;
}

.card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 14px 16px;
}

.card-value {
  font-size: 22px;
  font-weight: 600;
  font-family: var(--mono);
}

.card-value.ok {
  color: var(--ok);
}

.card-value.running {
  color: var(--run);
}

.card-label {
  margin-top: 4px;
  color: var(--text3);
  font-size: 11px;
}

.section {
  margin-bottom: 24px;
}
</style>
