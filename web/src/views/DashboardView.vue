<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import * as api from "../api/flow";
import { errText } from "../rpc/client";
import { activeCount, perWorkflowRates, successRate, type WorkflowRate } from "../state/dashboard";
import { editor, refreshWorkflows } from "../state/editor";
import { toast } from "../state/toast";
import type { RunRecord, RunStats } from "../types";

/** 仪表盘：全局概览。统计来自 run.stats（服务端 GROUP BY 精确计数）；
 *  最近运行表只是展示窗口，走 run.list(limit 10) */
const stats = ref<RunStats>({ total: 0, by_status: {}, by_workflow: [] });
const runs = ref<RunRecord[]>([]);
/** 首次拉取完成前不展示空态，避免「暂无…」闪烁 */
const loaded = ref(false);
/** 轮询失败只在 ok→error 跳变时 toast，避免每 5s 重复弹错 */
const pollFailed = ref(false);
let timer: ReturnType<typeof setInterval> | null = null;

const workflowNames = computed(() => new Map(editor.workflows.map((w) => [w.workflow_id, w.name])));
const summary = computed(() => ({
  total: stats.value.total,
  active: activeCount(stats.value.by_status),
  successRate: successRate(stats.value.by_status),
}));
const rates = computed(() => perWorkflowRates(stats.value.by_workflow));

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
    [stats.value, runs.value] = await Promise.all([api.runStats(), api.listRuns({ limit: 10 })]);
    pollFailed.value = false;
  } catch (e) {
    if (!pollFailed.value) toast.error(errText(e));
    pollFailed.value = true;
  } finally {
    loaded.value = true;
  }
}

async function copyRunId(id: string): Promise<void> {
  try {
    await navigator.clipboard.writeText(id);
    toast.success("已复制 run id");
  } catch {
    toast.error("复制失败，请手动复制");
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
        <div class="card-label">运行总数</div>
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
          <tr v-for="r in runs" :key="r.id">
            <td class="mono copyable" :title="r.id" @click="copyRunId(r.id)">
              {{ r.id.slice(0, 8) }}…
            </td>
            <td class="wf-cell">
              {{ workflowNames.get(r.workflow_id) ?? r.workflow_id }}
              <span class="muted">v{{ r.workflow_version }}</span>
            </td>
            <td>
              <span class="badge" :class="badgeClass(r.status)">
                {{ statusLabel[r.status] ?? r.status }}
              </span>
            </td>
            <td :title="r.started_at">{{ fmtTime(r.started_at) }}</td>
            <td class="actions">
              <RouterLink class="link" :to="`/runs/${r.id}`">详情</RouterLink>
            </td>
          </tr>
        </tbody>
      </table>
      <p v-if="!loaded" class="wf-empty">加载中…</p>
      <p v-else-if="runs.length === 0" class="wf-empty">暂无运行记录</p>
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
      <p v-if="!loaded" class="wf-empty">加载中…</p>
      <p v-else-if="rates.length === 0" class="wf-empty">暂无数据</p>
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

.copyable {
  cursor: pointer;
}

/* 名称缺失时回退渲染 36 字符 workflow_id，允许折行避免撑破表格 */
.wf-cell {
  word-break: break-all;
}
</style>
