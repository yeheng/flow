<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import * as api from "../api/flow";
import { client, errText } from "../rpc/client";
import { activeCount, perWorkflowRates, successRate, type WorkflowRate } from "../state/dashboard";
import { editor, refreshWorkflows } from "../state/editor";
import { runBadgeClass, runStatusLabel } from "../state/labels";
import { toast } from "../state/toast";
import type { RunRecord, RunStats } from "../types";
import AppButton from "../components/AppButton.vue";

/** 仪表盘：全局概览。统计来自 run.stats（服务端 GROUP BY 精确计数）；
 *  最近运行表只是展示窗口，走 run.list(limit 10) */
const stats = ref<RunStats>({ total: 0, by_status: {}, by_workflow: [] });
const runs = ref<RunRecord[]>([]);
/** 首次拉取完成前不展示空态，避免「暂无…」闪烁 */
const loaded = ref(false);
/** 刷新失败：展示错误条并保留上一次成功的 stats/最近运行，成功后清空 */
const error = ref("");
/** 是否已有过一次成功拉取：失败时据此说明表格里是旧数据 */
const hasData = ref(false);
/** 失败只在 ok→error 跳变时 toast：首刷/手动/重连可能连续触发，避免重复弹错 */
const pollFailed = ref(false);

const workflowNames = computed(() => new Map(editor.workflows.map((w) => [w.workflow_id, w.name])));
const summary = computed(() => ({
  total: stats.value.total,
  active: activeCount(stats.value.by_status),
  successRate: successRate(stats.value.by_status),
}));
const rates = computed(() => perWorkflowRates(stats.value.by_workflow));

/** 容量口径来自 OPS.md / 服务端常量：R_MAX=1000 在飞 run 上限（平台上限，加执行
 *  节点不提高）；A_MAX=16 在飞派发上限。非终态 = running + awaiting_resume */
const RUN_CAP = 1000;
const DISPATCH_CAP = 16;
const capacity = computed(() => {
  const active = activeCount(stats.value.by_status);
  return { active, warn: active >= RUN_CAP * 0.8 };
});

/** 零数据首屏：给引导，而不是先展示 0/— 与运维口径 */
const firstRun = computed(
  () => loaded.value && !error.value && stats.value.total === 0 && editor.workflows.length === 0,
);

const capacityTitle = computed(
  () =>
    `非终态上限 ${RUN_CAP}（平台上限，加执行节点不提高）；在飞派发上限 ${DISPATCH_CAP}。口径见 OPS.md`,
);

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
    error.value = "";
    hasData.value = true;
    pollFailed.value = false;
  } catch (e) {
    error.value = errText(e);
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

// 一次性加载 + 手动刷新；断线期间错过的事件由重连回调补齐，无需常驻轮询
const offReconnect = client.onReconnect(() => void refresh());

onMounted(() => {
  void refresh();
});

onUnmounted(() => {
  offReconnect();
});
</script>

<template>
  <div class="page">
    <div class="page-header">
      <h2>仪表盘</h2>
      <AppButton @click="refresh()">刷新</AppButton>
    </div>

    <div v-if="error" role="alert" class="error-banner">
      <span>
        加载失败：{{ error }}
        <template v-if="hasData">· 当前显示上一次成功刷新的数据</template>
      </span>
      <AppButton @click="refresh()">重试</AppButton>
    </div>

    <section v-if="firstRun" class="first-run">
      <h3>从这里开始</h3>
      <p>
        Flow 把流程编排成可发布的 DAG：编辑画布 → 发布 → 运行，在运行详情看节点、日志与输入输出；
        外部操作结果不明时会挂起等你裁决。
      </p>
      <div class="first-run-actions">
        <RouterLink class="link first-run-cta" to="/workflows">新建或导入工作流</RouterLink>
        <span class="muted">从左侧「工作流」进入，点「新建」即可开始。</span>
      </div>
    </section>

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

    <p v-if="!firstRun" class="capacity" :class="{ warn: capacity.warn }" :title="capacityTitle">
      非终态 {{ capacity.active }} / {{ RUN_CAP }}
      <span class="capacity-sub">· 在飞派发上限 {{ DISPATCH_CAP }}</span>
    </p>

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
            <td>
              <button type="button" class="copy-id" :title="r.id" @click="copyRunId(r.id)">
                {{ r.id.slice(0, 8) }}…
              </button>
            </td>
            <td class="wf-cell">
              {{ workflowNames.get(r.workflow_id) ?? r.workflow_id }}
              <span class="muted">v{{ r.workflow_version }}</span>
            </td>
            <td>
              <span class="badge" :class="runBadgeClass(r.status)">
                {{ runStatusLabel(r.status) }}
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
      <p v-else-if="!error && runs.length === 0" class="wf-empty">暂无运行记录</p>
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
      <p v-else-if="!error && rates.length === 0" class="wf-empty">暂无数据</p>
    </section>
  </div>
</template>

<style scoped>
.error-banner {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;
  padding: 8px 12px;
  border: 1px solid var(--danger);
  border-radius: 8px;
  background: color-mix(in srgb, var(--danger) 12%, transparent);
  color: var(--danger);
  font-size: 12px;
}

.cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 12px;
  margin-bottom: 24px;
}

.first-run {
  margin-bottom: 20px;
  padding: 16px 18px;
  border: 1px solid var(--border2);
  border-radius: var(--radius);
  background: var(--surface);
}

.first-run h3 {
  margin: 0 0 6px;
}

.first-run p {
  margin: 0 0 10px;
  color: var(--text2);
  font-size: 13px;
  line-height: 1.6;
}

.first-run-actions {
  display: flex;
  align-items: baseline;
  gap: 12px;
  flex-wrap: wrap;
}

.first-run-cta {
  font-weight: 600;
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

/* 容量口径小字：≥80% 用 warn 色提示接近平台上限 */
.capacity {
  margin: -12px 0 20px;
  font-size: 11px;
  color: var(--text3);
}

.capacity.warn {
  color: var(--warn);
}

.capacity-sub {
  margin-left: 8px;
}

.capacity-src {
  margin-left: 8px;
  opacity: 0.8;
}

/* copy-cell 按钮化：剥掉 button 默认外观，保持原可点击文本单元格样式 */
.copy-id {
  background: none;
  border: none;
  padding: 0;
  color: inherit;
  font: inherit;
  font-family: var(--mono);
  text-align: left;
  cursor: pointer;
}

.copy-id:hover {
  background: none;
  border-color: transparent;
  text-decoration: underline;
}

/* 名称缺失时回退渲染 36 字符 workflow_id，允许折行避免撑破表格 */
.wf-cell {
  word-break: break-all;
}
</style>
