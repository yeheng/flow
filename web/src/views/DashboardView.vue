<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import * as api from "../api/flow";
import { client, errText } from "../rpc/client";
import { activeCount, perWorkflowRates, successRate, type WorkflowRate } from "../state/dashboard";
import { editor, refreshWorkflows } from "../state/editor";
import { runBadgeClass, runStatusLabel } from "../state/labels";
import { approvalSummary } from "../state/approvals";
import { fmtTime } from "../state/time";
import { usePolling } from "../state/polling";
import { toast } from "../state/toast";
import { copyWithToast } from "../state/clipboard";
import type { ApprovalItem, RunRecord, RunStats } from "../types";
import AppButton from "../components/AppButton.vue";
import ApprovalCard from "../components/ApprovalCard.vue";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

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


function fmtRate(rate: number | null): string {
  return rate === null ? "—" : `${(rate * 100).toFixed(0)}%`;
}

function workflowName(r: WorkflowRate): string {
  return workflowNames.value.get(r.workflowId) ?? r.workflowId;
}

async function refresh(): Promise<void> {
  void refreshApprovals();
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
  await copyWithToast(id, "run id");
}

// ---- 待审批收件箱：进入页面拉一次 + 60s 轮询 + 手动刷新 ----

const approvals = ref<ApprovalItem[]>([]);
const approvalsLoading = ref(false);

async function refreshApprovals(): Promise<void> {
  if (approvalsLoading.value) return;
  approvalsLoading.value = true;
  try {
    approvals.value = (await api.approvalsList()).items;
  } catch {
    // 收件箱拉取失败不打断仪表盘：60s 轮询/手动刷新会重试，也不伪造空态
  } finally {
    approvalsLoading.value = false;
  }
}

/** 收件箱只展示待处理项（服务端 pending 在前、deadline 升序） */
const pendingApprovals = computed(() => approvals.value.filter((a) => a.status === "pending"));

function approvalWorkflowName(a: ApprovalItem): string {
  return workflowNames.value.get(a.workflow_id) ?? a.workflow_id;
}

// 一次性加载 + 手动刷新；断线期间错过的事件由重连回调补齐，无需常驻轮询
const offReconnect = client.onReconnect(() => {
  void refresh();
  void refreshApprovals();
});

// 审批是人工等待面：低频轮询（页面不可见暂停，恢复时立即补拉）
usePolling(refreshApprovals, 60_000);

onMounted(() => {
  void refresh();
  void refreshApprovals();
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

    <div class="cards grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
      <Card class="card gap-2 py-4 shadow-sm">
        <CardHeader class="px-6 pb-0">
          <CardTitle class="card-label text-sm font-medium">工作流</CardTitle>
        </CardHeader>
        <CardContent class="px-6">
          <div class="card-value text-2xl font-bold">{{ editor.workflows.length }}</div>
        </CardContent>
      </Card>
      <Card class="card gap-2 py-4 shadow-sm">
        <CardHeader class="px-6 pb-0">
          <CardTitle class="card-label text-sm font-medium">运行总数</CardTitle>
        </CardHeader>
        <CardContent class="px-6">
          <div class="card-value text-2xl font-bold">{{ summary.total }}</div>
        </CardContent>
      </Card>
      <Card class="card gap-2 py-4 shadow-sm">
        <CardHeader class="px-6 pb-0">
          <CardTitle class="card-label text-sm font-medium">成功率</CardTitle>
        </CardHeader>
        <CardContent class="px-6">
          <div class="card-value ok text-2xl font-bold">{{ fmtRate(summary.successRate) }}</div>
        </CardContent>
      </Card>
      <Card class="card gap-2 py-4 shadow-sm">
        <CardHeader class="px-6 pb-0">
          <CardTitle class="card-label text-sm font-medium">进行中</CardTitle>
        </CardHeader>
        <CardContent class="px-6">
          <div class="card-value running text-2xl font-bold">{{ summary.active }}</div>
        </CardContent>
      </Card>
    </div>

    <p v-if="!firstRun" class="capacity" :class="{ warn: capacity.warn }" :title="capacityTitle">
      非终态 {{ capacity.active }} / {{ RUN_CAP }}
      <span class="capacity-sub">在飞派发上限 {{ DISPATCH_CAP }}</span>
    </p>

    <!-- 待审批收件箱：有等待项才占屏；卡片可直接裁决，也可跳 run 详情 -->
    <section v-if="pendingApprovals.length > 0" class="section">
      <div class="approval-head-row">
        <h3>待审批（{{ pendingApprovals.length }}）</h3>
        <AppButton :disabled="approvalsLoading" @click="refreshApprovals()">
          {{ approvalsLoading ? "刷新中…" : "刷新" }}
        </AppButton>
      </div>
      <div class="approval-cards">
        <div
          v-for="a in pendingApprovals"
          :key="`${a.run_id}:${a.operation_id}`"
          class="approval-inbox-card"
        >
          <div class="approval-inbox-meta">
            <RouterLink class="link mono" :to="`/runs/${a.run_id}`" :title="a.run_id">
              run {{ a.run_id.slice(0, 8) }}…
            </RouterLink>
            <RouterLink class="link" :to="`/workflows/${a.workflow_id}`">
              {{ approvalWorkflowName(a) }}
            </RouterLink>
            <span class="muted">v{{ a.workflow_version }} · {{ a.node_id }}</span>
          </div>
          <p class="approval-inbox-summary mono" :title="approvalSummary(a.specs)">
            {{ approvalSummary(a.specs) }}
          </p>
          <ApprovalCard :item="a" @resolved="refreshApprovals()" />
        </div>
      </div>
    </section>

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

.cards {
  margin-bottom: 16px;
}

.first-run {
  margin-bottom: 20px;
  padding: 4px 0 8px 14px;
  border-left: 2px solid var(--signal);
  max-width: 62ch;
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

.card-value {
  font-variant-numeric: tabular-nums;
}

.card-value.ok {
  color: var(--ok);
}

.card-value.running {
  color: var(--run);
}

.section {
  margin-bottom: 24px;
}

/* 待审批收件箱：窄卡纵排（审批卡自带操作区，横排会挤） */
.approval-head-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 10px;
}

.approval-head-row h3 {
  margin: 0;
}

.approval-cards {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.approval-inbox-card {
  padding: 10px 12px;
  border: 1px solid var(--signal);
  border-radius: var(--radius);
  background: color-mix(in srgb, var(--signal) 8%, var(--surface));
}

.approval-inbox-meta {
  display: flex;
  align-items: baseline;
  gap: 10px;
  flex-wrap: wrap;
  font-size: 12px;
}

.approval-inbox-summary {
  margin: 4px 0 6px;
  font-size: 11px;
  color: var(--text2);
  word-break: break-all;
  overflow: hidden;
  text-overflow: ellipsis;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
}

/* 容量口径小字：≥80% 用 warn 色提示接近平台上限 */
.capacity {
  margin: 0 0 20px;
  font-size: 12px;
  color: var(--text3);
}

.capacity.warn {
  color: var(--warn);
}

.capacity-sub {
  margin-left: 8px;
}



/* 名称缺失时回退渲染 36 字符 workflow_id，允许折行避免撑破表格 */
.wf-cell {
  word-break: break-all;
}
</style>
