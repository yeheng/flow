<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from "vue";
import * as api from "../api/flow";
import { errText } from "../rpc/client";
import { editor, refreshWorkflows } from "../state/editor";
import { confirmDialog } from "../state/modal";
import { mergeRunPage, nextCursor } from "../state/run-list";
import { toast } from "../state/toast";
import type { RunRecord } from "../types";

/** 全局运行历史（/runs）与单工作流运行历史（/workflows/:id/runs）共用：后者传 workflowId 过滤 */
const props = defineProps<{ workflowId?: string }>();

const PAGE_SIZE = 50;
const runs = ref<RunRecord[]>([]);
const statusFilter = ref("");
const hasMore = ref(false);
const loadingMore = ref(false);
let timer: ReturnType<typeof setInterval> | null = null;

const workflowNames = computed(() => new Map(editor.workflows.map((w) => [w.workflow_id, w.name])));

const STATUS_OPTIONS: { value: string; label: string }[] = [
  { value: "", label: "全部状态" },
  { value: "running", label: "运行中" },
  { value: "succeeded", label: "成功" },
  { value: "failed", label: "失败" },
  { value: "cancelled", label: "已取消" },
  { value: "awaiting_resume", label: "挂起待恢复" },
];

const statusLabel: Record<string, string> = {
  running: "运行中",
  initializing: "初始化中",
  awaiting_resume: "挂起待恢复",
  succeeded: "成功",
  failed: "失败",
  cancelled: "已取消",
};

/** 状态徽章着色沿用画布运行态体系：蓝 running / 绿 succeeded / 红 failed / 黄 awaiting / 灰 cancelled */
function badgeClass(status: string): string {
  if (status === "awaiting_resume" || status === "initializing") return "run-awaiting";
  return `run-${status}`;
}

function fmtTime(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

function fmtDuration(r: RunRecord): string {
  if (!r.ended_at) return "—";
  const ms = new Date(r.ended_at).getTime() - new Date(r.started_at).getTime();
  if (Number.isNaN(ms)) return "—";
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60_000)}m${Math.round((ms % 60_000) / 1000)}s`;
}

/** 轮询只刷首页（最新的 PAGE_SIZE 条），已翻出的旧页保持不动 */
async function refresh(): Promise<void> {
  try {
    const page = await api.listRuns({
      workflowId: props.workflowId,
      status: statusFilter.value || undefined,
      limit: PAGE_SIZE,
    });
    runs.value = mergeRunPage(page, runs.value);
    hasMore.value = page.length === PAGE_SIZE;
  } catch (e) {
    toast.error(errText(e));
  }
}

async function loadMore(): Promise<void> {
  const cursor = nextCursor(runs.value);
  if (!cursor || loadingMore.value) return;
  loadingMore.value = true;
  try {
    const page = await api.listRuns({
      workflowId: props.workflowId,
      status: statusFilter.value || undefined,
      beforeRunId: cursor,
      limit: PAGE_SIZE,
    });
    runs.value = mergeRunPage(runs.value, page);
    hasMore.value = page.length === PAGE_SIZE;
  } catch (e) {
    toast.error(errText(e));
  } finally {
    loadingMore.value = false;
  }
}

async function onCancel(r: RunRecord): Promise<void> {
  if (!(await confirmDialog(`确定取消 run ${r.id.slice(0, 8)}…？`))) return;
  try {
    await api.runCancel(r.id);
    await refresh();
  } catch (e) {
    toast.error(errText(e));
  }
}

// 过滤条件变化：丢弃已翻页数据，回到第一页
watch(statusFilter, () => {
  runs.value = [];
  void refresh();
});

onMounted(async () => {
  if (editor.workflows.length === 0) await refreshWorkflows();
  await refresh();
  // 页面激活期间每 3 秒轮询；离开页面停止
  timer = setInterval(() => void refresh(), 3000);
});

onUnmounted(() => {
  if (timer) clearInterval(timer);
});

watch(
  () => props.workflowId,
  () => {
    runs.value = [];
    void refresh();
  },
);
</script>

<template>
  <div class="page">
    <div class="page-header">
      <h2>运行记录</h2>
      <div class="header-tools">
        <select v-model="statusFilter" class="status-filter">
          <option v-for="o in STATUS_OPTIONS" :key="o.value" :value="o.value">
            {{ o.label }}
          </option>
        </select>
        <RouterLink v-if="workflowId" class="link" :to="`/workflows/${workflowId}`">
          打开编辑器 →
        </RouterLink>
      </div>
    </div>
    <table class="data-table">
      <thead>
        <tr>
          <th>Run</th>
          <th v-if="!workflowId">工作流</th>
          <th>状态</th>
          <th>开始时间</th>
          <th>耗时</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="r in runs" :key="r.id">
          <td class="mono">{{ r.id.slice(0, 8) }}…</td>
          <td v-if="!workflowId">
            {{ workflowNames.get(r.workflow_id) ?? r.workflow_id }}
            <span class="muted">v{{ r.workflow_version }}</span>
          </td>
          <td>
            <span class="badge" :class="badgeClass(r.status)">
              {{ statusLabel[r.status] ?? r.status }}
            </span>
          </td>
          <td>{{ fmtTime(r.started_at) }}</td>
          <td>{{ fmtDuration(r) }}</td>
          <td class="actions">
            <RouterLink class="link" :to="`/runs/${r.id}`">详情</RouterLink>
            <button v-if="r.status === 'running'" class="link danger" @click="onCancel(r)">
              取消
            </button>
          </td>
        </tr>
      </tbody>
    </table>
    <p v-if="runs.length === 0" class="wf-empty">暂无运行记录</p>
    <div v-if="hasMore" class="load-more">
      <button :disabled="loadingMore" @click="loadMore">加载更多</button>
    </div>
  </div>
</template>

<style scoped>
.header-tools {
  display: flex;
  align-items: center;
  gap: 12px;
}

.status-filter {
  width: auto;
}

.load-more {
  margin-top: 12px;
  display: flex;
  justify-content: center;
}
</style>
