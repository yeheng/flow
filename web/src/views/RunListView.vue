<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from "vue";
import * as api from "../api/flow";
import { errText } from "../rpc/client";
import { editor, refreshWorkflows } from "../state/editor";
import { confirmDialog } from "../state/modal";
import { mergeRunPage, nextCursor } from "../state/run-list";
import { runStatusLabel, sourceLabel } from "../state/labels";
import { toast } from "../state/toast";
import type { RunRecord } from "../types";

/** 全局运行历史（/runs）与单工作流运行历史（/workflows/:id/runs）共用：后者传 workflowId 过滤 */
const props = defineProps<{ workflowId?: string }>();

const PAGE_SIZE = 50;
const runs = ref<RunRecord[]>([]);
const statusFilter = ref("");
const sourceFilter = ref("");
const hasMore = ref(false);
const loadingMore = ref(false);
/** 首次拉取完成后置 true；过滤条件变化时保持 true，表区直接展示新结果而不闪空态 */
const loaded = ref(false);
/** 轮询失败只在 ok→error 跳变时 toast，避免每 3s 重复弹错 */
const pollFailed = ref(false);
/** 正在取消的 run id：只禁用对应行的取消按钮 */
const cancellingId = ref<string | null>(null);
let timer: ReturnType<typeof setInterval> | null = null;
/** 轮询在途守卫：断线重连等慢响应场景下请求不叠加 */
let refreshing = false;

const workflowNames = computed(() => new Map(editor.workflows.map((w) => [w.workflow_id, w.name])));

const STATUS_OPTIONS: { value: string; label: string }[] = [
  { value: "", label: "全部状态" },
  { value: "running", label: "运行中" },
  { value: "succeeded", label: "成功" },
  { value: "failed", label: "失败" },
  { value: "cancelled", label: "已取消" },
  { value: "awaiting_resume", label: "挂起待恢复" },
];

const SOURCE_OPTIONS: { value: string; label: string }[] = [
  { value: "", label: "全部来源" },
  { value: "manual", label: "手动" },
  { value: "schedule", label: "定时调度" },
  { value: "webhook", label: "Webhook" },
  { value: "sub_workflow", label: "子流程" },
];

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

/** 轮询只刷首页（最新的 PAGE_SIZE 条），已翻出的旧页保持不动。
 * 多取一条探测是否还有更旧数据，避免「总数恰好整除页大小」时 hasMore 假阳性 */
async function refresh(): Promise<void> {
  if (refreshing) return;
  refreshing = true;
  try {
    const probe = await api.listRuns({
      workflowId: props.workflowId,
      status: statusFilter.value || undefined,
      source: sourceFilter.value || undefined,
      limit: PAGE_SIZE + 1,
    });
    hasMore.value = probe.length > PAGE_SIZE;
    runs.value = mergeRunPage(probe.slice(0, PAGE_SIZE), runs.value);
    pollFailed.value = false;
  } catch (e) {
    if (!pollFailed.value) toast.error(errText(e));
    pollFailed.value = true;
  } finally {
    refreshing = false;
    loaded.value = true;
  }
}

async function loadMore(): Promise<void> {
  const cursor = nextCursor(runs.value);
  if (!cursor || loadingMore.value) return;
  loadingMore.value = true;
  try {
    const probe = await api.listRuns({
      workflowId: props.workflowId,
      status: statusFilter.value || undefined,
      source: sourceFilter.value || undefined,
      beforeRunId: cursor,
      limit: PAGE_SIZE + 1,
    });
    hasMore.value = probe.length > PAGE_SIZE;
    runs.value = mergeRunPage(runs.value, probe.slice(0, PAGE_SIZE));
  } catch (e) {
    toast.error(errText(e));
  } finally {
    loadingMore.value = false;
  }
}

async function onCancel(r: RunRecord): Promise<void> {
  if (cancellingId.value) return;
  if (!(await confirmDialog(`确定取消 run ${r.id.slice(0, 8)}…？`, { danger: true }))) return;
  cancellingId.value = r.id;
  try {
    await api.runCancel(r.id);
    await refresh();
  } catch (e) {
    toast.error(errText(e));
  } finally {
    cancellingId.value = null;
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

// 过滤条件变化：丢弃已翻页数据，回到第一页
watch([statusFilter, sourceFilter], () => {
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
        <select v-model="sourceFilter" class="status-filter" aria-label="来源筛选">
          <option v-for="o in SOURCE_OPTIONS" :key="o.value" :value="o.value">
            {{ o.label }}
          </option>
        </select>
        <select v-model="statusFilter" class="status-filter" aria-label="状态筛选">
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
          <th>来源</th>
          <th>开始时间</th>
          <th>耗时</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="r in runs" :key="r.id">
          <td class="mono copyable" :title="r.id" @click="copyRunId(r.id)">
            {{ r.id.slice(0, 8) }}…
          </td>
          <td v-if="!workflowId" class="wf-cell">
            {{ workflowNames.get(r.workflow_id) ?? r.workflow_id }}
            <span class="muted">v{{ r.workflow_version }}</span>
          </td>
          <td>
            <span class="badge" :class="badgeClass(r.status)">
              {{ runStatusLabel(r.status) }}
            </span>
          </td>
          <td>{{ sourceLabel(r.source) }}</td>
          <td :title="r.started_at">{{ fmtTime(r.started_at) }}</td>
          <td>{{ fmtDuration(r) }}</td>
          <td class="actions">
            <RouterLink class="link" :to="`/runs/${r.id}`">详情</RouterLink>
            <button
              v-if="r.status === 'running'"
              class="link danger"
              :disabled="cancellingId === r.id"
              @click="onCancel(r)"
            >
              取消
            </button>
          </td>
        </tr>
      </tbody>
    </table>
    <p v-if="!loaded" class="wf-empty">加载中…</p>
    <p v-else-if="runs.length === 0" class="wf-empty">暂无运行记录</p>
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

.copyable {
  cursor: pointer;
}

/* 名称缺失时回退渲染 36 字符 workflow_id，允许折行避免撑破表格 */
.wf-cell {
  word-break: break-all;
}
</style>
