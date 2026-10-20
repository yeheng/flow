<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import * as api from "../api/flow";
import { errText } from "../rpc/client";
import { isResponseTooLarge } from "../rpc/errors";
import { editor, refreshWorkflows } from "../state/editor";
import { confirmDialog } from "../state/modal";
import {
  mergeRunPage,
  nextCursor,
  parseRunFilters,
  RUN_SOURCE_OPTIONS,
  RUN_STATUS_OPTIONS,
  serializeRunFilters,
} from "../state/run-list";
import { runBadgeClass, runStatusLabel, sourceLabel, TERMINAL_RUN_STATUSES } from "../state/labels";
import { regressionBaseline } from "../state/regression";
import { fmtTime, runDuration } from "../state/time";
import { staleGuard } from "../state/stale";
import { usePolling } from "../state/polling";
import { toast } from "../state/toast";
import { copyWithToast } from "../state/clipboard";
import type { RunRecord } from "../types";
import AppButton from "../components/AppButton.vue";

/** 全局运行历史（/runs）与单工作流运行历史（/workflows/:id/runs）共用：后者传 workflowId 过滤 */
const props = defineProps<{ workflowId?: string }>();

const route = useRoute();
const router = useRouter();

const PAGE_SIZE = 50;
const runs = ref<RunRecord[]>([]);
/** 筛选随路由 query 持久化：后退/刷新/分享链接都能还原 */
const initialFilters = parseRunFilters(route.query);
const statusFilter = ref(initialFilters.status);
const sourceFilter = ref(initialFilters.source);
const hasMore = ref(false);
const loadingMore = ref(false);
/** 首次拉取完成后置 true；过滤条件变化时保持 true，表区直接展示新结果而不闪空态 */
const loaded = ref(false);
/** 轮询失败只在 ok→error 跳变时 toast，避免每 3s 重复弹错 */
const pollFailed = ref(false);
/** 最近一次刷新的错误：表区无数据时升级为横幅（否则"暂无运行记录"是假空态） */
const loadError = ref<string | null>(null);
/** 正在取消的 run id：只禁用对应行的取消按钮 */
const cancellingId = ref<string | null>(null);
/**
 * 降级列表：物化列表（run.list.view）超 8MB 预算时走原始快照扫描
 * （无过滤/降序服务端不支持，只保留手动刷新）。
 */
const degraded = ref(false);
/** 降级扫描被上限截断：更旧数据未知 */
const degradedScanTruncated = ref(false);
/** 轮询在途守卫：断线重连等慢响应场景下请求不叠加 */
let refreshing = false;

const workflowNames = computed(() => new Map(editor.workflows.map((w) => [w.workflow_id, w.name])));

/** 状态/来源筛选词汇与 URL 往返：单一出处在 state/run-list */
const STATUS_OPTIONS = RUN_STATUS_OPTIONS;
const SOURCE_OPTIONS = RUN_SOURCE_OPTIONS;


/** 请求代际：筛选/工作流变化时作废在飞响应（staleGuard，全站同一份原语） */
const listStale = staleGuard();
/** 请求在途时又收到刷新：等它结束后按最新条件补拉一次，避免丢刷新 */
let pendingRefresh = false;

function currentQuery(): {
  workflowId?: string;
  status?: string;
  source?: string;
} {
  return {
    workflowId: props.workflowId,
    status: statusFilter.value || undefined,
    source: sourceFilter.value || undefined,
  };
}

/** 筛选/工作流变化：丢弃在途响应与已翻页数据，从第一页重拉 */
function resetAndRefresh(): void {
  listStale.invalidate();
  runs.value = [];
  hasMore.value = false;
  loaded.value = false;
  void refresh();
}

/** 页面激活期间每 3 秒轮询首页（页面不可见自动暂停）；降级扫描时 stop 切手动刷新 */
const { stop: stopPolling } = usePolling(refresh, 3000);

/** 轮询只刷首页（最新的 PAGE_SIZE 条），已翻出的旧页保持不动。
 * 多取一条探测是否还有更旧数据，避免「总数恰好整除页大小」时 hasMore 假阳性 */
async function refresh(): Promise<void> {
  if (refreshing) {
    pendingRefresh = true;
    return;
  }
  const gen = listStale.begin();
  refreshing = true;
  let switched = false;
  try {
    let page: RunRecord[];
    if (degraded.value) {
      const lite = await api.listRunsLite({ ...currentQuery(), limit: PAGE_SIZE });
      if (listStale.stale(gen)) return;
      page = lite.runs;
      hasMore.value = lite.hasMore;
      degradedScanTruncated.value = !lite.scanned;
    } else {
      const probe = await api.listRuns({ ...currentQuery(), limit: PAGE_SIZE + 1 });
      if (listStale.stale(gen)) return;
      page = probe.slice(0, PAGE_SIZE);
      hasMore.value = probe.length > PAGE_SIZE;
    }
    runs.value = mergeRunPage(page, runs.value);
    pollFailed.value = false;
    loadError.value = null;
  } catch (e) {
    if (listStale.stale(gen)) return;
    if (isResponseTooLarge(e)) {
      // 物化列表整页超预算（列表里的大值运行）：切原始快照降级路径重拉
      degraded.value = true;
      switched = true;
      pendingRefresh = true;
      stopPolling();
    } else {
      loadError.value = errText(e);
      if (!pollFailed.value) toast.error(errText(e));
      pollFailed.value = true;
    }
  } finally {
    refreshing = false;
    if (!listStale.stale(gen) && !switched) loaded.value = true;
    if (pendingRefresh) {
      pendingRefresh = false;
      void refresh();
    }
  }
}

async function loadMore(): Promise<void> {
  const cursor = nextCursor(runs.value);
  if (!cursor || loadingMore.value) return;
  const gen = listStale.mark();
  loadingMore.value = true;
  try {
    if (degraded.value) {
      const lite = await api.listRunsLite({ ...currentQuery(), beforeRunId: cursor, limit: PAGE_SIZE });
      if (listStale.stale(gen)) return;
      hasMore.value = lite.hasMore;
      runs.value = mergeRunPage(runs.value, lite.runs);
      degradedScanTruncated.value = !lite.scanned;
    } else {
      const probe = await api.listRuns({
        ...currentQuery(),
        beforeRunId: cursor,
        limit: PAGE_SIZE + 1,
      });
      if (listStale.stale(gen)) return;
      hasMore.value = probe.length > PAGE_SIZE;
      runs.value = mergeRunPage(runs.value, probe.slice(0, PAGE_SIZE));
    }
  } catch (e) {
    if (!listStale.stale(gen)) toast.error(errText(e));
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
  await copyWithToast(id, "run id");
}

/** 筛选变化 → 同步路由 query（空值不入 URL），再回到第一页 */
function onFiltersChanged(): void {
  const query = serializeRunFilters({ status: statusFilter.value, source: sourceFilter.value });
  const current = route.query;
  if ((current.status ?? "") !== (query.status ?? "") || (current.source ?? "") !== (query.source ?? "")) {
    void router.replace({ query });
  }
  resetAndRefresh();
}

watch([statusFilter, sourceFilter], onFiltersChanged);

// 后退/前进改路由 query：同步回筛选（值未变不触发，避免与 onFiltersChanged 打乒乓）
watch(
  () => [route.query.status, route.query.source],
  ([status, source]) => {
    const next = parseRunFilters({ status, source });
    if (next.status !== statusFilter.value || next.source !== sourceFilter.value) {
      statusFilter.value = next.status;
      sourceFilter.value = next.source;
    }
  },
);

onMounted(async () => {
  if (editor.workflows.length === 0) await refreshWorkflows();
  await refresh();
  // 降级模式是全量扫描，只保留手动刷新（轮询由 usePolling 统一管理）
  if (degraded.value) stopPolling();
});

watch(() => props.workflowId, resetAndRefresh);
</script>

<template>
  <div class="page">
    <div class="page-header">
      <h2>
        运行记录
        <span v-if="workflowId" class="muted">{{ workflowNames.get(workflowId) ?? workflowId }}</span>
      </h2>
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
        <span v-if="workflowId" class="wf-links">
          <RouterLink class="link" :to="`/workflows/${workflowId}`">编辑器</RouterLink>
          <RouterLink class="link" :to="`/workflows/${workflowId}/versions`">版本</RouterLink>
          <RouterLink class="link" :to="`/workflows/${workflowId}/triggers`">触发器</RouterLink>
        </span>
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
          <td>
            <button type="button" class="copy-id" :title="r.id" @click="copyRunId(r.id)">
              {{ r.id.slice(0, 8) }}…
            </button>
          </td>
          <td v-if="!workflowId" class="wf-cell">
            {{ workflowNames.get(r.workflow_id) ?? r.workflow_id }}
            <span class="muted">v{{ r.workflow_version }}</span>
          </td>
          <td>
            <span class="badge" :class="runBadgeClass(r.status)">
              {{ runStatusLabel(r.status) }}
            </span>
          </td>
          <td>
            {{ sourceLabel(r.source) }}
            <RouterLink
              v-if="regressionBaseline(r.source_detail)"
              class="link run-regression-tag"
              :to="`/runs/${regressionBaseline(r.source_detail)}`"
              title="回归自该基线 run"
            >
              回归
            </RouterLink>
          </td>
          <td :title="r.started_at">{{ fmtTime(r.started_at) }}</td>
          <td>{{ runDuration(r.started_at, r.ended_at) }}</td>
          <td class="actions">
            <RouterLink class="link" :to="`/runs/${r.id}`">详情</RouterLink>
            <!-- 非终态即可取消（含挂起等续跑的 awaiting_resume；服务端对终态 no-op） -->
            <AppButton
              v-if="!TERMINAL_RUN_STATUSES.has(r.status)"
              variant="link-danger"
              :disabled="cancellingId === r.id"
              @click="onCancel(r)"
            >
              取消
            </AppButton>
          </td>
        </tr>
      </tbody>
    </table>
    <p v-if="degraded" class="degraded-hint">
      列表含超大值，已降级为原始快照扫描（手动刷新；单次最多扫描 8192 条）。
      <AppButton variant="link" @click="resetAndRefresh">刷新</AppButton>
      <template v-if="degradedScanTruncated"> · 扫描被截断，更旧数据未知</template>
    </p>
    <p v-if="!loaded" class="wf-empty">加载中…</p>
    <div v-else-if="loadError && runs.length === 0" role="alert" class="error-banner">
      <span>加载运行记录失败：{{ loadError }}</span>
      <AppButton variant="link" @click="resetAndRefresh">重试</AppButton>
    </div>
    <p v-else-if="runs.length === 0" class="wf-empty">暂无运行记录</p>
    <div v-if="hasMore" class="load-more">
      <AppButton :disabled="loadingMore" @click="loadMore">加载更多</AppButton>
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

/* 工作流子页互跳（编辑器/版本/触发器）：紧凑一组 */
.wf-links {
  display: flex;
  align-items: center;
  gap: 10px;
}

.run-regression-tag {
  margin-left: 6px;
  font-size: 11px;
}

.degraded-hint {
  margin: 8px 0 0;
  padding: 6px 10px;
  border: 1px solid var(--warn);
  border-radius: 6px;
  color: var(--warn);
  font-size: 12px;
}

/* 名称缺失时回退渲染 36 字符 workflow_id，允许折行避免撑破表格 */
.wf-cell {
  word-break: break-all;
}
</style>
