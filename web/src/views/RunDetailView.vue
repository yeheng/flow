<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from "vue";
import { useRouter } from "vue-router";
import * as api from "../api/flow";
import { errText } from "../rpc/client";
import { editor, ensureNodeTypes, refreshWorkflows } from "../state/editor";
import { attachRun, detachRun, monitor } from "../state/monitor";
import { devMode } from "../state/dev";
import { runBadgeClass, runStatusLabel, sourceLabel, TERMINAL_RUN_STATUSES } from "../state/labels";
import { fmtTime, runDuration } from "../state/time";
import { regressionBaseline } from "../state/regression";
import { openOverlay, overlaysOf } from "../state/overlays";
import { staleGuard } from "../state/stale";
import { describeStoredValue, rawRunToRecord, type ValueMarker } from "../state/run-raw";
import { toast } from "../state/toast";
import { copyWithToast } from "../state/clipboard";
import { confirmDialog } from "../state/modal";
import type { Definition, RunEvent, RunRecord } from "../types";
import RunCanvas from "../components/RunCanvas.vue";
import RunMonitor from "../components/RunMonitor.vue";
import LogConsole from "../components/LogConsole.vue";
import NodeInspector from "../components/NodeInspector.vue";
import ResizeHandle from "../components/ResizeHandle.vue";
import AppButton from "../components/AppButton.vue";
import RegressionDialog from "../components/RegressionDialog.vue";
import RunComparePanel from "../components/RunComparePanel.vue";
import { usePanelResize } from "../state/panel-resize";

const props = defineProps<{ runId: string }>();

const router = useRouter();

const record = ref<RunRecord | null>(null);
const live = ref(false);
const definition = ref<Definition | null>(null);
/** 定义拉取中：区分「加载中」与「加载完成但没有定义」，避免首屏闪「不可用」 */
const loadingDefinition = ref(false);
const showEvents = ref(false);
const events = ref<RunEvent[] | null>(null);
const eventsCursor = ref<Record<string, unknown> | null>(null);
const eventsLoading = ref(false);
const activeTab = ref<"timeline" | "logs">("timeline");
// 关掉开发者模式时回到默认运行视图（日志页签属于调试面）
watch(devMode, (on) => {
  if (!on) activeTab.value = "timeline";
});
/** 快速切换 run（子 run 链接）时旧加载的响应一律丢弃 */
const loadStale = staleGuard();

/** 回归：以当前 run 为基线发起候选；候选 run 反过来给出基线与对比入口。
 * 弹层状态归全局弹层栈（kind="compare"/"regression"），本视图只渲染条目。 */
const compareEntries = computed(() =>
  overlaysOf<{ baseline: string | null; candidate: string }>("compare"),
);
const regressionEntries = computed(() => overlaysOf<{ baseline: RunRecord }>("regression"));

/** 视图自有弹层：切 run/卸载时关闭，防路由切换残留 */
function closeRunOverlays(): void {
  for (const entry of [...compareEntries.value, ...regressionEntries.value]) entry.close();
}

/**
 * 大值降级：物化视图（run.get.view）超 8MB 预算整体失败时走原始信封渲染，
 * 输入/输出给下载入口，时间线/日志不可用（timeline 同样物化大值会失败）。
 */
const liteMode = ref(false);
const inputValue = computed(() => describeStoredValue(record.value?.input));
const outputValue = computed(() => describeStoredValue(record.value?.output));
const downloading = ref("");

async function downloadRef(marker: ValueMarker): Promise<void> {
  if (!record.value || !marker.outputId || downloading.value) return;
  downloading.value = marker.outputId;
  try {
    await api.downloadValue(record.value.id, marker.outputId);
    toast.info("已开始下载完整值");
  } catch (e) {
    toast.error(errText(e));
  } finally {
    downloading.value = "";
  }
}

/** 以本 run 为基线的回归候选（扫描本工作流最近 500 条，按 source_detail 归属） */
const candidates = ref<RunRecord[] | null>(null);
const candidatesLoading = ref(false);
let candidatesLoaded = false;

async function loadCandidates(): Promise<void> {
  const current = record.value;
  if (!current || candidatesLoaded || candidatesLoading.value) return;
  candidatesLoading.value = true;
  candidatesLoaded = true;
  try {
    const runs = await api.listRuns({ workflowId: current.workflow_id, limit: 500 });
    candidates.value = runs.filter((r) => regressionBaseline(r.source_detail) === current.id);
  } catch (e) {
    candidatesLoaded = false;
    toast.error(errText(e));
  } finally {
    candidatesLoading.value = false;
  }
}

function onCandidatesToggle(e: Event): void {
  if ((e.target as HTMLDetailsElement).open) void loadCandidates();
}

/** 实时状态：monitor 正挂在这个 run 上时以订阅投影为准（run.get 快照不会自己刷新） */
const currentStatus = computed(() => {
  const r = record.value;
  if (!r) return "";
  return monitor.runId === r.id ? (monitor.status ?? r.status) : r.status;
});
const isTerminalRun = computed(() => TERMINAL_RUN_STATUSES.has(currentStatus.value));
/** 本 run 若由回归创建，source_detail = regression:<baseline> */
const regressionBase = computed(() => regressionBaseline(record.value?.source_detail));

function openCompare(prefill?: string | null, candidate?: string): void {
  openOverlay("compare", {
    baseline: prefill ?? regressionBase.value ?? null,
    candidate: candidate ?? record.value?.id ?? "",
  });
}

function openRegression(): void {
  if (record.value) openOverlay("regression", { baseline: record.value });
}

/** 取消当前 run：非终态均可（含挂起等续跑的 awaiting_resume）；状态由 monitor 投影/补拉刷新 */
const cancelling = ref(false);
async function onCancelRun(): Promise<void> {
  const current = record.value;
  if (!current || cancelling.value) return;
  if (!(await confirmDialog(`确定取消 run ${current.id.slice(0, 8)}…？`, { danger: true }))) return;
  cancelling.value = true;
  try {
    await api.runCancel(current.id);
    const fresh = await api.getRun(current.id);
    if (record.value?.id === current.id) {
      record.value = fresh.run;
      live.value = fresh.live;
    }
  } catch (e) {
    toast.error(errText(e));
  } finally {
    cancelling.value = false;
  }
}

function onRegressionCreated(runId: string): void {
  void router.push(`/runs/${runId}`);
}

// 回归候选终结后不自动弹遮罩：只亮「与基线对比」入口（UI 不做用户没要求的动作）
const compareReady = computed(() => !!regressionBase.value && isTerminalRun.value);

// 运行中打开的 run：run.get 快照不会自己刷新。monitor 投影进入终态后补拉一次
// record，让输出/错误/「实时」徽章与右侧时间线一致。journal 不记录终态墙钟，
// ended_at 仍可能为空——这是数据面限制，不用客户端时间编造。
watch(currentStatus, async (status) => {
  const current = record.value;
  if (!current || !status || status === current.status) return;
  const gen = loadStale.mark();
  try {
    const fresh = await api.getRun(current.id);
    if (loadStale.stale(gen) || record.value?.id !== current.id) return;
    record.value = fresh.run;
    live.value = fresh.live;
  } catch {
    // 订阅已给出终态，补拉失败不打断页面；下次进入会重新加载
  }
});

/** 节点检查器：画布/时间线/日志点击节点时打开 */
const inspectorNodeId = ref<string | null>(null);

// 右侧栏：拖左边缘改宽度
const asideEl = ref<HTMLElement | null>(null);
const asideResize = usePanelResize({
  target: asideEl,
  storageKey: "flow.rundetail-w",
  fallback: 380,
  min: 280,
  max: () => (asideEl.value?.parentElement?.clientWidth ?? 1200) * 0.7,
  measure: (rect, e) => rect.right - e.clientX,
});

// 底部面板：拖上边缘改高度（画布在上、面板在下，上下可调）
const bottomEl = ref<HTMLElement | null>(null);
const bottomResize = usePanelResize({
  target: bottomEl,
  storageKey: "flow.rundetail-h",
  fallback: 280,
  min: 160,
  max: () => (bottomEl.value?.parentElement?.clientHeight ?? 800) * 0.8,
  measure: (rect, e) => rect.bottom - e.clientY,
});

watch(
  () => editor.selectedNodeId,
  (id) => {
    if (id && monitor.nodes.some((n) => n.id === id)) inspectorNodeId.value = id;
  },
);

watch(
  () => monitor.runId,
  () => {
    inspectorNodeId.value = null;
  },
);

function closeInspector(): void {
  inspectorNodeId.value = null;
  editor.selectedNodeId = null;
}

function onSelectNode(nodeId: string): void {
  inspectorNodeId.value = nodeId;
  editor.selectedNodeId = nodeId;
}

function onOpenChild(childRunId: string): void {
  void router.push(`/runs/${childRunId}`);
}

const workflowName = computed(() => {
  const wf = record.value?.workflow_id;
  return editor.workflows.find((w) => w.workflow_id === wf)?.name ?? wf ?? "";
});


function fmtJson(v: unknown): string {
  if (v === undefined || v === null) return "";
  return JSON.stringify(v, null, 2);
}

async function copyRunId(): Promise<void> {
  if (record.value) await copyWithToast(record.value.id, "run id");
}

async function copyOutput(): Promise<void> {
  if (record.value)
    await copyWithToast(fmtJson(liteMode.value ? outputValue.value : record.value.output), "输出");
}

/** 原始事件按游标分页，长 run 不全量拉取 */
async function loadEvents(cursor: Record<string, unknown> | null): Promise<void> {
  if (eventsLoading.value) return;
  const gen = loadStale.mark();
  eventsLoading.value = true;
  try {
    const page = await api.runEventsPage(props.runId, cursor);
    if (loadStale.stale(gen)) return;
    events.value = [...(events.value ?? []), ...page.events];
    eventsCursor.value = page.nextCursor;
  } catch (e) {
    if (!loadStale.stale(gen)) toast.error(errText(e));
  } finally {
    if (!loadStale.stale(gen)) eventsLoading.value = false;
  }
}

async function toggleEvents(): Promise<void> {
  showEvents.value = !showEvents.value;
  if (showEvents.value && events.value === null) await loadEvents(null);
}

async function load(runId: string): Promise<void> {
  const gen = loadStale.begin();
  record.value = null;
  definition.value = null;
  loadingDefinition.value = true;
  events.value = null;
  eventsCursor.value = null;
  eventsLoading.value = false;
  showEvents.value = false;
  closeRunOverlays();
  liteMode.value = false;
  candidates.value = null;
  candidatesLoaded = false;
  try {
    const r = await api.getRun(runId);
    if (loadStale.stale(gen)) return;
    record.value = r.run;
    live.value = r.live;
  } catch {
    if (loadStale.stale(gen)) return;
    // 物化读面失败（超大值 / 服务端错误）：不猜错误码，直接试原始信封读面
    try {
      const raw = await api.getRunRaw(runId);
      if (loadStale.stale(gen)) return;
      record.value = rawRunToRecord(raw);
      live.value = false;
      liteMode.value = true;
    } catch (e2) {
      loadingDefinition.value = false;
      toast.error(`加载 run 失败：${errText(e2)}`);
      void router.replace("/runs");
      return;
    }
  }
  // RunRecord 含 workflow_id + workflow_version：拉取 run 钉死的那版定义渲染只读 DAG
  if (await ensureNodeTypes()) {
    try {
      const w = await api.getWorkflow(record.value.workflow_id, record.value.workflow_version);
      if (!loadStale.stale(gen)) definition.value = w.definition;
    } catch (e) {
      if (!loadStale.stale(gen)) toast.error(`加载工作流定义失败：${errText(e)}`);
    }
  }
  if (!loadStale.stale(gen)) loadingDefinition.value = false;
  if (liteMode.value) {
    // 降级页不挂载监控：先清掉上一个 run 的投影，否则右侧会显示别的 run 的数据
    await detachRun();
    return;
  }
  try {
    await attachRun(runId);
  } catch (e) {
    if (!loadStale.stale(gen)) toast.error(errText(e));
  }
}

onMounted(async () => {
  if (editor.workflows.length === 0) void refreshWorkflows();
  await load(props.runId);
});

watch(
  () => props.runId,
  (id, prev) => {
    if (id && id !== prev) void load(id);
  },
);

// 离开时 detach：仅当 monitor 仍挂在当前 run 上，避免误杀编辑器/其他页新建的 attach；
// 自有弹层一并关闭，防路由切换残留
onUnmounted(() => {
  closeRunOverlays();
  if (monitor.runId === props.runId) void detachRun();
});
</script>

<template>
  <div class="run-detail">
    <div v-if="record" class="run-header">
      <div class="run-header-row">
        <RouterLink class="link run-back" to="/runs">← 运行记录</RouterLink>
        <span class="badge" :class="runBadgeClass(currentStatus)">
          {{ runStatusLabel(currentStatus) }}
        </span>
        <span class="mono run-full-id" :title="record.id">{{ record.id }}</span>
        <button class="run-copy" title="复制 run id" @click="copyRunId">复制</button>
        <span v-if="live" class="badge run-running">实时</span>
        <span class="run-header-links">
          <RouterLink class="link" :to="`/workflows/${record.workflow_id}`">
            {{ workflowName }}
          </RouterLink>
          <span class="muted">v{{ record.workflow_version }}</span>
          <RouterLink class="link" :to="`/workflows/${record.workflow_id}/runs`">
            该工作流的运行记录
          </RouterLink>
          <AppButton
            v-if="!isTerminalRun"
            variant="link-danger"
            :disabled="cancelling"
            @click="onCancelRun()"
          >
            取消
          </AppButton>
          <AppButton
            v-if="isTerminalRun"
            variant="link"
            title="以本运行为基线，用指定版本重新执行（复用已记录的外部结果，不产生副作用）"
            @click="openRegression()"
          >
            回归
          </AppButton>
          <AppButton
            variant="link"
            title="输入任意基线 run id，比较状态、输出与节点结果"
            @click="openCompare()"
          >
            比较运行
          </AppButton>
        </span>
      </div>
      <div class="run-header-row run-header-meta">
        <span>开始 {{ fmtTime(record.started_at) }}</span>
        <span>结束 {{ fmtTime(record.ended_at) }}</span>
        <span>耗时 {{ runDuration(record.started_at, record.ended_at) }}</span>
        <span>
          来源 {{ sourceLabel(record.source) }}
          <RouterLink
            v-if="record.source === 'schedule'"
            class="link"
            :to="`/workflows/${record.workflow_id}/triggers`"
          >
            （触发器）
          </RouterLink>
        </span>
        <span v-if="regressionBase" class="run-regression-source">
          回归自基线
          <RouterLink class="link" :to="`/runs/${regressionBase}`" :title="regressionBase">
            {{ regressionBase.slice(0, 8) }}…
          </RouterLink>
          <!-- 候选终结后亮对比入口（不自动弹遮罩） -->
          <AppButton
            v-if="compareReady"
            variant="link"
            title="比较本候选与基线的状态、输出与节点结果"
            @click="openCompare()"
          >
            与基线比较
          </AppButton>
        </span>
      </div>
    </div>

    <section class="center">
      <RunCanvas
        v-if="definition"
        :definition="definition"
        @node-click="onSelectNode"
        @open-child="onOpenChild"
      />
      <div v-else-if="loadingDefinition" class="canvas-hint">加载中…</div>
      <div v-else class="canvas-hint">工作流定义不可用，仅展示时间线</div>
    </section>

    <div ref="bottomEl" class="bottom" :style="{ height: bottomResize.size.value + 'px' }">
      <ResizeHandle
        orientation="horizontal"
        class="edge-top"
        @dragstart="bottomResize.onDragStart"
      />
      <div class="io-panel">
        <template v-if="record">
          <p v-if="liteMode" class="run-hint run-lite-hint">
            该运行含超大值，已降级展示：时间线/日志不可用，输入/输出可下载完整值。
          </p>
          <div class="io-block">
            <div class="io-label muted">输入</div>
            <div v-if="liteMode && inputValue.kind === 'ref'" class="io-ref">
              大值 {{ inputValue.totalBytes ?? "?" }} 字节
              <AppButton
                variant="link"
                :disabled="downloading === inputValue.outputId"
                @click="downloadRef(inputValue)"
              >
                {{ downloading === inputValue.outputId ? "下载中…" : "下载完整值" }}
              </AppButton>
            </div>
            <code v-else class="run-io">{{ fmtJson(liteMode ? inputValue.value : record.input) || "（空）" }}</code>
          </div>
          <div v-if="record.error" class="io-block">
            <div class="io-label muted">错误</div>
            <code class="run-io run-io-error">{{ record.error }}</code>
          </div>
          <div v-else-if="record.output !== null && record.output !== undefined" class="io-block">
            <div class="io-label muted">
              输出
              <button
                v-if="!(liteMode && outputValue.kind === 'ref')"
                class="run-copy"
                title="复制输出 JSON"
                @click="copyOutput"
              >
                复制
              </button>
            </div>
            <div v-if="liteMode && outputValue.kind === 'ref'" class="io-ref">
              大值 {{ outputValue.totalBytes ?? "?" }} 字节
              <AppButton
                variant="link"
                :disabled="downloading === outputValue.outputId"
                @click="downloadRef(outputValue)"
              >
                {{ downloading === outputValue.outputId ? "下载中…" : "下载完整值" }}
              </AppButton>
            </div>
            <code v-else class="run-io">{{ fmtJson(liteMode ? outputValue.value : record.output) }}</code>
          </div>
          <details class="run-candidates" @toggle="onCandidatesToggle">
            <summary>回归候选（以本 run 为基线，点开加载）</summary>
            <p v-if="candidatesLoading" class="run-hint">加载中…</p>
            <template v-else-if="candidates && candidates.length">
              <div v-for="c in candidates" :key="c.id" class="candidate-row">
                <RouterLink class="link mono" :to="`/runs/${c.id}`" :title="c.id">
                  {{ c.id.slice(0, 8) }}…
                </RouterLink>
                <span class="badge" :class="runBadgeClass(c.status)">
                  {{ runStatusLabel(c.status) }}
                </span>
                <AppButton variant="link" @click="openCompare(record.id, c.id)">比较</AppButton>
              </div>
            </template>
            <p v-else-if="candidates" class="run-hint">
              暂无回归候选（按来源标记扫描本工作流最近 500 条）
            </p>
          </details>
        </template>
        <p v-else class="run-hint">加载中…</p>
      </div>
      <aside
        ref="asideEl"
        class="right"
        :style="{ width: asideResize.size.value + 'px', maxWidth: '45vw' }"
      >
        <ResizeHandle
          orientation="vertical"
          class="edge-left"
          @dragstart="asideResize.onDragStart"
        />
        <template v-if="liteMode">
          <p class="run-hint run-lite-hint">
            该运行含超大值：时间线/日志不可用。完整输入/输出可在左侧下载。
          </p>
        </template>
        <template v-else>
          <div v-if="devMode" class="aside-tabs">
            <button
              class="aside-tab"
              :class="{ active: activeTab === 'timeline' }"
              @click="activeTab = 'timeline'"
            >
              运行
            </button>
            <button
              class="aside-tab"
              :class="{ active: activeTab === 'logs' }"
              @click="activeTab = 'logs'"
            >
              日志（{{ monitor.logs.length }}）
            </button>
          </div>

          <NodeInspector
            v-if="inspectorNodeId"
            class="aside-inspector"
            :node-id="inspectorNodeId"
            @close="closeInspector"
            @open-child="onOpenChild"
          />

          <!-- 时间线超物化预算：停止重试并回落到日志面，不让面板永远空白 -->
          <template v-if="monitor.timelineUnavailable">
            <p class="run-hint">时间线不可用（数据量大或读取失败）；日志与原始事件仍可查看。</p>
            <LogConsole class="aside-log-console" @select-node="onSelectNode" />
          </template>
          <template v-else-if="activeTab === 'logs'">
            <LogConsole class="aside-log-console" @select-node="onSelectNode" />
          </template>
          <template v-else>
            <RunMonitor v-if="monitor.runId" child-run-navigate :run-input="record?.input" />
            <p v-else class="run-hint">正在加载…</p>
          </template>

          <!-- 原始事件属于调试面：只在开发者模式显示 -->
          <template v-if="devMode">
            <div class="events-toggle">
              <AppButton type="button" variant="link" @click="toggleEvents">
                {{ showEvents ? "收起原始事件" : "展开原始事件" }}
              </AppButton>
            </div>
            <div v-if="showEvents" class="events-list">
              <p v-if="events === null" class="run-hint">加载中…</p>
              <template v-else>
                <div v-for="e in events" :key="`${e.lsn}:${e.event_index}`" class="events-item mono">
                  <span class="muted">#{{ e.event.run_seq }}</span> {{ e.event.kind }}
                  <span v-if="e.event.node_id" class="muted">{{ e.event.node_id }}</span>
                </div>
                <p v-if="events.length === 0" class="run-hint">无事件</p>
                <AppButton
                  v-if="eventsCursor"
                  type="button"
                  variant="link"
                  :disabled="eventsLoading"
                  @click="loadEvents(eventsCursor)"
                >
                  {{ eventsLoading ? "加载中…" : "加载更多事件" }}
                </AppButton>
              </template>
            </div>
          </template>
        </template>
      </aside>
    </div>

    <!-- 弹层栈条目渲染：Esc 由全局 host 关栈顶，逐层关闭 -->
    <RegressionDialog
      v-for="entry in regressionEntries"
      :key="entry.id"
      :entry="entry"
      @created="onRegressionCreated"
    />
    <RunComparePanel v-for="entry in compareEntries" :key="entry.id" :entry="entry" />
  </div>
</template>

<style scoped>
.run-detail {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-height: 0;
}

.run-header {
  padding: 12px 16px;
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  gap: 8px;
  flex-shrink: 0;
}

.run-header-row {
  display: flex;
  align-items: baseline;
  gap: 10px;
  flex-wrap: wrap;
}

.run-back {
  white-space: nowrap;
}

.run-header-meta {
  color: var(--text2);
  font-size: 12px;
}

.run-regression-source {
  color: var(--accent2);
}

.run-header-links {
  margin-left: auto;
  display: flex;
  gap: 10px;
  align-items: baseline;
  flex-wrap: wrap;
  min-width: 0;
}

.run-full-id {
  color: var(--text2);
  font-size: 12px;
}

.run-copy {
  padding: 1px 8px;
  font-size: 11px;
}

.run-io {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--text2);
  white-space: pre-wrap;
  word-break: break-all;
  max-height: 120px;
  overflow-y: auto;
}

.run-io-error {
  color: var(--danger);
}

.run-lite-hint {
  color: var(--warn);
}

.io-ref {
  display: flex;
  align-items: center;
  gap: 8px;
  font-family: var(--mono);
  font-size: 11px;
  color: var(--text2);
}

.run-candidates {
  margin-top: 12px;
  font-size: 12px;
}

.run-candidates summary {
  cursor: pointer;
  color: var(--text2);
}

.candidate-row {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 3px 0;
}

.center {
  flex: 1;
  min-height: 0;
  position: relative;
}

/* 底部面板：左输入/输出，右运行/日志；上边缘拖高度，中间缝拖宽度 */
.bottom {
  position: relative;
  flex-shrink: 0;
  display: flex;
  min-height: 0;
  border-top: 1px solid var(--border);
}

.edge-top {
  top: 0;
}

.io-panel {
  flex: 1;
  min-width: 0;
  overflow-y: auto;
  padding: 10px 12px;
}

.io-block {
  margin-bottom: 10px;
}

.io-label {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  margin-bottom: 2px;
}

.right {
  position: relative;
  flex-shrink: 0;
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow-y: auto;
  padding: 0 12px 12px;
  border-left: 1px solid var(--border);
}

/* 面板有 overflow-y: auto，手柄放内侧避免挤出横向滚动条 */
.edge-left {
  left: 0;
}

.aside-tabs {
  display: flex;
  gap: 2px;
  border-bottom: 1px solid var(--border);
  padding-top: 8px;
}

.aside-tab {
  border: none;
  background: none;
  padding: 6px 10px;
  font-size: 12px;
  color: var(--text2);
  border-bottom: 2px solid transparent;
  cursor: pointer;
}

.aside-tab.active {
  color: var(--text);
  border-bottom-color: var(--run);
}

.aside-inspector {
  flex-shrink: 0;
}

.aside-log-console {
  flex: 1;
  min-height: 260px;
}

.events-toggle {
  margin-top: 10px;
}

.events-list {
  margin-top: 6px;
  max-height: 240px;
  overflow-y: auto;
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 6px 8px;
  background: var(--surface2);
}

.events-item {
  font-size: 11px;
  padding: 1px 0;
  display: flex;
  gap: 8px;
}
</style>
