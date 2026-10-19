<script setup lang="ts">
import { computed, onBeforeUnmount, ref, shallowRef, triggerRef } from "vue";
import {
  EventWindow,
  JournalClient,
  type JournalPage,
  type Receipt,
  type RunSnapshot,
} from "../api/journal";
import { isDesktop } from "../platform";
import CodeEditor from "../components/CodeEditor.vue";
import JsonNode from "../components/JsonNode.vue";
import JournalLogList from "../components/JournalLogList.vue";
import ResizeHandle from "../components/ResizeHandle.vue";
import AppButton from "../components/AppButton.vue";
import { runBadgeClass, runStatusLabel } from "../state/labels";
import { usePanelResize } from "../state/panel-resize";
import { confirmDialog } from "../state/modal";
import { toast } from "../state/toast";
import type { ObservationRecord } from "../types";

const desktop = isDesktop();
const url = ref("ws://127.0.0.1:9802");
const http = ref("http://127.0.0.1:9803");
const token = ref("");
const api = shallowRef<JournalClient>();
const workflows = ref<Array<{ workflow_id: string; name: string }>>([]);
const runs = ref<RunSnapshot[]>([]);
const workflow = ref("");
const name = ref("");
const input = ref("{}");
const definition = ref(
  '{"nodes":[{"id":"start","type":"start"},{"id":"end","type":"end"}],"edges":[{"from":"start","to":"end"}]}',
);
const run = shallowRef<RunSnapshot>();
const page = shallowRef<JournalPage>();
const audit = ref(false);
const message = ref("");
const busy = ref(false);
const windowed = shallowRef(new EventWindow());
const observations = shallowRef<ObservationRecord[]>([]);
const observationLoss = shallowRef<unknown>();
let refreshing = false;
let frame: number | undefined;
let stop: (() => Promise<void>) | undefined;
let disposed = false;

/** 观测增量水位：已展示的最大 seq（十进制字符串），作为下一页 from_seq。
 *  run.observations.page 的 from_seq 是闭区间（seq >= from_seq），追加时跳过水位上的那条 */
let obsSeq = "0";
/** 观测拉取在途守卫：事件密集时不叠加请求 */
let obsLoading = false;
/** 观测拉取节流窗口：任何订阅事件/对齐都请求一次，窗口内合并成一次 */
const OBS_PULL_INTERVAL_MS = 250;
let obsTimer: ReturnType<typeof setTimeout> | undefined;

function appendObservations(records: ObservationRecord[]): void {
  const fresh = records.filter((r) => BigInt(r.seq) > BigInt(obsSeq));
  const last = fresh[fresh.length - 1];
  if (last) obsSeq = last.seq;
  if (fresh.length) observations.value = [...observations.value, ...fresh];
}

/** 观测增量拉取：from_seq=水位继续分页，只追加；loss 显示仍整包替换 */
async function pullObservations() {
  if (!api.value || !run.value || obsLoading) return;
  obsLoading = true;
  try {
    const id = run.value.run_id;
    const logs = await api.value.call<{ records: ObservationRecord[]; loss: unknown }>(
      "run.observations.page",
      { run_id: id, from_seq: obsSeq, limit: 100 },
    );
    if (run.value?.run_id !== id) return;
    appendObservations(logs.records);
    observationLoss.value = logs.loss;
  } finally {
    obsLoading = false;
  }
}

function scheduleObsRefresh(): void {
  if (obsTimer !== undefined) return;
  obsTimer = setTimeout(() => {
    obsTimer = undefined;
    if (disposed) return;
    void pullObservations().catch((e) => {
      message.value = String(e);
    });
  }, OBS_PULL_INTERVAL_MS);
}

/** 会改变 run 状态的事件（journal EventKind 的 snake_case 名）：
 *  这些事件到达后需要补拉 run.get 摘要与观测页 */
const RUN_LIFECYCLE_KINDS = new Set([
  "run_started",
  "run_cancelled",
  "run_completed",
  "run_failed",
]);
let runRefreshQueued = false;
/** 摘要刷新的防抖：同一 tick 多条事件只刷一次；在途时由 refreshRun 的 refreshing 守卫去重 */
function scheduleRunRefresh(): void {
  if (runRefreshQueued) return;
  runRefreshQueued = true;
  void Promise.resolve().then(() => {
    runRefreshQueued = false;
    if (disposed) return;
    void refreshRun().catch((e) => {
      message.value = String(e);
    });
  });
}

/** JSON 字段即时校验：非法时行内报错并禁用对应提交按钮（Monaco 标红之外的显式拦截） */
function jsonError(text: string): string {
  try {
    JSON.parse(text);
    return "";
  } catch (e) {
    return String(e);
  }
}
const definitionError = computed(() => jsonError(definition.value));
const inputError = computed(() => jsonError(input.value));

// 左栏（表单区）：拖右边缘改宽度；经 CSS 变量传给 grid,1100px 以下的单列媒体查询不受影响
const sideEl = ref<HTMLElement | null>(null);
const sideResize = usePanelResize({
  target: sideEl,
  storageKey: "flow.journal-side-w",
  fallback: 360,
  min: 280,
  max: () => Math.min(640, (sideEl.value?.parentElement?.clientWidth ?? 1200) * 0.5),
  measure: (rect, e) => e.clientX - rect.left,
});
function formatJsonText(text: string): string {
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text; // 按钮在非法时已禁用，这里是兜底
  }
}
const downloads = computed(() => {
  const refs = new Set<string>();
  const visit = (v: unknown): void => {
    if (!v || typeof v !== "object") return;
    const o = v as Record<string, unknown>;
    if (o.type === "ref" && o.value && typeof o.value === "object") {
      const id = (o.value as Record<string, unknown>).output_id;
      if (typeof id === "string") refs.add(id);
    }
    for (const child of Object.values(o)) visit(child);
  };
  visit(run.value);
  return [...refs];
});

async function act(work: () => Promise<void>) {
  busy.value = true;
  message.value = "";
  try {
    await work();
  } catch (e) {
    message.value = String(e);
  } finally {
    busy.value = false;
  }
}

function receipt(r: Receipt) {
  message.value = r.visible
    ? "已提交并可见"
    : `已提交，投影尚未可见。请求 ${r.request_id}，提交 ${r.commit_cursor.lsn}；请查询原请求，勿重复创建。`;
}

async function load() {
  if (!api.value) return;
  workflows.value = (
    await api.value.call<{ values: typeof workflows.value }>("workflow.list", { limit: 64 })
  ).values;
  runs.value = (await api.value.call<{ values: RunSnapshot[] }>("run.list", { limit: 64 })).values;
}

async function connect() {
  api.value?.close();
  await stop?.();
  stop = undefined;
  run.value = undefined;
  api.value = new JournalClient(url.value, token.value, http.value);
  await load();
}

async function create() {
  if (!api.value) return;
  const r = await api.value.command("workflow.create", { name: name.value });
  receipt(r);
  workflow.value = String(r.result.workflow_id);
  await load();
}

async function save() {
  if (!api.value) return;
  if (definition.value.length > 1024 * 1024) throw new Error("定义超过 1 MiB");
  const r = await api.value.command("workflow.update", {
    workflow_id: workflow.value,
    definition: JSON.parse(definition.value),
  });
  receipt(r);
  if (!r.visible) return;
  const published = await api.value.command("workflow.publish", {
    workflow_id: workflow.value,
    version: r.result.version,
  });
  receipt(published);
}

async function start() {
  if (!api.value) return;
  const r = await api.value.command(
    "run.start",
    { workflow_id: workflow.value, input: JSON.parse(input.value) },
    "run.start:manual:",
  );
  receipt(r);
  if (r.visible) await watchRun(String(r.result.run_id));
  await load();
}

/** run 摘要刷新：只补拉 run.get；观测走 pullObservations 的 from_seq 增量 */
async function refreshRun() {
  if (!api.value || !run.value || refreshing) return;
  refreshing = true;
  try {
    const id = run.value.run_id;
    const current = await api.value.call<{ value: RunSnapshot }>("run.get", { run_id: id });
    if (run.value?.run_id !== id) return;
    run.value = current.value;
  } finally {
    refreshing = false;
  }
}

async function watchRun(id: string) {
  await stop?.();
  windowed.value = new EventWindow();
  page.value = undefined;
  observations.value = [];
  observationLoss.value = undefined;
  obsSeq = "0";
  stop = await api.value!.monitor(
    id,
    (v) => {
      run.value = v;
      // snapshot 回调在初次对齐、丢包重排、断线重连（JournalClient 内部
      // onReconnect → align）后都会到达：这些时点摘要与观测页可能错过数据，补拉一次
      scheduleRunRefresh();
      scheduleObsRefresh();
    },
    (e) => {
      windowed.value.push(e);
      // 任何事件都可能伴随观测落库：节流拉增量，不必区分事件类型
      scheduleObsRefresh();
      if (RUN_LIFECYCLE_KINDS.has(e.event.kind)) scheduleRunRefresh();
      if (frame === undefined)
        frame = requestAnimationFrame(() => {
          frame = undefined;
          triggerRef(windowed);
        });
    },
    (n) => {
      if (n) message.value = `实时缓冲丢弃 ${n} 条，已按快照重新对齐；历史可分页读取。`;
    },
  );
  await refreshRun();
  await pullObservations();
}

async function nextPage(reset = false) {
  if (!api.value || !run.value) return;
  page.value = await api.value.page(
    run.value.run_id,
    audit.value,
    reset ? undefined : page.value?.next_cursor,
  );
}

async function cancel() {
  if (!api.value || !run.value) return;
  if (!(await confirmDialog(`确定取消 run ${run.value.run_id.slice(0, 8)}…？`, { danger: true })))
    return;
  receipt(
    await api.value.command(
      "run.cancel",
      { run_id: run.value.run_id },
      `run.cancel:${run.value.run_id}`,
    ),
  );
  await refreshRun();
}

async function copyRunId(id: string) {
  try {
    await navigator.clipboard.writeText(id);
    toast.success("已复制 run id");
  } catch {
    toast.error("复制失败");
  }
}

onBeforeUnmount(() => {
  disposed = true;
  api.value?.close();
  if (frame !== undefined) cancelAnimationFrame(frame);
  if (obsTimer !== undefined) clearTimeout(obsTimer);
  void stop?.();
});
</script>

<template>
  <main class="page journal">
    <div class="page-header">
      <div class="page-title">
        <h2>事件审计</h2>
        <p class="muted page-sub">journal v2 事件与审计检查台（运维/调试面）</p>
      </div>
      <span v-if="api" class="muted mono journal-endpoint">{{ desktop ? "本地数据" : url }}</span>
    </div>

    <form class="panel connect-bar" @submit.prevent="act(connect)">
      <template v-if="!desktop">
        <div class="field">
          <label>RPC 地址</label>
          <input v-model="url" required />
        </div>
        <div class="field">
          <label>下载地址</label>
          <input v-model="http" required />
        </div>
        <div class="field">
          <label>访问令牌</label>
          <input v-model="token" type="password" autocomplete="off" required />
        </div>
      </template>
      <AppButton type="submit" variant="primary" :disabled="busy">
        {{ desktop ? "打开本地工作区" : "连接" }}
      </AppButton>
    </form>

    <p v-if="message" role="status" class="journal-status">{{ message }}</p>

    <div
      v-if="api"
      class="journal-grid"
      :style="{ '--journal-side-w': sideResize.size.value + 'px' }"
    >
      <div ref="sideEl" class="journal-side">
        <ResizeHandle
          orientation="vertical"
          class="edge-right"
          @dragstart="sideResize.onDragStart"
        />
        <section class="panel">
          <h3>工作流</h3>
          <div class="field">
            <label>选择工作流</label>
            <select v-model="workflow">
              <option value="">选择工作流</option>
              <option v-for="w in workflows" :key="w.workflow_id" :value="w.workflow_id">
                {{ w.name }}
              </option>
            </select>
          </div>
          <div class="field journal-new">
            <input v-model="name" placeholder="新工作流名称" />
            <AppButton type="button" :disabled="busy || !name" @click="act(create)">新建</AppButton>
          </div>
          <div class="field">
            <label>定义 JSON</label>
            <CodeEditor v-model="definition" language="json" height="220px" />
            <div v-if="definitionError" class="field-error">{{ definitionError }}</div>
            <button
              v-else
              type="button"
              class="link journal-format"
              @click="definition = formatJsonText(definition)"
            >
              格式化
            </button>
          </div>
          <AppButton
            type="button"
            variant="primary"
            :disabled="busy || !workflow || !!definitionError"
            @click="act(save)"
          >
            保存并发布新版本
          </AppButton>
          <div class="field">
            <label>运行输入 JSON</label>
            <CodeEditor v-model="input" language="json" height="110px" />
            <div v-if="inputError" class="field-error">{{ inputError }}</div>
            <button
              v-else
              type="button"
              class="link journal-format"
              @click="input = formatJsonText(input)"
            >
              格式化
            </button>
          </div>
          <AppButton
            type="button"
            variant="primary"
            :disabled="busy || !workflow || !!inputError"
            @click="act(start)"
          >
            开始运行
          </AppButton>
        </section>

        <section class="panel">
          <div class="panel-head">
            <h3>运行记录</h3>
            <AppButton type="button" :disabled="busy" @click="act(load)">刷新</AppButton>
          </div>
          <p class="muted journal-hint">每次最多展示 64 条。完整历史可通过分页接口读取。</p>
          <table v-if="runs.length" class="data-table">
            <thead>
              <tr>
                <th>run id</th>
                <th>状态</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="r in runs" :key="r.run_id">
                <td>
                  <button
                    type="button"
                    class="copy-id"
                    :title="r.run_id"
                    @click="copyRunId(r.run_id)"
                  >
                    {{ r.run_id.slice(0, 8) }}…
                  </button>
                </td>
                <td>
                  <span class="badge" :class="runBadgeClass(r.status)">
                    {{ runStatusLabel(r.status) }}
                  </span>
                </td>
                <td>
                  <AppButton type="button" variant="link" @click="act(() => watchRun(r.run_id))">
                    监控
                  </AppButton>
                </td>
              </tr>
            </tbody>
          </table>
          <p v-else class="muted journal-hint">暂无运行记录</p>
        </section>
      </div>

      <section v-if="run" class="panel journal-detail">
        <div class="panel-head">
          <h3>运行详情</h3>
          <AppButton type="button" variant="danger" :disabled="busy" @click="act(cancel)">
            取消运行
          </AppButton>
        </div>
        <div class="journal-runhead">
          <button type="button" class="copy-id" :title="run.run_id" @click="copyRunId(run.run_id)">
            {{ run.run_id.slice(0, 8) }}…
          </button>
          <span class="badge" :class="runBadgeClass(run.status)">
            {{ runStatusLabel(run.status) }}
          </span>
        </div>

        <h3>快照</h3>
        <div class="code-block journal-json"><JsonNode :value="run" /></div>

        <template v-if="downloads.length">
          <h3>完整值下载</h3>
          <div class="journal-downloads">
            <AppButton
              v-for="id in downloads"
              :key="id"
              type="button"
              @click="act(() => api!.download(run!.run_id, id))"
            >
              下载 {{ id }}
            </AppButton>
          </div>
        </template>

        <h3>
          实时事件
          <span class="muted journal-hint">展示丢弃 {{ windowed.dropped }} 条</span>
        </h3>
        <JournalLogList :items="windowed.events" empty-text="暂无实时事件" />

        <h3>历史事件与审计</h3>
        <div class="journal-audit">
          <label class="journal-check">
            <input v-model="audit" type="checkbox" @change="page = undefined" /> 审计记录
          </label>
          <AppButton type="button" @click="act(() => nextPage(true))">读取新快照</AppButton>
          <AppButton type="button" :disabled="!page?.next_cursor" @click="act(() => nextPage())">
            下一页
          </AppButton>
        </div>
        <JournalLogList :items="page?.events ?? []" empty-text="尚未读取历史事件" />

        <h3>观测日志</h3>
        <p class="muted journal-hint">以下丢弃计数属于整个观测存储，不代表业务数据丢失。</p>
        <pre class="code-block">{{ JSON.stringify(observationLoss) }}</pre>
        <JournalLogList :items="observations" empty-text="暂无观测日志" />
        <div class="journal-obs-more">
          <AppButton type="button" :disabled="obsLoading" @click="act(() => pullObservations())">
            加载更多
          </AppButton>
          <span class="muted journal-hint">从当前 seq 水位继续分页读取</span>
        </div>
      </section>
    </div>
  </main>
</template>

<style scoped>
.page-title {
  display: flex;
  align-items: baseline;
  gap: 10px;
  min-width: 0;
}

.page-sub {
  margin: 0;
  font-size: 11px;
}

.journal-endpoint {
  font-size: 11px;
}

.panel {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 16px;
}

.panel-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 8px;
}

.panel-head h3 {
  margin: 0;
}

.connect-bar {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  align-items: flex-end;
  margin-bottom: 12px;
}

.connect-bar .field {
  margin-bottom: 0;
  min-width: 200px;
}

.journal-status {
  margin: 0 0 12px;
  padding: 8px 12px;
  background: var(--surface2);
  border: 1px solid var(--border);
  border-radius: 6px;
  font-size: 12px;
  overflow-wrap: anywhere;
}

.journal-grid {
  display: grid;
  grid-template-columns: var(--journal-side-w, 360px) 1fr;
  gap: 16px;
  align-items: start;
}

@media (max-width: 1100px) {
  .journal-grid {
    grid-template-columns: 1fr;
  }

  .journal-grid .edge-right {
    display: none;
  }
}

.journal-side {
  position: relative;
  display: flex;
  flex-direction: column;
  gap: 16px;
  min-width: 0;
}

/* 手柄探入 16px 的列间距，扩大可抓区域 */
.edge-right {
  right: -8px;
}

.journal-new {
  display: flex;
  gap: 8px;
}

.journal-new button {
  flex-shrink: 0;
}

.journal-hint {
  font-size: 11px;
  font-weight: 400;
  text-transform: none;
  letter-spacing: normal;
}

.journal-detail {
  min-width: 0;
}

.journal-detail h3 {
  margin-top: 16px;
}

.journal-runhead {
  display: flex;
  align-items: center;
  gap: 10px;
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

.journal-obs-more {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-top: 8px;
}

.journal-downloads {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.journal-audit {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
}

.journal-check {
  display: flex;
  align-items: center;
  gap: 6px;
  color: var(--text2);
}

.journal-check input {
  width: auto;
}

.code-block {
  max-height: 360px;
  overflow: auto;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 12px;
  font-family: var(--mono);
  font-size: 12px;
}

/* JsonNode 容器：不用 pre 的 pre-wrap，由折叠树自己换行 */
.journal-json {
  white-space: normal;
}

.journal-format {
  margin-top: 4px;
}

.code-block::-webkit-scrollbar {
  width: 4px;
  height: 4px;
}

.code-block::-webkit-scrollbar-thumb {
  background: var(--surface3);
  border-radius: 2px;
}
</style>
