<script setup lang="ts">
import { computed, onBeforeUnmount, ref, shallowRef } from "vue";
import { JournalClient, type JournalPage, type RunSnapshot } from "../api/journal";
import { isDesktop } from "../platform";
import JsonNode from "../components/JsonNode.vue";
import JournalLogList from "../components/JournalLogList.vue";
import ResizeHandle from "../components/ResizeHandle.vue";
import AppButton from "../components/AppButton.vue";
import { runBadgeClass, runStatusLabel } from "../state/labels";
import { usePanelResize } from "../state/panel-resize";
import { copyWithToast } from "../state/clipboard";
import type { ObservationRecord } from "../types";

/**
 * 事件审计检查台（运维/调试面）：直连任意 journal 工作区，按 run 查看
 * 审计记录（run.audit.page）、原始快照（run.get）与观测原始记录
 * （run.observations.page）。工作流 CRUD、发起运行、实时监控分别在
 * 工作流列表/编辑器/运行记录页，本页不做二次实现。
 */

const desktop = isDesktop();
const url = ref("ws://127.0.0.1:9802");
const http = ref("http://127.0.0.1:9803");
const token = ref("");
const api = shallowRef<JournalClient>();
const runs = ref<RunSnapshot[]>([]);
const runId = ref("");
const run = shallowRef<RunSnapshot>();
const page = shallowRef<JournalPage>();
/** 审计流开关：默认审计（本页主旨）；取消勾选看 control 业务事件流 */
const audit = ref(true);
const message = ref("");
const busy = ref(false);
const observations = shallowRef<ObservationRecord[]>([]);
const observationLoss = shallowRef<unknown>();

/** 观测增量水位：已展示的最大 seq（十进制字符串），作为下一页 from_seq。
 *  run.observations.page 的 from_seq 是闭区间（seq >= from_seq），追加时跳过水位上的那条 */
let obsSeq = "0";
/** 观测拉取在途守卫：事件密集时不叠加请求 */
let obsLoading = false;

function appendObservations(records: ObservationRecord[]): void {
  const fresh = records.filter((r) => BigInt(r.seq) > BigInt(obsSeq));
  const last = fresh[fresh.length - 1];
  if (last) obsSeq = last.seq;
  if (fresh.length) observations.value = [...observations.value, ...fresh];
}

/** 观测增量拉取：from_seq=水位继续分页，只追加；loss 显示仍整包替换 */
async function pullObservations() {
  if (!api.value || !runId.value || obsLoading) return;
  obsLoading = true;
  try {
    const id = runId.value;
    const logs = await api.value.call<{ records: ObservationRecord[]; loss: unknown }>(
      "run.observations.page",
      { run_id: id, from_seq: obsSeq, limit: 100 },
    );
    if (runId.value !== id) return;
    appendObservations(logs.records);
    observationLoss.value = logs.loss;
  } finally {
    obsLoading = false;
  }
}

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

async function load() {
  if (!api.value) return;
  runs.value = (await api.value.call<{ values: RunSnapshot[] }>("run.list", { limit: 64 })).values;
}

async function connect() {
  api.value?.close();
  runId.value = "";
  run.value = undefined;
  page.value = undefined;
  observations.value = [];
  observationLoss.value = undefined;
  obsSeq = "0";
  api.value = new JournalClient(url.value, token.value, http.value);
  await load();
}

/** 选择 run：重置分页/观测水位，拉一次快照与首页观测 */
async function selectRun(id: string) {
  if (!api.value || !id) return;
  ++pageGeneration;
  runId.value = id;
  run.value = undefined;
  page.value = undefined;
  observations.value = [];
  observationLoss.value = undefined;
  obsSeq = "0";
  await refreshRun();
  await pullObservations();
}

async function refreshRun() {
  if (!api.value || !runId.value) return;
  const id = runId.value;
  const current = await api.value.call<{ value: RunSnapshot }>("run.get", { run_id: id });
  if (runId.value !== id) return;
  run.value = current.value;
}

/** 手动刷新：补拉 run 摘要，并从观测水位继续拉增量 */
async function refreshAll() {
  await refreshRun();
  await pullObservations();
}

let pageGeneration = 0;
async function nextPage(reset = false) {
  if (!api.value || !runId.value) return;
  const generation = ++pageGeneration;
  const id = runId.value;
  const client = api.value;
  const auditMode = audit.value;
  const next = await client.page(id, auditMode, reset ? undefined : page.value?.next_cursor);
  if (generation !== pageGeneration || runId.value !== id || api.value !== client || audit.value !== auditMode) return;
  page.value = next;
}

async function copyRunId(id: string) {
  await copyWithToast(id, "run id");
}

onBeforeUnmount(() => {
  api.value?.close();
});
</script>

<template>
  <main class="page journal">
    <div class="page-header">
      <div class="page-title">
        <h2>事件审计</h2>
        <p class="muted page-sub">journal v2 审计记录与原始数据检查台（运维/调试面）</p>
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
          <div class="panel-head">
            <h3>运行记录</h3>
            <AppButton type="button" :disabled="busy" @click="act(load)">刷新</AppButton>
          </div>
          <p class="muted journal-hint">每次最多展示 64 条，选择一行开始审计。</p>
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
                  <AppButton type="button" variant="link" :disabled="busy" @click="act(() => selectRun(r.run_id))">
                    查看
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
          <AppButton type="button" :disabled="busy" @click="act(refreshAll)">
            刷新
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

        <h3>历史事件与审计</h3>
        <div class="journal-audit">
          <label class="journal-check">
            <input v-model="audit" type="checkbox" @change="page = undefined" /> 审计记录
          </label>
          <AppButton type="button" @click="act(() => nextPage(true))">重新读取</AppButton>
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

.code-block::-webkit-scrollbar {
  width: 4px;
  height: 4px;
}

.code-block::-webkit-scrollbar-thumb {
  background: var(--surface3);
  border-radius: 2px;
}
</style>
