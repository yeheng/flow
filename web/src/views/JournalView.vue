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
import { runBadgeClass, runStatusLabel } from "../state/labels";
import { confirmDialog } from "../state/modal";
import { toast } from "../state/toast";

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
const observations = shallowRef<unknown[]>([]);
const observationLoss = shallowRef<unknown>();
let refreshing = false;
let frame: number | undefined;
let stop: (() => Promise<void>) | undefined;
let timer: ReturnType<typeof setInterval> | undefined;

const detail = computed(() => JSON.stringify(run.value, null, 2));
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
  if (timer) clearInterval(timer);
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

async function refreshRun() {
  if (!api.value || !run.value || refreshing) return;
  refreshing = true;
  try {
    const id = run.value.run_id;
    const current = await api.value.call<{ value: RunSnapshot }>("run.get", { run_id: id });
    if (run.value?.run_id !== id) return;
    run.value = current.value;
    const logs = await api.value.call<{ records: unknown[]; loss: unknown }>(
      "run.observations.page",
      { run_id: id, limit: 100 },
    );
    if (run.value?.run_id !== id) return;
    observations.value = logs.records;
    observationLoss.value = logs.loss;
  } finally {
    refreshing = false;
  }
}

async function watchRun(id: string) {
  await stop?.();
  if (timer) clearInterval(timer);
  windowed.value = new EventWindow();
  page.value = undefined;
  stop = await api.value!.monitor(
    id,
    (v) => {
      run.value = v;
    },
    (e) => {
      windowed.value.push(e);
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
  timer = setInterval(() => {
    void refreshRun().catch((e) => {
      message.value = String(e);
    });
  }, 2000);
  await refreshRun();
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
    await api.value.command("run.cancel", { run_id: run.value.run_id }, `run.cancel:${run.value.run_id}`),
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
  api.value?.close();
  if (frame !== undefined) cancelAnimationFrame(frame);
  void stop?.();
  if (timer) clearInterval(timer);
});
</script>

<template>
  <main class="page journal">
    <div class="page-header">
      <h2>JSONL 工作区</h2>
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
      <button type="submit" class="primary" :disabled="busy">
        {{ desktop ? "打开本地工作区" : "连接" }}
      </button>
    </form>

    <p v-if="message" role="status" class="journal-status">{{ message }}</p>

    <div v-if="api" class="journal-grid">
      <div class="journal-side">
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
            <button type="button" :disabled="busy || !name" @click="act(create)">新建</button>
          </div>
          <div class="field">
            <label>定义 JSON</label>
            <textarea
              v-model="definition"
              class="code"
              maxlength="1048576"
              rows="6"
              spellcheck="false"
            />
          </div>
          <button
            type="button"
            class="primary"
            :disabled="busy || !workflow"
            @click="act(save)"
          >
            保存并发布新版本
          </button>
          <div class="field">
            <label>运行输入 JSON</label>
            <textarea
              v-model="input"
              class="code"
              maxlength="8388608"
              rows="3"
              spellcheck="false"
            />
          </div>
          <button
            type="button"
            class="primary"
            :disabled="busy || !workflow"
            @click="act(start)"
          >
            开始运行
          </button>
        </section>

        <section class="panel">
          <div class="panel-head">
            <h3>运行记录</h3>
            <button type="button" :disabled="busy" @click="act(load)">刷新</button>
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
                <td class="mono copyable" :title="r.run_id" @click="copyRunId(r.run_id)">
                  {{ r.run_id.slice(0, 8) }}…
                </td>
                <td>
                  <span class="badge" :class="runBadgeClass(r.status)">
                    {{ runStatusLabel(r.status) }}
                  </span>
                </td>
                <td>
                  <button type="button" class="link" @click="act(() => watchRun(r.run_id))">
                    监控
                  </button>
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
          <button type="button" class="danger" :disabled="busy" @click="act(cancel)">
            取消运行
          </button>
        </div>
        <div class="journal-runhead">
          <span class="mono copyable" :title="run.run_id" @click="copyRunId(run.run_id)">
            {{ run.run_id.slice(0, 8) }}…
          </span>
          <span class="badge" :class="runBadgeClass(run.status)">
            {{ runStatusLabel(run.status) }}
          </span>
        </div>

        <h3>快照</h3>
        <pre class="code-block">{{ detail }}</pre>

        <template v-if="downloads.length">
          <h3>完整值下载</h3>
          <div class="journal-downloads">
            <button
              v-for="id in downloads"
              :key="id"
              type="button"
              @click="act(() => api!.download(run!.run_id, id))"
            >
              下载 {{ id }}
            </button>
          </div>
        </template>

        <h3>
          实时事件
          <span class="muted journal-hint">展示丢弃 {{ windowed.dropped }} 条</span>
        </h3>
        <pre class="code-block">{{ JSON.stringify(windowed.events, null, 2) }}</pre>

        <h3>历史事件与审计</h3>
        <div class="journal-audit">
          <label class="journal-check">
            <input v-model="audit" type="checkbox" @change="page = undefined" /> 审计记录
          </label>
          <button type="button" @click="act(() => nextPage(true))">读取新快照</button>
          <button type="button" :disabled="!page?.next_cursor" @click="act(() => nextPage())">
            下一页
          </button>
        </div>
        <pre class="code-block">{{ JSON.stringify(page?.events ?? [], null, 2) }}</pre>

        <h3>观测日志</h3>
        <p class="muted journal-hint">以下丢弃计数属于整个观测存储，不代表业务数据丢失。</p>
        <pre class="code-block">{{ JSON.stringify(observationLoss) }}</pre>
        <pre class="code-block">{{ JSON.stringify(observations, null, 2) }}</pre>
      </section>
    </div>
  </main>
</template>

<style scoped>
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
  grid-template-columns: 360px 1fr;
  gap: 16px;
  align-items: start;
}

@media (max-width: 1100px) {
  .journal-grid {
    grid-template-columns: 1fr;
  }
}

.journal-side {
  display: flex;
  flex-direction: column;
  gap: 16px;
  min-width: 0;
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

.copyable {
  cursor: pointer;
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

.code-block::-webkit-scrollbar {
  width: 4px;
  height: 4px;
}

.code-block::-webkit-scrollbar-thumb {
  background: var(--surface3);
  border-radius: 2px;
}
</style>
