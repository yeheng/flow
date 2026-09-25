<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from "vue";
import { useRouter } from "vue-router";
import * as api from "../api/flow";
import { errText } from "../rpc/client";
import { editor, ensureNodeTypes, refreshWorkflows } from "../state/editor";
import { attachRun, detachRun, monitor } from "../state/monitor";
import { toast } from "../state/toast";
import type { Definition, RunEvent, RunRecord } from "../types";
import RunCanvas from "../components/RunCanvas.vue";
import RunMonitor from "../components/RunMonitor.vue";

const props = defineProps<{ runId: string }>();

const router = useRouter();

const record = ref<RunRecord | null>(null);
const live = ref(false);
const definition = ref<Definition | null>(null);
const showEvents = ref(false);
const events = ref<RunEvent[] | null>(null);
/** 快速切换 run（子 run 链接）时旧加载的响应一律丢弃 */
let generation = 0;

const workflowName = computed(() => {
  const wf = record.value?.workflow_id;
  return editor.workflows.find((w) => w.workflow_id === wf)?.name ?? wf ?? "";
});

const statusLabel: Record<string, string> = {
  running: "运行中",
  initializing: "初始化中",
  awaiting_resume: "挂起待恢复",
  succeeded: "成功",
  failed: "失败",
  cancelled: "已取消",
};

function fmtTime(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

function fmtJson(v: unknown): string {
  if (v === undefined || v === null) return "";
  return JSON.stringify(v, null, 2);
}

async function copyRunId(): Promise<void> {
  if (!record.value) return;
  try {
    await navigator.clipboard.writeText(record.value.id);
    toast.info("已复制 run id");
  } catch {
    toast.error("复制失败");
  }
}

async function toggleEvents(): Promise<void> {
  showEvents.value = !showEvents.value;
  if (showEvents.value && events.value === null) {
    try {
      events.value = await api.runEvents(props.runId);
    } catch (e) {
      toast.error(errText(e));
    }
  }
}

async function load(runId: string): Promise<void> {
  const gen = ++generation;
  record.value = null;
  definition.value = null;
  events.value = null;
  showEvents.value = false;
  try {
    const r = await api.getRun(runId);
    if (gen !== generation) return;
    record.value = r.run;
    live.value = r.live;
  } catch (e) {
    if (gen !== generation) return;
    toast.error(`加载 run 失败：${errText(e)}`);
    void router.replace("/runs");
    return;
  }
  // RunRecord 含 workflow_id + workflow_version：拉取 run 钉死的那版定义渲染只读 DAG
  if (await ensureNodeTypes()) {
    try {
      const w = await api.getWorkflow(record.value.workflow_id, record.value.workflow_version);
      if (gen === generation) definition.value = w.definition;
    } catch (e) {
      if (gen === generation) toast.error(`加载工作流定义失败：${errText(e)}`);
    }
  }
  try {
    await attachRun(runId);
  } catch (e) {
    if (gen === generation) toast.error(errText(e));
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

// 离开时 detach：仅当 monitor 仍挂在当前 run 上，避免误杀编辑器/其他页新建的 attach
onUnmounted(() => {
  if (monitor.runId === props.runId) void detachRun();
});
</script>

<template>
  <div class="run-detail">
    <div v-if="record" class="run-header">
      <div class="run-header-row">
        <span class="badge" :class="`run-${record.status}`">
          {{ statusLabel[record.status] ?? record.status }}
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
        </span>
      </div>
      <div class="run-header-row run-header-meta">
        <span>开始 {{ fmtTime(record.started_at) }}</span>
        <span>结束 {{ fmtTime(record.ended_at) }}</span>
      </div>
      <div class="run-header-row">
        <span class="muted">输入</span>
        <code class="run-io">{{ fmtJson(record.input) || "（空）" }}</code>
      </div>
      <div v-if="record.error" class="run-header-row">
        <span class="muted">错误</span>
        <code class="run-io run-io-error">{{ record.error }}</code>
      </div>
      <div v-else-if="record.output !== null && record.output !== undefined" class="run-header-row">
        <span class="muted">输出</span>
        <code class="run-io">{{ fmtJson(record.output) }}</code>
      </div>
    </div>

    <div class="run-detail-main">
      <section class="center">
        <RunCanvas v-if="definition" :definition="definition" />
        <div v-else class="canvas-hint">工作流定义不可用，仅展示时间线</div>
      </section>
      <aside class="right">
        <h3>时间线</h3>
        <RunMonitor v-if="monitor.runId" child-run-navigate />
        <p v-else class="run-hint">正在加载…</p>
        <div class="events-toggle">
          <a class="link" @click="toggleEvents">{{
            showEvents ? "收起原始事件" : "展开原始事件"
          }}</a>
        </div>
        <div v-if="showEvents" class="events-list">
          <p v-if="events === null" class="run-hint">加载中…</p>
          <template v-else>
            <div v-for="e in events" :key="e.seq" class="events-item mono">
              <span class="muted">#{{ e.seq }}</span> {{ e.type }}
              <span v-if="e.node_id" class="muted">{{ e.node_id }}</span>
            </div>
            <p v-if="events.length === 0" class="run-hint">无事件</p>
          </template>
        </div>
      </aside>
    </div>
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
}

.run-header-meta {
  color: var(--text2);
  font-size: 12px;
}

.run-header-links {
  margin-left: auto;
  display: flex;
  gap: 10px;
  align-items: baseline;
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

.run-detail-main {
  flex: 1;
  display: grid;
  grid-template-columns: 1fr 380px;
  min-height: 0;
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
