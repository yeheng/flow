<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import * as api from "../api/flow";
import { webhookBase } from "../api/flow";
import { errText } from "../rpc/client";
import { editor, refreshWorkflows } from "../state/editor";
import { confirmDialog } from "../state/modal";
import { toast } from "../state/toast";
import type { Schedule, Webhook } from "../types";

/** 触发器页：定时调度（schedule.*）+ webhook（webhook.*），作用于单个工作流 */
const props = defineProps<{ workflowId: string }>();

const schedules = ref<Schedule[]>([]);
const webhooks = ref<Webhook[]>([]);
/** 首次拉取完成前不展示空态，避免「暂无…」闪烁 */
const loaded = ref(false);
/** 轮询失败只在 ok→error 跳变时 toast，避免每 10s 重复弹错 */
const pollFailed = ref(false);
let timer: ReturnType<typeof setInterval> | null = null;
/** 轮询在途守卫：慢响应时请求不叠加 */
let refreshing = false;

// 新建调度表单
const newCron = ref("");
const newInput = ref("");
const creating = ref(false);
const creatingWebhook = ref(false);
/** 正在启停的行（schedule.id / webhook.token）：只禁用对应按钮 */
const togglingScheduleId = ref<string | null>(null);
const togglingWebhookToken = ref<string | null>(null);

const workflowName = computed(
  () => editor.workflows.find((w) => w.workflow_id === props.workflowId)?.name ?? "",
);

async function refresh(): Promise<void> {
  if (refreshing) return;
  refreshing = true;
  try {
    [schedules.value, webhooks.value] = await Promise.all([
      api.listSchedules(props.workflowId),
      api.listWebhooks(props.workflowId),
    ]);
    pollFailed.value = false;
  } catch (e) {
    if (!pollFailed.value) toast.error(errText(e));
    pollFailed.value = true;
  } finally {
    refreshing = false;
    loaded.value = true;
  }
}

function fmtTime(iso: string | null): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

function fmtInput(input: unknown): string {
  if (input === null || input === undefined) return "—";
  const s = JSON.stringify(input);
  return s.length > 40 ? s.slice(0, 40) + "…" : s;
}

async function onCreateSchedule(): Promise<void> {
  const cron = newCron.value.trim();
  if (!cron) return;
  // input 文本域：空 = 无输入；非空必须是合法 JSON（前端先拦，cron 非法由后端 -32010 toast）
  let input: unknown;
  const raw = newInput.value.trim();
  if (raw) {
    try {
      input = JSON.parse(raw);
    } catch {
      toast.error("输入不是合法 JSON");
      return;
    }
  }
  creating.value = true;
  try {
    await api.createSchedule(props.workflowId, cron, input);
    newCron.value = "";
    newInput.value = "";
    toast.success("调度已创建");
    await refresh();
  } catch (e) {
    toast.error(errText(e));
  } finally {
    creating.value = false;
  }
}

async function onToggleSchedule(s: Schedule): Promise<void> {
  if (togglingScheduleId.value) return;
  togglingScheduleId.value = s.id;
  try {
    await api.updateSchedule(s.id, { enabled: !s.enabled });
    await refresh();
  } catch (e) {
    toast.error(errText(e));
  } finally {
    togglingScheduleId.value = null;
  }
}

async function onDeleteSchedule(s: Schedule): Promise<void> {
  if (!(await confirmDialog(`确定删除调度 ${s.cron_expr}？`, { danger: true }))) return;
  try {
    await api.deleteSchedule(s.id);
    await refresh();
  } catch (e) {
    toast.error(errText(e));
  }
}

function hookUrl(w: Webhook): string {
  return `${webhookBase()}/hook/${w.token}`;
}

function curlExample(w: Webhook): string {
  return `curl -X POST ${hookUrl(w)} -H 'Content-Type: application/json' -d '{"key":"value"}'`;
}

async function onCopy(w: Webhook): Promise<void> {
  try {
    await navigator.clipboard.writeText(hookUrl(w));
    toast.success("已复制 webhook URL");
  } catch {
    toast.error("复制失败，请手动复制");
  }
}

async function onCreateWebhook(): Promise<void> {
  if (creatingWebhook.value) return;
  creatingWebhook.value = true;
  try {
    await api.createWebhook(props.workflowId);
    toast.success("webhook 已创建");
    await refresh();
  } catch (e) {
    toast.error(errText(e));
  } finally {
    creatingWebhook.value = false;
  }
}

async function onToggleWebhook(w: Webhook): Promise<void> {
  if (togglingWebhookToken.value) return;
  togglingWebhookToken.value = w.token;
  try {
    await api.setWebhookEnabled(w.token, !w.enabled);
    await refresh();
  } catch (e) {
    toast.error(errText(e));
  } finally {
    togglingWebhookToken.value = null;
  }
}

async function onDeleteWebhook(w: Webhook): Promise<void> {
  if (!(await confirmDialog("确定删除该 webhook？删除后 URL 立即失效。", { danger: true }))) return;
  try {
    await api.deleteWebhook(w.token);
    await refresh();
  } catch (e) {
    toast.error(errText(e));
  }
}

onMounted(async () => {
  if (editor.workflows.length === 0) void refreshWorkflows();
  await refresh();
  // 页面激活期间每 10s 轮询：next_fire_at 会随时间走；离开页面停止
  timer = setInterval(() => void refresh(), 10000);
});

onUnmounted(() => {
  if (timer) clearInterval(timer);
});
</script>

<template>
  <div class="page">
    <div class="page-header">
      <h2>
        触发器
        <span class="muted">{{ workflowName || workflowId }}</span>
      </h2>
      <RouterLink class="link" :to="`/workflows/${workflowId}`">打开编辑器 →</RouterLink>
    </div>

    <section class="section">
      <div class="section-header">
        <h3>定时调度</h3>
      </div>
      <form class="create-form" @submit.prevent="onCreateSchedule">
        <input
          v-model="newCron"
          class="code cron-input"
          placeholder="cron 表达式（分 时 日 月 周），如 */5 * * * *"
        />
        <textarea
          v-model="newInput"
          class="code input-json"
          placeholder='输入 JSON（可选），每次触发作为 run 的 input，如 {"src":"cron"}'
          rows="2"
        />
        <button class="primary" type="submit" :disabled="creating || !newCron.trim()">
          新建调度
        </button>
      </form>
      <table class="data-table">
        <thead>
          <tr>
            <th>cron</th>
            <th>下次触发</th>
            <th>输入</th>
            <th>状态</th>
            <th>创建时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="s in schedules" :key="s.id">
            <td class="mono">{{ s.cron_expr }}</td>
            <td :title="s.next_fire_at ?? undefined">{{ fmtTime(s.next_fire_at) }}</td>
            <td class="mono muted" :title="JSON.stringify(s.input)">{{ fmtInput(s.input) }}</td>
            <td>
              <span class="badge" :class="s.enabled ? 'run-succeeded' : 'run-cancelled'">
                {{ s.enabled ? "已启用" : "已停用" }}
              </span>
            </td>
            <td :title="s.created_at">{{ fmtTime(s.created_at) }}</td>
            <td class="actions">
              <button
                class="link"
                :disabled="togglingScheduleId === s.id"
                @click="onToggleSchedule(s)"
              >
                {{ s.enabled ? "停用" : "启用" }}
              </button>
              <button class="link danger" @click="onDeleteSchedule(s)">删除</button>
            </td>
          </tr>
        </tbody>
      </table>
      <p v-if="!loaded" class="wf-empty">加载中…</p>
      <p v-else-if="schedules.length === 0" class="wf-empty">
        暂无调度。调度按本地时间触发，每次触发运行当前已发布版本。
      </p>
    </section>

    <section class="section">
      <div class="section-header">
        <h3>Webhook</h3>
        <button class="primary" :disabled="creatingWebhook" @click="onCreateWebhook">
          新建 webhook
        </button>
      </div>
      <table class="data-table">
        <thead>
          <tr>
            <th>Hook URL</th>
            <th>状态</th>
            <th>创建时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <template v-for="w in webhooks" :key="w.token">
            <tr>
              <td class="mono hook-url">{{ hookUrl(w) }}</td>
              <td>
                <span class="badge" :class="w.enabled ? 'run-succeeded' : 'run-cancelled'">
                  {{ w.enabled ? "已启用" : "已停用" }}
                </span>
              </td>
              <td :title="w.created_at">{{ fmtTime(w.created_at) }}</td>
              <td class="actions">
                <button class="link" @click="onCopy(w)">复制</button>
                <button
                  class="link"
                  :disabled="togglingWebhookToken === w.token"
                  @click="onToggleWebhook(w)"
                >
                  {{ w.enabled ? "停用" : "启用" }}
                </button>
                <button class="link danger" @click="onDeleteWebhook(w)">删除</button>
              </td>
            </tr>
            <tr class="curl-row">
              <td colspan="4">
                <details>
                  <summary class="muted">curl 示例</summary>
                  <pre class="mono curl-example">{{ curlExample(w) }}</pre>
                </details>
              </td>
            </tr>
          </template>
        </tbody>
      </table>
      <p v-if="!loaded" class="wf-empty">加载中…</p>
      <p v-else-if="webhooks.length === 0" class="wf-empty">
        暂无 webhook。POST 请求体（JSON）会作为 run 的 input，触发当前已发布版本。
      </p>
    </section>
  </div>
</template>

<style scoped>
.section {
  margin-bottom: 28px;
}

.section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 10px;
}

.section-header h3 {
  margin: 0;
}

.create-form {
  display: flex;
  gap: 10px;
  align-items: flex-start;
  margin-bottom: 12px;
}

.cron-input {
  width: 280px;
  flex-shrink: 0;
}

.input-json {
  flex: 1;
  resize: vertical;
}

.create-form button {
  flex-shrink: 0;
  height: 27px;
}

.hook-url {
  word-break: break-all;
}

/* curl 示例行贴住上一行，视觉上属于同一条 webhook */
.curl-row td {
  padding-top: 0;
  border-bottom: 1px solid var(--border);
}

.curl-row summary {
  cursor: pointer;
  font-size: 11px;
}

.curl-example {
  margin: 6px 0 2px;
  padding: 8px 10px;
  background: var(--surface2);
  border: 1px solid var(--border);
  border-radius: 6px;
  font-size: 11px;
  white-space: pre-wrap;
  word-break: break-all;
}
</style>
