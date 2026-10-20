<script setup lang="ts">
import { computed, onMounted, onUnmounted, reactive, ref, watch } from "vue";
import * as api from "../api/flow";
import { webhookBase } from "../api/flow";
import { client, errText } from "../rpc/client";
import { editor, ensureNodeTypes, refreshWorkflows } from "../state/editor";
import { discoverInputKeys } from "../state/run-input";
import { generateInvokeKey, invokeCurl, resultCurl, sha256Hex } from "../state/invocation";
import { confirmDialog } from "../state/modal";
import { fmtTime } from "../state/time";
import { usePolling } from "../state/polling";
import { toast } from "../state/toast";
import { copyWithToast } from "../state/clipboard";
import type { Definition, Schedule, VersionMeta, Webhook } from "../types";
import AppButton from "../components/AppButton.vue";
import WorkflowInputForm from "../components/WorkflowInputForm.vue";

/** 触发器页：定时调度（schedule.*）+ webhook（webhook.*），作用于单个工作流 */
const props = defineProps<{ workflowId: string }>();

const schedules = ref<Schedule[]>([]);
const webhooks = ref<Webhook[]>([]);
/** 首次拉取完成前不展示空态，避免「暂无…」闪烁 */
const loaded = ref(false);
/** 刷新失败：展示错误条并保留上一次成功的列表，成功后清空 */
const error = ref("");
/** 失败只在 ok→error 跳变时 toast：首刷/轮询/手动/重连可能连续触发，避免重复弹错 */
const pollFailed = ref(false);
/** 在途守卫：慢响应时手动/轮询/重连触发的刷新不叠加 */
let refreshing = false;

// 新建调度表单
const newCron = ref("");
const newInput = ref("");
const creating = ref(false);
const creatingWebhook = ref(false);
/** 正在启停的行：只禁用本行按钮，不同行可并发 */
const togglingScheduleIds = ref(new Set<string>());
const togglingWebhookTokens = ref(new Set<string>());
/** 正在删除的行：只禁用本行删除按钮，防双击重复请求 */
const deletingScheduleIds = ref(new Set<string>());
const deletingWebhookTokens = ref(new Set<string>());

// 调度行内编辑：一次只开一行；input 用纯 JSON 文本域（存量值可能不是对象，表单模式无法无损表示）
const editingScheduleId = ref<string | null>(null);
const editCron = ref("");
const editInput = ref("");
const savingScheduleIds = ref(new Set<string>());

const publishedVersions = ref<VersionMeta[]>([]);
const definition = ref<Definition | null>(null);

/** 输入表单的字段来源：最新版定义 + 节点类型 schema；拿不到时退化为纯 JSON 输入 */
function loadDefinition(): void {
  void (async () => {
    if (!(await ensureNodeTypes())) return;
    try {
      definition.value = (await api.getWorkflow(props.workflowId)).definition;
    } catch {
      // 定义拉不到不影响调度管理，输入区退化为纯 JSON 文本域
    }
  })();
}

/** 已发布版本：调用合同的版本固定只允许已发布（服务端同样校验 published） */
function loadPublishedVersions(): void {
  void (async () => {
    try {
      publishedVersions.value = (await api.listVersions(props.workflowId))
        .filter((v) => v.status === "published")
        .sort((a, b) => b.version - a.version);
    } catch {
      // 版本拉不到不影响触发器管理，调用合同下拉为空并给出提示
    }
  })();
}
const inputKeys = computed(() =>
  definition.value ? discoverInputKeys(definition.value, editor.nodeTypes) : [],
);

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
    for (const w of webhooks.value) formFor(w);
    error.value = "";
    pollFailed.value = false;
  } catch (e) {
    error.value = errText(e);
    if (!pollFailed.value) toast.error(errText(e));
    pollFailed.value = true;
  } finally {
    refreshing = false;
    loaded.value = true;
  }
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
  if (togglingScheduleIds.value.has(s.id)) return;
  togglingScheduleIds.value.add(s.id);
  try {
    await api.updateSchedule(s.id, { enabled: !s.enabled });
    await refresh();
  } catch (e) {
    toast.error(errText(e));
  } finally {
    togglingScheduleIds.value.delete(s.id);
  }
}

async function onDeleteSchedule(s: Schedule): Promise<void> {
  if (deletingScheduleIds.value.has(s.id)) return;
  if (!(await confirmDialog(`确定删除调度 ${s.cron_expr}？`, { danger: true }))) return;
  deletingScheduleIds.value.add(s.id);
  try {
    await api.deleteSchedule(s.id);
    await refresh();
  } catch (e) {
    toast.error(errText(e));
  } finally {
    deletingScheduleIds.value.delete(s.id);
  }
}

function startEditSchedule(s: Schedule): void {
  editingScheduleId.value = s.id;
  editCron.value = s.cron_expr;
  editInput.value = s.input == null ? "" : JSON.stringify(s.input);
}

function cancelEditSchedule(): void {
  editingScheduleId.value = null;
}

async function onSaveSchedule(s: Schedule): Promise<void> {
  if (savingScheduleIds.value.has(s.id)) return;
  const cron = editCron.value.trim();
  if (!cron) {
    toast.error("cron 表达式不能为空");
    return;
  }
  // 双 Option 语义：与原文一致 → 不传（不改）；清空 → null（清空）；改动 → 解析值
  const params: api.UpdateScheduleParams = {};
  if (cron !== s.cron_expr) params.cron = cron;
  const raw = editInput.value.trim();
  const original = s.input == null ? "" : JSON.stringify(s.input);
  if (raw !== original) {
    if (raw) {
      try {
        params.input = JSON.parse(raw);
      } catch {
        toast.error("输入不是合法 JSON");
        return;
      }
    } else {
      params.input = null;
    }
  }
  if (Object.keys(params).length === 0) {
    toast.info("没有改动");
    return;
  }
  savingScheduleIds.value.add(s.id);
  try {
    await api.updateSchedule(s.id, params);
    editingScheduleId.value = null;
    toast.success("调度已更新");
    await refresh();
  } catch (e) {
    toast.error(errText(e));
  } finally {
    savingScheduleIds.value.delete(s.id);
  }
}

function hookUrl(w: Webhook): string {
  return `${webhookBase()}/hooks/${w.token}`;
}

function curlExample(w: Webhook): string {
  // v2 入口：Bearer 认证（token = 部署令牌 FLOW_JOURNAL_TOKEN）
  return `curl -X POST ${hookUrl(w)} -H "Authorization: Bearer $FLOW_JOURNAL_TOKEN" -H 'Idempotency-Key: replace-with-a-stable-delivery-id' -H 'Content-Type: application/json' -d '{"key":"value"}'`;
}

async function onCopy(w: Webhook): Promise<void> {
  await copyWithToast(hookUrl(w), "webhook URL");
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
  if (togglingWebhookTokens.value.has(w.token)) return;
  togglingWebhookTokens.value.add(w.token);
  try {
    await api.setWebhookEnabled(w.token, !w.enabled);
    await refresh();
  } catch (e) {
    toast.error(errText(e));
  } finally {
    togglingWebhookTokens.value.delete(w.token);
  }
}

async function onDeleteWebhook(w: Webhook): Promise<void> {
  if (deletingWebhookTokens.value.has(w.token)) return;
  if (!(await confirmDialog("确定删除该 webhook？删除后 URL 立即失效。", { danger: true }))) return;
  deletingWebhookTokens.value.add(w.token);
  try {
    await api.deleteWebhook(w.token);
    await refresh();
  } catch (e) {
    toast.error(errText(e));
  } finally {
    deletingWebhookTokens.value.delete(w.token);
  }
}

// ---- webhook 调用合同：版本固定 + 调用密钥（服务端只存 SHA-256 哈希） ----

/**
 * 每张 webhook 的调用合同表单。按 token 持久：10s 轮询会整体替换 webhooks 数组，
 * 表单状态（未保存草稿、会话内记住的哈希）不受影响。
 */
interface InvokeForm {
  /** 版本下拉当前值（空 = 未选） */
  version: string;
  /** 粘贴入口输入框（点「使用」才进 keyDraft） */
  paste: string;
  /** 待保存的密钥明文：只在生成/粘贴到保存之间存在，落库后立刻清掉 */
  keyDraft: string;
  /** 保存后记住的哈希/版本：webhook.list 不回显时仍能展示，也能只改版本不重设密钥 */
  savedKeyHash: string | null;
  savedVersion: number | null;
  saving: boolean;
}
const invokeForms = reactive<Record<string, InvokeForm>>({});

function formFor(w: Webhook): InvokeForm {
  let form = invokeForms[w.token];
  if (!form) {
    form = {
      version: w.invoke_version != null ? String(w.invoke_version) : "",
      paste: "",
      keyDraft: "",
      savedKeyHash: w.invoke_key_hash ?? null,
      savedVersion: w.invoke_version ?? null,
      saving: false,
    };
    invokeForms[w.token] = form;
  }
  return form;
}

/** 已知调用密钥哈希：会话内保存结果优先，其次服务端回显（将来 webhook.list 回显时直接生效） */
function keyHashOf(w: Webhook): string | null {
  return formFor(w).savedKeyHash ?? w.invoke_key_hash ?? null;
}

function fixedVersionOf(w: Webhook): number | null {
  return formFor(w).savedVersion ?? w.invoke_version ?? null;
}

/** 服务端是否回显了调用合同：没有回显时要明说「保存后立即生效，列表接口暂不回显」 */
function invokeEchoed(w: Webhook): boolean {
  return w.invoke_version != null || w.invoke_key_hash != null;
}

/** 调用合同已配置（固定版本或调用密钥任一）：调用示例以 /invoke/{token} 为主 */
function contractReady(w: Webhook): boolean {
  return fixedVersionOf(w) !== null || keyHashOf(w) !== null;
}

function keyHashPrefix(w: Webhook): string {
  return (keyHashOf(w) ?? "").slice(0, 12);
}

function onGenerateKey(w: Webhook): void {
  formFor(w).keyDraft = generateInvokeKey();
}

function onUsePastedKey(w: Webhook): void {
  const form = formFor(w);
  const key = form.paste.trim();
  // 后端要求调用密钥至少 32 字节（POST /invoke 的 Bearer 校验）
  if (key.length < 32) {
    toast.error("调用密钥至少 32 字节");
    return;
  }
  form.keyDraft = key;
  form.paste = "";
}

async function onCopyInvokeKey(w: Webhook): Promise<void> {
  await copyWithToast(formFor(w).keyDraft, "调用密钥");
}

async function onSaveInvoke(w: Webhook): Promise<void> {
  const form = formFor(w);
  if (form.saving) return;
  const version = Number(form.version);
  if (!form.version || !Number.isInteger(version)) {
    toast.error("请先选择要固定的已发布版本");
    return;
  }
  const previousHash = keyHashOf(w);
  let keyHash: string;
  if (form.keyDraft) {
    // 覆盖旧键不可逆：旧明文立即失效且无法找回，必须先确认
    if (
      previousHash &&
      !(await confirmDialog(
        "重设调用密钥将覆盖旧密钥：旧密钥立即失效且明文无法找回，调用方需更换密钥。确定继续？",
        { danger: true, confirmText: "覆盖旧密钥" },
      ))
    ) {
      return;
    }
    keyHash = await sha256Hex(form.keyDraft);
  } else if (previousHash) {
    keyHash = previousHash; // 只改版本时复用已知哈希，不必重设密钥
  } else {
    toast.error("请先生成或粘贴调用密钥");
    return;
  }
  form.saving = true;
  try {
    await api.updateWebhookInvoke(w.token, { invoke_version: version, invoke_key_hash: keyHash });
    form.savedVersion = version;
    form.savedKeyHash = keyHash;
    form.keyDraft = ""; // 明文只显示一次：落库后立刻从界面消失
    toast.success("调用合同已保存，立即生效");
  } catch (e) {
    toast.error(errText(e));
  } finally {
    form.saving = false;
  }
}

// 首拉 + 10s 轮询（页面不可见自动暂停）+ 手动刷新 + 重连补齐；next_fire_at 只在刷新时更新，不做倒计时
const offReconnect = client.onReconnect(() => void refresh());
usePolling(refresh, 10_000);

onMounted(() => {
  if (editor.workflows.length === 0) void refreshWorkflows();
  void refresh();
  loadDefinition();
  loadPublishedVersions();
});

// RouterView 无 key：跨工作流前进/后退复用实例，参数变化时重置行内编辑态并按新 id 重拉
watch(
  () => props.workflowId,
  (id, prev) => {
    if (!id || id === prev) return;
    schedules.value = [];
    webhooks.value = [];
    loaded.value = false;
    error.value = "";
    editingScheduleId.value = null;
    newCron.value = "";
    newInput.value = "";
    for (const k of Object.keys(invokeForms)) delete invokeForms[k];
    definition.value = null;
    publishedVersions.value = [];
    void refresh();
    loadDefinition();
    loadPublishedVersions();
  },
);

onUnmounted(() => {
  offReconnect();
});
</script>

<template>
  <div class="page">
    <div class="page-header">
      <h2>
        触发器
        <span class="muted">{{ workflowName || workflowId }}</span>
      </h2>
      <div class="header-actions">
        <AppButton @click="refresh()">刷新</AppButton>
        <!-- 工作流子页互跳 -->
        <RouterLink class="link" :to="`/workflows/${workflowId}/versions`">版本</RouterLink>
        <RouterLink class="link" :to="`/workflows/${workflowId}/runs`">运行记录</RouterLink>
        <RouterLink class="link" :to="`/workflows/${workflowId}`">打开编辑器 →</RouterLink>
      </div>
    </div>

    <div v-if="error" role="alert" class="error-banner">
      <span>加载触发器失败：{{ error }}</span>
      <AppButton @click="refresh()">重试</AppButton>
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
        <WorkflowInputForm
          v-model="newInput"
          :input-keys="inputKeys"
          :rows="2"
          placeholder='输入 JSON（可选），每次触发作为 run 的 input，如 {"src":"cron"}'
        />
        <AppButton variant="primary" type="submit" :disabled="creating || !newCron.trim()">
          新建调度
        </AppButton>
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
          <template v-for="s in schedules" :key="s.id">
            <tr>
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
                <AppButton variant="link" @click="startEditSchedule(s)">编辑</AppButton>
                <AppButton
                  variant="link"
                  :disabled="togglingScheduleIds.has(s.id)"
                  @click="onToggleSchedule(s)"
                >
                  {{ s.enabled ? "停用" : "启用" }}
                </AppButton>
                <AppButton
                  variant="link-danger"
                  :disabled="deletingScheduleIds.has(s.id)"
                  @click="onDeleteSchedule(s)"
                >
                  删除
                </AppButton>
              </td>
            </tr>
            <tr v-if="editingScheduleId === s.id" class="edit-row">
              <td colspan="6">
                <div class="edit-form">
                  <input v-model="editCron" class="code cron-input edit-cron" placeholder="cron 表达式" />
                  <textarea
                    v-model="editInput"
                    class="code edit-input"
                    rows="2"
                    placeholder='触发 input（JSON，留空=每次触发无输入），如 {"src":"cron"}'
                  ></textarea>
                  <AppButton
                    variant="primary"
                    type="button"
                    :disabled="savingScheduleIds.has(s.id)"
                    @click="onSaveSchedule(s)"
                  >
                    保存
                  </AppButton>
                  <AppButton type="button" @click="cancelEditSchedule()">取消</AppButton>
                </div>
                <p class="muted edit-hint">
                  保存只提交改动项：input 清空 = 删除输入（null），保持原样 = 不修改。
                </p>
              </td>
            </tr>
          </template>
        </tbody>
      </table>
      <p v-if="!loaded" class="wf-empty">加载中…</p>
      <p v-else-if="!error && schedules.length === 0" class="wf-empty">
        暂无调度。调度按本地时间触发，每次触发运行当前已发布版本。
      </p>
    </section>

    <section class="section">
      <div class="section-header">
        <h3>Webhook</h3>
        <AppButton variant="primary" :disabled="creatingWebhook" @click="onCreateWebhook">
          新建 webhook
        </AppButton>
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
                <AppButton variant="link" @click="onCopy(w)">复制</AppButton>
                <AppButton
                  variant="link"
                  :disabled="togglingWebhookTokens.has(w.token)"
                  @click="onToggleWebhook(w)"
                >
                  {{ w.enabled ? "停用" : "启用" }}
                </AppButton>
                <AppButton
                  variant="link-danger"
                  :disabled="deletingWebhookTokens.has(w.token)"
                  @click="onDeleteWebhook(w)"
                >
                  删除
                </AppButton>
              </td>
            </tr>
            <tr class="curl-row">
              <td colspan="4">
                <details>
                  <summary class="muted">调用示例</summary>
                  <!-- 主次：已配置调用合同时业务调用 /invoke/{token} 为主示例 -->
                  <template v-if="contractReady(w)">
                    <p class="muted curl-label">
                      业务调用：POST /invoke/{{ w.token }}（Bearer 调用密钥 + Idempotency-Key）
                    </p>
                    <pre class="mono curl-example">{{ invokeCurl(webhookBase(), w.token) }}</pre>
                    <pre class="mono curl-example">{{ resultCurl(webhookBase()) }}</pre>
                    <p class="muted curl-label">运维/管理调用（管理 token）</p>
                    <pre class="mono curl-example">{{ curlExample(w) }}</pre>
                  </template>
                  <template v-else>
                    <p class="muted curl-label">
                      未配置调用合同：以下为运维/管理调用（管理 token）；配置调用合同后主示例为
                      /invoke/{token}
                    </p>
                    <pre class="mono curl-example">{{ curlExample(w) }}</pre>
                  </template>
                </details>
              </td>
            </tr>
            <tr class="contract-row">
              <td colspan="4">
                <details>
                  <summary class="muted">调用合同（版本固定 + 调用密钥）</summary>
                  <div class="contract-body">
                    <div class="contract-status">
                      <span v-if="fixedVersionOf(w) !== null" class="badge run-succeeded">
                        已固定 v{{ fixedVersionOf(w) }}
                      </span>
                      <span v-else class="badge">未固定版本</span>
                      <template v-if="keyHashOf(w)">
                        <span class="muted">密钥已设置 · SHA-256 前缀</span>
                        <code class="mono key-hash">{{ keyHashPrefix(w) }}…</code>
                      </template>
                      <span v-else class="muted">密钥未设置</span>
                    </div>
                    <p v-if="!invokeEchoed(w)" class="muted contract-note">
                      保存后立即生效，列表接口暂不回显 invoke_version / invoke_key_hash；下方状态是本次会话的记忆，刷新后如需改版本或密钥，需重设密钥。
                    </p>
                    <div class="key-area">
                      <template v-if="formFor(w).keyDraft">
                        <p class="key-warning">
                          服务端只存哈希，明文只显示这一次：请立即复制保存，丢失只能重设密钥。
                        </p>
                        <div class="key-plain-row">
                          <code class="mono key-plain">{{ formFor(w).keyDraft }}</code>
                          <AppButton variant="link" @click="onCopyInvokeKey(w)">复制密钥</AppButton>
                        </div>
                      </template>
                      <div v-else class="key-actions">
                        <AppButton @click="onGenerateKey(w)">
                          {{ keyHashOf(w) ? "重设密钥" : "生成密钥" }}
                        </AppButton>
                        <input
                          v-model="formFor(w).paste"
                          class="code key-paste"
                          placeholder="粘贴已有调用密钥（≥32 字节）"
                        />
                        <AppButton @click="onUsePastedKey(w)">使用</AppButton>
                      </div>
                    </div>
                    <div class="contract-save">
                      <label class="muted">
                        版本固定
                        <select v-model="formFor(w).version" class="invoke-version">
                          <option value="" disabled>
                            {{
                              publishedVersions.length
                                ? "选择要固定的已发布版本"
                                : "暂无已发布版本，请先发布"
                            }}
                          </option>
                          <option
                            v-for="v in publishedVersions"
                            :key="v.version"
                            :value="String(v.version)"
                          >
                            v{{ v.version }}（{{ fmtTime(v.created_at) }}）
                          </option>
                        </select>
                      </label>
                      <AppButton
                        variant="primary"
                        :disabled="formFor(w).saving"
                        @click="onSaveInvoke(w)"
                      >
                        保存调用合同
                      </AppButton>
                    </div>
                    <p class="muted contract-note">
                      版本固定与调用密钥一起保存；只改版本时沿用已知哈希，不必重设密钥。
                    </p>
                    <ul class="contract-notes muted">
                      <li>202 只表示受理，不代表业务完成；结果用回执里的 run_token 查询。</li>
                      <li>重试必须复用同一 Idempotency-Key，保证只触发一次；X-Correlation-Id 可选，用于业务关联。</li>
                      <li>run_token 只绑定该次 run，不要外发管理令牌。</li>
                    </ul>
                    <pre class="mono curl-example">{{ invokeCurl(webhookBase(), w.token) }}</pre>
                    <pre class="mono curl-example">{{ resultCurl(webhookBase()) }}</pre>
                  </div>
                </details>
              </td>
            </tr>
          </template>
        </tbody>
      </table>
      <p v-if="!loaded" class="wf-empty">加载中…</p>
      <p v-else-if="!error && webhooks.length === 0" class="wf-empty">
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

.header-actions {
  display: flex;
  align-items: center;
  gap: 10px;
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

.curl-label {
  margin: 6px 0 2px;
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

/* 调度编辑行与调用合同行都贴住上一行，视觉上属于同一条记录 */
.edit-row td,
.contract-row td {
  padding-top: 0;
  border-bottom: 1px solid var(--border);
}

.edit-form {
  display: flex;
  gap: 10px;
  align-items: flex-start;
  margin: 2px 0 4px;
}

.edit-input {
  width: 320px;
  font-family: inherit;
}

.edit-form button {
  flex-shrink: 0;
  height: 27px;
}

.edit-hint {
  margin: 2px 0 6px;
  font-size: 11px;
}

.contract-row summary {
  cursor: pointer;
  font-size: 11px;
}

.contract-body {
  margin: 6px 0 2px;
  padding: 8px 10px;
  background: var(--surface2);
  border: 1px solid var(--border);
  border-radius: 6px;
  font-size: 12px;
}

.contract-status {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 6px;
}

.contract-note {
  margin: 4px 0;
  font-size: 11px;
}

.key-area {
  margin: 6px 0;
}

.key-warning {
  margin: 0 0 4px;
  color: var(--danger);
  font-size: 11px;
}

.key-plain-row {
  display: flex;
  align-items: center;
  gap: 8px;
}

.key-plain {
  padding: 2px 6px;
  border: 1px solid var(--danger);
  border-radius: 4px;
  word-break: break-all;
}

.key-actions,
.contract-save {
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 4px 0;
}

.key-paste {
  width: 320px;
}

.contract-notes {
  margin: 4px 0;
  padding-left: 18px;
  font-size: 11px;
}
</style>
