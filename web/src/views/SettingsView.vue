<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import * as api from "../api/flow";
import { errText } from "../rpc/client";
import { isDesktop } from "../platform";
import { refreshSecrets } from "../state/editor";
import { confirmDialog } from "../state/modal";
import { toast } from "../state/toast";
import type { ConfigView, FlowConfig, SecretInfo } from "../types";

/**
 * 设置页：统一配置（config.get/update，重启生效）+ 密钥管理（secrets.*）。
 * 表单由字段描述符驱动——配置结构在 flow-config/src/lib.rs，这里只声明
 * 「哪个分区哪个键怎么渲染」，不复制默认值。
 */

const loading = ref(true);
const saving = ref(false);
const configPath = ref<string | null>(null);
const envOverrides = ref<string[]>([]);
/** 可编辑副本（深拷贝；database_url 保持 "<set>" 哨兵语义） */
const copy = ref<FlowConfig | null>(null);
const original = ref<FlowConfig | null>(null);

/** 模板里的非空中间层：copy 为 null（未加载）时给空对象，v-model 写进的是
 * copy.value 的同一份引用，加载完成后正常读写 */
const model = computed<FlowConfig>(() => copy.value ?? ({} as FlowConfig));

const secrets = ref<SecretInfo[]>([]);
const newSecretName = ref("");
const newSecretValue = ref("");
const addingSecret = ref(false);

const dirty = computed(() => {
  if (!copy.value || !original.value) return false;
  return JSON.stringify(copy.value) !== JSON.stringify(original.value);
});

const envSet = computed(() => new Set(envOverrides.value));

interface FieldDesc {
  key: string;
  label: string;
  type?: "string" | "number" | "bool" | "enum" | "password";
  options?: string[];
  help?: string;
}

interface SectionDesc {
  /** 分区键（FlowConfig 的顶层键） */
  sect: keyof FlowConfig;
  label: string;
  /** 桌面端隐藏（内嵌服务无网络监听/存储布局固定） */
  hideOnDesktop?: boolean;
  /** 仅 backend=postgres 时显示 */
  onlyPostgres?: boolean;
  fields: FieldDesc[];
}

const sections: SectionDesc[] = [
  {
    sect: "server",
    label: "服务器",
    hideOnDesktop: true,
    fields: [
      { key: "rpc_addr", label: "RPC 监听地址", help: "JSON-RPC over WebSocket" },
      { key: "http_addr", label: "Webhook HTTP 地址" },
      { key: "scheduler_enabled", label: "cron 调度器", type: "bool" },
      { key: "scheduler_tick_secs", label: "调度扫描间隔（秒）", type: "number" },
      { key: "journal_trigger_tick_secs", label: "journal 触发扫描间隔（秒）", type: "number" },
    ],
  },
  {
    sect: "storage",
    label: "存储",
    hideOnDesktop: true,
    fields: [
      { key: "backend", label: "后端", type: "enum", options: ["sqlite", "postgres"] },
      { key: "data_dir", label: "数据目录", help: "SQLite 模式的事件日志与密钥存放处" },
      { key: "database", label: "SQLite 库路径", help: "留空 = <数据目录>/flow.db" },
      {
        key: "database_url",
        label: "Postgres 连接串",
        type: "password",
        help: "已设置的值不回显；留空并保存 = 清除",
      },
    ],
  },
  {
    sect: "execution",
    label: "执行",
    fields: [
      {
        key: "mode",
        label: "执行模式",
        type: "enum",
        options: ["in_process", "ipc", "remote"],
        help: "in_process=进程内；ipc=本机执行器子进程；remote=远程 agent",
      },
      { key: "executor_bin", label: "执行器二进制", help: "留空 = 自召唤 flow executor" },
      { key: "x_max", label: "执行槽位（1..=16）", type: "number" },
    ],
  },
  {
    sect: "agent",
    label: "Agent（远程执行）",
    fields: [
      { key: "control_addr", label: "control 地址" },
      { key: "data_addr", label: "data 地址" },
      { key: "agent_id", label: "agent 身份", help: "须与客户端证书 CN 一致" },
      { key: "ca_cert", label: "CA 证书路径" },
      { key: "cert", label: "证书路径" },
      { key: "key", label: "私钥路径" },
      { key: "slots", label: "本机执行槽位", type: "number" },
    ],
  },
  {
    sect: "journal",
    label: "JSONL 工作区（journal-server）",
    hideOnDesktop: true,
    fields: [
      { key: "addr", label: "RPC 监听地址" },
      { key: "http_addr", label: "下载 HTTP 地址", help: "强制 loopback" },
      { key: "data_dir", label: "数据目录" },
    ],
  },
  {
    sect: "pg",
    label: "Postgres 调优",
    onlyPostgres: true,
    hideOnDesktop: true,
    fields: [
      { key: "role", label: "角色", type: "enum", options: ["all", "gateway", "executor"] },
      { key: "lease_ttl_ms", label: "租约 TTL（ms）", type: "number" },
      { key: "scan_interval_ms", label: "扫描间隔（ms）", type: "number" },
      { key: "inbox_poll_ms", label: "inbox 轮询（ms）", type: "number" },
      { key: "max_runs", label: "本进程最大 run 数", type: "number" },
      { key: "signal_wait_ms", label: "信号落账等待（ms）", type: "number" },
      { key: "signal_poll_ms", label: "信号落账轮询（ms）", type: "number" },
      { key: "subscribe_poll_ms", label: "订阅兜底轮询（ms）", type: "number" },
      { key: "statement_timeout_ms", label: "语句超时（ms）", type: "number" },
      { key: "lock_timeout_ms", label: "锁超时（ms）", type: "number" },
      { key: "idle_tx_timeout_ms", label: "idle 事务超时（ms）", type: "number" },
      { key: "max_connections", label: "连接池上限", type: "number" },
    ],
  },
];

const visibleSections = computed(() =>
  sections.filter((s) => {
    if (s.hideOnDesktop && isDesktop()) return false;
    if (s.onlyPostgres && copy.value?.storage.backend !== "postgres") return false;
    return true;
  }),
);

async function refresh(): Promise<void> {
  try {
    const view: ConfigView = await api.getConfig();
    original.value = view.config;
    copy.value = JSON.parse(JSON.stringify(view.config)) as FlowConfig;
    configPath.value = view.config_path;
    envOverrides.value = view.env_overrides;
    secrets.value = await api.listSecrets();
  } catch (e) {
    toast.error(errText(e));
  } finally {
    loading.value = false;
  }
}

onMounted(() => {
  void refresh();
});

function isOverridden(sect: string): boolean {
  // 环境变量名单是 FLOW_XXX 形式，逐字段精确对应太脆弱；这里只在分区级别提示
  // （分区里有任一覆盖就给整区加角标），精确值以 env 为准的语义不变。
  const sectEnv: Record<string, string[]> = {
    server: ["FLOW_ADDR", "FLOW_HTTP_ADDR", "FLOW_SCHEDULER"],
    storage: ["FLOW_BACKEND", "FLOW_DATA_DIR", "FLOW_DB", "FLOW_DATABASE_URL"],
    execution: ["FLOW_EXECUTION_MODE", "FLOW_EXECUTOR_BIN", "FLOW_REMOTE_CONTROL_ADDR", "FLOW_REMOTE_DATA_ADDR", "FLOW_REMOTE_CA", "FLOW_REMOTE_CERT", "FLOW_REMOTE_KEY", "FLOW_REMOTE_ATTACH_TIMEOUT_MS"],
    agent: ["FLOW_AGENT_CONTROL_ADDR", "FLOW_AGENT_DATA_ADDR", "FLOW_AGENT_ID", "FLOW_AGENT_CA", "FLOW_AGENT_CERT", "FLOW_AGENT_KEY", "FLOW_AGENT_SLOTS"],
    journal: ["FLOW_JOURNAL_ADDR", "FLOW_JOURNAL_HTTP_ADDR", "FLOW_JOURNAL_DATA_DIR"],
    pg: ["FLOW_ROLE", "FLOW_LEASE_TTL_MS", "FLOW_SCAN_INTERVAL_MS", "FLOW_INBOX_POLL_MS", "FLOW_MAX_RUNS", "FLOW_SIGNAL_WAIT_MS", "FLOW_SIGNAL_POLL_MS", "FLOW_SUBSCRIBE_POLL_MS", "FLOW_STATEMENT_TIMEOUT_MS", "FLOW_LOCK_TIMEOUT_MS", "FLOW_IDLE_TX_TIMEOUT_MS"],
  };
  return (sectEnv[sect] ?? []).some((name) => envSet.value.has(name));
}

async function onSave(): Promise<void> {
  if (!copy.value) return;
  saving.value = true;
  try {
    const view = await api.updateConfig(copy.value);
    original.value = view.config;
    copy.value = JSON.parse(JSON.stringify(view.config)) as FlowConfig;
    configPath.value = view.config_path;
    envOverrides.value = view.env_overrides;
    toast.success("配置已保存，重启 flow 后生效");
  } catch (e) {
    toast.error(errText(e));
  } finally {
    saving.value = false;
  }
}

function onReset(): void {
  if (!original.value) return;
  copy.value = JSON.parse(JSON.stringify(original.value)) as FlowConfig;
}

// ---- 密钥 ----

async function onAddSecret(): Promise<void> {
  const name = newSecretName.value.trim();
  const value = newSecretValue.value;
  if (!name || !value) {
    toast.error("名称与值都不能为空");
    return;
  }
  addingSecret.value = true;
  try {
    await api.setSecret(name, value);
    newSecretName.value = "";
    newSecretValue.value = "";
    toast.success(`密钥 ${name} 已保存`);
    await refreshSecrets();
    secrets.value = await api.listSecrets();
  } catch (e) {
    toast.error(errText(e));
  } finally {
    addingSecret.value = false;
  }
}

async function onDeleteSecret(name: string): Promise<void> {
  const ok = await confirmDialog(`删除密钥「${name}」？引用它的节点将在执行时报缺失。`);
  if (!ok) return;
  try {
    const deleted = await api.deleteSecret(name);
    if (!deleted) toast.info(`${name} 来自环境变量，请在部署环境移除`);
    else toast.success(`密钥 ${name} 已删除`);
    await refreshSecrets();
    secrets.value = await api.listSecrets();
  } catch (e) {
    toast.error(errText(e));
  }
}
</script>

<template>
  <main class="settings">
    <div class="settings-head">
      <h2>设置</h2>
      <span v-if="configPath" class="config-path" :title="configPath">{{ configPath }}</span>
      <span v-else class="config-path none">未使用配置文件（当前为默认值 + 环境变量）</span>
      <span class="spacer" />
      <button v-if="dirty" class="btn ghost" :disabled="saving" @click="onReset">放弃修改</button>
      <button class="btn primary" :disabled="saving || !dirty || loading" @click="onSave">
        {{ saving ? "保存中…" : "保存（重启后生效）" }}
      </button>
    </div>

    <div v-if="envOverrides.length" class="env-banner">
      以下环境变量在重启后仍会覆盖配置文件：
      <code>{{ envOverrides.join("、") }}</code>
    </div>

    <div v-if="loading" class="empty">加载中…</div>
    <template v-else>
      <section v-for="s in visibleSections" :key="s.sect" class="card">
        <div class="card-head">
          <h3>{{ s.label }}</h3>
          <span v-if="isOverridden(s.sect)" class="env-tag">有环境变量覆盖</span>
        </div>
        <div v-for="f in s.fields" :key="f.key" class="field">
          <label>{{ f.label }}</label>
          <select
            v-if="f.type === 'enum'"
            v-model="(model[s.sect] as unknown as Record<string, unknown>)[f.key]"
          >
            <option v-for="o in f.options" :key="o" :value="o">{{ o }}</option>
          </select>
          <label v-else-if="f.type === 'bool'" class="switch">
            <input
              v-model="(model[s.sect] as unknown as Record<string, unknown>)[f.key]"
              type="checkbox"
            />
            <span>{{
              (model[s.sect] as unknown as Record<string, unknown>)[f.key] ? "开启" : "关闭"
            }}</span>
          </label>
          <input
            v-else
            v-model="(model[s.sect] as unknown as Record<string, unknown>)[f.key]"
            :type="f.type === 'number' ? 'number' : f.type === 'password' ? 'password' : 'text'"
            :placeholder="f.type === 'password' ? '已设置（不回显）' : ''"
            spellcheck="false"
          />
          <div v-if="f.help" class="help">{{ f.help }}</div>
        </div>
      </section>
    </template>

    <section class="card">
      <div class="card-head">
        <h3>密钥</h3>
        <span class="env-tag">值加密存储，永不出服务端</span>
      </div>
      <div v-if="secrets.length === 0" class="empty">暂无密钥。工作流的 x-secret 参数引用这里的名称。</div>
      <table v-else class="table">
        <thead>
          <tr><th>名称</th><th>来源</th><th /></tr>
        </thead>
        <tbody>
          <tr v-for="s in secrets" :key="s.name">
            <td><code>{{ s.name }}</code></td>
            <td>
              <span class="src" :class="s.source">
                {{ s.source === "stored" ? "界面管理" : "环境变量" }}
              </span>
            </td>
            <td class="row-actions">
              <button v-if="s.source === 'stored'" class="btn danger" @click="onDeleteSecret(s.name)">
                删除
              </button>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="secret-add">
        <input v-model="newSecretName" placeholder="名称（如 OPENAI_KEY）" spellcheck="false" />
        <input
          v-model="newSecretValue"
          type="password"
          placeholder="值"
          autocomplete="new-password"
          spellcheck="false"
        />
        <button class="btn primary" :disabled="addingSecret" @click="onAddSecret">添加密钥</button>
      </div>
      <p class="help">
        执行时优先使用这里的密钥，其次回落环境变量 FLOW_SECRET_&lt;名称&gt;。
        workflow.update 会在保存时校验引用的密钥是否存在。
      </p>
    </section>
  </main>
</template>

<style scoped>
.settings {
  flex: 1;
  overflow-y: auto;
  padding: 20px 24px 40px;
  display: flex;
  flex-direction: column;
  gap: 14px;
  max-width: 860px;
}

.settings-head {
  display: flex;
  align-items: center;
  gap: 12px;
}

.settings-head h2 {
  margin: 0;
  font-size: 17px;
}

.spacer {
  flex: 1;
}

.config-path {
  font-family: var(--mono);
  font-size: 11px;
  color: var(--text3);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 380px;
}

.config-path.none {
  color: var(--warn);
}

.env-banner {
  padding: 8px 12px;
  border: 1px solid var(--warn);
  border-radius: 8px;
  background: color-mix(in srgb, var(--warn) 12%, transparent);
  font-size: 12px;
}

.env-banner code {
  font-family: var(--mono);
}

.card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 14px 16px;
}

.card-head {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 10px;
}

.card-head h3 {
  margin: 0;
  font-size: 14px;
}

.env-tag {
  font-size: 11px;
  color: var(--text3);
  border: 1px solid var(--border);
  border-radius: 999px;
  padding: 1px 8px;
}

.field {
  display: grid;
  grid-template-columns: 180px 1fr;
  align-items: center;
  gap: 10px;
  padding: 5px 0;
}

.field label {
  color: var(--text2);
  font-size: 12px;
}

.field input[type="text"],
.field input[type="number"],
.field input[type="password"],
.field select {
  background: var(--surface2);
  border: 1px solid var(--border);
  border-radius: 7px;
  color: var(--text);
  padding: 6px 9px;
  font-size: 12px;
  font-family: var(--mono);
  width: 100%;
}

.field input:focus,
.field select:focus {
  outline: none;
  border-color: var(--accent);
}

.field .help {
  grid-column: 2;
  color: var(--text3);
  font-size: 11px;
  margin-top: -4px;
}

.switch {
  display: flex;
  align-items: center;
  gap: 8px;
  color: var(--text);
}

.table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}

.table th {
  text-align: left;
  color: var(--text3);
  font-weight: 500;
  padding: 4px 8px;
  border-bottom: 1px solid var(--border);
}

.table td {
  padding: 6px 8px;
  border-bottom: 1px solid var(--border);
}

.row-actions {
  text-align: right;
}

.src {
  font-size: 11px;
  border-radius: 999px;
  padding: 1px 8px;
  border: 1px solid var(--border);
}

.src.stored {
  color: var(--accent2);
}

.src.env {
  color: var(--text3);
}

.secret-add {
  display: flex;
  gap: 8px;
  margin-top: 12px;
}

.secret-add input {
  flex: 1;
  background: var(--surface2);
  border: 1px solid var(--border);
  border-radius: 7px;
  color: var(--text);
  padding: 6px 9px;
  font-size: 12px;
}

.secret-add input:focus {
  outline: none;
  border-color: var(--accent);
}

.help {
  color: var(--text3);
  font-size: 11px;
  margin: 8px 0 0;
}

.empty {
  color: var(--text3);
  font-size: 12px;
  padding: 8px 0;
}

.btn {
  border: 1px solid var(--border);
  background: var(--surface2);
  color: var(--text);
  border-radius: 7px;
  padding: 6px 12px;
  font-size: 12px;
  cursor: pointer;
}

.btn.primary {
  background: var(--accent);
  border-color: var(--accent);
}

.btn.primary:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.btn.ghost {
  background: transparent;
}

.btn.danger {
  color: var(--danger);
}
</style>
