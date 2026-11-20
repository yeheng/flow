<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from "vue";
import { onBeforeRouteLeave } from "vue-router";
import * as api from "../api/flow";
import { errText } from "../rpc/client";
import { isDesktop } from "../platform";
import { apiToken, setApiToken, rpcUrl, httpUrl, setServerUrls } from "../config";
import { deepCloneJson } from "../state/clone";
import { confirmDialog } from "../state/modal";
import { toast } from "../state/toast";
import type {
  AgentCredential,
  ConfigView,
  FlowConfig,
  PluginInfo,
  PluginViolation,
  RemoteExecutionConfig,
} from "../types";
import ResidentServicesPanel from "../components/ResidentServicesPanel.vue";
import EnvironmentSettings from "../components/EnvironmentSettings.vue";
import AppButton from "../components/AppButton.vue";

/**
 * 设置页按功能分区，服务、执行和 Harness 共用一份配置草稿。
 * 表单由字段描述符驱动——配置结构在 flow-config/src/lib.rs，这里只声明
 * 「哪个分区哪个键怎么渲染」，不复制默认值。
 */

const connectionRpc = ref(rpcUrl());
const connectionHttp = ref(httpUrl());
const connectionError = ref("");
const connectionToken = ref(apiToken());
function saveConnection(): void {
  try {
    setServerUrls(connectionRpc.value, connectionHttp.value);
  } catch (error) {
    connectionError.value = errText(error);
    return;
  }
  setApiToken(connectionToken.value.trim());
  window.location.reload();
}

const loading = ref(true);
const saving = ref(false);
/** 初始 getConfig 失败时展示内联错误块（含重试），而不是一张空表单 */
const loadError = ref<string | null>(null);
const configPath = ref<string | null>(null);
const envOverrides = ref<string[]>([]);
/** 可编辑副本（深拷贝） */
const copy = ref<FlowConfig | null>(null);
const original = ref<FlowConfig | null>(null);

/** 模板里的非空中间层：copy 为 null（未加载）时给空对象，v-model 写进的是
 * copy.value 的同一份引用，加载完成后正常读写 */
const model = computed(
  () => (copy.value ?? {}) as Record<keyof FlowConfig, Record<string, unknown>>,
);

const tabs = [
  { id: "service", label: "服务" },
  { id: "execution", label: "执行器" },
  { id: "harness", label: "Harness" },
  { id: "environments", label: "运行环境" },
  { id: "variables", label: "环境变量" },
  { id: "secrets", label: "密钥" },
  { id: "plugins", label: "插件与服务" },
] as const;
type SettingsTab = (typeof tabs)[number]["id"];
const visibleTabs = tabs.filter((tab) => tab.id !== "service" || !isDesktop());
const tabKey = "flow.settings-tab";
const activeTab = ref<SettingsTab>(
  visibleTabs.find((tab) => tab.id === sessionStorage.getItem(tabKey))?.id ?? visibleTabs[0].id,
);
const configTab = computed(
  () =>
    activeTab.value === "service" ||
    activeTab.value === "execution" ||
    activeTab.value === "harness",
);
const catalogTab = computed(() => {
  const id = activeTab.value;
  return id === "environments" || id === "variables" || id === "secrets" ? id : null;
});

function selectTab(id: SettingsTab): void {
  activeTab.value = id;
  sessionStorage.setItem(tabKey, id);
}

function navigateTabs(event: KeyboardEvent): void {
  const index = visibleTabs.findIndex((tab) => tab.id === activeTab.value);
  const last = visibleTabs.length - 1;
  const offsets = {
    ArrowLeft: index === 0 ? last : index - 1,
    ArrowRight: index === last ? 0 : index + 1,
    Home: 0,
    End: last,
  };
  const target = offsets[event.key as keyof typeof offsets];
  if (target === undefined) return;
  event.preventDefault();
  const id = visibleTabs[target].id;
  selectTab(id);
  (event.currentTarget as HTMLElement).querySelector<HTMLElement>(`#settings-tab-${id}`)?.focus();
}

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
  /** 已废弃字段：只读展示（存量值随整包 round-trip 原样保留） */
  deprecated?: boolean;
}

interface SectionDesc {
  /** 分区键（FlowConfig 的顶层键） */
  sect: keyof FlowConfig;
  label: string;
  /** 桌面端隐藏（内嵌服务无网络监听/存储布局固定） */
  hideOnDesktop?: boolean;
  fields: FieldDesc[];
}

const plugins = ref<PluginInfo[]>([]);
const pluginViolations = ref<PluginViolation[] | null>(null);
const pluginError = ref("");

const pluginSource = ref("");
const pluginReplace = ref(false);
const pluginBusy = ref(false);
const pluginMutationError = ref("");
const residentPanel = ref<InstanceType<typeof ResidentServicesPanel> | null>(null);
type PluginMutation = { requestId: string } & (
  | { kind: "install"; source: string; replace: boolean }
  | { kind: "uninstall"; pluginId: string; stopFirst: boolean }
);
const pluginPending = ref<PluginMutation | null>(null);
async function runPluginMutation(): Promise<void> {
  const pending = pluginPending.value;
  if (!pending || pluginBusy.value) return;
  pluginBusy.value = true;
  pluginMutationError.value = "";
  try {
    const result =
      pending.kind === "install"
        ? await api.installPlugin(pending.source, pending.replace, pending.requestId)
        : await api.uninstallPlugin(pending.pluginId, pending.stopFirst, pending.requestId);
    pluginPending.value = null;
    await Promise.all([loadPlugins(), residentPanel.value?.refresh()]);
    toast.info(pending.kind === "install" ? "插件安装完成" : "插件已卸载，授予记录已保留");
    if (result.executor_reload_required) toast.info("节点插件已发布，执行器将在后续派发时轮换");
  } catch (error) {
    pluginMutationError.value = errText(error);
    await loadPlugins();
  } finally {
    pluginBusy.value = false;
  }
}
async function installPlugin(): Promise<void> {
  if (pluginPending.value || !pluginSource.value.trim()) return;
  if (
    pluginReplace.value &&
    !(await confirmDialog("替换已有插件？运行中的服务需要先停止。", {
      danger: true,
      confirmText: "替换",
    }))
  )
    return;
  pluginPending.value = {
    kind: "install",
    source: pluginSource.value.trim(),
    replace: pluginReplace.value,
    requestId: crypto.randomUUID(),
  };
  await runPluginMutation();
}
async function uninstallPlugin(plugin: PluginInfo): Promise<void> {
  if (pluginPending.value || plugin.id_matches_directory === false) return;
  if (
    !(await confirmDialog(
      plugin.running
        ? `停止服务并卸载 ${plugin.id}？授予记录和在飞运行会保留。`
        : `卸载 ${plugin.id}？授予记录和在飞运行会保留。`,
      { danger: true, confirmText: plugin.running ? "停止并卸载" : "卸载" },
    ))
  )
    return;
  pluginPending.value = {
    kind: "uninstall",
    pluginId: plugin.directory_name ?? plugin.id,
    stopFirst: !!plugin.running,
    requestId: crypto.randomUUID(),
  };
  await runPluginMutation();
}

async function loadPlugins(): Promise<void> {
  try {
    plugins.value = await api.listPlugins();
    pluginError.value = "";
  } catch (e) {
    pluginError.value = errText(e);
  }
}

async function recheckPlugins(): Promise<void> {
  try {
    pluginViolations.value = await api.recheckPlugins();
    toast.info("重校验完成");
  } catch (e) {
    toast.error(`重校验失败：${errText(e)}`);
  }
}

const sections: SectionDesc[] = [
  {
    sect: "server",
    label: "服务器",
    hideOnDesktop: true,
    fields: [
      { key: "scheduler_enabled", label: "cron 调度器", type: "bool" },
      { key: "journal_trigger_tick_secs", label: "journal 触发扫描间隔（秒）", type: "number" },
    ],
  },
  {
    sect: "storage",
    label: "存储",
    hideOnDesktop: true,
    fields: [
      { key: "backend", label: "后端", type: "enum", options: ["journal"] },
      { key: "data_dir", label: "数据目录", help: "持久化密钥存放处" },
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
        options: ["ipc", "remote"],
        help: "ipc=本机执行器子进程（全平台缺省）；remote=远程 agent；in_process 已删除",
      },
      { key: "executor_bin", label: "执行器二进制", help: "留空 = 自召唤 flow executor" },
      { key: "x_max", label: "执行槽位（1..=16）", type: "number" },
      {
        key: "prewarm",
        label: "预热执行器",
        type: "bool",
        help: "启动时预热一个空闲执行器（占一个 x_max 槽位；失败降级懒加载）",
      },
    ],
  },
  {
    sect: "agent",
    label: "Agent（远程执行）",
    fields: [
      { key: "control_addr", label: "control 地址" },
      { key: "agent_id", label: "agent 身份", help: "须与客户端证书 CN 一致" },
      { key: "ca_cert", label: "CA 证书路径" },
      { key: "cert", label: "证书路径" },
      { key: "key", label: "私钥路径" },
      { key: "slots", label: "本机执行槽位", type: "number" },
      { key: "wss_url", label: "WSS 上联地址", help: "设置后优先于 mTLS 直连" },
      {
        key: "token",
        label: "WSS 接入令牌",
        type: "password",
        help: "master 侧按 token→agent_id 映射反查身份",
      },
      {
        key: "data_addr",
        label: "data 地址",
        deprecated: true,
        help: "已废弃，仅存量兼容（v3 收敛为单条 TLS 连接，值被忽略）",
      },
    ],
  },
  {
    sect: "journal",
    label: "Journal 服务",
    hideOnDesktop: true,
    fields: [
      { key: "addr", label: "RPC 监听地址" },
      { key: "http_addr", label: "下载与 Webhook HTTP 地址", help: "强制 loopback" },
      { key: "data_dir", label: "数据目录" },
    ],
  },
  {
    // harness.agent 是嵌套分区，平铺字段留空；表单在下方专用模板块渲染
    sect: "harness",
    label: "Harness Agent",
    fields: [],
  },
];

const visibleSections = computed(() =>
  sections.filter(({ sect, hideOnDesktop }) => {
    if (hideOnDesktop && isDesktop()) return false;
    if (activeTab.value === "execution") return sect === "execution" || sect === "agent";
    if (activeTab.value === "harness") return sect === "harness";
    return sect === "server" || sect === "storage" || sect === "journal";
  }),
);

/** execution.remote 嵌套块的强类型入口；remote 为空（mode=ipc 常态）时 WSS 接入不可配 */
const remoteCfg = computed(() => copy.value?.execution.remote ?? null);

/** harness.agent 嵌套块入口；旧后端未回传时兜底空对象（保存前会补全结构） */
const harnessAgent = computed(
  () =>
    copy.value?.harness?.agent ?? {
      base_url: null,
      model: null,
      api: null,
      proxy: null,
      max_turns: null,
      max_tool_calls_per_turn: null,
      max_tokens: null,
      tool_timeout_secs: null,
    },
);

function addAgent(): void {
  const r: RemoteExecutionConfig | null = remoteCfg.value;
  if (!r) return;
  r.agents = [...(r.agents ?? []), { id: "", token: "" } satisfies AgentCredential];
}

function removeAgent(index: number): void {
  const r: RemoteExecutionConfig | null = remoteCfg.value;
  if (!r?.agents) return;
  r.agents = r.agents.filter((_, i) => i !== index);
}

/** 已废弃字段的只读回显：null/undefined 显示为空，不回写 */
function str(v: unknown): string {
  return v == null ? "" : String(v);
}

async function refresh(): Promise<void> {
  try {
    const view: ConfigView = await api.getConfig();
    original.value = view.config;
    copy.value = deepCloneJson(view.config) as FlowConfig;
    configPath.value = view.config_path;
    envOverrides.value = view.env_overrides;
    loadError.value = null;
  } catch (e) {
    loadError.value = errText(e);
    toast.error(errText(e));
  } finally {
    loading.value = false;
  }
}

function onRetryLoad(): void {
  loading.value = true;
  loadError.value = null;
  void refresh();
}

// 未保存的配置改动：路由离开先确认，刷新/关页走浏览器原生确认（与编辑器同一套守卫）
onBeforeRouteLeave(async () => {
  if (!dirty.value) return true;
  return confirmDialog("当前设置有未保存的修改，离开后会丢失，确定继续？");
});

function onBeforeUnload(e: BeforeUnloadEvent): void {
  if (!dirty.value) return;
  e.preventDefault();
  e.returnValue = "";
}

watch(
  activeTab,
  (tab) => {
    if (tab === "plugins") void loadPlugins();
  },
  { immediate: true },
);

onMounted(() => {
  void refresh();
  window.addEventListener("beforeunload", onBeforeUnload);
});

onUnmounted(() => {
  window.removeEventListener("beforeunload", onBeforeUnload);
});

function isOverridden(sect: string): boolean {
  // 环境变量名单是 FLOW_XXX 形式，逐字段精确对应太脆弱；这里只在分区级别提示
  // （分区里有任一覆盖就给整区加角标），精确值以 env 为准的语义不变。
  const sectEnv: Record<string, string[]> = {
    server: ["FLOW_SCHEDULER", "FLOW_JOURNAL_TRIGGER_TICK_SECS"],
    storage: ["FLOW_BACKEND", "FLOW_DATA_DIR"],
    execution: [
      "FLOW_EXECUTION_MODE",
      "FLOW_EXECUTOR_BIN",
      "FLOW_REMOTE_CONTROL_ADDR",
      "FLOW_REMOTE_DATA_ADDR",
      "FLOW_REMOTE_CA",
      "FLOW_REMOTE_CERT",
      "FLOW_REMOTE_KEY",
      "FLOW_REMOTE_ATTACH_TIMEOUT_MS",
    ],
    agent: [
      "FLOW_AGENT_CONTROL_ADDR",
      "FLOW_AGENT_DATA_ADDR",
      "FLOW_AGENT_ID",
      "FLOW_AGENT_CA",
      "FLOW_AGENT_CERT",
      "FLOW_AGENT_KEY",
      "FLOW_AGENT_SLOTS",
    ],
    journal: ["FLOW_JOURNAL_ADDR", "FLOW_JOURNAL_HTTP_ADDR", "FLOW_JOURNAL_DATA_DIR"],
  };
  return (sectEnv[sect] ?? []).some((name) => envSet.value.has(name));
}

async function onSave(): Promise<void> {
  if (!copy.value) return;
  saving.value = true;
  try {
    const view = await api.updateConfig(copy.value);
    original.value = view.config;
    copy.value = deepCloneJson(view.config) as FlowConfig;
    configPath.value = view.config_path;
    envOverrides.value = view.env_overrides;
    toast.success("配置已保存，重启 flow 后生效");
  } catch (e) {
    toast.error(errText(e));
  } finally {
    saving.value = false;
  }
}

async function onReset(): Promise<void> {
  if (!original.value) return;
  if (!(await confirmDialog("确定放弃所有未保存的修改？"))) return;
  copy.value = deepCloneJson(original.value) as FlowConfig;
}
</script>

<template>
  <main class="settings">
    <div class="settings-head">
      <h2>设置</h2>
      <span v-if="configTab && configPath" class="config-path" :title="configPath">{{
        configPath
      }}</span>
      <span class="spacer" />
      <AppButton v-if="configTab && dirty" variant="ghost" :disabled="saving" @click="onReset"
        >放弃修改</AppButton
      >
      <AppButton
        v-if="configTab"
        variant="primary"
        :disabled="saving || !dirty || loading"
        @click="onSave"
      >
        {{ saving ? "保存中…" : "保存（重启后生效）" }}
      </AppButton>
    </div>

    <div class="settings-tabs" role="tablist" aria-label="设置分区" @keydown="navigateTabs">
      <AppButton
        v-for="tab in visibleTabs"
        :id="`settings-tab-${tab.id}`"
        :key="tab.id"
        role="tab"
        :aria-selected="activeTab === tab.id"
        aria-controls="settings-panel"
        :tabindex="activeTab === tab.id ? 0 : -1"
        @click="selectTab(tab.id)"
        >{{ tab.label }}</AppButton
      >
    </div>
    <div
      id="settings-panel"
      :aria-labelledby="`settings-tab-${activeTab}`"
      role="tabpanel"
      class="settings-panel"
    >
      <EnvironmentSettings
        v-if="catalogTab"
        :mode="catalogTab"
        :overrides="envOverrides"
        @manage-environments="selectTab('environments')"
      />
      <template v-if="configTab">
        <section v-if="activeTab === 'service'" class="card">
          <div class="card-head">
            <h3>服务连接</h3>
            <code>{{ rpcUrl() }}</code>
          </div>
          <div class="field">
            <label for="connection-rpc">RPC 地址</label>
            <input
              id="connection-rpc"
              v-model="connectionRpc"
              placeholder="ws://127.0.0.1:9802"
              spellcheck="false"
            />
          </div>
          <div class="field">
            <label for="connection-http">HTTP 地址</label>
            <input
              id="connection-http"
              v-model="connectionHttp"
              placeholder="http://127.0.0.1:9803"
              spellcheck="false"
            />
            <div class="help">
              服务可独立运行；地址保存在当前浏览器，保存后刷新连接。清空地址恢复部署默认值。
            </div>
          </div>
          <div v-if="connectionError" role="alert">{{ connectionError }}</div>
          <div class="field">
            <label for="connection-token">API token</label>
            <input
              id="connection-token"
              v-model="connectionToken"
              type="password"
              autocomplete="off"
              spellcheck="false"
            />
            <div class="help">填写 FLOW_JOURNAL_TOKEN，保存在当前浏览器。</div>
          </div>
          <AppButton @click="saveConnection">保存并连接</AppButton>
        </section>

        <div v-if="envOverrides.length" class="env-banner">
          部署环境变量会覆盖配置文件。
          <AppButton variant="link" @click="selectTab('variables')">查看部署覆盖</AppButton>
        </div>

        <div v-if="loading" class="empty">加载中…</div>
        <div v-else-if="loadError" class="empty">
          加载配置失败：{{ loadError }}
          <AppButton @click="onRetryLoad">重试</AppButton>
        </div>
        <template v-else>
          <section v-for="s in visibleSections" :key="s.sect" class="card">
            <div class="card-head">
              <h3>{{ s.label }}</h3>
              <span v-if="isOverridden(s.sect)" class="env-tag">有环境变量覆盖</span>
            </div>
            <div v-for="f in s.fields" :key="f.key" class="field">
              <label>{{ f.label }}</label>
              <select v-if="f.type === 'enum'" v-model="model[s.sect][f.key]">
                <option v-for="o in f.options" :key="o" :value="o">{{ o }}</option>
              </select>
              <label v-else-if="f.type === 'bool'" class="switch">
                <input v-model="model[s.sect][f.key]" type="checkbox" />
                <span>{{ model[s.sect][f.key] ? "开启" : "关闭" }}</span>
              </label>
              <input
                v-else-if="f.deprecated"
                class="deprecated"
                :value="str(model[s.sect][f.key])"
                disabled
                spellcheck="false"
              />
              <input
                v-else
                v-model="model[s.sect][f.key]"
                :type="f.type === 'number' ? 'number' : f.type === 'password' ? 'password' : 'text'"
                :placeholder="f.type === 'password' ? '已设置（不回显）' : ''"
                spellcheck="false"
              />
              <div v-if="f.help" class="help">{{ f.help }}</div>
            </div>

            <!-- execution.remote 嵌套块：wss_addr 与 agents 凭据成对；整包 config.update 时分区替换 -->
            <template v-if="s.sect === 'execution'">
              <template v-if="remoteCfg">
                <div class="field">
                  <label>WSS 接入地址</label>
                  <input v-model="remoteCfg.wss_addr" placeholder="缺省关闭" spellcheck="false" />
                  <div class="help">
                    代理终止 TLS 场景的附加承载（execution.remote.wss_addr），与下方 agent
                    凭据成对配置
                  </div>
                </div>
                <div class="field">
                  <label>WSS agent 凭据</label>
                  <div class="agents">
                    <div v-for="(a, i) in remoteCfg.agents ?? []" :key="i" class="agent-row">
                      <input v-model="a.id" placeholder="agent id" spellcheck="false" />
                      <input v-model="a.token" placeholder="token" spellcheck="false" />
                      <AppButton variant="link-danger" @click="removeAgent(i)">删除</AppButton>
                    </div>
                    <AppButton @click="addAgent">添加凭据</AppButton>
                  </div>
                  <div class="help">
                    execution.remote.agents：token→agent_id 显式映射；id 与 token 表内不得重复
                  </div>
                </div>
                <div class="field">
                  <label>data 地址</label>
                  <input
                    class="deprecated"
                    :value="str(remoteCfg.data_addr)"
                    disabled
                    spellcheck="false"
                  />
                  <div class="help">已废弃，仅存量兼容（v3 收敛为单条 TLS 连接，值被忽略）</div>
                </div>
              </template>
              <div v-else class="help remote-empty">
                execution.remote 未配置（mode=remote 需要 control 地址与证书三件套），WSS
                接入暂不可编辑
              </div>
            </template>
            <!-- harness.agent 嵌套块：base_url/model 成对启用；api_key 不在这里（节点参数引用密钥名） -->
            <template v-if="s.sect === 'harness'">
              <div class="field">
                <label>base_url</label>
                <input
                  v-model="harnessAgent.base_url"
                  placeholder="https://api.deepseek.com/v1"
                  spellcheck="false"
                />
                <div class="help">
                  与 model 成对配置才启用；都留空 = 关闭内置 agent（http(s)、禁 userinfo/query）
                </div>
              </div>
              <div class="field">
                <label>model</label>
                <input
                  v-model="harnessAgent.model"
                  placeholder="deepseek-chat"
                  spellcheck="false"
                />
              </div>
              <div class="field">
                <label>api</label>
                <select v-model="harnessAgent.api">
                  <option :value="null">openai（默认）</option>
                  <option value="openai">openai</option>
                  <option value="anthropic">anthropic</option>
                </select>
                <div class="help">openai = OpenAI 兼容端点；anthropic = Anthropic 端点</div>
              </div>
              <div class="field">
                <label>proxy</label>
                <input
                  v-model="harnessAgent.proxy"
                  placeholder="http://127.0.0.1:7890"
                  spellcheck="false"
                />
                <div class="help">
                  LLM 出口代理（http(s)、禁 userinfo/query——代理凭据不进请求/journal）
                </div>
              </div>
              <div class="field">
                <label>max_turns</label>
                <input
                  v-model.number="harnessAgent.max_turns"
                  type="number"
                  placeholder="50（1..=200）"
                />
              </div>
              <div class="field">
                <label>max_tool_calls_per_turn</label>
                <input
                  v-model.number="harnessAgent.max_tool_calls_per_turn"
                  type="number"
                  placeholder="conga 默认（1..=100）"
                />
              </div>
              <p class="help">输出长度由模型 API 决定，不发送 max_tokens 限制。</p>
              <div class="field">
                <label>tool_timeout_secs</label>
                <input
                  v-model.number="harnessAgent.tool_timeout_secs"
                  type="number"
                  placeholder="无引擎级工具超时（取消/超时由 executor 杀进程组）"
                />
              </div>
            </template>
          </section>
        </template>
      </template>
      <template v-if="activeTab === 'plugins'">
        <section class="card">
          <div class="card-head">
            <h3>插件</h3>
            <AppButton variant="ghost" @click="recheckPlugins">重校验发布定义</AppButton>
          </div>
          <form class="field" @submit.prevent="installPlugin">
            <label for="plugin-source">宿主上的插件目录</label>
            <input
              id="plugin-source"
              v-model="pluginSource"
              placeholder="/absolute/path/to/plugin"
              :disabled="!!pluginPending"
              required
            />
            <label
              ><input v-model="pluginReplace" type="checkbox" :disabled="!!pluginPending" />
              替换已有插件</label
            >
            <AppButton type="submit" :disabled="!!pluginPending || !pluginSource.trim()"
              >安装插件</AppButton
            >
          </form>
          <div v-if="pluginMutationError" role="alert" class="empty">{{ pluginMutationError }}</div>
          <div v-if="pluginPending" class="field">
            <code>请求 ID：{{ pluginPending.requestId }}</code>
            <p>响应未确认时，可使用原请求重试。</p>
            <AppButton :disabled="pluginBusy" @click="runPluginMutation">按原参数重试</AppButton>
            <AppButton
              variant="ghost"
              :disabled="pluginBusy"
              @click="
                pluginPending = null;
                pluginMutationError = '';
              "
              >结束本次操作</AppButton
            >
          </div>
          <div v-if="pluginError" class="empty">插件清单加载失败：{{ pluginError }}</div>
          <div v-else-if="plugins.length === 0" class="empty">
            暂无已部署插件（plugins 目录见运维文档）。
          </div>
          <table v-else class="table">
            <thead>
              <tr>
                <th>插件</th>
                <th>版本</th>
                <th>节点类型</th>
                <th>状态</th>
                <th>配置项</th>
                <th>操作</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="p in plugins" :key="p.source">
                <td>
                  <code>{{ p.id }}</code>
                </td>
                <td>{{ p.version }}</td>
                <td>{{ (p.node_types ?? []).join(", ") }}</td>
                <td>
                  {{
                    p.load_state === "broken"
                      ? "损坏"
                      : p.load_state === "conflict"
                        ? "冲突"
                        : "已加载"
                  }}{{ p.running ? " · 运行中" : "" }}{{ p.draining ? " · 排空中" : "" }}
                  <div v-if="p.load_error">{{ p.load_error }}</div>
                  <div v-if="p.grant_state">授予：{{ p.grant_state }}</div>
                  <div v-if="p.entry_contained === false">入口位于插件目录外</div>
                  <div v-if="p.entry_symlink">入口是符号链接</div>
                  <div v-if="p.entry_oversize">入口超过 256 MiB</div>
                </td>
                <td>{{ (p.config_fields ?? []).map((f) => f.key).join(", ") || "—" }}</td>
                <td>
                  <AppButton
                    variant="ghost"
                    :disabled="!!pluginPending || p.id_matches_directory === false"
                    @click="uninstallPlugin(p)"
                    >{{ p.running ? "停止并卸载" : "卸载" }}</AppButton
                  >
                  <div v-if="p.id_matches_directory === false">
                    目录名与插件 ID 不一致，请手工处理：{{ p.source }}
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
          <div v-if="pluginViolations && pluginViolations.length > 0" class="empty">
            {{ pluginViolations.length }} 处已发布定义与当前插件不符（详见运行日志）：
            <ul>
              <li v-for="v in pluginViolations.slice(0, 5)" :key="`${v.workflow_id}:${v.node_id}`">
                {{ v.workflow_id }} v{{ v.version }} · {{ v.node_id }}（{{ v.node_type }}）：{{
                  v.reason
                }}
              </li>
            </ul>
          </div>
          <div v-else-if="pluginViolations" class="env-tag">全部已发布定义与当前插件一致</div>
        </section>

        <ResidentServicesPanel ref="residentPanel" />
      </template>
    </div>
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
  max-width: 1080px;
}

.settings-tabs {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  border-bottom: 1px solid var(--border);
  padding-bottom: 8px;
}

.settings-tabs button[aria-selected="true"] {
  color: var(--text);
  border-color: var(--signal);
  background: var(--surface2);
}

.settings-panel {
  display: flex;
  flex-direction: column;
  gap: 14px;
  min-width: 0;
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
  border-color: var(--signal);
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

.agents {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.agent-row {
  display: grid;
  grid-template-columns: 1fr 1fr auto;
  gap: 6px;
  align-items: center;
}

/* 已废弃字段只读回显 */
.field input.deprecated {
  color: var(--text3);
}

.remote-empty {
  margin: 4px 0 0;
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
</style>
