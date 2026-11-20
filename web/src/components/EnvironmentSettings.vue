<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from "vue";
import * as api from "../api/flow";
import { client, errText } from "../rpc/client";
import { refreshSecrets } from "../state/editor";
import {
  environments,
  refreshEnvironments,
  saveEnvironment,
  selectedEnvironment,
  type Environment,
} from "../state/environments";
import { confirmDialog, formDialog } from "../state/modal";
import { toast } from "../state/toast";
import type { SecretInfo } from "../types";
import AppButton from "./AppButton.vue";

const props = withDefaults(
  defineProps<{
    mode: "environments" | "variables" | "secrets";
    overrides?: string[];
  }>(),
  { overrides: () => [] },
);
defineEmits<{ "manage-environments": [] }>();

const scope = ref("default");
const environment = computed(() => environments.value.find((item) => item.id === scope.value));
const defaultSecrets = ref<SecretInfo[]>([]);
const defaultScope = computed(() => props.mode === "secrets" && scope.value === "default");
const environmentLoading = ref(true);
const secretsLoading = ref(false);
const environmentError = ref("");
const secretsError = ref("");
const mutationError = ref("");
const busy = ref(false);
const search = ref("");
const lifetime = new AbortController();
onUnmounted(() => lifetime.abort());

const rows = computed<SecretInfo[]>(() =>
  defaultScope.value
    ? defaultSecrets.value
    : (environment.value?.secrets ?? []).map((name) => ({ name, source: "environment" })),
);
const query = computed(() => search.value.trim().toLowerCase());
const matches = (value: string) => value.toLowerCase().includes(query.value);
const visibleEnvironments = computed(() =>
  environments.value.filter((item) => matches(item.name) || matches(item.id)),
);
const variables = computed(() => Object.entries(environment.value?.config ?? {}));
const visibleVariables = computed(() => variables.value.filter(([key]) => matches(key)));
const visibleSecrets = computed(() => rows.value.filter(({ name }) => matches(name)));
const loading = computed(() =>
  defaultScope.value ? secretsLoading.value : environmentLoading.value,
);
const loadError = computed(() =>
  defaultScope.value ? secretsError.value : environmentError.value,
);
const heading = computed(
  () =>
    ({
      environments: "运行环境",
      variables: "环境变量",
      secrets: "密钥",
    })[props.mode],
);

function syncScope(): void {
  if (environment.value) return;
  scope.value = props.mode === "variables" ? (environments.value[0]?.id ?? "default") : "default";
}

async function loadEnvironments(): Promise<void> {
  environmentLoading.value = true;
  environmentError.value = "";
  try {
    await refreshEnvironments();
    syncScope();
  } catch (error) {
    environmentError.value = errText(error);
  } finally {
    environmentLoading.value = false;
  }
}

async function loadDefaultSecrets(): Promise<void> {
  secretsLoading.value = true;
  secretsError.value = "";
  try {
    defaultSecrets.value = await api.listSecrets();
  } catch (error) {
    secretsError.value = errText(error);
  } finally {
    secretsLoading.value = false;
  }
}

onMounted(() => void loadEnvironments());
watch(
  () => props.mode,
  () => {
    search.value = "";
    mutationError.value = "";
    syncScope();
  },
);
watch(scope, () => {
  search.value = "";
  mutationError.value = "";
});
watch(
  defaultScope,
  (active) => {
    if (active) void loadDefaultSecrets();
  },
  { immediate: true },
);

function refresh(): void {
  void loadEnvironments();
  if (defaultScope.value) void loadDefaultSecrets();
}

async function mutate(action: () => Promise<unknown>, message: string): Promise<void> {
  if (busy.value) return;
  busy.value = true;
  mutationError.value = "";
  try {
    await action();
    toast.success(message);
  } catch (error) {
    mutationError.value = errText(error);
  } finally {
    busy.value = false;
  }
}

async function editEnvironment(source?: Environment, duplicate = false): Promise<void> {
  const existing = duplicate ? undefined : source;
  const values = await formDialog(
    existing ? "编辑运行环境" : duplicate ? "复制运行环境" : "新增运行环境",
    [
      ...(!existing ? [{ key: "id", label: "环境 ID", type: "string", required: true }] : []),
      { key: "name", label: "环境名称", type: "string", required: true },
    ],
    {
      values: { name: duplicate ? `${source?.name} 副本` : (source?.name ?? "") },
      validate: (fields) => {
        if (existing) return;
        const id = String(fields.id).trim();
        if (!/^[a-zA-Z0-9_-]{1,64}$/.test(id) || id === "default") {
          return "环境 ID 须为 1–64 个字母、数字、连字符或下划线，且不能为 default";
        }
        if (environments.value.some((item) => item.id === id)) return "该环境 ID 已存在";
      },
      confirmText: "保存环境",
      signal: lifetime.signal,
    },
  );
  if (!values) return;
  const id = existing?.id ?? String(values.id).trim();
  await mutate(
    async () => {
      await saveEnvironment({
        id,
        name: String(values.name).trim(),
        config: source?.config ?? {},
        revision: existing?.revision ?? 0,
      });
      scope.value = id;
    },
    existing ? "环境已更新" : "环境已创建",
  );
}

async function removeEnvironment(item: Environment): Promise<void> {
  if (!(await confirmDialog(`删除环境「${item.name}」及其中的配置和密钥？`, { danger: true })))
    return;
  await mutate(async () => {
    await client.call("environments.delete", { id: item.id, revision: item.revision });
    if (selectedEnvironment.value === item.id) selectedEnvironment.value = "default";
    await refreshEnvironments();
    syncScope();
    void refreshSecrets();
  }, "环境已删除");
}

function variableValue(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function editVariable(key?: string): Promise<void> {
  const item = environment.value;
  if (!item) return;
  const values = await formDialog(
    key === undefined ? "新增环境变量" : `编辑变量「${key}」`,
    [
      ...(key === undefined
        ? [{ key: "name", label: "变量名称", type: "string", required: true }]
        : []),
      {
        key: "value",
        label: "变量值",
        type: "string",
        required: true,
        help: '文本可直接输入。数字、布尔和 JSON 按类型保存；用引号保留文本类型，例如 "123"。',
      },
    ],
    {
      values: { value: key === undefined ? "" : JSON.stringify(item.config[key]) },
      validate: (fields) => {
        if (
          key === undefined &&
          Object.prototype.hasOwnProperty.call(item.config, String(fields.name).trim())
        ) {
          return "该变量已存在，请从列表编辑";
        }
      },
      signal: lifetime.signal,
      confirmText: "保存变量",
    },
  );
  if (!values) return;
  const name = key ?? String(values.name).trim();
  await mutate(
    () =>
      saveEnvironment({
        id: item.id,
        name: item.name,
        revision: item.revision,
        config: { ...item.config, [name]: variableValue(String(values.value)) },
      }),
    "环境变量已保存",
  );
}

async function removeVariable(key: string): Promise<void> {
  const item = environment.value;
  if (!item || !(await confirmDialog(`删除变量「${key}」？`, { danger: true }))) return;
  const config = { ...item.config };
  delete config[key];
  await mutate(
    () =>
      saveEnvironment({
        id: item.id,
        name: item.name,
        revision: item.revision,
        config,
      }),
    "环境变量已删除",
  );
}

function editableSecret(row: SecretInfo): boolean {
  return !defaultScope.value || row.source === "stored";
}

async function writeSecret(
  item: Environment | undefined,
  name: string,
  value: string | null,
): Promise<void> {
  if (item) {
    await saveEnvironment({
      id: item.id,
      name: item.name,
      revision: item.revision,
      config: item.config,
      secret: { name, value },
    });
  } else {
    if (value === null) await api.deleteSecret(name);
    else await api.setSecret(name, value);
    await loadDefaultSecrets();
  }
  void refreshSecrets();
}

async function editSecret(existing?: SecretInfo): Promise<void> {
  const item = environment.value;
  if (!item && !defaultScope.value) return;
  const values = await formDialog(
    existing ? `更新密钥「${existing.name}」` : "新增密钥",
    [
      ...(!existing ? [{ key: "name", label: "密钥名称", type: "string", required: true }] : []),
      { key: "value", label: "新密钥值", type: "string", secret: true, required: true },
    ],
    {
      validate: (fields) => {
        if (existing) return;
        const name = String(fields.name).trim();
        if (!/^[a-zA-Z0-9_]{1,64}$/.test(name)) return "密钥名称须为 1–64 个字母、数字或下划线";
        if (rows.value.some((row) => row.name === name)) return "该密钥名称已存在";
      },
      confirmText: "保存密钥",
      signal: lifetime.signal,
    },
  );
  if (!values) return;
  await mutate(
    () => writeSecret(item, existing?.name ?? String(values.name).trim(), String(values.value)),
    "密钥已保存",
  );
}

async function removeSecret(row: SecretInfo): Promise<void> {
  const item = environment.value;
  if (!editableSecret(row) || !(await confirmDialog(`删除密钥「${row.name}」？`, { danger: true })))
    return;
  await mutate(() => writeSecret(item, row.name, null), "密钥已删除");
}
</script>

<template>
  <section class="catalog">
    <header class="catalog-header">
      <div>
        <h3>{{ heading }}</h3>
        <p v-if="mode === 'environments'">运行启动时绑定环境版本。复制环境仅复制变量配置。</p>
        <p v-else-if="mode === 'variables'">
          通过 <code>${config.变量名}</code> 引用。修改供此环境的新运行使用。
        </p>
        <p v-else>工作流按名称引用密钥。更新密钥时填写新值。</p>
      </div>
      <div class="actions">
        <AppButton :disabled="busy || loading" @click="refresh">刷新</AppButton>
        <AppButton
          v-if="mode === 'environments'"
          variant="primary"
          :disabled="busy || loading || !!loadError"
          @click="editEnvironment()"
          >新增环境</AppButton
        >
        <AppButton
          v-else-if="mode === 'variables'"
          variant="primary"
          :disabled="busy || loading || !!loadError || !environment"
          @click="editVariable()"
          >新增变量</AppButton
        >
        <AppButton
          v-else
          variant="primary"
          :disabled="busy || loading || !!loadError"
          @click="editSecret()"
          >新增密钥</AppButton
        >
      </div>
    </header>

    <div class="catalog-filters">
      <label v-if="mode !== 'environments' && (mode === 'secrets' || environments.length)">
        运行环境
        <select v-model="scope" :disabled="busy">
          <option v-if="mode === 'secrets'" value="default">默认环境</option>
          <option v-for="item in environments" :key="item.id" :value="item.id">
            {{ item.name }} · v{{ item.revision }}
          </option>
        </select>
      </label>
      <input
        v-model="search"
        type="search"
        :aria-label="`搜索${heading}`"
        :placeholder="`搜索${heading}`"
      />
    </div>
    <p v-if="mutationError" class="error" role="alert">{{ mutationError }}</p>
    <p v-if="loadError" class="error" role="alert">{{ loadError }}</p>
    <p v-else-if="loading" class="empty">加载中…</p>

    <template v-else-if="mode === 'environments'">
      <div v-if="visibleEnvironments.length" class="table-scroll">
        <table>
          <thead>
            <tr>
              <th>名称</th>
              <th>环境 ID</th>
              <th>版本</th>
              <th>变量</th>
              <th>密钥</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in visibleEnvironments" :key="item.id">
              <td>{{ item.name }}</td>
              <td>
                <code>{{ item.id }}</code>
              </td>
              <td>v{{ item.revision }}</td>
              <td>{{ Object.keys(item.config).length }}</td>
              <td>{{ item.secrets.length }}</td>
              <td class="row-actions">
                <AppButton variant="link" :disabled="busy" @click="editEnvironment(item)"
                  >编辑</AppButton
                >
                <AppButton variant="link" :disabled="busy" @click="editEnvironment(item, true)"
                  >复制配置</AppButton
                >
                <AppButton variant="link-danger" :disabled="busy" @click="removeEnvironment(item)"
                  >删除</AppButton
                >
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <p v-else class="empty">
        {{ environments.length ? "未找到匹配的环境" : "暂无自定义运行环境" }}
      </p>
    </template>

    <template v-else-if="mode === 'variables'">
      <div v-if="!environment" class="empty">
        <p>创建一个运行环境后即可管理变量。</p>
        <AppButton @click="$emit('manage-environments')">管理运行环境</AppButton>
      </div>
      <div v-else-if="visibleVariables.length" class="table-scroll">
        <table>
          <thead>
            <tr>
              <th>名称</th>
              <th>值</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="[key, value] in visibleVariables" :key="key">
              <td>
                <code>{{ key }}</code>
              </td>
              <td>
                <code class="variable-value">{{ JSON.stringify(value) }}</code>
              </td>
              <td class="row-actions">
                <AppButton variant="link" :disabled="busy" @click="editVariable(key)"
                  >编辑</AppButton
                >
                <AppButton variant="link-danger" :disabled="busy" @click="removeVariable(key)"
                  >删除</AppButton
                >
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <p v-else class="empty">{{ variables.length ? "未找到匹配的变量" : "该环境暂无变量" }}</p>
    </template>

    <template v-else>
      <div v-if="visibleSecrets.length" class="table-scroll">
        <table>
          <thead>
            <tr>
              <th>名称</th>
              <th>来源</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="row in visibleSecrets" :key="row.name">
              <td>
                <code>{{ row.name }}</code>
              </td>
              <td>{{ editableSecret(row) ? "加密存储" : "部署环境变量" }}</td>
              <td v-if="editableSecret(row)" class="row-actions">
                <AppButton variant="link" :disabled="busy" @click="editSecret(row)"
                  >更新值</AppButton
                >
                <AppButton variant="link-danger" :disabled="busy" @click="removeSecret(row)"
                  >删除</AppButton
                >
              </td>
              <td v-else class="muted">由部署管理</td>
            </tr>
          </tbody>
        </table>
      </div>
      <p v-else class="empty">{{ rows.length ? "未找到匹配的密钥" : "该环境暂无密钥" }}</p>
    </template>
  </section>
  <section v-if="mode === 'variables' && overrides.length" class="catalog">
    <header class="catalog-header">
      <div>
        <h3>部署覆盖</h3>
        <p>由 Flow 进程启动时读取，优先于配置文件。</p>
      </div>
    </header>
    <ul class="overrides">
      <li v-for="name in overrides" :key="name">
        <code>{{ name }}</code>
      </li>
    </ul>
  </section>
</template>

<style scoped>
.catalog {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  overflow: hidden;
}
.catalog + .catalog {
  margin-top: 14px;
}
.catalog-header,
.catalog-filters {
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  padding: 14px 16px;
}
.catalog-header h3 {
  margin: 0;
  font-size: 14px;
}
.catalog-header p {
  color: var(--text2);
  margin: 5px 0 0;
  font-size: 12px;
}
.catalog-filters {
  border-top: 1px solid var(--border);
  justify-content: flex-start;
}
.catalog-filters label,
.actions,
.row-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}
.catalog-filters input {
  width: min(240px, 100%);
}
.table-scroll {
  overflow-x: auto;
}
table {
  border-collapse: collapse;
  width: 100%;
  font-size: 13px;
}
th,
td {
  padding: 9px 16px;
  text-align: left;
  border-top: 1px solid var(--border);
}
th {
  color: var(--text2);
  font-size: 12px;
  font-weight: 500;
  background: var(--surface2);
}
.row-actions {
  white-space: nowrap;
}
.variable-value {
  overflow-wrap: anywhere;
  white-space: pre-wrap;
}
.empty,
.error {
  margin: 0;
  padding: 14px 16px;
  font-size: 13px;
}
.empty {
  color: var(--text2);
}
.empty p {
  margin: 0 0 8px;
}
.error {
  color: var(--danger);
}
.overrides {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 20px;
  margin: 0;
  padding: 0 16px 14px;
  list-style: none;
}
</style>
