<script setup lang="ts">
import { ref, onMounted } from "vue";
import {
  environments,
  refreshEnvironments,
  saveEnvironment,
  type Environment,
} from "../state/environments";
import { client, errText } from "../rpc/client";
import { selectedEnvironment } from "../state/environments";
import { confirmDialog } from "../state/modal";
import AppButton from "./AppButton.vue";
const id = ref("");
const name = ref("");
const config = ref("{}");
const revision = ref(0);
const secretName = ref("");
const secretValue = ref("");
const error = ref("");
const busy = ref(false);
const names = ref<string[]>([]);
function edit(e?: Environment) {
  id.value = e?.id ?? "";
  name.value = e?.name ?? "";
  config.value = JSON.stringify(e?.config ?? {}, null, 2);
  revision.value = e?.revision ?? 0;
  names.value = e?.secrets ?? [];
  secretName.value = "";
  secretValue.value = "";
  error.value = "";
}
function duplicate() {
  id.value = "";
  name.value = `${name.value} 副本`;
  revision.value = 0;
  names.value = [];
  secretName.value = "";
  secretValue.value = "";
}
async function remove() {
  if (
    !(await confirmDialog("删除后无法恢复。被运行或触发器引用的环境不能删除。", { danger: true }))
  )
    return;
  busy.value = true;
  error.value = "";
  try {
    const deletedId = id.value;
    await client.call("environments.delete", { id: deletedId, revision: revision.value });
    await refreshEnvironments();
    if (selectedEnvironment.value === deletedId) selectedEnvironment.value = "default";
    edit();
  } catch (e) {
    error.value = errText(e);
  } finally {
    busy.value = false;
  }
}
async function refresh() {
  try {
    await refreshEnvironments();
  } catch (e) {
    error.value = errText(e);
  }
}
async function save(remove = false) {
  busy.value = true;
  error.value = "";
  try {
    const parsed: unknown = JSON.parse(config.value);
    if (!parsed || Array.isArray(parsed) || typeof parsed !== "object")
      throw new Error("配置必须是 JSON 对象");
    await saveEnvironment({
      id: id.value,
      name: name.value,
      revision: revision.value,
      config: parsed,
      ...(secretName.value
        ? { secret: { name: secretName.value, value: remove ? null : secretValue.value } }
        : {}),
    });
    edit(environments.value.find((e) => e.id === id.value));
  } catch (e) {
    error.value = errText(e);
  } finally {
    secretValue.value = "";
    busy.value = false;
  }
}
onMounted(refresh);
</script>
<template>
  <section class="environment-settings">
    <h3>运行环境</h3>
    <p>
      使用 ${config.xxx}
      引用普通配置；复制环境仅复制普通配置。配置在新运行启动时生效。已有运行和重试使用原版本；密钥不会回落到默认环境。
    </p>
    <div class="environment-actions">
      <AppButton :disabled="busy" @click="edit()">新建</AppButton>
      <AppButton v-for="e in environments" :key="e.id" :disabled="busy" @click="edit(e)"
        >{{ e.name }} · v{{ e.revision }}</AppButton
      >
    </div>
    <form @submit.prevent="save()">
      <label
        >环境 ID
        <input
          v-model="id"
          :disabled="revision > 0 || busy"
          required
          placeholder="dev / test / prod"
      /></label>
      <label>名称 <input v-model="name" :disabled="busy" required /></label>
      <label
        >普通配置（JSON）<textarea v-model="config" :disabled="busy" rows="6" spellcheck="false" />
      </label>
      <p>已配置密钥：{{ names.join("、") || "无" }}</p>
      <label>密钥名称 <input v-model="secretName" :disabled="busy" autocomplete="off" /></label>
      <label
        >新密钥值
        <input v-model="secretValue" :disabled="busy" type="password" autocomplete="new-password"
      /></label>
      <div class="environment-actions">
        <AppButton type="submit" :disabled="busy">保存环境</AppButton>
        <AppButton v-if="revision" type="button" :disabled="busy" @click="duplicate"
          >复制配置</AppButton
        >
        <AppButton v-if="revision" type="button" variant="danger" :disabled="busy" @click="remove"
          >删除环境</AppButton
        >
        <AppButton
          type="button"
          :disabled="busy || !secretName || !names.includes(secretName)"
          @click="save(true)"
          >删除指定密钥</AppButton
        >
      </div>
    </form>
    <p v-if="error" role="alert">{{ error }}</p>
  </section>
</template>
<style scoped>
.environment-settings {
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 16px;
  margin: 16px 0;
}
.environment-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
  margin: 12px 0;
}
label {
  display: flex;
  flex-direction: column;
  gap: 6px;
  margin: 10px 0;
}
input,
textarea {
  padding: 8px;
  background: var(--surface);
  color: inherit;
  border: 1px solid var(--border);
  border-radius: 4px;
}
p {
  color: var(--text-muted);
  font-size: 13px;
}
[role="alert"] {
  color: var(--danger);
}
</style>
