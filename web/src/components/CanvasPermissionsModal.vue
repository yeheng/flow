<script setup lang="ts">
import { computed, nextTick, onUnmounted, ref, useId, watch } from "vue";
import { CAPABILITY_ACTIONS, newCapability, validateCapabilities } from "../state/capabilities";
import { deepCloneJson } from "../state/clone";
import { editor, validateNow } from "../state/editor";
import { commit } from "../state/history";
import { capabilityActionLabel } from "../state/labels";
import { useModalFocusTrap } from "../state/modal-a11y";
import { overlaysOf } from "../state/overlays";
import type { Capability, NodePermissions, WorkflowPermissions } from "../types";
import AppButton from "./AppButton.vue";
import CapabilityEditor from "./CapabilityEditor.vue";

const entry = computed(() => overlaysOf<{ nodeId?: string }>("canvas-permissions")[0]);
const node = computed(() => editor.nodes.find((n) => n.id === entry.value?.payload.nodeId));
const isNode = computed(() => entry.value?.payload.nodeId !== undefined);
const open = computed(() => !!entry.value);
const titleId = useId();
const dialogEl = ref<HTMLElement | null>(null);
const workflowMode = ref<WorkflowPermissions["mode"]>("execute");
const nodeMode = ref<"inherit" | "restrict">("inherit");
const rows = ref<{ key: number; capability: Capability }[]>([]);
const allow = computed(() => rows.value.map((row) => row.capability));
const action = ref("net");
const needsAllow = computed(() => !isNode.value || nodeMode.value === "restrict");
const errors = computed(() => needsAllow.value ? validateCapabilities(allow.value) : []);
let previousFocus: HTMLElement | null = null;
let workflowId: string | null;
let nextRowKey = 0;

useModalFocusTrap(dialogEl, open);

watch(entry, async (current) => {
  if (!current) {
    previousFocus?.focus();
    previousFocus = null;
    return;
  }
  previousFocus = document.activeElement as HTMLElement | null;
  workflowId = editor.workflowId;
  const workflow = editor.definitionOptions.permissions;
  const permissions = node.value?.data.permissions;
  workflowMode.value = workflow?.mode ?? "execute";
  nodeMode.value = permissions?.mode ?? "inherit";
  const capabilities = isNode.value
    ? permissions?.mode === "restrict" ? permissions.allow : []
    : workflow?.allow ?? [];
  rows.value = capabilities.map((capability) => ({
    key: nextRowKey++, capability: deepCloneJson(capability),
  }));
  action.value = "net";
  await nextTick();
  if (entry.value?.id === current.id) dialogEl.value?.querySelector("select")?.focus();
}, { immediate: true });

onUnmounted(() => entry.value?.close());

function apply(): void {
  if (errors.value.length || workflowId !== editor.workflowId) return;
  if (isNode.value) {
    if (!node.value) return;
    const previous = node.value.data.permissions;
    const next: NodePermissions = nodeMode.value === "inherit"
      ? { mode: "inherit" }
      : { mode: "restrict", allow: deepCloneJson(allow.value) };
    if (JSON.stringify(previous ?? { mode: "inherit" }) !== JSON.stringify(next)) {
      commit();
      node.value.data.permissions = next;
    }
  } else {
    const next: WorkflowPermissions = {
      version: 1, mode: workflowMode.value, allow: deepCloneJson(allow.value),
    };
    if (JSON.stringify(editor.definitionOptions.permissions) !== JSON.stringify(next)) {
      commit();
      editor.definitionOptions.permissions = next;
    }
  }
  validateNow();
  entry.value?.close();
}

function addCapability(): void {
  rows.value.push({
    key: nextRowKey++,
    capability: newCapability(action.value, allow.value.map((cap) => cap.id)),
  });
}
</script>

<template>
  <div v-if="entry" class="permissions-overlay" @click.self="entry.close()">
    <section ref="dialogEl" class="permissions-dialog" role="dialog" aria-modal="true" :aria-labelledby="titleId">
      <h3 :id="titleId">{{ isNode ? "编辑节点权限" : "编辑工作流权限" }}</h3>
      <div class="permissions-body">
        <template v-if="isNode">
          <p class="muted">{{ node?.data.name || node?.id }} · 节点只能收紧工作流权限。</p>
          <p v-if="workflowMode === 'bypass'" class="muted">当前工作流跳过权限检查，节点限制在执行或规划模式下生效。</p>
        </template>
        <label v-if="isNode">权限模式
          <select v-model="nodeMode">
            <option value="inherit">继承工作流</option>
            <option value="restrict">限制本节点</option>
          </select>
        </label>
        <label v-else>权限模式
          <select v-model="workflowMode">
            <option value="execute">执行</option>
            <option value="plan">规划（只读）</option>
            <option value="bypass">跳过权限检查</option>
          </select>
        </label>
        <template v-if="needsAllow">
          <p v-if="allow.length === 0 && (isNode || workflowMode !== 'bypass')" class="muted">
            能力列表为空时拒绝受管外部操作。
          </p>
          <CapabilityEditor
            v-for="(row, index) in rows"
            :key="row.key"
            :capability="row.capability"
            @update:capability="row.capability = $event"
            @remove="rows.splice(index, 1)"
          />
          <div class="permission-actions">
            <select v-model="action" aria-label="新增能力动作">
              <option v-for="item in CAPABILITY_ACTIONS" :key="item" :value="item">{{ capabilityActionLabel(item) }}</option>
            </select>
            <AppButton @click="addCapability">添加能力</AppButton>
          </div>
          <ul v-if="errors.length" class="permission-errors" role="alert">
            <li v-for="error in errors" :key="error">{{ error }}</li>
          </ul>
        </template>
      </div>
      <footer class="permission-actions">
        <span class="muted">应用到画布，随工作流保存和发布。</span>
        <AppButton @click="entry.close()">取消</AppButton>
        <AppButton variant="primary" :disabled="errors.length > 0" @click="apply">应用到画布</AppButton>
      </footer>
    </section>
  </div>
</template>

<style scoped>
.permissions-overlay {
  position: fixed;
  inset: 0;
  z-index: 81;
  background: rgb(0 0 0 / 50%);
  display: grid;
  place-items: center;
}
.permissions-dialog {
  display: flex;
  flex-direction: column;
  width: min(760px, calc(100vw - 40px));
  max-height: 82vh;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: var(--radius);
  box-shadow: 0 8px 32px var(--shadow-hover);
}
.permissions-dialog h3,
.permissions-body,
.permissions-dialog footer {
  margin: 0;
  padding: 12px 16px;
}
.permissions-body {
  display: grid;
  gap: 12px;
  overflow-y: auto;
  border-block: 1px solid var(--border);
}
.permissions-body p { margin: 0; }
.permissions-body label, .permission-actions { display: flex; gap: 8px; align-items: center; }
.permissions-dialog footer { justify-content: flex-end; }
.permissions-dialog footer span { margin-right: auto; }
.permission-errors { color: var(--danger); margin: 0; }
</style>
