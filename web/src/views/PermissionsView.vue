<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, watch } from "vue";
import { onBeforeRouteLeave, onBeforeRouteUpdate } from "vue-router";
import * as api from "../api/flow";
import { errText } from "../rpc/client";
import { confirmDiscardIfDirty, dirty, editor, refreshWorkflows } from "../state/editor";
import { capabilityActionLabel, nodeTypeLabel } from "../state/labels";
import { staleGuard } from "../state/stale";
import type { EffectiveCapability, PermissionsEffective } from "../types";
import AppButton from "../components/AppButton.vue";

const props = defineProps<{ workflowId: string }>();
const scope = staleGuard();
const loading = ref(false);
const error = ref("");
const version = ref(0);
const publishedVersion = ref<number | null>(null);
const effective = ref<PermissionsEffective | null>(null);
const editRoute = computed(() => ({
  path: `/workflows/${props.workflowId}`,
  query: { edit: "permissions" },
}));
const workflowName = computed(
  () => editor.workflows.find((w) => w.workflow_id === props.workflowId)?.name ?? "",
);

async function load(): Promise<void> {
  const token = scope.begin();
  loading.value = true;
  error.value = "";
  try {
    if (editor.workflows.length === 0) await refreshWorkflows();
    if (scope.stale(token)) return;
    const workflow = await api.getWorkflow(props.workflowId);
    if (scope.stale(token)) return;
    const policy = await api.permissionsEffective({
      workflow_id: props.workflowId,
      version: workflow.version,
    });
    if (scope.stale(token)) return;
    version.value = workflow.version;
    publishedVersion.value = workflow.published_version;
    effective.value = policy;
  } catch (e) {
    if (!scope.stale(token)) error.value = errText(e);
  } finally {
    if (!scope.stale(token)) loading.value = false;
  }
}

function derivedLabel(derived: string | undefined): string {
  switch (derived) {
    case "not_granted_by_workflow":
      return "工作流未授予";
    case "requires_approval":
      return "需审批";
    case "auto":
      return "免审批";
    default:
      return derived ?? "";
  }
}

function capBrief(cap: EffectiveCapability): string {
  const derived = cap.derived ? `（${derivedLabel(cap.derived)}）` : "";
  return `${cap.id} · ${capabilityActionLabel(String(cap.action))} · ${
    cap.approval === "auto" ? "免审批" : "需审批"
  }${derived}`;
}

function hasEditorDraft(): boolean {
  return editor.workflowId === props.workflowId && dirty.value;
}

onBeforeRouteLeave((to) => to.path === editRoute.value.path ||
  !hasEditorDraft() || confirmDiscardIfDirty());
onBeforeRouteUpdate((to) => to.params.id === props.workflowId ||
  !hasEditorDraft() || confirmDiscardIfDirty());

function onBeforeUnload(event: BeforeUnloadEvent): void {
  if (!hasEditorDraft()) return;
  event.preventDefault();
  event.returnValue = "";
}

onMounted(() => {
  window.addEventListener("beforeunload", onBeforeUnload);
  void load();
});
onUnmounted(() => window.removeEventListener("beforeunload", onBeforeUnload));

// RouterView 复用实例，切换工作流时清除上一个版本的诊断。
watch(
  () => props.workflowId,
  (id, prev) => {
    if (!id || id === prev) return;
    scope.invalidate();
    version.value = 0;
    publishedVersion.value = null;
    effective.value = null;
    void load();
  },
);
</script>

<template>
  <div class="page">
    <div class="page-header">
      <h2>
        权限详情
        <span class="muted">{{ workflowName || workflowId }}</span>
        <span v-if="version" class="muted">
          v{{ version }} · {{ version === publishedVersion ? "已发布" : "已保存草稿" }}
        </span>
      </h2>
      <div class="header-actions">
        <RouterLink v-if="!effective" class="link" :to="editRoute">编辑工作流权限</RouterLink>
        <AppButton :disabled="loading" @click="load()">
          {{ loading ? "加载中…" : "刷新" }}
        </AppButton>
        <!-- 工作流子页互跳 -->
        <RouterLink class="link" :to="`/workflows/${workflowId}/versions`">版本</RouterLink>
        <RouterLink class="link" :to="`/workflows/${workflowId}/triggers`">触发器</RouterLink>
        <RouterLink class="link" :to="`/workflows/${workflowId}/runs`">运行记录</RouterLink>
        <RouterLink class="link" :to="`/workflows/${workflowId}`">返回编辑器</RouterLink>
      </div>
    </div>

    <p v-if="loading && !effective" class="wf-empty">加载中…</p>
    <p v-if="error" class="load-error" role="alert">{{ error }}</p>
    <section v-if="effective" class="section">
      <div class="section-header">
        <h3>工作流能力</h3>
        <RouterLink class="link" :to="editRoute">编辑工作流权限</RouterLink>
      </div>
      <p v-if="effective.legacy" class="muted">
        此版本未声明工作流权限。
      </p>
      <p v-else-if="!effective.workflow.length" class="muted">能力列表为空。</p>
      <details v-for="cap in effective.workflow" :key="cap.id" class="workflow-capability">
        <summary>{{ capBrief(cap) }}</summary>
        <pre>{{ JSON.stringify(cap, null, 2) }}</pre>
      </details>
    </section>

    <!-- 诊断与派生状态（permissions.effective，只读） -->
    <section v-if="effective" class="section">
      <div class="section-header">
        <h3>版本诊断（v{{ version }}）</h3>
      </div>
      <p class="muted eff-meta">
        策略摘要 <code class="mono">{{ effective.policy_digest.slice(0, 12) }}…</code>
        · 模式 {{ effective.mode }}
        <template v-if="effective.start_gate"> · 启动门 {{ effective.start_gate }}</template>
        <template v-if="effective.legacy"> · <span class="eff-legacy">legacy</span></template>
      </p>
      <ul v-if="effective.diagnostics.length > 0" class="eff-diagnostics">
        <li v-for="d in effective.diagnostics" :key="d">{{ d }}</li>
      </ul>
      <table v-if="effective.nodes.length > 0" class="data-table">
        <thead>
          <tr>
            <th>节点</th>
            <th>类型</th>
            <th>模式</th>
            <th>副作用</th>
            <th>派生能力</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="n in effective.nodes" :key="n.node_id">
            <td class="mono">{{ n.node_id }}</td>
            <td>{{ nodeTypeLabel(n.node_type) }}</td>
            <td>{{ n.mode === "restrict" ? "限制" : "继承" }}</td>
            <td>
              <span v-if="n.side_effect" class="badge run-awaiting">副作用</span>
              <span v-else class="muted">—</span>
            </td>
            <td>
              <div v-if="n.allow.length === 0" class="muted">（无）</div>
              <div v-for="cap in n.allow" :key="cap.id" class="eff-cap">{{ capBrief(cap) }}</div>
            </td>
          </tr>
        </tbody>
      </table>
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

.load-error { color: var(--danger); }
.workflow-capability { margin: 6px 0; }
.workflow-capability summary { cursor: pointer; }
.workflow-capability pre { overflow-x: auto; }

.eff-meta {
  margin: 0 0 8px;
  font-size: 12px;
}

.eff-legacy {
  color: var(--warn);
}

.eff-diagnostics {
  margin: 0 0 12px;
  padding-left: 16px;
  color: var(--text2);
  font-size: 12px;
  line-height: 1.7;
}

.eff-cap {
  font-size: 11px;
  color: var(--text2);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 420px;
}
</style>
