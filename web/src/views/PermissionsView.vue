<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import * as api from "../api/flow";
import { errText } from "../rpc/client";
import { deepCloneJson } from "../state/clone";
import { dirty, editor, refreshWorkflows } from "../state/editor";
import {
  CAPABILITY_ACTIONS,
  definitionHasSideEffectNodes,
  newCapability,
  validateCapabilities,
} from "../state/capabilities";
import { capabilityActionLabel, nodeTypeLabel } from "../state/labels";
import { confirmDialog } from "../state/modal";
import { staleGuard } from "../state/stale";
import { toast } from "../state/toast";
import type {
  Capability,
  Definition,
  EffectiveCapability,
  PermissionsEffective,
  WorkflowPermissions,
} from "../types";
import AppButton from "../components/AppButton.vue";
import CapabilityEditor from "../components/CapabilityEditor.vue";

/**
 * 权限页：工作流顶层 permissions 的编辑面（mode + CapabilityEditor 列表），
 * 保存走 workflow.update（definition 顶层 permissions）。诊断与节点派生状态
 * 来自 permissions.effective 的只读投影。节点级 Restrict 在编辑器参数弹窗里改。
 */
const props = defineProps<{ workflowId: string }>();

const scope = staleGuard();
const loading = ref(false);
const saving = ref(false);
const definition = ref<Definition | null>(null);
const version = ref(0);
/** 顶层权限草稿：null = legacy（定义未声明权限块） */
const perms = ref<WorkflowPermissions | null>(null);
const effective = ref<PermissionsEffective | null>(null);
/** 新增能力的动作选择（默认联网） */
const newAction = ref("net");

const workflowName = computed(
  () => editor.workflows.find((w) => w.workflow_id === props.workflowId)?.name ?? "",
);

const isLegacy = computed(() => perms.value === null);
const allow = computed<Capability[]>(() => perms.value?.allow ?? []);
const mode = computed<"execute" | "plan">(() => perms.value?.mode ?? "execute");
/** 定义里含副作用节点（legacy 横幅的严重级别判定） */
const hasSideEffect = computed(() => definitionHasSideEffectNodes(definition.value));
/** 保存前本地校验（服务端权威） */
const errors = computed(() => (perms.value ? validateCapabilities(perms.value.allow) : []));

async function load(): Promise<void> {
  const token = scope.begin();
  loading.value = true;
  try {
    if (editor.workflows.length === 0) await refreshWorkflows();
    const w = await api.getWorkflow(props.workflowId);
    if (scope.stale(token)) return;
    definition.value = w.definition;
    version.value = w.version;
    perms.value = w.definition.permissions ? deepCloneJson(w.definition.permissions) : null;
    const eff = await api.permissionsEffective({
      workflow_id: props.workflowId,
      version: w.version,
    });
    if (!scope.stale(token)) effective.value = eff;
  } catch (e) {
    if (!scope.stale(token)) toast.error(errText(e));
  } finally {
    if (!scope.stale(token)) loading.value = false;
  }
}

/** 一键迁移：创建空权限块（进入草稿，随「保存」走 workflow.update 落库） */
function createPermissionsBlock(): void {
  perms.value = { version: 1, mode: "execute", allow: [] };
}

function setMode(m: "execute" | "plan"): void {
  if (!perms.value || perms.value.mode === m) return;
  perms.value = { ...perms.value, mode: m };
}

function setAllow(next: Capability[]): void {
  if (!perms.value) return;
  perms.value = { ...perms.value, allow: next };
}

function updateCap(index: number, cap: Capability): void {
  const next = [...allow.value];
  next[index] = cap;
  setAllow(next);
}

function removeCap(index: number): void {
  setAllow(allow.value.filter((_, i) => i !== index));
}

function addCap(): void {
  setAllow([
    ...allow.value,
    newCapability(newAction.value, allow.value.map((c) => c.id)),
  ]);
}

async function save(): Promise<void> {
  if (saving.value || !definition.value || !perms.value) return;
  if (errors.value.length > 0) {
    toast.error(`权限块有 ${errors.value.length} 处校验错误，请先修复`);
    return;
  }
  // 编辑器画布开着同一工作流且有未保存修改：这里保存会分叉出新版本，先确认
  if (editor.workflowId === props.workflowId && dirty.value) {
    const ok = await confirmDialog(
      "编辑器画布上有该工作流的未保存修改：在权限页保存会生成新版本，画布稍后需重新加载。确定继续？",
    );
    if (!ok) return;
  }
  const token = scope.mark();
  saving.value = true;
  try {
    const next: Definition = {
      ...deepCloneJson(definition.value),
      permissions: deepCloneJson(perms.value),
    };
    const v = await api.updateWorkflow(props.workflowId, next);
    if (scope.stale(token)) return;
    definition.value = next;
    version.value = v;
    toast.success(`权限已保存（生成 v${v}）`);
    await refreshWorkflows();
    const eff = await api.permissionsEffective({ workflow_id: props.workflowId, version: v });
    if (!scope.stale(token)) effective.value = eff;
  } catch (e) {
    if (!scope.stale(token)) toast.error(errText(e));
  } finally {
    if (!scope.stale(token)) saving.value = false;
  }
}

/** 派生标注 → 中文（permissions.effective 节点条目的 derived） */
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

onMounted(() => void load());

// RouterView 无 key：跨工作流复用实例，参数变化时重置草稿并按新 id 重拉
watch(
  () => props.workflowId,
  (id, prev) => {
    if (!id || id === prev) return;
    scope.invalidate();
    definition.value = null;
    version.value = 0;
    perms.value = null;
    effective.value = null;
    void load();
  },
);
</script>

<template>
  <div class="page">
    <div class="page-header">
      <h2>
        权限
        <span class="muted">{{ workflowName || workflowId }}</span>
        <span v-if="version" class="muted">v{{ version }}</span>
      </h2>
      <div class="header-actions">
        <AppButton :disabled="loading" @click="load()">
          {{ loading ? "加载中…" : "刷新" }}
        </AppButton>
        <!-- 工作流子页互跳 -->
        <RouterLink class="link" :to="`/workflows/${workflowId}/versions`">版本</RouterLink>
        <RouterLink class="link" :to="`/workflows/${workflowId}/triggers`">触发器</RouterLink>
        <RouterLink class="link" :to="`/workflows/${workflowId}/runs`">运行记录</RouterLink>
        <RouterLink class="link" :to="`/workflows/${workflowId}`">打开编辑器 →</RouterLink>
      </div>
    </div>

    <p v-if="loading && !definition" class="wf-empty">加载中…</p>

    <!-- legacy 横幅：无权限块；含副作用节点时不能启动新 run -->
    <div v-if="!loading && isLegacy" class="legacy-banner" :class="{ severe: hasSideEffect }">
      <p class="legacy-text">
        工作流未声明权限（legacy）。
        <template v-if="hasSideEffect">
          含副作用节点的旧工作流不能启动新 run，请创建权限块并声明所需能力。
        </template>
        <template v-else>建议创建权限块，声明工作流允许的外部能力。</template>
      </p>
      <AppButton variant="primary" @click="createPermissionsBlock()">创建权限块</AppButton>
    </div>

    <template v-if="perms">
      <section class="section">
        <div class="section-header">
          <h3>工作流能力（顶层 allow）</h3>
          <AppButton variant="primary" :disabled="saving || errors.length > 0" @click="save()">
            {{ saving ? "保存中…" : "保存" }}
          </AppButton>
        </div>
        <div class="mode-row" role="group" aria-label="权限模式">
          <label class="mode-radio">
            <input
              type="radio"
              name="wf-perm-mode"
              :checked="mode === 'execute'"
              @change="setMode('execute')"
            />
            执行（execute）
          </label>
          <label class="mode-radio">
            <input
              type="radio"
              name="wf-perm-mode"
              :checked="mode === 'plan'"
              @change="setMode('plan')"
            />
            规划（plan）
          </label>
          <span class="muted mode-help">plan 只规划不执行副作用动作</span>
        </div>

        <p v-if="allow.length === 0" class="allow-empty">
          allow 为空：不放行任何外部能力（节点级声明除外）
        </p>
        <CapabilityEditor
          v-for="(cap, i) in allow"
          :key="`wf-cap:${i}:${cap.id}`"
          :capability="cap"
          @update:capability="updateCap(i, $event)"
          @remove="removeCap(i)"
        />
        <div class="perm-add">
          <select v-model="newAction" aria-label="新增能力动作">
            <option v-for="a in CAPABILITY_ACTIONS" :key="a" :value="a">
              {{ capabilityActionLabel(a) }}
            </option>
          </select>
          <AppButton @click="addCap()">添加能力</AppButton>
        </div>

        <ul v-if="errors.length > 0" class="perm-errors" role="alert">
          <li v-for="e in errors" :key="e">{{ e }}</li>
        </ul>
        <p class="muted perm-note">保存走 workflow.update：以当前最新定义生成新版本，需发布后才会被运行使用。</p>
      </section>
    </template>

    <!-- 诊断与派生状态（permissions.effective，只读） -->
    <section v-if="effective" class="section">
      <div class="section-header">
        <h3>生效状态（v{{ version }}）</h3>
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

/* legacy 横幅：默认提醒；含副作用节点升级为阻止级（不能启动新 run） */
.legacy-banner {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
  padding: 12px 14px;
  margin-bottom: 20px;
  border: 1px solid var(--warn);
  border-radius: var(--radius);
  background: rgba(210, 153, 34, 0.08);
}

.legacy-banner.severe {
  border-color: var(--danger);
  background: color-mix(in srgb, var(--danger) 10%, transparent);
}

.legacy-text {
  margin: 0;
  flex: 1;
  min-width: 260px;
  font-size: 13px;
  line-height: 1.6;
  color: var(--text2);
}

.mode-row {
  display: flex;
  gap: 16px;
  align-items: center;
  margin-bottom: 12px;
}

.mode-radio {
  display: flex;
  gap: 4px;
  align-items: center;
  font-size: 12px;
  color: var(--text2);
  cursor: pointer;
}

.mode-help {
  font-size: 11px;
}

.allow-empty {
  margin: 0 0 10px;
  padding: 8px 12px;
  border: 1px dashed var(--warn);
  border-radius: 6px;
  color: var(--text2);
  font-size: 12px;
}

.perm-add {
  display: flex;
  gap: 8px;
  align-items: center;
  margin-top: 10px;
}

.perm-errors {
  margin: 10px 0 0;
  padding-left: 16px;
  color: var(--danger);
  font-size: 12px;
  line-height: 1.6;
}

.perm-note {
  margin: 8px 0 0;
  font-size: 11px;
}

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
