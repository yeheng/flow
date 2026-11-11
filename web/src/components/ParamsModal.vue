<script setup lang="ts">
import { computed, nextTick, ref, useId, watch } from "vue";
import { editor, nodeValidationError, selectedNode } from "../state/editor";
import { useModalFocusTrap } from "../state/modal-a11y";
import { commitParams } from "../state/history";
import { overlaysOf, type OverlayEntry } from "../state/overlays";
import { CAPABILITY_ACTIONS, newCapability } from "../state/capabilities";
import { capabilityActionLabel } from "../state/labels";
import { toast } from "../state/toast";
import type { Capability, NodePermissions, WorkflowPermissions } from "../types";
import SchemaField from "./SchemaField.vue";
import AppButton from "./AppButton.vue";
import CapabilityEditor from "./CapabilityEditor.vue";

/** 栈上 params 条目的载荷：nodeId 标识编辑目标（入口：双击节点/右键「编辑参数」/校验定位） */
export interface ParamsPayload {
  nodeId: string;
}

const node = selectedNode;
const data = computed(() => node.value?.data ?? null);
const desc = computed(() => data.value?.nodeType ?? null);
const schema = computed(() => desc.value?.params_schema ?? null);
// 校验错误内联展示：弹层盖住画布，节点上的红框在编辑期间不可见
const invalidMsg = computed(() => (node.value ? nodeValidationError(node.value.id) : null));
const nameInput = ref<HTMLInputElement>();

/** 开 = 栈上有 params 条目且选中节点还在；Esc 由全局弹层栈关栈顶，此处不再监听 */
const entry = computed<OverlayEntry<ParamsPayload> | null>(
  () => overlaysOf<ParamsPayload>("params")[0] ?? null,
);
const open = computed(() => !!entry.value && !!node.value);

const dialogEl = ref<HTMLElement | null>(null);
const titleId = useId();
/** 打开弹窗前的焦点目标（触发按钮/画布节点）：关闭时还原 */
const prevFocus = ref<HTMLElement | null>(null);

// 右键「编辑参数」要直达改名：打开弹窗即聚焦名称输入框；关闭时把焦点还给打开前的元素
watch(
  [open, () => node.value?.id],
  async ([isOpen], [, prevId], onCleanup) => {
    if (!isOpen) {
      const el = prevFocus.value;
      prevFocus.value = null;
      el?.focus();
      return;
    }
    if (prevFocus.value === null) {
      prevFocus.value = (document.activeElement as HTMLElement | null) ?? null;
    }
    let cancelled = false;
    onCleanup(() => (cancelled = true));
    await nextTick();
    if (cancelled) return;
    nameInput.value?.focus();
    // 换节点重开时全选，便于直接覆盖
    if (prevId !== node.value?.id) nameInput.value?.select();
  },
  { immediate: true },
);

function close(): void {
  entry.value?.close();
}

// Tab 焦点圈定在弹层内（Esc 走全局弹层栈）
useModalFocusTrap(dialogEl, open);

function onName(ev: Event): void {
  if (!data.value || !node.value) return;
  // 同字段连续输入合并为一条 undo 历史
  commitParams(`${node.value.id}:__name__`);
  data.value.name = (ev.target as HTMLInputElement).value;
}

function setParam(name: string, value: unknown): void {
  if (!data.value || !node.value) return;
  commitParams(`${node.value.id}:${name}`);
  if (value === undefined) {
    delete data.value.params[name];
  } else {
    data.value.params[name] = value;
  }
}

const retry = computed(
  () => (data.value?.params.retry ?? {}) as { max_attempts?: number; backoff_ms?: number },
);

function setRetry(key: "max_attempts" | "backoff_ms", ev: Event): void {
  const v = (ev.target as HTMLInputElement).valueAsNumber;
  const next: Record<string, unknown> = { ...retry.value, [key]: Number.isNaN(v) ? undefined : v };
  // 两个字段都为空时整体删除 retry（空值语义在写入侧收敛，序列化器不认识具体参数名）
  const clean = Object.fromEntries(Object.entries(next).filter(([, x]) => x !== undefined));
  setParam("retry", Object.keys(clean).length > 0 ? clean : undefined);
}

// ---- 节点权限：Inherit / Restrict（写 node.permissions，definition 保存路径独立于 params） ----

/** 顶层无 permissions 块（legacy）：含副作用节点的旧工作流不能启动新 run */
const workflowLegacy = computed(() => editor.definitionOptions?.permissions === undefined);

/** 一键迁移：顶层写入空权限块（进入 definitionOptions，随画布保存/发布落库） */
function migrateWorkflowPermissions(): void {
  const block: WorkflowPermissions = { version: 1, mode: "execute", allow: [] };
  editor.definitionOptions.permissions = block;
  toast.success("已创建空权限块（allow 暂为空）：保存并发布后生效");
}

const nodePermissions = computed<NodePermissions | null>(
  () => (data.value?.permissions as NodePermissions | undefined) ?? null,
);

/** 缺省视为 inherit（definition 里不落显式块，语义相同） */
const nodeMode = computed<"inherit" | "restrict">(() =>
  nodePermissions.value?.mode === "restrict" ? "restrict" : "inherit",
);

const nodeAllow = computed<Capability[]>(() =>
  nodePermissions.value?.mode === "restrict" ? nodePermissions.value.allow : [],
);

function setNodePermissions(p: NodePermissions): void {
  if (!data.value || !node.value) return;
  // 与 params 同一 undo 通道（history 快照含 permissions），标签独立合并连续编辑
  commitParams(`${node.value.id}:__permissions__`);
  data.value.permissions = p;
}

function onNodeMode(mode: "inherit" | "restrict"): void {
  if (nodeMode.value === mode) return;
  setNodePermissions(
    mode === "inherit"
      ? { mode: "inherit" }
      : { mode: "restrict", allow: nodeAllow.value.map((c) => ({ ...c })) },
  );
}

function updateNodeCap(index: number, cap: Capability): void {
  const allow = [...nodeAllow.value];
  allow[index] = cap;
  setNodePermissions({ mode: "restrict", allow });
}

function removeNodeCap(index: number): void {
  setNodePermissions({ mode: "restrict", allow: nodeAllow.value.filter((_, i) => i !== index) });
}

/** 新增能力的动作选择（默认联网；闭集见 CAPABILITY_ACTIONS） */
const newAction = ref<string>("net");

function addNodeCap(): void {
  const action = newAction.value;
  setNodePermissions({
    mode: "restrict",
    allow: [...nodeAllow.value, newCapability(action, nodeAllow.value.map((c) => c.id))],
  });
}
</script>

<template>
  <div v-if="open" class="params-overlay" @click.self="close">
    <div
      ref="dialogEl"
      class="params-modal"
      role="dialog"
      aria-modal="true"
      :aria-labelledby="titleId"
    >
      <header class="params-header">
        <h3 :id="titleId">编辑参数</h3>
        <span v-if="node && data" class="params-subtitle">
          {{ data.nodeType.label }} · {{ node.id }}
        </span>
        <button class="params-close" title="关闭 (Esc)" @click="close">×</button>
      </header>
      <div class="params-body">
        <template v-if="node && data && desc">
          <p v-if="invalidMsg" role="alert" class="params-invalid">{{ invalidMsg }}</p>
          <div class="field">
            <label>节点 id</label>
            <div class="field-static">{{ node.id }}</div>
          </div>
          <div class="field">
            <label for="node-name">名称</label>
            <input id="node-name" ref="nameInput" type="text" :value="data.name" @input="onName" />
          </div>
          <!-- key 带节点 id：切换节点时强制重建，清掉 json 等字段的本地编辑状态 -->
          <SchemaField
            v-for="(prop, key) in schema?.properties ?? {}"
            :key="`${node.id}:${key}`"
            :name="key"
            :schema="prop"
            :required="schema?.required?.includes(key)"
            :model-value="data.params[key]"
            @update:model-value="setParam(key, $event)"
          />
          <fieldset class="permissions">
            <legend>权限</legend>
            <template v-if="workflowLegacy">
              <p class="retry-warning">
                工作流未声明权限（legacy）：含副作用节点的旧工作流不能启动新 run。
              </p>
              <AppButton @click="migrateWorkflowPermissions()">迁移（创建空权限块）</AppButton>
            </template>
            <div class="perm-mode">
              <label class="perm-radio">
                <input
                  type="radio"
                  name="node-perm-mode"
                  :checked="nodeMode === 'inherit'"
                  @change="onNodeMode('inherit')"
                />
                继承工作流（Inherit）
              </label>
              <label class="perm-radio">
                <input
                  type="radio"
                  name="node-perm-mode"
                  :checked="nodeMode === 'restrict'"
                  @change="onNodeMode('restrict')"
                />
                限制为本节点声明（Restrict）
              </label>
            </div>
            <template v-if="nodeMode === 'restrict'">
              <p v-if="nodeAllow.length === 0" class="perm-empty">
                空列表 = 禁止所有外部能力
              </p>
              <CapabilityEditor
                v-for="(cap, i) in nodeAllow"
                :key="`${node.id}:cap:${i}:${cap.id}`"
                :capability="cap"
                @update:capability="updateNodeCap(i, $event)"
                @remove="removeNodeCap(i)"
              />
              <div class="perm-add">
                <select v-model="newAction" aria-label="新增能力动作">
                  <option v-for="a in CAPABILITY_ACTIONS" :key="a" :value="a">
                    {{ capabilityActionLabel(a) }}
                  </option>
                </select>
                <AppButton @click="addNodeCap()">添加能力</AppButton>
              </div>
            </template>
            <p v-else class="field-help">
              节点沿用工作流顶层能力声明；需要收紧本节点时切换为 Restrict。
            </p>
          </fieldset>
          <fieldset v-if="desc.supports_retry" class="retry">
            <legend>重试</legend>
            <p v-if="desc.retry_scope === 'declared_repeatable_only'" class="retry-warning">
              自动重试只对显式声明可重复（effect_policy=repeatable）的失败生效
            </p>
            <p v-if="desc.side_effect" class="retry-warning">
              该节点会产生外部副作用：自动重试只用于脚本/条件等可安全重算的失败；崩溃恢复后结果未知的操作会挂起等人工裁决，不会自动重发。
            </p>
            <div class="field">
              <label for="retry-max-attempts">最大尝试次数</label>
              <input
                id="retry-max-attempts"
                type="number"
                min="1"
                :value="retry.max_attempts ?? ''"
                placeholder="1"
                @input="setRetry('max_attempts', $event)"
              />
            </div>
            <div class="field">
              <label for="retry-backoff-ms">退避（毫秒）</label>
              <input
                id="retry-backoff-ms"
                type="number"
                min="0"
                :value="retry.backoff_ms ?? ''"
                placeholder="0"
                @input="setRetry('backoff_ms', $event)"
              />
            </div>
          </fieldset>
        </template>
      </div>
      <footer class="params-footer">
        <span class="muted params-hint">修改应用到画布，发布后用于新运行；⌘Z 可撤销</span>
        <AppButton variant="primary" @click="close">完成</AppButton>
      </footer>
    </div>
  </div>
</template>

<style scoped>
.params-overlay {
  position: fixed;
  inset: 0;
  z-index: 80;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  align-items: center;
  justify-content: center;
}

.params-modal {
  width: 65vw;
  max-height: 82vh;
  display: flex;
  flex-direction: column;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: var(--radius);
  box-shadow: 0 8px 32px var(--shadow-hover);
}

.params-header {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 16px;
  border-bottom: 1px solid var(--border);
}

.params-header h3 {
  margin: 0;
  font-size: 14px;
}

.params-subtitle {
  color: var(--text2);
  font-size: 12px;
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.params-close {
  margin-left: auto;
  padding: 0 8px;
  border: none;
  background: none;
  font-size: 18px;
  line-height: 1.2;
  color: var(--text2);
}

.params-close:hover {
  color: var(--text);
  background: var(--surface2);
}

.params-body {
  padding: 14px 16px;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.params-invalid {
  margin: 0;
  padding: 8px 12px;
  border: 1px solid var(--danger);
  border-radius: 8px;
  background: color-mix(in srgb, var(--danger) 12%, transparent);
  color: var(--danger);
  font-size: 12px;
  line-height: 1.5;
}

.params-footer {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 16px;
  border-top: 1px solid var(--border);
}

.params-footer .params-hint {
  margin-right: auto;
  font-size: 12px;
}

fieldset.retry {
  border: 1px solid var(--border);
  border-radius: 6px;
  margin: 0;
  padding: 8px;
}

fieldset.retry legend {
  color: var(--text2);
  font-size: 12px;
}

.retry-warning {
  margin: 6px 0 8px;
  color: var(--warn);
  font-size: 11px;
  line-height: 1.5;
}

fieldset.permissions {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.perm-mode {
  display: flex;
  gap: 16px;
}

.perm-radio {
  display: flex;
  gap: 4px;
  align-items: center;
  font-size: 12px;
  color: var(--text2);
  cursor: pointer;
}

.perm-empty {
  margin: 0;
  padding: 6px 10px;
  border: 1px dashed var(--danger);
  border-radius: 6px;
  color: var(--danger);
  font-size: 11px;
}

.perm-add {
  display: flex;
  gap: 8px;
  align-items: center;
}
</style>
