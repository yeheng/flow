<script setup lang="ts">
import { onMounted, onUnmounted, ref, watch } from "vue";
import { onBeforeRouteLeave, onBeforeRouteUpdate, useRoute, useRouter } from "vue-router";
import {
  autoLayout,
  confirmDiscardIfDirty,
  copySelection,
  deleteSelection,
  dirty,
  editor,
  ensureNodeTypes,
  ensureSecrets,
  latestVersion,
  pasteClipboard,
  publish,
  publishing,
  refreshWorkflows,
  save,
  saving,
  selectWorkflow,
  validationErrors,
  validationUi,
} from "../state/editor";
import { history, redo, undo } from "../state/history";
import { promptDialog } from "../state/modal";
import { monitor, startRun } from "../state/monitor";
import { saveWorkflowAsNodeTemplate } from "../state/templates";
import { toast } from "../state/toast";
import NodePalette from "../components/NodePalette.vue";
import FlowCanvas from "../components/FlowCanvas.vue";
import ParamsModal from "../components/ParamsModal.vue";
import RunPanel from "../components/RunPanel.vue";

const route = useRoute();
const router = useRouter();

const PALETTE_KEY = "flow.palette-collapsed";
const paletteCollapsed = ref(localStorage.getItem(PALETTE_KEY) === "1");
function togglePalette(): void {
  paletteCollapsed.value = !paletteCollapsed.value;
  localStorage.setItem(PALETTE_KEY, paletteCollapsed.value ? "1" : "0");
}

// 抽屉开合不持久化（与节点面板不同）：手动收起后保持收起，直到下一次 run 启动自动展开
const runDrawerOpen = ref(!!monitor.runId);
watch(
  () => monitor.runId,
  (id) => {
    if (id) runDrawerOpen.value = true;
  },
);

async function load(id: string): Promise<void> {
  if (!(await ensureNodeTypes())) return;
  // 密钥名称清单不阻断加载：拉不到时 x-secret 字段仍可手输，重连后自动重试
  void ensureSecrets();
  await refreshWorkflows();
  // refreshWorkflows 失败（连接断开）时名单为空：跳过存在性校验，交给 selectWorkflow 报错
  if (editor.workflows.length > 0 && !editor.workflows.some((w) => w.workflow_id === id)) {
    toast.error("工作流不存在或已删除");
    void router.replace("/workflows");
    return;
  }
  // 已在编辑该工作流（含版本页「加载到编辑器」载入的旧版本）时不重载，保留画布与 undo 栈
  if (editor.workflowId !== id) await selectWorkflow(id);
}

onMounted(() => load(route.params.id as string));

// 同组件参数切换（如浏览器前进/后退）：先过脏检查，再加载新工作流
watch(
  () => route.params.id,
  (id, prev) => {
    if (id && id !== prev && id !== editor.workflowId) void load(id as string);
  },
);

onBeforeRouteUpdate(async (to) => {
  if (to.params.id === route.params.id) return true;
  return confirmDiscardIfDirty();
});

onBeforeRouteLeave(() => confirmDiscardIfDirty());

function isEditableTarget(t: EventTarget | null): boolean {
  return (
    t instanceof HTMLElement && t.closest("input, textarea, select, [contenteditable]") !== null
  );
}

// 快捷键只在编辑器页注册（运行详情等只读页不受影响）；输入框聚焦时不劫持
function onKeydown(e: KeyboardEvent): void {
  // 弹窗打开时吞掉全部快捷键：防删除/撤销/保存穿透到弹窗后面的画布
  if (editor.paramsOpen) {
    if (e.key === "Escape") editor.paramsOpen = false;
    return;
  }
  if (e.key === "Escape" && validationUi.open) {
    validationUi.open = false;
    return;
  }
  if ((e.key === "Delete" || e.key === "Backspace") && !isEditableTarget(e.target)) {
    if (deleteSelection()) e.preventDefault();
    return;
  }
  if (!(e.metaKey || e.ctrlKey)) return;
  const key = e.key.toLowerCase();
  if (key === "s") {
    e.preventDefault();
    void save();
    return;
  }
  if (key === "b") {
    e.preventDefault();
    togglePalette();
    return;
  }
  if (isEditableTarget(e.target)) return;
  if (key === "z" && e.shiftKey) {
    e.preventDefault();
    redo();
  } else if (key === "z") {
    e.preventDefault();
    undo();
  } else if (key === "y") {
    e.preventDefault();
    redo();
  } else if (key === "c") {
    // 无选中时不拦截浏览器默认复制
    if (copySelection()) e.preventDefault();
  } else if (key === "v") {
    if (pasteClipboard()) e.preventDefault();
  }
}

onMounted(() => window.addEventListener("keydown", onKeydown));
onUnmounted(() => window.removeEventListener("keydown", onKeydown));

// 校验错误 popover：点外部或 Esc 关闭（与 CanvasContextMenu 同一模式）
const validationAnchor = ref<HTMLElement>();

function onPointerDownOutside(e: PointerEvent): void {
  if (!validationUi.open) return;
  if (validationAnchor.value?.contains(e.target as Node)) return;
  validationUi.open = false;
}

onMounted(() => window.addEventListener("pointerdown", onPointerDownOutside, true));
onUnmounted(() => window.removeEventListener("pointerdown", onPointerDownOutside, true));

function locateError(nodeId?: string): void {
  if (nodeId) {
    editor.selectedNodeId = nodeId;
    editor.paramsOpen = true;
  }
  validationUi.open = false;
}

/** 一键存为常用节点：把当前工作流打包成 sub_workflow 单节点模板进模板区 */
async function saveAsNode(): Promise<void> {
  if (!editor.workflowId) return;
  const name = await promptDialog(
    "将当前工作流打包为可复用的子流程节点，存入左侧模板区，之后可在其他工作流中直接插入。注意：该工作流需要有已发布版本，才能作为子流程运行。",
    editor.workflowName,
  );
  if (!name?.trim()) return;
  const created = await saveWorkflowAsNodeTemplate(editor.workflowId, name);
  if (created) toast.success("已存为常用节点，可在模板区插入");
}
</script>

<template>
  <div class="editor-page">
    <div class="editor-topbar">
      <button
        class="palette-toggle"
        :title="paletteCollapsed ? '展开节点面板 (⌘B)' : '收起节点面板 (⌘B)'"
        @click="togglePalette()"
      >
        {{ paletteCollapsed ? "»" : "«" }}
      </button>
      <RouterLink class="link" to="/workflows">← 工作流列表</RouterLink>
      <span class="editor-title" :title="editor.workflowName || editor.workflowId || undefined">
        <span class="editor-title-name">{{ editor.workflowName || editor.workflowId }}</span>
        <span v-if="editor.version" class="muted">v{{ editor.version }}</span>
        <span v-if="editor.version > 0 && editor.version < latestVersion" class="muted">
          （基于旧版本，最新 v{{ latestVersion }}）
        </span>
        <span v-if="dirty" class="wf-dirty">（未保存）</span>
      </span>
      <div class="editor-actions">
        <button title="撤销 (⌘Z)" :disabled="!history.canUndo" @click="undo()">↶ 撤销</button>
        <button title="重做 (⌘⇧Z)" :disabled="!history.canRedo" @click="redo()">↷ 重做</button>
        <button :disabled="editor.nodes.length === 0" @click="autoLayout()">自动布局</button>
        <button
          title="把当前工作流打包成可复用的子流程节点（存入模板区）"
          :disabled="!editor.workflowId"
          @click="saveAsNode()"
        >
          存为节点
        </button>
        <span ref="validationAnchor" class="validation-anchor">
          <button
            v-if="validationErrors.length > 0"
            class="validation-badge"
            @click="validationUi.open = !validationUi.open"
          >
            {{ validationErrors.length }} 处错误
          </button>
          <div v-if="validationUi.open && validationErrors.length > 0" class="validation-popover">
            <button
              v-for="(err, i) in validationErrors"
              :key="i"
              type="button"
              class="validation-item"
              :class="{ clickable: !!err.nodeId }"
              @click="locateError(err.nodeId)"
            >
              {{ err.message }}
            </button>
          </div>
        </span>
        <RouterLink
          v-if="editor.workflowId"
          class="link"
          :to="`/workflows/${editor.workflowId}/versions`"
        >
          版本
        </RouterLink>
        <RouterLink
          v-if="editor.workflowId"
          class="link"
          :to="`/workflows/${editor.workflowId}/triggers`"
        >
          触发器
        </RouterLink>
        <RouterLink
          v-if="editor.workflowId"
          class="link"
          :to="`/workflows/${editor.workflowId}/runs`"
        >
          运行记录
        </RouterLink>
        <button title="保存 (⌘S)" :disabled="saving" @click="save()">
          {{ saving ? "保存中…" : "保存" }}
        </button>
        <button :disabled="publishing" @click="publish()">
          {{ publishing ? "发布中…" : "发布" }}
        </button>
        <button class="primary" :disabled="monitor.starting" @click="startRun()">运行</button>
        <button
          :title="runDrawerOpen ? '收起运行控制台' : '展开运行控制台'"
          @click="runDrawerOpen = !runDrawerOpen"
        >
          {{ runDrawerOpen ? "控制台 ▾" : "控制台 ▴" }}
        </button>
      </div>
    </div>
    <main class="editor-main">
      <aside class="left" :class="{ collapsed: paletteCollapsed }">
        <NodePalette />
      </aside>
      <section class="center">
        <FlowCanvas />
      </section>
    </main>
    <section v-if="runDrawerOpen" class="run-drawer">
      <RunPanel @collapse="runDrawerOpen = false" />
    </section>
    <ParamsModal />
  </div>
</template>

<style scoped>
.editor-page {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-height: 0;
}

.editor-main {
  flex: 1;
  display: flex;
  min-height: 0;
}

/* 左侧节点面板：可收起（宽度动画；收起后不占位、不留滚动条） */
aside.left {
  width: 240px;
  flex-shrink: 0;
  transition:
    width 0.18s ease,
    padding 0.18s ease,
    border-color 0.18s ease;
}

aside.left.collapsed {
  width: 0;
  padding-left: 0;
  padding-right: 0;
  border-right-color: transparent;
  overflow: hidden;
}

.palette-toggle {
  width: 28px;
  padding: 0;
  font-size: 14px;
  line-height: 1.1;
}

.center {
  flex: 1;
}

/* 运行抽屉：固定高度、内容内部滚动 */
.run-drawer {
  flex-shrink: 0;
  height: 340px;
  min-height: 0;
  display: flex;
  flex-direction: column;
  border-top: 1px solid var(--border);
  background: var(--surface);
}

.editor-topbar {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 14px;
  row-gap: 6px;
  padding: 8px 12px;
  background: var(--surface);
  border-bottom: 1px solid var(--border);
  flex-shrink: 0;
}

.editor-title {
  font-weight: 600;
  display: flex;
  gap: 8px;
  align-items: baseline;
  min-width: 0;
}

.editor-title-name {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.wf-dirty {
  color: var(--warn);
  font-weight: 400;
}

.editor-actions {
  margin-left: auto;
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  row-gap: 6px;
  align-items: center;
}

/* 校验错误徽章与列表 popover */
.validation-anchor {
  position: relative;
}

.validation-badge {
  color: var(--danger);
  border-color: var(--danger);
}

.validation-popover {
  position: absolute;
  top: calc(100% + 6px);
  right: 0;
  z-index: 50;
  width: 320px;
  max-height: 260px;
  overflow-y: auto;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: 8px;
  box-shadow: 0 8px 32px var(--shadow-hover);
  padding: 6px;
}

.validation-item {
  display: block;
  width: 100%;
  text-align: left;
  padding: 6px 8px;
  border: none;
  background: none;
  border-radius: 6px;
  font-size: 12px;
  font-weight: 400;
  color: var(--text2);
}

.validation-item:not(.clickable) {
  cursor: default;
}

.validation-item:not(.clickable):hover {
  background: none;
}

.validation-item.clickable {
  cursor: pointer;
}

.validation-item.clickable:hover {
  background: var(--surface2);
  color: var(--text);
}
</style>
