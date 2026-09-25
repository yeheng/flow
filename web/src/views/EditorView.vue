<script setup lang="ts">
import { onMounted, onUnmounted, watch } from "vue";
import { onBeforeRouteLeave, onBeforeRouteUpdate, useRoute, useRouter } from "vue-router";
import {
  autoLayout,
  confirmDiscardIfDirty,
  copySelection,
  dirty,
  editor,
  ensureNodeTypes,
  pasteClipboard,
  publish,
  refreshWorkflows,
  save,
  selectWorkflow,
  validationErrors,
  validationUi,
} from "../state/editor";
import { history, redo, undo } from "../state/history";
import { monitor, startRun } from "../state/monitor";
import { toast } from "../state/toast";
import NodePalette from "../components/NodePalette.vue";
import FlowCanvas from "../components/FlowCanvas.vue";
import ParamsPanel from "../components/ParamsPanel.vue";
import RunPanel from "../components/RunPanel.vue";

const route = useRoute();
const router = useRouter();

async function load(id: string): Promise<void> {
  if (!(await ensureNodeTypes())) return;
  await refreshWorkflows();
  // refreshWorkflows 失败（连接断开）时名单为空：跳过存在性校验，交给 selectWorkflow 报错
  if (editor.workflows.length > 0 && !editor.workflows.some((w) => w.workflow_id === id)) {
    toast.error("工作流不存在或已删除");
    void router.replace("/workflows");
    return;
  }
  await selectWorkflow(id);
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
  if (!(e.metaKey || e.ctrlKey)) return;
  const key = e.key.toLowerCase();
  if (key === "s") {
    e.preventDefault();
    void save();
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

function locateError(nodeId?: string): void {
  if (nodeId) editor.selectedNodeId = nodeId;
  validationUi.open = false;
}
</script>

<template>
  <div class="editor-page">
    <div class="editor-topbar">
      <RouterLink class="link" to="/workflows">← 工作流列表</RouterLink>
      <span class="editor-title">
        {{ editor.workflowName || editor.workflowId }}
        <span v-if="editor.version" class="muted">v{{ editor.version }}</span>
        <span v-if="dirty" class="wf-dirty">（未保存）</span>
      </span>
      <div class="editor-actions">
        <button title="撤销 (⌘Z)" :disabled="!history.canUndo" @click="undo()">↶ 撤销</button>
        <button title="重做 (⌘⇧Z)" :disabled="!history.canRedo" @click="redo()">↷ 重做</button>
        <button :disabled="editor.nodes.length === 0" @click="autoLayout()">自动布局</button>
        <span class="validation-anchor">
          <button
            v-if="validationErrors.length > 0"
            class="validation-badge"
            @click="validationUi.open = !validationUi.open"
          >
            {{ validationErrors.length }} 处错误
          </button>
          <div v-if="validationUi.open && validationErrors.length > 0" class="validation-popover">
            <div
              v-for="(err, i) in validationErrors"
              :key="i"
              class="validation-item"
              :class="{ clickable: !!err.nodeId }"
              @click="locateError(err.nodeId)"
            >
              {{ err.message }}
            </div>
          </div>
        </span>
        <RouterLink
          v-if="editor.workflowId"
          class="link"
          :to="`/workflows/${editor.workflowId}/runs`"
        >
          运行记录
        </RouterLink>
        <button @click="save()">保存</button>
        <button @click="publish()">发布</button>
        <button class="primary" :disabled="monitor.starting" @click="startRun()">运行</button>
      </div>
    </div>
    <main class="editor-main">
      <aside class="left">
        <NodePalette />
      </aside>
      <section class="center">
        <FlowCanvas />
      </section>
      <aside class="right">
        <ParamsPanel />
        <RunPanel />
      </aside>
    </main>
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
  display: grid;
  grid-template-columns: 240px 1fr 320px;
  min-height: 0;
}

.editor-topbar {
  display: flex;
  align-items: center;
  gap: 14px;
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
  gap: 8px;
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
  padding: 6px 8px;
  border-radius: 6px;
  font-size: 12px;
  color: var(--text2);
}

.validation-item.clickable {
  cursor: pointer;
}

.validation-item.clickable:hover {
  background: var(--surface2);
  color: var(--text);
}
</style>
