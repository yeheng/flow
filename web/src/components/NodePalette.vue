<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { editor, nodeTypeGroups } from "../state/editor";
import {
  ensureTemplates,
  insertTemplate,
  removeTemplate,
  renameTemplate,
  saveSelectionAsTemplate,
  templates,
} from "../state/templates";
import { confirmDialog, promptDialog } from "../state/modal";
import type { NodeTypeDesc } from "../types";

const groups = nodeTypeGroups;

const selectedCount = computed(() => editor.nodes.filter((n) => n.selected).length);

onMounted(() => {
  void ensureTemplates();
});

function onDragStart(event: DragEvent, nt: NodeTypeDesc): void {
  if (!event.dataTransfer) return;
  if (!editor.workflowId || atLimit(nt)) {
    event.preventDefault();
    return;
  }
  event.dataTransfer.setData("application/flow-node-type", nt.type);
  event.dataTransfer.effectAllowed = "copy";
}

function atLimit(nt: NodeTypeDesc): boolean {
  return !!nt.max_instances && editor.nodes.filter((n) => n.data.nodeType.type === nt.type).length >= nt.max_instances;
}

function onTemplateDragStart(event: DragEvent, id: string): void {
  if (!event.dataTransfer) return;
  event.dataTransfer.setData("application/flow-template-id", id);
  event.dataTransfer.effectAllowed = "copy";
}

/** 点击模板 = 插入到错位空档；拖拽落点由 FlowCanvas 处理 */
async function onClickInsertTemplate(id: string): Promise<void> {
  const n = editor.nodes.length;
  await insertTemplate(id, { x: 160 + (n % 5) * 48, y: 120 + (n % 5) * 48 });
}

const saving = ref(false);
async function onSaveAsTemplate(): Promise<void> {
  const name = await promptDialog(
    `模板名称（保存选中的 ${selectedCount.value} 个节点及其内部连线）`,
    "",
  );
  if (name === null || !name.trim()) return;
  saving.value = true;
  try {
    await saveSelectionAsTemplate(name);
  } finally {
    saving.value = false;
  }
}

async function onDeleteTemplate(id: string, name: string): Promise<void> {
  const ok = await confirmDialog(
    `删除模板「${name}」？已插入的节点不受影响。`,
  );
  if (!ok) return;
  await removeTemplate(id);
}

async function onRenameTemplate(id: string, current: string): Promise<void> {
  const name = await promptDialog("新的模板名称", current);
  if (name === null || !name.trim() || name.trim() === current) return;
  await renameTemplate(id, name);
}
</script>

<template>
  <div class="palette">
    <div class="palette-head">
      <h3>节点</h3>
      <button
        class="palette-save"
        :disabled="selectedCount === 0 || saving"
        title="把选中的节点与连线保存为可复用模板"
        @click="onSaveAsTemplate"
      >
        存为模板
      </button>
    </div>
    <div v-for="g in groups" :key="g.cat" class="palette-group">
      <div class="palette-cat">{{ g.label }}</div>
      <div
        v-for="nt in g.items"
        :key="nt.type"
        class="palette-item"
        :class="[`cat-${nt.category}`, { disabled: !editor.workflowId || atLimit(nt) }]"
        :draggable="!!editor.workflowId && !atLimit(nt)"
        :aria-disabled="!editor.workflowId || atLimit(nt)"
        :data-node-type="nt.type"
        :title="atLimit(nt) ? `${nt.label}：已达数量上限（${nt.max_instances} 个）` : `${nt.label}：拖到画布添加`"
        @dragstart="onDragStart($event, nt)"
      >
        <span class="palette-dot" />
        <span class="palette-name">{{ nt.label }}</span>
        <span class="palette-grip" aria-hidden="true">⠿</span>
      </div>
    </div>

    <div class="palette-group">
      <div class="palette-cat">模板</div>
      <div v-if="templates.list.length === 0" class="palette-empty">
        暂无模板。选中节点后点「存为模板」，可在任意流程复用。
      </div>
      <div
        v-for="t in templates.list"
        :key="t.id"
        class="palette-item palette-template"
        draggable="true"
        :title="`${t.name}（${t.node_count} 个节点）：拖到画布插入，或点击插入`"
        @dragstart="onTemplateDragStart($event, t.id)"
        @click="onClickInsertTemplate(t.id)"
      >
        <span class="palette-dot tpl" />
        <span class="palette-template-name">{{ t.name }}</span>
        <span class="palette-template-actions">
          <button class="tpl-btn" title="重命名" @click.stop="onRenameTemplate(t.id, t.name)">✎</button>
          <button class="tpl-btn danger" title="删除模板" @click.stop="onDeleteTemplate(t.id, t.name)">×</button>
        </span>
      </div>
    </div>
    <p class="palette-hint">拖动节点到画布添加；右键点节点开菜单，按住右键拖动可连线，空白处拖动平移画布。</p>
  </div>
</template>

<style scoped>
.palette-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 4px;
}

.palette-head h3 {
  margin: 0;
}

.palette-save {
  border: 1px solid var(--border);
  background: var(--surface2);
  color: var(--text1);
  border-radius: 6px;
  padding: 3px 8px;
  font-size: 11px;
  cursor: pointer;
}

.palette-save:disabled {
  opacity: 0.45;
  cursor: not-allowed;
}

.palette-save:not(:disabled):hover {
  border-color: var(--accent);
}

.palette-cat {
  color: var(--text3);
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  margin: 8px 0 4px;
}

.palette-item {
  display: flex;
  align-items: center;
  gap: 9px;
  padding: 7px 9px;
  border-radius: 7px;
  border: 1px solid transparent;
  cursor: grab;
  user-select: none;
  font-size: 12px;
  font-weight: 500;
  transition:
    background 0.15s,
    border-color 0.15s;
}

.palette-item:hover {
  background: var(--surface2);
  border-color: var(--border);
}

.palette-item:active {
  cursor: grabbing;
}

.palette-item.disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.palette-name {
  flex: 1;
}

.palette-grip {
  color: var(--text3);
  font-size: 15px;
}

.palette-dot {
  width: 9px;
  height: 9px;
  border-radius: 3px;
  flex-shrink: 0;
  background: var(--cat-color, var(--text3));
}

.palette-dot.tpl {
  background: var(--accent, #7aa2f7);
}

.palette-template {
  cursor: copy;
}

.palette-template-name {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.palette-template-actions {
  display: none;
  gap: 2px;
}

.palette-template:hover .palette-template-actions {
  display: inline-flex;
}

.tpl-btn {
  border: none;
  background: transparent;
  color: var(--text3);
  cursor: pointer;
  font-size: 12px;
  padding: 0 4px;
  border-radius: 4px;
}

.tpl-btn:hover {
  background: var(--surface3, var(--surface2));
  color: var(--text1);
}

.tpl-btn.danger:hover {
  color: #f7768e;
}

.palette-empty {
  color: var(--text3);
  font-size: 11px;
  padding: 4px 9px 8px;
  line-height: 1.5;
}
</style>
