<script setup lang="ts">
import { computed, onMounted, reactive, ref } from "vue";
import { addNode, editor, nodeTypeGroups } from "../state/editor";
import {
  ensureTemplates,
  insertTemplate,
  openTemplateSave,
  removeTemplate,
  renameTemplate,
  templates,
} from "../state/templates";
import { confirmDialog, promptDialog } from "../state/modal";
import { readJson, writeJson } from "../state/persist";
import type { NodeTypeDesc } from "../types";

/**
 * drawio 式节点面板：搜索 + 可折叠分组。分组折叠态按组 id 持久化；
 * 搜索时自动展开、隐藏空组。业务节点（模板）按分类列在最上的「业务节点」组。
 * 拖拽数据类型（flow-node-type / flow-template-id）与 data-node-type
 * 是画布落点与 e2e 的契约，不可改名。
 */
const search = ref("");

const GROUPS_KEY = "flow.palette-groups";
/** 折叠偏好：localStorage 是用户可碰的存储，损坏值走 readJson 回退空表（全部展开），不炸 setup */
const collapsed = reactive<Record<string, boolean>>({
  ...readJson<Record<string, boolean>>(GROUPS_KEY, {}),
});

function persistGroups(): void {
  writeJson(GROUPS_KEY, { ...collapsed });
}

function groupOpen(id: string): boolean {
  return search.value.trim() ? true : !collapsed[id];
}

/** 组头点击：搜索态照样记偏好（搜索强制展开只管显示，搜索结束回到偏好） */
function toggleGroup(id: string): void {
  collapsed[id] = !collapsed[id];
  persistGroups();
}

function matchesSearch(...text: string[]): boolean {
  const q = search.value.trim().toLowerCase();
  return !q || text.some((t) => t.toLowerCase().includes(q));
}

/** 节点类型分组：按搜索过滤，空组在搜索时隐藏 */
const nodeGroups = computed(() =>
  nodeTypeGroups.value
    .map((g) => ({
      ...g,
      items: g.items.filter((nt) => matchesSearch(nt.label, nt.type)),
    }))
    .filter((g) => g.items.length > 0 || !search.value.trim()),
);

/** 业务节点（模板）按分类分组：空分类归入「业务」 */
const templateGroups = computed(() => {
  const byCat = new Map<string, typeof templates.list>();
  for (const t of templates.list) {
    if (!matchesSearch(t.name)) continue;
    const cat = t.category?.trim() || "业务";
    if (!byCat.has(cat)) byCat.set(cat, []);
    byCat.get(cat)!.push(t);
  }
  return [...byCat.entries()].map(([cat, items]) => ({ cat, items }));
});

const searchEmpty = computed(
  () => !!search.value.trim() && nodeGroups.value.length === 0 && templateGroups.value.length === 0,
);

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
  return (
    !!nt.max_instances &&
    editor.nodes.filter((n) => n.data.nodeType.type === nt.type).length >= nt.max_instances
  );
}

function onTemplateDragStart(event: DragEvent, id: string): void {
  if (!event.dataTransfer) return;
  event.dataTransfer.setData("application/flow-template-id", id);
  event.dataTransfer.effectAllowed = "copy";
}

/** 点击业务节点 = 插入到错位空档；拖拽落点由 FlowCanvas 处理 */
async function onClickInsertTemplate(id: string): Promise<void> {
  const n = editor.nodes.length;
  await insertTemplate(id, { x: 160 + (n % 5) * 48, y: 120 + (n % 5) * 48 });
}

/** 点击节点类型 = 插入到错位空档（与业务节点点击同一落点规则）；拖拽落点由 FlowCanvas 处理 */
function onClickInsertNodeType(nt: NodeTypeDesc): void {
  if (!editor.workflowId || atLimit(nt)) return;
  const n = editor.nodes.length;
  addNode(nt.type, { x: 160 + (n % 5) * 48, y: 120 + (n % 5) * 48 });
}

const selectedCount = computed(() => editor.nodes.filter((n) => n.selected).length);

function onSaveAsBusinessNode(): void {
  openTemplateSave();
}

async function onDeleteTemplate(id: string, name: string): Promise<void> {
  const ok = await confirmDialog(`删除业务节点「${name}」？已插入的节点不受影响。`);
  if (!ok) return;
  await removeTemplate(id);
}

async function onRenameTemplate(id: string, current: string): Promise<void> {
  const name = await promptDialog("新的业务节点名称", current);
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
        :disabled="selectedCount === 0"
        title="把选中的节点（含参数与内部连线）保存为可复用业务节点"
        @click="onSaveAsBusinessNode"
      >
        存为业务节点
      </button>
    </div>
    <div class="palette-search">
      <input
        v-model="search"
        type="search"
        class="palette-search-input"
        placeholder="搜索节点…"
        aria-label="搜索节点"
      />
    </div>

    <!-- 业务节点：预配置的节点/子图模板，按分类分组 -->
    <section v-if="templateGroups.length > 0" class="palette-group">
      <button
        type="button"
        class="group-head"
        :aria-expanded="groupOpen('tpl')"
        @click="toggleGroup('tpl')"
      >
        <span class="group-toggle" aria-hidden="true">{{ groupOpen("tpl") ? "▾" : "▸" }}</span>
        <span class="group-label">业务节点</span>
        <span class="group-count">{{ templates.list.length }}</span>
      </button>
      <div v-if="groupOpen('tpl')" class="group-body">
        <template v-for="g in templateGroups" :key="g.cat">
          <div v-if="templateGroups.length > 1" class="palette-cat">{{ g.cat }}</div>
          <div
            v-for="t in g.items"
            :key="t.id"
            class="palette-item palette-template"
            draggable="true"
            tabindex="0"
            :title="`${t.name}（${t.node_count} 个节点）：拖到画布插入，或点击插入`"
            @dragstart="onTemplateDragStart($event, t.id)"
            @click="onClickInsertTemplate(t.id)"
            @keydown.enter="onClickInsertTemplate(t.id)"
          >
            <span class="palette-dot tpl" />
            <span class="palette-template-name">{{ t.name }}</span>
            <span class="palette-template-actions">
              <button
                class="tpl-btn"
                title="重命名"
                @click.stop="onRenameTemplate(t.id, t.name)"
                @keydown.stop
              >
                ✎
              </button>
              <button
                class="tpl-btn danger"
                title="删除业务节点"
                @click.stop="onDeleteTemplate(t.id, t.name)"
                @keydown.stop
              >
                ×
              </button>
            </span>
          </div>
        </template>
      </div>
    </section>
    <p v-else-if="!search.trim()" class="palette-empty">
      暂无业务节点。选中节点后点「存为业务节点」，可在任意流程复用。
    </p>

    <!-- 节点类型：按功能分组，组头可折叠 -->
    <section v-for="g in nodeGroups" :key="g.cat" class="palette-group">
      <button
        type="button"
        class="group-head"
        :aria-expanded="groupOpen(g.cat)"
        @click="toggleGroup(g.cat)"
      >
        <span class="group-toggle" aria-hidden="true">{{ groupOpen(g.cat) ? "▾" : "▸" }}</span>
        <span class="group-label">{{ g.label }}</span>
        <span class="group-count">{{ g.items.length }}</span>
      </button>
      <div v-if="groupOpen(g.cat)" class="group-body">
        <div
          v-for="nt in g.items"
          :key="nt.type"
          class="palette-item"
          :class="[`cat-${nt.category}`, { disabled: !editor.workflowId || atLimit(nt) }]"
          :draggable="!!editor.workflowId && !atLimit(nt)"
          :aria-disabled="!editor.workflowId || atLimit(nt)"
          :data-node-type="nt.type"
          tabindex="0"
          :title="
            atLimit(nt)
              ? `${nt.label}：已达数量上限（${nt.max_instances} 个）`
              : `${nt.label}：拖到画布或点击添加`
          "
          @dragstart="onDragStart($event, nt)"
          @click="onClickInsertNodeType(nt)"
          @keydown.enter="onClickInsertNodeType(nt)"
        >
          <span class="palette-dot" />
          <span class="palette-name">{{ nt.label }}</span>
          <span class="palette-grip" aria-hidden="true">⠿</span>
        </div>
      </div>
    </section>

    <p v-if="searchEmpty" class="palette-empty">无匹配节点</p>
    <p class="palette-hint">
      拖动节点到画布添加；右键点节点开菜单，按住右键拖动可连线，空白处拖动平移画布。
    </p>
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

.palette-search {
  margin-bottom: 6px;
}

.palette-search-input {
  width: 100%;
  padding: 5px 8px;
  font-size: 12px;
  border: 1px solid var(--border);
  border-radius: 6px;
  background: var(--surface2);
  color: var(--text1);
}

.palette-group {
  border: 1px solid var(--border);
  border-radius: 8px;
  margin-bottom: 6px;
  overflow: hidden;
}

.group-head {
  display: flex;
  align-items: center;
  gap: 6px;
  width: 100%;
  padding: 6px 9px;
  border: none;
  background: var(--surface2);
  color: var(--text2);
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  cursor: pointer;
  text-align: left;
}

.group-head:hover {
  color: var(--text1);
}

.group-toggle {
  width: 12px;
  flex-shrink: 0;
  font-size: 10px;
}

.group-label {
  flex: 1;
}

.group-count {
  color: var(--text3);
  font-weight: 500;
  letter-spacing: normal;
}

.group-body {
  padding: 3px;
}

.palette-cat {
  color: var(--text3);
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  margin: 6px 6px 2px;
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

.palette-item:hover:not(.disabled) {
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
  color: var(--danger);
}

.palette-empty {
  color: var(--text3);
  font-size: 11px;
  padding: 4px 9px 8px;
  line-height: 1.5;
}
</style>
