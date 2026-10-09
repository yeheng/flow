<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref, shallowRef, watch } from "vue";
import { MarkerType, VueFlow, useVueFlow } from "@vue-flow/core";
import type { NodeMouseEvent } from "@vue-flow/core";
import { Background, BackgroundVariant } from "@vue-flow/background";
import { Controls } from "@vue-flow/controls";
import { MiniMap } from "@vue-flow/minimap";
import {
  addNode,
  autoLayout,
  canPaste,
  copySelection,
  deleteSelection,
  drillIntoSubWorkflow,
  duplicateSelection,
  editor,
  isValidConnection,
  jumpToBreadcrumb,
  nodeTypeGroups,
  onConnect,
  pasteClipboard,
  sourcePortsOf,
  type EditorEdge,
  type EditorNode,
} from "../state/editor";
import { insertTemplate, saveSelectionAsTemplate } from "../state/templates";
import { promptDialog } from "../state/modal";
import { beginDrag, commit, endDrag } from "../state/history";
import { monitor, openChildRun } from "../state/monitor";
import FlowNode from "./FlowNode.vue";
import CanvasContextMenu, { type MenuEntry } from "./CanvasContextMenu.vue";

const emit = defineEmits<{ "edit-node": [] }>();

const { screenToFlowCoordinate, setViewport, getViewport, fitView } = useVueFlow();

const defaultEdgeOptions = {
  markerEnd: { type: MarkerType.ArrowClosed, color: "#7c6cff" },
};

function onDrop(event: DragEvent): void {
  const point = screenToFlowCoordinate({ x: event.clientX, y: event.clientY });
  // 模板拖放优先：整段片段按落点插入
  const templateId = event.dataTransfer?.getData("application/flow-template-id");
  if (templateId) {
    void insertTemplate(templateId, point);
    return;
  }
  const type = event.dataTransfer?.getData("application/flow-node-type");
  if (!type) return;
  addNode(type, point);
}

function onNodeClick(e: NodeMouseEvent): void {
  editor.selectedNodeId = e.node.id;
}

function onNodeDoubleClick(e: NodeMouseEvent): void {
  void drillIntoSubWorkflow(e.node.id);
}

// 删除键在 vue-flow 处理前先入 undo 栈（canvas-wrap 在冒泡路径上先于 window 监听器执行）
function onKeydown(e: KeyboardEvent): void {
  if (e.key !== "Delete" && e.key !== "Backspace") return;
  if (e.target instanceof HTMLElement && e.target.closest("input, textarea, [contenteditable]"))
    return;
  if (editor.nodes.some((n) => n.selected) || editor.edges.some((edge) => edge.selected)) commit();
}

// 运行中节点的下游边做流动动画
const runningNodeIds = computed(() => {
  if (!monitor.runId || monitor.workflowId !== editor.workflowId) return new Set<string>();
  return new Set(
    monitor.nodes.filter((n) => n.state === "running" || n.state === "retrying").map((n) => n.id),
  );
});
watch(
  runningNodeIds,
  (ids) => {
    for (const e of editor.edges) e.animated = ids.has(e.source);
  },
  { immediate: true },
);

// ---- 右键交互：节点上按住拖动 = 连线，空白处按住拖动 = 平移，原地松开 = 菜单 ----

const wrapEl = ref<HTMLElement>();
const menu = shallowRef<{ x: number; y: number; entries: MenuEntry[] } | null>(null);
/** 右键拖拽的临时连线（canvas-wrap 本地坐标）；valid 表示当前落点是合法目标 */
const linkLine = shallowRef<{ x1: number; y1: number; x2: number; y2: number; valid: boolean } | null>(
  null,
);
/** 激活后的右键模式（决定光标样式；「menu」模式不激活） */
const rmbMode = ref<"pan" | "link" | null>(null);

/** 位移超过该阈值（px）视为拖拽而非点击 */
const DRAG_THRESHOLD = 4;

interface RmbState {
  kind: "link" | "pan" | "menu";
  startX: number;
  startY: number;
  active: boolean;
  /** link：源节点与其出端口 handle 元素 */
  source?: string;
  sourceHandle?: string;
  sourceHandleEl?: Element | null;
  /** 原地松开时的菜单命中对象 */
  hitNodeId?: string;
  hitEdgeId?: string;
  /** pan：拖动起点的视口变换 */
  origin?: { x: number; y: number; zoom: number };
}

let rmb: RmbState | null = null;

function onCanvasMouseDown(e: MouseEvent): void {
  if (e.button !== 2) return;
  e.preventDefault();
  menu.value = null;
  const target = e.target as Element;
  const nodeId = target.closest(".vue-flow__node")?.getAttribute("data-id") ?? null;
  const state: RmbState = { kind: "pan", startX: e.clientX, startY: e.clientY, active: false };
  if (nodeId) {
    state.hitNodeId = nodeId;
    state.kind = "menu";
    const node = editor.nodes.find((n) => n.id === nodeId);
    // 有出端口的节点进入连线模式；end 这类无出端口节点原地松开只弹菜单
    if (node && sourcePortsOf(node.data.nodeType).length > 0) {
      state.kind = "link";
      state.source = nodeId;
      const picked = nearestSourceHandle(nodeId, e);
      state.sourceHandle = picked?.id ?? sourcePortsOf(node.data.nodeType)[0].id;
      state.sourceHandleEl = picked?.el ?? null;
    }
  } else {
    state.hitEdgeId =
      target.closest(".vue-flow__edge")?.getAttribute("data-id") ?? undefined;
    state.origin = getViewport();
  }
  rmb = state;
}

/** 节点有多个出端口（condition 真/假）时，取离鼠标最近的端口 */
function nearestSourceHandle(
  nodeId: string,
  e: MouseEvent,
): { id: string; el: Element } | null {
  const nodeEl = wrapEl.value?.querySelector(`.vue-flow__node[data-id="${CSS.escape(nodeId)}"]`);
  const handles = nodeEl?.querySelectorAll(".vue-flow__handle.source") ?? [];
  let best: { id: string; el: Element; dist: number } | null = null;
  for (const el of handles) {
    const id = el.getAttribute("data-handleid");
    if (!id) continue;
    const r = el.getBoundingClientRect();
    const dist = (r.left + r.width / 2 - e.clientX) ** 2 + (r.top + r.height / 2 - e.clientY) ** 2;
    if (!best || dist < best.dist) best = { id, el, dist };
  }
  return best ? { id: best.id, el: best.el } : null;
}

function onWindowMouseMove(e: MouseEvent): void {
  if (!rmb) return;
  const dx = e.clientX - rmb.startX;
  const dy = e.clientY - rmb.startY;
  if (!rmb.active) {
    if (dx * dx + dy * dy < DRAG_THRESHOLD * DRAG_THRESHOLD) return;
    rmb.active = true;
    if (rmb.kind !== "menu") rmbMode.value = rmb.kind;
  }
  if (rmb.kind === "pan") {
    setViewport({
      x: rmb.origin!.x + dx,
      y: rmb.origin!.y + dy,
      zoom: rmb.origin!.zoom,
    });
  } else if (rmb.kind === "link") {
    updateLinkLine(e);
  }
}

function updateLinkLine(e: MouseEvent): void {
  if (!rmb?.source) return;
  const rect = wrapEl.value?.getBoundingClientRect();
  if (!rect) return;
  // 每帧重查源 handle 位置：视图可能在拖动中变化
  const handleEl =
    rmb.sourceHandleEl?.isConnected === true
      ? rmb.sourceHandleEl
      : wrapEl.value?.querySelector(
          `.vue-flow__node[data-id="${CSS.escape(rmb.source)}"] .vue-flow__handle.source[data-handleid="${CSS.escape(rmb.sourceHandle ?? "out")}"]`,
        );
  if (!handleEl) return;
  const hr = handleEl.getBoundingClientRect();
  const target = nodeAtPoint(e.clientX, e.clientY);
  const valid =
    !!target &&
    isValidConnection({
      source: rmb.source,
      target,
      sourceHandle: rmb.sourceHandle ?? null,
      targetHandle: "in",
    });
  editor.linkTargetId = valid ? target : null;
  linkLine.value = {
    x1: hr.left + hr.width / 2 - rect.left,
    y1: hr.top + hr.height / 2 - rect.top,
    x2: e.clientX - rect.left,
    y2: e.clientY - rect.top,
    valid,
  };
}

/** 屏幕坐标下的目标节点 id（连线层 pointer-events:none，elementFromPoint 可穿透） */
function nodeAtPoint(x: number, y: number): string | null {
  const el = document.elementFromPoint(x, y);
  return el?.closest(".vue-flow__node")?.getAttribute("data-id") ?? null;
}

function onWindowMouseUp(e: MouseEvent): void {
  if (!rmb || e.button !== 2) return;
  const state = rmb;
  rmb = null;
  rmbMode.value = null;
  editor.linkTargetId = null;
  linkLine.value = null;
  if (!state.active) {
    openMenu(state, e.clientX, e.clientY);
    return;
  }
  if (state.kind === "link" && state.source) {
    const target = nodeAtPoint(e.clientX, e.clientY);
    if (target) {
      // onConnect 内部走 isValidConnection 校验，非法落点静默取消
      onConnect({
        source: state.source,
        target,
        sourceHandle: state.sourceHandle ?? null,
        targetHandle: "in",
      });
    }
  }
}

// ---- 菜单构造 ----

function openMenu(state: RmbState, x: number, y: number): void {
  const node = state.hitNodeId ? editor.nodes.find((n) => n.id === state.hitNodeId) : undefined;
  if (node) {
    menu.value = { x, y, entries: nodeMenu(node) };
    return;
  }
  const edge = state.hitEdgeId ? editor.edges.find((e) => e.id === state.hitEdgeId) : undefined;
  if (edge) {
    menu.value = { x, y, entries: edgeMenu(edge) };
    return;
  }
  menu.value = { x, y, entries: paneMenu(x, y) };
}

/** 菜单动作以「选中」为前提（复制/删除等都吃 selected），先把选择收敛到目标上 */
function selectOnlyNode(id: string): void {
  for (const n of editor.nodes) n.selected = n.id === id;
  for (const e of editor.edges) e.selected = false;
  editor.selectedNodeId = id;
}

function nodeMenu(node: EditorNode): MenuEntry[] {
  const entries: MenuEntry[] = [
    {
      key: "edit",
      label: "编辑参数",
      action: () => {
        selectOnlyNode(node.id);
        emit("edit-node");
      },
    },
    {
      key: "copy",
      label: "复制",
      hint: "⌘C",
      action: () => {
        selectOnlyNode(node.id);
        copySelection();
      },
    },
    {
      key: "dup",
      label: "创建副本",
      action: () => {
        selectOnlyNode(node.id);
        duplicateSelection();
      },
    },
    {
      key: "tpl",
      label: "存为模板",
      action: () => void saveNodeAsTemplate(node.id),
    },
  ];
  if (node.data.nodeType.type === "sub_workflow") {
    entries.push({
      key: "drill",
      label: "钻取子流程",
      action: () => void drillIntoSubWorkflow(node.id),
    });
  }
  entries.push(
    { key: "sep1" },
    {
      key: "del",
      label: "删除",
      hint: "Del",
      danger: true,
      action: () => {
        selectOnlyNode(node.id);
        deleteSelection();
      },
    },
  );
  return entries;
}

async function saveNodeAsTemplate(id: string): Promise<void> {
  const node = editor.nodes.find((n) => n.id === id);
  if (!node) return;
  selectOnlyNode(id);
  const name = await promptDialog(`模板名称（保存节点「${node.data.name || id}」及其连线）`, "");
  if (name === null || !name.trim()) return;
  await saveSelectionAsTemplate(name);
}

function edgeMenu(edge: EditorEdge): MenuEntry[] {
  return [
    {
      key: "del",
      label: "删除连线",
      danger: true,
      action: () => {
        for (const n of editor.nodes) n.selected = false;
        for (const e of editor.edges) e.selected = e.id === edge.id;
        deleteSelection();
      },
    },
  ];
}

function paneMenu(x: number, y: number): MenuEntry[] {
  const point = screenToFlowCoordinate({ x, y });
  const addChildren = nodeTypeGroups.value.flatMap((g) =>
    g.items.map((nt) => ({
      key: nt.type,
      label: nt.label,
      group: g.label,
      action: () => addNode(nt.type, point),
    })),
  );
  return [
    {
      key: "add",
      label: "添加节点",
      children: addChildren,
    },
    {
      key: "paste",
      label: "粘贴",
      hint: "⌘V",
      disabled: !canPaste.value,
      action: () => pasteClipboard(point),
    },
    { key: "sep1" },
    {
      key: "layout",
      label: "自动布局",
      disabled: editor.nodes.length === 0,
      action: () => autoLayout(),
    },
    {
      key: "fit",
      label: "适应视图",
      disabled: editor.nodes.length === 0,
      action: () => void fitView({ padding: 0.2, duration: 200 }),
    },
  ];
}

onMounted(() => {
  window.addEventListener("mousemove", onWindowMouseMove);
  window.addEventListener("mouseup", onWindowMouseUp);
});
onUnmounted(() => {
  window.removeEventListener("mousemove", onWindowMouseMove);
  window.removeEventListener("mouseup", onWindowMouseUp);
});
</script>

<template>
  <div
    ref="wrapEl"
    class="canvas-wrap"
    :class="rmbMode === 'pan' ? 'rmb-panning' : rmbMode === 'link' ? 'rmb-linking' : null"
    @mousedown.capture="onCanvasMouseDown"
    @contextmenu.prevent
    @drop="onDrop"
    @dragover.prevent
    @keydown="onKeydown"
  >
    <div v-if="editor.breadcrumb.length" class="breadcrumb">
      <template v-for="(c, i) in editor.breadcrumb" :key="`${i}:${c.workflowId}`">
        <button type="button" class="crumb" @click="jumpToBreadcrumb(i)">
          {{ c.name || c.workflowId }}
        </button>
        <span class="crumb-sep">/</span>
      </template>
      <span class="crumb-current">{{ editor.workflowName || editor.workflowId }}</span>
    </div>
    <VueFlow
      v-model:nodes="editor.nodes"
      v-model:edges="editor.edges"
      :is-valid-connection="isValidConnection"
      :delete-key-code="['Backspace', 'Delete']"
      :selection-key-code="'Shift'"
      :multi-selection-key-code="['Meta', 'Control']"
      :default-viewport="{ zoom: 1 }"
      :default-edge-options="defaultEdgeOptions"
      :min-zoom="0.2"
      :max-zoom="2"
      :snap-to-grid="true"
      :snap-grid="[16, 16]"
      color-mode="dark"
      fit-view-on-init
      @connect="onConnect"
      @node-click="onNodeClick"
      @node-double-click="onNodeDoubleClick"
      @node-drag-start="beginDrag()"
      @node-drag-stop="endDrag()"
      @pane-click="editor.selectedNodeId = null"
    >
      <template #node-flow="nodeProps">
        <FlowNode v-bind="nodeProps" @open-child="openChildRun" />
      </template>
      <Background
        :variant="BackgroundVariant.Dots"
        :gap="28"
        :size="1.5"
        color="rgba(255, 255, 255, 0.06)"
      />
      <Controls />
      <MiniMap />
    </VueFlow>
    <!-- 右键拖拽连线的临时连线层：不拦截任何指针事件 -->
    <svg v-if="linkLine" class="rmb-link-layer" aria-hidden="true">
      <line
        :x1="linkLine.x1"
        :y1="linkLine.y1"
        :x2="linkLine.x2"
        :y2="linkLine.y2"
        :class="linkLine.valid ? 'valid' : 'invalid'"
      />
      <circle
        :cx="linkLine.x2"
        :cy="linkLine.y2"
        r="4"
        :class="linkLine.valid ? 'valid' : 'invalid'"
      />
    </svg>
    <CanvasContextMenu
      v-if="menu"
      :x="menu.x"
      :y="menu.y"
      :entries="menu.entries"
      @close="menu = null"
    />
    <div v-if="!editor.workflowId" class="canvas-hint">在左侧新建或选择一个工作流</div>
  </div>
</template>

<style scoped>
.breadcrumb {
  position: absolute;
  top: 12px;
  left: 12px;
  z-index: 10;
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 5px 12px;
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: 7px;
  box-shadow: 0 2px 12px var(--shadow);
  font-size: 12px;
}

.crumb {
  padding: 0;
  border: none;
  background: none;
  font-size: 12px;
  font-weight: 400;
  color: var(--accent2);
  cursor: pointer;
}

.crumb:hover {
  background: none;
  text-decoration: underline;
}

.crumb-sep {
  color: var(--text3);
}

.crumb-current {
  font-weight: 600;
}

/* 右键连线层与光标样式（.canvas-wrap 本体在全局 style.css） */
.rmb-link-layer {
  position: absolute;
  inset: 0;
  z-index: 20;
  pointer-events: none;
}

.rmb-link-layer line {
  stroke-width: 2;
  stroke-dasharray: 6 4;
}

.rmb-link-layer line.invalid {
  stroke: #7c6cff;
}

.rmb-link-layer line.valid {
  stroke: var(--ok);
}

.rmb-link-layer circle.valid {
  fill: var(--ok);
}

.rmb-link-layer circle.invalid {
  fill: #7c6cff;
}
</style>

<style>
/* 右键拖拽期间的光标（需覆盖 handle 等子元素，放全局） */
.canvas-wrap.rmb-panning,
.canvas-wrap.rmb-panning * {
  cursor: grabbing !important;
}

.canvas-wrap.rmb-linking,
.canvas-wrap.rmb-linking * {
  cursor: crosshair !important;
}
</style>
