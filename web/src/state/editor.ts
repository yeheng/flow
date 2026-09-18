import { computed, reactive } from "vue";
import type { Connection } from "@vue-flow/core";
import * as api from "../api/flow";
import { RpcError, errText } from "../rpc/client";
import type {
  Definition,
  DefinitionEdge,
  NodeTypeDesc,
  PortDesc,
  Position,
  WorkflowSummary,
} from "../types";

export interface FlowNodeData {
  name: string;
  nodeType: NodeTypeDesc;
  params: Record<string, unknown>;
  [key: string]: unknown;
}

/**
 * 编辑器自有节点类型：结构与 @vue-flow/core 的 Node 兼容，
 * 但不引入其泛型（VNode/Component 会让 reactive 的类型展开爆炸，TS2589）
 */
export interface EditorNode {
  id: string;
  type: "flow";
  position: { x: number; y: number };
  data: FlowNodeData;
}

/** 同 EditorNode：结构兼容 @vue-flow/core 的 Edge，避免引入其泛型 */
export interface EditorEdge {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string | null;
  targetHandle?: string | null;
  /** 多出端口节点（如 condition）的出边标签，取自 ports 元数据 */
  label?: string;
  /** 源节点运行中时置 true，边做流动动画 */
  animated?: boolean;
}

/** 全局提示条：error 优先于 info 展示 */
export const ui = reactive({
  error: null as string | null,
  info: null as string | null,
});

interface BreadcrumbEntry {
  workflowId: string;
  name: string;
}

interface EditorState {
  nodeTypes: NodeTypeDesc[];
  workflows: WorkflowSummary[];
  workflowId: string | null;
  workflowName: string;
  version: number;
  publishedVersion: number | null;
  nodes: EditorNode[];
  edges: EditorEdge[];
  selectedNodeId: string | null;
  /** RunPanel 行 hover/click 联动画布高亮 */
  highlightNodeId: string | null;
  /** sub_workflow 钻取栈：栈顶是当前工作流的直接父级 */
  breadcrumb: BreadcrumbEntry[];
  dirty: boolean;
}

export const editor = reactive<EditorState>({
  nodeTypes: [],
  workflows: [],
  workflowId: null,
  workflowName: "",
  version: 0,
  publishedVersion: null,
  nodes: [],
  edges: [],
  selectedNodeId: null,
  highlightNodeId: null,
  breadcrumb: [],
  dirty: false,
});

export const selectedNode = computed<EditorNode | null>(
  () => editor.nodes.find((n) => n.id === editor.selectedNodeId) ?? null,
);

export function nodeTypeDesc(type: string): NodeTypeDesc | undefined {
  return editor.nodeTypes.find((t) => t.type === type);
}

/** 出端口（source）；协议约定 id 为 "in" 的是入端口，其余都是出端口 */
export function sourcePortsOf(desc: NodeTypeDesc): PortDesc[] {
  return desc.ports.filter((p) => p.id !== "in");
}

/** 多出端口节点的出边在边上标端口名（condition 的真/假），单出端口不需要 */
function edgeLabel(source: EditorNode | undefined, sourceHandle: string): string | undefined {
  if (!source) return undefined;
  const ports = sourcePortsOf(source.data.nodeType);
  if (ports.length <= 1) return undefined;
  return ports.find((p) => p.id === sourceHandle)?.label;
}

export async function initEditor(): Promise<void> {
  try {
    editor.nodeTypes = await api.listNodeTypes();
  } catch (e) {
    ui.error = `无法连接 flow-server：${errText(e)}`;
    return;
  }
  await refreshWorkflows();
  if (editor.workflows.length > 0) {
    await selectWorkflow(editor.workflows[0].workflow_id);
  }
}

export async function refreshWorkflows(): Promise<void> {
  try {
    editor.workflows = await api.listWorkflows();
  } catch (e) {
    ui.error = errText(e);
  }
}

/** 新工作流还没有任何版本：seed 一个合法的最小定义（start → end），等用户保存 */
function seedCanvas(): void {
  const start = nodeTypeDesc("start")!;
  const end = nodeTypeDesc("end")!;
  editor.nodes = [
    { id: "start_1", type: "flow", position: { x: 80, y: 200 }, data: { name: start.label, nodeType: start, params: {} } },
    { id: "end_1", type: "flow", position: { x: 480, y: 200 }, data: { name: end.label, nodeType: end, params: {} } },
  ];
  editor.edges = [
    { id: "e_start_1_out_end_1", source: "start_1", sourceHandle: "out", target: "end_1", targetHandle: "in" },
  ];
}

export async function selectWorkflow(id: string): Promise<void> {
  try {
    const w = await api.getWorkflow(id);
    editor.version = w.version;
    editor.publishedVersion = w.published_version;
    toFlow(w.definition);
  } catch (e) {
    // workflow.get 对无版本的工作流报「不存在」（-32011），按空画布处理
    if (!(e instanceof RpcError && e.code === -32011)) {
      ui.error = errText(e);
      return;
    }
    editor.version = 0;
    editor.publishedVersion = null;
    seedCanvas();
  }
  editor.workflowId = id;
  editor.workflowName = editor.workflows.find((w) => w.workflow_id === id)?.name ?? "";
  editor.selectedNodeId = null;
  editor.dirty = false;
  ui.error = null;
}

export async function createWorkflow(name: string): Promise<void> {
  try {
    const id = await api.createWorkflow(name);
    await refreshWorkflows();
    await selectWorkflow(id);
  } catch (e) {
    ui.error = errText(e);
  }
}

export async function removeWorkflow(id: string): Promise<void> {
  try {
    await api.deleteWorkflow(id);
    if (editor.workflowId === id) {
      editor.workflowId = null;
      editor.nodes = [];
      editor.edges = [];
    }
    await refreshWorkflows();
  } catch (e) {
    ui.error = errText(e);
  }
}

// ---- Vue Flow <-> Definition 双向转换 ----

/**
 * 旧定义可能缺 position：按拓扑分层给缺失节点兜底坐标（每层一列，层内纵排）。
 * 已有坐标的节点不动。
 */
function fallbackPositions(def: Definition): Map<string, Position> {
  const missing = new Set(def.nodes.filter((n) => !n.position).map((n) => n.id));
  const result = new Map<string, Position>();
  if (missing.size === 0) return result;

  const incoming = new Map<string, string[]>();
  for (const e of def.edges) {
    const list = incoming.get(e.to) ?? [];
    list.push(e.from);
    incoming.set(e.to, list);
  }
  const depthCache = new Map<string, number>();
  function depthOf(id: string, stack: Set<string>): number {
    const cached = depthCache.get(id);
    if (cached !== undefined) return cached;
    if (stack.has(id)) return 0; // 环防御：服务端 validate 会拦，画布先画出来
    stack.add(id);
    let depth = 0;
    for (const from of incoming.get(id) ?? []) {
      depth = Math.max(depth, depthOf(from, stack) + 1);
    }
    stack.delete(id);
    depthCache.set(id, depth);
    return depth;
  }

  const perLayer = new Map<number, number>();
  for (const id of missing) {
    const depth = depthOf(id, new Set());
    const row = perLayer.get(depth) ?? 0;
    perLayer.set(depth, row + 1);
    result.set(id, { x: 80 + depth * 220, y: 80 + row * 120 });
  }
  return result;
}

function toFlow(def: Definition): void {
  const fallback = fallbackPositions(def);
  editor.nodes = def.nodes.map((n) => ({
    id: n.id,
    type: "flow",
    position: n.position ?? fallback.get(n.id) ?? { x: 80, y: 80 },
    data: {
      name: n.name ?? "",
      nodeType: nodeTypeDesc(n.type)!,
      params: (n.params ?? {}) as Record<string, unknown>,
    },
  }));
  // 单出端口节点的唯一出口 handle id 是 "out"，definition 里不落 port
  editor.edges = def.edges.map((e) => ({
    id: `e_${e.from}_${e.port ?? "out"}_${e.to}`,
    source: e.from,
    sourceHandle: e.port ?? "out",
    target: e.to,
    targetHandle: "in",
    label: edgeLabel(editor.nodes.find((n) => n.id === e.from), e.port ?? "out"),
  }));
}

function cleanParams(params: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === "") continue;
    if (k === "retry" && typeof v === "object" && v !== null) {
      const r = v as Record<string, unknown>;
      if (r.max_attempts === undefined && r.backoff_ms === undefined) continue;
    }
    out[k] = v;
  }
  return out;
}

export function flowToDefinition(): Definition {
  return {
    nodes: editor.nodes.map((n) => ({
      id: n.id,
      type: n.data!.nodeType.type,
      name: n.data!.name,
      position: { x: Math.round(n.position.x), y: Math.round(n.position.y) },
      params: cleanParams(n.data!.params),
    })),
    edges: editor.edges.map((e) => {
      const edge: DefinitionEdge = { from: e.source, to: e.target };
      const source = editor.nodes.find((n) => n.id === e.source);
      // 多出端口节点（如 condition）的出边必须落 port；单出端口省略
      if (source && sourcePortsOf(source.data.nodeType).length > 1) {
        edge.port = e.sourceHandle ?? undefined;
      }
      return edge;
    }),
  };
}

// ---- 画布交互 ----

export function addNode(type: string, position: { x: number; y: number }): boolean {
  const desc = nodeTypeDesc(type);
  if (!desc) return false;
  const max = desc.max_instances ?? 0;
  if (max > 0 && editor.nodes.filter((n) => n.data?.nodeType.type === type).length >= max) {
    ui.error = `节点类型「${desc.label}」最多 ${max} 个`;
    return false;
  }
  let seq = 1;
  while (editor.nodes.some((n) => n.id === `${type}_${seq}`)) seq++;
  // 默认值在节点创建时从 schema 落进 params，与后端 default 语义一致
  const params: Record<string, unknown> = {};
  for (const [key, prop] of Object.entries(desc.params_schema.properties ?? {})) {
    if (prop.default !== undefined) params[key] = structuredClone(prop.default);
  }
  editor.nodes.push({
    id: `${type}_${seq}`,
    type: "flow",
    position,
    data: { name: desc.label, nodeType: desc, params },
  });
  editor.dirty = true;
  return true;
}

/** 交互层连线约束；全部由 ports 元数据驱动，服务端 validate 兜底 */
export function isValidConnection(conn: Connection): boolean {
  if (!conn.source || !conn.target || conn.source === conn.target) return false;
  const source = editor.nodes.find((n) => n.id === conn.source);
  const target = editor.nodes.find((n) => n.id === conn.target);
  if (!source || !target) return false;
  const outPorts = sourcePortsOf(source.data.nodeType);
  // 无出端口（end）/无入端口（start）不允许连线
  if (outPorts.length === 0) return false;
  if (!target.data.nodeType.ports.some((p) => p.id === "in")) return false;
  // sourceHandle 必须是源节点真实存在的出端口
  if (!outPorts.some((p) => p.id === (conn.sourceHandle ?? "out"))) return false;
  return !editor.edges.some(
    (e) =>
      e.source === conn.source &&
      e.target === conn.target &&
      (e.sourceHandle ?? "out") === (conn.sourceHandle ?? "out"),
  );
}

export function onConnect(conn: Connection): void {
  if (!isValidConnection(conn)) return;
  editor.edges.push({
    id: `e_${conn.source}_${conn.sourceHandle ?? "out"}_${conn.target}`,
    source: conn.source,
    sourceHandle: conn.sourceHandle ?? "out",
    target: conn.target,
    targetHandle: conn.targetHandle ?? "in",
    label: edgeLabel(
      editor.nodes.find((n) => n.id === conn.source),
      conn.sourceHandle ?? "out",
    ),
  });
  editor.dirty = true;
}

// ---- 保存 / 发布 ----

export async function save(): Promise<boolean> {
  if (!editor.workflowId) return false;
  try {
    editor.version = await api.updateWorkflow(editor.workflowId, flowToDefinition());
    editor.dirty = false;
    ui.error = null;
    ui.info = `已保存 v${editor.version}`;
    await refreshWorkflows();
    return true;
  } catch (e) {
    ui.error = errText(e);
    ui.info = null;
    return false;
  }
}

export async function publish(): Promise<void> {
  if (!editor.workflowId) return;
  // 发布前先把画布落库，保证发布的版本就是当前图
  if (!(await save())) return;
  try {
    await api.publishWorkflow(editor.workflowId, editor.version);
    editor.publishedVersion = editor.version;
    ui.info = `已发布 v${editor.version}`;
    await refreshWorkflows();
  } catch (e) {
    ui.error = errText(e);
  }
}

// ---- 切换守卫与 sub_workflow 钻取 ----

/** 有未保存修改时先确认；返回 true 表示可以继续切换 */
export function confirmDiscardIfDirty(): boolean {
  return (
    !editor.dirty || window.confirm("当前工作流有未保存的修改，切换后会丢失，确定继续？")
  );
}

/** 左侧工作流列表的切换入口：脏检查 + 清空钻取栈 */
export async function switchWorkflow(id: string): Promise<void> {
  if (id === editor.workflowId) return;
  if (!confirmDiscardIfDirty()) return;
  editor.breadcrumb = [];
  await selectWorkflow(id);
}

/** 双击 sub_workflow 节点钻取目标工作流 */
export async function drillIntoSubWorkflow(nodeId: string): Promise<void> {
  const node = editor.nodes.find((n) => n.id === nodeId);
  if (!node || node.data.nodeType.type !== "sub_workflow") return;
  const target = node.data.params.workflow_id;
  if (typeof target !== "string" || !target) {
    ui.error = "先在参数面板为子流程节点选择目标工作流";
    return;
  }
  if (!editor.workflows.some((w) => w.workflow_id === target)) {
    ui.error = "目标工作流不存在或已删除";
    return;
  }
  if (target === editor.workflowId) {
    ui.error = "子流程不能指向当前工作流自身";
    return;
  }
  if (!confirmDiscardIfDirty()) return;
  const from = { workflowId: editor.workflowId!, name: editor.workflowName };
  editor.breadcrumb.push(from);
  await selectWorkflow(target);
  // selectWorkflow 失败（如目标恰好被删）时回退栈，避免面包屑悬空
  if (editor.workflowId !== target) editor.breadcrumb.pop();
}

/** 面包屑点击：回到第 index 层祖先工作流 */
export async function jumpToBreadcrumb(index: number): Promise<void> {
  const target = editor.breadcrumb[index];
  if (!target) return;
  if (!confirmDiscardIfDirty()) return;
  const stack = editor.breadcrumb.slice(0, index);
  editor.breadcrumb = stack;
  await selectWorkflow(target.workflowId);
  if (editor.workflowId !== target.workflowId) {
    editor.breadcrumb = [];
  }
}
