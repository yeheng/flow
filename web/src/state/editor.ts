import { computed, reactive, ref, watch } from "vue";
import type { Connection } from "@vue-flow/core";
import * as api from "../api/flow";
import { RpcError, client, errText } from "../rpc/client";
import type {
  Definition,
  DefinitionEdge,
  NodeTypeDesc,
  PortDesc,
  Position,
  WorkflowSummary,
} from "../types";
import { commit, resetHistory } from "./history";
import { layeredPositions } from "./layout";
import { confirmDialog } from "./modal";
import { toast } from "./toast";
import { validateDefinition, type ValidationError } from "./validation";

export interface FlowNodeData {
  name: string;
  nodeType: NodeTypeDesc;
  params: Record<string, unknown>;
  /** 只读运行画布（RunCanvas）直接注入的节点运行态；编辑器画布为空，走 monitor 查询 */
  runState?: string | null;
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
  /** vue-flow v-model 回写的选中态（框选/多选、复制粘贴、删除用） */
  selected?: boolean;
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
  /** vue-flow v-model 回写的选中态 */
  selected?: boolean;
}

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
});

/**
 * 脏标记是派生值：当前画布规范化后的 definition 与保存点不同即为脏。
 * undo 回到保存点快照时自动变干净；保存/加载会移动保存点。
 */
let savedDefKey: string | null = null;

function defKey(): string {
  return JSON.stringify(flowToDefinition());
}

export const dirty = computed(() => savedDefKey !== null && defKey() !== savedDefKey);

/** 把当前画布记为保存点（加载/保存成功后调用；测试也可直接使用） */
export function markSaved(): void {
  savedDefKey = defKey();
}

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

/** 拉取节点类型清单（幂等）；编辑器与运行详情页共用。
 * 失败不留空清单哑等——重连后由 onReconnect 重试 */
export async function ensureNodeTypes(): Promise<boolean> {
  if (editor.nodeTypes.length > 0) return true;
  return loadNodeTypes();
}

async function loadNodeTypes(): Promise<boolean> {
  try {
    editor.nodeTypes = await api.listNodeTypes();
    return true;
  } catch (e) {
    toast.error(`无法连接 flow-server：${errText(e)}`);
    return false;
  }
}

// nodeTypes 缺失会让 addNode 静默失败、seedCanvas/解析定义直接抛错：
// 启动时拉取失败后，断线重连自动重拉，恢复画布可用性
client.onReconnect(() => {
  if (editor.nodeTypes.length > 0) return;
  void loadNodeTypes();
});

export async function refreshWorkflows(): Promise<void> {
  try {
    editor.workflows = await api.listWorkflows();
  } catch (e) {
    toast.error(errText(e));
  }
}

/** 新工作流还没有任何版本：seed 一个合法的最小定义（start → end），等用户保存。
 * nodeTypes 未就绪时不 seed（不硬解引用），清空画布并提示，返回 false */
function seedCanvas(): boolean {
  const start = nodeTypeDesc("start");
  const end = nodeTypeDesc("end");
  if (!start || !end) {
    editor.nodes = [];
    editor.edges = [];
    toast.error("节点类型清单未加载，无法初始化新工作流画布；连接恢复后请重新打开");
    return false;
  }
  editor.nodes = [
    {
      id: "start_1",
      type: "flow",
      position: { x: 80, y: 200 },
      data: { name: start.label, nodeType: start, params: {} },
    },
    {
      id: "end_1",
      type: "flow",
      position: { x: 480, y: 200 },
      data: { name: end.label, nodeType: end, params: {} },
    },
  ];
  editor.edges = [
    {
      id: "e_start_1_out_end_1",
      source: "start_1",
      sourceHandle: "out",
      target: "end_1",
      targetHandle: "in",
    },
  ];
  return true;
}

export async function selectWorkflow(id: string): Promise<void> {
  // nodeTypes 未就绪时 toFlow/seedCanvas 的类型断言都会落空：先挡住并提示
  if (editor.nodeTypes.length === 0) {
    toast.error("节点类型清单未加载，无法打开工作流；连接恢复后请重试");
    return;
  }
  try {
    const w = await api.getWorkflow(id);
    editor.version = w.version;
    editor.publishedVersion = w.published_version;
    const flow = definitionToFlow(w.definition);
    editor.nodes = flow.nodes;
    editor.edges = flow.edges;
  } catch (e) {
    // workflow.get 对无版本的工作流报「不存在」（-32011），按空画布处理
    if (!(e instanceof RpcError && e.code === -32011)) {
      toast.error(errText(e));
      return;
    }
    editor.version = 0;
    editor.publishedVersion = null;
    if (!seedCanvas()) return;
  }
  editor.workflowId = id;
  editor.workflowName = editor.workflows.find((w) => w.workflow_id === id)?.name ?? "";
  editor.selectedNodeId = null;
  markSaved();
  resetHistory();
}

/** 新建并选中；返回新工作流 id，失败返回 null */
export async function createWorkflow(name: string): Promise<string | null> {
  try {
    const id = await api.createWorkflow(name);
    await refreshWorkflows();
    await selectWorkflow(id);
    return id;
  } catch (e) {
    toast.error(errText(e));
    return null;
  }
}

export async function removeWorkflow(id: string): Promise<void> {
  try {
    await api.deleteWorkflow(id);
    if (editor.workflowId === id) {
      editor.workflowId = null;
      editor.nodes = [];
      editor.edges = [];
      markSaved();
      resetHistory();
    }
    await refreshWorkflows();
  } catch (e) {
    toast.error(errText(e));
  }
}

// ---- Vue Flow <-> Definition 双向转换 ----

/**
 * 旧定义可能缺 position：按拓扑分层给缺失节点兜底坐标（每层一列，层内纵排）。
 * 已有坐标的节点不动。
 */
function fallbackPositions(def: Definition): Map<string, Position> {
  const missing = new Set(def.nodes.filter((n) => !n.position).map((n) => n.id));
  if (missing.size === 0) return new Map();
  return layeredPositions(
    def.nodes.map((n) => n.id),
    def.edges,
    missing,
  );
}

/** Definition → 画布 nodes/edges（纯转换，不写 editor 状态；编辑器与运行详情只读画布共用） */
export function definitionToFlow(def: Definition): { nodes: EditorNode[]; edges: EditorEdge[] } {
  const fallback = fallbackPositions(def);
  const nodes = def.nodes.map((n) => ({
    id: n.id,
    type: "flow" as const,
    position: n.position ?? fallback.get(n.id) ?? { x: 80, y: 80 },
    data: {
      name: n.name ?? "",
      nodeType: nodeTypeDesc(n.type)!,
      params: (n.params ?? {}) as Record<string, unknown>,
    },
  }));
  // 单出端口节点的唯一出口 handle id 是 "out"，definition 里不落 port
  const edges = def.edges.map((e) => ({
    id: `e_${e.from}_${e.port ?? "out"}_${e.to}`,
    source: e.from,
    sourceHandle: e.port ?? "out",
    target: e.to,
    targetHandle: "in",
    label: edgeLabel(
      nodes.find((n) => n.id === e.from),
      e.port ?? "out",
    ),
  }));
  return { nodes, edges };
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
    toast.error(`节点类型「${desc.label}」最多 ${max} 个`);
    return false;
  }
  let seq = 1;
  while (editor.nodes.some((n) => n.id === `${type}_${seq}`)) seq++;
  // 默认值在节点创建时从 schema 落进 params，与后端 default 语义一致。
  // default 可能是 reactive Proxy（structuredClone 无法克隆），它来自 JSON-RPC，用 JSON 往返脱壳
  const params: Record<string, unknown> = {};
  for (const [key, prop] of Object.entries(desc.params_schema.properties ?? {})) {
    if (prop.default !== undefined) params[key] = JSON.parse(JSON.stringify(prop.default));
  }
  commit();
  editor.nodes.push({
    id: `${type}_${seq}`,
    type: "flow",
    position,
    data: { name: desc.label, nodeType: desc, params },
  });
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
  // v-model 回写时 Vue Flow 会用已入库的 Edge 再校验一次，去重检查须排除边自身
  const selfId = "id" in conn ? (conn as { id?: string }).id : undefined;
  return !editor.edges.some(
    (e) =>
      e.id !== selfId &&
      e.source === conn.source &&
      e.target === conn.target &&
      (e.sourceHandle ?? "out") === (conn.sourceHandle ?? "out"),
  );
}

export function onConnect(conn: Connection): void {
  if (!isValidConnection(conn)) return;
  commit();
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
}

// ---- 保存 / 发布 ----

export async function save(): Promise<boolean> {
  if (!editor.workflowId) return false;
  // 保存前过前端预校验：有错不打 RPC（服务端 validate 仍是最终裁决）
  const errs = validateNow();
  if (errs.length > 0) {
    validationUi.open = true;
    toast.error(`工作流定义有 ${errs.length} 处校验错误，请先修复`);
    return false;
  }
  try {
    editor.version = await api.updateWorkflow(editor.workflowId, flowToDefinition());
    markSaved();
    toast.success(`已保存 v${editor.version}`);
    await refreshWorkflows();
    return true;
  } catch (e) {
    toast.error(errText(e));
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
    toast.success(`已发布 v${editor.version}`);
    await refreshWorkflows();
  } catch (e) {
    toast.error(errText(e));
  }
}

// ---- 切换守卫与 sub_workflow 钻取 ----

/** 有未保存修改时先确认；返回 true 表示可以继续切换 */
export async function confirmDiscardIfDirty(): Promise<boolean> {
  return (
    !dirty.value || (await confirmDialog("当前工作流有未保存的修改，切换后会丢失，确定继续？"))
  );
}

/** 双击 sub_workflow 节点钻取目标工作流 */
export async function drillIntoSubWorkflow(nodeId: string): Promise<void> {
  const node = editor.nodes.find((n) => n.id === nodeId);
  if (!node || node.data.nodeType.type !== "sub_workflow") return;
  const target = node.data.params.workflow_id;
  if (typeof target !== "string" || !target) {
    toast.error("先在参数面板为子流程节点选择目标工作流");
    return;
  }
  if (!editor.workflows.some((w) => w.workflow_id === target)) {
    toast.error("目标工作流不存在或已删除");
    return;
  }
  if (target === editor.workflowId) {
    toast.error("子流程不能指向当前工作流自身");
    return;
  }
  if (!(await confirmDiscardIfDirty())) return;
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
  if (!(await confirmDiscardIfDirty())) return;
  const stack = editor.breadcrumb.slice(0, index);
  editor.breadcrumb = stack;
  await selectWorkflow(target.workflowId);
  if (editor.workflowId !== target.workflowId) {
    editor.breadcrumb = [];
  }
}

// ---- 复制 / 粘贴（内存剪贴板） ----

interface ClipboardNode {
  id: string;
  type: string;
  name: string;
  params: Record<string, unknown>;
  position: Position;
}

interface ClipboardEdge {
  source: string;
  target: string;
  sourceHandle?: string | null;
  targetHandle?: string | null;
}

let clipboard: { nodes: ClipboardNode[]; edges: ClipboardEdge[] } | null = null;
/** 同一份剪贴板连续粘贴的偏移序号；新复制重置 */
let pasteSerial = 0;

/** 复制选中节点及其内部边（两端都选中的边）；无选中返回 false（调用方不拦截浏览器默认行为） */
export function copySelection(): boolean {
  const selected = editor.nodes.filter((n) => n.selected);
  if (selected.length === 0) return false;
  const ids = new Set(selected.map((n) => n.id));
  clipboard = {
    nodes: selected.map((n) => ({
      id: n.id,
      type: n.data.nodeType.type,
      name: n.data.name,
      params: JSON.parse(JSON.stringify(n.data.params ?? {})),
      position: { x: n.position.x, y: n.position.y },
    })),
    edges: editor.edges
      .filter((e) => ids.has(e.source) && ids.has(e.target))
      .map((e) => ({
        source: e.source,
        target: e.target,
        sourceHandle: e.sourceHandle,
        targetHandle: e.targetHandle,
      })),
  };
  pasteSerial = 0;
  return true;
}

/** 粘贴：重新生成节点 id、位置按粘贴序号偏移 32px、内部边重连到新 id；外部边本就不在剪贴板里 */
export function pasteClipboard(): boolean {
  if (!clipboard || clipboard.nodes.length === 0) return false;
  const offset = 32 * (pasteSerial + 1);
  const idMap = new Map<string, string>();
  const newNodes: EditorNode[] = [];
  let skipped = 0;
  for (const cn of clipboard.nodes) {
    const desc = nodeTypeDesc(cn.type);
    if (!desc) {
      skipped++;
      continue;
    }
    const max = desc.max_instances ?? 0;
    const count =
      editor.nodes.filter((n) => n.data.nodeType.type === cn.type).length +
      newNodes.filter((n) => n.data.nodeType.type === cn.type).length;
    if (max > 0 && count >= max) {
      skipped++;
      continue;
    }
    let seq = 1;
    while (
      editor.nodes.some((n) => n.id === `${cn.type}_${seq}`) ||
      newNodes.some((n) => n.id === `${cn.type}_${seq}`)
    ) {
      seq++;
    }
    const id = `${cn.type}_${seq}`;
    idMap.set(cn.id, id);
    newNodes.push({
      id,
      type: "flow",
      position: { x: cn.position.x + offset, y: cn.position.y + offset },
      data: { name: cn.name, nodeType: desc, params: JSON.parse(JSON.stringify(cn.params)) },
      selected: true,
    });
  }
  if (newNodes.length === 0) {
    if (skipped > 0) toast.info(`${skipped} 个节点因数量上限未粘贴`);
    return false;
  }
  commit();
  pasteSerial++;
  // 选中粘贴结果，取消旧选择
  for (const n of editor.nodes) n.selected = false;
  editor.nodes.push(...newNodes);
  for (const ce of clipboard.edges) {
    const source = idMap.get(ce.source);
    const target = idMap.get(ce.target);
    if (!source || !target) continue;
    editor.edges.push({
      id: `e_${source}_${ce.sourceHandle ?? "out"}_${target}`,
      source,
      sourceHandle: ce.sourceHandle ?? "out",
      target,
      targetHandle: ce.targetHandle ?? "in",
      label: edgeLabel(
        editor.nodes.find((n) => n.id === source),
        ce.sourceHandle ?? "out",
      ),
    });
  }
  if (skipped > 0) toast.info(`${skipped} 个节点因数量上限未粘贴`);
  return true;
}

// ---- 自动布局 ----

/** 对全部节点做拓扑分层布局（每层一列，层内纵排），入 undo 栈 */
export function autoLayout(): void {
  if (editor.nodes.length === 0) return;
  commit();
  const positions = layeredPositions(
    editor.nodes.map((n) => n.id),
    editor.edges.map((e) => ({ from: e.source, to: e.target })),
  );
  for (const n of editor.nodes) {
    const p = positions.get(n.id);
    if (p) n.position = p;
  }
}

// ---- 前端预校验状态（规则见 validation.ts） ----

export const validationErrors = ref<ValidationError[]>([]);
/** 顶部条错误列表 popover 开关（保存被拦时自动展开） */
export const validationUi = reactive({ open: false });

let validationTimer: ReturnType<typeof setTimeout> | null = null;
watch(
  [() => editor.nodes, () => editor.edges],
  () => {
    if (validationTimer) clearTimeout(validationTimer);
    validationTimer = setTimeout(() => {
      validationErrors.value = validateDefinition(editor.nodes, editor.edges, editor.nodeTypes);
    }, 150);
  },
  { deep: true, immediate: true },
);

/** 同步重算（保存门禁用，不等 debounce） */
export function validateNow(): ValidationError[] {
  validationErrors.value = validateDefinition(editor.nodes, editor.edges, editor.nodeTypes);
  return validationErrors.value;
}

/** 单个节点的首条校验错误（画布标红/角标用） */
export function nodeValidationError(id: string): string | null {
  return validationErrors.value.find((e) => e.nodeId === id)?.message ?? null;
}
