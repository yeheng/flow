import { computed, reactive, ref, shallowRef, watch } from "vue";
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
  /** secrets.list 下发的密钥名称列表（x-secret 字段的选择器选项） */
  secrets: string[];
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
  /** 右键拖拽连线时悬停的合法目标节点（画布高亮用，拖拽结束清空） */
  linkTargetId: string | null;
  /** sub_workflow 钻取栈：栈顶是当前工作流的直接父级 */
  breadcrumb: BreadcrumbEntry[];
}

export const editor = reactive<EditorState>({
  nodeTypes: [],
  secrets: [],
  workflows: [],
  workflowId: null,
  workflowName: "",
  version: 0,
  publishedVersion: null,
  nodes: [],
  edges: [],
  selectedNodeId: null,
  highlightNodeId: null,
  linkTargetId: null,
  breadcrumb: [],
});

/** 节点分类的中文标签（调色板分组与画布右键「添加节点」子菜单共用） */
export const categoryLabels: Record<string, string> = {
  control: "控制",
  compute: "计算",
  integration: "集成",
  human: "人工",
  composition: "组合",
  ai: "AI",
  notify: "通知",
};

/** 按 category 分组，保持 nodetypes.list 的出现顺序 */
export const nodeTypeGroups = computed(() => {
  const order: string[] = [];
  const byCat = new Map<string, NodeTypeDesc[]>();
  for (const nt of editor.nodeTypes) {
    if (!byCat.has(nt.category)) {
      byCat.set(nt.category, []);
      order.push(nt.category);
    }
    byCat.get(nt.category)!.push(nt);
  }
  return order.map((cat) => ({
    cat,
    label: categoryLabels[cat] ?? cat,
    items: byCat.get(cat)!,
  }));
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

/**
 * 未知节点类型的占位描述：服务端升级引入新类型时旧前端必须能渲染、编辑、保存，
 * 而不是白屏。给一个入/出端口的通用卡片 + 空 schema（参数面板无表单，params 原样透传）；
 * validation 会报「类型未知」标红提示。
 */
export function unknownTypeDesc(type: string): NodeTypeDesc {
  return {
    type,
    label: type,
    category: "unknown",
    ports: [
      { id: "in", label: "入" },
      { id: "out", label: "出" },
    ],
    params_schema: { type: "object", properties: {} },
  };
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

/** 拉取密钥名称清单（幂等）；secrets 缺失只影响 x-secret 字段的候选列表，
 * 不阻断编辑器——失败不提示（字段仍可手输名称），重连后静默重试 */
export async function ensureSecrets(): Promise<boolean> {
  if (editor.secrets.length > 0) return true;
  return loadSecrets();
}

async function loadSecrets(): Promise<boolean> {
  try {
    editor.secrets = (await api.listSecrets()).map((s) => s.name);
    return true;
  } catch {
    return false;
  }
}

/** 密钥增删后调用（设置页）：立刻重拉候选清单并解除幂等缓存 */
export async function refreshSecrets(): Promise<void> {
  editor.secrets = [];
  await loadSecrets();
}

client.onReconnect(() => {
  if (editor.secrets.length > 0) return;
  void loadSecrets();
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

export async function selectWorkflow(id: string, version?: number): Promise<void> {
  // nodeTypes 未就绪时 toFlow/seedCanvas 的类型断言都会落空：先挡住并提示
  if (editor.nodeTypes.length === 0) {
    toast.error("节点类型清单未加载，无法打开工作流；连接恢复后请重试");
    return;
  }
  try {
    const w = await api.getWorkflow(id, version);
    editor.version = w.version;
    editor.publishedVersion = w.published_version;
    const flow = definitionToFlow(w.definition);
    editor.nodes = flow.nodes;
    editor.edges = flow.edges;
  } catch (e) {
    // workflow.get 对无版本的工作流报「不存在」（-32011）：仅当未指定版本时按空画布 seed；
    // 指定版本时的 -32011 是「该版本不存在」，如实报错
    if (!(e instanceof RpcError && e.code === -32011) || version !== undefined) {
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

/** 当前工作流的最新版本号（workflows 清单未载时回落当前编辑版本） */
export const latestVersion = computed(
  () =>
    editor.workflows.find((w) => w.workflow_id === editor.workflowId)?.latest_version ??
    editor.version,
);

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
      // 未知类型降级为占位描述（前向兼容：旧前端渲染新定义不白屏）
      nodeType: nodeTypeDesc(n.type) ?? unknownTypeDesc(n.type),
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

/** 通用序列化：只剥离空值，不认识任何具体参数名（retry 等字段的空值语义由写入侧保证） */
function cleanParams(params: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === "") continue;
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
  for (const node of editor.nodes) node.selected = false;
  for (const edge of editor.edges) edge.selected = false;
  editor.nodes.push({
    id: `${type}_${seq}`,
    type: "flow",
    position,
    data: { name: desc.label, nodeType: desc, params },
    selected: true,
  });
  editor.selectedNodeId = `${type}_${seq}`;
  return true;
}

/** 菜单与快捷键共用删除入口；关联连线随节点一起删除，一次撤销完整恢复。 */
export function deleteSelection(): boolean {
  const ids = new Set(editor.nodes.filter((node) => node.selected).map((node) => node.id));
  if (ids.size === 0 && !editor.edges.some((edge) => edge.selected)) return false;
  commit();
  editor.nodes = editor.nodes.filter((node) => !ids.has(node.id));
  editor.edges = editor.edges.filter(
    (edge) => !edge.selected && !ids.has(edge.source) && !ids.has(edge.target),
  );
  if (editor.selectedNodeId && ids.has(editor.selectedNodeId)) editor.selectedNodeId = null;
  if (editor.highlightNodeId && ids.has(editor.highlightNodeId)) editor.highlightNodeId = null;
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
    // 软冲突检测：另一标签页/会话可能在我们加载后保存了新版本。
    // 版本 append-only 不会丢数据，但为避免静默分叉先确认。（workflow.update 尚无
    // base_version 强校验，这里有 TOCTOU 窗口，挡住的是最常见的多标签页场景）
    const latest = await api.getWorkflow(editor.workflowId);
    if (latest.version > editor.version) {
      const ok = await confirmDialog(
        `工作流已有更新的 v${latest.version}（当前画布基于 v${editor.version}）。` +
          `继续保存将以当前画布内容生成 v${latest.version + 1}。确定继续？`,
      );
      if (!ok) return false;
    }
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

// ---- 复制 / 粘贴 / 模板插入（共享同一套片段落地逻辑） ----

/** 片段形状：剪贴板与节点模板（服务端 NodeTemplate 的 nodes/edges）共用 */
export interface ClipboardNode {
  id: string;
  type: string;
  name: string;
  params: Record<string, unknown>;
  position: Position;
}

export interface ClipboardEdge {
  source: string;
  target: string;
  sourceHandle?: string | null;
  targetHandle?: string | null;
}

const clipboard = shallowRef<{ nodes: ClipboardNode[]; edges: ClipboardEdge[] } | null>(null);
export const canPaste = computed(() => clipboard.value !== null);
/** 同一份剪贴板连续粘贴的偏移序号；新复制重置 */
let pasteSerial = 0;

function selectionFragment(): { nodes: ClipboardNode[]; edges: ClipboardEdge[] } | null {
  const selected = editor.nodes.filter((n) => n.selected);
  if (selected.length === 0) return null;
  const ids = new Set(selected.map((n) => n.id));
  return {
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
}

/** 复制选中节点及其内部边（两端都选中的边）；无选中返回 false（调用方不拦截浏览器默认行为） */
export function copySelection(): boolean {
  const fragment = selectionFragment();
  if (!fragment) return false;
  clipboard.value = fragment;
  pasteSerial = 0;
  return true;
}

/** 创建副本不改写剪贴板；节点参数与内部连线沿用片段插入规则。 */
export function duplicateSelection(): boolean {
  const fragment = selectionFragment();
  return fragment ? insertFragment(fragment.nodes, fragment.edges, { offset: { x: 32, y: 32 } }) : false;
}

export interface InsertOptions {
  /** 落点偏移（px）。缺省用剪贴板的连续粘贴序号偏移。 */
  offset?: Position;
  /** 插入后是否选中新节点（默认选中，粘贴/模板插入的行为一致） */
  selected?: boolean;
}

/**
 * 片段落地：重新生成节点 id、按 offset 平移、内部边重连到新 id、尊重
 * max_instances。粘贴与模板插入共用这一份——差异只在片段来源与偏移。
 * 返回是否插入了任何节点（全部撞上限时为 false）。
 */
export function insertFragment(
  nodes: ClipboardNode[],
  edges: ClipboardEdge[],
  opts: InsertOptions = {},
): boolean {
  if (nodes.length === 0) return false;
  const offset = opts.offset ?? { x: 32 * (pasteSerial + 1), y: 32 * (pasteSerial + 1) };
  const idMap = new Map<string, string>();
  const newNodes: EditorNode[] = [];
  let skipped = 0;
  for (const cn of nodes) {
    // 未知类型同样用占位描述插入：params 不能丢，旧前端渲染新类型不白屏
    const desc = nodeTypeDesc(cn.type) ?? unknownTypeDesc(cn.type);
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
    const selected = opts.selected ?? true;
    newNodes.push({
      id,
      type: "flow",
      position: { x: cn.position.x + offset.x, y: cn.position.y + offset.y },
      data: { name: cn.name, nodeType: desc, params: JSON.parse(JSON.stringify(cn.params)) },
      selected,
    });
  }
  if (newNodes.length === 0) {
    if (skipped > 0) toast.info(`${skipped} 个节点因数量上限未插入`);
    return false;
  }
  commit();
  pasteSerial++;
  // 选中插入结果，取消旧选择（selected=false 时不动选择态）
  if ((opts.selected ?? true) === true) {
    for (const n of editor.nodes) n.selected = false;
    for (const e of editor.edges) e.selected = false;
    editor.selectedNodeId = newNodes[0].id;
  }
  editor.nodes.push(...newNodes);
  for (const ce of edges) {
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
  if (skipped > 0) toast.info(`${skipped} 个节点因数量上限未插入`);
  return true;
}

/** 粘贴：偏移按连续粘贴序号递增（同一份片段越粘越远，不叠在一起） */
export function pasteClipboard(position?: Position): boolean {
  const fragment = clipboard.value;
  if (!fragment) return false;
  const offset = position
    ? {
        x: position.x - Math.min(...fragment.nodes.map((node) => node.position.x)),
        y: position.y - Math.min(...fragment.nodes.map((node) => node.position.y)),
      }
    : undefined;
  return insertFragment(fragment.nodes, fragment.edges, { offset });
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
