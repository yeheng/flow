import { computed, reactive } from "vue";
import type { Connection } from "@vue-flow/core";
import * as api from "../api/flow";
import { RpcError, errText } from "../rpc/client";
import type {
  Definition,
  DefinitionEdge,
  NodeTypeDesc,
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
}

/** 全局提示条：error 优先于 info 展示 */
export const ui = reactive({
  error: null as string | null,
  info: null as string | null,
});

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
  dirty: false,
});

export const selectedNode = computed<EditorNode | null>(
  () => editor.nodes.find((n) => n.id === editor.selectedNodeId) ?? null,
);

export function nodeTypeDesc(type: string): NodeTypeDesc | undefined {
  return editor.nodeTypes.find((t) => t.type === type);
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

function toFlow(def: Definition): void {
  editor.nodes = def.nodes.map((n) => ({
    id: n.id,
    type: "flow",
    position: n.position ?? { x: 0, y: 0 },
    data: {
      name: n.name ?? "",
      nodeType: nodeTypeDesc(n.type)!,
      params: (n.params ?? {}) as Record<string, unknown>,
    },
  }));
  // 非 condition 节点的唯一出口 handle id 是 "out"，definition 里不落 port
  editor.edges = def.edges.map((e) => ({
    id: `e_${e.from}_${e.port ?? "out"}_${e.to}`,
    source: e.from,
    sourceHandle: e.port ?? "out",
    target: e.to,
    targetHandle: "in",
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
      if (source?.data?.nodeType.type === "condition") {
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
  const params: Record<string, unknown> = {};
  for (const p of desc.params) {
    if (p.default !== undefined) params[p.name] = structuredClone(p.default);
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

/** 交互层连线约束；服务端 validate 兜底 */
export function isValidConnection(conn: Connection): boolean {
  if (!conn.source || !conn.target || conn.source === conn.target) return false;
  const source = editor.nodes.find((n) => n.id === conn.source);
  const target = editor.nodes.find((n) => n.id === conn.target);
  if (!source || !target) return false;
  if (source.data!.nodeType.type === "end") return false; // end 无出边
  if (target.data!.nodeType.type === "start") return false; // start 无入边
  if (source.data!.nodeType.type === "condition") {
    // condition 只能从 true/false 出口连出
    if (conn.sourceHandle !== "true" && conn.sourceHandle !== "false") return false;
  }
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
