import { reactive } from "vue";
import { editor, type EditorEdge, type EditorNode } from "./editor";

/**
 * 编辑器撤销/重做：快照式历史栈。
 * 变更入口在动手前调 commit() 压入变更前快照；undo/redo 对换当前状态与栈顶。
 * 节点拖拽经 beginDrag/endDrag 合并为一条；参数连续输入经 commitParams 按 node+field 合并。
 */

export interface Snapshot {
  nodes: EditorNode[];
  edges: EditorEdge[];
}

const LIMIT = 100;
const past: Snapshot[] = [];
const future: Snapshot[] = [];

/** 顶部条按钮禁用态 */
export const history = reactive({ canUndo: false, canRedo: false });

function sync(): void {
  history.canUndo = past.length > 0;
  history.canRedo = future.length > 0;
}

function cloneNode(n: EditorNode): EditorNode {
  return {
    id: n.id,
    type: "flow",
    position: { x: n.position.x, y: n.position.y },
    data: {
      name: n.data.name,
      nodeType: n.data.nodeType,
      // params 可能带 reactive Proxy，JSON 往返脱壳
      params: JSON.parse(JSON.stringify(n.data.params ?? {})),
    },
  };
}

export function takeSnapshot(): Snapshot {
  return {
    nodes: editor.nodes.map(cloneNode),
    edges: editor.edges.map((e) => ({ ...e })),
  };
}

function pushPast(s: Snapshot): void {
  past.push(s);
  if (past.length > LIMIT) past.shift();
  future.length = 0;
}

/** 参数合并会话标记：同一 node+field 的连续输入只入一次栈；任何其他变更都会重置 */
let lastParamTag: string | null = null;

/** 结构变更（增删节点/边、连线、粘贴、自动布局）入口：动手前调用 */
export function commit(): void {
  pushPast(takeSnapshot());
  lastParamTag = null;
  sync();
}

/** 参数面板编辑入口：tag 为 `${nodeId}:${field}`，同 tag 连续调用合并为一条历史 */
export function commitParams(tag: string): void {
  if (tag === lastParamTag) return;
  commit();
  lastParamTag = tag;
}

// ---- 拖拽合并：dragStart 记基线，dragStop 确有位移才入栈 ----

let dragBaseline: Snapshot | null = null;

function positionKey(s: Snapshot): string {
  return JSON.stringify(
    s.nodes.map((n) => [n.id, Math.round(n.position.x), Math.round(n.position.y)]),
  );
}

export function beginDrag(): void {
  dragBaseline = takeSnapshot();
}

export function endDrag(): void {
  if (!dragBaseline) return;
  if (positionKey(dragBaseline) !== positionKey(takeSnapshot())) {
    pushPast(dragBaseline);
    lastParamTag = null;
  }
  dragBaseline = null;
  sync();
}

// ---- undo / redo ----

function apply(s: Snapshot): void {
  // 栈内快照再克隆一份回画布，避免后续编辑直接改到栈内容
  editor.nodes = s.nodes.map(cloneNode);
  editor.edges = s.edges.map((e) => ({ ...e }));
  if (editor.selectedNodeId && !editor.nodes.some((n) => n.id === editor.selectedNodeId)) {
    editor.selectedNodeId = null;
  }
}

export function undo(): void {
  const s = past.pop();
  if (!s) return;
  future.push(takeSnapshot());
  apply(s);
  lastParamTag = null;
  sync();
}

export function redo(): void {
  const s = future.pop();
  if (!s) return;
  past.push(takeSnapshot());
  apply(s);
  lastParamTag = null;
  sync();
}

/** 切换/加载工作流时清空历史 */
export function resetHistory(): void {
  past.length = 0;
  future.length = 0;
  lastParamTag = null;
  dragBaseline = null;
  sync();
}
