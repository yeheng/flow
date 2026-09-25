import type { EditorEdge, EditorNode } from "./editor";
import type { NodeTypeDesc } from "../types";

export interface ValidationError {
  /** 能定位到具体节点的错误带 nodeId，用于画布标红与点击定位 */
  nodeId?: string;
  message: string;
}

/**
 * 保存前的前端预校验，规则对齐 crates/flow-engine/src/model.rs Definition::validate；
 * 另加两项前端元数据可做的检查：max_instances 上限与 params_schema required 非空。
 * 服务端 validate 仍是最终裁决。
 */
export function validateDefinition(
  nodes: EditorNode[],
  edges: EditorEdge[],
  nodeTypes: NodeTypeDesc[],
): ValidationError[] {
  const errors: ValidationError[] = [];
  if (nodes.length === 0) {
    errors.push({ message: "工作流没有任何节点" });
    return errors;
  }

  const typeOf = new Map(nodeTypes.map((t) => [t.type, t]));
  const byId = new Map<string, EditorNode>();

  for (const node of nodes) {
    if (!node.id.trim()) {
      errors.push({ message: "存在空的节点 id" });
      continue;
    }
    if (byId.has(node.id)) {
      errors.push({ nodeId: node.id, message: `节点 id 重复：${node.id}` });
      continue;
    }
    byId.set(node.id, node);
    const desc = node.data?.nodeType;
    if (!desc || !typeOf.has(desc.type)) {
      errors.push({ nodeId: node.id, message: `节点 ${node.id} 的类型未知` });
      continue;
    }
    // required 参数非空（undefined / null / 空串视为缺失）
    for (const key of desc.params_schema.required ?? []) {
      const v = node.data.params[key];
      if (v === undefined || v === null || v === "") {
        const label = desc.params_schema.properties?.[key]?.["x-label"] ?? key;
        errors.push({ nodeId: node.id, message: `节点「${node.id}」缺少必填参数 ${label}` });
      }
    }
  }
  // max_instances 上限
  for (const desc of nodeTypes) {
    const max = desc.max_instances ?? 0;
    if (max <= 0) continue;
    const count = nodes.filter((n) => n.data?.nodeType.type === desc.type).length;
    if (count > max) {
      errors.push({ message: `节点类型「${desc.label}」最多 ${max} 个，当前 ${count} 个` });
    }
  }

  const isType = (n: EditorNode, t: string) => n.data?.nodeType.type === t;
  const starts = nodes.filter((n) => isType(n, "start"));
  if (starts.length !== 1) {
    errors.push({
      nodeId: starts.length > 1 ? starts[1].id : undefined,
      message: `必须且只能有一个 start 节点，当前有 ${starts.length} 个`,
    });
  }
  if (!nodes.some((n) => isType(n, "end"))) {
    errors.push({ message: "至少需要一个 end 节点" });
  }

  const incoming = new Map<string, EditorEdge[]>();
  const outgoing = new Map<string, EditorEdge[]>();
  const edgeKeys = new Set<string>();
  for (const e of edges) {
    if (!byId.has(e.source)) errors.push({ message: `边的起点不存在：${e.source}` });
    if (!byId.has(e.target)) errors.push({ message: `边的终点不存在：${e.target}` });
    if (e.source === e.target) {
      errors.push({ nodeId: e.source, message: `节点 ${e.source} 存在自环` });
      continue;
    }
    const key = `${e.source}->${e.target}#${e.sourceHandle ?? "out"}`;
    if (edgeKeys.has(key)) {
      errors.push({ message: `重复的边：${e.source} -> ${e.target}` });
      continue;
    }
    edgeKeys.add(key);
    const source = byId.get(e.source);
    if (source && isType(source, "condition")) {
      const handle = e.sourceHandle ?? "out";
      if (handle !== "true" && handle !== "false") {
        errors.push({
          nodeId: e.source,
          message: `condition 节点 ${e.source} 的出边端口必须是 true/false`,
        });
      }
    }
    incoming.set(e.target, [...(incoming.get(e.target) ?? []), e]);
    outgoing.set(e.source, [...(outgoing.get(e.source) ?? []), e]);
  }

  for (const node of nodes) {
    const inc = incoming.get(node.id) ?? [];
    const out = outgoing.get(node.id) ?? [];
    if (isType(node, "start")) {
      if (inc.length > 0) errors.push({ nodeId: node.id, message: `start 节点不能有入边` });
    } else if (isType(node, "end")) {
      if (out.length > 0) errors.push({ nodeId: node.id, message: `end 节点不能有出边` });
    } else if (inc.length === 0) {
      errors.push({ nodeId: node.id, message: `节点 ${node.id} 没有入边，永远无法触发` });
    }
  }

  // DAG 无环：Kahn 拓扑排序
  const indegree = new Map<string, number>(
    nodes.map((n) => [n.id, (incoming.get(n.id) ?? []).length]),
  );
  const queue = nodes.filter((n) => indegree.get(n.id) === 0).map((n) => n.id);
  let sorted = 0;
  while (queue.length > 0) {
    const id = queue.shift()!;
    sorted++;
    for (const e of outgoing.get(id) ?? []) {
      const d = (indegree.get(e.target) ?? 0) - 1;
      indegree.set(e.target, d);
      if (d === 0) queue.push(e.target);
    }
  }
  if (sorted !== nodes.length) {
    errors.push({ message: "工作流存在环，必须是 DAG" });
  } else if (starts.length === 1) {
    // 从 start 可达性（有环时跳过，避免误报叠加）
    const visited = new Set<string>();
    const stack = [starts[0].id];
    while (stack.length > 0) {
      const id = stack.pop()!;
      if (visited.has(id)) continue;
      visited.add(id);
      for (const e of outgoing.get(id) ?? []) stack.push(e.target);
    }
    const unreachable = nodes.filter((n) => !visited.has(n.id));
    if (unreachable.length > 0) {
      errors.push({
        nodeId: unreachable[0].id,
        message: `存在从 start 不可达的节点：${unreachable.map((n) => n.id).join(", ")}`,
      });
    }
  }

  return errors;
}
