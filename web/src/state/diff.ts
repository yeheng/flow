import type { Definition, DefinitionEdge } from "../types";

export interface FieldChange {
  /** 变更位置：type / name / params.<key> */
  field: string;
  from: unknown;
  to: unknown;
}

export interface NodeChange {
  id: string;
  changes: FieldChange[];
}

export interface DefinitionDiff {
  /** 节点 id 列表（b 相对 a） */
  nodesAdded: string[];
  nodesRemoved: string[];
  nodesChanged: NodeChange[];
  edgesAdded: DefinitionEdge[];
  edgesRemoved: DefinitionEdge[];
}

function edgeKey(e: DefinitionEdge): string {
  return `${e.from}->${e.to}#${e.port ?? ""}`;
}

/** JSON 值深比较（键序无关） */
function jsonEqual(a: unknown, b: unknown): boolean {
  if (Object.is(a, b)) return true;
  if (typeof a !== "object" || typeof b !== "object" || a === null || b === null) return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  const ka = Object.keys(a as object);
  const kb = Object.keys(b as object);
  if (ka.length !== kb.length) return false;
  return ka.every((k) =>
    jsonEqual((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k]),
  );
}

/**
 * 结构化定义 diff：a 为基线（旧），b 为目标（新）。
 * 节点按 id 配对，字段级对比 type/name/params（params 逐键深比较）；
 * position 只是画布布局，不算变更。
 */
export function diffDefinitions(a: Definition, b: Definition): DefinitionDiff {
  const aNodes = new Map(a.nodes.map((n) => [n.id, n]));
  const bNodes = new Map(b.nodes.map((n) => [n.id, n]));

  const nodesAdded = b.nodes.filter((n) => !aNodes.has(n.id)).map((n) => n.id);
  const nodesRemoved = a.nodes.filter((n) => !bNodes.has(n.id)).map((n) => n.id);

  const nodesChanged: NodeChange[] = [];
  for (const bn of b.nodes) {
    const an = aNodes.get(bn.id);
    if (!an) continue;
    const changes: FieldChange[] = [];
    if (an.type !== bn.type) changes.push({ field: "type", from: an.type, to: bn.type });
    if ((an.name ?? "") !== (bn.name ?? "")) {
      changes.push({ field: "name", from: an.name ?? "", to: bn.name ?? "" });
    }
    const aParams = (an.params ?? {}) as Record<string, unknown>;
    const bParams = (bn.params ?? {}) as Record<string, unknown>;
    for (const key of new Set([...Object.keys(aParams), ...Object.keys(bParams)])) {
      if (!jsonEqual(aParams[key], bParams[key])) {
        changes.push({ field: `params.${key}`, from: aParams[key], to: bParams[key] });
      }
    }
    if (changes.length > 0) nodesChanged.push({ id: bn.id, changes });
  }

  const aEdges = new Map(a.edges.map((e) => [edgeKey(e), e]));
  const bEdges = new Map(b.edges.map((e) => [edgeKey(e), e]));
  const edgesAdded = b.edges.filter((e) => !aEdges.has(edgeKey(e)));
  const edgesRemoved = a.edges.filter((e) => !bEdges.has(edgeKey(e)));

  return { nodesAdded, nodesRemoved, nodesChanged, edgesAdded, edgesRemoved };
}

/** diff 是否为空（两个版本内容一致） */
export function isEmptyDiff(d: DefinitionDiff): boolean {
  return (
    d.nodesAdded.length === 0 &&
    d.nodesRemoved.length === 0 &&
    d.nodesChanged.length === 0 &&
    d.edgesAdded.length === 0 &&
    d.edgesRemoved.length === 0
  );
}
