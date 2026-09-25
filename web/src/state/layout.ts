import type { Position } from "../types";

interface EdgeLike {
  from: string;
  to: string;
}

/** 每个节点的拓扑深度（最长入链长度）；环防御：遇环按 0 处理（服务端 validate 会拦，画布先画出来） */
function depthsOf(nodeIds: string[], edges: EdgeLike[]): Map<string, number> {
  const incoming = new Map<string, string[]>();
  for (const e of edges) {
    const list = incoming.get(e.to) ?? [];
    list.push(e.from);
    incoming.set(e.to, list);
  }
  const cache = new Map<string, number>();
  function depthOf(id: string, stack: Set<string>): number {
    const cached = cache.get(id);
    if (cached !== undefined) return cached;
    if (stack.has(id)) return 0;
    stack.add(id);
    let depth = 0;
    for (const from of incoming.get(id) ?? []) {
      depth = Math.max(depth, depthOf(from, stack) + 1);
    }
    stack.delete(id);
    cache.set(id, depth);
    return depth;
  }
  for (const id of nodeIds) depthOf(id, new Set());
  return cache;
}

/** 拓扑分层布局：每层一列，层内纵排。onlyMissing 给定时只为这些节点分配行号（旧定义缺 position 的兜底场景） */
export function layeredPositions(
  nodeIds: string[],
  edges: EdgeLike[],
  onlyMissing?: Set<string>,
): Map<string, Position> {
  const depths = depthsOf(nodeIds, edges);
  const targets = onlyMissing ?? new Set(nodeIds);
  const perLayer = new Map<number, number>();
  const result = new Map<string, Position>();
  for (const id of nodeIds) {
    if (!targets.has(id)) continue;
    const depth = depths.get(id) ?? 0;
    const row = perLayer.get(depth) ?? 0;
    perLayer.set(depth, row + 1);
    result.set(id, { x: 80 + depth * 220, y: 80 + row * 120 });
  }
  return result;
}
