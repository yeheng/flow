/**
 * 活动流（「活动」tab）的纯投影：时间线节点 + 观测日志合并成
 * 面向普通用户的执行叙事。数据全部来自 monitor 的两个权威面
 * （timeline = 状态权威，observation = 过程叙事），本模块只做排序、
 * 过滤与格式化，不做任何 IO。
 */
import type { LogLine, TimelineNode } from "../types";

export interface ActivityEntry {
  node: TimelineNode;
  /** 该节点的观测日志（全部尝试，按到达序） */
  logs: LogLine[];
}

/** start/end 是零工作量的流程框架节点：活动视图不讲它们的「故事」 */
export function isFrameNode(node: TimelineNode): boolean {
  return node.type === "start" || node.type === "end";
}

/**
 * 活动条目：按开始时间讲「故事」（先发生的前面）；尚未开始的节点按
 * 定义顺序垫底，让用户预知还有哪些步骤。ES2019+ 的 sort 稳定，
 * 同刻开始的并行分支保持定义顺序。
 */
export function activityEntries(
  nodes: TimelineNode[],
  logsByNode: Record<string, LogLine[]>,
): ActivityEntry[] {
  const started: TimelineNode[] = [];
  const pending: TimelineNode[] = [];
  for (const node of nodes) {
    if (isFrameNode(node)) continue;
    (node.started_at === null ? pending : started).push(node);
  }
  started.sort((a, b) => Date.parse(a.started_at!) - Date.parse(b.started_at!));
  return [...started, ...pending].map((node) => ({
    node,
    logs: logsByNode[node.id] ?? [],
  }));
}

/** 步骤进度：非框架节点中已到终态的数量（skipped/failed 也算「走过了」） */
export function runProgress(nodes: TimelineNode[]): { done: number; total: number } {
  let total = 0;
  let done = 0;
  for (const node of nodes) {
    if (isFrameNode(node)) continue;
    total++;
    if (node.state === "completed" || node.state === "failed" || node.state === "skipped") {
      done++;
    }
  }
  return { done, total };
}

/** 友好耗时：null（未开始）→ 空串；<1s 毫秒；<60s 秒；再往上分秒 */
export function friendlyDuration(ms: number | null): string {
  if (ms === null) return "";
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  const minutes = Math.floor(ms / 60_000);
  const seconds = Math.round((ms % 60_000) / 1000);
  return `${minutes}m ${seconds}s`;
}

/**
 * 运行时长（活动流头部）：最早节点开始 → 结束（或当前时刻）。
 * 没有任何节点开始时返回 null（头部显示「准备中」而不是误导性的 0s）。
 */
export function runElapsedMs(
  nodes: TimelineNode[],
  endedAt: string | null,
  now: number,
): number | null {
  let earliest: number | null = null;
  for (const node of nodes) {
    if (node.started_at === null) continue;
    const at = Date.parse(node.started_at);
    if (!Number.isNaN(at) && (earliest === null || at < earliest)) earliest = at;
  }
  if (earliest === null) return null;
  const end = endedAt !== null ? Date.parse(endedAt) : now;
  return Math.max(0, (Number.isNaN(end) ? now : end) - earliest);
}
