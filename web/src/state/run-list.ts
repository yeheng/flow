import type { RunRecord } from "../types";

/** 游标分页合并：page 追加到 existing 尾部，按 id 去重（轮询刷新首页时新旧页可能重叠） */
export function mergeRunPage(existing: RunRecord[], page: RunRecord[]): RunRecord[] {
  const seen = new Set(existing.map((r) => r.id));
  return [...existing, ...page.filter((r) => !seen.has(r.id))];
}

/** 下一页游标：当前列表最末（最旧）一条的 id；空列表不可翻页 */
export function nextCursor(runs: RunRecord[]): string | null {
  return runs.length > 0 ? runs[runs.length - 1].id : null;
}

/** run.source 的中文展示映射（词汇表：flow-dto DbRunSource）；未知值原样显示 */
export function sourceLabel(source: string): string {
  switch (source) {
    case "manual":
      return "手动";
    case "schedule":
      return "定时调度";
    case "webhook":
      return "Webhook";
    case "sub_workflow":
      return "子流程";
    default:
      return source;
  }
}
