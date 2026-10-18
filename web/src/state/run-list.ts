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

/** 状态/来源筛选词汇（与 flow-dto RunStatus/RunSource 对齐）：选项与 URL 往返共用 */
export const RUN_STATUS_OPTIONS: { value: string; label: string }[] = [
  { value: "", label: "全部状态" },
  { value: "running", label: "运行中" },
  { value: "succeeded", label: "成功" },
  { value: "failed", label: "失败" },
  { value: "cancelled", label: "已取消" },
  { value: "awaiting_resume", label: "挂起待恢复" },
];

export const RUN_SOURCE_OPTIONS: { value: string; label: string }[] = [
  { value: "", label: "全部来源" },
  { value: "manual", label: "手动" },
  { value: "schedule", label: "定时调度" },
  { value: "webhook", label: "Webhook" },
  { value: "sub_workflow", label: "子流程" },
];

const STATUS_VALUES = RUN_STATUS_OPTIONS.map((o) => o.value);
const SOURCE_VALUES = RUN_SOURCE_OPTIONS.map((o) => o.value);

/** 路由 query → 筛选条件：未知值回落「全部」，避免把非法值发给服务端 */
export function parseRunFilters(query: Record<string, unknown>): {
  status: string;
  source: string;
} {
  const status = typeof query.status === "string" ? query.status : "";
  const source = typeof query.source === "string" ? query.source : "";
  return {
    status: STATUS_VALUES.includes(status) ? status : "",
    source: SOURCE_VALUES.includes(source) ? source : "",
  };
}

/** 筛选条件 → 路由 query：「全部」不入 URL（链接保持干净） */
export function serializeRunFilters(filters: {
  status: string;
  source: string;
}): Record<string, string> {
  const query: Record<string, string> = {};
  if (filters.status) query.status = filters.status;
  if (filters.source) query.source = filters.source;
  return query;
}
