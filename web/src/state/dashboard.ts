import type { RunRecord } from "../types";

/**
 * 仪表盘聚合：数据源是 run.list(limit 500) 的一页数据在客户端聚合。
 * 已知局限：run 超过 500 条时统计只覆盖最新 500 条，总量与成功率都不准；
 * 要精确统计需要后端加聚合接口（P4 之前不做）。
 */

export interface RunSummary {
  total: number;
  /** running / initializing / awaiting_resume：仍在执行 */
  active: number;
  succeeded: number;
  failed: number;
  cancelled: number;
  /** 终态 run 中 succeeded 的占比（0-1）；没有终态 run 时为 null */
  successRate: number | null;
}

const TERMINAL = new Set(["succeeded", "failed", "cancelled"]);

export function summarizeRuns(runs: RunRecord[]): RunSummary {
  const summary: RunSummary = {
    total: runs.length,
    active: 0,
    succeeded: 0,
    failed: 0,
    cancelled: 0,
    successRate: null,
  };
  for (const r of runs) {
    if (r.status === "succeeded") summary.succeeded++;
    else if (r.status === "failed") summary.failed++;
    else if (r.status === "cancelled") summary.cancelled++;
    else summary.active++;
  }
  const terminal = summary.succeeded + summary.failed + summary.cancelled;
  summary.successRate = terminal > 0 ? summary.succeeded / terminal : null;
  return summary;
}

export interface WorkflowRate {
  workflowId: string;
  total: number;
  succeeded: number;
  /** 终态成功率（0-1）；没有终态 run 时为 null */
  successRate: number | null;
}

/** 按工作流分组的成功率，按 run 总数倒序 */
export function perWorkflowRates(runs: RunRecord[]): WorkflowRate[] {
  const groups = new Map<string, { total: number; succeeded: number; terminal: number }>();
  for (const r of runs) {
    const g = groups.get(r.workflow_id) ?? { total: 0, succeeded: 0, terminal: 0 };
    g.total++;
    if (TERMINAL.has(r.status)) g.terminal++;
    if (r.status === "succeeded") g.succeeded++;
    groups.set(r.workflow_id, g);
  }
  return [...groups.entries()]
    .map(([workflowId, g]) => ({
      workflowId,
      total: g.total,
      succeeded: g.succeeded,
      successRate: g.terminal > 0 ? g.succeeded / g.terminal : null,
    }))
    .sort((a, b) => b.total - a.total);
}
