import type { WorkflowRunStats } from "../types";

/**
 * 仪表盘聚合：数据源是 run.stats（服务端 GROUP BY 精确计数），不再做客户端采样聚合。
 * 这里的纯函数只负责把 by_status 映射成展示用的派生值。
 */

/** 进行中（非终态）计数：running / initializing / awaiting_resume */
export function activeCount(byStatus: Record<string, number>): number {
  return (
    (byStatus["running"] ?? 0) +
    (byStatus["initializing"] ?? 0) +
    (byStatus["awaiting_resume"] ?? 0)
  );
}

/** 终态 run 中 succeeded 的占比（0-1）；没有终态 run 时为 null（显示 —，不显示 0%） */
export function successRate(byStatus: Record<string, number>): number | null {
  const terminal =
    (byStatus["succeeded"] ?? 0) + (byStatus["failed"] ?? 0) + (byStatus["cancelled"] ?? 0);
  return terminal > 0 ? (byStatus["succeeded"] ?? 0) / terminal : null;
}

export interface WorkflowRate {
  workflowId: string;
  total: number;
  succeeded: number;
  /** 终态成功率（0-1）；没有终态 run 时为 null */
  successRate: number | null;
}

/** 按工作流成功率表：run.stats 的 by_workflow 分组，按 run 总数倒序 */
export function perWorkflowRates(byWorkflow: WorkflowRunStats[]): WorkflowRate[] {
  return byWorkflow
    .map((w) => ({
      workflowId: w.workflow_id,
      total: w.total,
      succeeded: w.by_status["succeeded"] ?? 0,
      successRate: successRate(w.by_status),
    }))
    .sort((a, b) => b.total - a.total);
}
