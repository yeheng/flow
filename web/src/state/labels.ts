/**
 * 运行态领域词汇表：全站唯一出处。
 * run.status / timeline phase / 节点 state / run.source 的中文映射集中在这里，
 * 新增状态或来源时只改这一处。
 */

/** run 记录状态（run.list / run.get 的 status） */
export function runStatusLabel(status: string): string {
  switch (status) {
    case "running":
      return "运行中";
    case "initializing":
      return "初始化中";
    case "awaiting_resume":
      return "挂起待恢复";
    case "succeeded":
      return "成功";
    case "failed":
      return "失败";
    case "cancelled":
      return "已取消";
    default:
      return status;
  }
}

/** run 投影 phase（timeline 的 fold 状态）；特殊 status 由 runStatusLabel 覆盖 */
export function runPhaseLabel(phase: string): string {
  switch (phase) {
    case "running":
      return "运行中";
    case "succeeded":
      return "成功";
    case "failed":
      return "失败";
    case "cancelled":
      return "已取消";
    default:
      return phase;
  }
}

/** 时间线节点状态（NodeRunState） */
export function nodeStateLabel(state: string): string {
  switch (state) {
    case "pending":
      return "等待";
    case "running":
      return "运行中";
    case "retrying":
      return "重试中";
    case "completed":
      return "完成";
    case "failed":
      return "失败";
    case "skipped":
      return "跳过";
    default:
      return state;
  }
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
