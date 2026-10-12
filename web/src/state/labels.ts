/**
 * 运行态领域词汇表：全站唯一出处。
 * run.status / 节点 state / run.source 的中文映射集中在这里，
 * 新增状态或来源时只改这一处。
 */

/** runs.status 词汇表（flow-dto RunStatus，服务端写入口校验的那一份）。 */
export const TERMINAL_RUN_STATUSES: ReadonlySet<string> = new Set([
  "succeeded",
  "failed",
  "cancelled",
]);

/** 仍在执行、接受输入的状态。awaiting_resume 属于这一类（挂起但可续跑）。 */
export const ACTIVE_RUN_STATUSES: ReadonlySet<string> = new Set([
  "running",
  "awaiting_resume",
]);

/** run 记录状态（run.list / run.get 的 status） */
export function runStatusLabel(status: string): string {
  switch (status) {
    case "running":
      return "运行中";
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

/** 状态徽章着色沿用画布运行态体系：蓝 running / 绿 succeeded / 红 failed / 黄 awaiting / 灰 cancelled */
export function runBadgeClass(status: string): string {
  if (status === "awaiting_resume") return "run-awaiting";
  return `run-${status}`;
}

/** 节点「忙」= running 或 retrying（retrying 只在前端存在，服务器不发射） */
export function isBusyNodeState(state: string): boolean {
  return state === "running" || state === "retrying";
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

/** 节点类型中文名（与服务端 nodetypes descriptor 的 label 对齐；未知类型原样） */
export function nodeTypeLabel(type: string): string {
  switch (type) {
    case "start":
      return "开始";
    case "end":
      return "结束";
    case "script":
      return "脚本";
    case "condition":
      return "条件分支";
    case "delay":
      return "等待";
    case "http_call":
      return "HTTP 请求";
    case "email":
      return "邮件";
    case "harness":
      return "Harness 代理";
    case "human_task":
      return "人工节点";
    case "sub_workflow":
      return "子工作流";
    default:
      return type;
  }
}

/** 节点被跳过的原因（NodeSkipped 的 reason）→ 人话 */
export function skipReasonLabel(reason: string): string {
  switch (reason) {
    case "upstream_failed":
      return "上游节点失败，本步未执行";
    case "upstream_skipped":
      return "上游节点被跳过，本步未执行";
    case "branch_not_taken":
      return "条件分支未选中，本步未执行";
    default:
      return reason;
  }
}

/** run.source 的中文展示映射（词汇表：flow-dto RunSource）；未知值原样显示 */
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
