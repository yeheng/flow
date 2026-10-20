/**
 * 全站时间/时长格式化的唯一出处（领域词汇在 labels.ts，时间在 time.ts）。
 * 非法/缺失时间戳回退原值或 "—"，避免页面出现 Invalid Date。
 */

/** 完整日期时间（列表/详情列）：null → "—"，非法回退原值 */
export function fmtTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

/** 仅时刻（日志行/时间线等窄列）：非法回退原值 */
export function fmtClock(ts: string): string {
  const d = new Date(ts);
  return Number.isNaN(d.getTime()) ? ts : d.toLocaleTimeString();
}

/** 友好耗时：null → 空串；<1s 毫秒；<60s 秒；再往上分秒 */
export function friendlyDuration(ms: number | null): string {
  if (ms === null) return "";
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  const minutes = Math.floor(ms / 60_000);
  const seconds = Math.round((ms % 60_000) / 1000);
  return `${minutes}m ${seconds}s`;
}

/** run 耗时（列表/详情）：起止任一缺失（运行中/数据面限制）显示 "—" */
export function runDuration(startedAt: string | null, endedAt: string | null): string {
  if (!startedAt || !endedAt) return "—";
  const ms = new Date(endedAt).getTime() - new Date(startedAt).getTime();
  return Number.isNaN(ms) ? "—" : friendlyDuration(ms);
}
