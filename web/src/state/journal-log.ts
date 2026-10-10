/**
 * JournalLogList 的纯函数部分：把异构的 journal 记录（JournalEvent /
 * 观测记录）归一成展示行。与组件分离以便 vitest 直测（对齐
 * monitor-logic.ts 的拆分方式）。
 */

export interface JournalRow {
  key: string;
  /** 行首标签：事件 kind / 记录 level / type */
  label: string;
  /** 行着色类：对齐 LogConsole 的 log-warn/log-error/log-debug */
  level: "" | "log-warn" | "log-error" | "log-debug";
  /** 次要信息（seq / lsn / 时间） */
  meta: string;
  /** 单行摘要 */
  text: string;
  /** 展开时 JsonNode 渲染的原始值 */
  raw: unknown;
  /** 搜索匹配用的全量小写 JSON */
  haystack: string;
}

const TEXT_LIMIT = 160;

function asRecord(v: unknown): Record<string, unknown> | null {
  return v !== null && typeof v === "object" && !Array.isArray(v)
    ? (v as Record<string, unknown>)
    : null;
}

function truncate(s: string): string {
  return s.length > TEXT_LIMIT ? `${s.slice(0, TEXT_LIMIT)}…` : s;
}

function levelOf(label: string): JournalRow["level"] {
  const l = label.toLowerCase();
  if (l.includes("fail") || l.includes("error")) return "log-error";
  if (l.includes("warn") || l.includes("retry")) return "log-warn";
  if (l.includes("debug")) return "log-debug";
  return "";
}

function fmtTs(v: unknown): string {
  if (typeof v !== "string") return "";
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? "" : d.toLocaleTimeString();
}

export function summarizeJournalRecord(item: unknown, index: number): JournalRow {
  const rec = asRecord(item);
  // JournalEvent: { lsn, event_index, event: { run_seq, kind, payload } }
  const event = asRecord(rec?.event);
  // 原生 Observation 将日志字段放在 line 内。
  const line = asRecord(rec?.line);
  const label = String(
    event?.kind ?? line?.level ?? rec?.kind ?? rec?.level ?? rec?.type ?? "record",
  );
  const key = rec?.lsn ? `${rec.lsn}:${String(rec.event_index ?? index)}` : `row-${index}`;

  const metaParts: string[] = [];
  if (event?.run_seq !== undefined) metaParts.push(`seq ${String(event.run_seq)}`);
  else if (rec?.seq !== undefined) metaParts.push(`seq ${String(rec.seq)}`);
  else if (rec?.lsn !== undefined) metaParts.push(`lsn ${String(rec.lsn)}`);
  if (line?.node_id !== undefined) metaParts.push(String(line.node_id));
  const ts = fmtTs(rec?.ts ?? rec?.time ?? rec?.timestamp);
  if (ts) metaParts.push(ts);

  const msg = line?.message ?? rec?.message ?? rec?.msg ?? rec?.text;
  const text =
    typeof msg === "string" ? truncate(msg) : truncate(JSON.stringify(event?.payload ?? item));

  return {
    key,
    label,
    level: levelOf(label),
    meta: metaParts.join(" · "),
    text,
    raw: item,
    haystack: JSON.stringify(item).toLowerCase(),
  };
}
