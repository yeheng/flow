import type { LogLine, ObservationRecord } from "../types";
export const MAX_LOG_LINES = 10000;
const KEEP_LOG_LINES = 8000;
export interface LogProjection { logs: LogLine[]; logsByNode: Record<string, LogLine[]>; lastLogSeq: string }
export function logWindow(
  total: number,
  cap: number,
  extra: number,
): { rendered: number; hidden: number } {
  if (total <= cap + extra) {
    return { rendered: total, hidden: 0 };
  }
  return { rendered: cap + extra, hidden: total - (cap + extra) };
}


/** Observations have their own cursor; control events never change it. */
export function appendObservations(p: LogProjection, records: ObservationRecord[]): void {
  for (const record of records) {
    if (BigInt(record.seq) <= BigInt(p.lastLogSeq)) continue;
    p.lastLogSeq = record.seq;
    const line = {...record.line, seq:record.seq, ts:record.ts};
    p.logs.push(line); (p.logsByNode[line.node_id] ??= []).push(line);
  }
  if (p.logs.length > MAX_LOG_LINES) {
    const dropped = p.logs.splice(0, p.logs.length - KEEP_LOG_LINES);
    const counts = new Map<string, number>();
    for (const line of dropped) counts.set(line.node_id,(counts.get(line.node_id) ?? 0)+1);
    for (const [id,count] of counts) {
      const bucket = p.logsByNode[id];
      if (!bucket) continue;
      if (count >= bucket.length) delete p.logsByNode[id]; else bucket.splice(0,count);
    }
  }
}
