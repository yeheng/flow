import type { RunEvent, Timeline, TimelineNode } from "../types";

/**
 * run 投影：run.timeline 初始对齐 + run.event 增量应用的纯逻辑。
 * 从 state/monitor.ts 抽出以便单测；monitor 的 reactive 状态结构上满足本接口。
 */
export interface RunProjection {
  phase: string | null;
  status: string | null;
  output: unknown;
  fatalError: string | null;
  lastSeq: number;
  nodes: TimelineNode[];
}

/** 用 run.timeline 快照整体对齐投影 */
export function alignProjection(p: RunProjection, tl: Timeline): void {
  p.nodes = tl.nodes;
  p.phase = tl.phase;
  p.status = tl.status;
  p.output = tl.output;
  p.fatalError = tl.fatal_error;
  p.lastSeq = tl.last_seq;
}

export type SeqAction = "apply" | "skip" | "resync";

/**
 * 事件序号决策：重复/陈旧事件跳过；恰好下一条则应用；
 * 出现缺口（订阅 Lagged 丢事件）时需要整体 resync。
 */
export function seqAction(lastSeq: number, seq: number): SeqAction {
  if (seq <= lastSeq) return "skip";
  if (seq !== lastSeq + 1) return "resync";
  return "apply";
}

/** 订阅建立与 timeline 对齐之间缓冲的事件：丢弃已覆盖的、按 seq 排序后补放 */
export function drainBuffer(buffer: RunEvent[], lastSeq: number): RunEvent[] {
  return buffer.filter((e) => e.seq > lastSeq).sort((a, b) => a.seq - b.seq);
}

/** 应用单条 run.event 到投影；调用方保证 seq 连续（见 seqAction） */
export function applyEvent(p: RunProjection, env: RunEvent): void {
  p.lastSeq = env.seq;
  const rec = env.node_id ? p.nodes.find((n) => n.id === env.node_id) : undefined;
  switch (env.type) {
    case "run_started":
      p.phase = "running";
      p.status = "running";
      break;
    case "node_started":
      if (rec) {
        rec.state = "running";
        rec.attempts = Math.max(rec.attempts, env.attempt ?? 1);
        rec.started_at = env.ts;
        rec.ended_at = null;
        rec.duration_ms = null;
        rec.output = null;
        rec.error = null;
        // 重试 = 新 attempt = 新的确定性 child_run_id
        rec.child_run_id = env.child_run_id;
      }
      break;
    case "node_completed":
      if (rec) {
        rec.state = "completed";
        rec.attempts = Math.max(rec.attempts, env.attempt ?? 1);
        rec.ended_at = env.ts;
        rec.duration_ms = env.duration_ms ?? null;
        rec.output = env.output ?? null;
        rec.error = null;
      }
      break;
    case "node_failed":
      if (rec) {
        rec.state = env.retryable ? "retrying" : "failed";
        rec.attempts = Math.max(rec.attempts, env.attempt ?? 1);
        rec.ended_at = env.ts;
        rec.error = env.error ?? null;
      }
      break;
    case "node_skipped":
      if (rec) {
        rec.state = "skipped";
        rec.reason = env.reason;
        rec.ended_at = env.ts;
      }
      break;
    case "signal_received":
      break; // 信号落盘本身不改变时间线，节点终态由后续事件推进
    case "run_completed":
      p.phase = "succeeded";
      p.status = "succeeded";
      p.output = env.output;
      break;
    case "run_failed":
      p.phase = "failed";
      p.status = "failed";
      p.fatalError = env.error ?? null;
      break;
    case "run_cancelled":
      p.phase = "cancelled";
      p.status = "cancelled";
      break;
  }
}
