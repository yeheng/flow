import type { LogLine, RunEvent, Timeline, TimelineNode } from "../types";

/**
 * run 投影：run.timeline 初始对齐 + run.event 增量应用的纯逻辑。
 * 从 state/monitor.ts 抽出以便单测；monitor 的 reactive 状态结构上满足本接口。
 *
 * 日志（node_log）与状态事件同流同 seq 空间（可观察性设计 §2）：
 * - 日志有自己的去重水位 lastLogSeq（run_tail 单订阅内按 seq 升序投递），
 *   历史/实时日志统一走一条路径，无需第二来源；
 * - 状态事件的水位 lastSeq 用单调过滤（seq > lastSeq 才应用）——状态事件
 *   之间隔着日志行的 seq，严格相邻不再成立；缺口修复由 run_tail 内部补齐 +
 *   断线重连后的整体 re-attach 承担，客户端不再做缺口检测。
 */
export interface RunProjection {
  status: string | null;
  output: unknown;
  fatalError: string | null;
  lastSeq: number;
  nodes: TimelineNode[];
  /** 节点日志（按 seq 升序追加；环形上限见 MAX_LOG_LINES） */
  logs: LogLine[];
  /**
   * `logs` 按 node_id 的索引：`Map` 的对象版（避免 JSON 序列化开销）。
   * NodeInspector 与 LogConsole 的节点过滤此前每次都全量扫 `logs`
   * （8000 行 × 每次追加一行 = O(n²)），现在直读桶。
   * 与 `logs` 由 applyLog 一处同步维护，别处不要单独改。
   */
  logsByNode: Record<string, LogLine[]>;
  /** 日志去重水位：已收到的最大 node_log seq */
  lastLogSeq: number;
}

/** 日志环形上限：超过后丢弃最旧的，与后端单 run 预算同量级 */
export const MAX_LOG_LINES = 10000;
/** 触发上限后保留的行数：避免逐行 trim 的抖动 */
const KEEP_LOG_LINES = 8000;

/** 用 run.timeline 快照整体对齐投影（日志不在 timeline 里，由订阅回放补齐） */
export function alignProjection(p: RunProjection, tl: Timeline): void {
  p.nodes = tl.nodes;
  p.status = tl.status;
  p.output = tl.output;
  p.fatalError = tl.fatal_error;
  p.lastSeq = tl.last_seq;
  p.logs = [];
  // 索引必须与 logs 同步清空：留着一个装着上一条 run 的桶，
  // NodeInspector 直读它就会显示别的 run 的日志
  p.logsByNode = {};
  p.lastLogSeq = 0;
}

export type SeqAction = "apply" | "skip";

/**
 * 状态事件序号决策：陈旧（回放已覆盖）跳过；否则应用。
 *
 * 契约的保证人（改 run_tail 前先读这段）：后端 run_tail（flow-backend/src/
 * run_tail.rs）保证单订阅内 seq **严格连续且不重复**——回放段 1..N 与实时段
 * 经 BTreeMap 去重合流，广播 Lagged 造成的缺口由它自己重读磁盘补齐后才继续
 * 投递。因此客户端只需单调去重过滤；本订阅之外的丢失（断线）由重连后的
 * re-attach 整体重建投影兑底。状态事件之间的 seq 间隔是日志行——严格相邻
 * 在这里本来就不成立。
 */
export function seqAction(lastSeq: number, seq: number): SeqAction {
  if (seq <= lastSeq) return "skip";
  return "apply";
}

/** 订阅建立与 timeline 对齐之间缓冲的事件：按 seq 排序后全部补放
 *  （状态事件的去重交给 seqAction，日志交给 lastLogSeq 水位） */
export function drainBuffer(buffer: RunEvent[]): RunEvent[] {
  return [...buffer].sort((a, b) => a.seq - b.seq);
}

/**
 * 日志控制台渲染窗口（纯函数，便于单测）：默认渲染最近 cap 行；
 * 「加载更早」把窗口向前扩 extra 行，直到覆盖全部。返回
 * [渲染行数, 省略行数]——状态存全量（预算封顶），DOM 只开窗口。
 *
 * extra 的天花板由调用方（LogConsole 的 MAX_EARLIER）保证，这里不做二次限制：
 * 上限是 UI 的资源预算，不是窗口算术的职责。
 */
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

/** node_log 事件 → 前端日志行，带水位去重；环形上限内追加 */
export function applyLog(p: RunProjection, env: RunEvent): void {
  const seq = env.seq;
  if (seq <= p.lastLogSeq) return;
  p.lastLogSeq = seq;
  const line: LogLine = {
    seq,
    ts: env.ts,
    node_id: env.node_id ?? "",
    attempt: env.attempt ?? 0,
    level: env.level ?? "info",
    stream: env.stream ?? "engine",
    message: env.message ?? "",
  };
  p.logs.push(line);
  (p.logsByNode[line.node_id] ??= []).push(line);
  if (p.logs.length > MAX_LOG_LINES) {
    // 环形裁剪要同时裁索引，否则桶里会攒下已被丢弃的行。
    //
    // `logs` 与每个桶都是 seq 升序追加的，所以 `splice(0, n)` 删掉的 n 条
    // 正是每个受影响桶的一段**前缀**——按 node_id 计数后一次 shift 掉即可。
    // 别退回逐行 `indexOf`：那是 O(丢弃条数 × 桶长)，单节点刷屏时一次裁剪
    // 就是上百万次身份比较，而这个函数每追加一行都会走到。
    const dropped = p.logs.splice(0, p.logs.length - KEEP_LOG_LINES);
    const counts = new Map<string, number>();
    for (const old of dropped) counts.set(old.node_id, (counts.get(old.node_id) ?? 0) + 1);
    for (const [id, n] of counts) {
      const bucket = p.logsByNode[id];
      if (!bucket) continue;
      if (n >= bucket.length) delete p.logsByNode[id];
      else bucket.splice(0, n);
    }
  }
}

/** 应用单条 run.event 到投影；状态事件由调用方保证 seq 单调（见 seqAction） */
export function applyEvent(p: RunProjection, env: RunEvent): void {
  // 日志不是状态：不推进 lastSeq（否则会把状态水位拖回去）
  if (env.type === "node_log") {
    applyLog(p, env);
    return;
  }
  p.lastSeq = env.seq;
  const rec = env.node_id ? p.nodes.find((n) => n.id === env.node_id) : undefined;
  switch (env.type) {
    case "run_started":
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
        // 输入面快照随 attempt 刷新（node_started 写入时已脱敏）
        rec.input = env.input;
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
      p.status = "succeeded";
      p.output = env.output;
      break;
    case "run_failed":
      p.status = "failed";
      p.fatalError = env.error ?? null;
      break;
    case "run_cancelled":
      p.status = "cancelled";
      break;
  }
}
