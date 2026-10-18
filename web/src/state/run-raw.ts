/**
 * 原始快照（journal Run.summary 信封）→ 产品面记录的纯映射。
 * 物化视图（run.get.view / run.list.view）超过 8MB 预算时会整体失败，
 * 降级路径走 run.get / run.list 原始信封：不解包大值，只给占位与下载入口。
 */
import type { RawRunSnapshot, RunRecord, StoredValue, StoredValueRef } from "../types";

export interface ValueMarker {
  kind: "empty" | "inline" | "ref";
  /** inline：真实业务值 */
  value?: unknown;
  /** ref：可下载的 output_id 与字节数（十进制字符串） */
  outputId?: string;
  totalBytes?: string;
}

/** StoredValue 信封 → 展示描述；非信封（防御）按 inline 业务值处理 */
export function describeStoredValue(value: unknown): ValueMarker {
  if (value === null || value === undefined) return { kind: "empty" };
  if (typeof value === "object" && !Array.isArray(value)) {
    const keys = Object.keys(value);
    const wire = value as { type?: unknown; value?: unknown };
    if (
      keys.length === 2 &&
      "value" in wire &&
      wire.value !== null &&
      typeof wire.value === "object"
    ) {
      if (wire.type === "inline") return { kind: "inline", value: wire.value };
      if (wire.type === "ref") {
        const ref = (wire.value as StoredValueRef["value"]) ?? {};
        return { kind: "ref", outputId: ref.output_id, totalBytes: ref.total_bytes };
      }
    }
  }
  return { kind: "inline", value };
}

export function isStoredValueRef(value: unknown): value is StoredValueRef {
  return describeStoredValue(value).kind === "ref";
}

/** 原始快照 → RunRecord 形状（input/output 保留信封原文，供大值占位/下载） */
export function rawRunToRecord(raw: RawRunSnapshot): RunRecord {
  return {
    id: raw.run_id,
    workflow_id: raw.workflow_id,
    workflow_version: raw.workflow_version,
    status: raw.status,
    input: raw.input as StoredValue,
    output: raw.output,
    error: raw.error ?? null,
    source: raw.source,
    source_detail: raw.source_detail ?? null,
    started_at: raw.created_at,
    ended_at: null,
  };
}

export interface RawPageFilter {
  workflowId?: string;
  status?: string;
  source?: string;
  /** 只要比它更旧的（run.list.view 的 before_run_id 同义） */
  beforeRunId?: string;
}

export interface RawPageResult {
  /** 降序（最新在前），最多 limit 条 */
  rows: RawRunSnapshot[];
  /** 向前还有更旧的匹配行 */
  hasMore: boolean;
}

function matches(raw: RawRunSnapshot, filter: RawPageFilter): boolean {
  if (filter.workflowId !== undefined && raw.workflow_id !== filter.workflowId) return false;
  if (filter.status !== undefined && raw.status !== filter.status) return false;
  if (filter.source !== undefined && raw.source !== filter.source) return false;
  return true;
}

/**
 * 从升序扫描的多页原始行中选出「最新的 limit 条匹配行」。
 * raw run.list 只有键升序 + after 游标，没有过滤/降序，降级列表只能整段
 * 扫描后取尾部；complete=false 表示被扫描上限截断（比可见页更旧的数据未知）。
 */
export function selectRawRuns(
  pages: RawRunSnapshot[][],
  filter: RawPageFilter,
  limit: number,
  complete: boolean,
): RawPageResult {
  const kept: RawRunSnapshot[] = [];
  let matched = 0;
  let cursorReached = false;
  for (const page of pages) {
    for (const raw of page) {
      if (filter.beforeRunId !== undefined && raw.run_id >= filter.beforeRunId) {
        cursorReached = true;
        break;
      }
      if (!matches(raw, filter)) continue;
      matched++;
      kept.push(raw);
      if (kept.length > limit) kept.shift();
    }
    if (cursorReached) break;
  }
  kept.reverse();
  return {
    rows: kept,
    hasMore: matched > kept.length || !(cursorReached || complete),
  };
}
