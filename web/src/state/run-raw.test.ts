import { describe, expect, it } from "vitest";
import { describeStoredValue, rawRunToRecord, selectRawRuns } from "./run-raw";
import type { RawRunSnapshot, StoredValueRef } from "../types";

function ref(outputId: string, totalBytes: string): StoredValueRef {
  return {
    type: "ref",
    value: {
      journal_id: "j",
      output_id: outputId,
      codec: "json",
      version: 1,
      chunk_count: "1",
      total_bytes: totalBytes,
      digest: "d",
    },
  };
}

function raw(over: Partial<RawRunSnapshot> & { run_id: string }): RawRunSnapshot {
  return {
    workflow_id: "wf1",
    workflow_version: 1,
    status: "succeeded",
    input: { type: "inline", value: null },
    output: null,
    error: null,
    source: "manual",
    source_detail: null,
    created_at: "2026-10-18T00:00:00Z",
    ...over,
  };
}

describe("describeStoredValue", () => {
  it("inline / ref / 空 三种信封", () => {
    expect(describeStoredValue({ type: "inline", value: { ok: 1 } })).toEqual({
      kind: "inline",
      value: { ok: 1 },
    });
    expect(
      describeStoredValue({
        type: "ref",
        value: { output_id: "out-1", total_bytes: "2048" },
      }),
    ).toEqual({ kind: "ref", outputId: "out-1", totalBytes: "2048" });
    expect(describeStoredValue(null)).toEqual({ kind: "empty" });
    expect(describeStoredValue(undefined)).toEqual({ kind: "empty" });
  });

  it("非信封业务值按 inline 处理（含形似对象）", () => {
    expect(describeStoredValue({ a: 1 })).toEqual({ kind: "inline", value: { a: 1 } });
    expect(describeStoredValue({ type: "inline", value: 1, extra: true })).toEqual({
      kind: "inline",
      value: { type: "inline", value: 1, extra: true },
    });
    expect(describeStoredValue([1, 2])).toEqual({ kind: "inline", value: [1, 2] });
  });
});

describe("rawRunToRecord", () => {
  it("字段映射 + 信封原文保留", () => {
    const record = rawRunToRecord(
      raw({
        run_id: "r1",
        source: "schedule",
        source_detail: "s1",
        output: ref("o", "9"),
      }),
    );
    expect(record).toMatchObject({
      id: "r1",
      status: "succeeded",
      source: "schedule",
      source_detail: "s1",
      started_at: "2026-10-18T00:00:00Z",
      ended_at: null,
    });
    expect(record.output).toEqual(ref("o", "9"));
  });
});

describe("selectRawRuns", () => {
  const rows = (ids: string[], patch: Partial<RawRunSnapshot> = {}) =>
    ids.map((id) => raw({ run_id: id, ...patch }));

  it("取最新 limit 条并降序，hasMore 指向更旧的匹配行", () => {
    const result = selectRawRuns([rows(["r1", "r2", "r3", "r4", "r5"])], {}, 3, true);
    expect(result.rows.map((r) => r.run_id)).toEqual(["r5", "r4", "r3"]);
    expect(result.hasMore).toBe(true);
  });

  it("过滤在扫描期生效，游标边界不包含自身", () => {
    const pages = [
      rows(["r1", "r2"], { status: "failed" }),
      rows(["r3", "r4"]),
      rows(["r5", "r6"], { status: "failed" }),
    ];
    const result = selectRawRuns(pages, { status: "failed", beforeRunId: "r6" }, 10, true);
    expect(result.rows.map((r) => r.run_id)).toEqual(["r5", "r2", "r1"]);
    expect(result.hasMore).toBe(false);
  });

  it("扫描被截断时 hasMore 为真", () => {
    const result = selectRawRuns([rows(["r1", "r2"])], {}, 10, false);
    expect(result.hasMore).toBe(true);
    expect(result.rows.map((r) => r.run_id)).toEqual(["r2", "r1"]);
  });
});
