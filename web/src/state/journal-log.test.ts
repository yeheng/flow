import { describe, expect, it } from "vitest";
import { summarizeJournalRecord } from "./journal-log";

describe("summarizeJournalRecord", () => {
  it("原生 Observation 使用 line 内的级别与正文", () => {
    const row = summarizeJournalRecord(
      {
        seq: "9",
        run_id: "run",
        dispatch_id: "dispatch",
        ts: "2026-10-09T02:00:00Z",
        line: { node_id: "script", attempt: 1, level: "error", stream: "stderr", message: "boom" },
      },
      0,
    );
    expect(row.label).toBe("error");
    expect(row.level).toBe("log-error");
    expect(row.text).toBe("boom");
    expect(row.meta).toContain("seq 9 · script");
  });
  it("识别 JournalEvent 结构：kind 作标签、run_seq 作 meta、payload 作摘要", () => {
    const row = summarizeJournalRecord(
      {
        lsn: "42",
        event_index: 0,
        event: { run_seq: "7", kind: "node_completed", payload: { node: "n", output: { ok: 1 } } },
      },
      0,
    );
    expect(row.key).toBe("42:0");
    expect(row.label).toBe("node_completed");
    expect(row.meta).toBe("seq 7");
    expect(row.text).toContain('"ok":1');
    expect(row.level).toBe("");
  });

  it("失败/重试类事件映射到行着色", () => {
    const failed = summarizeJournalRecord(
      { event: { run_seq: "1", kind: "node_failed", payload: null } },
      0,
    );
    const retry = summarizeJournalRecord({ level: "warn", message: "slow" }, 1);
    expect(failed.level).toBe("log-error");
    expect(retry.level).toBe("log-warn");
  });

  it("观测记录：message 直取、ts 转本地时分秒", () => {
    const row = summarizeJournalRecord(
      { level: "info", message: "hello", ts: "2026-10-09T02:00:00Z" },
      0,
    );
    expect(row.label).toBe("info");
    expect(row.text).toBe("hello");
    expect(row.meta).toContain(":00");
  });

  it("无结构记录回退：标签 record、摘要为截断 JSON、key 用序号", () => {
    const long = "x".repeat(200);
    const row = summarizeJournalRecord({ payload: long }, 3);
    expect(row.label).toBe("record");
    expect(row.key).toBe("row-3");
    expect(row.text.length).toBeLessThanOrEqual(161);
    expect(row.text.endsWith("…")).toBe(true);
  });

  it("haystack 是全量小写 JSON（搜索匹配原文，不受摘要截断影响）", () => {
    const row = summarizeJournalRecord({ event: { kind: "k", payload: { Token: "ABC" } } }, 0);
    expect(row.haystack).toContain('"token":"abc"');
  });
});
