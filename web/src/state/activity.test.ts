import { describe, expect, it } from "vitest";
import {
  activityEntries,
  friendlyDuration,
  isFrameNode,
  runElapsedMs,
  runProgress,
} from "./activity";
import type { LogLine, TimelineNode } from "../types";

function node(overrides: Partial<TimelineNode>): TimelineNode {
  return {
    id: "n",
    name: "n",
    type: "script",
    state: "pending",
    attempts: 1,
    started_at: null,
    ended_at: null,
    duration_ms: null,
    output: null,
    error: null,
    ...overrides,
  };
}

describe("activityEntries", () => {
  it("按开始时间排序，未开始的按定义顺序垫底", () => {
    const entries = activityEntries(
      [
        node({ id: "a", started_at: "2026-10-12T03:00:02Z", state: "completed" }),
        node({ id: "pending-2", type: "end" }),
        node({ id: "b", started_at: "2026-10-12T03:00:01Z", state: "completed" }),
        node({ id: "pending-1", type: "http_call" }),
        node({ id: "s", type: "start", started_at: "2026-10-12T03:00:00Z" }),
      ],
      {},
    );
    expect(entries.map((e) => e.node.id)).toEqual(["b", "a", "pending-1"]);
  });

  it("挂上该节点的观测日志，没有日志的节点给空数组", () => {
    const line = { seq: "1", ts: "t", node_id: "a", attempt: 1, level: "info", stream: "engine", message: "开始执行：脚本" } as LogLine;
    const entries = activityEntries(
      [node({ id: "a", started_at: "2026-10-12T03:00:00Z" }), node({ id: "b", started_at: "2026-10-12T03:00:01Z" })],
      { a: [line] },
    );
    expect(entries[0].logs).toEqual([line]);
    expect(entries[1].logs).toEqual([]);
  });

  it("同刻开始的并行节点保持定义顺序（sort 稳定）", () => {
    const at = "2026-10-12T03:00:00Z";
    const entries = activityEntries(
      [node({ id: "x", started_at: at }), node({ id: "y", started_at: at })],
      {},
    );
    expect(entries.map((e) => e.node.id)).toEqual(["x", "y"]);
  });
});

describe("runProgress", () => {
  it("只统计非框架节点，终态（完成/失败/跳过）计入 done", () => {
    const progress = runProgress([
      node({ id: "s", type: "start", state: "completed" }),
      node({ id: "a", state: "completed" }),
      node({ id: "b", state: "failed" }),
      node({ id: "c", state: "skipped" }),
      node({ id: "d", state: "running" }),
      node({ id: "e", state: "pending" }),
      node({ id: "end", type: "end", state: "pending" }),
    ]);
    expect(progress).toEqual({ done: 3, total: 5 });
  });
});

describe("friendlyDuration", () => {
  it("null → 空串；毫秒/秒/分钟三档", () => {
    expect(friendlyDuration(null)).toBe("");
    expect(friendlyDuration(230)).toBe("230ms");
    expect(friendlyDuration(1234)).toBe("1.2s");
    expect(friendlyDuration(63_000)).toBe("1m 3s");
  });
});

describe("runElapsedMs", () => {
  const nodes = [
    node({ id: "a", started_at: "2026-10-12T03:00:00Z", ended_at: "2026-10-12T03:00:02Z" }),
    node({ id: "b", started_at: "2026-10-12T03:00:01Z" }),
  ];

  it("取最早节点开始时间；运行中算到当前时刻", () => {
    expect(runElapsedMs(nodes, null, Date.parse("2026-10-12T03:00:04Z"))).toBe(4000);
  });

  it("结束时间存在时以结束时间封口", () => {
    expect(runElapsedMs(nodes, "2026-10-12T03:00:03Z", Date.parse("2026-10-12T09:00:00Z"))).toBe(
      3000,
    );
  });

  it("没有任何节点开始时返回 null（准备中）", () => {
    expect(runElapsedMs([node({ id: "a" })], null, 1234)).toBeNull();
  });
});

describe("isFrameNode", () => {
  it("只有 start/end 是框架节点", () => {
    expect(isFrameNode(node({ type: "start" }))).toBe(true);
    expect(isFrameNode(node({ type: "end" }))).toBe(true);
    expect(isFrameNode(node({ type: "script" }))).toBe(false);
  });
});
