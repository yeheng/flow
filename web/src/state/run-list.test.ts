import { describe, expect, it } from "vitest";
import { mergeRunPage, nextCursor, sourceLabel } from "./run-list";
import type { RunRecord } from "../types";

function run(id: string): RunRecord {
  return {
    id,
    workflow_id: "w1",
    workflow_version: 1,
    status: "succeeded",
    input: null,
    output: null,
    error: null,
    source: "manual",
    source_detail: null,
    started_at: "2026-01-01T00:00:00Z",
    ended_at: null,
  };
}

describe("游标分页合并", () => {
  it("追加并按 id 去重（轮询刷新首页与已翻页重叠时）", () => {
    const existing = [run("a"), run("b"), run("c")];
    // 首页刷新后来了新 run「n」，与旧页重叠 a
    const merged = mergeRunPage([run("n"), run("a")], existing);
    expect(merged.map((r) => r.id)).toEqual(["n", "a", "b", "c"]);
  });

  it("loadMore 方向：旧页追加到尾部", () => {
    const merged = mergeRunPage([run("a"), run("b")], [run("c"), run("a")]);
    expect(merged.map((r) => r.id)).toEqual(["a", "b", "c"]);
  });

  it("nextCursor 取最末一条；空列表返回 null", () => {
    expect(nextCursor([run("a"), run("b")])).toBe("b");
    expect(nextCursor([])).toBeNull();
  });
});

describe("run.source 中文映射", () => {
  it("词汇表内映射为中文；未知值原样显示", () => {
    expect(sourceLabel("manual")).toBe("手动");
    expect(sourceLabel("schedule")).toBe("定时调度");
    expect(sourceLabel("webhook")).toBe("Webhook");
    expect(sourceLabel("sub_workflow")).toBe("子流程");
    expect(sourceLabel("something_new")).toBe("something_new");
  });
});
