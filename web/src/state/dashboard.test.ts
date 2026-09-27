import { describe, expect, it } from "vitest";
import { perWorkflowRates, summarizeRuns } from "./dashboard";
import type { RunRecord } from "../types";

function run(id: string, status: string, workflowId = "w1"): RunRecord {
  return {
    id,
    workflow_id: workflowId,
    workflow_version: 1,
    status,
    input: null,
    output: null,
    error: null,
    started_at: "2026-01-01T00:00:00Z",
    ended_at: null,
  };
}

describe("summarizeRuns", () => {
  it("按状态计数；成功率只算终态", () => {
    const s = summarizeRuns([
      run("a", "succeeded"),
      run("b", "succeeded"),
      run("c", "failed"),
      run("d", "running"),
      run("e", "awaiting_resume"),
      run("f", "cancelled"),
    ]);
    expect(s.total).toBe(6);
    expect(s.succeeded).toBe(2);
    expect(s.failed).toBe(1);
    expect(s.cancelled).toBe(1);
    expect(s.active).toBe(2);
    expect(s.successRate).toBeCloseTo(0.5);
  });

  it("没有终态 run 时成功率为 null（显示 —，不显示 0%）", () => {
    expect(summarizeRuns([run("a", "running")]).successRate).toBeNull();
    expect(summarizeRuns([]).successRate).toBeNull();
  });
});

describe("perWorkflowRates", () => {
  it("按工作流分组，按 run 总数倒序", () => {
    const rates = perWorkflowRates([
      run("a", "succeeded", "w1"),
      run("b", "failed", "w1"),
      run("c", "succeeded", "w2"),
      run("d", "succeeded", "w2"),
      run("e", "succeeded", "w2"),
      run("f", "running", "w3"),
    ]);
    expect(rates.map((r) => r.workflowId)).toEqual(["w2", "w1", "w3"]);
    expect(rates[0]).toMatchObject({ total: 3, succeeded: 3, successRate: 1 });
    expect(rates[1]).toMatchObject({ total: 2, succeeded: 1, successRate: 0.5 });
    expect(rates[2].successRate).toBeNull();
  });

  it("空列表返回空数组", () => {
    expect(perWorkflowRates([])).toEqual([]);
  });
});
