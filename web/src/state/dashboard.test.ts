import { describe, expect, it } from "vitest";
import { activeCount, perWorkflowRates, successRate } from "./dashboard";
import type { WorkflowRunStats } from "../types";

function wf(workflowId: string, byStatus: Record<string, number>): WorkflowRunStats {
  return {
    workflow_id: workflowId,
    total: Object.values(byStatus).reduce((a, b) => a + b, 0),
    by_status: byStatus,
  };
}

describe("successRate / activeCount", () => {
  it("成功率只算终态；进行中三态计入 active", () => {
    const byStatus = { succeeded: 2, failed: 1, cancelled: 1, running: 1, awaiting_resume: 1 };
    expect(successRate(byStatus)).toBeCloseTo(0.5);
    expect(activeCount(byStatus)).toBe(2);
  });

  it("没有终态 run 时成功率为 null（显示 —，不显示 0%）", () => {
    expect(successRate({ running: 3 })).toBeNull();
    expect(successRate({})).toBeNull();
  });
});

describe("perWorkflowRates", () => {
  it("按 run 总数倒序，成功率来自各组的 by_status", () => {
    const rates = perWorkflowRates([
      wf("w1", { succeeded: 1, failed: 1 }),
      wf("w2", { succeeded: 3 }),
      wf("w3", { running: 1 }),
    ]);
    expect(rates.map((r) => r.workflowId)).toEqual(["w2", "w1", "w3"]);
    expect(rates[0]).toMatchObject({ total: 3, succeeded: 3, successRate: 1 });
    expect(rates[1]).toMatchObject({ total: 2, succeeded: 1, successRate: 0.5 });
    expect(rates[2].successRate).toBeNull();
  });

  it("空分组返回空数组", () => {
    expect(perWorkflowRates([])).toEqual([]);
  });
});
