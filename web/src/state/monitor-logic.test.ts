import { describe, expect, it } from "vitest";
import {
  alignProjection,
  applyEvent,
  drainBuffer,
  seqAction,
  type RunProjection,
} from "./monitor-logic";
import type { RunEvent, Timeline, TimelineNode } from "../types";

function proj(nodes: TimelineNode[] = []): RunProjection {
  return { phase: null, status: null, output: undefined, fatalError: null, lastSeq: 0, nodes };
}

function node(id: string): TimelineNode {
  return {
    id,
    name: id,
    type: "script",
    state: "pending",
    attempts: 0,
    started_at: null,
    ended_at: null,
    duration_ms: null,
    output: null,
    error: null,
  };
}

function ev(
  seq: number,
  type: RunEvent["type"] = "run_started",
  extra: Partial<RunEvent> = {},
): RunEvent {
  return { seq, ts: "2026-01-01T00:00:00Z", run_id: "r1", type, ...extra };
}

function timeline(lastSeq: number, nodes: TimelineNode[]): Timeline {
  return {
    run_id: "r1",
    status: "running",
    phase: "running",
    workflow_id: "w1",
    workflow_version: 3,
    started_at: null,
    ended_at: null,
    output: undefined,
    fatal_error: null,
    last_seq: lastSeq,
    nodes,
  };
}

describe("seqAction：事件序号决策", () => {
  it("恰好下一条 → apply", () => {
    expect(seqAction(3, 4)).toBe("apply");
  });
  it("重复/陈旧 → skip", () => {
    expect(seqAction(3, 3)).toBe("skip");
    expect(seqAction(3, 1)).toBe("skip");
  });
  it("出现缺口 → resync", () => {
    expect(seqAction(3, 5)).toBe("resync");
    expect(seqAction(0, 7)).toBe("resync");
  });
});

describe("drainBuffer：对齐后补放缓冲事件", () => {
  it("丢弃 timeline 已覆盖的事件，按 seq 排序", () => {
    const buf = [ev(5), ev(2), ev(4), ev(3)];
    const drained = drainBuffer(buf, 2);
    expect(drained.map((e) => e.seq)).toEqual([3, 4, 5]);
  });
  it("全部已被覆盖 → 空", () => {
    expect(drainBuffer([ev(1), ev(2)], 5)).toEqual([]);
  });
});

describe("alignProjection：timeline 快照对齐", () => {
  it("拷贝节点与 run 级字段", () => {
    const p = proj();
    const nodes = [node("a"), node("b")];
    alignProjection(p, timeline(9, nodes));
    expect(p.nodes).toHaveLength(2);
    expect(p.lastSeq).toBe(9);
    expect(p.phase).toBe("running");
  });
});

describe("applyEvent：事件增量应用", () => {
  it("node_started → running，记录 attempt 与开始时间", () => {
    const p = proj([node("a")]);
    applyEvent(p, ev(1, "node_started", { node_id: "a", attempt: 2 }));
    expect(p.nodes[0].state).toBe("running");
    expect(p.nodes[0].attempts).toBe(2);
    expect(p.lastSeq).toBe(1);
  });

  it("node_completed → completed，写入输出与耗时", () => {
    const p = proj([node("a")]);
    applyEvent(p, ev(1, "node_completed", { node_id: "a", output: { ok: 1 }, duration_ms: 12 }));
    expect(p.nodes[0].state).toBe("completed");
    expect(p.nodes[0].output).toEqual({ ok: 1 });
    expect(p.nodes[0].duration_ms).toBe(12);
  });

  it("node_failed：retryable → retrying，否则 failed", () => {
    const p = proj([node("a"), node("b")]);
    applyEvent(p, ev(1, "node_failed", { node_id: "a", retryable: true, error: "boom" }));
    applyEvent(p, ev(2, "node_failed", { node_id: "b", error: "boom" }));
    expect(p.nodes[0].state).toBe("retrying");
    expect(p.nodes[1].state).toBe("failed");
  });

  it("run_completed / run_failed / run_cancelled 推进 phase 与终态字段", () => {
    const p = proj();
    applyEvent(p, ev(1, "run_completed", { output: 42 }));
    expect(p.phase).toBe("succeeded");
    expect(p.output).toBe(42);

    const p2 = proj();
    applyEvent(p2, ev(1, "run_failed", { error: "fatal" }));
    expect(p2.phase).toBe("failed");
    expect(p2.fatalError).toBe("fatal");

    const p3 = proj();
    applyEvent(p3, ev(1, "run_cancelled"));
    expect(p3.phase).toBe("cancelled");
  });

  it("signal_received 不改变投影", () => {
    const p = proj([node("a")]);
    applyEvent(p, ev(1, "signal_received", { node_id: "a" }));
    expect(p.nodes[0].state).toBe("pending");
    expect(p.lastSeq).toBe(1);
  });
});

describe("缓冲 → 对齐 → 补放：attach 时序", () => {
  it("订阅建立与 timeline 对齐之间到达的事件不丢、不重", () => {
    const p = proj();
    // 订阅先建立，事件 5、6 在对齐前到达 → 缓冲
    const buffer = [
      ev(6, "run_completed", { output: "done" }),
      ev(5, "node_completed", { node_id: "a" }),
    ];
    // timeline 对齐到 seq 4
    alignProjection(p, timeline(4, [node("a")]));
    // 补放：seq 5、6 依次应用
    const applied: number[] = [];
    for (const e of drainBuffer(buffer, p.lastSeq)) {
      if (seqAction(p.lastSeq, e.seq) !== "apply") break;
      applyEvent(p, e);
      applied.push(e.seq);
    }
    expect(applied).toEqual([5, 6]);
    expect(p.phase).toBe("succeeded");
    expect(p.nodes[0].state).toBe("completed");
  });

  it("缓冲事件与 timeline 之间有缺口 → 触发 resync 而非硬应用", () => {
    const p = proj();
    alignProjection(p, timeline(4, []));
    const buffer = [ev(7, "run_completed")];
    const drained = drainBuffer(buffer, p.lastSeq);
    // 期望 seq 5，来了 7：决策必须是 resync
    expect(seqAction(p.lastSeq, drained[0].seq)).toBe("resync");
  });
});
