import { describe, expect, it } from "vitest";
import {
  alignProjection,
  applyEvent,
  drainBuffer,
  logWindow,
  seqAction,
  MAX_LOG_LINES,
  type RunProjection,
} from "./monitor-logic";
import type { RunEvent, Timeline, TimelineNode } from "../types";

function proj(nodes: TimelineNode[] = []): RunProjection {
  return {
    phase: null,
    status: null,
    output: undefined,
    fatalError: null,
    lastSeq: 0,
    nodes,
    logs: [],
    lastLogSeq: 0,
  };
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
  it("更大的 seq（状态事件之间隔着日志行的 seq）→ apply", () => {
    // 缺口修复由 run_tail 内部补齐 + 重连后 re-attach 承担，客户端只做单调过滤
    expect(seqAction(3, 5)).toBe("apply");
    expect(seqAction(0, 7)).toBe("apply");
  });
});

describe("drainBuffer：对齐后补放缓冲事件", () => {
  it("全量按 seq 排序（含历史日志行；状态事件由 seqAction 去重）", () => {
    const buf = [ev(5), ev(2), ev(4), ev(3)];
    const drained = drainBuffer(buf);
    expect(drained.map((e) => e.seq)).toEqual([2, 3, 4, 5]);
  });
  it("日志行也参与排序：状态与日志交错的单一流", () => {
    const buf = [ev(5, "node_log"), ev(2), ev(3, "node_log")];
    expect(drainBuffer(buf).map((e) => e.type)).toEqual([
      "run_started",
      "node_log",
      "node_log",
    ]);
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
  it("node_started → running，记录 attempt、开始时间与输入面", () => {
    const p = proj([node("a")]);
    applyEvent(p, ev(1, "node_started", { node_id: "a", attempt: 2, input: { ms: 5 } }));
    expect(p.nodes[0].state).toBe("running");
    expect(p.nodes[0].attempts).toBe(2);
    expect(p.nodes[0].input).toEqual({ ms: 5 });
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
    for (const e of drainBuffer(buffer)) {
      if (seqAction(p.lastSeq, e.seq) !== "apply") continue;
      applyEvent(p, e);
      applied.push(e.seq);
    }
    expect(applied).toEqual([5, 6]);
    expect(p.phase).toBe("succeeded");
    expect(p.nodes[0].state).toBe("completed");
  });

  it("缺口（中间夹着日志行）按单调过滤平滑处理", () => {
    const p = proj();
    alignProjection(p, timeline(4, []));
    const buffer = [ev(7, "run_completed")];
    const drained = drainBuffer(buffer);
    expect(seqAction(p.lastSeq, drained[0].seq)).toBe("apply");
  });
});

describe("node_log：日志与状态同流但水位独立", () => {
  it("applyEvent(node_log) 追加日志行且不推进状态水位", () => {
    const p = proj([node("a")]);
    applyEvent(p, ev(2, "node_started", { node_id: "a", attempt: 1 }));
    applyEvent(p, ev(3, "node_log", { node_id: "a", attempt: 1, level: "info", stream: "stdout", message: "hello" }));
    expect(p.logs).toHaveLength(1);
    expect(p.logs[0]).toMatchObject({ seq: 3, node_id: "a", level: "info", stream: "stdout", message: "hello" });
    // lastSeq 停在 2（node_started），日志不拖动状态水位
    expect(p.lastSeq).toBe(2);
    expect(p.lastLogSeq).toBe(3);
  });

  it("lastLogSeq 水位去重：陈旧日志重复投递被忽略", () => {
    const p = proj();
    applyEvent(p, ev(5, "node_log", { node_id: "a", message: "m5" }));
    applyEvent(p, ev(3, "node_log", { node_id: "a", message: "m3" }));
    expect(p.logs.map((l) => l.message)).toEqual(["m5"]);
  });

  it("环形上限：超过 MAX_LOG_LINES 丢弃最旧行", () => {
    const p = proj();
    const base = p.lastLogSeq;
    for (let i = 1; i <= MAX_LOG_LINES + 2000; i++) {
      applyEvent(p, ev(base + i, "node_log", { node_id: "a", message: `m${i}` }));
    }
    expect(p.logs.length).toBeLessThanOrEqual(MAX_LOG_LINES);
    expect(p.logs[p.logs.length - 1].message).toBe(`m${MAX_LOG_LINES + 2000}`);
    expect(p.logs[0].message).not.toBe("m1");
  });

  it("历史日志（seq ≤ lastSeq）经补放路径进入日志列表", () => {
    // attach 场景：timeline 对齐到 2，回放段余下事件补放，日志行全部可见
    const p = proj([node("a")]);
    alignProjection(p, timeline(2, [node("a")]));
    const buffer = [
      ev(1, "run_started"),
      ev(2, "node_started", { node_id: "a", attempt: 1 }),
      ev(3, "node_log", { node_id: "a", attempt: 1, message: "log-a" }),
      ev(4, "node_completed", { node_id: "a" }),
      ev(6, "run_completed", { output: "x" }),
    ];
    for (const e of drainBuffer(buffer)) {
      if (e.type === "node_log") {
        applyEvent(p, e);
        continue;
      }
      if (seqAction(p.lastSeq, e.seq) === "apply") applyEvent(p, e);
    }
    expect(p.logs.map((l) => l.message)).toEqual(["log-a"]);
    expect(p.phase).toBe("succeeded");
    expect(p.nodes[0].state).toBe("completed");
  });
});

describe("logWindow：日志控制台渲染窗口", () => {
  it("总量小于窗口 → 全渲染", () => {
    expect(logWindow(100, 1500, 0)).toEqual({ rendered: 100, hidden: 0 });
  });
  it("超窗 → 渲染最近 cap 行，其余省略", () => {
    expect(logWindow(3000, 1500, 0)).toEqual({ rendered: 1500, hidden: 1500 });
  });
  it("加载更早逐步扩窗，覆盖全部后不再省略", () => {
    expect(logWindow(3000, 1500, 1500)).toEqual({ rendered: 3000, hidden: 0 });
    expect(logWindow(2900, 1500, 1000)).toEqual({ rendered: 2500, hidden: 400 });
  });
});
