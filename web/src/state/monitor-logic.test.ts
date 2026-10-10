import { describe, expect, it } from "vitest";
import {
  appendObservations,
  logWindow,
  MAX_LOG_LINES,
  type LogProjection,
} from "./monitor-logic";
import type { ObservationRecord } from "../types";

function proj(): LogProjection {
  return { logs: [], logsByNode: {}, lastLogSeq: "0" };
}

let seqCounter = 0;
function rec(nodeId: string, message: string, seq?: string): ObservationRecord {
  const s = seq ?? String(++seqCounter);
  return {
    seq: s,
    run_id: "r1",
    dispatch_id: "d1",
    ts: "2026-01-01T00:00:00Z",
    line: {
      node_id: nodeId,
      attempt: 1,
      level: "info",
      stream: "stdout",
      message,
    },
  };
}

describe("appendObservations：观测日志增量追加", () => {
  it("按 seq 追加行并推进游标；行带 seq/ts 覆盖", () => {
    const p = proj();
    appendObservations(p, [rec("a", "m1"), rec("a", "m2")]);
    expect(p.logs.map((l) => l.message)).toEqual(["m1", "m2"]);
    expect(p.logs[0]).toMatchObject({
      seq: "1",
      ts: "2026-01-01T00:00:00Z",
      node_id: "a",
      level: "info",
      stream: "stdout",
    });
    expect(p.lastLogSeq).toBe("2");
  });

  it("游标去重：seq ≤ lastLogSeq 的重复投递被忽略（BigInt 语义，非字典序）", () => {
    const p = proj();
    appendObservations(p, [rec("a", "m9", "9")]);
    appendObservations(p, [rec("a", "stale", "8"), rec("a", "dup", "9"), rec("a", "m10", "10")]);
    expect(p.logs.map((l) => l.message)).toEqual(["m9", "m10"]);
    expect(p.lastLogSeq).toBe("10");
  });

  it("控制事件不经过这条路径——观测游标独立推进", () => {
    // lastLogSeq 只由 ObservationRecord 推进；这里只验证初始投影语义
    const p = proj();
    expect(p.lastLogSeq).toBe("0");
    expect(p.logs).toHaveLength(0);
    expect(p.logsByNode).toEqual({});
  });

  it("按 node_id 的索引与 logs 始终一致（环形裁剪后仍引用级一致）", () => {
    const p = proj();
    // 三个节点的量差别很大：让各桶被裁剪的条数不同
    const total = MAX_LOG_LINES + 500;
    const records: ObservationRecord[] = [];
    for (let i = 1; i <= total; i++) {
      const nodeId = i % 10 < 7 ? "hot" : i % 10 < 9 ? "warm" : "cold";
      records.push(rec(nodeId, `m${i}`, String(i)));
    }
    appendObservations(p, records);

    const flat = Object.values(p.logsByNode).flat();
    expect(flat.length).toBe(p.logs.length);
    expect(new Set(flat).size).toBe(flat.length); // 同一行不得进两个桶
    for (const id of ["hot", "warm", "cold"]) {
      const bucket = p.logsByNode[id] ?? [];
      const expected = p.logs.filter((l) => l.node_id === id);
      expect(bucket).toEqual(expected);
      // 引用一致：桶里就是 logs 里那几行，不是拷贝
      for (const line of bucket) expect(p.logs).toContain(line);
      // 桶内 seq 严格升序（裁剪掉的必须是一段前缀，中间不能掉）
      for (let i = 1; i < bucket.length; i++) {
        expect(Number(bucket[i].seq)).toBeGreaterThan(Number(bucket[i - 1].seq));
      }
    }
    expect(p.logs.length).toBeLessThanOrEqual(MAX_LOG_LINES);
    expect(p.logs[p.logs.length - 1].message).toBe(`m${total}`);
  });

  it("整桶裁空后索引键被删除，不留空数组残骸", () => {
    const p = proj();
    const records: ObservationRecord[] = [];
    // cold 只有一条，环形裁剪必然把它整桶裁掉
    for (let i = 1; i <= MAX_LOG_LINES + 10; i++) {
      records.push(rec(i === 1 ? "cold" : "hot", `m${i}`, String(i)));
    }
    appendObservations(p, records);
    expect(p.logsByNode["cold"]).toBeUndefined();
    expect(p.logsByNode["hot"]).toEqual(p.logs.filter((l) => l.node_id === "hot"));
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
    // 生产里 extra 恒为 EARLIER_STEP 的倍数（点击 + 天花板都是 1500 的倍数）
    expect(logWindow(4000, 1500, 1500)).toEqual({ rendered: 3000, hidden: 1000 });
    expect(logWindow(6000, 1500, 6000)).toEqual({ rendered: 6000, hidden: 0 });
  });
});
