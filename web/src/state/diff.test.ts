import { describe, expect, it } from "vitest";
import { diffDefinitions, isEmptyDiff } from "./diff";
import type { Definition } from "../types";

function def(
  nodes: {
    id: string;
    type?: string;
    name?: string;
    params?: Record<string, unknown>;
    x?: number;
  }[],
  edges: { from: string; to: string; port?: string }[] = [],
): Definition {
  return {
    nodes: nodes.map((n) => ({
      id: n.id,
      type: n.type ?? "script",
      name: n.name,
      position: { x: n.x ?? 0, y: 0 },
      params: n.params ?? {},
    })),
    edges,
  };
}

describe("diffDefinitions", () => {
  it("相同定义产生空 diff；空定义边界", () => {
    const a = def([{ id: "n", params: { code: "1" } }]);
    const b = def([{ id: "n", params: { code: "1" } }]);
    expect(isEmptyDiff(diffDefinitions(a, b))).toBe(true);
    expect(isEmptyDiff(diffDefinitions(def([]), def([])))).toBe(true);
  });

  it("新增/删除节点", () => {
    const a = def([{ id: "a" }, { id: "b" }]);
    const b = def([{ id: "b" }, { id: "c" }]);
    const d = diffDefinitions(a, b);
    expect(d.nodesAdded).toEqual(["c"]);
    expect(d.nodesRemoved).toEqual(["a"]);
    expect(d.nodesChanged).toEqual([]);
  });

  it("字段级变更：type / name / params 逐键，未变字段不列出", () => {
    const a = def([
      { id: "n", name: "旧名", params: { code: "1", retries: 2, same: "x" } },
      { id: "m", params: { keep: true } },
    ]);
    const b = def([
      { id: "n", type: "condition", name: "新名", params: { code: "2", same: "x", extra: [1, 2] } },
      { id: "m", params: { keep: true } },
    ]);
    const d = diffDefinitions(a, b);
    expect(d.nodesChanged).toHaveLength(1);
    const changes = d.nodesChanged[0].changes;
    expect(changes).toContainEqual({ field: "type", from: "script", to: "condition" });
    expect(changes).toContainEqual({ field: "name", from: "旧名", to: "新名" });
    expect(changes).toContainEqual({ field: "params.code", from: "1", to: "2" });
    expect(changes).toContainEqual({ field: "params.retries", from: 2, to: undefined });
    expect(changes).toContainEqual({ field: "params.extra", from: undefined, to: [1, 2] });
    expect(changes.some((c) => c.field === "params.same")).toBe(false);
  });

  it("params 深比较：对象内容相同但键序不同不算变更", () => {
    const a = def([{ id: "n", params: { cfg: { x: 1, y: [1, 2] } } }]);
    const b = def([{ id: "n", params: { cfg: { y: [1, 2], x: 1 } } }]);
    expect(isEmptyDiff(diffDefinitions(a, b))).toBe(true);
  });

  it("位置变化不算变更", () => {
    const a = def([{ id: "n", x: 0 }]);
    const b = def([{ id: "n", x: 500 }]);
    expect(isEmptyDiff(diffDefinitions(a, b))).toBe(true);
  });

  it("边增删按 from/to/port 配对", () => {
    const a = def(
      [],
      [
        { from: "a", to: "b" },
        { from: "c", to: "d", port: "true" },
      ],
    );
    const b = def(
      [],
      [
        { from: "a", to: "b" },
        { from: "c", to: "d", port: "false" },
        { from: "e", to: "f" },
      ],
    );
    const d = diffDefinitions(a, b);
    expect(d.edgesAdded).toHaveLength(2);
    expect(d.edgesRemoved).toHaveLength(1);
    expect(d.edgesRemoved[0]).toEqual({ from: "c", to: "d", port: "true" });
  });
});
