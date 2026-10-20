import { describe, expect, it } from "vitest";
import { reactive, ref, shallowRef } from "vue";
import { deepCloneJson } from "./clone";

describe("deepCloneJson", () => {
  it("clones plain JSON faithfully", () => {
    const source = { a: 1, b: { c: ["x", 2, null] } };
    const copy = deepCloneJson(source);
    expect(copy).toEqual(source);
    expect(copy).not.toBe(source);
    expect(copy.b).not.toBe(source.b);
  });

  // 回归：structuredClone 对 raw 树深处藏着的 Proxy/ref 抛
  // DataCloneError，曾把保存/发布整条链路炸掉（Unhandled Promise
  // Rejection: DataCloneError）。JSON 面语义下必须退化为 JSON 往返。
  it("survives a reactive proxy nested inside a raw tree", () => {
    const inner = reactive({ nested: "value" });
    const raw: Record<string, unknown> = { params: { ok: 1 } };
    // 绕过 reactive setter 直接挂 Proxy（跨树赋值等运行时形态）
    raw["leak"] = inner;
    const copy = deepCloneJson(raw);
    expect(copy).toEqual({ params: { ok: 1 }, leak: { nested: "value" } });
  });

  it("survives refs and shallow containers in the tree", () => {
    const holder = shallowRef({
      count: ref(3),
      label: ref("x"),
    });
    const copy = deepCloneJson(holder.value);
    expect(copy).toEqual({ count: 3, label: "x" });
  });
});
