import { describe, expect, it } from "vitest";
import { staleGuard } from "./stale";

describe("staleGuard", () => {
  it("begin 的令牌被更晚的 begin/invalidate 作废", () => {
    const guard = staleGuard();
    const first = guard.begin();
    expect(guard.stale(first)).toBe(false);
    const second = guard.begin();
    expect(guard.stale(first)).toBe(true);
    expect(guard.stale(second)).toBe(false);
    guard.invalidate();
    expect(guard.stale(second)).toBe(true);
  });

  it("并发操作各自比对自己的令牌", () => {
    const guard = staleGuard();
    const slow = guard.begin();
    const fast = guard.begin();
    expect(guard.stale(slow)).toBe(true); // 被更新的操作顶掉
    expect(guard.stale(fast)).toBe(false);
  });
});
