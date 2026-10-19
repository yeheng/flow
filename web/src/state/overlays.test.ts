import { describe, expect, it } from "vitest";
import {
  closeAllOverlays,
  closeTopOverlay,
  isOverlayTop,
  openOverlay,
  overlaysOf,
  topOverlay,
} from "./overlays";

describe("overlays：全局弹层栈", () => {
  it("压栈/出栈/栈顶与 kind 查询", () => {
    const a = openOverlay("params", { nodeId: "n1" });
    const b = openOverlay("modal", { message: "m" });
    expect(topOverlay.value?.id).toBe(b.id);
    expect(isOverlayTop("modal")).toBe(true);
    expect(isOverlayTop("params")).toBe(false);
    expect(overlaysOf("params").map((e) => e.id)).toEqual([a.id]);

    b.close();
    expect(topOverlay.value?.id).toBe(a.id);
    a.close();
    expect(topOverlay.value).toBeNull();
    closeAllOverlays();
  });

  it("close 回调带结果；重复 close 幂等", () => {
    const seen: unknown[] = [];
    const entry = openOverlay("modal", {}, (r) => seen.push(r));
    entry.close("ok");
    entry.close("again"); // 已出栈，不再回调
    expect(seen).toEqual(["ok"]);
  });

  it("closeTopOverlay 按取消语义关栈顶；栈空返回 false", () => {
    const seen: unknown[] = [];
    openOverlay("x", {}, (r) => seen.push(r));
    expect(closeTopOverlay()).toBe(true);
    expect(seen).toEqual([undefined]); // 无结果 = 取消
    expect(closeTopOverlay()).toBe(false);
    closeAllOverlays();
  });

  it("Esc 只关栈顶一层（window capture）", () => {
    const closed: string[] = [];
    openOverlay("first", {}, () => closed.push("first"));
    openOverlay("second", {}, () => closed.push("second"));
    window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    expect(closed).toEqual(["second"]);
    window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    expect(closed).toEqual(["second", "first"]);
    closeAllOverlays();
  });
});
