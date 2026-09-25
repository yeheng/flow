import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { dismissToast, showToast, toasts } from "./toast";

describe("toast 队列", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    toasts.splice(0);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("info/success 4.5s 自动消失，error 8s", () => {
    showToast("info", "提示");
    showToast("success", "成功");
    showToast("error", "失败");
    expect(toasts).toHaveLength(3);

    vi.advanceTimersByTime(4600);
    expect(toasts.map((t) => t.kind)).toEqual(["error"]);

    vi.advanceTimersByTime(8000);
    expect(toasts).toHaveLength(0);
  });

  it("多条堆叠且可手动关闭", () => {
    showToast("info", "a");
    showToast("info", "b");
    expect(toasts.map((t) => t.text)).toEqual(["a", "b"]);
    dismissToast(toasts[0].id);
    expect(toasts.map((t) => t.text)).toEqual(["b"]);
  });
});
