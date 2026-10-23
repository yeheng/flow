import { ref, type Ref } from "vue";

export interface PanelResizeOptions {
  /** 目标元素：拖拽开始时取它的 rect 作为测量基准 */
  target: Ref<HTMLElement | null>;
  /** localStorage 键（pointerup 时写回） */
  storageKey: string;
  fallback: number;
  min: number;
  max: () => number;
  /** 由指针位置计算新尺寸：右栏 rect.right - clientX；左栏 clientX - rect.left；底栏 rect.bottom - clientY */
  measure: (rect: DOMRect, e: PointerEvent) => number;
}

function readSize(key: string, fallback: number): number {
  try {
    const v = Number(window.localStorage.getItem(key));
    return Number.isFinite(v) && v > 0 ? v : fallback;
  } catch { return fallback; }
}

/** 面板拖拽 resize：拖动中只改 size ref，松手才写 localStorage；active 供调用方关掉过渡动画 */
export function usePanelResize(opts: PanelResizeOptions) {
  const size = ref(readSize(opts.storageKey, opts.fallback));
  const active = ref(false);

  function onDragStart(e: PointerEvent): void {
    const el = opts.target.value;
    if (!el || e.button !== 0) return;
    e.preventDefault();
    const rect = el.getBoundingClientRect();
    active.value = true;
    const onMove = (ev: PointerEvent): void => {
      size.value = Math.min(Math.max(opts.measure(rect, ev), opts.min), opts.max());
    };
    const onUp = (): void => {
      window.removeEventListener("pointermove", onMove);
      window.removeEventListener("pointerup", onUp);
      active.value = false;
      try { window.localStorage.setItem(opts.storageKey, String(size.value)); } catch { /* Private storage is optional. */ }
    };
    window.addEventListener("pointermove", onMove);
    window.addEventListener("pointerup", onUp);
  }

  return { size, active, onDragStart };
}
