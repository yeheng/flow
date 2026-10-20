import { onMounted, onUnmounted, type Ref } from "vue";

/** Tab 焦点只在弹窗内循环；内部非边缘焦点交给控件本身（Monaco 靠 Tab 缩进） */
const FOCUSABLE = 'button, [href], input, textarea, select, [tabindex]:not([tabindex="-1"])';

/** 单次 Tab 拦截：焦点到达边缘时绕回另一端，弹窗外焦点拉回弹窗 */
export function trapTabIn(dialogEl: HTMLElement | null, e: KeyboardEvent): void {
  const items = Array.from(dialogEl?.querySelectorAll<HTMLElement>(FOCUSABLE) ?? []).filter(
    (el) => !el.hasAttribute("disabled"),
  );
  if (items.length === 0) return;
  const active = document.activeElement;
  const inside = dialogEl?.contains(active) ?? false;
  const edge = e.shiftKey ? items[0] : items[items.length - 1];
  if (inside && active !== edge) return;
  e.preventDefault();
  (e.shiftKey ? items[items.length - 1] : items[0]).focus();
}

/**
 * 弹窗焦点陷阱：挂 window 监听 Tab（点过消息区、焦点跑到弹窗外也循环）。
 * open 为 false 时不拦。Esc 归全局弹层栈，这里不掺和。
 */
export function useModalFocusTrap(dialogEl: Ref<HTMLElement | null>, open: Ref<boolean>): void {
  function onWindowKeydown(e: KeyboardEvent): void {
    if (!open.value || e.key !== "Tab") return;
    trapTabIn(dialogEl.value, e);
  }
  onMounted(() => window.addEventListener("keydown", onWindowKeydown));
  onUnmounted(() => window.removeEventListener("keydown", onWindowKeydown));
}
