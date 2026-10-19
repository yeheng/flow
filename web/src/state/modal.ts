/**
 * confirm/prompt 两种系统弹窗：叠在全局弹层栈上（kind="modal"），
 * 对外仍是 Promise API。打开新弹窗时旧弹窗按取消收场，避免 Promise 悬挂
 * （原有语义）。类型安全：confirm 解析 boolean、prompt 解析 string|null，
 * 不再有跨类型强转。
 */
import { openOverlay, overlaysOf, type OverlayEntry } from "./overlays";

export interface ModalPayload {
  kind: "confirm" | "prompt";
  message: string;
  defaultValue: string;
  danger?: boolean;
  confirmText?: string;
}

export type ModalEntry = OverlayEntry<ModalPayload>;

/** 渲染方（AppModal）取当前弹窗条目；由打开语义保证同刻至多一条 */
export function modalEntries(): ModalEntry[] {
  return overlaysOf<ModalPayload>("modal");
}

/** 取消栈上未结的旧弹窗（新弹窗顶替旧弹窗的既有语义） */
function cancelExisting(): void {
  for (const entry of modalEntries()) entry.close(null);
}

export function confirmDialog(
  message: string,
  opts?: { danger?: boolean; confirmText?: string },
): Promise<boolean> {
  return new Promise((resolve) => {
    cancelExisting();
    openOverlay<ModalPayload>(
      "modal",
      {
        kind: "confirm",
        message,
        defaultValue: "",
        danger: opts?.danger,
        confirmText: opts?.confirmText,
      },
      (result) => resolve(result === true),
    );
  });
}

export function promptDialog(message: string, defaultValue = ""): Promise<string | null> {
  return new Promise((resolve) => {
    cancelExisting();
    openOverlay<ModalPayload>("modal", { kind: "prompt", message, defaultValue }, (result) =>
      resolve(typeof result === "string" ? result : null),
    );
  });
}

/** 组件层唯一的关闭出口：value 为 null 表示取消 */
export function settleModal(entry: ModalEntry, value: string | null): void {
  entry.close(entry.payload.kind === "confirm" ? value !== null : value);
}
