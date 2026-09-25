import { reactive } from "vue";

export type ToastKind = "info" | "success" | "error";

export interface ToastItem {
  id: number;
  kind: ToastKind;
  text: string;
}

export const toasts = reactive<ToastItem[]>([]);

let nextId = 1;

export function dismissToast(id: number): void {
  const i = toasts.findIndex((t) => t.id === id);
  if (i >= 0) toasts.splice(i, 1);
}

export function showToast(kind: ToastKind, text: string): void {
  const id = nextId++;
  toasts.push({ id, kind, text });
  // error 停留更久，便于读完错误信息；三类都可手动关闭
  setTimeout(() => dismissToast(id), kind === "error" ? 8000 : 4500);
}

export const toast = {
  info: (text: string): void => showToast("info", text),
  success: (text: string): void => showToast("success", text),
  error: (text: string): void => showToast("error", text),
};
