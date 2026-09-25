import { reactive } from "vue";

export interface ModalRequest {
  kind: "confirm" | "prompt";
  message: string;
  defaultValue: string;
  resolve: (value: boolean | string | null) => void;
}

export const modalState = reactive<{ current: ModalRequest | null }>({ current: null });

export function confirmDialog(message: string): Promise<boolean> {
  return new Promise((resolve) => {
    // 已有弹窗时按取消关闭旧的，避免 Promise 悬挂
    modalState.current?.resolve(modalState.current.kind === "confirm" ? false : null);
    modalState.current = {
      kind: "confirm",
      message,
      defaultValue: "",
      resolve: resolve as (v: boolean | string | null) => void,
    };
  });
}

export function promptDialog(message: string, defaultValue = ""): Promise<string | null> {
  return new Promise((resolve) => {
    modalState.current?.resolve(modalState.current.kind === "confirm" ? false : null);
    modalState.current = {
      kind: "prompt",
      message,
      defaultValue,
      resolve: resolve as (v: boolean | string | null) => void,
    };
  });
}

/** 组件层唯一的关闭出口：value 为 null 表示取消 */
export function settleModal(value: string | null): void {
  const cur = modalState.current;
  if (!cur) return;
  modalState.current = null;
  cur.resolve(cur.kind === "confirm" ? value !== null : value);
}
