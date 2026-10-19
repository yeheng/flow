/**
 * 全局弹层栈：confirm/prompt/参数面板/业务节点/对比/回归/右键菜单共用一份
 * 「什么开着」的事实。此前每个弹层各持一个布尔（editor.paramsOpen、
 * templateSave.open、showCompare…），Esc 各管各的 → 叠层时同关两层。
 *
 * 规则只有一条：**Esc 关栈顶**（installOverlayEsc 统一处理，组件不再各自
 * 监听 Escape）；渲染方按 kind 取自己的条目；close() 是唯一关闭出口。
 */
import { computed, reactive } from "vue";

export interface OverlayEntry<P = unknown> {
  id: number;
  kind: string;
  payload: P;
  /** 关闭该弹层：出栈 + 回调（取消语义由调用方在回调里解释） */
  close: (result?: unknown) => void;
}

const stack = reactive<{ entries: OverlayEntry[] }>({ entries: [] });
let nextId = 1;

/** 栈顶弹层：Esc 与「吞快捷键」判定只看它 */
export const topOverlay = computed(() => stack.entries[stack.entries.length - 1] ?? null);

/** 打开弹层：压栈并返回条目；onClose 在 close() 时收到结果（undefined=取消） */
export function openOverlay<P>(
  kind: string,
  payload: P,
  onClose?: (result: unknown) => void,
): OverlayEntry<P> {
  installOverlayEsc();
  const id = nextId++;
  let closed = false;
  const entry: OverlayEntry<P> = {
    id,
    kind,
    payload,
    close: (result?: unknown) => {
      if (closed) return; // 幂等：重复 close 不再回调
      closed = true;
      const index = stack.entries.findIndex((e) => e.id === id);
      if (index >= 0) stack.entries.splice(index, 1);
      onClose?.(result);
    },
  };
  stack.entries.push(entry as OverlayEntry);
  return entry;
}

/** 关闭栈顶（Esc 的唯一出口）；栈空返回 false */
export function closeTopOverlay(result?: unknown): boolean {
  const top = topOverlay.value;
  if (!top) return false;
  top.close(result);
  return true;
}

/** 关闭全部弹层（路由切换/测试收尾），未结条目按取消语义回调 */
export function closeAllOverlays(result?: unknown): void {
  for (const entry of [...stack.entries]) entry.close(result);
}

/** 指定 kind 的条目（渲染用；通常 0..1 条） */
export function overlaysOf<P>(kind: string): OverlayEntry<P>[] {
  return stack.entries.filter((e) => e.kind === kind) as OverlayEntry<P>[];
}

/** 某 kind 是否在栈顶（弹层内部「吞快捷键」等判定用） */
export function isOverlayTop(kind: string): boolean {
  return topOverlay.value?.kind === kind;
}

let escInstalled = false;

/**
 * 全局 Esc 监听（capture + stopImmediatePropagation）：只关栈顶、其余
 * 键盘处理（如编辑器快捷键/校验浮层）在有弹层时整体让位。
 * 幂等；首次 openOverlay 时自动生效——不存在「谁负责安装」的协调问题。
 */
export function installOverlayEsc(): () => void {
  if (escInstalled) return () => {};
  escInstalled = true;
  const onKeydown = (e: KeyboardEvent): void => {
    if (e.key !== "Escape") return;
    // 关栈顶按「取消」收场（无结果）：prompt 解析 null、confirm 解析 false，
    // 不能带结果字符串——那会被当成用户输入
    if (closeTopOverlay()) {
      e.preventDefault();
      e.stopImmediatePropagation();
    }
  };
  window.addEventListener("keydown", onKeydown, true);
  return () => {
    window.removeEventListener("keydown", onKeydown, true);
    escInstalled = false;
  };
}
