import { afterEach, describe, expect, it, vi } from "vitest";
import { mount } from "@vue/test-utils";
import { flushPromises } from "@vue/test-utils";
import CanvasContextMenu, { type MenuEntry, type MenuPayload } from "./CanvasContextMenu.vue";
import { closeAllOverlays, openOverlay, overlaysOf, type OverlayEntry } from "../state/overlays";

/** 从 window 派发键盘事件：菜单监听挂 window（capture），任何焦点位置都收得到 */
function key(k: string): void {
  window.dispatchEvent(new KeyboardEvent("keydown", { key: k, bubbles: true, cancelable: true }));
}

/** 打开菜单 = 压一条 context-menu 栈条目（FlowCanvas 的做法），再渲染该条目 */
function mountMenu(entries: MenuEntry[]) {
  const entry = openOverlay<MenuPayload>("context-menu", { x: 20, y: 20, entries });
  const w = mount(CanvasContextMenu, { props: { entry }, attachTo: document.body });
  return { w, entry };
}

function isOpen(entry: OverlayEntry<MenuPayload>): boolean {
  return overlaysOf("context-menu").some((e) => e.id === entry.id);
}

afterEach(() => {
  document.body.innerHTML = "";
  closeAllOverlays();
});

describe("CanvasContextMenu：键盘可达（role=menu/menuitem）", () => {
  it("打开即聚焦首项；Enter 触发该项动作并关闭条目", () => {
    const edit = vi.fn();
    const copy = vi.fn();
    const { w, entry } = mountMenu([
      { key: "edit", label: "编辑参数", action: edit },
      { key: "copy", label: "复制", action: copy },
      { key: "sep" },
      { key: "del", label: "删除", action: vi.fn(), danger: true },
    ]);
    const items = w.findAll('[role="menuitem"]');
    expect(items).toHaveLength(3);
    expect(document.activeElement).toBe(items[0].element);

    key("Enter");
    expect(edit).toHaveBeenCalledTimes(1);
    expect(copy).not.toHaveBeenCalled();
    expect(isOpen(entry)).toBe(false);
    w.unmount();
  });

  it("Space 触发当前项；方向键上下移动焦点并回绕", () => {
    const first = vi.fn();
    const last = vi.fn();
    const { w, entry } = mountMenu([
      { key: "a", label: "A", action: first },
      { key: "b", label: "B", action: vi.fn() },
      { key: "c", label: "C", action: last },
    ]);
    const items = w.findAll('[role="menuitem"]');

    key("ArrowDown");
    expect(document.activeElement).toBe(items[1].element);
    key("ArrowDown");
    expect(document.activeElement).toBe(items[2].element);
    key("ArrowDown"); // 回绕到首项
    expect(document.activeElement).toBe(items[0].element);
    key("ArrowUp"); // 回绕到末项
    expect(document.activeElement).toBe(items[2].element);

    key(" ");
    expect(last).toHaveBeenCalledTimes(1);
    expect(isOpen(entry)).toBe(false);
    w.unmount();
  });

  it("menuitem 带漫游 tabindex；禁用项不聚焦不触发", () => {
    const del = vi.fn();
    const { w } = mountMenu([
      { key: "del", label: "删除", action: del, disabled: true },
      { key: "copy", label: "复制", action: vi.fn() },
    ]);
    const items = w.findAll('[role="menuitem"]');
    expect(w.find(".ctx-menu").attributes("role")).toBe("menu");
    expect(items[0].attributes("tabindex")).toBe("-1");
    expect(items[1].attributes("tabindex")).toBe("0");
    // 首项禁用：焦点直接落到第二项
    expect(document.activeElement).toBe(items[1].element);
    key("Enter");
    expect(del).not.toHaveBeenCalled();
    w.unmount();
  });

  it("Enter 展开子菜单并聚焦首项，再 Enter 执行子项动作", async () => {
    const addLlm = vi.fn();
    const { w, entry } = mountMenu([
      {
        key: "add",
        label: "添加节点",
        children: [
          { key: "llm", label: "模型调用", action: addLlm },
          { key: "http", label: "HTTP 请求", action: vi.fn() },
        ],
      },
      { key: "copy", label: "复制", action: vi.fn() },
    ]);
    const items = w.findAll('[role="menuitem"]');
    expect(document.activeElement).toBe(items[0].element);

    key("Enter"); // 父项：展开子菜单
    await flushPromises();
    const subItems = w.findAll('.ctx-sub [role="menuitem"]');
    expect(subItems).toHaveLength(2);
    expect(document.activeElement).toBe(subItems[0].element);
    expect(items[0].attributes("aria-expanded")).toBe("true");

    key("Enter"); // 子项：执行动作并关闭
    expect(addLlm).toHaveBeenCalledTimes(1);
    expect(isOpen(entry)).toBe(false);
    w.unmount();
  });
});

describe("CanvasContextMenu：Esc 归全局弹层栈", () => {
  it("Esc 关菜单（host 关栈顶，菜单自身不监听 Escape）", () => {
    const { w, entry } = mountMenu([{ key: "a", label: "A", action: vi.fn() }]);
    key("Escape");
    expect(isOpen(entry)).toBe(false);
    w.unmount();
  });

  it("Esc 叠层只关一层：菜单压在参数弹层上时先关菜单", () => {
    const params = openOverlay("params", { nodeId: "n1" });
    const { w, entry } = mountMenu([{ key: "a", label: "A", action: vi.fn() }]);

    key("Escape");
    expect(isOpen(entry)).toBe(false); // 只关栈顶
    expect(overlaysOf("params").map((e) => e.id)).toEqual([params.id]);

    key("Escape");
    expect(overlaysOf("params")).toEqual([]);
    w.unmount();
  });
});
