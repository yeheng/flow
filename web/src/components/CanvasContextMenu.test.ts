import { afterEach, describe, expect, it, vi } from "vitest";
import { mount } from "@vue/test-utils";
import { flushPromises } from "@vue/test-utils";
import CanvasContextMenu, { type MenuEntry } from "./CanvasContextMenu.vue";

/** 从 window 派发键盘事件：菜单监听挂 window（capture），任何焦点位置都收得到 */
function key(k: string): void {
  window.dispatchEvent(new KeyboardEvent("keydown", { key: k, bubbles: true, cancelable: true }));
}

function mountMenu(entries: MenuEntry[]) {
  return mount(CanvasContextMenu, { props: { x: 20, y: 20, entries }, attachTo: document.body });
}

afterEach(() => {
  document.body.innerHTML = "";
});

describe("CanvasContextMenu：键盘可达（role=menu/menuitem）", () => {
  it("打开即聚焦首项；Enter 触发该项动作并关闭", () => {
    const edit = vi.fn();
    const copy = vi.fn();
    const w = mountMenu([
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
    expect(w.emitted("close")).toHaveLength(1);
    w.unmount();
  });

  it("Space 触发当前项；方向键上下移动焦点并回绕", () => {
    const first = vi.fn();
    const last = vi.fn();
    const w = mountMenu([
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
    expect(w.emitted("close")).toHaveLength(1);
    w.unmount();
  });

  it("Esc 关闭菜单", () => {
    const w = mountMenu([{ key: "a", label: "A", action: vi.fn() }]);
    key("Escape");
    expect(w.emitted("close")).toHaveLength(1);
    w.unmount();
  });

  it("menuitem 带漫游 tabindex；禁用项不聚焦不触发", () => {
    const del = vi.fn();
    const w = mountMenu([
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
    const w = mountMenu([
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
    expect(w.emitted("close")).toHaveLength(1);
    w.unmount();
  });
});
