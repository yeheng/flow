import { afterEach, describe, expect, it } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import AppModal from "./AppModal.vue";
import { confirmDialog, modalState, promptDialog } from "../state/modal";

/** keydown 派发到 window：弹窗监听挂 window，消息区点过/失焦后也响应 */
function key(k: string, init: KeyboardEventInit = {}): void {
  window.dispatchEvent(
    new KeyboardEvent("keydown", { key: k, bubbles: true, cancelable: true, ...init }),
  );
}

afterEach(() => {
  document.body.innerHTML = "";
  modalState.current = null;
});

describe("AppModal：焦点管理", () => {
  it("prompt 打开聚焦输入框；Esc 取消并把焦点还原到打开前元素", async () => {
    const trigger = document.createElement("button");
    document.body.appendChild(trigger);
    trigger.focus();

    const w = mount(AppModal, { attachTo: document.body });
    const p = promptDialog("模板名称？", "默认名");
    await flushPromises();

    const input = w.find("input").element as HTMLInputElement;
    expect(document.activeElement).toBe(input);
    expect(input.value).toBe("默认名");

    key("Escape");
    await expect(p).resolves.toBeNull();
    expect(modalState.current).toBeNull();
    expect(document.activeElement).toBe(trigger);
    w.unmount();
    trigger.remove();
  });

  it("confirm：Enter 确认（window keydown，点过消息区也生效）", async () => {
    const w = mount(AppModal, { attachTo: document.body });
    const p = confirmDialog("删除该节点？", { danger: true });
    await flushPromises();
    // 默认焦点在确定按钮上
    expect(document.activeElement).toBe(w.findAll("button")[1].element);

    await w.find(".modal-message").trigger("click");
    key("Enter");
    await expect(p).resolves.toBe(true);
    expect(modalState.current).toBeNull();
    w.unmount();
  });

  it("Tab 焦点在弹窗内循环（末项 → 首项，Shift+Tab 反向）", async () => {
    const w = mount(AppModal, { attachTo: document.body });
    const p = confirmDialog("继续？");
    await flushPromises();

    const buttons = w.findAll("button").map((b) => b.element as HTMLElement);
    const first = buttons[0];
    const last = buttons[buttons.length - 1];

    last.focus();
    key("Tab");
    expect(document.activeElement).toBe(first);
    key("Tab", { shiftKey: true });
    expect(document.activeElement).toBe(last);

    key("Escape");
    await p;
    w.unmount();
  });

  it("dialog：role=dialog、aria-modal 且 aria-labelledby 指向消息文本", async () => {
    const w = mount(AppModal, { attachTo: document.body });
    const p = promptDialog("名称？");
    await flushPromises();

    const dialog = w.get(".modal");
    expect(dialog.attributes("role")).toBe("dialog");
    expect(dialog.attributes("aria-modal")).toBe("true");
    const id = dialog.attributes("aria-labelledby");
    expect(id).toBeTruthy();
    expect(w.find(".modal-message").attributes("id")).toBe(id);

    key("Escape");
    await p;
    w.unmount();
  });
});
