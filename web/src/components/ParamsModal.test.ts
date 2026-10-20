import { afterEach, describe, expect, it, vi } from "vitest";
import { mount, flushPromises } from "@vue/test-utils";
import { defineComponent } from "vue";
import type { NodeTypeDesc } from "../types";

// CodeEditor 只在 code widget 分支渲染；本测试的 schema 不含 code，stub 掉避免 jsdom 加载 Monaco
vi.mock("./CodeEditor.vue", () => ({ default: defineComponent({ template: "<div />" }) }));
vi.mock("../state/history", () => ({ commitParams: vi.fn() }));
vi.mock("../state/editor", async () => {
  const { ref } = await import("vue");
  return { selectedNode: ref(null), nodeValidationError: () => null };
});

import ParamsModal from "./ParamsModal.vue";
import { selectedNode } from "../state/editor";
import { closeAllOverlays, openOverlay, overlaysOf, topOverlay } from "../state/overlays";
import { confirmDialog } from "../state/modal";

function openParams(desc: Partial<NodeTypeDesc>): void {
  (selectedNode as unknown as { value: unknown }).value = {
    id: "n1",
    data: {
      name: "节点一",
      params: {},
      nodeType: {
        type: "http_call",
        label: "HTTP 请求",
        category: "action",
        ports: [],
        params_schema: { type: "object", properties: {} },
        ...desc,
      },
    },
  };
  openOverlay("params", { nodeId: "n1" });
}

function key(k: string, init: KeyboardEventInit = {}): void {
  window.dispatchEvent(
    new KeyboardEvent("keydown", { key: k, bubbles: true, cancelable: true, ...init }),
  );
}

afterEach(() => {
  document.body.innerHTML = "";
  closeAllOverlays();
  (selectedNode as unknown as { value: unknown }).value = null;
});

describe("ParamsModal：retry_scope 提示", () => {
  it("retry_scope=declared_repeatable_only 且支持重试时提示自动重试作用域", async () => {
    const w = mount(ParamsModal, { attachTo: document.body });
    openParams({ supports_retry: true, retry_scope: "declared_repeatable_only" });
    await flushPromises();

    expect(w.find(".retry").text()).toContain(
      "自动重试只对显式声明可重复（effect_policy=repeatable）的失败生效",
    );
    w.unmount();
  });

  it("未声明 retry_scope（或不支持重试）时不显示该提示", async () => {
    const w = mount(ParamsModal, { attachTo: document.body });
    openParams({ supports_retry: true });
    await flushPromises();
    expect(w.text()).not.toContain("自动重试只对显式声明可重复");

    closeAllOverlays();
    (selectedNode as unknown as { value: unknown }).value = null;
    await flushPromises();
    openParams({ supports_retry: false, retry_scope: "declared_repeatable_only" });
    await flushPromises();
    expect(w.text()).not.toContain("自动重试只对显式声明可重复");
    w.unmount();
  });
});

describe("ParamsModal：焦点管理", () => {
  it("打开聚焦名称输入框；Esc（全局弹层栈）关闭并把焦点还原到打开前元素", async () => {
    const trigger = document.createElement("button");
    document.body.appendChild(trigger);
    trigger.focus();

    openParams({ supports_retry: true });
    const w = mount(ParamsModal, { attachTo: document.body });
    await flushPromises();

    const nameInput = w.find("#node-name").element as HTMLInputElement;
    expect(document.activeElement).toBe(nameInput);

    key("Escape");
    expect(overlaysOf("params")).toEqual([]);
    await flushPromises();
    expect(document.activeElement).toBe(trigger);
    w.unmount();
    trigger.remove();
  });

  it("Tab 焦点在弹窗内循环（末项 → 首项，Shift+Tab 反向）", async () => {
    openParams({ supports_retry: true });
    const w = mount(ParamsModal, { attachTo: document.body });
    await flushPromises();

    const items = Array.from(
      (w.element as HTMLElement).querySelectorAll<HTMLElement>(
        'button, [href], input, textarea, select, [tabindex]:not([tabindex="-1"])',
      ),
    );
    expect(items.length).toBeGreaterThan(1);
    const first = items[0];
    const last = items[items.length - 1];

    last.focus();
    key("Tab");
    expect(document.activeElement).toBe(first);
    key("Tab", { shiftKey: true });
    expect(document.activeElement).toBe(last);
    w.unmount();
  });

  it("dialog：aria-labelledby 指向标题「编辑参数」", async () => {
    openParams({});
    const w = mount(ParamsModal, { attachTo: document.body });
    await flushPromises();

    const dialog = w.get(".params-modal");
    expect(dialog.attributes("role")).toBe("dialog");
    expect(dialog.attributes("aria-modal")).toBe("true");
    const id = dialog.attributes("aria-labelledby");
    expect(id).toBeTruthy();
    expect(w.find("h3").attributes("id")).toBe(id);
    expect(w.find("h3").text()).toBe("编辑参数");
    w.unmount();
  });
});

describe("ParamsModal：弹层栈", () => {
  it("Esc 叠层只关一层：confirm 压在参数弹层上时先关 confirm", async () => {
    openParams({});
    const w = mount(ParamsModal, { attachTo: document.body });
    await flushPromises();
    const p = confirmDialog("继续？");
    await flushPromises();
    expect(topOverlay.value?.kind).toBe("modal");

    key("Escape");
    await expect(p).resolves.toBe(false);
    // 只关栈顶：参数弹层还在
    expect(overlaysOf("params")).toHaveLength(1);
    expect(topOverlay.value?.kind).toBe("params");
    expect(w.find(".params-modal").exists()).toBe(true);

    key("Escape");
    await flushPromises();
    expect(overlaysOf("params")).toEqual([]);
    expect(w.find(".params-modal").exists()).toBe(false);
    w.unmount();
  });
});
