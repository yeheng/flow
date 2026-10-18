import { describe, expect, it, vi } from "vitest";
import { mount } from "@vue/test-utils";
import { defineComponent } from "vue";

// CodeEditor 只在 code widget 分支渲染；kv 测试不需要 Monaco，stub 掉避免 jsdom 加载
vi.mock("./CodeEditor.vue", () => ({
  default: defineComponent({ template: "<div />" }),
}));
vi.mock("../state/editor", () => ({ editor: { secrets: [], workflows: [] } }));

import SchemaField from "./SchemaField.vue";

const KV_SCHEMA = {
  type: "object",
  "x-widget": "key-value",
  "x-label": "请求头",
} as const;

function mountKv(modelValue: unknown) {
  return mount(SchemaField, {
    props: { name: "headers", schema: KV_SCHEMA as never, modelValue },
  });
}

describe("SchemaField key-value widget", () => {
  it("从 object 初始化行，编辑键值后 emit Record", async () => {
    const w = mountKv({ Accept: "application/json" });
    const rows = w.findAll(".kv-row");
    expect(rows).toHaveLength(1);
    expect((rows[0].find(".kv-key").element as HTMLInputElement).value).toBe("Accept");

    await rows[0].find(".kv-value").setValue("application/json; charset=utf-8");
    expect(w.emitted("update:modelValue")!.slice(-1)[0]![0]).toEqual({
      Accept: "application/json; charset=utf-8",
    });
  });

  it("添加行 → 填入键值后计入 params；空键行不写入", async () => {
    const w = mountKv(undefined);
    await w.find(".kv-add").trigger("click");
    await w.find(".kv-add").trigger("click");
    const rows = w.findAll(".kv-row");
    expect(rows).toHaveLength(2);
    await rows[0].find(".kv-key").setValue("X-Token");
    await rows[0].find(".kv-value").setValue("abc");
    // 第二行键为空：不写入
    expect(w.emitted("update:modelValue")!.slice(-1)[0]![0]).toEqual({ "X-Token": "abc" });
  });

  it("重复键：标红且重复行不写入（无静默 last-wins）", async () => {
    const w = mountKv({ A: "1" });
    await w.find(".kv-add").trigger("click");
    const rows = w.findAll(".kv-row");
    await rows[1].find(".kv-key").setValue("A");
    await rows[1].find(".kv-value").setValue("2");
    expect(w.findAll(".kv-dup")).toHaveLength(1);
    expect(w.emitted("update:modelValue")!.slice(-1)[0]![0]).toEqual({ A: "1" });
  });

  it("删完全部行 → emit undefined（从 params 剔除该键）", async () => {
    const w = mountKv({ A: "1" });
    await w.find(".kv-remove").trigger("click");
    expect(w.emitted("update:modelValue")!.slice(-1)[0]![0]).toBeUndefined();
  });
});

/** 与 @testing-library 的 getByLabelText 同契约：按 label 文案定位关联控件（for/id 或包裹） */
function getByLabelText(root: HTMLElement, text: string): HTMLElement {
  const label = Array.from(root.querySelectorAll("label")).find((l) =>
    (l.textContent ?? "").trim().startsWith(text),
  );
  if (!label) throw new Error(`找不到文案「${text}」的 label`);
  const id = label.getAttribute("for");
  const el = id
    ? Array.from(root.querySelectorAll<HTMLElement>("input, select, textarea")).find(
        (c) => c.id === id,
      )
    : label.querySelector<HTMLElement>("input, select, textarea");
  if (!el) throw new Error(`label「${text}」没有关联到控件`);
  return el;
}

describe("SchemaField label 关联（getByLabelText 可定位）", () => {
  it("text：label for 指向 input，可按文案定位并输入", async () => {
    const w = mount(SchemaField, {
      props: {
        name: "prompt",
        schema: { type: "string", "x-label": "提示词" } as never,
        modelValue: "你好",
      },
    });
    const input = getByLabelText(w.element as HTMLElement, "提示词");
    expect(input.tagName).toBe("INPUT");
    expect(input).toBe(w.find("input").element);
    expect((input as HTMLInputElement).value).toBe("你好");
    await w.find("input").setValue("改写");
    expect(w.emitted("update:modelValue")!.slice(-1)[0]![0]).toBe("改写");
  });

  it("boolean/number：label 与 checkbox/数字输入框关联", () => {
    const bool = mount(SchemaField, {
      props: {
        name: "cache",
        schema: { type: "boolean", "x-label": "启用缓存" } as never,
        modelValue: true,
      },
    });
    const box = getByLabelText(bool.element as HTMLElement, "启用缓存");
    expect((box as HTMLInputElement).type).toBe("checkbox");

    const num = mount(SchemaField, {
      props: {
        name: "timeout",
        schema: { type: "integer", "x-label": "超时秒数" } as never,
        modelValue: 3,
      },
    });
    const n = getByLabelText(num.element as HTMLElement, "超时秒数");
    expect((n as HTMLInputElement).type).toBe("number");
  });

  it("select（enum / workflow-picker）：label 关联 + aria-label", () => {
    const en = mount(SchemaField, {
      props: {
        name: "mode",
        schema: { type: "string", enum: ["fast", "slow"], "x-label": "重试策略" } as never,
        modelValue: "fast",
      },
    });
    const sel = getByLabelText(en.element as HTMLElement, "重试策略");
    expect(sel.tagName).toBe("SELECT");
    expect(sel.getAttribute("aria-label")).toBe("重试策略");

    const pick = mount(SchemaField, {
      props: {
        name: "workflow_id",
        schema: { type: "string", "x-widget": "workflow-picker", "x-label": "目标工作流" } as never,
        modelValue: undefined,
      },
    });
    const psel = getByLabelText(pick.element as HTMLElement, "目标工作流");
    expect(psel.tagName).toBe("SELECT");
    expect(psel.getAttribute("aria-label")).toBe("目标工作流");
  });
});
