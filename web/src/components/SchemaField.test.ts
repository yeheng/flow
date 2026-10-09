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
    expect(w.emitted("update:modelValue")!.at(-1)![0]).toEqual({
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
    expect(w.emitted("update:modelValue")!.at(-1)![0]).toEqual({ "X-Token": "abc" });
  });

  it("重复键：标红且重复行不写入（无静默 last-wins）", async () => {
    const w = mountKv({ A: "1" });
    await w.find(".kv-add").trigger("click");
    const rows = w.findAll(".kv-row");
    await rows[1].find(".kv-key").setValue("A");
    await rows[1].find(".kv-value").setValue("2");
    expect(w.findAll(".kv-dup")).toHaveLength(1);
    expect(w.emitted("update:modelValue")!.at(-1)![0]).toEqual({ A: "1" });
  });

  it("删完全部行 → emit undefined（从 params 剔除该键）", async () => {
    const w = mountKv({ A: "1" });
    await w.find(".kv-remove").trigger("click");
    expect(w.emitted("update:modelValue")!.at(-1)![0]).toBeUndefined();
  });
});
