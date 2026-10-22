import { describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";
import WorkflowInputForm from "./WorkflowInputForm.vue";

function mountForm(inputKeys: string[], modelValue: string) {
  return mount(WorkflowInputForm, { props: { inputKeys, modelValue } });
}

function lastEmit(w: ReturnType<typeof mountForm>): unknown[] {
  const events = w.emitted("update:modelValue") ?? [];
  return events[events.length - 1];
}

describe("WorkflowInputForm", () => {
  it("外部补填和清空立即回填表单，不反写旧值", async () => {
    const w = mountForm(["id"], "");
    await w.setProps({ modelValue: '{"id":"from-dialog"}' });
    expect((w.get("input").element as HTMLInputElement).value).toBe("from-dialog");
    await w.setProps({ modelValue: "" });
    expect((w.get("input").element as HTMLInputElement).value).toBe("");
    expect(w.emitted("update:modelValue")).toBeUndefined();
  });

  it("自身输入回显保留 5.0 的编辑缓冲", async () => {
    const w = mountForm(["count"], "");
    await w.get("input").setValue("5.0");
    await w.setProps({ modelValue: lastEmit(w)[0] as string });
    expect((w.get("input").element as HTMLInputElement).value).toBe("5.0");
  });

  it("改动其他字段不会将 schema 声明的字符串变成数字或布尔值", async () => {
    const w = mountForm(["id", "count"], '{"id":"true","count":1}');
    await w.setProps({
      inputSchema: {
        type: "object",
        properties: { id: { type: "string" }, count: { type: "integer" } },
      },
    });
    await w.findAll("input")[1].setValue("2");
    expect(JSON.parse(lastEmit(w)[0] as string)).toEqual({ id: "true", count: 2 });
  });

  it("字段和输入同时切换时不把旧工作流的值写进新工作流", async () => {
    const w = mountForm(["id"], '{"id":"old"}');
    await w.setProps({ inputKeys: ["id", "count"], modelValue: '{"id":"new","count":2}' });
    expect(w.findAll("input").map((i) => (i.element as HTMLInputElement).value)).toEqual([
      "new",
      "2",
    ]);
    expect(w.emitted("update:modelValue")).toBeUndefined();
  });
  it("无字段时只渲染 JSON 文本域", () => {
    const w = mountForm([], "");
    expect(w.find(".wf-input-json").exists()).toBe(true);
    expect(w.find(".wf-input-toggle").exists()).toBe(false);
  });

  it("表单模式逐字段输入，组装为 JSON 文本 emit", async () => {
    const w = mountForm(["count", "src"], "");
    const fields = w.findAll(".wf-input-field input");
    expect(fields).toHaveLength(2);
    await fields[0].setValue("5");
    expect(lastEmit(w)).toEqual(['{"count":5}']);
    await fields[1].setValue("cron");
    expect(lastEmit(w)).toEqual(['{"count":5,"src":"cron"}']);
  });

  it("挂载时从已有 JSON 回填表单", () => {
    const w = mountForm(["token", "user"], '{"token":"abc","user":{"id":1}}');
    const fields = w.findAll(".wf-input-field input");
    expect((fields[0].element as HTMLInputElement).value).toBe("abc");
    expect((fields[1].element as HTMLInputElement).value).toBe('{"id":1}');
  });

  it("切换到 JSON 模式显示组装结果，编辑直接透传", async () => {
    const w = mountForm(["k"], "");
    await w.find(".wf-input-field input").setValue("v");
    await w.setProps({ modelValue: lastEmit(w)[0] as string });
    await w.find(".wf-input-toggle").trigger("click");
    const ta = w.find(".wf-input-json");
    expect((ta.element as HTMLTextAreaElement).value).toBe('{"k":"v"}');
    await ta.setValue('{"k":2}');
    expect(lastEmit(w)).toEqual(['{"k":2}']);
    // 切回表单：JSON 改动回填到字段
    await w.setProps({ modelValue: '{"k":2}' });
    await w.find(".wf-input-toggle").trigger("click");
    expect((w.find(".wf-input-field input").element as HTMLInputElement).value).toBe("2");
  });

  // P0-2: inputKeys 是父组件 computed，节点参数一变就返回新数组引用；
  // 旧实现无条件 hydrate + emitForm，会把 JSON 模式的原文/未声明键重写掉。
  it("modelValue 非对象时键变化不得改写原文（数组不被清成 / 兜底不丢原文）", async () => {
    const w = mountForm(["k"], "[1,2]");
    // 同内容新引用：不得 emit
    await w.setProps({ inputKeys: ["k"] });
    // 键集合变化：仍不得 emit（解析不出对象基底，无法无损 merge）
    await w.setProps({ inputKeys: ["k", "x"] });
    expect(w.emitted("update:modelValue")).toBeUndefined();
    // 兜底提示明确：表单编辑不写回
    expect(w.find(".wf-input-warn").exists()).toBe(true);
    await w.findAll(".wf-input-field input")[0].setValue("9");
    expect(w.emitted("update:modelValue")).toBeUndefined();
  });

  it("JSON→表单切换不 emit，未声明键在后续编辑中保留", async () => {
    const w = mountForm(["k"], '{"k":1,"extra":2}');
    await w.find(".wf-input-toggle").trigger("click"); // form → json
    await w.find(".wf-input-toggle").trigger("click"); // json → form
    expect(w.emitted("update:modelValue")).toBeUndefined();
    expect((w.find(".wf-input-field input").element as HTMLInputElement).value).toBe("1");

    await w.find(".wf-input-field input").setValue("2");
    expect(lastEmit(w)).toEqual(['{"k":2,"extra":2}']);
  });

  it("JSON 模式下键变化不 emit，半截 JSON 不会被改写", async () => {
    const w = mountForm(["k"], "");
    await w.find(".wf-input-toggle").trigger("click"); // form → json
    const ta = w.find(".wf-input-json");
    await ta.setValue('{"k":');
    expect((ta.element as HTMLTextAreaElement).value).toBe('{"k":');
    const before = w.emitted("update:modelValue")!.length;

    await w.setProps({ inputKeys: ["k", "x"] });
    expect(w.emitted("update:modelValue")).toHaveLength(before);
  });

  it("表单模式下同内容的新数组引用不 emit（父组件 computed 每次重算）", async () => {
    const w = mountForm(["a"], '{"a":1}');
    await w.setProps({ inputKeys: ["a"] });
    expect(w.emitted("update:modelValue")).toBeUndefined();
  });

  it("表单模式下键增删才 emit merge 结果（保留未声明键）", async () => {
    const w = mountForm(["a"], '{"a":1,"extra":2}');
    await w.findAll(".wf-input-field input")[0].setValue("1"); // 同值输入：emit 带额外键
    expect(lastEmit(w)).toEqual(['{"a":1,"extra":2}']);

    await w.setProps({ modelValue: lastEmit(w)[0] as string });
    await w.setProps({ inputKeys: ["a", "b"] });
    expect(lastEmit(w)).toEqual(['{"a":1,"extra":2}']);
    await w.findAll(".wf-input-field input")[1].setValue("3");
    expect(lastEmit(w)).toEqual(['{"a":1,"extra":2,"b":3}']);
  });
});
