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
});
