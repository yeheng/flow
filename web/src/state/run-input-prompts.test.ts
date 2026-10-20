import { beforeEach, describe, expect, it, vi } from "vitest";
import { requestRunInput } from "./run-input";
import { formDialog } from "./modal";
import type { Definition } from "../types";

vi.mock("./modal", () => ({ formDialog: vi.fn() }));
beforeEach(() => vi.resetAllMocks());

const definition: Definition = {
  nodes: [],
  edges: [],
  input_schema: {
    type: "object",
    required: ["name"],
    properties: {
      name: { type: "string", title: "名称", minLength: 1 },
      count: { type: "integer", default: 5 },
    },
  },
};

describe("运行前输入弹窗", () => {
  it("无必填字段的对象合同可以直接用空对象运行", async () => {
    expect(
      await requestRunInput(
        { nodes: [], edges: [], input_schema: { type: "object" } },
        [],
        undefined,
      ),
    ).toEqual({ input: {} });
    expect(formDialog).not.toHaveBeenCalled();
  });
  it("显式 null 不会被当作合法的对象输入", async () => {
    vi.mocked(formDialog).mockResolvedValue(null);
    expect(
      await requestRunInput({ nodes: [], edges: [], input_schema: { type: "object" } }, [], null),
    ).toBeNull();
    expect(formDialog).toHaveBeenCalled();
  });
  it("已有必填输入时不因缺少可选字段而强制弹窗", async () => {
    expect(await requestRunInput(definition, [], { name: "n" })).toEqual({ input: { name: "n" } });
    expect(formDialog).not.toHaveBeenCalled();
  });
  it("缺少声明字段弹表单，保留已有额外输入，取消时不启动", async () => {
    vi.mocked(formDialog).mockResolvedValue(null);
    expect(await requestRunInput(definition, [], { extra: 42 })).toBeNull();
    expect(formDialog).toHaveBeenCalledWith(
      "填写工作流运行输入",
      expect.arrayContaining([
        expect.objectContaining({ key: "name", label: "名称", required: true, type: "string" }),
        expect.objectContaining({ key: "count", defaultValue: 5 }),
      ]),
      expect.objectContaining({ values: { extra: 42 } }),
    );
  });
  it("确认后组装 typed JSON，并检查约束", async () => {
    vi.mocked(formDialog).mockResolvedValue({ name: "001", count: 5 });
    expect(await requestRunInput(definition, [], { extra: false })).toEqual({
      input: { extra: false, name: "001", count: 5 },
    });
    const validate = vi.mocked(formDialog).mock.calls[0][2]!.validate!;
    expect(validate({ name: "", count: 5 })).toBeTruthy();
    expect(validate({ name: "ok", count: "5" })).toBeTruthy();
    expect(validate({ name: "ok", count: 5 })).toBeUndefined();
  });
  it("合法的已有输入直接运行，包括 false、0 和空数组", async () => {
    expect(await requestRunInput(definition, [], { name: "n", count: 0 })).toEqual({
      input: { name: "n", count: 0 },
    });
    expect(formDialog).not.toHaveBeenCalled();
  });
  it("标量 input_schema 使用 JSON 表单", async () => {
    vi.mocked(formDialog).mockResolvedValue({ $: 3 });
    expect(
      await requestRunInput(
        { nodes: [], edges: [], input_schema: { type: "integer", minimum: 1 } },
        [],
        undefined,
      ),
    ).toEqual({ input: 3 });
  });
});
