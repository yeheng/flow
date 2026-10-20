import { describe, expect, it } from "vitest";
import { parseFormFields, schemaFields, validateInputSchema } from "./form-fields";

describe("workflow input contracts", () => {
  const schema = {
    type: "object",
    required: ["id", "count"],
    properties: {
      id: { type: "string", title: "编号", minLength: 2 },
      count: { type: "integer", minimum: 1 },
      tags: { type: "array", items: { type: "string" } },
      nested: { type: "object", required: ["ok"], properties: { ok: { type: "boolean" } } },
    },
    additionalProperties: false,
  };
  it("从声明生成必填字段和标签", () => {
    expect(schemaFields(schema).map((f) => [f.key, f.label, f.required])).toEqual([
      ["id", "编号", true],
      ["count", "count", true],
      ["tags", "tags", false],
      ["nested", "nested", false],
    ]);
  });
  it("检查缺值、类型、范围、嵌套和额外字段", () => {
    for (const value of [
      {},
      { id: "ok", count: "1" },
      { id: "ok", count: 0 },
      { id: "a", count: 1 },
      { id: "ok", count: 1, tags: [1] },
      { id: "ok", count: 1, nested: {} },
      { id: "ok", count: 1, extra: true },
    ]) {
      expect(validateInputSchema(schema, value)).toBeTruthy();
    }
    expect(
      validateInputSchema(schema, { id: "ok", count: 1, tags: ["x"], nested: { ok: false } }),
    ).toBeUndefined();
  });
  it("JSON 数组、对象和数字不会退化为字符串", () => {
    expect(
      parseFormFields([{ key: "v", label: "v", type: "object", required: true }], { v: '{"a":1}' }),
    ).toEqual({ v: { a: 1 } });
    expect(() =>
      parseFormFields([{ key: "v", label: "v", type: "integer" }], { v: "1.5" }),
    ).toThrow(/integer/);
  });
});
