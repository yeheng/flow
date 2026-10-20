import { describe, expect, it } from "vitest";
import { assembleInput, discoverInputKeys, inputFieldText, parseInputFields } from "./run-input";
import type { Definition, NodeTypeDesc } from "../types";

const NODE_TYPES: NodeTypeDesc[] = [
  {
    type: "http",
    label: "HTTP 请求",
    category: "net",
    ports: [],
    params_schema: {
      type: "object",
      properties: {
        url: { type: "string" },
        headers: { type: "object" },
      },
    },
  },
  {
    type: "script",
    label: "脚本",
    category: "logic",
    ports: [],
    params_schema: {
      type: "object",
      properties: {
        body: { type: "string", "x-widget": "code", "x-opaque": true },
      },
    },
  },
];

function def(nodes: Definition["nodes"]): Definition {
  return { nodes, edges: [] };
}

describe("discoverInputKeys", () => {
  it("扫描模板引用 ${input.xxx}", () => {
    const d = def([
      {
        id: "a",
        type: "http",
        params: { url: "https://api.example.com/?token=${input.token}" },
      },
    ]);
    expect(discoverInputKeys(d, NODE_TYPES)).toEqual(["token"]);
  });

  it("嵌套路径只取顶层字段，容忍花括号内空格", () => {
    const d = def([
      { id: "a", type: "http", params: { url: "${ input.user.name }/${input.user.id}" } },
    ]);
    expect(discoverInputKeys(d, NODE_TYPES)).toEqual(["user"]);
  });

  it("普通字符串里的 input.xxx 不算（必须走 ${} 模板）", () => {
    const d = def([{ id: "a", type: "http", params: { url: "https://input.example.com/help" } }]);
    expect(discoverInputKeys(d, NODE_TYPES)).toEqual([]);
  });

  it("x-opaque 脚本体按代码扫描 input.xxx 全局引用", () => {
    const d = def([
      {
        id: "s",
        type: "script",
        params: { body: "const t = input.token; return input.retry ? t : t + input.tag;" },
      },
    ]);
    expect(discoverInputKeys(d, NODE_TYPES)).toEqual(["retry", "tag", "token"]);
  });

  it("未知节点类型只扫模板引用，不碰代码", () => {
    const d = def([{ id: "x", type: "unknown_type", params: { body: "return input.secret;" } }]);
    expect(discoverInputKeys(d, NODE_TYPES)).toEqual([]);
  });

  it("数组与嵌套对象里的字符串也会扫到", () => {
    const d = def([
      {
        id: "a",
        type: "http",
        params: { headers: { Authorization: "Bearer ${input.token}", list: ["${input.ns}"] } },
      },
    ]);
    expect(discoverInputKeys(d, NODE_TYPES)).toEqual(["ns", "token"]);
  });

  it("多节点去重并排序", () => {
    const d = def([
      { id: "a", type: "http", params: { url: "${input.z}" } },
      { id: "b", type: "http", params: { url: "${input.a}/${input.z}" } },
    ]);
    expect(discoverInputKeys(d, NODE_TYPES)).toEqual(["a", "z"]);
  });

  it("input 节点声明的字段也是候选（提前填好运行期不挂起）", () => {
    const d = def([
      {
        id: "i",
        type: "input",
        params: { fields: [{ key: "token" }, { key: "n", default: 1 }] },
      },
      { id: "a", type: "http", params: { url: "${input.token}/x" } },
    ]);
    expect(discoverInputKeys(d, NODE_TYPES)).toEqual(["n", "token"]);
  });
});

describe("assembleInput", () => {
  it("JSON 可解析的值保留类型", () => {
    expect(assembleInput({ n: "5", b: "true", o: '{"a":1}', s: "hello" })).toEqual({
      n: 5,
      b: true,
      o: { a: 1 },
      s: "hello",
    });
  });

  it("空字段省略，全空返回 undefined", () => {
    expect(assembleInput({ a: "", b: "  " })).toBeUndefined();
    expect(assembleInput({ a: "", b: "x" })).toEqual({ b: "x" });
  });
});

describe("parseInputFields", () => {
  it("解析合法项，忽略非法项", () => {
    const params = {
      fields: [
        { key: "token", label: "令牌" },
        { key: "n", default: 1 },
        { key: "  " },
        { label: "无 key" },
        "junk",
      ],
    };
    expect(parseInputFields(params)).toEqual([
      { key: "token", label: "令牌" },
      { key: "n", default: 1 },
    ]);
  });

  it("快照缺失或 fields 非数组时为空", () => {
    expect(parseInputFields(undefined)).toEqual([]);
    expect(parseInputFields({})).toEqual([]);
    expect(parseInputFields({ fields: "${input.fields}" })).toEqual([]);
  });
});

describe("inputFieldText", () => {
  const field = { key: "token", label: "令牌", default: "dft" };

  it("run 输入已有的值优先，字符串原样、对象 JSON 化", () => {
    expect(inputFieldText(field, { token: "abc" })).toBe("abc");
    expect(inputFieldText({ key: "o" }, { o: { x: 1 } })).toBe('{"x":1}');
  });

  it("null 视为未提供，回落默认值，再回落空串", () => {
    expect(inputFieldText(field, { token: null })).toBe("dft");
    expect(inputFieldText(field, {})).toBe("dft");
    expect(inputFieldText({ key: "x" }, { other: 1 })).toBe("");
    expect(inputFieldText(field, undefined)).toBe("dft");
    expect(inputFieldText(field, ["not-object"])).toBe("dft");
  });
});

// Runtime prompts use the contract declared in the definition.
describe("运行输入 schema", () => {
  it("声明字段即使没有模板引用也被发现", () => {
    expect(discoverInputKeys({nodes:[],edges:[],input_schema:{type:"object",required:["id"],properties:{count:{type:"integer"}}}}, [])).toEqual(["count","id"]);
  });
});
