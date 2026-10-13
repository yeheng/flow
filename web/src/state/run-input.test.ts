import { describe, expect, it } from "vitest";
import { assembleInput, discoverInputKeys } from "./run-input";
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
    const d = def([
      { id: "a", type: "http", params: { url: "https://input.example.com/help" } },
    ]);
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
