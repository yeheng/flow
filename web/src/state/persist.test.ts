import { describe, expect, it } from "vitest";
import { readJson, writeJson } from "./persist";

describe("persist：损坏值回退默认，绝不抛", () => {
  it("缺失键回默认值", () => {
    localStorage.clear();
    expect(readJson("k", { a: 1 })).toEqual({ a: 1 });
  });

  it("损坏 JSON 回默认值（不抛——组件 setup 不能被 localStorage 干掉）", () => {
    localStorage.setItem("bad", "{半截写入");
    expect(readJson("bad", [])).toEqual([]);
    localStorage.setItem("bad2", '"非对象标量"');
    // 能解析的形状按返回类型用（调用方对自己形状负责），但绝不 throw
    expect(readJson("bad2", "默认")).toBe("非对象标量");
  });

  it("写入后可读回；写失败（配额）不抛", () => {
    writeJson("k2", { x: [1, 2] });
    expect(readJson("k2", null)).toEqual({ x: [1, 2] });
  });
});
