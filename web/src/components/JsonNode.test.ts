import { describe, expect, it } from "vitest";
import { mount } from "@vue/test-utils";
import JsonNode from "./JsonNode.vue";

describe("JsonNode", () => {
  it("超大对象默认只渲染上限个子项，点击提示展开剩余", async () => {
    const big: Record<string, number> = {};
    for (let i = 0; i < 5000; i++) big[`k${i}`] = i;
    const w = mount(JsonNode, { props: { value: big } });
    expect(w.findAll(".json-leaf")).toHaveLength(200);
    expect(w.text()).toContain("显示剩余 4800 项");

    await w.find(".json-more").trigger("click");
    expect(w.findAll(".json-leaf")).toHaveLength(5000);
    expect(w.find(".json-more").exists()).toBe(false);
  });

  it("超大数组同样受限", () => {
    const w = mount(JsonNode, { props: { value: new Array(1000).fill(0) } });
    expect(w.findAll(".json-leaf")).toHaveLength(200);
    expect(w.text()).toContain("显示剩余 800 项");
  });

  it("逗号只加在非末项", () => {
    const w = mount(JsonNode, { props: { value: { a: 1, b: 2 } } });
    const leaves = w.findAll(".json-leaf");
    expect(leaves).toHaveLength(2);
    expect(leaves[0].find(".j-punct").exists()).toBe(true);
    expect(leaves[1].find(".j-punct").exists()).toBe(false);
  });

  it("递归子节点的末项同样不带逗号", () => {
    const w = mount(JsonNode, { props: { value: { a: { x: 1, y: 2 }, b: 3 } } });
    // DOM 顺序：a 的 x、y，然后顶层 b
    const leaves = w.findAll(".json-leaf");
    expect(leaves).toHaveLength(3);
    expect(leaves.map((leaf) => leaf.find(".j-punct").exists())).toEqual([true, false, false]);
  });
});
