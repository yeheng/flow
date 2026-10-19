import { describe, expect, it } from "vitest";
import {
  comparisonNodeStatusLabel,
  comparisonPathLabel,
  comparisonRows,
  comparisonSummary,
  formatComparisonValue,
  normalizeFixtures,
  regressionBaseline,
  unwrapComparisonValue,
} from "./regression";
import type { RunComparison } from "../types";

function report(overrides: Partial<RunComparison> = {}): RunComparison {
  return {
    baseline: "base",
    candidate: "cand",
    complete: true,
    passed: true,
    differences: [],
    ...overrides,
  };
}

describe("regressionBaseline", () => {
  it("解析 regression:<baseline> 来源标记", () => {
    expect(regressionBaseline("regression:run-1")).toBe("run-1");
  });

  it("非回归来源与空值返回 null", () => {
    expect(regressionBaseline(null)).toBeNull();
    expect(regressionBaseline(undefined)).toBeNull();
    expect(regressionBaseline("")).toBeNull();
    expect(regressionBaseline("schedule-1")).toBeNull();
    expect(regressionBaseline("regression:")).toBeNull();
    expect(regressionBaseline("regression:   ")).toBeNull();
  });
});

describe("unwrapComparisonValue", () => {
  it("解包 inline 信封，嵌套信封递归解包", () => {
    expect(unwrapComparisonValue({ type: "inline", value: { ok: true } })).toEqual({ ok: true });
    expect(unwrapComparisonValue({ type: "inline", value: { type: "inline", value: 3 } })).toBe(3);
  });

  it("ref 大值给出可行动占位而不是内部指针", () => {
    const text = unwrapComparisonValue({
      type: "ref",
      value: { output_id: "out-1", journal_id: "j", total_bytes: "2048" },
    });
    expect(text).toBe("（大值引用 out-1，2048 字节；在 run 详情中查看或下载）");
  });

  it("非信封对象不被误判为信封（递归保留业务字段）", () => {
    expect(unwrapComparisonValue({ type: "inline", value: 1, extra: true })).toEqual({
      type: "inline",
      value: 1,
      extra: true,
    });
  });
});

describe("formatComparisonValue", () => {
  it("字符串原样、undefined 用占位、对象格式化 JSON", () => {
    expect(formatComparisonValue("failed")).toBe("failed");
    expect(formatComparisonValue(undefined)).toBe("—");
    expect(formatComparisonValue(null)).toBe("null");
    expect(formatComparisonValue({ a: 1 })).toBe('{\n  "a": 1\n}');
  });

  it("先解包 StoredValue 再展示", () => {
    expect(formatComparisonValue({ type: "inline", value: { a: 1 } })).toBe('{\n  "a": 1\n}');
    expect(formatComparisonValue({ type: "inline", value: "文本" })).toBe("文本");
  });
});

describe("comparisonNodeStatusLabel", () => {
  it("journal NodeStatus 原词翻译，未知原样，缺失占位", () => {
    expect(comparisonNodeStatusLabel("succeeded")).toBe("成功");
    expect(comparisonNodeStatusLabel("waiting")).toBe("等待外部操作");
    expect(comparisonNodeStatusLabel(null)).toBe("—");
    expect(comparisonNodeStatusLabel("custom")).toBe("custom");
  });
});

describe("comparisonPathLabel", () => {
  it("已知路径给中文标签，未知路径原样", () => {
    expect(comparisonPathLabel("status")).toBe("运行状态");
    expect(comparisonPathLabel("output")).toBe("最终输出");
    expect(comparisonPathLabel("nodes.n1")).toBe("节点 n1");
    expect(comparisonPathLabel("other")).toBe("other");
  });
});

describe("comparisonRows", () => {
  it("输出/状态差异取 expected/actual，状态翻译成中文", () => {
    const rows = comparisonRows([
      { path: "status", expected: "failed", actual: "succeeded" },
      { path: "output", expected: 1, actual: 2 },
    ]);
    expect(rows[0]).toEqual({
      path: "status",
      label: "运行状态",
      expected: "失败",
      actual: "成功",
    });
    expect(rows[1].expected).toBe("1");
    expect(rows[1].actual).toBe("2");
  });

  it("节点差异合并 status 与解包后的 output", () => {
    const rows = comparisonRows([
      {
        path: "nodes.n1",
        expected_status: "succeeded",
        actual_status: "failed",
        expected_output: { type: "inline", value: { ok: true } },
        actual_output: null,
      },
    ]);
    expect(rows[0].label).toBe("节点 n1");
    expect(rows[0].expected).toBe('{\n  "status": "成功",\n  "output": {\n    "ok": true\n  }\n}');
    expect(rows[0].actual).toBe('{\n  "status": "失败",\n  "output": null\n}');
  });
});

describe("comparisonSummary", () => {
  it("未结束不判定", () => {
    expect(comparisonSummary(report({ complete: false, passed: false }))).toEqual({
      label: "两侧运行尚未结束，暂不能判定",
      tone: "warn",
    });
  });

  it("一致与差异", () => {
    expect(comparisonSummary(report()).tone).toBe("ok");
    expect(comparisonSummary(report({ passed: false, differences: [{ path: "status" }] }))).toEqual(
      {
        label: "1 处与基线不同",
        tone: "danger",
      },
    );
  });

  it("truncated：差异被截到 64 条上限，摘要给下界而不是精确计数", () => {
    const differences = Array.from({ length: 64 }, (_, i) => ({ path: `nodes.n${i}` }));
    expect(
      comparisonSummary(report({ passed: false, differences, truncated: true })),
    ).toEqual({ label: "≥64 处与基线不同", tone: "danger" });
  });
});

describe("comparisonPathLabel：子 run / 请求指纹 / 错误", () => {
  it("递归路径逐段翻译", () => {
    expect(comparisonPathLabel("error")).toBe("错误");
    expect(comparisonPathLabel("nodes.n1.request")).toBe("节点 n1 请求指纹");
    expect(comparisonPathLabel("children.c1")).toBe("子 run(c1)");
    expect(comparisonPathLabel("children.c1.nodes.n2.output")).toBe(
      "子 run(c1) · 节点 n2 · 最终输出",
    );
    expect(comparisonPathLabel("children.c1.status")).toBe("子 run(c1) · 运行状态");
  });
});

describe("comparisonRows：子 run 内部节点差异与状态翻译", () => {
  it("children.*.nodes.* 的节点差异按 status+output 合并", () => {
    const rows = comparisonRows([
      {
        path: "children.c1.nodes.n2",
        expected_status: "succeeded",
        actual_status: "waiting",
        expected_output: null,
        actual_output: null,
      },
    ]);
    expect(rows[0].label).toBe("子 run(c1) · 节点 n2");
    expect(rows[0].expected).toContain('"status": "成功"');
    expect(rows[0].actual).toContain('"status": "等待外部操作"');
  });

  it("children.*.status 也翻译 run 状态，request 指纹按值展示", () => {
    const rows = comparisonRows([
      { path: "children.c1.status", expected: "succeeded", actual: "failed" },
      { path: "nodes.n1.request", expected: "abc", actual: null },
    ]);
    expect(rows[0]).toMatchObject({ expected: "成功", actual: "失败" });
    expect(rows[1]).toMatchObject({ expected: "abc", actual: "null" });
  });
});

describe("normalizeFixtures", () => {
  const hex64 = "a".repeat(64);

  it("空文本=不覆盖；合法表解析并小写指纹", () => {
    expect(normalizeFixtures("")).toEqual({ error: "" });
    expect(normalizeFixtures("")).not.toHaveProperty("fixtures");
    const result = normalizeFixtures(
      JSON.stringify({ '["n1"]': { fingerprint: hex64.toUpperCase(), outcome: { ok: 1 } } }),
    );
    expect(result.error).toBe("");
    expect(result.fixtures).toEqual({
      '["n1"]': { fingerprint: hex64, outcome: { ok: 1 } },
    });
  });

  it("非法 JSON / 非对象 / 键不紧凑 / 指纹不合法 / 缺 outcome 都报可展示错误", () => {
    expect(normalizeFixtures("{").error).toContain("不是合法 JSON");
    expect(normalizeFixtures("[]").error).toContain("覆盖表必须是对象");
    expect(
      normalizeFixtures(JSON.stringify({ '[ "n1" ]': { fingerprint: hex64, outcome: 1 } })).error,
    ).toContain("不是紧凑 JSON");
    expect(
      normalizeFixtures(JSON.stringify({ '["n1"]': { fingerprint: "zz", outcome: 1 } })).error,
    ).toContain("64 位十六进制");
    expect(normalizeFixtures(JSON.stringify({ '["n1"]': { fingerprint: hex64 } })).error).toContain(
      "缺 outcome",
    );
    expect(
      normalizeFixtures(JSON.stringify({ n1: { fingerprint: hex64, outcome: 1 } })).error,
    ).toContain("JSON 数组字符串");
  });
});
