/**
 * 回归运行的前端纯逻辑：基线关系解析、run.compare 差异的可读化、
 * fixture 覆盖的 wire 归一化。
 * 服务端来源标记见 crates/flow-backend/src/regression.rs（source_detail = regression:<baseline>），
 * 差异形状见 compare_runs 的返回（含子 run 递归与 request 指纹）。
 */
import { runStatusLabel } from "./labels";
import type { RunComparison, RunComparisonDifference } from "../types";

export const REGRESSION_PREFIX = "regression:";

/** 从 run.source_detail 解析回归基线 run id；非回归运行返回 null */
export function regressionBaseline(sourceDetail: string | null | undefined): string | null {
  if (!sourceDetail || !sourceDetail.startsWith(REGRESSION_PREFIX)) return null;
  const baseline = sourceDetail.slice(REGRESSION_PREFIX.length).trim();
  return baseline.length > 0 ? baseline : null;
}

/**
 * run.compare 直接返回 journal 的 StoredValue wire（`{type:"inline",value}` /
 * `{type:"ref",value:{output_id,total_bytes,…}}`），而 run.get.view 返回业务值。
 * 展示前只解包「恰好 type+value 两个键」的信封，避免误伤业务对象；ref 是不可
 * 内联的大值，给出可行动占位而不是内部指针。
 */
export function unwrapComparisonValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(unwrapComparisonValue);
  if (value !== null && typeof value === "object") {
    const keys = Object.keys(value);
    const wire = value as { type?: unknown; value?: unknown };
    if (keys.length === 2 && "value" in wire) {
      if (wire.type === "inline") return unwrapComparisonValue(wire.value);
      if (wire.type === "ref" && wire.value !== null && typeof wire.value === "object") {
        const ref = wire.value as { output_id?: unknown; total_bytes?: unknown };
        const id = typeof ref.output_id === "string" ? ref.output_id : "未知";
        const size =
          typeof ref.total_bytes === "string" || typeof ref.total_bytes === "number"
            ? `，${ref.total_bytes} 字节`
            : "";
        return `（大值引用 ${id}${size}；在 run 详情中查看或下载）`;
      }
    }
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([key, item]) => [
        key,
        unwrapComparisonValue(item),
      ]),
    );
  }
  return value;
}

/** 差异条目的展示值：字符串原样（状态名不套引号），对象/数组格式化 JSON */
export function formatComparisonValue(value: unknown): string {
  const unwrapped = unwrapComparisonValue(value);
  if (unwrapped === undefined) return "—";
  if (unwrapped === null) return "null";
  if (typeof unwrapped === "string") return unwrapped;
  try {
    return JSON.stringify(unwrapped, null, 2);
  } catch {
    return String(unwrapped);
  }
}

/**
 * 节点状态原词（journal NodeStatus：pending/running/waiting/succeeded/skipped/failed）
 * → 中文。timeline 的展示词（completed/retrying）不适用于 run.compare 的差异。
 */
export function comparisonNodeStatusLabel(status: string | null | undefined): string {
  switch (status) {
    case undefined:
    case null:
      return "—";
    case "pending":
      return "等待";
    case "running":
      return "运行中";
    case "waiting":
      return "等待外部操作";
    case "succeeded":
      return "成功";
    case "skipped":
      return "跳过";
    case "failed":
      return "失败";
    default:
      return status;
  }
}

/**
 * 差异路径 → 人话。路径是点分段：
 * status / error / output /
 * nodes.<id> / nodes.<id>.request /
 * children.<key>（子 run 只存在于一侧）/ children.<key>.…（子 run 内部递归）。
 */
export function comparisonPathLabel(path: string): string {
  const parts = path.split(".");
  const labels: string[] = [];
  for (let i = 0; i < parts.length; i++) {
    const seg = parts[i];
    if (seg === "children") {
      const key = parts[++i] ?? "?";
      labels.push(`子 run(${key})`);
    } else if (seg === "nodes") {
      const key = parts[++i] ?? "?";
      if (parts[i + 1] === "request") {
        i++;
        labels.push(`节点 ${key} 请求指纹`);
      } else {
        labels.push(`节点 ${key}`);
      }
    } else if (seg === "status") {
      labels.push("运行状态");
    } else if (seg === "error") {
      labels.push("错误");
    } else if (seg === "output") {
      labels.push("最终输出");
    } else {
      labels.push(seg);
    }
  }
  return labels.join(" · ");
}

export interface ComparisonRow {
  path: string;
  label: string;
  expected: string;
  actual: string;
}

function isNodeDifference(d: RunComparisonDifference): boolean {
  return (
    d.expected_status !== undefined ||
    d.actual_status !== undefined ||
    d.expected_output !== undefined ||
    d.actual_output !== undefined
  );
}

/**
 * 把 compare_runs 的差异条目归一成「期望/实际」两列：
 * - 节点差异（含子 run 内部）合并 status 与解包后的 output；
 * - status 路径按 run 状态词汇翻译（含 children.*.status）；
 * - 其余（error/output/request/children.<key>）按值格式化。
 */
export function comparisonRows(differences: RunComparisonDifference[]): ComparisonRow[] {
  return differences.map((d) => {
    const label = comparisonPathLabel(d.path);
    if (isNodeDifference(d)) {
      return {
        path: d.path,
        label,
        expected: formatComparisonValue({
          status: comparisonNodeStatusLabel(d.expected_status),
          output: d.expected_output,
        }),
        actual: formatComparisonValue({
          status: comparisonNodeStatusLabel(d.actual_status),
          output: d.actual_output,
        }),
      };
    }
    if (d.path === "status" || d.path.endsWith(".status")) {
      return {
        path: d.path,
        label,
        expected:
          typeof d.expected === "string"
            ? runStatusLabel(d.expected)
            : formatComparisonValue(d.expected),
        actual:
          typeof d.actual === "string" ? runStatusLabel(d.actual) : formatComparisonValue(d.actual),
      };
    }
    return {
      path: d.path,
      label,
      expected: formatComparisonValue(d.expected),
      actual: formatComparisonValue(d.actual),
    };
  });
}

export interface ComparisonSummary {
  label: string;
  tone: "ok" | "danger" | "warn";
}

/** 对比结论：两侧未终结时不做通过/失败断言（complete=false） */
export function comparisonSummary(report: RunComparison): ComparisonSummary {
  if (!report.complete) return { label: "两侧运行尚未结束，暂不能判定", tone: "warn" };
  if (report.passed) return { label: "与基线一致", tone: "ok" };
  return { label: `${report.differences.length} 处与基线不同`, tone: "danger" };
}

/** fixtures 覆盖的用户输入形态（outcome 写业务值即可，API 层包 inline 信封） */
export interface FixtureOverrideInput {
  fingerprint: string;
  outcome: unknown;
}

export interface NormalizeFixturesResult {
  fixtures?: Record<string, FixtureOverrideInput>;
  error: string;
}

/**
 * 解析「覆盖外部操作结果」JSON：
 * `{ "<节点路径 JSON 数组>": { "fingerprint": "<64 hex>", "outcome": <业务值或信封> } }`。
 * 键必须是紧凑 JSON 字符串数组（服务端 node_key 形态，如 "[\"n1\"]"），
 * fingerprint 必须 64 hex；空文本=不覆盖。解析失败给可展示错误，不抛异常。
 */
export function normalizeFixtures(text: string): NormalizeFixturesResult {
  const raw = text.trim();
  if (!raw) return { error: "" };
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (e) {
    return { error: `不是合法 JSON：${String(e)}` };
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    return { error: "覆盖表必须是对象" };
  }
  const fixtures: Record<string, FixtureOverrideInput> = {};
  for (const [key, entry] of Object.entries(parsed as Record<string, unknown>)) {
    let path: unknown;
    try {
      path = JSON.parse(key);
    } catch {
      return { error: `键 ${key} 不是 JSON 数组字符串（形如 "[\\"n1\\"]"）` };
    }
    if (!Array.isArray(path) || path.length === 0 || path.some((x) => typeof x !== "string")) {
      return { error: `键 ${key} 必须是非空字符串数组的 JSON 文本` };
    }
    if (JSON.stringify(path) !== key) {
      return { error: `键 ${key} 不是紧凑 JSON（不能有多余空白）` };
    }
    const obj = (entry ?? {}) as { fingerprint?: unknown; outcome?: unknown };
    const fingerprint = obj.fingerprint;
    if (typeof fingerprint !== "string" || !/^[0-9a-f]{64}$/i.test(fingerprint)) {
      return { error: `键 ${key} 的 fingerprint 必须是 64 位十六进制` };
    }
    if (!("outcome" in obj)) {
      return { error: `键 ${key} 缺 outcome` };
    }
    fixtures[key] = { fingerprint: fingerprint.toLowerCase(), outcome: obj.outcome };
  }
  return { fixtures, error: "" };
}
