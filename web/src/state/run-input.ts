import type { Definition, NodeTypeDesc, PropertySchema } from "../types";

/**
 * 工作流运行输入发现：扫描定义里的 `${input.xxx}` 模板引用与脚本
 * （x-opaque 字段，不参与模板展开）里的 `input.xxx` 全局引用，汇总出
 * run input 需要的顶层字段，供触发时的输入表单用。只取第一段——
 * `${input.user.name}` 归为字段 `user`，嵌套结构以 JSON 值填入。
 */

const TPL_RE = /\$\{\s*input\.([A-Za-z_$][\w$]*)/g;
const CODE_RE = /\binput\.([A-Za-z_$][\w$]*)/g;

function collectString(s: string, re: RegExp, out: Set<string>): void {
  for (const m of s.matchAll(re)) out.add(m[1]);
}

function collectParams(value: unknown, schema: PropertySchema | undefined, out: Set<string>): void {
  if (typeof value === "string") {
    collectString(value, schema?.["x-opaque"] ? CODE_RE : TPL_RE, out);
    return;
  }
  if (Array.isArray(value)) {
    for (const item of value) collectParams(item, undefined, out);
    return;
  }
  if (value !== null && typeof value === "object") {
    for (const [k, v] of Object.entries(value)) {
      collectParams(v, schema?.properties?.[k], out);
    }
  }
}

/** 节点类型未知（schema 缺失）时按模板引用扫描，不会碰脚本代码 */
export function discoverInputKeys(def: Definition, nodeTypes: NodeTypeDesc[]): string[] {
  const out = new Set<string>();
  for (const node of def.nodes) {
    const schema = nodeTypes.find((t) => t.type === node.type)?.params_schema;
    collectParams(node.params, schema, out);
  }
  return [...out].sort();
}

/** 表单值 → run input：空字段省略；能 JSON.parse 的用解析值（数字/布尔/对象），否则按字符串 */
export function assembleInput(values: Record<string, string>): unknown {
  const out: Record<string, unknown> = {};
  for (const [k, raw] of Object.entries(values)) {
    const t = raw.trim();
    if (!t) continue;
    try {
      out[k] = JSON.parse(t);
    } catch {
      out[k] = t;
    }
  }
  return Object.keys(out).length > 0 ? out : undefined;
}
