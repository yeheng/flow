import type { Definition, NodeTypeDesc, PropertySchema } from "../types";
import { schemaFields, schemaObject, validateInputSchema, type FormField } from "./form-fields";
import { formDialog } from "./modal";

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
  const out = new Set<string>(schemaFields(def.input_schema).map((f) => f.key));
  for (const node of def.nodes) {
    // input 节点声明的字段同样是 run input 的候选：启动时提前填好，运行期就不挂起
    if (node.type === "input") {
      for (const f of parseInputFields(node.params)) out.add(f.key);
    }
    const schema = nodeTypes.find((t) => t.type === node.type)?.params_schema;
    collectParams(node.params, schema, out);
  }
  return [...out].sort();
}

/** Fields come from the published contract, with legacy references as a fallback. */
export async function requestRunInput(
  def: Definition,
  nodeTypes: NodeTypeDesc[],
  input: unknown,
  signal?: AbortSignal,
): Promise<{ input: unknown } | null> {
  if (signal?.aborted) return null;
  const schema = schemaObject(def.input_schema);
  const isObject = input !== null && typeof input === "object" && !Array.isArray(input);
  const base = isObject ? (input as Record<string, unknown>) : {};
  const fields = schemaFields(def.input_schema);
  const declared = new Set(fields.map((f) => f.key));
  if (schema.type === undefined || schema.type === "object") {
    for (const node of def.nodes.filter((n) => n.type === "input")) {
      for (const f of parseInputFields(node.params)) {
        if (schema.additionalProperties !== false && !fields.some((field) => field.key === f.key))
          fields.push({
            key: f.key,
            label: f.label ?? f.key,
            required: true,
            defaultValue: f.default,
          });
      }
    }
    if (schema.additionalProperties !== false) {
      for (const key of discoverInputKeys(def, nodeTypes)) {
        if (!fields.some((f) => f.key === key)) fields.push({ key, label: key });
      }
    }
  }
  const candidate =
    input === undefined ? (schema.type === "object" || fields.length ? {} : null) : input;
  const error = validateInputSchema(def.input_schema, candidate);
  const missing = fields.some(
    (f) =>
      (declared.has(f.key) ? f.required : f.defaultValue == null) &&
      !Object.prototype.hasOwnProperty.call(base, f.key),
  );
  if (!error && !missing)
    return { input: input === undefined && schema.type === "object" ? {} : input };
  if (
    (schema.type === undefined || schema.type === "object") &&
    fields.length &&
    (schema.additionalProperties !== false ||
      Object.keys(base).every((key) => declared.has(key))) &&
    (input === undefined || isObject)
  ) {
    const merge = (values: Record<string, unknown>) => {
      const next = { ...base };
      for (const field of fields) delete next[field.key];
      return { ...next, ...values };
    };
    const values = await formDialog("填写工作流运行输入", fields, {
      values: base,
      confirmText: "运行",
      signal,
      validate: (values) => validateInputSchema(def.input_schema, merge(values)),
    });
    return values ? { input: merge(values) } : null;
  }
  const rootField: FormField = {
    key: "$",
    label: "运行输入 JSON",
    type: "json",
    required: true,
    defaultValue: input ?? schema.default,
    help: typeof schema.description === "string" ? schema.description : error,
  };
  const values = await formDialog("填写工作流运行输入", [rootField], {
    confirmText: "运行",
    signal,
    validate: (values) => validateInputSchema(def.input_schema, values.$),
  });
  return values ? { input: values.$ } : null;
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

/** input 节点声明的字段（params.fields 的合法项；形态在建图期已由后端校验） */
export interface InputField {
  key: string;
  label?: string;
  default?: unknown;
}

/** 从节点参数/输入面快照解析 fields，非法项忽略（快照可能缺失或是 ${} 模板原文） */
export function parseInputFields(params: unknown): InputField[] {
  const fields = (params as { fields?: unknown } | null | undefined)?.fields;
  if (!Array.isArray(fields)) return [];
  const out: InputField[] = [];
  for (const f of fields) {
    if (f === null || typeof f !== "object") continue;
    const { key, label } = f as { key?: unknown; label?: unknown };
    if (typeof key !== "string" || !key.trim()) continue;
    const dft = (f as { default?: unknown }).default;
    out.push({
      key,
      ...(typeof label === "string" && label ? { label } : {}),
      ...(dft !== undefined ? { default: dft } : {}),
    });
  }
  return out;
}

/** 字段的表单初值文本：run 输入已有的值（null 视为未提供）→ 默认值 → 空 */
export function inputFieldText(field: InputField, runInput: unknown): string {
  const src =
    runInput !== null && typeof runInput === "object" && !Array.isArray(runInput)
      ? (runInput as Record<string, unknown>)[field.key]
      : undefined;
  const v = src ?? field.default;
  if (v === undefined || v === null) return "";
  return typeof v === "string" ? v : JSON.stringify(v);
}
