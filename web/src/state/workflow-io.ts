/**
 * 工作流导入（与 flow-cli workflow import 共用原子提交接口）：
 * - 文件格式：`{name?, description?, definition}` 信封（examples/*.workflow.json）
 *   或裸 definition（{nodes, edges}）；
 * - name 解析：用户输入 > 信封 name > 文件名去扩展名；
 * - upsert：服务端按 name 反查——存在则追加新版本（run 钉死旧版本不受
 *   影响），不存在则新建；默认发布（只有 published 可执行）。
 * 服务端 workflow.validate 预检，workflow.import 原子提交。
 */
import * as api from "../api/flow";
import type { Definition, NodeTypeDesc } from "../types";
import { deepCloneJson } from "./clone";
import { formDialog } from "./modal";
import { schemaObject, type FormField } from "./form-fields";

export interface ParsedImport {
  name: string;
  definition: Definition;
}

/** 文件名兜底名：去路径 + 依次剥 `.json`、`.workflow` 后缀
 * （`a/b.github-trending.workflow.json` → `github-trending`，与 CLI file_stem 同规则） */
export function nameFromFile(filename: string): string {
  const base = filename.split(/[\\/]/).pop() ?? filename;
  let stem = base.replace(/\.json$/i, "");
  stem = stem.replace(/\.workflow$/i, "");
  return stem || base;
}

/** 拆导入文件并做最小形状检查（nodes/edges 数组）；名字解析见模块注释 */
export function parseImportDocument(
  text: string,
  filename: string,
  nameOverride?: string,
): ParsedImport {
  let document: unknown;
  try {
    document = JSON.parse(text);
  } catch (e) {
    throw new Error(`不是合法 JSON：${e instanceof Error ? e.message : String(e)}`);
  }
  if (typeof document !== "object" || document === null || Array.isArray(document)) {
    throw new Error('定义必须是 JSON 对象：{ "nodes": [...], "edges": [...] }');
  }
  const doc = document as Record<string, unknown>;
  const rawDefinition = Object.prototype.hasOwnProperty.call(doc, "definition")
    ? doc.definition
    : doc;
  if (typeof rawDefinition !== "object" || rawDefinition === null || Array.isArray(rawDefinition)) {
    throw new Error("definition 必须是 JSON 对象");
  }
  const definition = rawDefinition as Record<string, unknown>;
  if (!Array.isArray(definition.nodes)) {
    throw new Error('定义缺少 nodes 数组：{ "nodes": [...], "edges": [...] }');
  }
  if (!Array.isArray(definition.edges)) {
    throw new Error('定义缺少 edges 数组：{ "nodes": [...], "edges": [...] }');
  }
  for (const node of definition.nodes) {
    if (
      !node ||
      typeof node !== "object" ||
      Array.isArray(node) ||
      typeof node.id !== "string" ||
      typeof node.type !== "string"
    ) {
      throw new Error("每个节点必须包含字符串 id 和 type");
    }
    if (
      node.params !== undefined &&
      (node.params === null || typeof node.params !== "object" || Array.isArray(node.params))
    ) {
      throw new Error(`节点 ${node.id} 的 params 必须是对象`);
    }
  }
  const name =
    nameOverride?.trim() ||
    (typeof doc.name === "string" && doc.name.trim()) ||
    nameFromFile(filename);
  if (!name) throw new Error("无法确定工作流名：文件没有 name 字段且文件名为空");
  return { name, definition: definition as unknown as Definition };
}

export interface ImportResult {
  workflow_id: string;
  name: string;
  version: number;
  /** true = 新建；false = 同名追加版本 */
  created: boolean;
  /** published | draft */
  status: string;
}

/** Required node configuration is completed before authoritative graph validation. */
export function missingImportFields(
  definition: Definition,
  nodeTypes: NodeTypeDesc[],
): FormField[] {
  const fields: FormField[] = [];
  definition.nodes.forEach((node, index) => {
    const schema = nodeTypes.find((t) => t.type === node.type)?.params_schema;
    const params = node.params ?? {};
    const required = new Set(schema?.required ?? []);
    if (node.type === "harness") required.add(params.mode === "agent" ? "api_key" : "command");
    for (const key of required) {
      const value = params[key];
      if (value !== undefined && value !== null && !(typeof value === "string" && !value.trim()))
        continue;
      const property = schemaObject(schema?.properties?.[key]);
      fields.push({
        key: JSON.stringify([index, key]),
        label: `${node.name || node.id} · ${property["x-label"] ?? key}`,
        type: typeof property.type === "string" ? property.type : "json",
        required: true,
        enum: Array.isArray(property.enum) ? property.enum : undefined,
        defaultValue: property.default,
        help: typeof property["x-help"] === "string" ? property["x-help"] : undefined,
      });
    }
  });
  return fields;
}

/** Returns null when the user cancels. No workflow exists until the atomic import commits. */
export async function importWorkflow(
  name: string,
  definition: Definition,
  opts: { publish?: boolean } = {},
): Promise<ImportResult | null> {
  const prepared = deepCloneJson(definition);
  const fields = missingImportFields(prepared, await api.listNodeTypes());
  if (fields.length) {
    const values = await formDialog("补全工作流配置", fields, { confirmText: "继续校验" });
    if (!values) return null;
    for (const [path, value] of Object.entries(values)) {
      const [index, key] = JSON.parse(path) as [number, string];
      prepared.nodes[index].params ??= {};
      Object.defineProperty(prepared.nodes[index].params, key, {
        value,
        enumerable: true,
        configurable: true,
        writable: true,
      });
    }
  }
  let validation = await api.validateWorkflow(prepared);
  if (validation.missing_secrets.length) {
    const names = [...new Set(validation.missing_secrets.map((s) => s.name))];
    const values = await formDialog(
      "配置工作流需要的密钥",
      names.map((key) => ({
        key,
        label: key,
        type: "string",
        required: true,
        secret: true,
        help: `用于节点 ${validation.missing_secrets
          .filter((s) => s.name === key)
          .map((s) => s.node_id)
          .join("、")}；保存到密钥管理`,
      })),
      { confirmText: "保存并校验" },
    );
    if (!values) return null;
    for (const key of names) await api.setSecret(key, String(values[key]));
    validation = await api.validateWorkflow(prepared);
  }
  if (!validation.valid) throw new Error("工作流校验未通过，请检查配置后重试");
  return api.importWorkflowDefinition(name, prepared, opts.publish ?? true);
}
