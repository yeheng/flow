/**
 * 工作流导入（与 flow-cli workflow import 同一份语义，客户端编排而非新 RPC）：
 * - 文件格式：`{name?, description?, definition}` 信封（examples/*.workflow.json）
 *   或裸 definition（{nodes, edges}）；
 * - name 解析：用户输入 > 信封 name > 文件名去扩展名；
 * - upsert：workflow.list 按 name 反查——存在则追加新版本（run 钉死旧版本不受
 *   影响），不存在则新建；默认发布（只有 published 可执行）。
 * 服务端 workflow.update 仍是最终裁决（整图 validate）。
 */
import * as api from "../api/flow";
import type { Definition } from "../types";

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
  const definition = (doc.definition ?? doc) as Record<string, unknown>;
  if (!Array.isArray(definition.nodes)) {
    throw new Error('定义缺少 nodes 数组：{ "nodes": [...], "edges": [...] }');
  }
  if (!Array.isArray(definition.edges)) {
    throw new Error('定义缺少 edges 数组：{ "nodes": [...], "edges": [...] }');
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

/** 按 name upsert + 可选发布。调用方负责 toast 与列表刷新。 */
export async function importWorkflow(
  name: string,
  definition: Definition,
  opts: { publish?: boolean } = {},
): Promise<ImportResult> {
  const listed = await api.listWorkflows();
  const existing = listed.find((w) => w.name === name);
  let workflowId: string;
  let created = false;
  if (existing) {
    workflowId = existing.workflow_id;
  } else {
    workflowId = await api.createWorkflow(name);
    created = true;
  }
  const version = await api.updateWorkflow(workflowId, definition);
  let status = "draft";
  if (opts.publish ?? true) {
    await api.publishWorkflow(workflowId, version);
    status = "published";
  }
  return { workflow_id: workflowId, name, version, created, status };
}
