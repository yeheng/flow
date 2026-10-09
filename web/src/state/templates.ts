/**
 * 可复用节点模板的面板状态：template.list 的信封缓存 + 保存/删除动作。
 * 载荷（nodes/edges）不进内存缓存——插入时按需 template.get，避免列表
 * 打开就拉全部片段。
 */
import { reactive } from "vue";
import * as api from "../api/flow";
import type { FragmentNode, NodeTemplate, NodeTemplateSummary } from "../types";
import { errText } from "../rpc/client";
import { editor, insertFragment } from "./editor";
import { toast } from "./toast";

interface TemplatesState {
  list: NodeTemplateSummary[];
  loaded: boolean;
  loading: boolean;
}

export const templates = reactive<TemplatesState>({
  list: [],
  loaded: false,
  loading: false,
});

/** 拉取模板清单（幂等；force 供保存/删除后刷新） */
export async function ensureTemplates(force = false): Promise<boolean> {
  if (templates.loaded && !force) return true;
  if (templates.loading) return false;
  templates.loading = true;
  try {
    templates.list = await api.listTemplates();
    templates.loaded = true;
    return true;
  } catch (e) {
    toast.error(errText(e));
    return false;
  } finally {
    templates.loading = false;
  }
}

/** 把当前画布选中片段存为模板；成功后刷新清单并返回新模板 */
export async function saveSelectionAsTemplate(name: string): Promise<NodeTemplate | null> {
  try {
    const selected = editor.nodes.filter((n) => n.selected);
    if (selected.length === 0) {
      toast.info("先选中要保存的节点");
      return null;
    }
    const ids = new Set(selected.map((n) => n.id));
    const nodes = selected.map((n) => ({
      id: n.id,
      type: n.data.nodeType.type,
      name: n.data.name,
      params: JSON.parse(JSON.stringify(n.data.params ?? {})),
      position: { x: n.position.x, y: n.position.y },
    }));
    const edges = editor.edges
      .filter((e) => ids.has(e.source) && ids.has(e.target))
      .map((e) => ({
        source: e.source,
        target: e.target,
        sourceHandle: e.sourceHandle,
        targetHandle: e.targetHandle,
      }));
    const template = await api.createTemplate(name.trim(), nodes, edges);
    await ensureTemplates(true);
    return template;
  } catch (e) {
    toast.error(errText(e));
    return null;
  }
}

/** 一键存为常用节点：把当前工作流打包成 sub_workflow 单节点模板（预填 workflow_id）。
 * 注意节点运行时走发布版——工作流没有已发布版本时插入后能保存但运行会失败。 */
export async function saveWorkflowAsNodeTemplate(
  workflowId: string,
  name: string,
): Promise<NodeTemplate | null> {
  try {
    const trimmed = name.trim();
    const nodes: FragmentNode[] = [
      { id: "sub_1", type: "sub_workflow", name: trimmed, params: { workflow_id: workflowId } },
    ];
    const template = await api.createTemplate(trimmed, nodes, [], "子流程");
    await ensureTemplates(true);
    return template;
  } catch (e) {
    toast.error(errText(e));
    return null;
  }
}

/** 取模板载荷并插入画布（落点 position）；成功返回 true */
export async function insertTemplate(
  templateId: string,
  position?: { x: number; y: number },
): Promise<boolean> {
  try {
    const template = await api.getTemplate(templateId);
    return insertFragment(
      template.nodes.map((n) => ({
        id: n.id,
        type: n.type,
        name: n.name,
        params: n.params ?? {},
        position: n.position ?? { x: 0, y: 0 },
      })),
      template.edges,
      { offset: position },
    );
  } catch (e) {
    toast.error(errText(e));
    return false;
  }
}

/** 删除模板；刷新清单。返回是否真的删了（env 来源不适用此处，恒为 stored）。 */
export async function removeTemplate(templateId: string): Promise<boolean> {
  try {
    await api.deleteTemplate(templateId);
    await ensureTemplates(true);
    return true;
  } catch (e) {
    toast.error(errText(e));
    return false;
  }
}

/** 模板重命名 */
export async function renameTemplate(templateId: string, name: string): Promise<boolean> {
  try {
    await api.updateTemplate(templateId, { name: name.trim() });
    await ensureTemplates(true);
    return true;
  } catch (e) {
    toast.error(errText(e));
    return false;
  }
}
