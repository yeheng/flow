/**
 * 权限模型的前端纯逻辑：能力条目的构造/校验 + 定义 JSON 与编辑器形态的互转。
 * 词汇（中文标签）在 labels.ts 与此处共用；服务端 validate 仍是最终裁决。
 */
import type { Capability, CapabilityAction, Definition } from "../types";

/** 动作闭集（与 CapabilityAction 对齐）：编辑器下拉选项，顺序即展示顺序 */
export const CAPABILITY_ACTIONS: readonly CapabilityAction[] = [
  "exec",
  "shell",
  "net",
  "secret",
  "llm_call",
  "send_email",
  "file_read",
  "file_write",
  "file_delete",
];

/** net 能力的 methods 闭集（编辑器多选） */
export const NET_METHODS: readonly string[] = [
  "GET",
  "POST",
  "PUT",
  "PATCH",
  "DELETE",
  "HEAD",
  "OPTIONS",
];

/** 副作用节点类型（legacy 判定用）：与 nodetypes descriptor 的 side_effect 标注对齐的已知集 */
export const SIDE_EFFECT_NODE_TYPES: ReadonlySet<string> = new Set([
  "http_call",
  "email",
  "llm",
  "harness",
]);

function isCapabilityAction(action: string): action is CapabilityAction {
  return (CAPABILITY_ACTIONS as readonly string[]).includes(action);
}

/** 按 action 给出最小合法形态的闭集字段（新条目/切换 action 时的底稿） */
function actionDefaults(action: string): Record<string, unknown> {
  switch (action) {
    case "net":
      return { methods: ["GET"], url_prefix: "https://" };
    case "secret":
      return { names: [] };
    case "exec":
      return { executable: "", argv_exact: [] };
    case "shell":
      return { script_digest: "" };
    case "llm_call":
      return { providers: [], models: [] };
    case "send_email":
      return { recipients: [] };
    case "file_read":
    case "file_write":
    case "file_delete":
      return { path: "" };
    default:
      return {};
  }
}

/** id 自动生成：cap-<action>-<n>，n 取该层未占用的最小序号 */
export function newCapabilityId(action: string, existing: Iterable<string>): string {
  const taken = new Set(existing);
  let n = 1;
  while (taken.has(`cap-${action}-${n}`)) n++;
  return `cap-${action}-${n}`;
}

/** 新建能力条目（默认 ask 需审批；闭集字段按 action 落最小形态） */
export function newCapability(action: string, existing: Iterable<string> = []): Capability {
  return {
    id: newCapabilityId(action, existing),
    action,
    approval: "ask",
    ...actionDefaults(action),
  };
}

/** 切换 action：保留公共字段（id/approval/expires_at），闭集字段换新底稿 */
export function switchCapabilityAction(cap: Capability, action: string): Capability {
  return {
    id: cap.id,
    action,
    approval: cap.approval,
    ...(cap.expires_at !== undefined ? { expires_at: cap.expires_at } : {}),
    ...actionDefaults(action),
  };
}

// ---- expires_at：datetime-local（编辑器）↔ ISO（定义 JSON） ----

/** datetime-local 输入值（YYYY-MM-DDTHH:mm）→ ISO 字符串；空串 → null */
export function expiresAtFromInput(text: string): string | null {
  if (!text) return null;
  const d = new Date(text);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

/** ISO 字符串 → datetime-local 输入值；空/非法 → "" */
export function expiresAtToInput(iso: string | null | undefined): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  // toLocaleString 拿不到「本地无秒」格式：手动拼 YYYY-MM-DDTHH:mm
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

// ---- 保存前本地校验（提示用；服务端权威） ----

const TEMPLATE_MARK = "${";

/** 单条能力的校验错误（前缀「第 N 条」由调用方加）；空数组 = 通过 */
export function validateCapability(cap: Capability): string[] {
  const errors: string[] = [];
  if (!cap.id?.trim()) errors.push("能力 id 不能为空");
  if (!isCapabilityAction(String(cap.action))) errors.push(`未知动作 ${String(cap.action)}`);
  // `${` 模板禁止：顶层字符串字段与字符串数组都不允许携带
  for (const [key, value] of Object.entries(cap)) {
    if (value === null || value === undefined) continue;
    if (typeof value === "string") {
      if (value.includes(TEMPLATE_MARK)) errors.push(`字段 ${key} 禁止 ${TEMPLATE_MARK} 模板`);
    } else if (Array.isArray(value)) {
      for (const item of value) {
        if (typeof item === "string" && item.includes(TEMPLATE_MARK)) {
          errors.push(`字段 ${key} 禁止 ${TEMPLATE_MARK} 模板`);
          break;
        }
      }
    }
  }
  const str = (key: string): string => (typeof cap[key] === "string" ? (cap[key] as string) : "");
  const listFilled = (key: string): boolean =>
    Array.isArray(cap[key]) && (cap[key] as unknown[]).length > 0;
  switch (cap.action) {
    case "exec": {
      const exe = str("executable");
      if (!exe) errors.push("executable 不能为空");
      else if (!exe.startsWith("/")) errors.push("executable 必须是绝对路径");
      break;
    }
    case "shell": {
      if (!/^[0-9a-f]{64}$/.test(str("script_digest"))) {
        errors.push("script_digest 必须是 64 位小写十六进制");
      }
      break;
    }
    case "net": {
      const prefix = str("url_prefix");
      if (!prefix) {
        errors.push("url_prefix 不能为空");
        break;
      }
      try {
        const u = new URL(prefix);
        if (u.username || u.password) errors.push("URL 前缀禁止携带 userinfo");
        if (u.search) errors.push("URL 前缀禁止携带 query");
      } catch {
        errors.push("URL 前缀不是合法的绝对 URL");
      }
      if (!listFilled("methods")) errors.push("methods 至少选择一个");
      break;
    }
    case "secret": {
      if (!listFilled("names")) errors.push("密钥名至少一个");
      break;
    }
    case "llm_call": {
      if (!listFilled("providers") && !listFilled("models")) errors.push("providers/models 至少填一项");
      break;
    }
    case "send_email": {
      if (!listFilled("recipients")) errors.push("收件人至少一个");
      break;
    }
    case "file_read":
    case "file_write":
    case "file_delete": {
      // path 与 subtree 二选一
      const hasPath = str("path") !== "";
      const hasSubtree = str("subtree") !== "";
      if (hasPath === hasSubtree) errors.push("path 与 subtree 必须二选一");
      break;
    }
  }
  return errors;
}

/** 能力列表校验：单条规则 + id 每层唯一（同一 allow 列表内） */
export function validateCapabilities(caps: Capability[]): string[] {
  const errors: string[] = [];
  const seen = new Set<string>();
  caps.forEach((cap, i) => {
    const at = `第 ${i + 1} 条`;
    if (cap.id) {
      if (seen.has(cap.id)) errors.push(`${at}：能力 id 重复（${cap.id}）`);
      seen.add(cap.id);
    }
    for (const msg of validateCapability(cap)) errors.push(`${at}：${msg}`);
  });
  return errors;
}

// ---- legacy 判定：definition 无顶层 permissions 且含副作用节点 ----

/** 定义里是否含副作用节点（类型闭集 + 调用方补充的 side_effect 标注） */
export function definitionHasSideEffectNodes(
  def: Definition | null | undefined,
  extraSideEffectTypes: Iterable<string> = [],
): boolean {
  if (!def) return false;
  const types = new Set([...SIDE_EFFECT_NODE_TYPES, ...extraSideEffectTypes]);
  return def.nodes.some((n) => types.has(n.type));
}
