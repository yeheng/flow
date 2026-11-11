/**
 * 审批卡的纯渲染逻辑：spec 单行摘要、actor 标签、request_preview 截断判定、
 * 剩余时间格式化。ApprovalCard 逐字段渲染用同一份取值口径（URL/命令等都是
 * 不可信文本，这里只做字符串拼接，不产生 HTML）。
 */
import type { ApprovalActor, ApprovalSpec } from "../types";

/** 安全取字符串字段：非字符串（缺失/其他类型）返回 undefined */
export function specString(spec: ApprovalSpec, key: string): string | undefined {
  const v = spec[key];
  return typeof v === "string" ? v : undefined;
}

/** 安全取字符串数组字段 */
export function specStringList(spec: ApprovalSpec, key: string): string[] {
  const v = spec[key];
  return Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];
}

/** actor 展示标签：label → node_id → kind（一般 {kind:"node",run_id,node_id}） */
export function actorLabel(actor: ApprovalActor): string {
  const label = actor.label;
  if (typeof label === "string" && label) return label;
  if (typeof actor.node_id === "string" && actor.node_id) return actor.node_id;
  return actor.kind;
}

/**
 * spec 的单行摘要（列表/收件箱用；卡片内逐字段渲染更细）：
 * net → "GET https://…"；exec → "exe a b"；shell → "sh · sha256:前16…（脚本摘要）"；
 * secret → 密钥名（绝不显示值）；llm_call → provider/model；send_email → 收件人；file_* → 路径。
 */
export function approvalSpecSummary(spec: ApprovalSpec): string {
  switch (spec.action) {
    case "net": {
      const method = specString(spec, "method") ?? "?";
      const url = specString(spec, "url") ?? "";
      return `${method} ${url}`.trim();
    }
    case "exec": {
      const exe = specString(spec, "executable") ?? "";
      const argv = specStringList(spec, "argv");
      return [exe, ...argv].filter(Boolean).join(" ");
    }
    case "shell": {
      const exe = specString(spec, "executable") ?? "shell";
      const digest = specString(spec, "script_digest");
      const brief = digest ? `sha256:${digest.slice(0, 16)}…` : "sha256:?";
      return `${exe} · 脚本摘要 ${brief}`;
    }
    case "secret":
      return `密钥 ${specString(spec, "secret_name") ?? "?"}`;
    case "llm_call": {
      const provider = specString(spec, "provider") ?? "?";
      const model = specString(spec, "model") ?? "?";
      return `${provider} / ${model}`;
    }
    case "send_email":
      return `收件人 ${specStringList(spec, "recipients").join(", ")}`;
    case "file_read":
    case "file_write":
    case "file_delete": {
      const path = specString(spec, "path");
      if (path) return path;
      return specString(spec, "subtree") ? `子树 ${specString(spec, "subtree")}` : "?";
    }
    default:
      return JSON.stringify(spec);
  }
}

/** 一条审批的操作摘要：全部 spec 的一行拼接（多条用「；」分隔，超长由 UI 截断） */
export function approvalSummary(specs: ApprovalSpec[]): string {
  return specs.map(approvalSpecSummary).join("；");
}

/** request_preview 是否为截断标记（{truncated:true,bytes}：过大只给字节数） */
export function isTruncatedPreview(preview: unknown): preview is { truncated: true; bytes?: number } {
  return (
    typeof preview === "object" &&
    preview !== null &&
    (preview as { truncated?: unknown }).truncated === true
  );
}

/** deadline 剩余时间（本地时钟估算，服务端为准）：已过期返回 null */
export function remainingText(now: number, deadline: number): string | null {
  const ms = deadline - now;
  if (ms <= 0) return null;
  const totalSec = Math.floor(ms / 1000);
  if (totalSec < 60) return `${totalSec}s`;
  const minutes = Math.floor(totalSec / 60);
  const seconds = totalSec % 60;
  if (minutes < 60) return `${minutes}m ${seconds}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}
