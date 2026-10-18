/**
 * webhook 调用合同的前端逻辑：版本固定 + 调用密钥（服务端只存 SHA-256 哈希）
 * + 调用/查询示例。路由见 crates/flow-rpc/src/journal_triggers.rs：
 * POST /invoke/{token}（Bearer 调用密钥 + Idempotency-Key）→ 202 受理回执；
 * GET /invocations/{run_id}（Bearer run_token）→ 结果/订阅/交付/取消。
 */

/** 生成 32 字节随机调用密钥（hex）；明文只在此刻展示一次 */
export function generateInvokeKey(): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/** 调用密钥的服务端指纹（SHA-256 hex，与 codec::digest 同一口径） */
export async function sha256Hex(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

export function invokeUrl(base: string, token: string): string {
  return `${base.replace(/\/$/, "")}/invoke/${token}`;
}

export function invocationResultUrl(base: string, runId: string): string {
  return `${base.replace(/\/$/, "")}/invocations/${runId}`;
}

/** 业务调用示例：固定 Idempotency-Key（重试安全）与可选业务关联 ID */
export function invokeCurl(base: string, token: string): string {
  return [
    `curl -X POST "${invokeUrl(base, token)}" \\`,
    `  -H "Authorization: Bearer <调用密钥>" \\`,
    `  -H "Idempotency-Key: <唯一键，重试复用>" \\`,
    `  -H "X-Correlation-Id: <业务关联ID>" \\`,
    `  -H "Content-Type: application/json" \\`,
    `  -d '{"key": "value"}'`,
    `# → 202 {run_id, run_token, result_url, events_url, receipt}`,
  ].join("\n");
}

/** 结果查询示例：run_token 只绑定该 run，管理 token 不外发 */
export function resultCurl(base: string, runId = "<run_id>"): string {
  return [
    `curl "${invocationResultUrl(base, runId)}" \\`,
    `  -H "Authorization: Bearer <run_token>"`,
    `# 413 时返回 output_ref + download_url；事件订阅见 /events，人工输入见 /signals/<node>`,
  ].join("\n");
}
