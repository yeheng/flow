import { E2E_TOKEN } from "./config";
import { WRITE_METHODS } from "../src/rpc/methods";
// e2e 种子与断言辅助：JSON-RPC over WebSocket（Node 22 内置全局 WebSocket，无需 ws 依赖）。
// 方法签名对齐 web/src/api/flow.ts。

const RPC_URL = "ws://127.0.0.1:19311";

export async function rpc<T = unknown>(
  method: string,
  params: Record<string, unknown> = {},
): Promise<T> {
  params = { ...params, _token: E2E_TOKEN };
  if (WRITE_METHODS.has(method) && params.request_id === undefined)
    params.request_id = crypto.randomUUID();
  const ws = new WebSocket(RPC_URL);
  try {
    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => resolve();
      ws.onerror = () => reject(new Error("无法连接 e2e flow-journal-server"));
    });
    return await new Promise<T>((resolve, reject) => {
      ws.onmessage = (e) => {
        const msg = JSON.parse(String(e.data)) as {
          result?: unknown;
          error?: { code: number; message: string };
        };
        if (msg.error) reject(new Error(`${msg.error.code}: ${msg.error.message}`));
        else {
          const reply = msg.result as { committed?: boolean; result?: unknown };
          resolve((reply?.committed === true ? reply.result : reply) as T);
        }
      };
      ws.send(JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }));
    });
  } finally {
    ws.close();
  }
}

export interface SeedWorkflow {
  id: string;
  version: number;
}

/** 最小合法图：start → script → end。code 是 JS 函数体（throw 可造失败 run）。 */
export async function seedWorkflow(name: string, code: string): Promise<SeedWorkflow> {
  const { workflow_id } = await rpc<{ workflow_id: string }>("workflow.create", { name });
  const { version } = await rpc<{ version: number }>("workflow.update", {
    workflow_id,
    definition: {
      nodes: [
        { id: "s", type: "start" },
        { id: "n", type: "script", params: { code } },
        { id: "e", type: "end" },
      ],
      edges: [
        { from: "s", to: "n" },
        { from: "n", to: "e" },
      ],
    },
  });
  await rpc("workflow.publish", { workflow_id, version });
  return { id: workflow_id, version };
}

export async function startRun(workflowId: string): Promise<string> {
  const r = await rpc<{ run_id: string }>("run.start", { workflow_id: workflowId });
  return r.run_id;
}

export async function runStatus(runId: string): Promise<string> {
  const r = await rpc<{ run: { status: string } }>("run.get.view", { run_id: runId });
  return r.run.status;
}

/** 等 run 到终态（succeeded/failed/cancelled），返回终态 */
export async function waitTerminal(runId: string, timeoutMs = 15_000): Promise<string> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const status = await runStatus(runId);
    if (["succeeded", "failed", "cancelled"].includes(status)) return status;
    if (Date.now() > deadline)
      throw new Error(`run ${runId} 未在 ${timeoutMs}ms 内终结（${status}）`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

export interface RunStats {
  total: number;
  by_status: Record<string, number>;
  by_workflow: { workflow_id: string; total: number; by_status: Record<string, number> }[];
}

export async function runStats(workflowId?: string): Promise<RunStats> {
  const params: Record<string, unknown> = {};
  if (workflowId !== undefined) params.workflow_id = workflowId;
  return rpc<RunStats>("run.stats", params);
}

/** 名称加随机后缀，保证用例间种子互不依赖、可独立重跑 */
export function uniq(prefix: string): string {
  return `${prefix}-${Math.random().toString(36).slice(2, 8)}`;
}
