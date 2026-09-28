import { client } from "../rpc/client";
import { httpUrl } from "../config";
import type {
  Definition,
  NodeTypeDesc,
  RunEvent,
  RunRecord,
  RunStats,
  Schedule,
  Timeline,
  VersionMeta,
  Webhook,
  WorkflowDetail,
  WorkflowSummary,
} from "../types";

export async function listNodeTypes(): Promise<NodeTypeDesc[]> {
  const r = await client.call<{ node_types: NodeTypeDesc[] }>("nodetypes.list", {});
  return r.node_types;
}

export async function listWorkflows(): Promise<WorkflowSummary[]> {
  const r = await client.call<{ workflows: WorkflowSummary[] }>("workflow.list", {});
  return r.workflows;
}

export async function createWorkflow(name: string): Promise<string> {
  const r = await client.call<{ workflow_id: string }>("workflow.create", { name });
  return r.workflow_id;
}

export async function updateWorkflow(workflowId: string, definition: Definition): Promise<number> {
  const r = await client.call<{ workflow_id: string; version: number }>("workflow.update", {
    workflow_id: workflowId,
    definition: definition as unknown as Record<string, unknown>,
  });
  return r.version;
}

export async function publishWorkflow(workflowId: string, version: number): Promise<void> {
  await client.call("workflow.publish", { workflow_id: workflowId, version });
}

export async function getWorkflow(workflowId: string, version?: number): Promise<WorkflowDetail> {
  const params: Record<string, unknown> = { workflow_id: workflowId };
  if (version !== undefined) params.version = version;
  return client.call<WorkflowDetail>("workflow.get", params);
}

export async function listVersions(workflowId: string): Promise<VersionMeta[]> {
  const r = await client.call<{ versions: VersionMeta[] }>("workflow.versions", {
    workflow_id: workflowId,
  });
  return r.versions;
}

export async function deleteWorkflow(workflowId: string): Promise<void> {
  await client.call("workflow.delete", { workflow_id: workflowId });
}

export async function startRun(
  workflowId: string,
  input?: unknown,
): Promise<{ run_id: string; workflow_version: number }> {
  const params: Record<string, unknown> = { workflow_id: workflowId };
  if (input !== undefined) params.input = input;
  return client.call("run.start", params);
}

export interface ListRunsOptions {
  workflowId?: string;
  status?: string;
  /** 触发来源过滤：manual / schedule / webhook / sub_workflow */
  source?: string;
  /** 游标分页：返回该 run 之前更旧的记录 */
  beforeRunId?: string;
  limit?: number;
}

export async function listRuns(opts: ListRunsOptions = {}): Promise<RunRecord[]> {
  const params: Record<string, unknown> = {};
  if (opts.workflowId !== undefined) params.workflow_id = opts.workflowId;
  if (opts.status !== undefined) params.status = opts.status;
  if (opts.source !== undefined) params.source = opts.source;
  if (opts.beforeRunId !== undefined) params.before_run_id = opts.beforeRunId;
  if (opts.limit !== undefined) params.limit = opts.limit;
  const r = await client.call<{ runs: RunRecord[] }>("run.list", params);
  return r.runs;
}

/** 精确统计（GROUP BY）：不带 workflowId 时附带 by_workflow 分组 */
export async function runStats(workflowId?: string): Promise<RunStats> {
  const params: Record<string, unknown> = {};
  if (workflowId !== undefined) params.workflow_id = workflowId;
  return client.call<RunStats>("run.stats", params);
}

export async function getRun(runId: string): Promise<{ run: RunRecord; live: boolean }> {
  return client.call("run.get", { run_id: runId });
}

export async function runTimeline(runId: string): Promise<Timeline> {
  return client.call<Timeline>("run.timeline", { run_id: runId });
}

export async function runEvents(runId: string, fromSeq?: number): Promise<RunEvent[]> {
  const params: Record<string, unknown> = { run_id: runId };
  if (fromSeq !== undefined) params.from_seq = fromSeq;
  const r = await client.call<{ events: RunEvent[] }>("run.events", params);
  return r.events;
}

export async function runCancel(runId: string): Promise<void> {
  await client.call("run.cancel", { run_id: runId });
}

export async function runSignal(runId: string, nodeId: string, payload: unknown): Promise<void> {
  // Postgres 后端的信号经持久 inbox 落账，signal_id 必填（1-128 字符）；
  // UI 无自动重试，一次点击 = 一个逻辑交付，本地生成唯一 id 即可
  await client.call("run.signal", {
    run_id: runId,
    signal_id: crypto.randomUUID(),
    node_id: nodeId,
    payload,
  });
}

export function subscribeRun(
  runId: string,
  onEvent: (e: RunEvent) => void,
): Promise<() => Promise<void>> {
  return client.subscribe("run.subscribe", { run_id: runId }, (e) => onEvent(e as RunEvent));
}

// ---- 触发器：cron 调度与 webhook（schedule.* / webhook.*） ----

export async function createSchedule(
  workflowId: string,
  cron: string,
  input?: unknown,
): Promise<Schedule> {
  const params: Record<string, unknown> = { workflow_id: workflowId, cron };
  if (input !== undefined) params.input = input;
  return client.call<Schedule>("schedule.create", params);
}

export async function listSchedules(workflowId?: string): Promise<Schedule[]> {
  const params: Record<string, unknown> = {};
  if (workflowId !== undefined) params.workflow_id = workflowId;
  const r = await client.call<{ schedules: Schedule[] }>("schedule.list", params);
  return r.schedules;
}

export interface UpdateScheduleParams {
  cron?: string;
  /** 双 Option 语义：undefined=不改；null=清空；其余值=替换 */
  input?: unknown;
  enabled?: boolean;
}

export async function updateSchedule(id: string, p: UpdateScheduleParams): Promise<void> {
  const params: Record<string, unknown> = { id };
  if (p.cron !== undefined) params.cron = p.cron;
  if (p.input !== undefined) params.input = p.input;
  if (p.enabled !== undefined) params.enabled = p.enabled;
  await client.call("schedule.update", params);
}

export async function deleteSchedule(id: string): Promise<void> {
  await client.call("schedule.delete", { id });
}

export async function createWebhook(workflowId: string): Promise<Webhook> {
  return client.call<Webhook>("webhook.create", { workflow_id: workflowId });
}

export async function listWebhooks(workflowId?: string): Promise<Webhook[]> {
  const params: Record<string, unknown> = {};
  if (workflowId !== undefined) params.workflow_id = workflowId;
  const r = await client.call<{ webhooks: Webhook[] }>("webhook.list", params);
  return r.webhooks;
}

export async function setWebhookEnabled(token: string, enabled: boolean): Promise<void> {
  await client.call("webhook.set_enabled", { token, enabled });
}

export async function deleteWebhook(token: string): Promise<void> {
  await client.call("webhook.delete", { token });
}

/** webhook HTTP 入口基址（POST /hook/<token>）：运行时 config.json 优先，其次 VITE_FLOW_HTTP，最后本地默认 */
export function webhookBase(): string {
  return httpUrl();
}
