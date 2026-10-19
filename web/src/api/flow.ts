import { client } from "../rpc/client";
export { JournalClient } from "./journal";
export { downloadValue, runValueUrl } from "./download";
import { httpUrl } from "../config";
import { rawRunToRecord, selectRawRuns } from "../state/run-raw";
import type {
  AdjudicateDecision,
  ConfigView,
  Definition,
  FlowConfig,
  FragmentEdge,
  FragmentNode,
  NodeTypeDesc,
  NodeTemplate,
  NodeTemplateSummary,
  RunEvent,
  ObservationRecord,
  ObservationLoss,
  RawRunSnapshot,
  RunComparison,
  RunRecord,
  RunStats,
  Schedule,
  SecretInfo,
  Timeline,
  VersionMeta,
  Webhook,
  WebhookInvokeConfig,
  WorkflowDetail,
  WorkflowSummary,
} from "../types";

export async function listNodeTypes(): Promise<NodeTypeDesc[]> {
  const r = await client.call<{ node_types: NodeTypeDesc[] }>("nodetypes.list", {});
  return r.node_types;
}

/** secrets.list 返回 {name, source} 清单（值不出服务端；source=stored|env） */
export async function listSecrets(): Promise<SecretInfo[]> {
  const r = await client.call<{ secrets: SecretInfo[] }>("secrets.list", {});
  return r.secrets;
}

/** 写入（或覆盖）一个持久化密钥。真值只进服务端内存与加密落盘。 */
export async function setSecret(name: string, value: string): Promise<void> {
  await client.call("secrets.set", { name, value });
}

/** 删除一个持久化密钥。返回 false 表示该名字来自环境变量（删不了）。 */
export async function deleteSecret(name: string): Promise<boolean> {
  const r = await client.call<{ deleted: boolean }>("secrets.delete", { name });
  return r.deleted;
}

// ---- 统一配置（重启生效；文件为可编辑真相，env 覆盖单列展示） ----

export async function getConfig(): Promise<ConfigView> {
  return client.call<ConfigView>("config.get", {});
}

/** 部分更新：patch 分区可选，给了就整节替换。返回写回后的同一视图。 */
export async function updateConfig(patch: Partial<FlowConfig>): Promise<ConfigView> {
  return client.call<ConfigView>("config.update", { patch });
}

// ---- 可复用节点模板 ----

export async function listTemplates(): Promise<NodeTemplateSummary[]> {
  const r = await client.call<{ templates: NodeTemplateSummary[] }>("template.list", {});
  return r.templates;
}

export async function getTemplate(id: string): Promise<NodeTemplate> {
  return client.call<NodeTemplate>("template.get", { id });
}

export async function createTemplate(
  name: string,
  nodes: FragmentNode[],
  edges: FragmentEdge[],
  category?: string,
): Promise<NodeTemplate> {
  const params: Record<string, unknown> = { name, nodes, edges };
  if (category) params.category = category;
  const created = await client.call<{id:string}>("template.create", params);
  return getTemplate(created.id);
}

/** 双 Option 语义：category === undefined 不改；null 清空分组 */
export async function updateTemplate(
  id: string,
  p: { name?: string; category?: string | null },
): Promise<void> {
  const params: Record<string, unknown> = { id };
  if (p.name !== undefined) params.name = p.name;
  if (p.category !== undefined) params.category = p.category;
  await client.call("template.update", params);
}

export async function deleteTemplate(id: string): Promise<void> {
  await client.call("template.delete", { id });
}

export async function listWorkflows(): Promise<WorkflowSummary[]> {
  const r = await client.call<{ workflows: WorkflowSummary[] }>("workflow.list.view", {});
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
  return client.call<WorkflowDetail>("workflow.get.view", params);
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
  const r = await client.call<{ runs: RunRecord[] }>("run.list.view", params);
  return r.runs;
}

/** 精确统计（GROUP BY）：不带 workflowId 时附带 by_workflow 分组 */
export async function runStats(workflowId?: string): Promise<RunStats> {
  const params: Record<string, unknown> = {};
  if (workflowId !== undefined) params.workflow_id = workflowId;
  return client.call<RunStats>("run.stats", params);
}

export async function getRun(runId: string): Promise<{ run: RunRecord; live: boolean }> {
  return client.call("run.get.view", { run_id: runId });
}

/** 原始 run 快照（StoredValue 信封，不物化）：物化视图超 8MB 预算时的降级读面 */
export async function getRunRaw(runId: string): Promise<RawRunSnapshot> {
  const r = await client.call<{ value: RawRunSnapshot | null }>("run.get", { run_id: runId });
  if (!r.value) throw new Error("run 不存在");
  return r.value;
}

/**
 * 降级运行列表：raw run.list 只有键升序，无过滤/降序，扫描最近 RAW_SCAN_PAGES
 * 页取最新 limit 条匹配行。仅在物化列表超预算时使用；scanned=false 表示被
 * 扫描上限截断（更旧数据未知）。
 */
export async function listRunsLite(
  opts: ListRunsOptions = {},
): Promise<{ runs: RunRecord[]; hasMore: boolean; scanned: boolean }> {
  const RAW_PAGE = 256;
  const RAW_SCAN_PAGES = 32;
  const limit = opts.limit ?? 50;
  const pages: RawRunSnapshot[][] = [];
  let after = "";
  let complete = true;
  for (let i = 0; i < RAW_SCAN_PAGES; i++) {
    const page = await client.call<{ values: RawRunSnapshot[] }>("run.list", {
      after,
      limit: RAW_PAGE,
    });
    const values = page.values ?? [];
    pages.push(values);
    if (values.length === 0) break;
    after = values[values.length - 1].run_id;
    if (opts.beforeRunId !== undefined && after >= opts.beforeRunId) break;
    if (i === RAW_SCAN_PAGES - 1) complete = false;
  }
  const selected = selectRawRuns(
    pages,
    {
      workflowId: opts.workflowId,
      status: opts.status,
      source: opts.source,
      beforeRunId: opts.beforeRunId,
    },
    limit,
    complete,
  );
  return { runs: selected.rows.map(rawRunToRecord), hasMore: selected.hasMore, scanned: complete };
}

export async function runTimeline(runId: string): Promise<Timeline> {
  return client.call<Timeline>("run.timeline", { run_id: runId });
}

/** 原始事件分页：一页 256 条，按游标继续取（长 run 不做全量拉取） */
export async function runEventsPage(
  runId: string,
  cursor?: Record<string, unknown> | null,
): Promise<{ events: RunEvent[]; nextCursor: Record<string, unknown> | null }> {
  const page = await client.call<{
    events: RunEvent[];
    next_cursor: Record<string, unknown> | null;
  }>("run.events.page", { run_id: runId, cursor, limit: 256 });
  return { events: page.events, nextCursor: page.next_cursor };
}

export function runObservations(
  runId: string,
  fromSeq = "0",
): Promise<{ records: ObservationRecord[]; loss: ObservationLoss }> {
  return client.call("run.observations.page", { run_id: runId, from_seq: fromSeq, limit: 256 });
}

/**
 * 以 baseline run 为基线创建候选运行（run.regress）。
 * 候选复用基线记录的时钟/种子、人工输入与外部操作结果，不重新发起 HTTP/harness
 * 副作用；请求摘要不一致的节点会显式失败。baseline 必须已终结且带执行上下文。
 * fixtures 可显式覆盖外部操作结果（键是节点路径 JSON 数组，如 "[\"n1\"]"）。
 */
export async function startRegression(
  baselineRunId: string,
  opts: {
    workflowId?: string;
    version?: number;
    fixtures?: Record<string, { fingerprint: string; outcome: unknown }>;
  } = {},
): Promise<{ run_id: string; workflow_version: number }> {
  const params: Record<string, unknown> = { baseline: baselineRunId };
  if (opts.workflowId !== undefined) params.workflow_id = opts.workflowId;
  if (opts.version !== undefined) params.version = opts.version;
  if (opts.fixtures !== undefined) {
    // outcome 允许写业务值：wire 上包一层 inline 信封（已是信封则透传）
    params.fixtures = Object.fromEntries(
      Object.entries(opts.fixtures).map(([key, fixture]) => [
        key,
        {
          fingerprint: fixture.fingerprint,
          outcome: isStoredEnvelope(fixture.outcome)
            ? fixture.outcome
            : { type: "inline", value: fixture.outcome },
        },
      ]),
    );
  }
  return client.call("run.regress", params);
}

function isStoredEnvelope(value: unknown): boolean {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    Object.keys(value).length === 2 &&
    "value" in (value as Record<string, unknown>) &&
    typeof (value as { type?: unknown }).type === "string"
  );
}

/** 比较两个 run 的最终状态、输出与逐节点结果（run.compare） */
export function compareRuns(baselineRunId: string, candidateRunId: string): Promise<RunComparison> {
  return client.call("run.compare", { baseline: baselineRunId, candidate: candidateRunId });
}

export async function runCancel(runId: string): Promise<void> {
  await client.call("run.cancel", { run_id: runId });
}

export async function runSignal(runId: string, nodeId: string, payload: unknown): Promise<void> {
  await client.call("run.signal", {
    run_id: runId,
    node_id: nodeId,
    payload,
  });
}

/**
 * 人工裁决 uncertain 外部操作（run.adjudicate）。系统绝不自动重发，
 * 人工决定就是授权凭据：reason 必填（≤4096 字节）；retry 可能产生重复副作用。
 */
export async function adjudicateRun(
  runId: string,
  nodeId: string,
  operationId: string,
  decision: AdjudicateDecision,
  reason: string,
  output?: unknown,
): Promise<void> {
  await client.call("run.adjudicate", {
    run_id: runId,
    node_id: nodeId,
    operation_id: operationId,
    decision,
    reason,
    output: output ?? null,
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
  const created = await client.call<{id:string}>("schedule.create", params);
  const schedule = (await listSchedules(workflowId)).find(s => s.id === created.id);
  if (!schedule) throw new Error("已提交的调度未出现在读面中");
  return schedule;
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

/**
 * 设置 webhook 调用合同（webhook.change patch）：invoke_version 固定调用版本、
 * invoke_key_hash 调用密钥的 SHA-256 hex（明文只在生成时展示一次）。
 * 返回服务端合并后的完整配置（webhook.list 不回显这两个字段）。
 */
export async function updateWebhookInvoke(
  token: string,
  config: WebhookInvokeConfig,
): Promise<Record<string, unknown>> {
  const receipt = await client.call<Record<string, unknown>>("webhook.change", {
    key: token,
    patch: config,
  });
  return receipt;
}

/** webhook HTTP 入口基址：v2 形态 POST /hooks/<key>（Bearer token 认证）。
 * 运行时 config.json 优先，其次 VITE_FLOW_HTTP，最后本地默认。 */
export function webhookBase(): string {
  return httpUrl();
}
/** webhook 调用 URL（v2：Bearer 认证 + Idempotency-Key） */
export function webhookUrl(key: string): string {
  return `${httpUrl().replace(/\/$/, "")}/hooks/${key}`;
}
