// 与 crates/flow-engine/src/model.rs、crates/flow-rpc/src/lib.rs 的 JSON 结构逐字段对齐

export interface Position {
  x: number;
  y: number;
}

export interface DefinitionNode {
  id: string;
  type: string;
  name?: string;
  position?: Position;
  params: Record<string, unknown>;
  /** 节点级权限：缺省 = inherit（沿用工作流能力）；restrict = 收紧为闭集 */
  permissions?: NodePermissions;
}

export interface DefinitionEdge {
  from: string;
  to: string;
  /** condition 出边为 "true" | "false"，其余节点出边不得带 */
  port?: string;
}

export interface Definition {
  semantics?: number;
  input_schema?: unknown;
  output_schema?: unknown;
  /** 工作流级权限块：缺省 = legacy（含副作用节点的旧工作流不能启动新 run） */
  permissions?: WorkflowPermissions;
  nodes: DefinitionNode[];
  edges: DefinitionEdge[];
}

export interface PortDesc {
  id: string;
  label: string;
}

/**
 * nodetypes.list 返回的 params_schema：JSON Schema 子集（draft-07 object），
 * 属性可带 x-widget / x-secret / x-opaque / x-label / x-help 扩展键。
 * 未识别的 x-* 键一律忽略，新增扩展键不需要改前端。
 */
export interface PropertySchema {
  type?: "string" | "integer" | "number" | "boolean" | "object" | "array";
  enum?: string[];
  default?: unknown;
  required?: string[];
  properties?: Record<string, PropertySchema>;
  "x-widget"?: "code" | "json" | "workflow-picker" | "key-value";
  /** true 表示该字符串字段存的是密钥名称（值在服务端 FLOW_SECRET_*），前端渲染为密钥名称选择器 */
  "x-secret"?: boolean;
  /**
   * true 表示该字段是 flow 自己的语法（JS 代码），不参与 `${}` 模板展开。
   * 前端无需据此渲染（与 x-widget: code 是两回事：harness.prompt / email.body
   * 也是 code 组件但要展开），仅作语义标注随 schema 一起透传。
   */
  "x-opaque"?: boolean;
  "x-label"?: string;
  "x-help"?: string;
}

export interface ParamsSchema {
  type: "object";
  required?: string[];
  properties?: Record<string, PropertySchema>;
}

/** nodetypes.list 返回的画布能力清单条目 */
export interface NodeTypeDesc {
  type: string;
  label: string;
  category: string;
  max_instances?: number;
  ports: PortDesc[];
  params_schema: ParamsSchema;
  supports_retry?: boolean;
  side_effect?: boolean;
  /** 证据粒度：node / external_call / process / model_call（LLM 适配器用） */
  trace_granularity?: "node" | "external_call" | "process" | "model_call" | string;
  /** 自动重试的作用域：declared_repeatable_only 只对显式声明可重复的节点生效 */
  retry_scope?: "declared_repeatable_only" | string;
}

/** plugins.list 条目：已部署插件的 manifest 投影 */
export interface PluginInfo {
  load_state?: 'loaded' | 'broken' | 'conflict';
  load_error?: string | null;
  directory_name?: string;
  id_matches_directory?: boolean;
  entry_contained?: boolean;
  entry_symlink?: boolean;
  entry_oversize?: boolean;
  running?: boolean;
  service?: boolean;
  id: string;
  name: string;
  version: string;
  node_types: string[];
  invoke: string;
  protocol: number;
  config_fields?: { key: string; type: string; required?: boolean; default?: unknown }[];
  ui?: Record<string, unknown>;
  source: string;
}

/** plugins.recheck 违规项 */
export interface PluginViolation {
  workflow_id: string;
  version: number;
  node_id: string;
  node_type: string;
  reason: string;
}

export interface WorkflowSummary {
  workflow_id: string;
  name: string;
  latest_version: number;
  published_version: number | null;
  created_at: string;
}

export interface WorkflowDetail {
  workflow_id: string;
  version: number;
  status: string;
  definition: Definition;
  published_version: number | null;
}

/** workflow.versions 返回的版本元数据（不含 definition，按需走 workflow.get） */
export interface VersionMeta {
  workflow_id: string;
  version: number;
  status: string;
  checksum: string;
  created_at: string;
}

/** schedule.create / schedule.list 返回的 cron 调度（crates/flow-dto/src/lib.rs Schedule） */
export interface Schedule {
  environment_id?: string | null;
  id: string;
  workflow_id: string;
  cron_expr: string;
  /** Option<Value>：未设置输入时为 null */
  input: unknown;
  enabled: boolean;
  created_at: string;
  /** 服务端算好的下次触发时间（RFC3339，本地时区偏移）；存量数据 cron 损坏时为 null */
  next_fire_at: string | null;
}

/** webhook.create / webhook.list 返回的 webhook 触发器（flow-dto Webhook，token 即 URL 凭证） */
export interface Webhook {
  environment_id?: string | null;
  token: string;
  workflow_id: string;
  enabled: boolean;
  created_at: string;
  /** 调用合同（服务端回显时才有；webhook.list 不回显则刷新后不可见） */
  invoke_version?: number | null;
  invoke_key_hash?: string | null;
}

/** run.list / run.get 返回的运行记录（crates/flow-dto/src/lib.rs RunRecord） */
export interface RunRecord {
  id: string;
  workflow_id: string;
  workflow_version: number;
  status: string;
  input: unknown;
  /** Option<Value>：未结束时为 null */
  output: unknown;
  error: string | null;
  /** 触发来源：manual / schedule / webhook / sub_workflow */
  source: string;
  /** 来源细节：schedule id / webhook token；manual 与 sub_workflow 为 null */
  source_detail: string | null;
  started_at: string;
  ended_at: string | null;
}

/** run.compare 的一处差异（crates/flow-backend/src/regression.rs compare_runs） */
export interface RunComparisonDifference {
  /**
   * 差异路径，句点分段：
   * status / error / output / nodes.<id> / nodes.<id>.request /
   * children.<key>（子 run 只存在于一侧）/ children.<key>.…（递归子 run 内部）
   */
  path: string;
  expected?: unknown;
  actual?: unknown;
  /** 节点差异的两种形态 */
  expected_status?: string | null;
  actual_status?: string | null;
  expected_output?: unknown;
  actual_output?: unknown;
}

/** run.compare 返回：两侧都终结才 complete；差异为空才 passed */
export interface RunComparison {
  baseline: string;
  candidate: string;
  complete: boolean;
  passed: boolean;
  differences: RunComparisonDifference[];
  /** 差异超过 64 条被截断（服务端 record_difference 上限） */
  truncated?: boolean;
}

/** journal 值信封（run.get / run.list 原始快照里的 StoredValue wire） */
export interface StoredValueRef {
  type: "ref";
  value: {
    journal_id: string;
    output_id: string;
    codec: string;
    version: number;
    /** 十进制字符串（服务端 decimal 编码） */
    chunk_count: string;
    total_bytes: string;
    digest: string;
  };
}

export interface StoredValueInline {
  type: "inline";
  value: unknown;
}

export type StoredValue = StoredValueRef | StoredValueInline;

/** run.get / run.list 原始快照条目（journal Run.summary，input/output 是信封） */
export interface RawRunSnapshot {
  run_id: string;
  workflow_id: string;
  workflow_version: number;
  status: string;
  input: StoredValue;
  output: StoredValue | null;
  error: string | null;
  source: string;
  source_detail: string | null;
  created_at: string;
  [key: string]: unknown;
}

/** webhook 调用合同：invoke_version 固定版本 / invoke_key_hash 调用密钥 SHA-256 哈希 */
export interface WebhookInvokeConfig {
  invoke_version: number;
  invoke_key_hash: string;
}

/** run.stats 返回（GROUP BY 精确计数） */
export interface RunStats {
  total: number;
  /** status → count，只含实际出现的状态 */
  by_status: Record<string, number>;
  /** 仅在不带 workflow_id 过滤时返回（否则空数组） */
  by_workflow: WorkflowRunStats[];
}

export interface WorkflowRunStats {
  workflow_id: string;
  total: number;
  by_status: Record<string, number>;
}

/** 节点片段（模板与剪贴板共用形状）：与 DefinitionNode 相似但 id 必填、position 可选 */
export interface FragmentNode {
  id: string;
  type: string;
  name: string;
  position?: Position;
  params: Record<string, unknown>;
}

/** 片段内部边（vue-flow 句柄语义：sourceHandle 默认 "out"，targetHandle 默认 "in"） */
export interface FragmentEdge {
  source: string;
  target: string;
  sourceHandle?: string | null;
  targetHandle?: string | null;
}

/** 可复用节点模板（crates/flow-dto/src/lib.rs NodeTemplate） */
export interface NodeTemplate {
  id: string;
  name: string;
  category: string | null;
  nodes: FragmentNode[];
  edges: FragmentEdge[];
  created_at: string;
  updated_at: string;
}

/** template.list 条目：不含片段载荷（按需走 template.get） */
export interface NodeTemplateSummary {
  id: string;
  name: string;
  category: string | null;
  node_count: number;
  created_at: string;
  updated_at: string;
}

/** 密钥清单条目：source=stored（界面管理，可删）| env（进程环境变量） */
export interface SecretInfo {
  name: string;
  source: "stored" | "env" | "environment";
}

// ---- 统一配置（crates/flow-config/src/lib.rs Config；分区形状对齐） ----

export interface ServerConfig {
  scheduler_enabled: boolean;
  journal_trigger_tick_secs: number;
}

export interface StorageConfig {
  /** journal 是唯一后端；v1 的 sqlite/postgres 已删除 */
  backend: "journal";
  data_dir: string;
}

export interface RemoteExecutionConfig {
  control_addr: string;
  ca_cert: string;
  cert: string;
  key: string;
  attach_timeout_ms: number;
  /** 已废弃（v3 收敛为单条 TLS 连接），仅存量兼容 */
  data_addr?: string | null;
  /** WSS 接入监听地址（代理终止 TLS 场景的附加承载）；缺省关闭 */
  wss_addr?: string | null;
  /** WSS 接入的 agent 凭据映射（token→agent_id），与 wss_addr 成对配置 */
  agents?: AgentCredential[];
}

/** WSS 接入的 agent 凭据（id 与 token 表内唯一） */
export interface AgentCredential {
  id: string;
  token: string;
}

export interface ExecutionConfig {
  /** in_process 已全平台删除（配置校验会拒绝），只有 ipc / remote。 */
  mode: "ipc" | "remote";
  executor_bin: string | null;
  x_max: number;
  /** 启动时预热一个空闲执行器（占一个 x_max 槽位；失败降级懒加载） */
  prewarm?: boolean;
  remote: RemoteExecutionConfig | null;
}

export interface AgentConfig {
  control_addr: string | null;
  agent_id: string | null;
  ca_cert: string | null;
  cert: string | null;
  key: string | null;
  executor_bin: string | null;
  slots: number;
  /** 已废弃（v3 收敛为单条 TLS 连接），仅存量兼容 */
  data_addr?: string | null;
  /** WSS 上联地址；设置后优先于 mTLS 直连 */
  wss_url?: string | null;
  /** WSS 接入令牌（master 侧按 token→agent_id 映射反查身份） */
  token?: string | null;
}

export interface JournalConfig {
  addr: string;
  http_addr: string;
  data_dir: string | null;
  cluster?: {
    cluster_id: string;
    node_id: string;
    database_url_env: string;
    rpc_url: string | null;
    http_url: string | null;
    heartbeat_ms: number;
    lease_ms: number;
  } | null;
}

/** `[harness.agent]`：harness 节点 agent 模式的模型配置（run 启动时冻结） */
export interface HarnessAgentConfig {
  base_url: string | null;
  model: string | null;
  /** openai（默认，OpenAI 兼容）| anthropic */
  api: string | null;
  /** LLM 出口代理；禁止 userinfo/query */
  proxy: string | null;
  max_turns: number | null;
  max_tool_calls_per_turn: number | null;
  max_tokens: number | null;
  tool_timeout_secs: number | null;
}

export interface HarnessConfig {
  agent: HarnessAgentConfig;
}

export interface FlowConfig {
  server: ServerConfig;
  storage: StorageConfig;
  execution: ExecutionConfig;
  agent: AgentConfig;
  journal: JournalConfig;
  harness: HarnessConfig;
}

/** config.get / config.update 的响应 */
export interface ConfigView {
  config: FlowConfig;
  config_path: string | null;
  /** 启动时生效的环境变量覆盖名单（仅名称）；这些项在重启后仍会覆盖文件 */
  env_overrides: string[];
}

export type NodeRunState = "pending" | "running" | "retrying" | "completed" | "failed" | "skipped";

/** run.timeline 节点条目的审批投影：节点等待人工批准时的摘要（approvals 面词汇表）。
 *  终态 run 的 pending 显示为 "cancelled"。 */
export interface NodeApprovalSummary {
  gate: ApprovalGate | string;
  status: ApprovalStatus | string;
  operation_id: string;
  request_hash: string;
  deadline: number;
  matched: string[];
  decision: ApprovalDecision | null;
}

/** run.timeline 节点条目（crates/flow-rpc/src/lib.rs timeline_value） */
export interface TimelineNode {
  wait?: { kind: string; child_run_id?: string | null } | null;
  operation_id?: string | null;
  id: string;
  name: string;
  type: string;
  state: NodeRunState;
  attempts: number;
  started_at: string | null;
  ended_at: string | null;
  duration_ms: number | null;
  /** 节点输入面快照：模板展开后的 params（写入时已脱敏）；旧 run 为空 */
  input?: unknown;
  output: unknown;
  error: string | null;
  reason?: string;
  /** sub_workflow 节点启动的子 run；其余节点为空 */
  child_run_id?: string;
  /** 审批等待投影：wait.kind === "approval" 时非空；旧 run / 无审批为空 */
  approval?: NodeApprovalSummary | null;
}


/** run.adjudicate 的人工决定：接受产出 / 人工授权重发 / 判定失败（服务端词汇表） */
export type AdjudicateDecision = "accept_output" | "retry" | "failed";

/** 节点日志级别/来源（与 crates/flow-engine/src/event.rs 的词汇表对齐） */
export type LogLevel = "debug" | "info" | "warn" | "error";
export type LogStream = "engine" | "stdout" | "stderr";

/**
 * 前端日志行：来自 observation store 的节点日志（日志控制台/节点检查器共用）。
 * 观测日志有损且不是 journal 权威事实，丢失计数见 ObservationLoss。
 */
export interface LogLine {
  seq: string;
  ts: string;
  node_id: string;
  attempt: number;
  level: LogLevel;
  stream: LogStream;
  message: string;
}

export interface Timeline {
  run_id: string;
  status: string;
  workflow_id: string;
  workflow_version: number;
  started_at: string | null;
  ended_at: string | null;
  output: unknown;
  fatal_error: string | null;
  last_seq: number;
  nodes: TimelineNode[];
}

/** Native V2 event positions; control/audit sequences are decimal strings. */
export interface RunEvent {
  lsn: string;
  event_index: number;
  event: { run_id: string | null; run_seq: string; kind: string; node_id?: string; payload: unknown };
}
export interface ObservationRecord {
  seq: string;
  run_id: string;
  dispatch_id: string;
  ts: string;
  line: Omit<LogLine, "seq" | "ts">;
}

/** observation store 的有损标记：日志可能少行，UI 必须明示（run.observations.page 的 loss） */
export interface ObservationLoss {
  queue_dropped?: number;
  retention_dropped?: number;
  storage_dropped?: number;
  history_incomplete?: boolean;
}

// ---- 权限模型（capability / approval；与 crates/flow-permissions 的 JSON 形状逐字段对齐） ----

/** 能力动作闭集（服务端词汇表；未知值原样透传渲染，不判死） */
export type CapabilityAction =
  | "exec"
  | "shell"
  | "net"
  | "secret"
  | "llm_call"
  | "send_email"
  | "file_read"
  | "file_write"
  | "file_delete";

/**
 * 能力条目（定义 JSON 的 CapabilityJSON，序列化进 definition 用）：
 * 公共字段 id/action/approval/expires_at 之外，按 action 携带闭集字段——
 * net: methods/url_prefix；secret: names；exec: executable(绝对路径)/argv_exact；
 * shell: executable(可选)/script_digest(64hex)；file_*: path 或 subtree（二选一，相对路径）；
 * llm_call: providers/models；send_email: recipients。
 * 前端不做精确判型（字段随 action 变化），校验规则见 state/capabilities.ts。
 */
export interface Capability {
  id: string;
  action: CapabilityAction | string;
  /** auto=免审批 ask=需审批（命中后挂起等人工批准/拒绝） */
  approval: "auto" | "ask";
  expires_at?: string | null;
  [key: string]: unknown;
}

/** definition 顶层 permissions：version 固定为 1；mode=plan 只规划不执行副作用 */
export interface WorkflowPermissions {
  version: 1;
  mode: "execute" | "plan";
  allow: Capability[];
}

/** definition 节点 permissions：inherit 沿用工作流能力；restrict 只放行本列表 */
export type NodePermissions = { mode: "inherit" } | { mode: "restrict"; allow: Capability[] };

/** 审批门：node=节点门（进入节点前）/ tool=工具门（节点内单次工具调用） */
export type ApprovalGate = "node" | "tool";

/** 审批状态词汇表（approvals.* 与 run.timeline 节点 approval.status 同一份） */
export type ApprovalStatus =
  | "pending"
  | "allowed"
  | "denied"
  | "expired"
  | "superseded"
  | "withdrawn"
  | "cancelled";

/** 审批请求里的单条需求（ApprovalSpec）：action 之外的字段按 action 解释。
 *  URL/命令/脚本等都是不可信文本，只做纯文本插值渲染，禁止 v-html。 */
export interface ApprovalSpec {
  action: CapabilityAction | string;
  // exec: executable/argv/cwd/env_digest/stdin_digest?；shell: executable/script_digest/cwd/env_digest/stdin_digest?
  // net: method/url/headers_digest/body_digest；secret: secret_name
  // llm_call: provider/model/payload_digest；send_email: recipients/payload_digest
  // file_*: path/content_digest?
  [key: string]: unknown;
}

/** 审批发起方：一般 {kind:"node",run_id,node_id}，label 可选 */
export interface ApprovalActor {
  kind: string;
  [key: string]: unknown;
}

/** 已有裁决（allow/deny + 裁决人/依据/时间） */
export interface ApprovalDecision {
  decision: "allow" | "deny";
  by: ApprovalActor;
  reason?: string;
  matched_capabilities: string[];
  evaluated_at: string;
}

/** approvals.list 单条审批（卡片逐字段渲染） */
export interface ApprovalItem {
  run_id: string;
  workflow_id: string;
  workflow_version: number;
  node_id: string;
  gate: ApprovalGate | string;
  status: ApprovalStatus | string;
  /** 审批身份：run.approve / run.deny 必须原样回传 */
  operation_id: string;
  request_hash: string;
  actor: ApprovalActor;
  /** 需求集合，全部渲染 */
  specs: ApprovalSpec[];
  /** G4 插件节点：manifest 声明的需求（模式形状；specs 为空时展示） */
  needs?: ApprovalSpec[];
  context: {
    run_id: string;
    workflow_id: string;
    workflow_version: number;
    node_id: string;
    node_type: string;
    dispatch_id: string;
    attempt: number;
    request_fingerprint: string;
    tool_call_id?: string;
    [key: string]: unknown;
  };
  policy_digest: string;
  /** 命中的能力依据（"workflow:xx" / "node:xx"） */
  matched: string[];
  /** 毫秒时间戳，服务端为准 */
  asked_at: number;
  deadline: number;
  decision: ApprovalDecision | null;
  /** 请求正文 JSON；可能为 {truncated:true,bytes}（过大只给字节数） */
  request_preview: unknown;
}

/** approvals.list 返回：pending 在前、deadline 升序；server_time 为服务端毫秒时钟 */
export interface ApprovalsPage {
  items: ApprovalItem[];
  server_time: number;
}

/** run.approve / run.deny 的命令回执：result 为 {decision:"allowed",scope} 或
 *  {already_decided:true,decision} 等（approval 面的物化结果） */
export interface CommandReceipt {
  result?: unknown;
  [key: string]: unknown;
}

/** permissions.effective 的节点条目：allow 带 derived 派生标注 */
export interface EffectiveCapability extends Capability {
  derived?: "not_granted_by_workflow" | "requires_approval" | "auto" | string;
}

export interface EffectiveNode {
  node_id: string;
  node_type: string;
  mode: "inherit" | "restrict" | string;
  allow: EffectiveCapability[];
  side_effect: boolean;
}

/** 能力天花板：如 harness 子代理自带的上限（source 说明来源） */
export interface EffectiveCeiling {
  source: string;
  allow: Capability[];
}

/** permissions.effective 返回：定义版本或 run 的策略投影（诊断只读展示） */
export interface PermissionsEffective {
  /** true = 工作流未声明顶层 permissions（legacy，不能启动新 run） */
  legacy: boolean;
  mode: "execute" | "plan" | string;
  policy_digest: string;
  workflow: Capability[];
  nodes: EffectiveNode[];
  ceilings: EffectiveCeiling[];
  diagnostics: string[];
  start_gate: string | null;
  /** 定义版本查询时附带 */
  workflow_id?: string;
  version?: number;
  /** run 查询时附带：策略已冻结 + run 内已登记的批准 */
  run_id?: string;
  frozen?: boolean;
  approvals?: unknown;
}


export interface ResidentService {
  plugin_id:string;
  instance:string;
  alive:boolean;
  ui?:string|null;
  binding?:{run_id:string;node_id:string}|null;
}
