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
}

export interface DefinitionEdge {
  from: string;
  to: string;
  /** condition 出边为 "true" | "false"，其余节点出边不得带 */
  port?: string;
}

export interface Definition {
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
  type?: "string" | "integer" | "number" | "boolean" | "object";
  enum?: string[];
  default?: unknown;
  required?: string[];
  properties?: Record<string, PropertySchema>;
  "x-widget"?: "code" | "json" | "workflow-picker";
  /** true 表示该字符串字段存的是密钥名称（值在服务端 FLOW_SECRET_*），前端渲染为密钥名称选择器 */
  "x-secret"?: boolean;
  /**
   * true 表示该字段是 flow 自己的语法（JS 代码），不参与 `${}` 模板展开。
   * 前端无需据此渲染（与 x-widget: code 是两回事：llm.prompt / email.body
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
  token: string;
  workflow_id: string;
  enabled: boolean;
  created_at: string;
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

export type NodeRunState = "pending" | "running" | "retrying" | "completed" | "failed" | "skipped";

/** run.timeline 节点条目（crates/flow-rpc/src/lib.rs timeline_value） */
export interface TimelineNode {
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
}

export type RunPhase = "running" | "succeeded" | "failed" | "cancelled";

/** 节点日志级别/来源（与 crates/flow-engine/src/event.rs 的词汇表对齐） */
export type LogLevel = "debug" | "info" | "warn" | "error";
export type LogStream = "engine" | "stdout" | "stderr";

/** 前端日志行：node_log 事件的前端形态（日志控制台/节点检查器共用） */
export interface LogLine {
  seq: number;
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
  /**
   * 服务端 timeline 仍带 phase（fold 相位），但前端不再消费：它与 status 对
   * awaiting_resume 不一致（phase=running / status=awaiting_resume），是前端
   * 曾经需要 SPECIAL_STATUS 特判才能渲染对配色的根因。字段保留在 wire 上，
   * 前端只以 status 为准。
   */
  phase: RunPhase;
  workflow_id: string;
  workflow_version: number;
  started_at: string | null;
  ended_at: string | null;
  output: unknown;
  fatal_error: string | null;
  last_seq: number;
  nodes: TimelineNode[];
}

/**
 * run.event 通知载荷：crates/flow-engine/src/event.rs 的 Envelope，
 * event 经 #[serde(flatten)] 扁平展开（type + 各事件字段）。
 */
export interface RunEvent {
  seq: number;
  ts: string;
  run_id: string;
  type:
    | "run_started"
    | "node_started"
    | "node_completed"
    | "node_failed"
    | "node_skipped"
    | "node_log"
    | "signal_received"
    | "run_completed"
    | "run_failed"
    | "run_cancelled";
  node_id?: string;
  attempt?: number;
  child_run_id?: string;
  input?: unknown;
  output?: unknown;
  duration_ms?: number;
  error?: string;
  retryable?: boolean;
  reason?: string;
  level?: LogLevel;
  stream?: LogStream;
  message?: string;
  payload?: unknown;
}
