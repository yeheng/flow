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
 * 属性可带 x-widget / x-label / x-help 扩展键
 */
export interface PropertySchema {
  type?: "string" | "integer" | "number" | "boolean" | "object";
  enum?: string[];
  default?: unknown;
  required?: string[];
  properties?: Record<string, PropertySchema>;
  "x-widget"?: "code" | "json" | "workflow-picker";
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
  started_at: string;
  ended_at: string | null;
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
  output: unknown;
  error: string | null;
  reason?: string;
  /** sub_workflow 节点启动的子 run；其余节点为空 */
  child_run_id?: string;
}

export type RunPhase = "running" | "succeeded" | "failed" | "cancelled";

export interface Timeline {
  run_id: string;
  status: string;
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
    | "signal_received"
    | "run_completed"
    | "run_failed"
    | "run_cancelled";
  node_id?: string;
  attempt?: number;
  child_run_id?: string;
  output?: unknown;
  duration_ms?: number;
  error?: string;
  retryable?: boolean;
  reason?: string;
  payload?: unknown;
}
