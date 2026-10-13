//! IPC 消息目录（二期 §3.3/§3.4，I01 冻结）。
//!
//! 物理帧为 v4 单流：`u32 body_len | u8 kind | u32 meta_len | JSON meta |
//! raw blob`。kind=0 时 meta 是一个信封
//! `{"v":4,"type":"AuditBatch","dispatch_id":"…","body":{…}}`；kind=1/2
//! 时 meta 是不含 bytes 的元数据信封、blob 承载原始字节。
//! epoch、序号、字节累计位置等可能超过 JavaScript 安全整数范围的值统一
//! 使用十进制字符串（[`flow_journal::decimal`]）。会话身份由连接上下文
//! 提供，任务帧只携带 dispatch_id 及必要业务字段。
//!
//! 未知 `type` / 缺失必需字段 / 版本不兼容都构成协议错误：明确拒绝，
//! 不降级成无法解释的执行行为（二期 §3.2）。

use flow_journal::StoredValue;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::contract::PROTOCOL_VERSION;
use super::remote::{Capacity, ResumeDecision, ResumeItem, RouteEnvelope};

type Result<T> = std::result::Result<T, ProtocolError>;

/// 协议层错误：明确拒绝语义（二期 §3.2“明确拒绝，不降级”）。
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("protocol frame too large: {0} > {1}")]
    FrameTooLarge(usize, usize),
    #[error("protocol frame malformed: {0}")]
    Malformed(String),
    #[error("unsupported protocol message: {0}")]
    UnknownMessage(String),
    #[error("message payload invalid: {0}")]
    InvalidPayload(String),
    #[error("session/channel closed")]
    Closed,
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// 单条审计记录（AuditBatch 载荷单元）。`kind` 取一期 `EventKind` 的
/// snake_case 名（如 `input_prepared`/`value_chunk`/`value_published`/
/// `operation_outcome`）。窗口计费按 [`AuditRecord::encoded_bytes`] 的
/// 确定性编码：字段顺序固定为 audit_seq/kind/payload，payload 由
/// serde_json 的有序 map 保证确定性。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditRecord {
    #[serde(with = "flow_journal::decimal")]
    pub audit_seq: u64,
    pub kind: String,
    pub payload: Value,
}

impl AuditRecord {
    /// 协议确定性编码字节数（发送端保存原始编码用于重传，主进程按同一
    /// 规则校验字节数，二期 §3.5）。
    pub fn encoded_bytes(&self) -> u64 {
        record_bytes(self.audit_seq, &self.kind, &self.payload)
    }
}

/// 确定性记录编码：`{"audit_seq":"N","kind":"…","payload":{…}}`。
pub fn record_bytes(audit_seq: u64, kind: &str, payload: &Value) -> u64 {
    #[derive(Serialize)]
    struct Wire<'a> {
        audit_seq: &'a str,
        kind: &'a str,
        payload: &'a Value,
    }
    let seq = audit_seq.to_string();
    serde_json::to_vec(&Wire {
        audit_seq: seq.as_str(),
        kind,
        payload,
    })
    .map(|v| v.len() as u64)
    .unwrap_or(u64::MAX)
}

/// 结果载荷（Result.outcome）。等待请求（delay/signal/child）由主进程在
/// 同一结果接受事务登记 wake_at/信号等待/父子关系（二期 §3.8）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum ResultOutcome {
    /// 正常完成：固定结果描述（小结果 inline，大值为已发送 chunk 的 ValueRef）
    /// 与可选 condition 分支。
    Success {
        output: StoredValue,
        branch: Option<bool>,
    },
    /// 失败结果（准备失败走同一派发的失败 Result，不发送第二个 Execute）。
    /// `uncertain_operation`：授权后调用未得到可验证 Outcome——主进程按
    /// 一期规则登记 uncertain 等待而非节点失败。
    Failure {
        error: String,
        #[serde(default)]
        uncertain_operation: bool,
    },
    /// 持久等待登记请求；主进程确认后释放槽位，等待不占执行进程。
    Wait { wait: WaitRequest },
}

/// 等待请求。`wake_at` 为 epoch 毫秒（delay）；child 等待的 child_run_id
/// 由主进程在结果接受事务创建并登记（执行器不创建子 run）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitRequest {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "decimal_i64_opt"
    )]
    pub wake_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<StoredValue>,
    /// sub_workflow 等待：子 run 由主进程在结果接受事务创建。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_mapping: Option<Value>,
}

/// i64 的可选十进制字符串编解码（与 flow_journal::decimal 同规则）。
pub mod decimal_i64_opt {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(v: &Option<i64>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(n) => s.serialize_str(&n.to_string()),
            None => s.serialize_none(),
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
        let raw: Option<String> = Option::deserialize(d)?;
        match raw {
            None => Ok(None),
            Some(s) => {
                let n: i64 = s.parse().map_err(serde::de::Error::custom)?;
                if n.to_string() != s {
                    return Err(serde::de::Error::custom("noncanonical decimal"));
                }
                Ok(Some(n))
            }
        }
    }
}

/// Option<u64> 的十进制字符串编解码。
pub mod decimal_u64_opt {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(v: &Option<u64>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(n) => s.serialize_str(&n.to_string()),
            None => s.serialize_none(),
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
        let raw: Option<String> = Option::deserialize(d)?;
        match raw {
            None => Ok(None),
            Some(s) => {
                let n: u64 = s.parse().map_err(serde::de::Error::custom)?;
                if n.to_string() != s {
                    return Err(serde::de::Error::custom("noncanonical decimal"));
                }
                Ok(Some(n))
            }
        }
    }
}

/// u32 的十进制字符串编解码（dispatch attempt）。
pub mod decimal_u32 {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(v: &u32, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u32, D::Error> {
        let s = String::deserialize(d)?;
        let v: u32 = s.parse().map_err(serde::de::Error::custom)?;
        if v.to_string() != s {
            return Err(serde::de::Error::custom("noncanonical decimal"));
        }
        Ok(v)
    }
}

pub mod kind {
    pub const HELLO: &str = "Hello";
    pub const WELCOME: &str = "Welcome";
    pub const READY: &str = "Ready";
    pub const EXECUTE: &str = "Execute";
    pub const ACCEPTED: &str = "Accepted";
    pub const AUDIT_BATCH: &str = "AuditBatch";
    pub const AUDIT_ACK: &str = "AuditAck";
    pub const REQUEST_OPERATION: &str = "RequestOperation";
    pub const OPERATION_PERMIT: &str = "OperationPermit";
    pub const TRANSFER_CHUNK: &str = "TransferChunk";
    pub const INPUT_READY: &str = "InputReady";
    pub const RESULT: &str = "Result";
    pub const RESULT_COMMITTED: &str = "ResultCommitted";
    pub const CANCEL: &str = "Cancel";
    pub const STOPPED: &str = "Stopped";
    pub const HEARTBEAT: &str = "Heartbeat";
    pub const OBSERVABILITY_BATCH: &str = "ObservabilityBatch";
    pub const REJECT: &str = "Reject";
    // 远程层（三期 §1.2）
    pub const ROUTED: &str = "Routed";
    pub const AGENT_HELLO: &str = "AgentHello";
    pub const AGENT_WELCOME: &str = "AgentWelcome";
    pub const CAPACITY_REPORT: &str = "CapacityReport";
    pub const BIND_EXECUTOR: &str = "BindExecutor";
    pub const BIND_EXECUTOR_ACK: &str = "BindExecutorAck";
    pub const RESUME: &str = "Resume";
    pub const RESUME_REPLY: &str = "ResumeReply";
    pub const DRAIN: &str = "Drain";
    pub const DRAIN_COMPLETE: &str = "DrainComplete";
}

/// 统一消息目录。通道与方向约束见二期 §3.4 表格；transport 层按通道
/// 校验消息类型是否合法（如 AuditBatch 不得出现在 control）。
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    /// control，子→主。启动检查第一步；不授予任务执行权。
    Hello {
        boot_id: String,
        build: String,
        capabilities: Vec<String>,
        /// 回连承载的会话令牌（loopback 防本机抢连；unix socketpair 走 FD
        /// 能力不需要，为 None）。主进程在 spawn 时经 env 注入，Hello 里
        /// 回验——抢连者在毫秒窗口内拿不到该值。
        session_token: Option<String>,
    },
    /// control，主→子。选定版本、会话身份、限额与超时。
    Welcome {
        journal_id: String,
        master_epoch: u64,
        session_id: String,
        executor_id: String,
        window_bytes: u64,
        max_record_bytes: u64,
        heartbeat_ms: u64,
        drain_grace_ms: u64,
    },
    /// control，子→主。I/O 循环初始化完成，可接任务。
    Ready,
    /// control，主→子。一次节点派发：内部准备与执行共用同一 dispatch。
    Execute {
        command_id: String,
        dispatch_id: String,
        task: ExecuteTask,
    },
    /// control，子→主。仅表示接收；已提交 Execute 授权纯计算。
    Accepted {
        command_id: String,
        dispatch_id: String,
    },
    /// data，子→主。审计事实批次（含 InputPrepared、输出 chunk、
    /// OperationOutcome 等）。ACK 仅确认持久性。
    AuditBatch {
        dispatch_id: String,
        first_seq: u64,
        records: Vec<AuditRecord>,
    },
    /// control，主→子。只随连续持久前缀前进；不授予外部调用权。
    AuditAck {
        dispatch_id: String,
        durable_audit_seq: u64,
        durable_bytes: u64,
        commit_lsn: u64,
    },
    /// control，子→主。请求外部操作许可；请求体经 data 通道分块传输或
    /// 小请求 inline。携带请求指纹；主进程校验完整内容后才授权。
    RequestOperation {
        dispatch_id: String,
        command_id: String,
        operation_id: String,
        request_fingerprint: String,
        /// 请求字节的 data 通道 transfer 标识（op:<operation_id>）。
        /// v2 起请求字节一律走 data 通道，无 inline 特例。
        transfer_id: String,
    },
    /// control，主→子。Intent 与 Authorized 同事务提交成功后发出；
    /// 绑定会话身份；重复 permit_id 不再调用。
    OperationPermit {
        dispatch_id: String,
        operation_id: String,
        permit_id: String,
        request_fingerprint: String,
        /// 主进程解析的凭证值（仅本次调用使用，不进入授权事实）。
        credential: Option<String>,
    },
    /// data，双向。输入分块传输：offset + raw bytes（帧内 blob）+ digest。
    /// `dispatch_id` 是路由键：三期中继按它把帧包进 Routed 信封发往归属
    /// 执行器（没有它，run_session 查不到路由会静默丢帧——Ref 输入在
    /// remote 模式下全部黑洞）。
    TransferChunk {
        dispatch_id: String,
        transfer_id: String,
        offset: u64,
        /// 原始字节（内存中不 base64）；帧内作为 raw blob 承载。
        bytes: Vec<u8>,
        digest: String,
    },
    /// control，发送方。输入接收器校验并准备完成；不表示新的权威提交，
    /// 不释放审计持久窗口。`dispatch_id` 同 TransferChunk（路由键）。
    InputReady {
        dispatch_id: String,
        transfer_id: String,
        total_bytes: u64,
        digest: String,
    },
    /// control，子→主。固定结果描述 + 最后审计序号；发送前生产者已全部
    /// 停止并完成输出捕获封口。
    Result {
        dispatch_id: String,
        result_id: String,
        last_audit_seq: u64,
        outcome: ResultOutcome,
    },
    /// control，主→子。屏障满足、业务结果或持久等待登记提交后确认。
    ResultCommitted {
        dispatch_id: String,
        result_id: String,
    },
    /// control，主→子。持久取消意图驱动停止。
    Cancel {
        command_id: String,
        dispatch_id: String,
    },
    /// control，子→主。尽力封口状态；sealed=true 仅当全部生产者停止。
    Stopped {
        dispatch_id: String,
        sealed: bool,
        last_audit_seq: u64,
        pending_operation: Option<String>,
    },
    /// control，双向。存活与进度提示；不是执行结果或持久提交证明。
    Heartbeat {
        dispatch_id: Option<String>,
        phase: String,
    },
    /// data，子→主，单向。独立预算，无确认、不占 audit_seq、可丢弃、
    /// 永不反压 golden source（二期 §4.3）。
    ObservabilityBatch {
        dispatch_id: Option<String>,
        lines: Vec<ObservationLine>,
    },
    /// control，双向。协议错误：明确拒绝后关闭连接。
    Reject { reason: String },
    /// 远程路由帧（三期 §1.2）：外层包络 + 内层业务消息（审计原始编码
    /// 原样嵌套）。通道跟随内层。
    Routed {
        envelope: RouteEnvelope,
        inner: Box<Message>,
    },
    /// control，agent→主。上联握手：TLS 主体映射 agent_id。
    AgentHello {
        agent_boot_id: String,
        build: String,
        capabilities: Vec<String>,
        slots: u32,
    },
    /// control，主→agent。选定 link_session、限额与心跳周期（v4 单流上联，
    /// 无第二条 data 连接，也就没有 v2 的 data 绑定凭据）。
    AgentWelcome {
        journal_id: String,
        master_epoch: u64,
        link_session_id: String,
        agent_id: String,
        heartbeat_ms: u64,
        /// agent 级转发预算（C_agent，字节）。
        agent_window_bytes: u64,
    },
    /// control，agent→主。资源报告。
    CapacityReport { capacity: Capacity },
    /// control，主→agent。绑定本地执行器到派发；确认后才能 Execute。
    BindExecutor {
        executor_id: String,
        dispatch_id: String,
    },
    /// control，agent→主。绑定确认（含本地执行器 boot 身份）。
    BindExecutorAck {
        executor_id: String,
        executor_boot_id: String,
        dispatch_id: String,
        ok: bool,
        error: Option<String>,
    },
    /// control，agent→主（新上联会话）。断线对账清单（分页）。
    Resume {
        items: Vec<ResumeItem>,
        page: u32,
        more: bool,
    },
    /// control，主→agent。逐项裁决。
    ResumeReply {
        decisions: Vec<ResumeDecision>,
        page: u32,
    },
    /// control，主→agent。drain：停止接新派发，收尾后退出（三期 §1.6）。
    Drain { grace_ms: u64 },
    /// control，agent→主。drain 完成：在飞任务终态已转发。
    DrainComplete { drained: u32 },
}

/// 观测行（nodelog console/stdout/stderr 叙事）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationLine {
    pub level: String,
    pub stream: String,
    pub message: String,
}

/// 一次节点派发的输入面。dispatch 必须解析到固定 run/node/attempt/
/// node_execution_id；接收端按连接上下文校验派发归属。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecuteTask {
    pub run_id: String,
    pub node_id: String,
    pub node_execution_id: String,
    #[serde(with = "decimal_u32")]
    pub attempt: u32,
    /// 节点定义（含未展开 params）。控制帧唯一允许的变长项（有界：
    /// dispatch 侧按 CONTROL_MAX_FRAME/2 预检，超限拒绝派发而非杀会话）。
    pub node: Value,
    /// run 输入与前驱输出的声明：控制帧只携带身份与 codec，值字节
    /// 无论大小一律走 data 通道（TransferChunk + InputReady）。
    pub input: TaskValue,
    pub predecessors: Vec<(String, TaskValue)>,
    /// 已提交 InputPrepared 的元数据（重试恢复；执行器跳过求值，不重新
    /// 求时间/随机模板）。input/predecessors/params 的值经 inputs 声明
    /// 传输（in:run-input / in:<node_id> / in:prepared-params）。
    pub prepared: Option<PreparedMeta>,
    /// 已提交但缺 Outcome 的操作恢复出的历史 Outcome 元数据（重试复用，
    /// 不重发；体字节走 op-body:<output_id> 传输）。
    pub previous_outcome: Option<Value>,
    /// 观测开/关（主进程未配置观测存储时关闭，避免无谓传输）。
    pub observability: bool,
}

/// 输入值声明（v2）：控制帧不携带用户值字节。
/// - `Ref`：journal 引用的身份元数据（start/end 节点输出按引用回传，
///   需要完整 ValueRef）；字节由主进程从 journal 流式送出。
/// - `Local`：原 inline 值，只声明存在；字节由主进程分块送出，
///   接收端按 JSON 解码。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TaskValue {
    Ref(flow_journal::ValueRef),
    Local,
}

/// 已提交 InputPrepared 的元数据（值字段不入控制帧）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedMeta {
    pub engine_build: String,
    #[serde(with = "decimal_u32")]
    pub node_semantics: u32,
    /// 已提交准备参数的值声明（字节走 in:prepared-params 传输）。
    pub params: TaskValue,
}

impl Message {
    pub fn type_name(&self) -> &'static str {
        match self {
            Message::Hello { .. } => kind::HELLO,
            Message::Welcome { .. } => kind::WELCOME,
            Message::Ready => kind::READY,
            Message::Execute { .. } => kind::EXECUTE,
            Message::Accepted { .. } => kind::ACCEPTED,
            Message::AuditBatch { .. } => kind::AUDIT_BATCH,
            Message::AuditAck { .. } => kind::AUDIT_ACK,
            Message::RequestOperation { .. } => kind::REQUEST_OPERATION,
            Message::OperationPermit { .. } => kind::OPERATION_PERMIT,
            Message::TransferChunk { .. } => kind::TRANSFER_CHUNK,
            Message::InputReady { .. } => kind::INPUT_READY,
            Message::Result { .. } => kind::RESULT,
            Message::ResultCommitted { .. } => kind::RESULT_COMMITTED,
            Message::Cancel { .. } => kind::CANCEL,
            Message::Stopped { .. } => kind::STOPPED,
            Message::Heartbeat { .. } => kind::HEARTBEAT,
            Message::ObservabilityBatch { .. } => kind::OBSERVABILITY_BATCH,
            Message::Reject { .. } => kind::REJECT,
            Message::Routed { .. } => kind::ROUTED,
            Message::AgentHello { .. } => kind::AGENT_HELLO,
            Message::AgentWelcome { .. } => kind::AGENT_WELCOME,
            Message::CapacityReport { .. } => kind::CAPACITY_REPORT,
            Message::BindExecutor { .. } => kind::BIND_EXECUTOR,
            Message::BindExecutorAck { .. } => kind::BIND_EXECUTOR_ACK,
            Message::Resume { .. } => kind::RESUME,
            Message::ResumeReply { .. } => kind::RESUME_REPLY,
            Message::Drain { .. } => kind::DRAIN,
            Message::DrainComplete { .. } => kind::DRAIN_COMPLETE,
        }
    }

    /// 消息所属通道（二期 §3.4）。方向由收发双方位置决定；transport 按
    /// 此表拒绝放错通道的消息。
    pub fn channel(&self) -> Channel {
        match self {
            Message::TransferChunk { .. }
            | Message::ObservabilityBatch { .. }
            | Message::AuditBatch { .. } => Channel::Data,
            // 路由帧跟随内层通道（审计/传输走 data，其余 control）。
            Message::Routed { inner, .. } => inner.channel(),
            _ => Channel::Control,
        }
    }

    pub fn dispatch_id(&self) -> Option<&str> {
        match self {
            Message::Execute { dispatch_id, .. }
            | Message::Accepted { dispatch_id, .. }
            | Message::AuditBatch { dispatch_id, .. }
            | Message::AuditAck { dispatch_id, .. }
            | Message::RequestOperation { dispatch_id, .. }
            | Message::OperationPermit { dispatch_id, .. }
            | Message::Result { dispatch_id, .. }
            | Message::ResultCommitted { dispatch_id, .. }
            | Message::Cancel { dispatch_id, .. }
            | Message::TransferChunk { dispatch_id, .. }
            | Message::InputReady { dispatch_id, .. }
            | Message::Stopped { dispatch_id, .. } => Some(dispatch_id),
            Message::Heartbeat { dispatch_id, .. }
            | Message::ObservabilityBatch { dispatch_id, .. } => dispatch_id.as_deref(),
            Message::Routed { envelope, .. } => envelope.dispatch_id.as_deref(),
            _ => None,
        }
    }

    /// 编码为信封 JSON 值。十进制字段一律经带 `#[serde(with = "decimal")]`
    /// 的 Wire 结构序列化，保证 u64 始终是十进制字符串。
    pub fn to_envelope(&self) -> Value {
        #[derive(Serialize)]
        struct HelloW<'a> {
            boot_id: &'a str,
            build: &'a str,
            capabilities: &'a [String],
            #[serde(skip_serializing_if = "Option::is_none")]
            session_token: Option<&'a str>,
        }
        #[derive(Serialize)]
        struct WelcomeW {
            journal_id: String,
            #[serde(with = "flow_journal::decimal")]
            master_epoch: u64,
            session_id: String,
            executor_id: String,
            #[serde(with = "flow_journal::decimal")]
            window_bytes: u64,
            #[serde(with = "flow_journal::decimal")]
            max_record_bytes: u64,
            #[serde(with = "flow_journal::decimal")]
            heartbeat_ms: u64,
            #[serde(with = "flow_journal::decimal")]
            drain_grace_ms: u64,
        }
        #[derive(Serialize)]
        struct AckW {
            #[serde(with = "flow_journal::decimal")]
            durable_audit_seq: u64,
            #[serde(with = "flow_journal::decimal")]
            durable_bytes: u64,
            #[serde(with = "flow_journal::decimal")]
            commit_lsn: u64,
        }
        #[derive(Serialize)]
        struct ReadyW {
            transfer_id: String,
            #[serde(with = "flow_journal::decimal")]
            total_bytes: u64,
            digest: String,
        }
        #[derive(Serialize)]
        struct ResultW<'a> {
            result_id: String,
            #[serde(with = "flow_journal::decimal")]
            last_audit_seq: u64,
            outcome: &'a ResultOutcome,
        }
        #[derive(Serialize)]
        struct StoppedW {
            sealed: bool,
            #[serde(with = "flow_journal::decimal")]
            last_audit_seq: u64,
            pending_operation: Option<String>,
        }
        let body = match self {
            Message::Hello {
                boot_id,
                build,
                capabilities,
                session_token,
            } => body_value(&HelloW {
                boot_id,
                build,
                capabilities,
                session_token: session_token.as_deref(),
            }),
            Message::Welcome {
                journal_id,
                master_epoch,
                session_id,
                executor_id,
                window_bytes,
                max_record_bytes,
                heartbeat_ms,
                drain_grace_ms,
            } => body_value(&WelcomeW {
                journal_id: journal_id.clone(),
                master_epoch: *master_epoch,
                session_id: session_id.clone(),
                executor_id: executor_id.clone(),
                window_bytes: *window_bytes,
                max_record_bytes: *max_record_bytes,
                heartbeat_ms: *heartbeat_ms,
                drain_grace_ms: *drain_grace_ms,
            }),
            Message::Ready => None,
            Message::Execute {
                command_id,
                dispatch_id: _,
                task,
            } => body_value(serde_json::json!({
                "command_id": command_id,
                "task": task,
            })),
            Message::Accepted {
                command_id,
                dispatch_id: _,
            } => body_value(serde_json::json!({"command_id": command_id})),
            Message::AuditBatch {
                dispatch_id: _,
                first_seq,
                records,
            } => body_value(serde_json::json!({
                "first_seq": first_seq.to_string(),
                "records": records,
            })),
            Message::AuditAck {
                dispatch_id: _,
                durable_audit_seq,
                durable_bytes,
                commit_lsn,
            } => body_value(&AckW {
                durable_audit_seq: *durable_audit_seq,
                durable_bytes: *durable_bytes,
                commit_lsn: *commit_lsn,
            }),
            Message::RequestOperation {
                dispatch_id: _,
                command_id,
                operation_id,
                request_fingerprint,
                transfer_id,
            } => body_value(serde_json::json!({
                "command_id": command_id,
                "operation_id": operation_id,
                "request_fingerprint": request_fingerprint,
                "transfer_id": transfer_id,
            })),
            Message::OperationPermit {
                dispatch_id: _,
                operation_id,
                permit_id,
                request_fingerprint,
                credential,
            } => body_value(serde_json::json!({
                "operation_id": operation_id,
                "permit_id": permit_id,
                "request_fingerprint": request_fingerprint,
                "credential": credential,
            })),
            // TransferChunk 生产路径恒走 raw kind（见 to_frame_parts）；此处
            // 仅提供不含 bytes 的元数据信封（JSON 携带 TransferChunk 会被
            // 解码端明确拒绝，不做 base64 兼容）。
            Message::TransferChunk {
                transfer_id,
                offset,
                digest,
                ..
            } => Some(json!({
                "transfer_id": transfer_id,
                "offset": offset.to_string(),
                "digest": digest,
            })),
            Message::InputReady {
                dispatch_id: _,
                transfer_id,
                total_bytes,
                digest,
            } => body_value(&ReadyW {
                transfer_id: transfer_id.clone(),
                total_bytes: *total_bytes,
                digest: digest.clone(),
            }),
            Message::Result {
                dispatch_id: _,
                result_id,
                last_audit_seq,
                outcome,
            } => body_value(&ResultW {
                result_id: result_id.clone(),
                last_audit_seq: *last_audit_seq,
                outcome,
            }),
            Message::ResultCommitted {
                dispatch_id: _,
                result_id,
            } => body_value(serde_json::json!({"result_id": result_id})),
            Message::Cancel { command_id, .. } => {
                body_value(serde_json::json!({"command_id": command_id}))
            }
            Message::Stopped {
                dispatch_id: _,
                sealed,
                last_audit_seq,
                pending_operation,
            } => body_value(&StoppedW {
                sealed: *sealed,
                last_audit_seq: *last_audit_seq,
                pending_operation: pending_operation.clone(),
            }),
            Message::Heartbeat {
                dispatch_id: _,
                phase,
            } => body_value(serde_json::json!({"phase": phase})),
            Message::ObservabilityBatch {
                dispatch_id: _,
                lines,
            } => body_value(serde_json::json!({"lines": lines})),
            Message::Reject { reason } => body_value(serde_json::json!({"reason": reason})),
            Message::Routed { envelope, inner } => body_value(serde_json::json!({
                "envelope": envelope,
                "inner": inner.to_envelope(),
            })),
            Message::AgentHello {
                agent_boot_id,
                build,
                capabilities,
                slots,
            } => {
                #[derive(Serialize)]
                struct Wire {
                    agent_boot_id: String,
                    build: String,
                    capabilities: Vec<String>,
                    #[serde(with = "decimal_u32")]
                    slots: u32,
                }
                body_value(&Wire {
                    agent_boot_id: agent_boot_id.clone(),
                    build: build.clone(),
                    capabilities: capabilities.clone(),
                    slots: *slots,
                })
            }
            Message::AgentWelcome {
                journal_id,
                master_epoch,
                link_session_id,
                agent_id,
                heartbeat_ms,
                agent_window_bytes,
            } => {
                #[derive(Serialize)]
                struct Wire {
                    journal_id: String,
                    #[serde(with = "flow_journal::decimal")]
                    master_epoch: u64,
                    link_session_id: String,
                    agent_id: String,
                    #[serde(with = "flow_journal::decimal")]
                    heartbeat_ms: u64,
                    #[serde(with = "flow_journal::decimal")]
                    agent_window_bytes: u64,
                }
                body_value(&Wire {
                    journal_id: journal_id.clone(),
                    master_epoch: *master_epoch,
                    link_session_id: link_session_id.clone(),
                    agent_id: agent_id.clone(),
                    heartbeat_ms: *heartbeat_ms,
                    agent_window_bytes: *agent_window_bytes,
                })
            }
            Message::CapacityReport { capacity } => {
                body_value(serde_json::json!({"capacity": capacity}))
            }
            Message::BindExecutor {
                executor_id,
                dispatch_id,
            } => body_value(serde_json::json!({
                "executor_id": executor_id,
                "dispatch_id": dispatch_id,
            })),
            Message::BindExecutorAck {
                executor_id,
                executor_boot_id,
                dispatch_id,
                ok,
                error,
            } => body_value(serde_json::json!({
                "executor_id": executor_id,
                "executor_boot_id": executor_boot_id,
                "dispatch_id": dispatch_id,
                "ok": ok,
                "error": error,
            })),
            Message::Resume { items, page, more } => body_value(serde_json::json!({
                "items": items,
                "page": page.to_string(),
                "more": more,
            })),
            Message::ResumeReply { decisions, page } => body_value(serde_json::json!({
                "decisions": decisions,
                "page": page.to_string(),
            })),
            Message::Drain { grace_ms } => {
                body_value(serde_json::json!({"grace_ms": grace_ms.to_string()}))
            }
            Message::DrainComplete { drained } => {
                body_value(serde_json::json!({"drained": drained.to_string()}))
            }
        };
        let mut envelope = serde_json::Map::new();
        envelope.insert("v".into(), json!(PROTOCOL_VERSION));
        envelope.insert("type".into(), json!(self.type_name()));
        if let Some(dispatch_id) = self.dispatch_id() {
            envelope.insert("dispatch_id".into(), json!(dispatch_id));
        }
        if let Some(body) = body {
            envelope.insert("body".into(), body);
        }
        Value::Object(envelope)
    }

    /// 从信封 JSON 解码（兼容入口：已有 Value 树）。生产路径用
    /// [`Message::decode_envelope`]（T05：借用信封 + 类型化单遍解析）。
    pub fn from_envelope(value: Value) -> Result<Self> {
        if !value.is_object() {
            return Err(ProtocolError::Malformed("envelope must be object".into()));
        }
        let version = value["v"].as_u64().unwrap_or(0);
        if version != PROTOCOL_VERSION as u64 {
            return Err(ProtocolError::Malformed(format!(
                "protocol version {version} not supported"
            )));
        }
        let type_name = value["type"]
            .as_str()
            .ok_or_else(|| ProtocolError::Malformed("missing type".into()))?;
        let body = value.get("body").cloned().unwrap_or(Value::Null);
        let envelope_dispatch = value.get("dispatch_id").and_then(Value::as_str);
        Self::decode_kind(&body, type_name, envelope_dispatch)
    }

    /// 从信封 JSON 字节直接解码（T05 生产路径）：信封借用解析（RawValue
    /// body 不建 Value 树、不做 body 深拷贝），类型化 Wire 单遍解析。
    pub fn decode_envelope(bytes: &[u8]) -> Result<Self> {
        #[derive(serde::Deserialize)]
        struct Envelope<'a> {
            v: u64,
            #[serde(borrow, rename = "type")]
            kind: &'a str,
            #[serde(borrow, default)]
            dispatch_id: Option<&'a str>,
            #[serde(borrow, default)]
            body: Option<&'a serde_json::value::RawValue>,
        }
        let envelope: Envelope<'_> = serde_json::from_slice(bytes)
            .map_err(|e| ProtocolError::Malformed(format!("envelope: {e}")))?;
        if envelope.v != PROTOCOL_VERSION as u64 {
            return Err(ProtocolError::Malformed(format!(
                "protocol version {} not supported",
                envelope.v
            )));
        }
        // body 缺省视作 null（与 from_envelope 兼容：Wire 解析再报具体错误）。
        let null_body: &serde_json::value::RawValue =
            serde_json::from_str("null").expect("static null RawValue");
        let body = envelope.body.unwrap_or(null_body);
        Self::decode_kind(body, envelope.kind, envelope.dispatch_id)
    }

    /// 按类型解码 body（两入口共用同一套 Wire 结构与校验；静态分发）。
    fn decode_kind<B: BodySource + ?Sized>(
        body: &B,
        type_name: &str,
        envelope_dispatch: Option<&str>,
    ) -> Result<Self> {
        let dispatch_id = |envelope_dispatch: Option<&str>| -> Result<String> {
            envelope_dispatch
                .map(str::to_owned)
                .ok_or_else(|| ProtocolError::InvalidPayload(format!("{type_name}: dispatch_id")))
        };
        let message = match type_name {
            kind::HELLO => {
                #[derive(Deserialize)]
                struct Wire {
                    boot_id: String,
                    build: String,
                    capabilities: Vec<String>,
                    #[serde(default)]
                    session_token: Option<String>,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::Hello {
                    boot_id: wire.boot_id,
                    build: wire.build,
                    capabilities: wire.capabilities,
                    session_token: wire.session_token,
                }
            }
            kind::WELCOME => {
                #[derive(Deserialize)]
                struct Wire {
                    journal_id: String,
                    #[serde(with = "flow_journal::decimal")]
                    master_epoch: u64,
                    session_id: String,
                    executor_id: String,
                    #[serde(with = "flow_journal::decimal")]
                    window_bytes: u64,
                    #[serde(with = "flow_journal::decimal")]
                    max_record_bytes: u64,
                    #[serde(with = "flow_journal::decimal")]
                    heartbeat_ms: u64,
                    #[serde(with = "flow_journal::decimal")]
                    drain_grace_ms: u64,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::Welcome {
                    journal_id: wire.journal_id,
                    master_epoch: wire.master_epoch,
                    session_id: wire.session_id,
                    executor_id: wire.executor_id,
                    window_bytes: wire.window_bytes,
                    max_record_bytes: wire.max_record_bytes,
                    heartbeat_ms: wire.heartbeat_ms,
                    drain_grace_ms: wire.drain_grace_ms,
                }
            }
            kind::READY => Message::Ready,
            kind::EXECUTE => {
                #[derive(Deserialize)]
                struct Wire {
                    command_id: String,
                    task: ExecuteTask,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::Execute {
                    command_id: wire.command_id,
                    dispatch_id: dispatch_id(envelope_dispatch)?,
                    task: wire.task,
                }
            }
            kind::ACCEPTED => {
                #[derive(Deserialize)]
                struct Wire {
                    command_id: String,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::Accepted {
                    command_id: wire.command_id,
                    dispatch_id: dispatch_id(envelope_dispatch)?,
                }
            }
            kind::AUDIT_BATCH => {
                #[derive(Deserialize)]
                struct Wire {
                    #[serde(with = "flow_journal::decimal")]
                    first_seq: u64,
                    records: Vec<AuditRecord>,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::AuditBatch {
                    dispatch_id: dispatch_id(envelope_dispatch)?,
                    first_seq: wire.first_seq,
                    records: wire.records,
                }
            }
            kind::AUDIT_ACK => {
                #[derive(Deserialize)]
                struct Wire {
                    #[serde(with = "flow_journal::decimal")]
                    durable_audit_seq: u64,
                    #[serde(with = "flow_journal::decimal")]
                    durable_bytes: u64,
                    #[serde(with = "flow_journal::decimal")]
                    commit_lsn: u64,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::AuditAck {
                    dispatch_id: dispatch_id(envelope_dispatch)?,
                    durable_audit_seq: wire.durable_audit_seq,
                    durable_bytes: wire.durable_bytes,
                    commit_lsn: wire.commit_lsn,
                }
            }
            kind::REQUEST_OPERATION => {
                #[derive(Deserialize)]
                struct Wire {
                    command_id: String,
                    operation_id: String,
                    request_fingerprint: String,
                    transfer_id: String,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::RequestOperation {
                    dispatch_id: dispatch_id(envelope_dispatch)?,
                    command_id: wire.command_id,
                    operation_id: wire.operation_id,
                    request_fingerprint: wire.request_fingerprint,
                    transfer_id: wire.transfer_id,
                }
            }
            kind::OPERATION_PERMIT => {
                #[derive(Deserialize)]
                struct Wire {
                    operation_id: String,
                    permit_id: String,
                    request_fingerprint: String,
                    credential: Option<String>,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::OperationPermit {
                    dispatch_id: dispatch_id(envelope_dispatch)?,
                    operation_id: wire.operation_id,
                    permit_id: wire.permit_id,
                    request_fingerprint: wire.request_fingerprint,
                    credential: wire.credential,
                }
            }
            kind::TRANSFER_CHUNK => {
                // v4 恒用 raw kind 承载 TransferChunk；JSON 信封携带即协议错误。
                return Err(ProtocolError::Malformed(
                    "TransferChunk must use the raw frame kind".into(),
                ));
            }
            kind::INPUT_READY => {
                #[derive(Deserialize)]
                struct Wire {
                    transfer_id: String,
                    #[serde(with = "flow_journal::decimal")]
                    total_bytes: u64,
                    digest: String,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::InputReady {
                    dispatch_id: dispatch_id(envelope_dispatch)?,
                    transfer_id: wire.transfer_id,
                    total_bytes: wire.total_bytes,
                    digest: wire.digest,
                }
            }
            kind::RESULT => {
                #[derive(Deserialize)]
                struct Wire {
                    result_id: String,
                    #[serde(with = "flow_journal::decimal")]
                    last_audit_seq: u64,
                    outcome: ResultOutcome,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::Result {
                    dispatch_id: dispatch_id(envelope_dispatch)?,
                    result_id: wire.result_id,
                    last_audit_seq: wire.last_audit_seq,
                    outcome: wire.outcome,
                }
            }
            kind::RESULT_COMMITTED => {
                #[derive(Deserialize)]
                struct Wire {
                    result_id: String,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::ResultCommitted {
                    dispatch_id: dispatch_id(envelope_dispatch)?,
                    result_id: wire.result_id,
                }
            }
            kind::CANCEL => {
                #[derive(Deserialize)]
                struct Wire {
                    command_id: String,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::Cancel {
                    command_id: wire.command_id,
                    dispatch_id: dispatch_id(envelope_dispatch)?,
                }
            }
            kind::STOPPED => {
                #[derive(Deserialize)]
                struct Wire {
                    sealed: bool,
                    #[serde(with = "flow_journal::decimal")]
                    last_audit_seq: u64,
                    pending_operation: Option<String>,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::Stopped {
                    dispatch_id: dispatch_id(envelope_dispatch)?,
                    sealed: wire.sealed,
                    last_audit_seq: wire.last_audit_seq,
                    pending_operation: wire.pending_operation,
                }
            }
            kind::HEARTBEAT => {
                #[derive(Deserialize)]
                struct Wire {
                    phase: String,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::Heartbeat {
                    dispatch_id: envelope_dispatch.map(str::to_owned),
                    phase: wire.phase,
                }
            }
            kind::OBSERVABILITY_BATCH => {
                #[derive(Deserialize)]
                struct Wire {
                    lines: Vec<ObservationLine>,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::ObservabilityBatch {
                    dispatch_id: envelope_dispatch.map(str::to_owned),
                    lines: wire.lines,
                }
            }
            kind::REJECT => {
                #[derive(Deserialize)]
                struct Wire {
                    reason: String,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::Reject {
                    reason: wire.reason,
                }
            }
            kind::ROUTED => {
                #[derive(Deserialize)]
                struct Wire {
                    envelope: RouteEnvelope,
                    inner: Value,
                }
                let wire: Wire = body.parse(type_name)?;
                let inner = Message::from_envelope(wire.inner)
                    .map_err(|e| ProtocolError::InvalidPayload(format!("Routed inner: {e}")))?;
                if !super::remote::routable(&inner) {
                    return Err(ProtocolError::InvalidPayload(
                        "Routed inner must be a business message".into(),
                    ));
                }
                Message::Routed {
                    envelope: wire.envelope,
                    inner: Box::new(inner),
                }
            }
            kind::AGENT_HELLO => {
                #[derive(Deserialize)]
                struct Wire {
                    agent_boot_id: String,
                    build: String,
                    capabilities: Vec<String>,
                    #[serde(with = "decimal_u32")]
                    slots: u32,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::AgentHello {
                    agent_boot_id: wire.agent_boot_id,
                    build: wire.build,
                    capabilities: wire.capabilities,
                    slots: wire.slots,
                }
            }
            kind::AGENT_WELCOME => {
                #[derive(Deserialize)]
                struct Wire {
                    journal_id: String,
                    #[serde(with = "flow_journal::decimal")]
                    master_epoch: u64,
                    link_session_id: String,
                    agent_id: String,
                    #[serde(with = "flow_journal::decimal")]
                    heartbeat_ms: u64,
                    #[serde(with = "flow_journal::decimal")]
                    agent_window_bytes: u64,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::AgentWelcome {
                    journal_id: wire.journal_id,
                    master_epoch: wire.master_epoch,
                    link_session_id: wire.link_session_id,
                    agent_id: wire.agent_id,
                    heartbeat_ms: wire.heartbeat_ms,
                    agent_window_bytes: wire.agent_window_bytes,
                }
            }
            kind::CAPACITY_REPORT => {
                #[derive(Deserialize)]
                struct Wire {
                    capacity: Capacity,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::CapacityReport {
                    capacity: wire.capacity,
                }
            }
            kind::BIND_EXECUTOR => {
                #[derive(Deserialize)]
                struct Wire {
                    executor_id: String,
                    dispatch_id: String,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::BindExecutor {
                    executor_id: wire.executor_id,
                    dispatch_id: wire.dispatch_id,
                }
            }
            kind::BIND_EXECUTOR_ACK => {
                #[derive(Deserialize)]
                struct Wire {
                    executor_id: String,
                    executor_boot_id: String,
                    dispatch_id: String,
                    ok: bool,
                    #[serde(default)]
                    error: Option<String>,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::BindExecutorAck {
                    executor_id: wire.executor_id,
                    executor_boot_id: wire.executor_boot_id,
                    dispatch_id: wire.dispatch_id,
                    ok: wire.ok,
                    error: wire.error,
                }
            }
            kind::RESUME => {
                #[derive(Deserialize)]
                struct Wire {
                    items: Vec<ResumeItem>,
                    #[serde(with = "decimal_u32")]
                    page: u32,
                    more: bool,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::Resume {
                    items: wire.items,
                    page: wire.page,
                    more: wire.more,
                }
            }
            kind::RESUME_REPLY => {
                #[derive(Deserialize)]
                struct Wire {
                    decisions: Vec<ResumeDecision>,
                    #[serde(with = "decimal_u32")]
                    page: u32,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::ResumeReply {
                    decisions: wire.decisions,
                    page: wire.page,
                }
            }
            kind::DRAIN => {
                #[derive(Deserialize)]
                struct Wire {
                    #[serde(with = "flow_journal::decimal")]
                    grace_ms: u64,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::Drain {
                    grace_ms: wire.grace_ms,
                }
            }
            kind::DRAIN_COMPLETE => {
                #[derive(Deserialize)]
                struct Wire {
                    #[serde(with = "decimal_u32")]
                    drained: u32,
                }
                let wire: Wire = body.parse(type_name)?;
                Message::DrainComplete {
                    drained: wire.drained,
                }
            }
            other => {
                return Err(ProtocolError::UnknownMessage(other.to_string()));
            }
        };
        Ok(message)
    }
}

/// 解码 body 来源（T05）：RawValue 借用直析（生产路径，无 Value 树、无
/// body 深拷贝）与 Value（from_envelope 兼容路径）走同一套类型化 Wire。
trait BodySource {
    fn parse<T: serde::de::DeserializeOwned>(&self, type_name: &str) -> Result<T>;
}

impl BodySource for Value {
    fn parse<T: serde::de::DeserializeOwned>(&self, type_name: &str) -> Result<T> {
        serde_json::from_value(self.clone())
            .map_err(|e| ProtocolError::InvalidPayload(format!("{type_name}: {e}")))
    }
}

impl BodySource for serde_json::value::RawValue {
    fn parse<T: serde::de::DeserializeOwned>(&self, type_name: &str) -> Result<T> {
        serde_json::from_str(self.get())
            .map_err(|e| ProtocolError::InvalidPayload(format!("{type_name}: {e}")))
    }
}

/// 帧 kind 字节：0 = meta 即完整 JSON 信封（无 raw）；1 = TransferChunk
/// （meta 为不含 bytes 的元数据，raw 承载原始字节）；2 = Routed 信封内层为
/// TransferChunk（远程中继：meta 为不含 bytes 的 Routed 信封，raw 承载内层
/// 原始字节）。
pub const KIND_JSON: u8 = 0;
pub const KIND_TRANSFER_CHUNK: u8 = 1;
pub const KIND_ROUTED_TRANSFER_CHUNK: u8 = 2;

/// TransferChunk 的元数据信封（不含 bytes）：`{"v","type","dispatch_id","body"}`。
fn transfer_chunk_meta(dispatch_id: &str, transfer_id: &str, offset: u64, digest: &str) -> Value {
    let mut envelope = serde_json::Map::new();
    envelope.insert("v".into(), json!(PROTOCOL_VERSION));
    envelope.insert("type".into(), json!(kind::TRANSFER_CHUNK));
    envelope.insert("dispatch_id".into(), json!(dispatch_id));
    envelope.insert(
        "body".into(),
        json!({
            "transfer_id": transfer_id,
            "offset": offset.to_string(),
            "digest": digest,
        }),
    );
    Value::Object(envelope)
}

/// Routed 信封 + 内层 TransferChunk 元数据（远程 raw 承载的中继形态）。
/// 外层 `dispatch_id` 与内层必须一致（路由键 = 载荷归属）：包络自身缺失
/// 时以内层为准，保证本端编码出的帧一定能过解码端的一致性校验。
fn routed_transfer_meta(
    envelope: &super::remote::RouteEnvelope,
    inner_meta: Value,
    inner_dispatch_id: &str,
) -> Value {
    let mut outer = serde_json::Map::new();
    outer.insert("v".into(), json!(PROTOCOL_VERSION));
    outer.insert("type".into(), json!(kind::ROUTED));
    outer.insert("dispatch_id".into(), json!(inner_dispatch_id));
    outer.insert(
        "body".into(),
        json!({
            "envelope": envelope,
            "inner": inner_meta,
        }),
    );
    Value::Object(outer)
}

/// 从 TransferChunk 元数据信封 + blob 还原消息。
/// 元数据必须自带版本与类型标识（与 kind=1 分支同等校验），dispatch_id
/// 必填——静默补空串会让路由键与载荷归属分离。
fn transfer_chunk_from_meta(envelope: &Value, blob: &[u8]) -> Result<Message> {
    if blob.len() > super::contract::TRANSFER_CHUNK_BYTES {
        return Err(ProtocolError::FrameTooLarge(
            blob.len(),
            super::contract::TRANSFER_CHUNK_BYTES,
        ));
    }
    let version = envelope.get("v").and_then(Value::as_u64).unwrap_or(0);
    if version != PROTOCOL_VERSION as u64 {
        return Err(ProtocolError::Malformed(format!(
            "transfer meta version {version} not supported"
        )));
    }
    if envelope.get("type").and_then(Value::as_str) != Some(kind::TRANSFER_CHUNK) {
        return Err(ProtocolError::Malformed(
            "transfer meta type mismatch".into(),
        ));
    }
    let dispatch_id = envelope
        .get("dispatch_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| ProtocolError::InvalidPayload("transfer meta dispatch_id".into()))?
        .to_string();
    #[derive(Deserialize)]
    struct BodyW {
        transfer_id: String,
        #[serde(with = "flow_journal::decimal")]
        offset: u64,
        digest: String,
    }
    let body: BodyW = serde_json::from_value(envelope.get("body").cloned().unwrap_or(Value::Null))
        .map_err(|e| ProtocolError::Malformed(format!("transfer meta: {e}")))?;
    Ok(Message::TransferChunk {
        dispatch_id,
        transfer_id: body.transfer_id,
        offset: body.offset,
        bytes: blob.to_vec(),
        digest: body.digest,
    })
}

impl Message {
    /// 帧载荷拆分（T12）：返回 `(kind, JSON meta, raw blob)`。只有带
    /// 二进制体的类型（TransferChunk）走 raw；其余类型 kind=0、meta 即
    /// 完整 v4 信封、blob 为空。内存表示与业务路径完全一致（单一业务
    /// 逻辑，无 v3/v4 双轨）。
    pub fn to_frame_parts(&self) -> Result<(u8, Vec<u8>, Vec<u8>)> {
        match self {
            Message::TransferChunk {
                dispatch_id,
                transfer_id,
                offset,
                bytes,
                digest,
            } => {
                let meta = serde_json::to_vec(&transfer_chunk_meta(
                    dispatch_id,
                    transfer_id,
                    *offset,
                    digest,
                ))
                .map_err(|e| ProtocolError::Malformed(format!("meta: {e}")))?;
                Ok((KIND_TRANSFER_CHUNK, meta, bytes.clone()))
            }
            // 远程中继：Routed 内层是 TransferChunk 时同样走 raw（meta 的
            // inner 为不含 bytes 的 TransferChunk 元数据，blob 承载内层字节）。
            Message::Routed { envelope, inner }
                if matches!(**inner, Message::TransferChunk { .. }) =>
            {
                let Message::TransferChunk {
                    dispatch_id,
                    transfer_id,
                    offset,
                    bytes,
                    digest,
                } = &**inner
                else {
                    unreachable!()
                };
                let meta = routed_transfer_meta(
                    envelope,
                    transfer_chunk_meta(dispatch_id, transfer_id, *offset, digest),
                    dispatch_id,
                );
                let meta = serde_json::to_vec(&meta)
                    .map_err(|e| ProtocolError::Malformed(format!("meta: {e}")))?;
                Ok((KIND_ROUTED_TRANSFER_CHUNK, meta, bytes.clone()))
            }
            other => {
                let meta = serde_json::to_vec(&other.to_envelope())
                    .map_err(|e| ProtocolError::Malformed(format!("envelope: {e}")))?;
                Ok((KIND_JSON, meta, Vec::new()))
            }
        }
    }

    /// 帧载荷重组：kind/meta/blob → 消息。
    ///
    /// 校验对称性（v4 契约「明确拒绝，不降级」）：三个 kind 的同构字段
    /// （版本 `v`、meta `type` 与 kind 一致、dispatch_id 必填）必须同等
    /// 校验；kind=0 不得携带 raw blob；kind=2 的外层 meta、body.envelope
    /// 与内层三处 dispatch_id 必须一致（路由键 = 载荷归属）。
    pub fn from_frame_parts(kind_byte: u8, meta: &[u8], blob: &[u8]) -> Result<Self> {
        match kind_byte {
            KIND_JSON => {
                if !blob.is_empty() {
                    return Err(ProtocolError::Malformed(
                        "JSON frame must not carry a raw blob".into(),
                    ));
                }
                Self::decode_envelope(meta)
            }
            KIND_TRANSFER_CHUNK => {
                #[derive(Deserialize)]
                struct MetaW {
                    v: u64,
                    #[serde(rename = "type")]
                    kind: String,
                    dispatch_id: String,
                    body: BodyW,
                }
                #[derive(Deserialize)]
                struct BodyW {
                    transfer_id: String,
                    #[serde(with = "flow_journal::decimal")]
                    offset: u64,
                    digest: String,
                }
                let wire: MetaW = serde_json::from_slice(meta)
                    .map_err(|e| ProtocolError::Malformed(format!("v4 meta: {e}")))?;
                if wire.v != PROTOCOL_VERSION as u64 {
                    return Err(ProtocolError::Malformed(format!(
                        "protocol version {} not supported",
                        wire.v
                    )));
                }
                if wire.kind != kind::TRANSFER_CHUNK {
                    return Err(ProtocolError::Malformed(format!(
                        "v4 kind {} mismatches meta type {}",
                        kind_byte, wire.kind
                    )));
                }
                if blob.len() > super::contract::TRANSFER_CHUNK_BYTES {
                    return Err(ProtocolError::FrameTooLarge(
                        blob.len(),
                        super::contract::TRANSFER_CHUNK_BYTES,
                    ));
                }
                Ok(Message::TransferChunk {
                    dispatch_id: wire.dispatch_id,
                    transfer_id: wire.body.transfer_id,
                    offset: wire.body.offset,
                    bytes: blob.to_vec(),
                    digest: wire.body.digest,
                })
            }
            KIND_ROUTED_TRANSFER_CHUNK => {
                #[derive(Deserialize)]
                struct MetaW {
                    v: u64,
                    #[serde(rename = "type")]
                    kind: String,
                    dispatch_id: String,
                    body: BodyW,
                }
                #[derive(Deserialize)]
                struct BodyW {
                    envelope: super::remote::RouteEnvelope,
                    inner: serde_json::Value,
                }
                let wire: MetaW = serde_json::from_slice(meta)
                    .map_err(|e| ProtocolError::Malformed(format!("routed meta: {e}")))?;
                if wire.v != PROTOCOL_VERSION as u64 {
                    return Err(ProtocolError::Malformed(format!(
                        "protocol version {} not supported",
                        wire.v
                    )));
                }
                if wire.kind != kind::ROUTED {
                    return Err(ProtocolError::Malformed(format!(
                        "frame kind {kind_byte} mismatches meta type {}",
                        wire.kind
                    )));
                }
                let inner = transfer_chunk_from_meta(&wire.body.inner, blob)?;
                let inner_id = inner
                    .dispatch_id()
                    .expect("transfer meta dispatch_id validated non-empty");
                // 路由键一致性：外层 meta 与 body.envelope 的 dispatch_id 都
                // 必须等于内层载荷的归属。任一处缺失/错位都拒绝——否则路由
                // 键与载荷归属可被分离构造，包络校验形同虚设。
                if wire.dispatch_id != inner_id
                    || wire.body.envelope.dispatch_id.as_deref() != Some(inner_id)
                {
                    return Err(ProtocolError::InvalidPayload(
                        "routed transfer dispatch_id mismatch".into(),
                    ));
                }
                if !super::remote::routable(&inner) {
                    return Err(ProtocolError::InvalidPayload(
                        "Routed inner must be a business message".into(),
                    ));
                }
                Ok(Message::Routed {
                    envelope: wire.body.envelope,
                    inner: Box::new(inner),
                })
            }
            other => Err(ProtocolError::Malformed(format!(
                "unknown frame kind {other}"
            ))),
        }
    }
}

fn body_value(value: impl Serialize) -> Option<Value> {
    serde_json::to_value(value).ok()
}

/// 通道标识。v4 全部承载都是单流：帧头只有 `u32 body_len | u8 kind |
/// u32 meta_len`，没有通道标签字节——通道由消息类型声明，只作为消息
/// 分类与预算上限的依据（control 禁大载荷、data 独立预算）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    Control,
    Data,
}

#[cfg(test)]
mod strict_bind_ack_tests {
    use super::*;

    #[test]
    fn bind_ack_requires_explicit_outcome() {
        let mut message = serde_json::json!({"v":PROTOCOL_VERSION,"type":"BindExecutorAck","body":{"executor_id":"e","executor_boot_id":"b","dispatch_id":"d"}});
        assert!(Message::from_envelope(message.clone()).is_err());
        message["body"]["ok"] = Value::Bool(false);
        assert!(matches!(
            Message::from_envelope(message).unwrap(),
            Message::BindExecutorAck { ok: false, .. }
        ));
    }
}
