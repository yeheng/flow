//! IPC 消息目录（二期 §3.3/§3.4，I01 冻结）。
//!
//! 物理帧为 `u32 大端长度 + UTF-8 JSON`；JSON 是一个信封：
//! `{"v":1,"type":"AuditBatch","dispatch_id":"…","body":{…}}`。
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
        /// inline 请求（≤ control 帧上限时使用）；大请求走 transfer。
        request: Option<StoredValue>,
        /// 大请求的 data 通道 transfer 标识；None 表示 inline。
        transfer_id: Option<String>,
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
    /// data，双向。输入分块传输：offset + bytes(base64) + digest。
    TransferChunk {
        transfer_id: String,
        offset: u64,
        /// base64 编码后的原始字节；计费包含编码后的体积。
        bytes: String,
        digest: String,
    },
    /// control，发送方。输入接收器校验并准备完成；不表示新的权威提交，
    /// 不释放审计持久窗口。
    InputReady {
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
    /// 节点定义（含未展开 params）。
    pub node: Value,
    /// run 输入与前驱输出（StoredValue：小值 inline，大值走 transfer）。
    pub input: StoredValue,
    pub predecessors: Vec<(String, StoredValue)>,
    /// 已提交 InputPrepared（重试恢复时携带；执行器跳过求值，
    /// 不重新求时间/随机模板）。
    pub prepared: Option<Value>,
    /// 已提交但缺 Outcome 的操作恢复出的历史 Outcome（重试复用，不重发）。
    pub previous_outcome: Option<Value>,
    /// 观测开/关（主进程未配置观测存储时关闭，避免无谓传输）。
    pub observability: bool,
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
        }
    }

    /// 消息所属通道（二期 §3.4）。方向由收发双方位置决定；transport 按
    /// 此表拒绝放错通道的消息。
    pub fn channel(&self) -> Channel {
        match self {
            Message::TransferChunk { .. }
            | Message::ObservabilityBatch { .. }
            | Message::AuditBatch { .. } => Channel::Data,
            _ => Channel::Control,
        }
    }

    fn dispatch_id(&self) -> Option<&str> {
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
            | Message::Stopped { dispatch_id, .. } => Some(dispatch_id),
            Message::Heartbeat { dispatch_id, .. }
            | Message::ObservabilityBatch { dispatch_id, .. } => dispatch_id.as_deref(),
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
        struct ChunkW {
            transfer_id: String,
            #[serde(with = "flow_journal::decimal")]
            offset: u64,
            bytes: String,
            digest: String,
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
            } => body_value(&HelloW {
                boot_id,
                build,
                capabilities,
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
                request,
                transfer_id,
            } => body_value(serde_json::json!({
                "command_id": command_id,
                "operation_id": operation_id,
                "request_fingerprint": request_fingerprint,
                "request": request,
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
            Message::TransferChunk {
                transfer_id,
                offset,
                bytes,
                digest,
            } => body_value(&ChunkW {
                transfer_id: transfer_id.clone(),
                offset: *offset,
                bytes: bytes.clone(),
                digest: digest.clone(),
            }),
            Message::InputReady {
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

    /// 从信封 JSON 解码。未知类型 / 信封字段错误明确拒绝。
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
        let dispatch_id = |v: &Value| -> Result<String> {
            v["dispatch_id"]
                .as_str()
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
                }
                let wire: Wire = from_body(&body, type_name)?;
                Message::Hello {
                    boot_id: wire.boot_id,
                    build: wire.build,
                    capabilities: wire.capabilities,
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
                let wire: Wire = from_body(&body, type_name)?;
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
                let wire: Wire = from_body(&body, type_name)?;
                Message::Execute {
                    command_id: wire.command_id,
                    dispatch_id: dispatch_id(&value)?,
                    task: wire.task,
                }
            }
            kind::ACCEPTED => {
                #[derive(Deserialize)]
                struct Wire {
                    command_id: String,
                }
                let wire: Wire = from_body(&body, type_name)?;
                Message::Accepted {
                    command_id: wire.command_id,
                    dispatch_id: dispatch_id(&value)?,
                }
            }
            kind::AUDIT_BATCH => {
                #[derive(Deserialize)]
                struct Wire {
                    #[serde(with = "flow_journal::decimal")]
                    first_seq: u64,
                    records: Vec<AuditRecord>,
                }
                let wire: Wire = from_body(&body, type_name)?;
                Message::AuditBatch {
                    dispatch_id: dispatch_id(&value)?,
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
                let wire: Wire = from_body(&body, type_name)?;
                Message::AuditAck {
                    dispatch_id: dispatch_id(&value)?,
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
                    request: Option<StoredValue>,
                    transfer_id: Option<String>,
                }
                let wire: Wire = from_body(&body, type_name)?;
                Message::RequestOperation {
                    dispatch_id: dispatch_id(&value)?,
                    command_id: wire.command_id,
                    operation_id: wire.operation_id,
                    request_fingerprint: wire.request_fingerprint,
                    request: wire.request,
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
                let wire: Wire = from_body(&body, type_name)?;
                Message::OperationPermit {
                    dispatch_id: dispatch_id(&value)?,
                    operation_id: wire.operation_id,
                    permit_id: wire.permit_id,
                    request_fingerprint: wire.request_fingerprint,
                    credential: wire.credential,
                }
            }
            kind::TRANSFER_CHUNK => {
                #[derive(Deserialize)]
                struct Wire {
                    transfer_id: String,
                    #[serde(with = "flow_journal::decimal")]
                    offset: u64,
                    bytes: String,
                    digest: String,
                }
                let wire: Wire = from_body(&body, type_name)?;
                Message::TransferChunk {
                    transfer_id: wire.transfer_id,
                    offset: wire.offset,
                    bytes: wire.bytes,
                    digest: wire.digest,
                }
            }
            kind::INPUT_READY => {
                #[derive(Deserialize)]
                struct Wire {
                    transfer_id: String,
                    #[serde(with = "flow_journal::decimal")]
                    total_bytes: u64,
                    digest: String,
                }
                let wire: Wire = from_body(&body, type_name)?;
                Message::InputReady {
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
                let wire: Wire = from_body(&body, type_name)?;
                Message::Result {
                    dispatch_id: dispatch_id(&value)?,
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
                let wire: Wire = from_body(&body, type_name)?;
                Message::ResultCommitted {
                    dispatch_id: dispatch_id(&value)?,
                    result_id: wire.result_id,
                }
            }
            kind::CANCEL => {
                #[derive(Deserialize)]
                struct Wire {
                    command_id: String,
                }
                let wire: Wire = from_body(&body, type_name)?;
                Message::Cancel {
                    command_id: wire.command_id,
                    dispatch_id: dispatch_id(&value)?,
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
                let wire: Wire = from_body(&body, type_name)?;
                Message::Stopped {
                    dispatch_id: dispatch_id(&value)?,
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
                let wire: Wire = from_body(&body, type_name)?;
                Message::Heartbeat {
                    dispatch_id: value
                        .get("dispatch_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    phase: wire.phase,
                }
            }
            kind::OBSERVABILITY_BATCH => {
                #[derive(Deserialize)]
                struct Wire {
                    lines: Vec<ObservationLine>,
                }
                let wire: Wire = from_body(&body, type_name)?;
                Message::ObservabilityBatch {
                    dispatch_id: value
                        .get("dispatch_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    lines: wire.lines,
                }
            }
            kind::REJECT => {
                #[derive(Deserialize)]
                struct Wire {
                    reason: String,
                }
                let wire: Wire = from_body(&body, type_name)?;
                Message::Reject {
                    reason: wire.reason,
                }
            }
            other => {
                return Err(ProtocolError::UnknownMessage(other.to_string()));
            }
        };
        Ok(message)
    }
}

fn from_body<T: serde::de::DeserializeOwned>(body: &Value, type_name: &str) -> Result<T> {
    serde_json::from_value(body.clone())
        .map_err(|e| ProtocolError::InvalidPayload(format!("{type_name}: {e}")))
}

fn body_value(value: impl Serialize) -> Option<Value> {
    serde_json::to_value(value).ok()
}

/// 通道标识。control 与 data 分离消除流内队头阻塞（二期 §3.1）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    Control,
    Data,
}
