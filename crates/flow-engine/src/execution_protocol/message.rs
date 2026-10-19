//! IPC 消息目录（postcard 单流）。
//!
//! 物理帧为 `u32 body_len | postcard(Message)`：整个 [`Message`] 枚举经
//! postcard（LE varint）编解码，二进制体（TransferChunk.bytes）以原始
//! 字节承载，变体索引即帧 kind——旧版 JSON 信封、三种帧 kind 与十进制
//! 字符串（JS 安全整数戒律）均已删除：本线路两端都是 Rust，且用户
//! JS 摸到的是节点输入值，不是协议帧。
//!
//! 版本控制：每帧不自带版本号，握手帧携带 [`PROTOCOL_VERSION`] 并在
//! 握手处双端硬校验：执行器侧 [`Message::Hello`]/[`Message::Welcome`]，
//! agent 上联侧 [`Message::AgentHello`]/[`Message::AgentWelcome`]。任一
//! 侧不匹配即明确拒绝——跨版本配对在握手处收口，不依赖逐帧校验。
//! postcard 变体索引是位置编码，缺版本门的跨版本配对会静默错解成
//! 另一个消息类型，这是最难查的协议 bug 形态。
//!
//! 审计记录的**计费口径**仍用 JSON：[`record_bytes`] 是主进程与执行器
//! 共用的确定性编码长度函数（窗口预算与幂等比对），与 wire 编码无关。

use flow_journal::{StoredValue, ValueRef};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use super::contract::PROTOCOL_VERSION;
use super::remote::{Capacity, ResumeDecision, ResumeItem, RouteEnvelope};

/// `serde_json::Value` 的 wire 承载（postcard 路径）：Value 的 Deserialize
/// 走 `deserialize_any`（自描述格式专用），postcard 永不支持。binary 格式
/// 下以规范 JSON 字节承载——这些字段（审计 payload、节点定义、历史
/// outcome、input_mapping）本就是 journal-JSON 语义，两端共用同一编码；
/// human-readable 格式（journal/诊断 JSON）保持 Value 原生形状不变。
pub mod wire_json {
    use super::*;

    pub fn serialize<S: Serializer>(value: &Value, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            return value.serialize(serializer);
        }
        serde_json::to_vec(value)
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Value, D::Error> {
        if deserializer.is_human_readable() {
            return Value::deserialize(deserializer);
        }
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        serde_json::from_slice(&bytes).map_err(serde::de::Error::custom)
    }

    /// `Option<Value>` 字段的同规则承载（None/Some 判别由 serde option 原生
    /// 处理；Some 的内层走 [`super::wire_json`] 的双路径）。
    pub mod option {
        use super::*;

        pub fn serialize<S: Serializer>(
            value: &Option<Value>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            if serializer.is_human_readable() {
                return value.serialize(serializer);
            }
            #[derive(Serialize)]
            struct Wrapper(#[serde(with = "super")] Value);
            match value {
                Some(value) => serializer.serialize_some(&Wrapper(value.clone())),
                None => serializer.serialize_none(),
            }
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<Value>, D::Error> {
            if deserializer.is_human_readable() {
                return Option::<Value>::deserialize(deserializer);
            }
            struct Visitor;
            impl<'de> serde::de::Visitor<'de> for Visitor {
                type Value = Option<Value>;
                fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    formatter.write_str("option of json value")
                }
                fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                    Ok(None)
                }
                fn visit_some<D: Deserializer<'de>>(
                    self,
                    deserializer: D,
                ) -> Result<Self::Value, D::Error> {
                    super::deserialize(deserializer).map(Some)
                }
            }
            deserializer.deserialize_option(Visitor)
        }
    }
}

/// `StoredValue` 的 wire 承载（postcard 路径）：journal 的 StoredValue 是
/// internally tagged（JSON 按 `type`/`value` 判别），其反序列化经 serde 的
/// Content 缓冲走 `deserialize_any`——postcard 永不支持。binary 格式下用
/// 外部 tagged 镜像（inline = JSON 字节，ref = ValueRef），转换走
/// `StoredValue::inline` 保持尺寸校验；human-readable 格式原样透传。
pub mod wire_stored {
    use super::*;

    #[derive(Serialize, Deserialize)]
    enum Wire {
        Inline(Vec<u8>),
        Ref(ValueRef),
    }

    pub fn serialize<S: Serializer>(value: &StoredValue, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            return value.serialize(serializer);
        }
        let wire = match value {
            StoredValue::Inline(value) => {
                Wire::Inline(serde_json::to_vec(value).map_err(serde::ser::Error::custom)?)
            }
            StoredValue::Ref(reference) => Wire::Ref(reference.clone()),
        };
        wire.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<StoredValue, D::Error> {
        if deserializer.is_human_readable() {
            return StoredValue::deserialize(deserializer);
        }
        match Wire::deserialize(deserializer)? {
            Wire::Inline(bytes) => {
                let value: Value =
                    serde_json::from_slice(&bytes).map_err(serde::de::Error::custom)?;
                StoredValue::inline(value).map_err(serde::de::Error::custom)
            }
            Wire::Ref(reference) => {
                reference.validate().map_err(serde::de::Error::custom)?;
                Ok(StoredValue::Ref(reference))
            }
        }
    }

    /// `Option<StoredValue>` 字段的同规则承载。
    pub mod option {
        use super::*;

        pub fn serialize<S: Serializer>(
            value: &Option<StoredValue>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            if serializer.is_human_readable() {
                return value.serialize(serializer);
            }
            #[derive(Serialize)]
            struct Wrapper(#[serde(with = "super")] StoredValue);
            match value {
                Some(value) => serializer.serialize_some(&Wrapper(value.clone())),
                None => serializer.serialize_none(),
            }
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Option<StoredValue>, D::Error> {
            if deserializer.is_human_readable() {
                return Option::<StoredValue>::deserialize(deserializer);
            }
            struct Visitor;
            impl<'de> serde::de::Visitor<'de> for Visitor {
                type Value = Option<StoredValue>;
                fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    formatter.write_str("option of stored value")
                }
                fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                    Ok(None)
                }
                fn visit_some<D: Deserializer<'de>>(
                    self,
                    deserializer: D,
                ) -> Result<Self::Value, D::Error> {
                    super::deserialize(deserializer).map(Some)
                }
            }
            deserializer.deserialize_option(Visitor)
        }
    }
}

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
/// 确定性 JSON 编码：字段顺序固定，serde_json 的 BTreeMap 保证键序。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditRecord {
    pub audit_seq: u64,
    pub kind: String,
    #[serde(with = "wire_json")]
    pub payload: Value,
}

impl AuditRecord {
    /// 协议确定性编码字节数（计费口径，见模块文档）。
    pub fn encoded_bytes(&self) -> u64 {
        record_bytes(self.audit_seq, &self.kind, &self.payload)
    }
}

/// 确定性记录计费编码：`{"audit_seq":"N","kind":"…","payload":{…}}`。
/// 仅用于窗口计费与幂等内容比对（两端同一函数），不进 wire。
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
pub enum ResultOutcome {
    /// 正常完成：固定结果描述（小结果 inline，大值为已发送 chunk 的 ValueRef）
    /// 与可选 condition 分支。
    Success {
        #[serde(with = "wire_stored")]
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

/// 等待请求类别。闭集枚举上 wire（postcard 变体索引），解码层对
/// 非法变体直接拒绝——拼写错误与越界值不可能变成另一个意思。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitKind {
    /// 定时等待：`wake_at`（epoch 毫秒）到期后唤醒。
    Delay,
    /// 人工信号等待：run.signal 交付后唤醒。
    Signal,
    /// 子工作流等待（sub_workflow）：子 run 由主进程在结果接受事务
    /// 创建并登记父子关系（执行器不创建子 run）。
    Child,
}

/// 等待请求。`wake_at` 为 epoch 毫秒（delay）；child 等待的 child_run_id
/// 由主进程在结果接受事务创建并登记（执行器不创建子 run）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WaitRequest {
    #[serde(rename = "type")]
    pub kind: WaitKind,
    #[serde(default)]
    pub wake_at: Option<i64>,
    #[serde(default, with = "wire_stored::option")]
    pub output: Option<StoredValue>,
    /// sub_workflow 等待：子 run 由主进程在结果接受事务创建。
    #[serde(default)]
    pub workflow_id: Option<String>,
    #[serde(default, with = "wire_json::option")]
    pub input_mapping: Option<Value>,
}

/// `Routed` 包络的最大嵌套层数。合法拓扑（agent 对业务帧 wrap 一次）只
/// 需要 1 层；留余量到 8。超过即拒绝——深度门必须先于解码展开。
pub const ROUTED_MAX_DEPTH: u32 = 8;

thread_local! {
    /// `Routed.inner` 的当前解码深度。解码全程同步（无 await 点），
    /// 线程本地计数不会跨任务交错。
    static ROUTED_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// `Routed.inner` 字段的反序列化入口：每进入一层包络深度 +1，超限直接
/// 报错，不展开 `Box<Message>`。成功解码的值深度 ≤ [`ROUTED_MAX_DEPTH`]，
/// 派生的 Drop/Debug/frame_limit 递归因此同样有界。
fn deserialize_bounded_inner<'de, D>(deserializer: D) -> Result<Box<Message>, D::Error>
where
    D: Deserializer<'de>,
{
    let depth = ROUTED_DEPTH.with(|c| c.get());
    if depth >= ROUTED_MAX_DEPTH {
        return Err(serde::de::Error::custom(format!(
            "routed nesting exceeds {} levels",
            ROUTED_MAX_DEPTH
        )));
    }
    ROUTED_DEPTH.with(|c| c.set(depth + 1));
    let result = Box::<Message>::deserialize(deserializer);
    ROUTED_DEPTH.with(|c| c.set(depth));
    result
}

/// 统一消息目录（28 个变体）。通道与方向约束见二期 §3.4；帧级预算按
/// 消息类型分类（frame.rs 的 frame_limit：大载荷类型走 DATA 上限）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Message {
    /// control，子→主。启动检查第一步；不授予任务执行权。`version`
    /// 必须等于主进程的 [`PROTOCOL_VERSION`]，不匹配即拒绝（版本门
    /// 集中在握手，逐帧不携带版本号）。
    Hello {
        version: u32,
        boot_id: String,
        build: String,
        capabilities: Vec<String>,
        /// 回连承载的会话令牌（本地 socket 回连防本机抢连；unix
        /// socketpair 走 FD 能力不需要，为 None）。主进程在 spawn 时经
        /// env 注入，Hello 里回验——抢连者在毫秒窗口内拿不到该值。
        session_token: Option<String>,
    },
    /// control，主→子。选定会话身份、限额与超时；`version` 同样必检。
    Welcome {
        version: u32,
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
    /// `task` 装箱：ExecuteTask 是枚举里最大的变体（含节点定义与前驱声明），
    /// 不装则整个 Message 被拍到 440+ 字节、每个 clone/排队都放大拷贝；
    /// postcard 对 Box 透明，wire 格式不变。
    Execute {
        command_id: String,
        dispatch_id: String,
        task: Box<ExecuteTask>,
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
    /// control，子→主。请求外部操作许可；请求体经 data 通道分块传输。
    /// 携带请求指纹；主进程校验完整内容后才授权。
    RequestOperation {
        dispatch_id: String,
        command_id: String,
        operation_id: String,
        request_fingerprint: String,
        /// 请求字节的 data 通道 transfer 标识（op:<operation_id>）。
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
    /// data，双向。输入分块传输：offset + raw bytes + digest。
    /// `dispatch_id` 是路由键：远程中继按它把帧包进 Routed 信封发往归属
    /// 执行器（没有它，run_session 查不到路由会静默丢帧——Ref 输入在
    /// remote 模式下全部黑洞）。
    TransferChunk {
        dispatch_id: String,
        transfer_id: String,
        offset: u64,
        /// 原始字节（postcard 原生承载，无 base64 膨胀）。
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
    /// 远程路由帧（三阶段）：外层包络 + 内层业务消息。postcard 下
    /// 内层 TransferChunk 的字节由嵌套枚举原生承载——旧版专用
    /// routed-transfer-chunk 帧与其三处 dispatch_id 一致性校验
    /// 随枚举嵌套自然消失。
    ///
    /// `inner` 的反序列化带递归深度门（[`ROUTED_MAX_DEPTH`]）：嵌套在
    /// wire 上每层只占约 7 字节，1 MiB 的合法长度 data 帧可承载约 15 万层
    /// `Box<Message>` 递归，postcard 递归下降解码（以及派生的 Drop/Debug/
    /// frame_limit）都会栈溢出——abort，不可捕获。这道门在握手与令牌
    /// 校验之前，是唯一能拦住对等端深度炸弹的位置。
    Routed {
        envelope: RouteEnvelope,
        #[serde(deserialize_with = "deserialize_bounded_inner")]
        inner: Box<Message>,
    },
    /// control，agent→主。上联握手：身份由承载层决定（mTLS 证书 CN /
    /// WSS 凭据映射表），不信客户端自报字段。`version` 必须等于主进程的
    /// [`PROTOCOL_VERSION`]，不匹配即拒绝。
    AgentHello {
        version: u32,
        agent_boot_id: String,
        build: String,
        capabilities: Vec<String>,
        slots: u32,
    },
    /// control，主→agent。选定 link_session、限额与心跳周期（单流上联，
    /// 无第二条 data 连接，也就没有 data 绑定凭据）；`version` 同样必检。
    AgentWelcome {
        version: u32,
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
pub struct ObservationLine {
    pub level: String,
    pub stream: String,
    pub message: String,
}

/// 一次节点派发的输入面。dispatch 必须解析到固定 run/node/attempt/
/// node_execution_id；接收端按连接上下文校验派发归属。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecuteTask {
    pub run_id: String,
    pub node_id: String,
    pub node_execution_id: String,
    pub attempt: u32,
    /// 节点定义（含未展开 params）。控制帧唯一允许的变长项（有界：
    /// dispatch 侧按 CONTROL_MAX_FRAME/2 预检，超限拒绝派发而非杀会话）。
    #[serde(with = "wire_json")]
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
    #[serde(default, with = "wire_json::option")]
    pub previous_outcome: Option<Value>,
    /// 观测开/关（主进程未配置观测存储时关闭，避免无谓传输）。
    pub observability: bool,
}

/// 输入值声明（v2 起控制帧不携带用户值字节）。
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
pub struct PreparedMeta {
    pub engine_build: String,
    pub node_semantics: u32,
    /// 已提交准备参数的值声明（字节走 in:prepared-params 传输）。
    pub params: TaskValue,
}

impl Message {
    pub fn type_name(&self) -> &'static str {
        match self {
            Message::Hello { .. } => "Hello",
            Message::Welcome { .. } => "Welcome",
            Message::Ready => "Ready",
            Message::Execute { .. } => "Execute",
            Message::Accepted { .. } => "Accepted",
            Message::AuditBatch { .. } => "AuditBatch",
            Message::AuditAck { .. } => "AuditAck",
            Message::RequestOperation { .. } => "RequestOperation",
            Message::OperationPermit { .. } => "OperationPermit",
            Message::TransferChunk { .. } => "TransferChunk",
            Message::InputReady { .. } => "InputReady",
            Message::Result { .. } => "Result",
            Message::ResultCommitted { .. } => "ResultCommitted",
            Message::Cancel { .. } => "Cancel",
            Message::Stopped { .. } => "Stopped",
            Message::Heartbeat { .. } => "Heartbeat",
            Message::ObservabilityBatch { .. } => "ObservabilityBatch",
            Message::Reject { .. } => "Reject",
            Message::Routed { .. } => "Routed",
            Message::AgentHello { .. } => "AgentHello",
            Message::AgentWelcome { .. } => "AgentWelcome",
            Message::CapacityReport { .. } => "CapacityReport",
            Message::BindExecutor { .. } => "BindExecutor",
            Message::BindExecutorAck { .. } => "BindExecutorAck",
            Message::Resume { .. } => "Resume",
            Message::ResumeReply { .. } => "ResumeReply",
            Message::Drain { .. } => "Drain",
            Message::DrainComplete { .. } => "DrainComplete",
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

    /// 构造携带当前协议版本的 Hello（执行器侧握手用）。
    pub fn hello(
        boot_id: impl Into<String>,
        build: impl Into<String>,
        capabilities: Vec<String>,
        session_token: Option<String>,
    ) -> Self {
        Message::Hello {
            version: PROTOCOL_VERSION,
            boot_id: boot_id.into(),
            build: build.into(),
            capabilities,
            session_token,
        }
    }

    /// 构造携带当前协议版本的 AgentHello（agent 上联握手用）。
    pub fn agent_hello(
        agent_boot_id: impl Into<String>,
        build: impl Into<String>,
        capabilities: Vec<String>,
        slots: u32,
    ) -> Self {
        Message::AgentHello {
            version: PROTOCOL_VERSION,
            agent_boot_id: agent_boot_id.into(),
            build: build.into(),
            capabilities,
            slots,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 帧内尾随垃圾必须被拒绝：take_from_bytes 的剩余切片非空即协议错误
    /// （防「合法消息 + 附加字节」的帧内走私）。
    #[test]
    fn postcard_decode_rejects_trailing_bytes() {
        let body = postcard::to_allocvec(&Message::Ready).unwrap();
        let mut padded = body.clone();
        padded.extend_from_slice(&[0xDE, 0xAD]);
        let (message, rest) = postcard::take_from_bytes::<Message>(&padded).unwrap();
        assert_eq!(message, Message::Ready);
        assert!(!rest.is_empty(), "postcard 必须把尾随字节留在剩余切片");
    }

    /// 巨型长度声明（String len varint = u64::MAX 量级）必须立即报错，
    /// 不得先按声明长度分配——postcard 的 take_n 先做边界检查。
    #[test]
    fn postcard_decode_rejects_giant_length_claims() {
        // Hello 变体（索引 0）+ version=1 + boot_id 长度声明 u64::MAX
        // varint（9 字节 0xFF 前缀 + 0x3F 终结）。
        let mut body = vec![0u8, 1];
        body.extend_from_slice(&[0xFF; 9][..8]);
        body.push(0x3F);
        assert!(
            postcard::from_bytes::<Message>(&body).is_err(),
            "巨型长度声明必须报错而非分配"
        );
    }

    /// 变体索引越界（目录 28 个变体）必须明确拒绝。
    #[test]
    fn postcard_decode_rejects_unknown_variant_index() {
        assert!(postcard::from_bytes::<Message>(&[28]).is_err());
        assert!(postcard::from_bytes::<Message>(&[200]).is_err());
    }

    /// `Routed` 嵌套深度炸弹：每层包络在 wire 上仅约 7 字节，1 MiB 的
    /// data 帧可承载约 15 万层递归——没有深度门，postcard 递归下降解码
    /// （及派生 Drop/Debug）会栈溢出 abort，且发生在握手/令牌校验之前。
    /// 超过 [`ROUTED_MAX_DEPTH`] 的嵌套必须在解码层被拒。
    #[test]
    fn postcard_decode_rejects_deep_routed_nesting() {
        let envelope = RouteEnvelope {
            agent_id: "agent".into(),
            agent_boot_id: "aboot".into(),
            link_session_id: "link".into(),
            executor_id: "exec".into(),
            executor_boot_id: "eboot".into(),
            dispatch_id: Some("dispatch-1".into()),
        };
        let wrap = |inner: Message| Message::Routed {
            envelope: envelope.clone(),
            inner: Box::new(inner),
        };
        // 恰好在限内（ROUTED_MAX_DEPTH 层）必须可往返。
        let mut bounded = Message::Ready;
        for _ in 0..ROUTED_MAX_DEPTH {
            bounded = wrap(bounded);
        }
        let bytes = postcard::to_allocvec(&bounded).unwrap();
        assert_eq!(postcard::from_bytes::<Message>(&bytes).unwrap(), bounded);
        // 超限一层即拒绝——错误在解码层返回，不是栈溢出。
        let mut deep = wrap(bounded);
        for _ in 0..3 {
            deep = wrap(deep);
        }
        let bytes = postcard::to_allocvec(&deep).unwrap();
        assert!(
            postcard::from_bytes::<Message>(&bytes).is_err(),
            "超过 ROUTED_MAX_DEPTH 的嵌套必须被拒绝"
        );
        // 深度计数在成功/失败解码后都要归零（不污染同线程后续解码）。
        assert_eq!(
            postcard::from_bytes::<Message>(&postcard::to_allocvec(&Message::Ready).unwrap())
                .unwrap(),
            Message::Ready
        );
    }

    /// 闭集枚举（替代字符串状态）的非法变体必须在解码层被拒——字符串当
    /// 枚举用时拼写错/越界值会静默通过，postcard 变体索引不会。
    #[test]
    fn closed_set_enums_reject_unknown_variants() {
        use super::super::remote::ResumeState;
        // WaitKind：Delay/Signal/Child 三个变体。
        assert!(postcard::from_bytes::<WaitKind>(&[0]).is_ok());
        assert!(postcard::from_bytes::<WaitKind>(&[2]).is_ok());
        assert!(postcard::from_bytes::<WaitKind>(&[3]).is_err());
        assert!(postcard::from_bytes::<WaitKind>(&[200]).is_err());
        // ResumeState：Executing/Stopped/ResultReady/Exited 四个变体。
        assert!(postcard::from_bytes::<ResumeState>(&[3]).is_ok());
        assert!(postcard::from_bytes::<ResumeState>(&[4]).is_err());
        assert!(postcard::from_bytes::<ResumeState>(&[200]).is_err());
    }
}
