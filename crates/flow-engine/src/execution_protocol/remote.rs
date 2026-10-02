//! 远程路由层（三期 §1.2）：agent/executor 双层身份、BindExecutor、
//! Resume/ResumeReply 与容量报告。
//!
//! 远程 transport 复用二期的长度帧与业务消息；外层增加路由包络
//! `agent_id / agent_boot_id / link_session_id / executor_id /
//! executor_boot_id / dispatch_id`，内层 [`Message::Routed`] 保留任务消息
//! 与审计原始编码。agent 是受信任中继：不产生权威 ACK/Permit/
//! ResultCommitted，不推进 DAG，不写权威状态。

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::contract::IDENTITY_MAX_BYTES;
use super::message::Message;

/// 路由包络：把一帧业务消息固定到「上联会话 + 执行器 + 派发」。
/// agent 将本地 dispatch 归属映射到已确认路由；主进程按包络校验来源，
/// 不信执行器/agent 自报机器身份。重建会话可替换经验证的包络，不能修改
/// dispatch、audit_seq 或记录内容。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteEnvelope {
    pub agent_id: String,
    pub agent_boot_id: String,
    pub link_session_id: String,
    pub executor_id: String,
    pub executor_boot_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dispatch_id: Option<String>,
}

/// agent 能力/资源报告（三期 §1.6）：主进程按已分配资源扣减，报告有延迟
/// 也不能重复超配。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capacity {
    /// 本机受控执行槽位总数。
    pub slots: u32,
    /// 当前在飞派发数（agent 视角）。
    pub in_flight: u32,
    /// 待转发字节（有界队列占用，计入 agent 实际 RSS 预算）。
    #[serde(with = "flow_journal::decimal")]
    pub pending_bytes: u64,
    /// 可用内存 MiB（粗粒度；仅能力匹配用）。
    pub memory_mib: u32,
    /// agent 支持的能力（与执行器能力分别协商，不冒充子进程）。
    pub capabilities: Vec<String>,
}

/// Resume 清单项（三期 §1.4）：重连后 agent 上报本地未决任务事实。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeItem {
    pub dispatch_id: String,
    pub executor_boot_id: String,
    /// 本地任务状态。
    #[serde(rename = "type")]
    pub state: String, // executing | stopped | result_ready | exited
    /// 最后已获主进程确认的审计序号（agent 观察值；主日志游标优先）。
    #[serde(with = "flow_journal::decimal")]
    pub durable_audit_seq: u64,
    /// 已生成未确认的固定 result_id（若有）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_id: Option<String>,
    /// 结果最后审计序号（result_ready 时）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "super::message::decimal_u64_opt")]
    pub result_last_audit_seq: Option<u64>,
    /// 是否丢失未确认数据 / 曾强制终止。
    pub lost_data: bool,
    pub forced_kill: bool,
}

/// ResumeReply 裁决动作（三期 §1.4 表）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ResumeAction {
    /// 结果已在权威日志；返回原提交确认，不再执行。
    AlreadyCommitted,
    /// 主进程给出真实 durable_audit_seq；仅补传其后仍保有的记录，
    /// 不授权新外部操作。
    UploadOnly {
        #[serde(with = "flow_journal::decimal")]
        durable_audit_seq: u64,
    },
    /// 派发仍有效且执行器已生成封口结果；提交须过二期 §3.6 屏障。
    SubmitExistingResult,
    /// 持久取消/派发废止；终止旧执行，只收允许的迟到审计。
    CancelAndDrain,
    /// boot/任期变化、缺数据或外部操作未知；记录事实进入恢复/人工核对，
    /// 不透明重跑。
    ReconcileRequired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumeDecision {
    pub dispatch_id: String,
    pub action: ResumeAction,
}

pub mod capability {
    /// 主↔agent 与 agent↔executor 能力分别协商；这是 agent 上联能力
    /// （中继/资源管理），不是执行能力。
    pub const RELAY: &str = "relay";
    pub const RESOURCE_REPORT: &str = "resource_report";
    pub const RECONNECT: &str = "reconnect";
    pub const REQUIRED: &[&str] = &[RELAY, RESOURCE_REPORT, RECONNECT];
}

/// 校验身份字段（与本地握手同规则）。
pub fn validate_route_identity(value: &str) -> bool {
    !value.is_empty() && value.len() <= IDENTITY_MAX_BYTES
}

/// 内层消息必须是可路由的业务帧（禁止嵌套 Routed / 上联管理帧）。
pub fn routable(inner: &Message) -> bool {
    !matches!(
        inner,
        Message::Routed { .. }
            | Message::AgentHello { .. }
            | Message::AgentWelcome { .. }
            | Message::DataBind { .. }
            | Message::CapacityReport { .. }
            | Message::BindExecutor { .. }
            | Message::BindExecutorAck { .. }
            | Message::Resume { .. }
            | Message::ResumeReply { .. }
            | Message::Drain { .. }
            | Message::DrainComplete { .. }
    )
}

/// Routed 帧内层信封（保留原始编码：内层 JSON 原样嵌套，不改字段）。
pub fn routed_envelope_value(envelope: &RouteEnvelope, inner: &Message) -> Value {
    serde_json::json!({
        "envelope": envelope,
        "inner": inner.to_envelope(),
    })
}

/// Resume 清单分页上限（控制通道小消息约束）。
pub const RESUME_PAGE_MAX: usize = 128;
