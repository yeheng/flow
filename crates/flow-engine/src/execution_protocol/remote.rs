//! 远程路由层（三期 §1.2）：agent/executor 双层身份、BindExecutor、
//! Resume/ResumeReply 与容量报告。
//!
//! 远程 transport 复用长度帧与业务消息；外层增加路由包络
//! `agent_id / agent_boot_id / link_session_id / executor_id /
//! executor_boot_id / dispatch_id`，内层 [`Message::Routed`] 保留任务消息
//! 与审计原始编码。agent 是受信任中继：不产生权威 ACK/Permit/
//! ResultCommitted，不推进 DAG，不写权威状态。
//!
//! 守护的核心不变量：路由身份由**已确认的绑定关系**决定，执行器与
//! agent 都不能自报机器身份来获得路由权；断线重连后只有通过 Resume
//! 对账的派发才允许继续，不透明重跑。

use serde::{Deserialize, Serialize};

use super::contract::IDENTITY_MAX_BYTES;
use super::message::Message;

/// 路由包络：把一帧业务消息固定到「上联会话 + 执行器 + 派发」。
/// agent 将本地 dispatch 归属映射到已确认路由；主进程按包络校验来源，
/// 不信执行器/agent 自报机器身份。重建会话可替换经验证的包络，不能修改
/// dispatch、audit_seq 或记录内容。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RouteEnvelope {
    /// 上联 agent 标识。
    pub agent_id: String,
    /// agent 本次启动标识（区分重连后的新任期）。
    pub agent_boot_id: String,
    /// 上联会话标识。
    pub link_session_id: String,
    /// 目标执行器标识。
    pub executor_id: String,
    /// 执行器本次启动标识。
    pub executor_boot_id: String,
    /// 归属派发（可空：会话级消息如心跳没有派发归属）。
    #[serde(default)]
    pub dispatch_id: Option<String>,
}

/// agent 能力/资源报告（三期 §1.6）：主进程按已分配资源扣减，报告有延迟
/// 也不能重复超配。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Capacity {
    /// 本机受控执行槽位总数。
    pub slots: u32,
    /// 当前在飞派发数（agent 视角）。
    pub in_flight: u32,
    /// 待转发字节（有界队列占用，计入 agent 实际 RSS 预算）。
    pub pending_bytes: u64,
    /// 可用内存 MiB（粗粒度；仅能力匹配用）。
    pub memory_mib: u32,
    /// agent 支持的能力（与执行器能力分别协商，不冒充子进程）。
    pub capabilities: Vec<String>,
}

/// Resume 清单项的本地任务状态。闭集枚举上 wire（postcard 变体索引），
/// 非法变体在解码层即拒——字符串当枚举用时的拼写错/越界值不可能混进来。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumeState {
    /// 执行器仍在跑（无固定结果）。
    Executing,
    /// 已停止但未封口。
    Stopped,
    /// 已生成固定结果（result_id / result_last_audit_seq 有值）。
    ResultReady,
    /// 执行器进程已退出。
    Exited,
}

/// Resume 清单项（三阶段）：重连后 agent 上报本地未决任务事实。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResumeItem {
    /// 派发标识（对账主键）。
    pub dispatch_id: String,
    /// 执行器启动标识（用于确认是不是同一份执行事实）。
    pub executor_boot_id: String,
    /// 本地任务状态。
    pub state: ResumeState,
    /// 最后已获主进程确认的审计序号（agent 观察值；主日志游标优先）。
    pub durable_audit_seq: u64,
    /// 已生成未确认的固定 result_id（若有）。
    #[serde(default)]
    pub result_id: Option<String>,
    /// 结果最后审计序号（result_ready 时）。
    #[serde(default)]
    pub result_last_audit_seq: Option<u64>,
    /// 是否丢失未确认数据 / 曾强制终止。
    pub lost_data: bool,
    /// 是否曾被强制终止（终止后不能再信任其结果）。
    pub forced_kill: bool,
}

/// ResumeReply 裁决动作（三期 §1.4 表）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ResumeAction {
    /// 结果已在权威日志；返回原提交确认，不再执行。
    AlreadyCommitted,
    /// 主进程给出真实 durable_audit_seq；仅补传其后仍保有的记录，
    /// 不授权新外部操作。
    UploadOnly { durable_audit_seq: u64 },
    /// 派发仍有效且执行器已生成封口结果；提交须过二期 §3.6 屏障。
    SubmitExistingResult,
    /// 持久取消/派发废止；终止旧执行，只收允许的迟到审计。
    CancelAndDrain,
    /// boot/任期变化、缺数据或外部操作未知；记录事实进入恢复/人工核对，
    /// 不透明重跑。
    ReconcileRequired,
}

/// 单个派发的对账裁决（主进程 → agent）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResumeDecision {
    /// 被裁决的派发标识。
    pub dispatch_id: String,
    /// 裁决动作。
    pub action: ResumeAction,
}

/// agent 上联能力目录（与执行器能力分别协商）。
///
/// 为什么要与执行器能力分开：agent 是中继，不执行用户代码；把两者
/// 混在一张目录里会让「agent 声称会执行」被误当成执行能力。
pub mod capability {
    /// 主↔agent 与 agent↔executor 能力分别协商；这是 agent 上联能力
    /// （中继/资源管理），不是执行能力。
    pub const RELAY: &str = "relay";
    /// agent 能上报资源占用（主进程据此扣减已分配资源）。
    pub const RESOURCE_REPORT: &str = "resource_report";
    /// agent 能断线重连并参与 Resume 对账。
    pub const RECONNECT: &str = "reconnect";
    /// agent 上联的必需能力目录（缺任一项即拒绝上联）。
    pub const REQUIRED: &[&str] = &[RELAY, RESOURCE_REPORT, RECONNECT];
}

/// 校验身份字段（与本地握手同规则）。
///
/// 返回 bool 而不是 Result：调用方用它做过滤判断，不需要区分失败原因。
pub fn validate_route_identity(value: &str) -> bool {
    !value.is_empty() && value.len() <= IDENTITY_MAX_BYTES
}

/// 内层消息必须是可路由的业务帧（禁止嵌套 Routed / 上联管理帧 /
/// 握手帧）。`Reject` 与 `Heartbeat` 允许：执行器的任务失败通知与心跳
/// 要经 agent 中继给主进程。
///
/// 这是一道**结构**闸门：嵌套 Routed 会让路由层自指（包络无限套娃），
/// 握手帧走中继则会绕过主进程侧的版本门与令牌校验。
pub fn routable(inner: &Message) -> bool {
    !matches!(
        inner,
        Message::Routed { .. }
            | Message::Hello { .. }
            | Message::Welcome { .. }
            | Message::Ready
            | Message::AgentHello { .. }
            | Message::AgentWelcome { .. }
            | Message::CapacityReport { .. }
            | Message::BindExecutor { .. }
            | Message::BindExecutorAck { .. }
            | Message::Resume { .. }
            | Message::ResumeReply { .. }
            | Message::Drain { .. }
            | Message::DrainComplete { .. }
    )
}

/// Resume 清单分页上限（控制通道小消息约束）。
pub const RESUME_PAGE_MAX: usize = 128;
