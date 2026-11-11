//! V2 fact reducer. Replaying this module never evaluates templates or executes a node.
//!
//! 本模块是「journal 事实 → 折叠态」的唯一实现：把已落盘的事件按序折叠成
//! 工作流、运行与节点的当前状态，并保证任何一次失败都不会把状态改脏。
//! 它守护的核心不变量是 I5——所有 run 状态迁移只能由本折叠器产生，
//! 不存在第二份状态语义；同时保证「事实已接受但折叠失败」时状态回到折叠前。
//! 没有这一层，SQLite 事务提交成功后内存态就会与权威事实分叉，
//! 崩溃恢复将读到与事实不一致的 run 状态（进而重复派发或漏发外部副作用）。
use flow_journal::{Event, EventKind, StoredValue, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use crate::Definition;

mod undo;
use undo::UndoLog;

/// 折叠层的错误类型：直接复用 journal 的错误族，避免第二套错误码语义。
type Result<T> = flow_journal::Result<T>;

/// 构造一条「事实非法」错误。
///
/// 折叠层拒绝一条事实时用它统一报错形状：消息进入协调循环日志并让事务
/// 整批回滚，调用方不需要区分「形状非法」与「状态冲突」两类错误。
fn invalid(s: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(s.into())
}

// Accept numeric snapshot counters from earlier development logs, emit lossless strings.
// 早期开发日志里的计数器是 JSON 数字，而磁盘契约要求十进制字符串（u64 会超出
// f64 的精确整数区间）。这里只放宽读取侧，写出侧一律用字符串，
// 以免旧日志里的数值被二次编码成不精确的形式。
mod snapshot_decimal {
    use serde::{Deserialize, Deserializer, Serializer};

    /// 快照计数器一律写成十进制字符串，避免 JSON number 的精度损失。
    pub fn serialize<S: Serializer>(n: &u64, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&n.to_string())
    }

    /// 读取时同时接受十进制字符串与旧的 JSON 数字形式。
    ///
    /// 解析失败按自定义错误返回，让调用方看到「计数器不可解析」而不是一个
    /// 语义含糊的默认 0——把损坏的快照当成 0 会让提交水位回退、覆盖已有事实。
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<u64, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Number {
            Text(String),
            Number(u64),
        }
        match Number::deserialize(d)? {
            Number::Number(n) => Ok(n),
            Number::Text(n) => n.parse().map_err(serde::de::Error::custom),
        }
    }
}

/// 通用幂等回执保留窗口：超过即按 lsn 淘汰最旧一半。调度执行身份另由
/// schedule_fires 保留，业务去重不依赖这个有限重试窗口。
const COMMAND_WINDOW: usize = 8192;

/// run 生命周期词汇表的唯一定义在 `flow-dto`；这里只 re-export，
/// 避免出现第二份 `ALL`/`as_str`。
pub use flow_dto::RunStatus;

/// 节点执行状态（`Node.status`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    Pending,
    Running,
    Waiting,
    Succeeded,
    Skipped,
    Failed,
}

impl NodeStatus {
    /// 节点状态的稳定字符串形式（与 `serde` 的 snake_case 重命名一致）。
    ///
    /// 这是对外暴露的稳定字面量，会落进快照、HTTP 响应与事件载荷；
    /// 改字面量等于改协议，必须同步改 `serde` 属性。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Succeeded => "succeeded",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
        }
    }
}

impl std::fmt::Display for NodeStatus {
    /// 显示时复用 `as_str`，保证日志与错误信息里的状态词与协议字面量一致。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 持久等待类别（`Wait.kind`，WaitRegistered 携带）。事件载荷里的裸字符串
/// 比对一律经 [`WaitKind::as_str`]，不写第二份字面量。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitKind {
    Delay,
    Signal,
    Child,
    Retry,
    Uncertain,
    /// 人工权限审批等待（09 §6）：非封口——由 PermissionAsked 折叠产生，
    /// PermissionDecided 解除。run 保持 Running（业务等待不是平台故障），
    /// 不触碰 AwaitingResume。
    Approval,
}

impl WaitKind {
    /// 等待类别的稳定字符串形式。
    ///
    /// `WaitKind::Uncertain` 的字面量还决定 run 是否进入 `awaiting_resume`，
    /// 因此这里的取值必须与 `Wait` 的线格式 kind 完全一致。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Delay => "delay",
            Self::Signal => "signal",
            Self::Child => "child",
            Self::Retry => "retry",
            Self::Uncertain => "uncertain",
            Self::Approval => "approval",
        }
    }
}

/// 工作流元数据：定义版本集合与软删除标记。
///
/// 定义本体以 `StoredValue` 引用正文（内联或分块），折叠层只搬运引用，
/// 不把定义字节复制进内存快照——大定义会让每次状态复制都变成热点。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub workflow_id: String,
    pub name: String,
    pub created_at: String,
    pub versions: BTreeMap<u64, DefinitionVersion>,
    #[serde(default)]
    pub deleted: bool,
}
/// 一个工作流定义版本。
///
/// `checksum` 是定义正文的摘要，版本号必须连续递增——重放时靠这两者确认
/// 「提交的就是这份定义」，避免定义被事后替换。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefinitionVersion {
    pub version: u64,
    pub definition: StoredValue,
    pub checksum: String,
    pub published: bool,
    pub created_at: String,
}
/// 一次运行的当前状态：输入、输出、状态机位置与节点账本。
///
/// 这里每个字段都是「从事实折叠出来的结果」，不是可独立写入的事实；
/// 任何修改都必须经由 [`State::apply`] 产生事件再折叠回来（I5）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_version: u64,
    pub definition: StoredValue,
    pub input: StoredValue,
    pub source: String,
    pub source_detail: Option<String>,
    pub parent: Option<Parent>,
    pub depth: u32,
    pub created_at: String,
    pub status: RunStatus,
    pub output: Option<StoredValue>,
    pub error: Option<String>,
    #[serde(with = "snapshot_decimal")]
    pub last_run_seq: u64,
    pub nodes: BTreeMap<String, Node>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<ExecutionSnapshot>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub signals: BTreeMap<String, StoredValue>,
    /// 冻结策略（09 G2/I30）：随 RunStarted 快照冻结，派发与审批只读它。
    /// None = 权限模型之前启动的 run（缺少策略快照：G3 一律拒绝外部操作）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<flow_dto::permission::FrozenPolicy>,
    /// Run 精确授权投影（RunApprovalUpserted/Removed 折叠；只免重复询问）。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub approvals: Vec<flow_dto::permission::RunApproval>,
    /// Run 授权事件水位：policy_revision = 策略 digest + 该水位（§3.3）。
    #[serde(default, skip_serializing_if = "is_zero")]
    pub approval_seq: u64,
}

/// serde 帮手：零值计数器不序列化（旧快照字节不变）。
fn is_zero(value: &u64) -> bool {
    *value == 0
}

// 运行快照在 run 启动时冻结：runtime、时钟、随机种子、绑定与被引用定义版本
// 都是外部依赖身份。子 run 继承它，重试/恢复/回归必须用同一份，否则同一条
// 事实序列在不同进程里会算出不同结果，崩溃恢复后无法复现原执行。
/// Immutable dependencies and deterministic inputs, inherited by child runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSnapshot {
    pub runtime: String,
    pub clock_ms: i64,
    pub seed: String,
    pub path: Vec<String>,
    pub definitions: BTreeMap<String, DefinitionVersion>,
    pub bindings: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regression: Option<Regression>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    /// 内置 agent 的模型配置（run 启动时冻结；回归候选继承 baseline）。
    /// 模型/端点/限额与 runtime/seed 同级，都是外部依赖身份——重试/恢复/
    /// 回归必须用同一份。旧 journal 反序列化为 None（未配置时代）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<flow_dto::AgentRunConfig>,
    /// 插件声明冻结（U1）：历史 run 的 UI 渲染自持——插件升级/卸载/
    /// agent 机消失都不影响旧 run 的详情展示。ui 是 UiProjection 的
    /// 序列化（结构契约见 flow-config::plugin_manifest）；旧 journal
    /// 反序列化为 None（快照冻结之前的时代）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_plugin: Option<AgentPluginSnapshot>,
    /// 本 run 引用的全部插件声明投影（动态节点类型的渲染自持源；
    /// harness/llm 由 agent_plugin 承载，此处收动态类型）。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plugins: Vec<AgentPluginSnapshot>,
}

/// 快照冻结的插件声明元数据（id/version/UI 投影）。
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentPluginSnapshot {
    /// 插件 id（展示归属）。
    pub id: String,
    /// 插件版本（诊断展示；渲染永不回读插件目录）。
    pub version: String,
    /// UiProjection 的序列化（palette/params/summary_keys）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ui: Option<serde_json::Value>,
    /// PermissionProjection 的序列化（规范化能力需求 + required；
    /// 09 §7.2）。None = legacy-unrestricted 插件（无权限声明）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissions: Option<serde_json::Value>,
    /// manifest 内容哈希（安装身份；授予按 (plugin_id, content_hash) 键）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
}
impl ExecutionSnapshot {
    /// 节点在快照路径下的唯一键（路径的 JSON 编码）。
    ///
    /// 子工作流里同名节点在不同层级必须区分开，所以键是「整条路径」而不是
    /// 节点名；序列化成字符串是为了能直接放进 map key 与事件载荷。
    pub fn node_key(&self, node: &str) -> String {
        let mut path = self.path.clone();
        path.push(node.into());
        serde_json::to_string(&path).unwrap()
    }
    /// 取出属于本层路径的回归信号注入值。
    ///
    /// 回归用完整路径（JSON 数组字符串）登记信号，只有与当前 `path` 逐段相等
    /// 的那些才作用于本节点；路径不匹配就静默忽略，避免父层信号泄漏进子工作流。
    pub fn signal_inputs(&self) -> BTreeMap<String, StoredValue> {
        self.regression
            .as_ref()
            .map(|r| {
                r.signals
                    .iter()
                    .filter_map(|(path, value)| {
                        let mut path: Vec<String> = serde_json::from_str(path).ok()?;
                        let node = path.pop()?;
                        (path == self.path).then(|| (node, value.clone()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}
/// 回归运行的基线与固件（fixture）集合。
///
/// `baseline` 指向被对照的基线 run；`fixtures` 固化外部操作的结果，
/// 使回归候选在没有真实外部副作用的情况下复现同一条执行路径。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Regression {
    pub baseline: String,
    pub fixtures: BTreeMap<String, OperationFixture>,
    #[serde(default)]
    pub signals: BTreeMap<String, StoredValue>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub overrides: std::collections::BTreeSet<String>,
}
/// 回归固件：把一次外部操作的结果钉死。
///
/// `fingerprint` 必须与被固化的请求摘要一致，候选 run 才能用同一份
/// 固件重放该操作——否则回归测的是另一条请求路径。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationFixture {
    pub fingerprint: String,
    pub outcome: StoredValue,
}
impl Run {
    // 摘要刻意丢掉 attempts 账本：账本体积随派发次数增长，复制它会让
    // 每次状态快照都变成 O(历史长度)，而 run 摘要只需要用于索引与展示。
    /// Run metadata without the per-node execution ledger.
    pub fn summary(&self) -> Self {
        Self {
            run_id: self.run_id.clone(),
            workflow_id: self.workflow_id.clone(),
            workflow_version: self.workflow_version,
            definition: self.definition.clone(),
            input: self.input.clone(),
            source: self.source.clone(),
            source_detail: self.source_detail.clone(),
            parent: self.parent.clone(),
            depth: self.depth,
            created_at: self.created_at.clone(),
            status: self.status,
            output: self.output.clone(),
            error: self.error.clone(),
            last_run_seq: self.last_run_seq,
            nodes: BTreeMap::new(),
            execution: self.execution.clone(),
            signals: self.signals.clone(),
            policy: self.policy.clone(),
            approvals: self.approvals.clone(),
            approval_seq: self.approval_seq,
        }
    }
    // 刷新规则只覆盖非终态 run：终态事实（成功/失败/取消）一旦落账就不该
    // 被后来的事件改写，否则重放与在线折叠会给出不同结论。
    /// Terminal facts win; otherwise all outstanding uncertain waits participate.
    pub fn refresh_status(&mut self) {
        if !self.status.terminal() {
            self.status = if self.nodes.values().any(|node| {
                node.wait
                    .as_ref()
                    .is_some_and(|wait| wait.kind() == WaitKind::Uncertain)
            }) {
                RunStatus::AwaitingResume
            } else {
                RunStatus::Running
            };
        }
    }
}
/// 子 run 的血缘指针：它由哪个 run 的哪个节点派生。
///
/// 只记录起点而不复制父状态：子 run 继承的是父的冻结执行快照
/// （见 [`ExecutionSnapshot`]），血缘指针用于回收与展示，不参与裁决。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Parent {
    pub run_id: String,
    pub node_id: String,
}
/// 节点级折叠状态：一次逻辑节点在某个 run 内的当前形态。
///
/// 关键在于 `attempts`（按 dispatch_id 索引的尝试账本）独立于节点本身存在：
/// 结果事件只封存 attempt 的审计账本，节点状态可被覆盖，而 attempt 账本
/// 承载不可变的审计证据链，必须跨尝试累积保留。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub node_execution_id: String,
    pub attempt: u32,
    pub dispatch_id: String,
    pub status: NodeStatus,
    pub output: Option<StoredValue>,
    pub branch: Option<bool>,
    pub prepared: Option<Prepared>,
    pub wait: Option<Wait>,
    pub operation: Option<Operation>,
    pub attempts: BTreeMap<String, Attempt>,
    pub error: Option<String>,
    /// 跳过原因（NodeSkipped 事件携带；时间线展示用，不影响裁决）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skip_reason: Option<String>,
    /// 最近一次权限询问（09 §6）。跨 DispatchStarted 保留：被重派发取代的
    /// 询问留下 deadline 供继承（不得因等待重启而刷新 TTL）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval: Option<ApprovalState>,
    /// 节点级 Allow 裁决留下的授权凭条（I26 折叠闸）：OperationAuthorized
    /// 必须消费同 operation_id + fingerprint 的凭条，消费即清空。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permit: Option<PermitGrant>,
}

/// 权限询问的状态机：`Pending → Allowed | Denied | Expired`，或因派发
/// 结束而 `Superseded`（重派发）/ `Withdrawn`（会话结束/节点出结果）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalStatus {
    Pending,
    Allowed,
    Denied,
    Expired,
    Superseded,
    Withdrawn,
}

impl ApprovalStatus {
    /// 线上名称（视图与 RPC 展示用）。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Allowed => "allowed",
            Self::Denied => "denied",
            Self::Expired => "expired",
            Self::Superseded => "superseded",
            Self::Withdrawn => "withdrawn",
        }
    }
}

/// 一次权限询问的折叠态（PermissionAsked 载荷 + 当前状态）。
///
/// 审批 RPC 从这里取回完整请求重新评估（能力过期 → 直接拒绝），并用
/// `operation_request` 构造 OperationAuthorized——审批卡片的每个字段都来自
/// master 的持久请求，插件不能覆盖关键字段（§7.2）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalState {
    /// `node`（G3 节点级）或 `tool`（G5 工具级）。
    pub gate: String,
    pub request: flow_dto::permission::PermissionRequest,
    pub dispatch_id: String,
    pub asked_at: i64,
    /// 绝对截止（毫秒）；TTL 到期 = Deny + 节点业务失败（I34）。
    pub deadline: i64,
    /// 操作请求正文（G3：批准后用于 OperationIntent/Authorized）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_request: Option<StoredValue>,
    /// 评估时命中的能力项（审批卡片「依据」）。
    #[serde(default)]
    pub matched: Vec<String>,
    pub status: ApprovalStatus,
    /// 裁决记录（终态时）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision: Option<flow_dto::permission::DecisionRecord>,
}

/// 节点级 Allow 凭条：绑定 operation_id + 请求指纹 + request_hash。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermitGrant {
    pub operation_id: String,
    pub request_hash: String,
    pub fingerprint: String,
}

/// `PolicyFrozen` 事件载荷（09 §10.1）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyFrozenPayload {
    pub policy_digest: String,
    pub permission_schema_version: u32,
    pub legacy: bool,
}

/// `PermissionAsked` 事件载荷。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PermissionAskedPayload {
    /// `node`（G3）| `tool`（G5）。
    pub gate: String,
    pub request: flow_dto::permission::PermissionRequest,
    pub asked_at: i64,
    pub deadline: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_request: Option<StoredValue>,
    #[serde(default)]
    pub matched: Vec<String>,
}

/// `PermissionDecided` 事件载荷：裁决记录 + 结论状态。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PermissionDecidedPayload {
    pub gate: String,
    pub record: flow_dto::permission::DecisionRecord,
    /// 被裁决请求的字节摘要（I26 折叠闸用它对上 OperationAuthorized）。
    pub request_fingerprint: String,
    /// `allowed` | `denied` | `expired`。
    pub status: ApprovalStatus,
}
impl Node {
    // 与 run 摘要同理：节点摘要只保留执行字段，尝试历史单独索引。
    /// Execution fields only; attempt history is staged and indexed independently.
    pub fn summary(&self) -> Self {
        Self {
            node_execution_id: self.node_execution_id.clone(),
            attempt: self.attempt,
            dispatch_id: self.dispatch_id.clone(),
            status: self.status,
            output: self.output.clone(),
            branch: self.branch,
            prepared: self.prepared.clone(),
            wait: self.wait.clone(),
            operation: self.operation.clone(),
            attempts: BTreeMap::new(),
            error: self.error.clone(),
            skip_reason: self.skip_reason.clone(),
            approval: self.approval.clone(),
            permit: self.permit.clone(),
        }
    }
}
/// 一次派发（dispatch）的尝试账本：审计序号、摘要、结果事件与完整性标记。
///
/// `sealed` 表示结果已按 `sealed_through` 封口；封口后只接受迟到审计，
/// 不再接受新的结果事件——这是「证据已齐、结论不可再被改写」的物理体现。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    pub audit_seq: u64,
    pub audit_digests: BTreeMap<u64, String>,
    #[serde(default)]
    pub audit_lsns: BTreeMap<u64, u64>,
    #[serde(default)]
    pub result: Option<Event>,
    #[serde(default)]
    pub result_lsn: Option<u64>,
    pub sealed: bool,
    pub integrity: String,
}
/// 输入准备事实的冻结内容：节点将要消费的输入、前驱输出与参数。
///
/// `node_semantics` 记录执行语义版本，用于判断旧准备事实能否被当前引擎
/// 解释；`engine_build` 记录写入该准备事实的引擎构建号。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Prepared {
    pub input: StoredValue,
    pub predecessors: BTreeMap<String, StoredValue>,
    pub params: StoredValue,
    pub engine_build: String,
    pub node_semantics: u32,
}
// 内部枚举按「种类」携带各自需要的字段，避免用一堆 Option 拼出无效组合；
// 但对外的 JSON 形状必须保持旧检查点/事件的原样（显式 null 也要保留），
// 因此通过 WaitWire 做双向适配，旧日志才能原样读回。
/// Internal waits carry the fields required by their kind. The adapter preserves
/// the original checkpoint/event JSON shape, including explicit null fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "WaitWire", into = "WaitWire")]
pub enum Wait {
    Delay {
        wake_at: i64,
        output: Option<StoredValue>,
    },
    Signal {
        output: Option<StoredValue>,
    },
    Child {
        child_run_id: String,
    },
    Retry {
        wake_at: i64,
    },
    Uncertain,
    /// 权限审批等待：`deadline` 走线上 `wake_at` 字段（定时等待语义），
    /// operation_id/request_hash 绑定被询问的那一次请求实例。
    Approval {
        operation_id: String,
        request_hash: String,
        deadline: i64,
    },
}
/// `Wait` 的线上表示：字段齐平、带显式 null，用来保持旧 JSON 形状。
///
/// 审批等待新增的两个字段 skip_none：旧种类的 JSON 形状一个字节都不变。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WaitWire {
    kind: WaitKind,
    wake_at: Option<i64>,
    child_run_id: Option<String>,
    #[serde(default)]
    output: Option<StoredValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_hash: Option<String>,
}
impl TryFrom<WaitWire> for Wait {
    type Error = &'static str;

    /// 把宽松的线上形状收敛成内部枚举，并拒绝「字段与种类不匹配」的组合。
    ///
    /// 宁可反序列化失败也不猜测：把 `child_run_id` 悄悄塞进一个 delay 等待
    /// 会让恢复逻辑走错分支，而损坏的事实必须以显式错误暴露。
    ///
    /// # Errors
    /// 出现与 `kind` 不相容的字段，或缺少该 kind 必填的字段时返回错误。
    fn try_from(w: WaitWire) -> std::result::Result<Self, Self::Error> {
        if w.child_run_id.is_some() && w.kind != WaitKind::Child {
            return Err("child_run_id on non-child wait");
        }
        if w.wake_at.is_some()
            && !matches!(w.kind, WaitKind::Delay | WaitKind::Retry | WaitKind::Approval)
        {
            return Err("wake_at on non-timed wait");
        }
        if w.output.is_some() && !matches!(w.kind, WaitKind::Delay | WaitKind::Signal) {
            return Err("output on non-value wait");
        }
        if (w.operation_id.is_some() || w.request_hash.is_some()) && w.kind != WaitKind::Approval
        {
            return Err("approval identity on non-approval wait");
        }
        Ok(match w.kind {
            WaitKind::Delay => Self::Delay {
                wake_at: w.wake_at.ok_or("delay missing wake_at")?,
                output: w.output,
            },
            WaitKind::Signal => Self::Signal { output: w.output },
            WaitKind::Child => Self::Child {
                child_run_id: w
                    .child_run_id
                    .filter(|id| !id.is_empty())
                    .ok_or("child missing child_run_id")?,
            },
            WaitKind::Retry => Self::Retry {
                wake_at: w.wake_at.ok_or("retry missing wake_at")?,
            },
            WaitKind::Uncertain => Self::Uncertain,
            WaitKind::Approval => Self::Approval {
                operation_id: w
                    .operation_id
                    .filter(|id| !id.is_empty())
                    .ok_or("approval missing operation_id")?,
                request_hash: w
                    .request_hash
                    .filter(|hash| !hash.is_empty())
                    .ok_or("approval missing request_hash")?,
                deadline: w.wake_at.ok_or("approval missing deadline")?,
            },
        })
    }
}
impl From<Wait> for WaitWire {
    /// 把内部枚举摊平成线上形状，未使用的字段显式写成 null。
    fn from(w: Wait) -> Self {
        let mut wire = Self {
            kind: w.kind(),
            wake_at: w.wake_at(),
            child_run_id: None,
            output: w.output().cloned(),
            operation_id: None,
            request_hash: None,
        };
        match w {
            Wait::Child { child_run_id } => wire.child_run_id = Some(child_run_id),
            Wait::Approval {
                operation_id,
                request_hash,
                ..
            } => {
                wire.operation_id = Some(operation_id);
                wire.request_hash = Some(request_hash);
            }
            _ => {}
        }
        wire
    }
}
impl Wait {
    /// 当前等待的类别，供状态判定与载荷比对使用。
    pub fn kind(&self) -> WaitKind {
        match self {
            Self::Delay { .. } => WaitKind::Delay,
            Self::Signal { .. } => WaitKind::Signal,
            Self::Child { .. } => WaitKind::Child,
            Self::Retry { .. } => WaitKind::Retry,
            Self::Uncertain => WaitKind::Uncertain,
            Self::Approval { .. } => WaitKind::Approval,
        }
    }
    /// 定时唤醒时刻（毫秒时间戳）；delay/retry 等待与审批截止有值。
    pub fn wake_at(&self) -> Option<i64> {
        match self {
            Self::Delay { wake_at, .. } | Self::Retry { wake_at } => Some(*wake_at),
            Self::Approval { deadline, .. } => Some(*deadline),
            _ => None,
        }
    }
    /// 被等待的子 run id；仅 child 等待有值。
    pub fn child_run_id(&self) -> Option<&String> {
        match self {
            Self::Child { child_run_id } => Some(child_run_id),
            _ => None,
        }
    }
    /// 等待时已产出的值（delay/signal 等待可携带）；其余种类为 None。
    pub fn output(&self) -> Option<&StoredValue> {
        match self {
            Self::Delay { output, .. } | Self::Signal { output } => output.as_ref(),
            _ => None,
        }
    }
}
/// 外部副作用操作：请求指纹与（尚未捕获的）结果。
///
/// `outcome` 为 None 表示「已授权但结果未知」——这是最危险的状态，
/// 折叠层据此禁止再次派发同一节点，必须等待人工裁决。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub operation_id: String,
    pub fingerprint: String,
    pub permit_id: String,
    pub request: StoredValue,
    pub outcome: Option<StoredValue>,
}
/// 命令幂等回执：同一 (scope, request_id) 只能被提交一次。
///
/// `lsn` 记录该命令被接受时的日志位置，是回执淘汰的排序依据。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandRecord {
    pub scope: String,
    pub request_id: Option<String>,
    pub fingerprint: String,
    pub result: Value,
    #[serde(with = "flow_journal::decimal")]
    pub lsn: u64,
}
// 裁决形状契约由命令预检与事实折叠共用，避免「命令层放行但折叠层拒绝」
// 这类两处语义漂移；理由长度上限防止裁决事实膨胀成不可审计的大字段。
// 决策不在允许集合内，或理由为空/超过 4096 字节时返回错误。
/// One adjudication contract for command preflight and fact folding.
pub fn validate_adjudication(decision: &str, reason: &str) -> Result<()> {
    if reason.trim().is_empty() || reason.len() > 4096 {
        return Err(invalid(
            "manual resolution requires a reason of at most 4096 bytes",
        ));
    }
    if !matches!(decision, "accept_output" | "retry" | "failed") {
        return Err(invalid("decision must be accept_output | retry | failed"));
    }
    Ok(())
}
/// 校验一次裁决是否适用于「当前那条未决外部操作」。
///
/// 裁决只能作用于此刻真正处于 Uncertain 等待、且 operation_id 匹配的操作；
/// 已终态的 run、已解决的操作，或对已有 outcome 的操作重复裁决，一律拒绝。
/// 放宽这条守卫会让旧裁决错误地作用于新的操作上——后果是重复外部副作用。
///
/// # Errors
/// 裁决形状非法或不满足上述适用性条件时返回错误。
pub fn adjudicate_guard(
    terminal: bool,
    node: &Node,
    operation_id: &str,
    decision: &str,
    reason: &str,
) -> Result<()> {
    validate_adjudication(decision, reason)?;
    if terminal
        || node
            .wait
            .as_ref()
            .is_none_or(|w| w.kind() != WaitKind::Uncertain)
        || node.operation.as_ref().is_none_or(|o| {
            o.operation_id != operation_id || (o.outcome.is_some() && decision == "retry")
        })
    {
        return Err(invalid(
            "manual resolution requires the current uncertain operation",
        ));
    }
    Ok(())
}

// 调度触发水位独立于有界的 RPC 回执窗口存在：窗口会淘汰旧回执，但
// 「这个槽位在某个时刻已经触发过」的业务事实必须永久保留，
// 否则重放会重复触发同一次调度。
/// Business trigger watermark, independent of the bounded RPC receipt window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleFire {
    pub at: chrono::DateTime<chrono::Utc>,
    pub command: CommandRecord,
}
/// 折叠后的全局状态：工作流、run、调度、命令回执与提交水位。
///
/// 它是 journal 事实在内存中的物化视图，必须能由事实完全重放重建；
/// 因此 `values` 与 `compact_audits` 这类派生字段标记为 `skip`，
/// 重放前由 reducer 自己重新填充，绝不从序列化的快照里信任。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct State {
    /// Rebuilt by replay before accepting commands; never trusted from serialized snapshots.
    #[serde(skip)]
    pub values: flow_journal::value::Publications,
    /// Backend keeps permanent audit receipts in a rebuildable disk index.
    #[serde(skip)]
    pub compact_audits: bool,
    pub version: u32,
    pub journal_id: String,
    #[serde(with = "snapshot_decimal")]
    pub applied_lsn: u64,
    pub workflows: BTreeMap<String, Workflow>,
    pub runs: BTreeMap<String, Run>,
    /// Permanent workflow/run relationship, bounded by workflow count rather than run history.
    #[serde(default)]
    pub run_workflows: std::collections::BTreeSet<String>,
    pub schedules: BTreeMap<String, Value>,
    #[serde(default)]
    pub schedule_fires: BTreeMap<String, ScheduleFire>,
    pub webhooks: BTreeMap<String, Value>,
    /// 可复用节点模板（画布片段）：UI 资产而非 run 事实，但持久化必须随
    /// 权威走——journal 是唯一权威，模板以 TemplateChanged 事实落账。
    #[serde(default)]
    pub templates: BTreeMap<String, Value>,
    pub commands: BTreeMap<String, CommandRecord>,
    #[serde(default)]
    pub legacy: BTreeMap<String, Value>,
    /// Writer tenure (JSONL refactor phase 2, §3.2). Bumped durably each time a master
    /// process takes the exclusive journal write lock and before it dispatches tasks.
    /// Zero means a journal written before the epoch event existed.
    #[serde(default, with = "snapshot_decimal")]
    pub master_epoch: u64,
    /// 插件能力授予（09 §7，I35）：键 = `plugin_id:content_hash`。授予是
    /// 安装期事实（谁在何时批准了哪个内容身份的哪些能力），run 冻结时
    /// 从这里取上限；不回写 plugin.toml。
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub plugin_grants: BTreeMap<String, PluginGrant>,
}

/// 授予层能力：按 manifest 闭集解析（允许族级 any / 裸程序名）。
mod grant_capabilities {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(
        value: &Vec<flow_dto::permission::Capability>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        value.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Vec<flow_dto::permission::Capability>, D::Error> {
        let values = Vec::<serde_json::Value>::deserialize(deserializer)?;
        values
            .iter()
            .map(|value| {
                flow_dto::permission::parse_manifest_capability(value)
                    .map_err(serde::de::Error::custom)
            })
            .collect()
    }
}

/// 一条插件授予记录（PluginGrantUpserted 的折叠产物）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginGrant {
    pub plugin_id: String,
    pub content_hash: String,
    /// 授予的能力（manifest 声明的子集；legacy 确认为空。Any 变体只
    /// 存在于授予/上限层，按 manifest 闭集解析）。
    #[serde(with = "grant_capabilities")]
    pub capabilities: Vec<flow_dto::permission::Capability>,
    /// legacy-unrestricted 插件的显式风险确认（无声明可授）。
    #[serde(default)]
    pub legacy_confirmed: bool,
    pub granted_at: String,
    pub by: flow_dto::permission::Principal,
}

impl PluginGrant {
    /// 授予键（唯一身份）。
    pub fn key(plugin_id: &str, content_hash: &str) -> String {
        format!("{plugin_id}:{content_hash}")
    }
}

/// 命令幂等键：由 scope 与 request_id 组成的 JSON 元组编码。
///
/// 用元组编码而不是字符串拼接，避免分隔符歧义（scope 里出现分隔符时
/// 两种不同的 (scope, request_id) 可能拼出同一个键）。
pub fn command_key(scope: &str, request_id: &str) -> String {
    serde_json::to_string(&(scope, request_id)).expect("string tuple")
}

impl Run {
    /// run 是否已进入终态；终态 run 不再接受派发、信号或状态改写。
    pub fn terminal(&self) -> bool {
        self.status.terminal()
    }
}

/// `check_values` 顺带产出的重载荷解析结果，`apply_event` 直接复用：
/// RunStarted / InputPrepared 的结构化载荷每次折叠只解析一次。
/// 两处永远在 `apply_mode` 里紧挨着调用，`None` 到达需要它的分支
/// 只可能是内部调用顺序被破坏，按非法事实报错而不是 panic。
enum Parsed {
    None,
    Run(Box<Run>),
    Prepared(Box<Prepared>),
}

impl State {
    // 先查有界窗口，再回落到 `schedule_fires` 里的最新调度水位：
    // 业务去重不能依赖那个有限重试窗口，否则窗口一满就会漏掉幂等判定。
    /// The latest schedule slot retains its receipt even after unrelated RPC
    /// commands fill the general dedup window.
    pub fn command_record(&self, scope: &str, request_id: &str) -> Option<&CommandRecord> {
        self.commands
            .get(&command_key(scope, request_id))
            .or_else(|| {
                let key = scope.strip_prefix("run.start:schedule:")?;
                let fire = self.schedule_fires.get(key)?;
                (fire.command.request_id.as_deref() == Some(request_id)).then_some(&fire.command)
            })
    }
    // 事务内暂存保证：后一个事件格式非法时，之前的兄弟事件不会「暴露」出去——
    // 整批要么全部生效，要么全部不生效。提交只触碰被影响到的 map/对象。
    // 事务校验失败或事件与既有状态矛盾时返回错误，且状态保持调用前不变。
    /// Transaction-local staging guarantees that a malformed later event cannot expose its
    /// earlier sibling. Only affected maps/objects are touched by the committed replacement.
    pub fn apply(&mut self, tx: &Transaction) -> Result<()> {
        self.apply_with(tx, |_| Ok(()))
    }
    // `inspect` 回调看到的正是 `apply` 会留下的结果，便于调用方在提交前
    // 就决定是否接受（例如生成当前实体的索引行）。回调返回错误时，
    // 状态按受影响实体的 undo 恢复，不会留下半截提交。
    // 折叠失败或 `inspect` 返回错误时返回错误，状态已回滚。
    /// Commit the transaction and inspect the post-transaction state. The inspect
    /// callback sees exactly what `apply` would leave behind; on error the state is
    /// restored from the affected-entity undo.
    pub fn apply_with<T>(
        &mut self,
        tx: &Transaction,
        inspect: impl FnOnce(&State) -> Result<T>,
    ) -> Result<T> {
        self.apply_mode(std::slice::from_ref(tx), true, inspect)
    }
    /// 只校验一个事务能否被折叠，不提交任何改动。
    ///
    /// 走的是与 `apply` 完全相同的折叠路径，只是最后回滚；因此它给出的
    /// 结论与真正提交时一致，不会出现「预检通过但提交失败」。
    ///
    /// # Errors
    /// 事务无法被折叠时返回错误，状态保持不变。
    pub fn check(&mut self, tx: &Transaction) -> Result<()> {
        self.apply_mode(std::slice::from_ref(tx), false, |_| Ok(()))
    }
    // 即使编码失败也要恢复——预演绝不允许留下副作用，否则「只是看一眼」
    // 的调用就会污染权威视图。恢复用的是与 `apply` 相同的受影响实体 undo，
    // 而不是整份克隆。折叠失败或 `inspect` 返回错误时返回错误，状态已回滚。
    /// Inspect the proposed state and always restore the old state, including when
    /// encoding fails. Uses the same affected-entity undo as apply(), not a full clone.
    pub fn preview<T>(
        &mut self,
        tx: &Transaction,
        inspect: impl FnOnce(&State) -> Result<T>,
    ) -> Result<T> {
        let mut output = None;
        self.apply_mode(std::slice::from_ref(tx), false, |state| {
            output = Some(inspect(state)?);
            Ok(())
        })?;
        Ok(output.expect("successful preview"))
    }
    // 分块正文的字节由写入方负责，折叠器只需要元数据——因此前缀事务里
    // 只允许出现纯值事件，混进带 run/node 身份的事件会被拒绝。
    // 批次不满足原子转移约束、折叠失败或 `inspect` 返回错误时返回错误。
    /// One atomic command, optionally preceded by dataset-scoped value chunks.
    /// Chunk payload bytes are writer-owned; the reducer only needs metadata.
    pub fn preview_batch<T>(
        &mut self,
        transactions: &[Transaction],
        inspect: impl FnOnce(&State) -> Result<T>,
    ) -> Result<T> {
        self.apply_mode(transactions, false, inspect)
    }
    /// 折叠并提交一批事务（值分块前缀 + 收尾命令）。
    ///
    /// 与 `preview_batch` 共用同一条折叠路径，区别仅在于是否提交水位。
    ///
    /// # Errors
    /// 批次折叠失败时返回错误，整批回滚，水位不前进。
    pub fn apply_batch(&mut self, transactions: &[Transaction]) -> Result<()> {
        self.apply_mode(transactions, true, |_| Ok(()))
    }
    /// 折叠引擎主循环：校验 → 逐事件暂存 undo 并折叠 → 检视 → 提交或回滚。
    ///
    /// `commit` 决定是否推进水位与固化 journal 身份；`preview`/`check` 传 false。
    /// 所有错误路径都走同一条 undo 恢复路径，因此「折叠到一半失败」不会留下
    /// 半套状态。
    ///
    /// # Errors
    /// 批次结构非法、事件与状态矛盾、值引用校验失败或 `inspect` 报错时返回错误。
    fn apply_mode<T>(
        &mut self,
        transactions: &[Transaction],
        commit: bool,
        inspect: impl FnOnce(&State) -> Result<T>,
    ) -> Result<T> {
        // 空批次没有「最后一个事务」可作为水位来源，直接判非法。
        let last = transactions
            .last()
            .ok_or_else(|| invalid("empty atomic transition"))?;
        for (index, tx) in transactions.iter().enumerate() {
            // 原子转移的「前缀」只允许承载数据集级值分块：任何带 run/node/
            // dispatch 身份或带序号的事件都不许出现在前缀里，否则这批就不再
            // 是「一个原子命令 + 它引用的正文」，折叠语义会漂移。
            if index + 1 < transactions.len()
                && tx.events.iter().any(|event| {
                    event.kind != EventKind::ValueChunk
                        || event.run_id.is_some()
                        || event.node_id.is_some()
                        || event.dispatch_id.is_some()
                        || event.run_seq != 0
                        || event.audit_seq != 0
                })
            {
                return Err(invalid(
                    "atomic transition prefix must contain dataset value chunks only",
                ));
            }
            tx.validate()?;
            // LSN 必须严格连续且递增 1：gap 意味着有事实没被折叠，
            // 重复则意味着同一份事实被折了两次。两者都会破坏重放确定性。
            if tx.lsn
                != self
                    .applied_lsn
                    .checked_add(index as u64 + 1)
                    .ok_or_else(|| invalid("LSN overflow"))?
                || tx.journal_id != last.journal_id
                || (!self.journal_id.is_empty() && self.journal_id != tx.journal_id)
            {
                return Err(invalid("reducer identity/LSN mismatch"));
            }
            for (index, event) in tx.events.iter().enumerate() {
                // 审计事件必须能定位到 run 与节点：审计序号是按
                // (run, node, dispatch) 计数的，缺了身份就无法归位。
                if event.audit_seq > 0
                    && (event.run_id.is_none()
                        || event
                            .node_id
                            .as_deref()
                            .or_else(|| event.payload["node_id"].as_str())
                            .is_none())
                {
                    return Err(invalid("audit requires run and node identity"));
                }
                if event.kind == EventKind::MasterEpochStarted {
                    if event.run_id.is_some()
                        || event.dispatch_id.is_some()
                        || event.node_id.is_some()
                        || event.run_seq != 0
                        || event.audit_seq != 0
                    {
                        return Err(invalid("master epoch event must be dataset-scoped"));
                    }
                    let epoch = number(&event.payload, "epoch")?;
                    if epoch != self.master_epoch + 1 {
                        return Err(invalid("master epoch must advance by exactly one"));
                    }
                }
                // 意图与授权必须相邻且载荷全同（I6/I10）：外部副作用只能在
                // 授权已经落盘之后发出，两者之间夹入别的事件会让「已授权」
                // 与「已发出」之间出现无法审计的空档。
                if event.kind == EventKind::OperationIntent {
                    let next = tx
                        .events
                        .get(index + 1)
                        .ok_or_else(|| invalid("intent without atomic authorization"))?;
                    if next.kind != EventKind::OperationAuthorized
                        || next.run_id != event.run_id
                        || next.dispatch_id != event.dispatch_id
                        || next.node_id != event.node_id
                        || next.payload != event.payload
                        || event.audit_seq != 0
                        || next.audit_seq != 0
                    {
                        return Err(invalid(
                            "intent/authorization must share transaction and payload",
                        ));
                    }
                }
                // 反向校验：授权前面必须紧跟意图，否则授权是「凭空授权」。
                if event.kind == EventKind::OperationAuthorized
                    && (index == 0 || tx.events[index - 1].kind != EventKind::OperationIntent)
                {
                    return Err(invalid("authorization without intent"));
                }
                // 策略冻结与 run 启动同事务且紧随其后（G2/I30）：中间夹入别的
                // 事实会出现「run 已存在但策略未冻结」的窗口。
                if event.kind == EventKind::PolicyFrozen
                    && index
                        .checked_sub(1)
                        .map(|previous| &tx.events[previous])
                        .is_none_or(|previous| {
                            previous.kind != EventKind::RunStarted
                                || previous.run_id != event.run_id
                        })
                {
                    return Err(invalid("policy freeze must immediately follow its run start"));
                }
            }
        }
        // Undo only changed entities, not the full historical state. Inline values are bounded.
        // Payload-only audits change counters and one receipt entry, never node state.
        // Their rollback cost is independent of the accumulated run history.
        // 纯审计批次（值分块/发布、迟到审计）不改业务实体状态：
        // 这类批次的折叠与回滚代价只与审计条目数有关，与累计的运行历史无关。
        let audit_only = transactions
            .iter()
            .flat_map(|tx| &tx.events)
            .all(Event::is_audit_only);
        let mut undo = UndoLog::default();
        let result = (|| {
            for tx in transactions {
                for event in &tx.events {
                    // 顺序敏感：先拍 undo 前像，再改状态。前像必须在改动之前
                    // 采集，否则回滚会恢复到「已被本次事件改过」的状态。
                    undo.capture(self, event)?;
                    // 值引用必须先登记再被业务事件引用，否则引用完整性检查
                    // 会把合法引用误判为「引用了未发布的值」。
                    self.values.apply(event, &tx.journal_id)?;
                    let parsed = self.check_values(event)?;
                    self.apply_event(event, tx.lsn, &mut undo, parsed)?;
                    if let Some(run) = event.run_id.as_ref().and_then(|id| self.runs.get_mut(id)) {
                        // 审计批次不动 run 状态：让审计事件顺带改写 run 的
                        // 状态会让「只补证据」变成「改结论」。
                        if !audit_only {
                            run.refresh_status();
                        }
                    }
                }
            }
            inspect(self)
        })();
        // 失败或仅预演都回滚；只有真正提交才推进水位并固化 journal 身份。
        // 水位只能在整批成功后前进，否则会出现「水位声称折叠过、实际没折叠」
        // 的空洞，重放时那一段事实会被跳过。
        if result.is_err() || !commit {
            undo.restore(self)?;
        } else {
            self.version = 1;
            self.journal_id = last.journal_id.clone();
            self.applied_lsn = last.lsn;
        }
        result
    }

    /// 校验事件载荷里引用的所有值都已发布且摘要一致。
    ///
    /// 折叠层只搬运值引用而不复制正文，所以「引用是否指向已发布正文」
    /// 必须在这里逐个确认：漏检会让折叠态持有指向不存在正文的引用，
    /// 后续物化时才炸，且那时已经无从事后修复。
    ///
    /// RunStarted / InputPrepared 的结构化载荷在这里解析一次，随返回值
    /// 交给 `apply_event` 复用——热路径不再对同一载荷付两次 JSON 反序列化
    /// （RunStarted 此前把整个 Run 解析两遍、InputPrepared 三遍）。
    ///
    /// # Errors
    /// 任一引用缺失、未发布或不匹配时返回错误。
    fn check_values(&self, event: &Event) -> Result<Parsed> {
        let p = &event.payload;
        let check = |v: &Value| -> Result<()> {
            self.values
                .check(&serde_json::from_value::<StoredValue>(v.clone())?)
        };
        let mut parsed = Parsed::None;
        match event.kind {
            EventKind::LegacyImport => check(&p["data"])?,
            EventKind::WorkflowUpdated => check(&p["version"]["definition"])?,
            EventKind::RunStarted => {
                check(&p["input"])?;
                check(&p["definition"])?;
                let run: Run = serde_json::from_value(p.clone())?;
                for signal in run.signals.values() {
                    self.values.check(signal)?;
                }
                if let Some(execution) = &run.execution {
                    // 执行快照的三个上限：冻结的定义数量、子工作流嵌套深度、
                    // runtime 标识长度。快照在 run 启动时冻结，一旦定下就不能
                    // 随事件变化，否则恢复时会算出与原执行不同的输入。
                    if execution.definitions.len() > 128
                        || execution.path.len() > crate::MAX_SUB_WORKFLOW_DEPTH as usize
                        || execution.runtime.len() != 64
                    {
                        return Err(invalid("invalid execution snapshot"));
                    }
                    for definition in execution.definitions.values() {
                        self.values.check(&definition.definition)?;
                    }
                    if let Some(regression) = &execution.regression {
                        for fixture in regression.fixtures.values() {
                            self.values.check(&fixture.outcome)?;
                            if let StoredValue::Inline(v) = &fixture.outcome {
                                if let Some(raw) = v.get("body_raw") {
                                    self.values.check(&StoredValue::Ref(
                                        serde_json::from_value(raw.clone())?,
                                    ))?;
                                }
                            }
                        }
                        for value in regression.signals.values() {
                            self.values.check(value)?;
                        }
                    }
                }
                parsed = Parsed::Run(Box::new(run));
            }
            EventKind::InputPrepared => {
                let prepared: Prepared = serde_json::from_value(p["prepared"].clone())?;
                self.values.check(&prepared.input)?;
                self.values.check(&prepared.params)?;
                for value in prepared.predecessors.values() {
                    self.values.check(value)?;
                }
                parsed = Parsed::Prepared(Box::new(prepared));
            }
            EventKind::NodeCompleted
            | EventKind::RunCompleted
            | EventKind::WaitResolved
            | EventKind::Adjudicated => check(&p["output"])?,
            EventKind::SignalReceived => check(&p["payload"])?,
            EventKind::OperationIntent | EventKind::OperationAuthorized => {
                check(&p["operation"]["request"])?
            }
            EventKind::OperationOutcome => {
                let outcome: StoredValue = serde_json::from_value(p["outcome"].clone())?;
                self.values.check(&outcome)?;
                if let StoredValue::Inline(value) = outcome {
                    if let Some(raw) = value.get("body_raw") {
                        self.values
                            .check(&StoredValue::Ref(serde_json::from_value(raw.clone())?))?;
                    }
                }
            }
            EventKind::WaitRegistered => {
                let wait: Wait = serde_json::from_value(p["wait"].clone())?;
                if let Some(value) = wait.output() {
                    self.values.check(value)?;
                }
            }
            EventKind::PermissionAsked => {
                // 审批卡片的请求正文必须是已发布值：批准时据此构造授权事实。
                let asked: PermissionAskedPayload = serde_json::from_value(p.clone())?;
                if let Some(value) = &asked.operation_request {
                    self.values.check(value)?;
                }
            }
            _ => {}
        }
        Ok(parsed)
    }

    /// 把单个事件折叠进状态。
    ///
    /// 这是 run/node 语义的唯一实现（I5）：每条事件要么被接受并改变状态，
    /// 要么返回错误让整批回滚，绝不部分生效。`undo` 用于登记本次事件
    /// 引入的、尚未被其它分支覆盖的可回滚前像。
    ///
    /// # Errors
    /// 事件与既有状态矛盾（版本不连续、序号不连续、指纹不符、
    /// 越过结果屏障等）时返回错误。
    fn apply_event(
        &mut self,
        event: &Event,
        lsn: u64,
        undo: &mut UndoLog,
        parsed: Parsed,
    ) -> Result<()> {
        let p = &event.payload;
        // 迟到审计只补审计证据，绝不推进 run 序号：run 序号代表业务推进，
        // 让补证据顺带推进业务水位会让下游误以为节点已经向前走了。
        if event.kind == EventKind::LateAudit && (event.run_seq != 0 || event.audit_seq == 0) {
            return Err(invalid(
                "late evidence requires audit identity and cannot advance run sequence",
            ));
        }
        match event.kind {
            // 分段开始/封段只是日志分段标记，对折叠状态没有任何影响。
            EventKind::SegmentStarted | EventKind::SegmentSealed => return Ok(()),
            EventKind::MasterEpochStarted => {
                self.master_epoch = number(p, "epoch")?;
                return Ok(());
            }
            EventKind::Command => {
                let mut command: CommandRecord = serde_json::from_value(p.clone())?;
                // lsn 以折叠时的提交位置为准，不信载荷里的自报值：
                // 载荷可能来自旧日志或被伪造的写入者，自报 lsn 会破坏淘汰排序。
                command.lsn = lsn;
                // Also folds old logs: the Command and RunStarted facts already
                // contain the stable slot identity and result. No business code
                // runs during this migration/replay.
                // 调度触发的水位在这里重建：Command 与 RunStarted 事实里
                // 已经含有稳定的槽位身份与结果，重放时无需再执行一次业务。
                if let Some(key) = command.scope.strip_prefix("run.start:schedule:") {
                    let at = command
                        .request_id
                        .as_deref()
                        .and_then(|id| chrono::DateTime::parse_from_rfc3339(id).ok());
                    let run = command.result["run_id"]
                        .as_str()
                        .and_then(|id| self.runs.get(id));
                    if let (Some(at), Some(run)) = (at, run) {
                        // 只有「槽位仍存在、run 确实由该槽位触发、且时间更新」
                        // 三者同时成立才推进水位。否则旧回执会覆盖更新的水位，
                        // 或把已删除的调度重新点亮。
                        if self.schedules.contains_key(key)
                            && run.source == "schedule"
                            && run.source_detail.as_deref() == Some(key)
                            && self.schedule_fires.get(key).is_none_or(|fire| at > fire.at)
                        {
                            self.schedule_fires.insert(
                                key.to_owned(),
                                ScheduleFire {
                                    at: at.with_timezone(&chrono::Utc),
                                    command: command.clone(),
                                },
                            );
                        }
                    }
                }
                if let Some(id) = &command.request_id {
                    let key = command_key(&command.scope, id);
                    // 同一 (scope, request_id) 二次提交是协议违规：命令层已经
                    // 保证不会发生，出现即意味着有第二个写入者或事实被重放，
                    // 必须报错而不是覆盖掉旧回执。
                    if self.commands.contains_key(&key) {
                        return Err(invalid("duplicate committed command identity"));
                    }
                    self.commands.insert(key, command);
                    // 幂等窗口有界：回ceipt 保留最近 COMMAND_WINDOW 条
                    //（按 lsn 序淘汰最旧一半），长跑服务内存不随命令数
                    // 无界增长。重放确定性：阈值与淘汰规则只依赖事件序。
                    if self.commands.len() > COMMAND_WINDOW {
                        let mut order: Vec<(u64, String)> = self
                            .commands
                            .iter()
                            .map(|(key, record)| (record.lsn, key.clone()))
                            .collect();
                        order.sort_unstable();
                        // 一次淘汰最旧的一半：留一半余量避免每来一条命令就淘汰
                        // 一次（抖动），又保证窗口不会无界增长。淘汰掉的回执
                        // 要登记 undo，否则本批后续失败时它们就永久消失了。
                        let evict = self.commands.len() - COMMAND_WINDOW / 2;
                        for (_, key) in order.into_iter().take(evict) {
                            if let Some(record) = self.commands.remove(&key) {
                                undo.command(key, Some(record));
                            }
                        }
                    }
                }
                return Ok(());
            }
            EventKind::WorkflowCreated => {
                let workflow: Workflow = serde_json::from_value(p.clone())?;
                // 创建必须唯一：同名 workflow 重复创建会让版本号序列失去意义
                // （两个来源抢同一个 versions map）。
                if self.workflows.contains_key(&workflow.workflow_id) {
                    return Err(invalid("workflow already exists"));
                }
                self.workflows
                    .insert(workflow.workflow_id.clone(), workflow);
                return Ok(());
            }
            EventKind::WorkflowUpdated => {
                let id = string(p, "workflow_id")?;
                let version: DefinitionVersion = serde_json::from_value(p["version"].clone())?;
                let workflow = self
                    .workflows
                    .get_mut(&id)
                    .ok_or_else(|| invalid("missing workflow"))?;
                // 版本号必须严格连续递增：跳号意味着中间有一份定义从未落账，
                // 而重放时按版本取定义会取到错的那一份。
                if version.version != workflow.versions.last_key_value().map_or(1, |(v, _)| v + 1) {
                    return Err(invalid("non-contiguous definition version"));
                }
                workflow.versions.insert(version.version, version);
                return Ok(());
            }
            EventKind::WorkflowPublished => {
                let id = string(p, "workflow_id")?;
                let v = number(p, "version")?;
                // 只能发布已存在的版本：发布不存在的版本会让「按版本取定义」
                // 在运行期才失败，那时已经发不出去了。
                let version = self
                    .workflows
                    .get_mut(&id)
                    .and_then(|w| w.versions.get_mut(&v))
                    .ok_or_else(|| invalid("missing workflow version"))?;
                version.published = true;
                return Ok(());
            }
            EventKind::WorkflowDeleted => {
                // 软删除：只置标记，不抹掉定义与版本。已存在的 run 仍要按
                // 版本引用旧定义，硬删会让这些 run 无法重放。
                if let Some(workflow) = self.workflows.get_mut(&string(p, "workflow_id")?) {
                    workflow.deleted = true;
                }
                return Ok(());
            }
            EventKind::ScheduleChanged => {
                // 调度被删除时一并撤掉它的触发水位：水位留着会让被删的槽位
                // 在重建后仍然显示「已触发过」。
                if p["deleted"] == true {
                    self.schedule_fires.remove(&string(p, "id")?);
                }
                replace_config(&mut self.schedules, p, "id")?;
                return Ok(());
            }
            EventKind::WebhookChanged => {
                replace_config(&mut self.webhooks, p, "token")?;
                return Ok(());
            }
            EventKind::TemplateChanged => {
                replace_config(&mut self.templates, p, "id")?;
                return Ok(());
            }
            EventKind::WorkflowPermissionsChanged => {
                // 审计标记：只校验指向已存在的定义版本（与 WorkflowUpdated 同
                // 事务、在其后），本身不改状态——权限内容就在定义版本里。
                let id = string(p, "workflow_id")?;
                let version = number(p, "definition_version")?;
                if self
                    .workflows
                    .get(&id)
                    .and_then(|w| w.versions.get(&version))
                    .is_none()
                {
                    return Err(invalid("permission change for missing workflow version"));
                }
                return Ok(());
            }
            EventKind::PluginGrantUpserted => {
                // 授予是安装期事实：同键重复授予以后写为准（升级差异确认），
                // 不同键互不影响。能力走 flow-dto 闭集解析。
                let grant: PluginGrant = serde_json::from_value(p.clone())?;
                if grant.plugin_id.trim().is_empty()
                    || grant.content_hash.len() != 64
                    || !grant.legacy_confirmed && grant.capabilities.is_empty()
                {
                    return Err(invalid("malformed plugin grant"));
                }
                self.plugin_grants.insert(
                    PluginGrant::key(&grant.plugin_id, &grant.content_hash),
                    grant,
                );
                return Ok(());
            }
            EventKind::RunStarted => {
                let mut run = match parsed {
                    Parsed::Run(run) => *run,
                    _ => return Err(invalid("run payload missing parsed form")),
                };
                if event.run_id.as_deref() != Some(run.run_id.as_str())
                    || event.run_seq != 1
                    || self.runs.contains_key(&run.run_id)
                {
                    return Err(invalid("invalid run creation"));
                }
                run.last_run_seq = 1;
                // 出生态钉死（§6.1：Running 是唯一出生态）：载荷里的 status
                // 与 nodes 不是可信输入——「终态出生」或预置节点账本只能经
                // 畸形事实混入，这里强制归一到 writer 契约形状。
                run.status = RunStatus::Running;
                run.nodes.clear();
                // Run 授权只能经 RunApprovalUpserted 事实产生，出生载荷不可预置。
                run.approvals.clear();
                run.approval_seq = 0;
                // 冻结策略的身份必须自洽：digest 与层内容逐字对得上（I30）。
                if run
                    .policy
                    .as_ref()
                    .is_some_and(|policy| !policy.digest_is_consistent())
                {
                    return Err(invalid("frozen policy digest mismatch"));
                }
                // 记录 run→workflow 归属：它按 workflow 数量而非 run 数量
                // 增长，是「一个工作流名下有哪些 run」的永久索引。
                self.run_workflows.insert(run.workflow_id.clone());
                self.runs.insert(run.run_id.clone(), run);
                return Ok(());
            }
            // Master-produced values precede their referencing business fact and have no run.
            // 写者产出的值事件先于引用它的业务事实出现，本身不带 run 身份，
            // 对 run/node 状态无影响，直接跳过。
            EventKind::ValueChunk | EventKind::ValuePublished if event.run_id.is_none() => {
                return Ok(());
            }
            EventKind::LegacyImport => {
                let key = string(p, "key")?;
                // 旧基线键只能导入一次：重复导入会让「基线」指向两份数据，
                // 而基线是回归比较的唯一参照。
                if self.legacy.contains_key(&key) {
                    return Err(invalid("duplicate legacy baseline key"));
                }
                self.legacy.insert(key, p.clone());
                return Ok(());
            }
            _ => {}
        }
        let run = self
            .runs
            .get_mut(
                event
                    .run_id
                    .as_deref()
                    .ok_or_else(|| invalid("run identity required"))?,
            )
            .ok_or_else(|| invalid("missing run"))?;
        // run 序号必须严格 +1 连续：断号意味着中间有事实丢失，
        // 而折叠出来的状态会跳过那段业务过程。
        if event.run_seq > 0 {
            if event.run_seq != run.last_run_seq + 1 {
                return Err(invalid("non-contiguous run sequence"));
            }
            run.last_run_seq = event.run_seq;
        }
        match event.kind {
            EventKind::RunCancelled => {
                // 终态不可被 Cancelled 改写（与 RunFailed 的终态无操作对齐）：
                // 一条 Cancelled 事实不得把已 Succeeded/Failed 的 run 改写成
                // Cancelled 并抹掉 error——命令层有守卫，折叠层是重放权威。
                if !run.terminal() {
                    run.status = RunStatus::Cancelled;
                    run.error = None;
                }
                return Ok(());
            }
            EventKind::RunCompleted => {
                // 终态不可再被「完成」改写：重复完成会让输出与失败原因互相矛盾。
                if run.terminal() {
                    return Err(invalid("terminal run completion"));
                }
                run.status = RunStatus::Succeeded;
                run.output = Some(serde_json::from_value(p["output"].clone())?);
                return Ok(());
            }
            EventKind::RunFailed => {
                // 失败对已终态的 run 是无操作（与取消对齐）：终态是已落账的事实，
                // 不能被后来的失败事实改写。
                if !run.terminal() {
                    run.status = RunStatus::Failed;
                    run.error = Some(string(p, "error")?);
                }
                return Ok(());
            }
            EventKind::SignalReceived => {
                if p["queued"] == true {
                    // 终态 run 不再接受信号：终态之后到达的信号属于迟到投递，
                    // 接受它会让已收口的 run 复活。
                    if run.terminal() {
                        return Err(invalid("signal on terminal run"));
                    }
                    let id = event
                        .node_id
                        .clone()
                        .ok_or_else(|| invalid("signal node required"))?;
                    // 信号收件箱按节点唯一占位：同一节点重复投递信号是冲突，
                    // 覆盖会让先到的那份载荷丢失。
                    if run
                        .signals
                        .insert(id, serde_json::from_value(p["payload"].clone())?)
                        .is_some()
                    {
                        return Err(invalid("signal inbox slot already occupied"));
                    }
                }
                return Ok(());
            }
            EventKind::PolicyFrozen => {
                // 冻结记录必须与 RunStarted 快照里的策略逐字对得上，且只能是
                // run 的第二条事实（结构规则另保证同事务紧随）。
                let frozen: PolicyFrozenPayload = serde_json::from_value(p.clone())?;
                let policy = run
                    .policy
                    .as_ref()
                    .ok_or_else(|| invalid("policy freeze without frozen policy"))?;
                if event.run_seq != 2
                    || frozen.policy_digest != policy.digest
                    || frozen.legacy != policy.legacy
                    || frozen.permission_schema_version
                        != flow_dto::permission::PERMISSION_SCHEMA_VERSION
                {
                    return Err(invalid("policy freeze does not match the frozen run policy"));
                }
                return Ok(());
            }
            EventKind::RunApprovalUpserted => {
                // Run 授权只免除重复询问，不增加能力（I27/I30）：没有冻结策略
                // 或 legacy run 无从谈起；终态 run 不再接受新授权。
                if run.terminal() || run.policy.as_ref().is_none_or(|policy| policy.legacy) {
                    return Err(invalid("run approval requires a live governed run"));
                }
                let approval: flow_dto::permission::RunApproval =
                    serde_json::from_value(p.clone())?;
                if approval.digest.len() != 64 || approval.node_id.is_empty() {
                    return Err(invalid("malformed run approval"));
                }
                run.approvals
                    .retain(|a| !(a.node_id == approval.node_id && a.digest == approval.digest));
                run.approvals.push(approval);
                run.approval_seq += 1;
                return Ok(());
            }
            EventKind::RunApprovalRemoved => {
                let node = string(p, "node_id")?;
                let digest = string(p, "digest")?;
                let before = run.approvals.len();
                run.approvals
                    .retain(|a| !(a.node_id == node && a.digest == digest));
                if run.approvals.len() == before {
                    return Err(invalid("run approval not found"));
                }
                run.approval_seq += 1;
                return Ok(());
            }
            _ => {}
        }
        let node_id = event
            .node_id
            .clone()
            .map(Ok)
            .unwrap_or_else(|| string(p, "node_id"))?;
        if event.kind == EventKind::DispatchStarted {
            // 终态 run 不再派发：派发意味着还会有业务活动，终态 run 不该有。
            if run.terminal() {
                return Err(invalid("dispatch on terminal run"));
            }
            let dispatch_id = event
                .dispatch_id
                .clone()
                .ok_or_else(|| invalid("missing dispatch"))?;
            let execution = string(p, "node_execution_id")?;
            let attempt =
                u32::try_from(number(p, "attempt")?).map_err(|_| invalid("attempt overflow"))?;
            let node = run.nodes.entry(node_id).or_insert_with(|| Node {
                node_execution_id: execution.clone(),
                attempt: 0,
                dispatch_id: String::new(),
                status: NodeStatus::Pending,
                output: None,
                branch: None,
                prepared: None,
                wait: None,
                operation: None,
                attempts: BTreeMap::new(),
                error: None,
                skip_reason: None,
                approval: None,
                permit: None,
            });
            // 三重派发守卫：同一逻辑节点（node_execution_id 不变）、
            // attempt 严格 +1、且上一条外部操作的结果已经捕获。
            // 最后一条是 C4 的折叠层闸门——结果未知时再派一次，
            // 等于对可能已经发生的外部副作用再触发一次。
            if node.node_execution_id != execution
                || attempt != node.attempt + 1
                || node.operation.as_ref().is_some_and(|o| o.outcome.is_none())
            {
                return Err(invalid("unsafe dispatch retry"));
            }
            // dispatch 变更：旧 pending 询问被取代（保留 deadline 供新询问继承，
            // 不得因等待重启而刷新 TTL）；旧 dispatch 的授权凭条作废——旧审批
            // 不能自动套用到新派发（§6.1）。
            if let Some(approval) = node
                .approval
                .as_mut()
                .filter(|approval| approval.status == ApprovalStatus::Pending)
            {
                approval.status = ApprovalStatus::Superseded;
            }
            node.permit = None;
            run.status = RunStatus::Running;
            node.attempt = attempt;
            node.dispatch_id = dispatch_id.clone();
            node.status = NodeStatus::Running;
            node.wait = None;
            // 每次派发开一条新的 attempt 账本：审计序号与结果证据按
            // dispatch 维度累积，重试不会抹掉上一轮已收集的审计记录。
            node.attempts.insert(
                dispatch_id,
                Attempt {
                    audit_seq: 0,
                    audit_digests: BTreeMap::new(),
                    audit_lsns: BTreeMap::new(),
                    result: None,
                    result_lsn: None,
                    sealed: false,
                    integrity: "unknown".into(),
                },
            );
            return Ok(());
        }
        if event.kind == EventKind::NodeSkipped {
            // 折叠层守卫（I5/铁律 1 的精神）：skip 只能作用于「从未派发」的
            // 节点——已派发节点的 attempts 账本是权威审计历史（I17 屏障的
            // 落点），一条 skip 事件不能整体抹掉它。writer 侧当前不对已派发
            // 节点发 skip；这道闸防的是 writer 演进出错。
            if let Some(existing) = run.nodes.get(&node_id) {
                if existing.status != NodeStatus::Pending
                    || !existing.dispatch_id.is_empty()
                    || !existing.attempts.is_empty()
                    || existing.prepared.is_some()
                    || existing.operation.is_some()
                {
                    return Err(invalid("cannot skip dispatched node"));
                }
            }
            run.nodes.insert(
                node_id,
                Node {
                    node_execution_id: String::new(),
                    attempt: 0,
                    dispatch_id: String::new(),
                    status: NodeStatus::Skipped,
                    output: None,
                    branch: None,
                    prepared: None,
                    wait: None,
                    operation: None,
                    attempts: BTreeMap::new(),
                    error: None,
                    // 跳过原因只在事件里携带；折影保留一份供时间线展示
                    //（upstream_failed / upstream_skipped / branch_not_taken …）
                    skip_reason: p.get("reason").and_then(Value::as_str).map(str::to_owned),
                    approval: None,
                    permit: None,
                },
            );
            return Ok(());
        }
        if event.kind == EventKind::InputPrepared {
            let Parsed::Prepared(prepared) = &parsed else {
                return Err(invalid("prepared payload missing parsed form"));
            };
            // 准备事实不得与已提交的 run 输入漂移：执行器按 prepared.input
            // 取值，若它与 run.input 不同，重放就会得到不同的结果。
            // node_semantics 只接受已知版本，未知版本不能猜。
            if prepared.input != run.input || !matches!(prepared.node_semantics, 1 | 2) {
                return Err(invalid(
                    "prepared input/semantics differs from committed run",
                ));
            }
            // 每个前驱都必须已 Succeeded 且输出逐字一致：准备事实
            // 记录的是「执行时看到的输入」，与已提交输出不符就说明
            // 有事实被改写或漏写。
            for (id, value) in &prepared.predecessors {
                if run.nodes.get(id).is_none_or(|n| {
                    n.status != NodeStatus::Succeeded || n.output.as_ref() != Some(value)
                }) {
                    return Err(invalid(
                        "prepared predecessor differs from committed output",
                    ));
                }
            }
        }
        let terminal = run.terminal();
        // 有冻结策略的 run 受 I26 折叠闸约束（缺快照的旧 run 由 master 侧拒绝）。
        let governed = run.policy.is_some();
        let node = run
            .nodes
            .get_mut(&node_id)
            .ok_or_else(|| invalid("missing node dispatch"))?;
        // 陈旧派发不得改写当前节点：节点已进入新一轮 attempt 后，
        // 上一轮的结果事件只能进审计账本，不能再改节点状态。
        // LateAudit 是例外——它本来就只补证据。
        if event.dispatch_id.as_deref() != Some(node.dispatch_id.as_str())
            && event.kind != EventKind::LateAudit
        {
            return Err(invalid("stale dispatch cannot alter current node"));
        }
        if event.audit_seq > 0 {
            let dispatch = event
                .dispatch_id
                .as_deref()
                .ok_or_else(|| invalid("missing audit dispatch"))?;
            let attempt = node
                .attempts
                .get_mut(dispatch)
                .ok_or_else(|| invalid("unknown audit dispatch"))?;
            // 审计序号在 dispatch 内严格 +1（I16）；已封口的 attempt
            // 只接受 LateAudit 追加证据（I18），不再接受新的业务结果。
            if event.audit_seq != attempt.audit_seq + 1
                || (attempt.sealed && event.kind != EventKind::LateAudit)
            {
                return Err(invalid("audit gap/duplicate or sealed dispatch"));
            }
            attempt.audit_seq = event.audit_seq;
            // 压缩模式下不保留逐条摘要：审计收据永久存在磁盘索引里，
            // 内存里只留计数，避免长跑进程因审计条目无界增长。
            if !self.compact_audits {
                attempt.audit_lsns.insert(event.audit_seq, lsn);
                attempt.audit_digests.insert(
                    event.audit_seq,
                    flow_journal::codec::digest(&flow_journal::codec::bounded_json(
                        event,
                        flow_journal::MAX_LINE_BYTES,
                    )?),
                );
            }
        }
        match event.kind {
            EventKind::InputPrepared => {
                // 一个节点只准备一次输入：重复准备意味着两份不同的输入事实，
                // 而执行只应消费其中一份。
                if node.prepared.is_some() {
                    return Err(invalid("input already prepared"));
                }
                let Parsed::Prepared(prepared) = parsed else {
                    return Err(invalid("prepared payload missing parsed form"));
                };
                node.prepared = Some(*prepared);
            }
            EventKind::OperationIntent => {
                // 意图必须在输入准备之后：没准备输入就无法确定这次
                // 外部调用要发什么，意图也就无从校验。
                if node.prepared.is_none() {
                    return Err(invalid("operation before input"));
                }
            }
            EventKind::OperationAuthorized => {
                // 终态 run 或已有 operation 时不得再授权：并发授权会让
                // 同一个节点出现两条未决外部操作，后果无法判定。
                if terminal || node.operation.is_some() {
                    return Err(invalid("operation already authorized/cancelled"));
                }
                let operation: Operation = serde_json::from_value(p["operation"].clone())?;
                let digest = match &operation.request {
                    StoredValue::Inline(value) => flow_journal::codec::digest(
                        &flow_journal::codec::bounded_json(value, flow_journal::INLINE_BYTES)?,
                    ),
                    StoredValue::Ref(value) => value.digest.clone(),
                };
                // 指纹必须等于请求字节的摘要（I11）：指纹是「授权的到底是
                // 哪一次请求」的唯一绑定，不匹配就等于授权了别的东西。
                // 同时要求 operation_id/permit_id 非空且 outcome 尚未捕获。
                if operation.operation_id.is_empty()
                    || operation.permit_id.is_empty()
                    || operation.fingerprint != digest
                    || operation.outcome.is_some()
                {
                    return Err(invalid("invalid operation identity/request fingerprint"));
                }
                // I26：有冻结策略的 run 中，授权必须消费一张同 operation_id +
                // 指纹的 Allow 凭条（PermissionDecided 已落盘）。凭条一次性：
                // 不能挪给下一次调用。
                if governed {
                    let permitted = node.permit.as_ref().is_some_and(|permit| {
                        permit.operation_id == operation.operation_id
                            && permit.fingerprint == operation.fingerprint
                    });
                    if !permitted {
                        return Err(invalid(
                            "operation authorized without a recorded allow decision (I26)",
                        ));
                    }
                    node.permit = None;
                }
                node.operation = Some(operation);
            }
            EventKind::OperationOutcome => {
                // 结果必须落在已授权的操作上，且一条操作只接受一次结果：
                // 二次结果意味着外部系统给出了不一致的答案，不能取其一。
                let op = node
                    .operation
                    .as_mut()
                    .ok_or_else(|| invalid("outcome before authorization"))?;
                if op.outcome.is_some() {
                    return Err(invalid("operation already has an outcome"));
                }
                op.outcome = Some(serde_json::from_value(p["outcome"].clone())?);
            }
            EventKind::NodeCompleted | EventKind::WaitRegistered | EventKind::NodeFailed => {
                // 派发出结果即本次会话结束：仍 pending 的询问随之失效（会话
                // 丢失走 Uncertain 路径，不得把旧批准交给新会话，§6.1）。
                if let Some(approval) = node
                    .approval
                    .as_mut()
                    .filter(|approval| approval.status == ApprovalStatus::Pending)
                {
                    approval.status = ApprovalStatus::Withdrawn;
                }
                node.permit = None;
                let n = number(p, "sealed_through")?;
                let attempt = node
                    .attempts
                    .get_mut(&node.dispatch_id)
                    .ok_or_else(|| invalid("missing attempt"))?;
                // 结果审计屏障（I17）：sealed_through 必须正好等于当前
                // attempt 的审计序号——审计还没收齐就宣布结果，等于把
                // 缺失的证据永久埋葬。准备缺失时必须显式标注
                // preparation_failed，否则无法区分「失败」与「事实缺失」。
                if n != attempt.audit_seq
                    || (node.prepared.is_none() && p["preparation_failed"] != true)
                {
                    return Err(invalid("result audit barrier"));
                }
                attempt.sealed = true;
                attempt.result = Some(event.clone());
                attempt.result_lsn = Some(lsn);
                attempt.integrity = p["integrity"].as_str().unwrap_or("complete").into();
                // 终态 run 收到结果事件：证据照记（审计面），但不改业务状态。
                if terminal {
                    return Ok(());
                }
                match event.kind {
                    EventKind::NodeCompleted => {
                        node.status = NodeStatus::Succeeded;
                        node.output = Some(serde_json::from_value(p["output"].clone())?);
                        node.branch = p.get("branch").and_then(Value::as_bool);
                    }
                    EventKind::WaitRegistered => {
                        node.status = NodeStatus::Waiting;
                        node.wait = Some(serde_json::from_value(p["wait"].clone())?);
                        // 与 v1 词汇对齐：正常业务等待（delay/human/child/
                        // retry）期间 run 仍是 running；只有不确定外部结果
                        // （平台故障面）才进入 awaiting_resume——同词同义，
                        // 运维看到 awaiting_resume 即代表需要人工介入。
                        run.status = if wait_kind_uncertain(p) {
                            RunStatus::AwaitingResume
                        } else {
                            RunStatus::Running
                        };
                    }
                    _ => {
                        node.status = NodeStatus::Failed;
                        node.wait = None;
                        node.error = Some(string(p, "error")?);
                        // 纯重试与外部操作互斥（I9）：带未决 operation 的节点
                        // 不能用 retry_wake_at 自动重来，因为外部副作用可能
                        // 已经发生，只能等人工裁决。
                        if let Some(wake_at) = p.get("retry_wake_at").and_then(Value::as_i64) {
                            if node.operation.is_some() {
                                return Err(invalid("external operation cannot use pure retry"));
                            }
                            node.status = NodeStatus::Waiting;
                            node.wait = Some(Wait::Retry { wake_at });
                            run.status = RunStatus::Running;
                        }
                    }
                }
            }
            EventKind::WaitResolved => {
                // 等待解除时顺带清掉信号收件箱里的对应槽位：信号已被消费，
                // 留着会让后续同节点信号被误判为「槽位已占用」。
                if !terminal {
                    run.signals
                        .remove(event.node_id.as_deref().unwrap_or_default());
                    node.status = NodeStatus::Succeeded;
                    node.wait = None;
                    node.output = Some(serde_json::from_value(p["output"].clone())?);
                }
            }
            EventKind::Adjudicated => {
                let decision = p["decision"].as_str().unwrap_or_default();
                adjudicate_guard(
                    terminal,
                    node,
                    p["operation_id"].as_str().unwrap_or_default(),
                    decision,
                    p["reason"].as_str().unwrap_or_default(),
                )?;
                match decision {
                    // 人工显式接受产出（不伪造外部响应被捕获的事实）
                    "accept_output" => {
                        node.status = NodeStatus::Succeeded;
                        node.wait = None;
                        node.output = Some(serde_json::from_value(p["output"].clone())?);
                    }
                    // 人工显式授权重发：清除未决操作，节点回 pending 等驱动器
                    // 以 attempt+1 重新派发（绝不自动重发的禁令针对机器，人工
                    // 裁决就是授权凭据）。审计历史保留在 attempts 里。
                    "retry" => {
                        node.status = NodeStatus::Pending;
                        node.wait = None;
                        node.operation = None;
                        node.error = None;
                        node.output = None;
                    }
                    // 人工判定失败：节点终态 failed，run 由收尾判定写 RunFailed
                    _ => {
                        node.status = NodeStatus::Failed;
                        node.wait = None;
                        node.error = Some(string(p, "reason")?);
                    }
                }
                // 裁决即人工介入完成：awaiting_resume 回到 running（终态由
                // 后续 RunCompleted/RunFailed 事件落定）
                run.status = RunStatus::Running;
            }
            EventKind::PermissionAsked => {
                // master 写的节点事实：绑定当前 dispatch、占 run_seq、不占
                // audit_seq；终态 run 不再询问。
                if terminal || event.audit_seq != 0 || event.run_seq == 0 {
                    return Err(invalid("permission ask must be a live master run event"));
                }
                let asked: PermissionAskedPayload = serde_json::from_value(p.clone())?;
                let request = &asked.request;
                // 自报 hash 必须等于请求 canonical 摘要；请求上下文必须指向
                // 本节点的当前派发（插件自报 node_id 不能改变 master 的绑定）。
                if request.request_hash != request.compute_hash()
                    || request.context.dispatch_id != node.dispatch_id
                    || request.context.node_id != node_id
                    || request.specs.is_empty()
                    || asked.deadline <= asked.asked_at
                {
                    return Err(invalid("invalid permission ask identity"));
                }
                if node.status != NodeStatus::Running
                    || node
                        .approval
                        .as_ref()
                        .is_some_and(|approval| approval.status == ApprovalStatus::Pending)
                {
                    return Err(invalid(
                        "permission ask requires a running node without a pending approval",
                    ));
                }
                match asked.gate.as_str() {
                    // G3：节点级询问发生在副作用授权之前。
                    "node" => {
                        if node.prepared.is_none()
                            || node.operation.is_some()
                            || asked.operation_request.is_none()
                        {
                            return Err(invalid("node-level ask must precede authorization"));
                        }
                    }
                    // G5：工具级询问发生在已授权且在飞的 agent 回合内。
                    "tool" => {
                        if node.operation.as_ref().is_none_or(|o| o.outcome.is_some()) {
                            return Err(invalid(
                                "tool-level ask requires an in-flight authorized turn",
                            ));
                        }
                    }
                    _ => return Err(invalid("unknown permission gate")),
                }
                node.status = NodeStatus::Waiting;
                node.wait = Some(Wait::Approval {
                    operation_id: request.operation_id.clone(),
                    request_hash: request.request_hash.clone(),
                    deadline: asked.deadline,
                });
                node.approval = Some(ApprovalState {
                    gate: asked.gate,
                    dispatch_id: node.dispatch_id.clone(),
                    asked_at: asked.asked_at,
                    deadline: asked.deadline,
                    operation_request: asked.operation_request,
                    matched: asked.matched,
                    status: ApprovalStatus::Pending,
                    decision: None,
                    request: asked.request,
                });
            }
            EventKind::PermissionDecided => {
                if terminal || event.audit_seq != 0 || event.run_seq == 0 {
                    return Err(invalid("permission decision must be a live master run event"));
                }
                let decided: PermissionDecidedPayload = serde_json::from_value(p.clone())?;
                let record = &decided.record;
                use flow_dto::permission::Decision;
                let consistent = matches!(
                    (record.decision, decided.status),
                    (Decision::Allow, ApprovalStatus::Allowed)
                        | (Decision::Deny, ApprovalStatus::Denied | ApprovalStatus::Expired)
                );
                if !consistent || record.request_hash.len() != 64 {
                    return Err(invalid("inconsistent permission decision"));
                }
                let pending = node.approval.as_ref().filter(|approval| {
                    approval.status == ApprovalStatus::Pending
                        && approval.request.operation_id == record.operation_id
                });
                if let Some(pending) = pending {
                    // 只能裁决该节点当前 pending 的那一次请求实例（I32）：
                    // 换了 hash（内容/派发变了）的旧批准一律拒绝。
                    if pending.request.request_hash != record.request_hash
                        || pending.gate != decided.gate
                        || pending.request.context.request_fingerprint
                            != decided.request_fingerprint
                    {
                        return Err(invalid("decision for a different request instance"));
                    }
                    node.wait = None;
                    node.status = NodeStatus::Running;
                    let approval = node.approval.as_mut().expect("pending approval");
                    approval.status = decided.status;
                    approval.decision = Some(decided.record.clone());
                } else {
                    // 直接裁决（auto 放行 / 能力拒绝）：节点必须正在跑当前派发；
                    // 过期只能作用于 pending 询问。
                    if node.status != NodeStatus::Running
                        || decided.status == ApprovalStatus::Expired
                    {
                        return Err(invalid("direct decision requires a running dispatch"));
                    }
                }
                if decided.gate == "node" && record.decision == Decision::Allow {
                    node.permit = Some(PermitGrant {
                        operation_id: record.operation_id.clone(),
                        request_hash: record.request_hash.clone(),
                        fingerprint: decided.request_fingerprint.clone(),
                    });
                }
            }
            EventKind::OperationRetryAuthorized => {
                // 机器重试授权的四道闸：run 未终态、输入已准备、
                // 针对的是当前那条未决操作（id 与指纹都对得上）、
                // 且操作被显式声明为 repeatable。
                // 任何一条不满足都只能走人工裁决。
                if terminal
                    || node.prepared.is_none()
                    || node.operation.as_ref().is_none_or(|o| {
                        o.outcome.is_some()
                            || p["operation_id"] != o.operation_id
                            || p["fingerprint"] != o.fingerprint
                    })
                    || p["policy"] != "repeatable"
                {
                    return Err(invalid("invalid repeatable-operation retry authorization"));
                }
                // 授权重试后清掉未决操作：结果未知的那次不再有法律效力，
                // 节点转入定时等待，等驱动器以新的 attempt 重新派发。
                node.operation = None;
                node.status = NodeStatus::Waiting;
                node.wait = Some(Wait::Retry {
                    wake_at: p["wake_at"]
                        .as_i64()
                        .ok_or_else(|| invalid("retry deadline required"))?,
                });
                node.error = None;
                run.status = RunStatus::Running;
            }
            EventKind::ValueChunk | EventKind::ValuePublished | EventKind::LateAudit => {}
            _ => return Err(invalid("unsupported event for node")),
        }
        Ok(())
    }
}

/// 判断一个等待载荷是否是「不确定等待」（平台故障面）。
///
/// 只有 uncertain 才把 run 拉到 awaiting_resume（I12）；其余等待类别
/// 都是正常业务等待，run 保持 running（I14）。
fn wait_kind_uncertain(payload: &Value) -> bool {
    payload["wait"]["kind"].as_str() == Some(WaitKind::Uncertain.as_str())
}

/// 写入一条配置类事实（调度 / webhook / 模板 / 旧基线共用）。
///
/// 整条替换而非字段合并：配置是「最后一次写入说了算」的快照语义，
/// 合并会让不同事件的字段混在一起，无法从事实复现出同一份配置。
fn replace_config(map: &mut BTreeMap<String, Value>, payload: &Value, key: &str) -> Result<()> {
    let id = string(payload, key)?;
    if payload.get("deleted") == Some(&Value::Bool(true)) {
        map.remove(&id);
    } else {
        map.insert(id, payload.clone());
    }
    Ok(())
}
/// 从载荷里取一个必填字符串字段。
///
/// 缺失即报错而不是给默认值：载荷字段缺失通常意味着事实被截断或来自
/// 不兼容的旧版本，静默补空串会让折叠出「看起来合法」但实际错误的状态。
pub fn string(v: &Value, key: &str) -> Result<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| invalid(format!("missing string {key}")))
}
/// 从载荷里取一个必填的十进制 u64 字段（按字符串编码）。
///
/// 序号、版本、epoch 等都以十进制字符串落盘（避免 JSON number 的精度问题），
/// 因此这里只接受字符串形式；解析失败即报错。
pub fn number(v: &Value, key: &str) -> Result<u64> {
    v.get(key)
        .and_then(Value::as_str)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| invalid(format!("missing decimal {key}")))
}

/// 校验一份工作流定义的结构与语义。
///
/// 定义是执行器的输入，格式错误的定义一旦落账就会在派发时才失败，
/// 那时已经可能触发了外部副作用；所以必须在写入门禁上就拦住。
///
/// # Errors
/// 结构不符或语义校验不通过时返回错误。
pub fn validate_definition(value: &Value) -> Result<Definition> {
    validate_definition_with_dynamic(value, &Default::default())
}

/// 带动态（插件声明）节点类型的定义校验：backend 在写入口按已安装
/// manifest 计算集合并传入；引擎自身不发现插件。
pub fn validate_definition_with_dynamic(
    value: &Value,
    dynamic_types: &std::collections::BTreeSet<String>,
) -> Result<Definition> {
    let definition: Definition = serde_json::from_value(value.clone())
        .map_err(|e| invalid(format!("definition 结构不符：{e}")))?;
    definition
        .validate_with_dynamic(dynamic_types)
        .map_err(|e| invalid(e.to_string()))?;
    Ok(definition)
}

#[cfg(test)]
mod command_window_tests {
    use super::*;
    /// 构造一个恰好装满 `COMMAND_WINDOW` 条回执的状态。
    fn full_window() -> State {
        let mut state = State {
            journal_id: "window".into(),
            applied_lsn: COMMAND_WINDOW as u64,
            ..Default::default()
        };
        for n in 0..COMMAND_WINDOW {
            let id = n.to_string();
            state.commands.insert(
                command_key("test", &id),
                CommandRecord {
                    scope: "test".into(),
                    request_id: Some(id),
                    fingerprint: "f".into(),
                    result: serde_json::json!(n),
                    lsn: n as u64 + 1,
                },
            );
        }
        state
    }
    /// 装满窗口之后到来的下一条命令事务。
    fn next() -> Transaction {
        Transaction {
            v: 2,
            journal_id: "window".into(),
            lsn: COMMAND_WINDOW as u64 + 1,
            tx_id: "next".into(),
            events: vec![Event::new(
                EventKind::Command,
                serde_json::json!({"scope":"test","request_id":"next","fingerprint":"f","result":null,"lsn":"0"}),
            )],
        }
    }
    /// 超窗淘汰必须是可回滚的：预演与失败提交都不应留下改动，
    /// 而在线折叠与「从零重放」应得到同一份状态（重放确定性）。
    #[test]
    fn checking_and_rollback_restore_evicted_identities() {
        let mut state = full_window();
        let before = serde_json::to_value(&state).unwrap();
        let tx = next();
        state.check(&tx).unwrap();
        assert_eq!(serde_json::to_value(&state).unwrap(), before);
        let mut invalid = tx.clone();
        invalid.events.push(Event::new(
            EventKind::WorkflowDeleted,
            serde_json::json!({}),
        ));
        assert!(state.apply(&invalid).is_err());
        assert_eq!(serde_json::to_value(&state).unwrap(), before);
        state.apply(&tx).unwrap();
        assert_eq!(state.commands.len(), COMMAND_WINDOW / 2);
        let mut replay = full_window();
        replay.apply(&tx).unwrap();
        assert_eq!(
            serde_json::to_value(&state).unwrap(),
            serde_json::to_value(&replay).unwrap()
        );
    }
}

#[cfg(test)]
mod aggregate_status_tests {
    use super::*;
    /// 聚合状态：只要还有任一节点停在不确定等待上，run 就是 awaiting_resume；
    /// 其他分支的活动不得抹掉它；终态则优先，不会被刷新改写。
    #[test]
    fn unresolved_parallel_waits_survive_other_branch_activity_and_terminal_wins() {
        let mut run: Run = serde_json::from_value(serde_json::json!({
            "run_id":"r", "workflow_id":"w", "workflow_version":1,
            "definition":serde_json::to_value(StoredValue::Inline(Value::Null)).unwrap(), "input":serde_json::to_value(StoredValue::Inline(Value::Null)).unwrap(),
            "source":"manual", "source_detail":null, "parent":null, "depth":0,
            "created_at":"now", "status":"running", "output":null, "error":null,
            "last_run_seq":"1", "nodes":{}
        })).unwrap();
        let node = |wait: bool| Node {
            node_execution_id: "n".into(),
            attempt: 1,
            dispatch_id: "d".into(),
            status: if wait {
                NodeStatus::Waiting
            } else {
                NodeStatus::Running
            },
            output: None,
            branch: None,
            prepared: None,
            wait: if wait { Some(Wait::Uncertain) } else { None },
            operation: None,
            attempts: BTreeMap::new(),
            error: None,
            skip_reason: None,
            approval: None,
            permit: None,
        };
        run.nodes.insert("a".into(), node(true));
        run.nodes.insert("b".into(), node(true));
        run.refresh_status();
        assert_eq!(run.status, RunStatus::AwaitingResume);
        run.nodes.get_mut("b").unwrap().wait = None;
        run.status = RunStatus::Running;
        run.refresh_status();
        assert_eq!(run.status, RunStatus::AwaitingResume);
        run.nodes.get_mut("a").unwrap().wait = None;
        run.refresh_status();
        assert_eq!(run.status, RunStatus::Running);
        run.nodes.insert("a".into(), node(true));
        run.status = RunStatus::Cancelled;
        run.refresh_status();
        assert_eq!(run.status, RunStatus::Cancelled);
    }
}

#[cfg(test)]
mod wait_compat_tests {
    use super::*;
    use serde_json::json;
    /// 旧检查点/旧事件的等待形状必须原样往返（显式 null 也要保留），
    /// 而字段与种类不匹配的组合必须反序列化失败。
    #[test]
    fn legacy_wait_shapes_round_trip_and_illegal_combinations_fail() {
        for (kind, wake_at, child_run_id) in [
            ("delay", json!(123), Value::Null),
            ("retry", json!(456), Value::Null),
            ("child", Value::Null, json!("child-1")),
            ("signal", Value::Null, Value::Null),
            ("uncertain", Value::Null, Value::Null),
        ] {
            let old =
                json!({"kind":kind,"wake_at":wake_at,"child_run_id":child_run_id,"output":null});
            let wait: Wait = serde_json::from_value(old.clone()).unwrap();
            assert_eq!(serde_json::to_value(wait).unwrap(), old);
        }
        for bad in [
            json!({"kind":"delay"}),
            json!({"kind":"retry"}),
            json!({"kind":"child"}),
            json!({"kind":"child","child_run_id":""}),
            json!({"kind":"signal","wake_at":1}),
            json!({"kind":"uncertain","child_run_id":"c"}),
            json!({"kind":"retry","wake_at":1,"output":{"type":"inline","value":null}}),
        ] {
            assert!(
                serde_json::from_value::<Wait>(bad.clone()).is_err(),
                "{bad}"
            );
        }
        let signal: Wait = serde_json::from_value(json!({"kind":"signal"})).unwrap();
        assert_eq!(signal.kind(), WaitKind::Signal);
    }

    /// 审批等待：deadline 走 wake_at，身份字段只允许出现在 approval 上。
    #[test]
    fn approval_wait_shape_round_trips_and_is_kind_bound() {
        let approval = json!({"kind":"approval","wake_at":99,"child_run_id":null,"output":null,
            "operation_id":"op","request_hash":"h"});
        let wait: Wait = serde_json::from_value(approval.clone()).unwrap();
        assert_eq!(wait.kind(), WaitKind::Approval);
        assert_eq!(wait.wake_at(), Some(99));
        assert_eq!(serde_json::to_value(wait).unwrap(), approval);
        for bad in [
            json!({"kind":"approval","wake_at":1,"request_hash":"h"}),
            json!({"kind":"approval","operation_id":"op","request_hash":"h"}),
            json!({"kind":"delay","wake_at":1,"operation_id":"op"}),
        ] {
            assert!(serde_json::from_value::<Wait>(bad.clone()).is_err(), "{bad}");
        }
    }
}

#[cfg(test)]
mod permission_fold_tests {
    //! 09 权限事实的折叠语义：策略冻结、询问/裁决状态机、I26 授权闸与
    //! Run 精确授权投影。每个用例都走真实 `State::apply`（I5 唯一实现）。
    use super::*;
    use flow_dto::permission::{
        ApprovalScope, Decision, DecisionRecord, ExecutionMode, FrozenPolicy, Modifiers,
        OperationSpec, PermissionRequest, Principal, RequestContext, WorkflowPermissions,
    };
    use serde_json::json;

    struct Harness {
        state: State,
        lsn: u64,
    }

    impl Harness {
        fn apply(&mut self, events: Vec<Event>) -> Result<()> {
            let tx = Transaction {
                v: 2,
                journal_id: "perm".into(),
                lsn: self.lsn + 1,
                tx_id: format!("tx-{}", self.lsn + 1),
                events,
            };
            self.state.apply(&tx)?;
            self.lsn += 1;
            Ok(())
        }
        fn seq(&self) -> u64 {
            self.state.runs.get("r").map_or(1, |r| r.last_run_seq + 1)
        }
        fn node(&self) -> &Node {
            &self.state.runs["r"].nodes["n"]
        }
    }

    fn policy(legacy: bool) -> FrozenPolicy {
        let workflow = WorkflowPermissions {
            version: 1,
            mode: ExecutionMode::Execute,
            allow: vec![],
        };
        FrozenPolicy::sealed(
            ExecutionMode::Execute,
            legacy,
            workflow,
            BTreeMap::new(),
            vec![],
            vec![],
        )
    }

    fn run_event(kind: EventKind, seq: u64, payload: Value) -> Event {
        let mut event = Event::new(kind, payload);
        event.run_id = Some("r".into());
        event.run_seq = seq;
        event
    }

    fn node_event(kind: EventKind, seq: u64, dispatch: &str, payload: Value) -> Event {
        let mut event = run_event(kind, seq, payload);
        event.node_id = Some("n".into());
        event.dispatch_id = Some(dispatch.into());
        event
    }

    fn inline(value: Value) -> StoredValue {
        StoredValue::Inline(value)
    }

    /// 启动受管 run，派发节点 n（dispatch d1）并提交输入准备。
    fn started(policy: Option<FrozenPolicy>) -> Harness {
        let mut harness = Harness {
            state: State::default(),
            lsn: 0,
        };
        let mut run: Run = serde_json::from_value(json!({
            "run_id":"r", "workflow_id":"w", "workflow_version":1,
            "definition": inline(Value::Null), "input": inline(Value::Null),
            "source":"manual", "source_detail":null, "parent":null, "depth":0,
            "created_at":"now", "status":"running", "output":null, "error":null,
            "last_run_seq":"0", "nodes":{}
        }))
        .unwrap();
        run.policy = policy.clone();
        let mut events = vec![run_event(
            EventKind::RunStarted,
            1,
            serde_json::to_value(&run).unwrap(),
        )];
        if let Some(policy) = &policy {
            events.push(run_event(
                EventKind::PolicyFrozen,
                2,
                json!({"policy_digest": policy.digest, "permission_schema_version": 1,
                       "legacy": policy.legacy}),
            ));
        }
        harness.apply(events).unwrap();
        let seq = harness.seq();
        harness
            .apply(vec![node_event(
                EventKind::DispatchStarted,
                seq,
                "d1",
                json!({"node_execution_id":"x","attempt":"1"}),
            )])
            .unwrap();
        let mut prepared = node_event(
            EventKind::InputPrepared,
            0,
            "d1",
            json!({"prepared": {
                "input": inline(Value::Null), "predecessors": {},
                "params": inline(json!({})), "engine_build": "b", "node_semantics": 2
            }}),
        );
        prepared.audit_seq = 1;
        harness.apply(vec![prepared]).unwrap();
        harness
    }

    fn request(dispatch: &str, operation: &str) -> PermissionRequest {
        PermissionRequest {
            actor: Principal::Node {
                run_id: "r".into(),
                node_id: "n".into(),
            },
            operation_id: operation.into(),
            specs: vec![OperationSpec::Secret {
                secret_name: "k".into(),
            }],
            context: RequestContext {
                run_id: "r".into(),
                workflow_id: "w".into(),
                workflow_version: 1,
                node_id: "n".into(),
                node_type: "http_call".into(),
                dispatch_id: dispatch.into(),
                attempt: 1,
                request_fingerprint: null_fingerprint(),
                tool_call_id: None,
            },
            policy_digest: policy(false).digest,
            request_hash: String::new(),
        }
        .sealed()
    }

    fn decided(request: &PermissionRequest, decision: Decision, status: &str) -> Value {
        json!({
            "gate": "node",
            "record": DecisionRecord {
                operation_id: request.operation_id.clone(),
                request_hash: request.request_hash.clone(),
                evaluated_at: chrono::Utc::now(),
                policy_revision: "rev".into(),
                decision,
                by: Principal::System,
                matched_capabilities: vec![],
                modifiers: Modifiers::default(),
                scope: ApprovalScope::Once,
                reason: None,
            },
            "request_fingerprint": request.context.request_fingerprint,
            "status": status,
        })
    }

    /// 指纹 = 请求字节摘要（I11）：测试请求是内联 null。
    fn null_fingerprint() -> String {
        flow_journal::codec::digest(
            &flow_journal::codec::bounded_json(&Value::Null, flow_journal::INLINE_BYTES).unwrap(),
        )
    }

    fn operation(id: &str) -> Value {
        let digest = null_fingerprint();
        json!({"operation": {"operation_id": id, "fingerprint": digest,
            "permit_id": "p", "request": inline(Value::Null), "outcome": null}})
    }

    fn authorize(harness: &mut Harness, id: &str) -> Result<()> {
        let seq = harness.seq();
        harness.apply(vec![
            node_event(EventKind::OperationIntent, seq, "d1", operation(id)),
            node_event(EventKind::OperationAuthorized, seq + 1, "d1", operation(id)),
        ])
    }

    #[test]
    fn policy_freeze_must_match_and_follow_run_start() {
        let harness = started(Some(policy(false)));
        assert!(harness.state.runs["r"].policy.is_some());
        // 摘要对不上：拒绝。
        let mut bad = Harness {
            state: State::default(),
            lsn: 0,
        };
        let mut run: Run = serde_json::from_value(
            serde_json::to_value(&harness.state.runs["r"].summary()).unwrap(),
        )
        .unwrap();
        run.last_run_seq = 0;
        let events = vec![
            run_event(EventKind::RunStarted, 1, serde_json::to_value(&run).unwrap()),
            run_event(
                EventKind::PolicyFrozen,
                2,
                json!({"policy_digest": "0".repeat(64), "permission_schema_version": 1,
                       "legacy": false}),
            ),
        ];
        assert!(bad.apply(events).is_err());
        // 不紧随 RunStarted：结构规则拒绝。
        let lone = run_event(
            EventKind::PolicyFrozen,
            2,
            json!({"policy_digest": run.policy.as_ref().unwrap().digest,
                   "permission_schema_version": 1, "legacy": false}),
        );
        assert!(bad.apply(vec![lone]).is_err());
    }

    #[test]
    fn i26_authorization_requires_a_recorded_allow() {
        // 受管 run：无 Allow 凭条的授权被折叠层拒绝。
        let mut harness = started(Some(policy(false)));
        assert!(authorize(&mut harness, "op1").is_err());
        // 直接 Allow 裁决后同 operation 可授权，凭条随之消费。
        let req = request("d1", "op1");
        let seq = harness.seq();
        harness
            .apply(vec![node_event(
                EventKind::PermissionDecided,
                seq,
                "d1",
                decided(&req, Decision::Allow, "allowed"),
            )])
            .unwrap();
        assert!(harness.node().permit.is_some());
        authorize(&mut harness, "op1").unwrap();
        assert!(harness.node().permit.is_none());
        assert!(harness.node().operation.is_some());

        // 无冻结策略的旧 run 不受折叠闸约束（master 侧负责拒绝）。
        let mut legacy = started(None);
        authorize(&mut legacy, "op1").unwrap();
    }

    #[test]
    fn allow_for_another_operation_does_not_authorize() {
        let mut harness = started(Some(policy(false)));
        let req = request("d1", "op1");
        let seq = harness.seq();
        harness
            .apply(vec![node_event(
                EventKind::PermissionDecided,
                seq,
                "d1",
                decided(&req, Decision::Allow, "allowed"),
            )])
            .unwrap();
        // 凭条绑定 op1，不能挪给 op2（I36）。
        assert!(authorize(&mut harness, "op2").is_err());
    }

    #[test]
    fn ask_then_decide_is_a_non_sealing_wait() {
        let mut harness = started(Some(policy(false)));
        let req = request("d1", "op1");
        let seq = harness.seq();
        harness
            .apply(vec![node_event(
                EventKind::PermissionAsked,
                seq,
                "d1",
                json!({"gate":"node","request":req,"asked_at":1,"deadline":1000,
                       "operation_request": inline(Value::Null),"matched":["api"]}),
            )])
            .unwrap();
        let node = harness.node();
        assert_eq!(node.status, NodeStatus::Waiting);
        assert_eq!(node.wait.as_ref().unwrap().kind(), WaitKind::Approval);
        // 非封口：attempt 未 sealed，run 仍 Running（不是 AwaitingResume）。
        assert!(!node.attempts["d1"].sealed);
        assert_eq!(harness.state.runs["r"].status, RunStatus::Running);

        // 重复询问被拒（同一时刻只有一个 pending）。
        let seq = harness.seq();
        assert!(harness
            .apply(vec![node_event(
                EventKind::PermissionAsked,
                seq,
                "d1",
                json!({"gate":"node","request":request("d1","op2"),"asked_at":1,
                       "deadline":1000,"operation_request": inline(Value::Null)}),
            )])
            .is_err());

        // 换了 hash 的裁决拒绝（I32）。
        let mut forged = req.clone();
        forged.context.request_fingerprint = "e".repeat(64);
        let forged = forged.sealed();
        let seq = harness.seq();
        assert!(harness
            .apply(vec![node_event(
                EventKind::PermissionDecided,
                seq,
                "d1",
                decided(&forged, Decision::Allow, "allowed"),
            )])
            .is_err());

        // 正确裁决：等待解除、节点回 Running、留下凭条。
        let seq = harness.seq();
        harness
            .apply(vec![node_event(
                EventKind::PermissionDecided,
                seq,
                "d1",
                decided(&req, Decision::Allow, "allowed"),
            )])
            .unwrap();
        let node = harness.node();
        assert_eq!(node.status, NodeStatus::Running);
        assert!(node.wait.is_none());
        assert_eq!(
            node.approval.as_ref().unwrap().status,
            ApprovalStatus::Allowed
        );
        authorize(&mut harness, "op1").unwrap();
    }

    #[test]
    fn expiry_only_applies_to_pending_and_redispatch_supersedes() {
        let mut harness = started(Some(policy(false)));
        let req = request("d1", "op1");
        // 没有 pending 询问时不能「过期」。
        let seq = harness.seq();
        assert!(harness
            .apply(vec![node_event(
                EventKind::PermissionDecided,
                seq,
                "d1",
                decided(&req, Decision::Deny, "expired"),
            )])
            .is_err());
        let seq = harness.seq();
        harness
            .apply(vec![node_event(
                EventKind::PermissionAsked,
                seq,
                "d1",
                json!({"gate":"node","request":req,"asked_at":1,"deadline":1000,
                       "operation_request": inline(Value::Null)}),
            )])
            .unwrap();
        // 状态不一致的裁决（Allow + expired）拒绝。
        let seq = harness.seq();
        assert!(harness
            .apply(vec![node_event(
                EventKind::PermissionDecided,
                seq,
                "d1",
                decided(&req, Decision::Allow, "expired"),
            )])
            .is_err());
        // 会话结束（节点出结果）：pending 询问 withdrawn。
        let seq = harness.seq();
        harness
            .apply(vec![node_event(
                EventKind::NodeFailed,
                seq,
                "d1",
                json!({"error":"session lost","sealed_through":"1"}),
            )])
            .unwrap();
        assert_eq!(
            harness.node().approval.as_ref().unwrap().status,
            ApprovalStatus::Withdrawn
        );
    }

    #[test]
    fn run_approvals_need_a_governed_live_run() {
        let mut harness = started(Some(policy(false)));
        let approval = json!({"node_id":"n","actor":{"kind":"node","run_id":"r","node_id":"n"},
            "digest": "a".repeat(64), "expires_at": null, "by": {"kind":"local_admin"}});
        let seq = harness.seq();
        harness
            .apply(vec![run_event(EventKind::RunApprovalUpserted, seq, approval.clone())])
            .unwrap();
        assert_eq!(harness.state.runs["r"].approvals.len(), 1);
        assert_eq!(harness.state.runs["r"].approval_seq, 1);
        // upsert 同一 (node, digest) 不重复。
        let seq = harness.seq();
        harness
            .apply(vec![run_event(EventKind::RunApprovalUpserted, seq, approval)])
            .unwrap();
        assert_eq!(harness.state.runs["r"].approvals.len(), 1);
        let seq = harness.seq();
        harness
            .apply(vec![run_event(
                EventKind::RunApprovalRemoved,
                seq,
                json!({"node_id":"n","digest":"a".repeat(64),"by":{"kind":"local_admin"}}),
            )])
            .unwrap();
        assert!(harness.state.runs["r"].approvals.is_empty());
        assert_eq!(harness.state.runs["r"].approval_seq, 3);

        // legacy 策略的 run 不接受 Run 授权。
        let mut legacy = started(Some(policy(true)));
        let seq = legacy.seq();
        assert!(legacy
            .apply(vec![run_event(
                EventKind::RunApprovalUpserted,
                seq,
                json!({"node_id":"n","actor":{"kind":"system"},"digest":"a".repeat(64),
                       "expires_at":null,"by":{"kind":"local_admin"}}),
            )])
            .is_err());
    }
}

#[cfg(test)]
mod atomic_value_tests {
    use super::*;
    use serde_json::json;
    /// 值分块前缀 + 收尾命令的批次：预演不留改动，失败整批回滚
    /// （含发布缓存与提交水位），LSN 缺口同样被拒。
    #[test]
    fn preview_and_apply_value_prefix_share_atomic_rollback_and_cursor_rules() {
        let mut state = State::default();
        let reference = flow_journal::ValueRef {
            journal_id: "batch".into(),
            output_id: "v".into(),
            codec: flow_journal::value::ValueCodec::Json,
            version: 1,
            chunk_count: 1,
            total_bytes: 1,
            digest: "0".repeat(64),
        };
        let prefix = Transaction {
            v: 2,
            journal_id: "batch".into(),
            lsn: 1,
            tx_id: "chunk".into(),
            events: vec![Event::new(
                EventKind::ValueChunk,
                json!({"output_id":"v","chunk_index":"0","data":null}),
            )],
        };
        let mut final_tx = Transaction {
            v: 2,
            journal_id: "batch".into(),
            lsn: 2,
            tx_id: "command".into(),
            events: vec![
                Event::new(EventKind::ValuePublished, json!(reference)),
                Event::new(
                    EventKind::WorkflowCreated,
                    json!({"workflow_id":"w","name":"batch","created_at":"2026-10-23T00:00:00Z","versions":{},"deleted":false}),
                ),
            ],
        };
        let before = serde_json::to_value(&state).unwrap();
        assert!(state
            .preview_batch(&[prefix.clone(), final_tx.clone()], |s| Ok(s
                .workflows
                .contains_key("w")))
            .unwrap());
        assert_eq!(serde_json::to_value(&state).unwrap(), before);
        final_tx.events.push(Event::new(
            EventKind::WorkflowPublished,
            json!({"workflow_id":"missing","version":"1"}),
        ));
        assert!(state
            .apply_batch(&[prefix.clone(), final_tx.clone()])
            .is_err());
        assert_eq!(
            serde_json::to_value(&state).unwrap(),
            before,
            "failure must restore publication, state and cursor"
        );
        final_tx.events.pop();
        let mut gap = final_tx.clone();
        gap.lsn = 3;
        assert!(state.apply_batch(&[prefix.clone(), gap]).is_err());
        assert_eq!(serde_json::to_value(&state).unwrap(), before);
        state.apply_batch(&[prefix, final_tx]).unwrap();
        assert_eq!(state.applied_lsn, 2);
        assert!(state.workflows.contains_key("w"));
        assert!(state.values.check(&StoredValue::Ref(reference)).is_ok());
    }
}
