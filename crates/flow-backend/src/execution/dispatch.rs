//! 主进程侧 IPC 派发编排（I05–I08）：审计持久确认、操作许可、结果屏障、
//! 取消排空与持久等待登记。
//!
//! 全部持久事实仍经一期 `Attempt`/`JournalBackend::internal` 单行事务写入，
//! journal 记录与进程内执行逐字节同构：InputPrepared/ValueChunk/
//! ValuePublished/OperationIntent/OperationAuthorized/OperationOutcome/
//! NodeCompleted/NodeFailed/WaitRegistered 及 sealed_through 屏障语义不变。
//!
//! 职责边界：本模块是「master 侧派发编排」——把节点任务发出去、把执行器
//! 的审计与结果收回来、并把两者之间的协议交互收敛成可重放的账本状态。
//! 它**不做**节点计算（那在 flow-executor），也**不**决定谁该跑（那在
//! journal_driver）。
//!
//! 守护的核心不变量：
//! 1. 主进程是唯一权威写者——AuditAck / OperationPermit / ResultCommitted
//!    只能由主进程生成，agent 只转发；执行器自称的确认一律不作数。
//! 2. 结果必须晚于它依赖的审计事实：结果帧带 `last_audit_seq`，未追平
//!    则先挂起等审计，绝不先提交结果再补审计（否则崩溃后会出现「结果
//!    在、证据不在」的不可裁决状态）。
//! 3. 外部副作用先落意图再放行（C3）：OperationIntent 与
//!    OperationAuthorized 必须同一事务提交，许可才可下发。
//!
//! 为什么需要它：崩溃恢复的困难全在「请求发出去了吗」。这里把每一次
//! 对外交互都变成可判定的事实，使恢复期能区分「从未发出」（可安全重试）
//! 与「已授权、结果未知」（必须停下来等人工裁决）。

use crate::journal::JournalError;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use sha2::Digest as _;
use tokio::sync::mpsc;

use crate::journal::JournalBackend;
use crate::journal_execution::Attempt;
use flow_engine::execution_protocol::contract::{
    CANCEL_REAP_TIMEOUT_MS, DURABLE_WAIT_TIMEOUT_MS, TRANSFER_CHUNK_BYTES,
};
use flow_engine::execution_protocol::message::{
    Message, ResultOutcome, WaitKind as WaitRequestKind,
};
use flow_engine::execution_protocol::record_bytes;
use flow_engine::execution_protocol::TransferKey;
use flow_engine::journal_state::{Prepared, Run, RunStatus, Wait, WaitKind};
use flow_engine::observation::ObservationStore;
use flow_engine::{Node, NodeType};
use flow_journal::value::ValueCodec;
use flow_journal::{EventKind, StoredValue};

use super::pool::ExecutorPool;
use super::{FromSession, ToSession};

type Result<T> = std::result::Result<T, crate::journal::JournalError>;
/// 把一条诊断信息包装成 journal 层错误。
///
/// 派发层的所有失败最终都要经由 journal_driver 统一包装成终态或
/// uncertain，因此这里统一走 `flow_journal::Error::Invalid`——错误类型
/// 决定上游如何区分「平台故障」与「工作流失败」，不能随手挑一个。
fn invalid(s: impl Into<String>) -> crate::journal::JournalError {
    crate::journal::JournalError::Journal(flow_journal::Error::Invalid(s.into()))
}
/// 取消后排空截止（P2-8）：见 contract 的 [`CANCEL_REAP_TIMEOUT_MS`]。
const CANCEL_DRAIN_TIMEOUT: Duration = Duration::from_millis(CANCEL_REAP_TIMEOUT_MS);
/// internal() 闭包内的错误类型（flow_journal::Error）。
///
/// 与外部 [`invalid`] 的区别只在类型：闭包跑在 journal 事务内部，只能
/// 返回 `flow_journal::Error`，不能带 journal 外层的上下文包装。
fn fj(s: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(s.into())
}

/// IPC 派发器：journal_driver 调度循环的执行端口（替换进程内 execute）。
/// 本地池全平台（unix socketpair / 其余 loopback 回连）；驱动逻辑见
/// DispatchRunner（远程 agent 链路复用）。
#[derive(Clone)]
pub struct IpcDispatcher {
    pool: Arc<ExecutorPool>,
}

/// 派发链路：命令发送 + 事件流。本地池会话（二期）与远程 agent 路由
/// （三期）共用同一编排；远程模式链路死亡后可重建继续驱动同一 runner。
pub(crate) struct DispatchLink {
    /// 派发方 → 会话（本地执行器或 agent 中继）的命令通道。
    pub commands: mpsc::Sender<ToSession>,
    /// 会话 → 派发方的事件通道；派发侧独占消费。
    pub events: mpsc::Receiver<FromSession>,
}

impl IpcDispatcher {
    /// 绑定一个执行器池。派发器本身无状态：会话的借还由池管理，节点级
    /// 编排状态在每次 `execute` 内新建的 [`DispatchRunner`] 里。
    pub fn new(pool: Arc<ExecutorPool>) -> Self {
        Self { pool }
    }

    /// 关停派发端口：池内全部执行器进程被 kill 并 wait/reap，不留僵尸。
    pub(crate) async fn shutdown(&self) {
        self.pool.shutdown().await;
    }

    /// 预热 `count` 个空闲执行器。仅协调器任务在开始调度前调用；
    /// 失败由调用方降级为首次派发懒加载（见 [`ExecutorPool::prewarm`]）。
    pub(crate) async fn prewarm(
        &self,
        count: usize,
    ) -> std::result::Result<usize, super::pool::PoolError> {
        self.pool.prewarm(count).await
    }

    /// 执行一个节点派发。journal 语义与一期 `journal_driver::execute` 等价；
    /// 失败时返回 Err，由 journal_driver 的统一错误包装写入终态/uncertain。
    pub(crate) async fn execute(
        &self,
        attempt: &Attempt,
        node: &Node,
        predecessors: BTreeMap<String, StoredValue>,
        observations: Option<ObservationStore>,
    ) -> Result<()> {
        let mut lease = self
            .pool
            .acquire()
            .await
            .map_err(|e| invalid(format!("executor pool: {e}")))?;
        let mut runner =
            DispatchRunner::new(attempt.clone(), node.clone(), predecessors, observations);
        let mut link = DispatchLink {
            commands: lease.to_session.clone(),
            // 事件接收端必须从租约里「取走」：派发期间它归派发侧独占消费，
            // 用占位通道换出，避免与池的空闲复用并发读同一条流。
            events: std::mem::replace(&mut lease.events, mpsc::channel(1).1),
        };
        let outcome = runner.drive(&mut link).await;
        // 归还真实事件流（供空闲会话复用）。
        lease.events = std::mem::replace(&mut link.events, mpsc::channel(1).1);
        // A late durable result can win after Cancel was already queued. Its
        // business result is valid, but the process is draining and cannot be reused.
        // 复用条件四项全满足：派发成功、会话仍健康、未发取消、节点不是 harness。
        // harness 节点执行的是不可复用的 agent 会话，即便成功也不进空闲池。
        self.pool
            .release(
                lease,
                outcome.is_ok()
                    && runner.session_healthy
                    && !runner.cancel_sent
                    && !node.is_harness_agent(),
            )
            .await;
        outcome
    }
}

/// T09 确定性故障注入：测试在命名检查点注册 hold 开关；未注册时
/// [`fault::checkpoint`] 立即返回。用于在持久化前后、Permit 发送前后、
/// ResultCommitted 前后确定性断线/取消——代替依赖 sleep 的竞态复现。
/// 注册表全局唯一：使用注入的测试必须串行场景。
/// 仅测试构建与 `fault-injection` feature 编译进二进制（集成测试经
/// flow-backend 的 dev 自依赖启用该 feature）；生产构建用下方空壳，
/// 注册表与同步原语不进产物。
#[cfg(any(test, feature = "fault-injection"))]
pub mod fault {
    use std::collections::HashMap;
    use std::sync::OnceLock;
    use tokio::sync::watch;

    /// 单个检查点的门：`hold` 决定是否暂停，`reached` 通知测试「已到达」。
    #[derive(Clone)]
    struct Gate {
        hold: watch::Receiver<bool>,
        reached: watch::Sender<bool>,
    }
    /// 全局检查点注册表。OnceLock 保证进程内只有一份，Mutex 保护并发注册。
    static REGISTRY: OnceLock<std::sync::Mutex<HashMap<String, Gate>>> = OnceLock::new();

    /// 注册检查点（初始 hold=true：到达即暂停，直至发送端置 false 释放）。
    /// 返回各检查点的发送端；重复注册同名点以最后一次为准。
    pub fn register(points: &[&str]) -> HashMap<String, watch::Sender<bool>> {
        let registry =
            REGISTRY.get_or_init(|| std::sync::Mutex::new(HashMap::<String, Gate>::new()));
        let mut guard = registry.lock().unwrap_or_else(|poison| poison.into_inner());
        let mut senders = HashMap::new();
        for point in points {
            let (sender, receiver) = watch::channel(true);
            let (reached, _) = watch::channel(false);
            guard.insert(
                (*point).to_string(),
                Gate {
                    hold: receiver,
                    reached,
                },
            );
            senders.insert((*point).to_string(), sender);
        }
        senders
    }

    /// 清空注册表（测试场景隔离：drop 接收端使仍在等待的检查点立即放行，
    /// 防止上一场景 panic 残留的 hold 毒化后续场景）。
    pub fn clear() {
        if let Some(registry) = REGISTRY.get() {
            registry
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clear();
        }
    }

    /// Wait for an actual checkpoint arrival, rather than guessing from elapsed time.
    pub async fn wait_reached(name: &str) {
        let mut reached = REGISTRY
            .get()
            .expect("registered gate")
            .lock()
            .unwrap()
            .get(name)
            .expect("registered gate")
            .reached
            .subscribe();
        reached
            .wait_for(|value| *value)
            .await
            .expect("gate sender retained");
    }

    /// A node-scoped gate does not accidentally stop a predecessor's receipt.
    pub async fn checkpoint_for(name: &str, node: &str) {
        checkpoint(&format!("{name}:{node}")).await;
        checkpoint(name).await;
    }

    /// 到达检查点：未注册/已释放立即返回；hold 中则确定性等待释放。
    pub async fn checkpoint(name: &str) {
        let Some(registry) = REGISTRY.get() else {
            return;
        };
        let receiver = {
            registry
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .get(name)
                .cloned()
        };
        if let Some(mut gate) = receiver {
            gate.reached.send_replace(true);
            tracing::debug!(name, "fault checkpoint holding");
            let _ = gate.hold.wait_for(|hold| !hold).await;
        }
    }
}

/// 生产构建的空壳：调用点保留、注册表不进产物，checkpoint 立即返回。
#[cfg(not(any(test, feature = "fault-injection")))]
pub mod fault {
    /// 生产构建：检查点是空操作（立即返回）。
    pub async fn checkpoint(_name: &str) {}
    /// 生产构建：节点级检查点是空操作（立即返回）。
    pub async fn checkpoint_for(_name: &str, _node: &str) {}
}

/// 单个操作请求传输的字节预算（沿用值预算 VALUE_BUDGET）。
/// 超过即拒绝——执行器侧对输入聚合有独立上限，主进程侧再设一道是为了
/// 防止恶意/异常对端用大量传输条目耗尽主进程内存。
const OPERATION_TRANSFER_LIMIT: u64 = flow_journal::VALUE_BUDGET as u64;
/// 并发在途的操作请求传输条目数上限。
/// 条目数即内存占用：每个条目都持有一段已接收的字节缓冲。
const OPERATION_TRANSFER_COUNT: usize = 16;

/// 一个操作请求的传输槽：已收字节 + 收齐声明（总长度与摘要）。
struct OpTransfer {
    /// 连续接收前缀（T07）：单流按序到达，无需块树与排序装配。
    /// 装配期独占可写；消费侧取共享句柄（跨 await 逃借不整段克隆）。
    buf: Arc<Vec<u8>>,
    /// `(total_bytes, digest)`：执行器用 `InputReady` 声明整值长度与摘要。
    ready: Option<(u64, String)>,
}

/// 一个已到达但尚未满足屏障的结果帧。
///
/// 结果帧可能先于它依赖的审计批次到达（两条入站路径的到达序不保证）。
/// 先挂起，等 `durable_seq` 追平 `last_audit_seq` 再提交，避免出现
/// 「结果已提交、证据未落盘」的不可裁决状态。
struct PendingResult {
    /// 执行器给出的结果标识（幂等键）。
    result_id: String,
    /// 该结果依赖的最后一条审计序号。
    last_audit_seq: u64,
    /// 结果内容（成功/失败/等待）。
    outcome: ResultOutcome,
}

/// 一次节点派发的编排状态机。
///
/// 跨链路重建时（远程 Resume）必须保留同一实例：其中的审计游标、
/// 已提交摘要表、结果挂起状态就是「幂等与屏障」的记忆，丢掉它等于允许
/// 同一批审计被重复提交或让结果越过屏障落地。
pub(crate) struct DispatchRunner {
    /// 本次派发的身份与 journal 句柄（终态提交走它）。
    attempt: Attempt,
    /// 被派发的节点定义（重试策略、harness 判定都取自它）。
    node: Node,
    /// 前驱节点的输出值（按 journal 引用/内联两种形态）。
    predecessors: BTreeMap<String, StoredValue>,
    /// 节点可观察性日志的落盘目标；缺省表示该派发不收集观测。
    observations: Option<ObservationStore>,
    /// 已持久化的最后审计序号（连续前缀长度）。
    durable_seq: u64,
    /// 已持久化的审计字节累计（回执里的窗口计费依据）。
    durable_bytes: u64,
    /// 已提交记录的确定性编码摘要（同序号同内容幂等；异内容拒绝）。
    committed: BTreeMap<u64, String>,
    /// 已到达但屏障未满足、暂挂的结果。
    pending_result: Option<PendingResult>,
    /// 在途的操作请求传输槽（按 transfer_id 索引）。
    op_transfers: BTreeMap<String, OpTransfer>,
    /// 是否已发出 Cancel（发出后不再复用该会话）。
    cancel_sent: bool,
    /// Cancel 发出时刻：取消后的排空必须有截止时间——持续心跳但不响应
    /// Cancel 的执行器不能无限占用信号量槽位与会话。
    cancel_sent_at: Option<tokio::time::Instant>,
    /// 会话是否仍可复用（取消/故障后一律置假，会话整体回收重建）。
    session_healthy: bool,
    /// 本次派发的尝试序号（观测日志归属用；在 drive_link 读记录时对齐）。
    task_attempt: u32,
    /// 是否处于远程重放模式（远程链路的空闲判定与取消回收与本地不同）。
    remote_replay: bool,
    /// 连续空闲超时次数（远程链路）：首次按失联走 Resume 对账，第二次
    /// 硬失败——防「静默执行器 ↔ 重挂」无限循环占用槽位。
    idle_strikes: u32,
    /// Parent-side lifecycle bound, retained across remote reconnects. Heartbeats
    /// cannot extend it. The operation request replaces the preparation bound.
    agent_deadline: Option<tokio::time::Instant>,
}

impl DispatchRunner {
    /// 新建一次派发的编排状态。
    ///
    /// 唯一的隐含状态是 harness agent 的生命周期上限：harness 节点可能
    /// 长时间运行，但主进程不能无限期地等它——上限之外必须主动排空，
    /// 否则一个不响应的 agent 会一直占着全局派发槽位。
    fn new(
        attempt: Attempt,
        node: Node,
        predecessors: BTreeMap<String, StoredValue>,
        observations: Option<ObservationStore>,
    ) -> Self {
        // 生命周期上限 = harness 自身最大超时 + 两倍持久等待超时：多出的
        // 两倍留给「准备已提交、等待授权/等待持久 ACK」的正常空档。心跳
        // 不能延长它（见 agent_deadline 字段说明），否则 agent 只要一直
        // 发心跳就能永久占住槽位。
        let agent_deadline = node.is_harness_agent().then(|| {
            tokio::time::Instant::now()
                + Duration::from_millis(
                    flow_engine::exec::HARNESS_TIMEOUT_MAX_MS + 2 * DURABLE_WAIT_TIMEOUT_MS,
                )
        });
        Self {
            attempt,
            node,
            predecessors,
            observations,
            durable_seq: 0,
            durable_bytes: 0,
            committed: BTreeMap::new(),
            pending_result: None,
            op_transfers: BTreeMap::new(),
            cancel_sent: false,
            cancel_sent_at: None,
            session_healthy: true,
            task_attempt: 1,
            remote_replay: false,
            idle_strikes: 0,
            agent_deadline,
        }
    }

    /// 在给定链路上驱动派发；链路死亡返回 `JournalError::LinkClosed`，
    /// 由远程模式重连后继续驱动同一 runner（保留账本/屏障状态）。
    pub(crate) async fn drive(&mut self, link: &mut DispatchLink) -> Result<()> {
        self.drive_link(link, false).await
    }

    /// 远程模式：resume=true 时不重发 Execute（重挂同一执行器继续）。
    pub(crate) async fn drive_with_resume(
        &mut self,
        link: &mut DispatchLink,
        resume: bool,
    ) -> Result<()> {
        self.remote_replay = true;
        self.drive_link(link, resume).await
    }

    /// 构造一个派发 runner（远程模式用）。
    ///
    /// 与 `new` 等价；单独暴露是因为远程派发器拿不到 `IpcDispatcher`
    /// 的会话租约，必须自己建 runner 再交给 `drive_with_resume`。
    pub(crate) fn runner(
        attempt: Attempt,
        node: Node,
        predecessors: BTreeMap<String, StoredValue>,
        observations: Option<ObservationStore>,
    ) -> Self {
        Self::new(attempt, node, predecessors, observations)
    }

    /// `resume`：重连后继续驱动同一任务（不重发 Execute；输入传输幂等
    /// 重传；已授权未 Outcome 的操作重发许可——许可由主进程权威生成，
    /// 绑定当前会话，非旧会话重放）。
    async fn drive_link(&mut self, link: &mut DispatchLink, resume: bool) -> Result<()> {
        let run = self.snapshot().await?;
        let record = run
            .nodes
            .get(&self.attempt.node_id)
            .ok_or_else(|| invalid("dispatch record missing"))?
            .clone();
        if record.dispatch_id != self.attempt.dispatch_id {
            // 陈旧派发：节点已被重试取代，此时提交任何事实都会污染新尝试
            // 的账本。宁可失败也不能写错。
            return Err(invalid("stale dispatch"));
        }
        self.task_attempt = record.attempt;
        // 恢复路径携带上一份结果：重挂后的执行器据此识别「这份结果已经
        // 产出过」，从而走幂等提交而不是重新执行。
        let mut previous_outcome = record
            .operation
            .as_ref()
            .and_then(|o| o.outcome.clone())
            .map(|v| serde_json::to_value(&v))
            .transpose()?;
        let mut node_value = serde_json::to_value(&self.node)?;
        if let Some(execution) = &run.execution {
            if execution.runtime != flow_engine::RUNTIME_ID {
                return Err(invalid("run requires a different executor runtime"));
            }
            let fixture = execution
                .regression
                .as_ref()
                .and_then(|r| r.fixtures.get(&execution.node_key(&self.node.id)));
            if let Some(fixture) = fixture {
                previous_outcome = Some(serde_json::to_value(&fixture.outcome)?);
            }
            let mut execution_value = json!({
                "runtime": execution.runtime, "clock_ms":execution.clock_ms,
                "seed": format!("{}:{}", execution.seed, execution.node_key(&self.node.id)),
                "regression": execution.regression.is_some(),
                "fixture_fingerprint": fixture.map(|f| &f.fingerprint),
            });
            // 冻结的 agent 配置（与 operation 请求里的 agent 是同一份快照的
            // 两次序列化；测试断言两者相等）。旧 journal / 未配置时无此键。
            if let Some(agent) = &execution.agent {
                execution_value["agent"] = serde_json::to_value(agent)?;
            }
            node_value["_flow_execution"] = execution_value;
        }
        let task = flow_engine::execution_protocol::ExecuteTask {
            run_id: self.attempt.run_id.clone(),
            node_id: self.attempt.node_id.clone(),
            node_execution_id: record.node_execution_id.clone(),
            attempt: record.attempt,
            node: node_value,
            input: task_value(&run.input),
            predecessors: self
                .predecessors
                .iter()
                .map(|(id, value)| (id.clone(), task_value(value)))
                .collect(),
            prepared: record.prepared.as_ref().map(|committed| {
                flow_engine::execution_protocol::PreparedMeta {
                    engine_build: committed.engine_build.clone(),
                    node_semantics: committed.node_semantics,
                    params: task_value(&committed.params),
                }
            }),
            previous_outcome: previous_outcome.clone(),
            observability: self.observations.is_some(),
        };
        let command_id = format!("cmd-{}", uuid::Uuid::now_v7());
        // 命令标识用 v7 UUID：时间有序便于日志按发生顺序定位，全局唯一
        // 便于跨会话追踪同一条命令。
        let execute = Message::Execute {
            command_id,
            dispatch_id: self.attempt.dispatch_id.clone(),
            task: Box::new(task),
        };
        // 整帧预检（C6/I23，P2-7）：对整个 Execute 消息的真实 wire 编码长度
        // 做上限校验。半预算分别预检 node/previous_outcome 会漏掉两者相加
        // 再加 predecessors 声明的越界——超限若漏到会话编码点爆发，
        // `encode_frame` 报 FrameTooLarge 会杀掉整条会话（远程模式是整条
        // agent 上联），这正是「拒绝派发而不是杀会话」要防的失败模式。
        if !resume {
            let encoded = postcard::to_allocvec(&execute)
                .map_err(|e| invalid(format!("execute encode probe: {e}")))?;
            if encoded.len() > flow_engine::execution_protocol::CONTROL_MAX_FRAME {
                return Err(invalid(format!(
                    "execute frame exceeds control budget: {} bytes > {}",
                    encoded.len(),
                    flow_engine::execution_protocol::CONTROL_MAX_FRAME
                )));
            }
        }
        if !resume {
            // 首挂：Execute 只发一次。resume 路径重发它等于让执行器重跑
            // 节点——而恢复期的正确做法是靠审计重传与结果封口推进。
            link.commands
                .send(ToSession::Send(Box::new(execute)))
                .await
                .map_err(|_| invalid("executor session closed before execute"))?;
        } else {
            // 重挂：授权已提交但 Outcome 未到（许可可能在断线中丢失）时
            // 重发当前会话许可；其余靠执行器审计重传自然恢复。
            if let Some(operation) = record.operation.clone() {
                if operation.outcome.is_none() && !self.cancel_sent {
                    let request_value = match &operation.request {
                        StoredValue::Inline(value) => Some(value.clone()),
                        _ => None,
                    };
                    let credential = request_value.and_then(|v| {
                        v["credential"]["secret_ref"]
                            .as_str()
                            .and_then(flow_engine::secrets::get_secret)
                            .filter(|s| !s.is_empty())
                    });
                    // 许可证丢失会让节点挂到 idle 超时——发送失败必须留痕。
                    if link
                        .commands
                        .send(ToSession::Send(Box::new(Message::OperationPermit {
                            dispatch_id: self.attempt.dispatch_id.clone(),
                            operation_id: operation.operation_id.clone(),
                            permit_id: operation.permit_id.clone(),
                            request_fingerprint: operation.fingerprint.clone(),
                            credential,
                        })))
                        .await
                        .is_err()
                    {
                        tracing::warn!(
                            dispatch = %self.attempt.dispatch_id,
                            "operation permit resend dropped: session link closed"
                        );
                    }
                }
            }
        }
        // 输入传输（独立任务，与审计读取并发；data 通道）。
        let transfer_session = link.commands.clone();
        let transfer_backend = self.attempt.backend.clone();
        let transfer_input = run.input.clone();
        let transfer_preds = self.predecessors.clone();
        let transfer_prepared = record.prepared.clone();
        let transfer_outcome = previous_outcome.clone();
        let mut transfer = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(transfer_inputs(
            transfer_session,
            self.attempt.dispatch_id.clone(),
            transfer_backend,
            transfer_input,
            transfer_preds,
            transfer_prepared,
            transfer_outcome,
        )));
        let mut transfer_done = false;
        // 主事件循环。
        //
        // 时限语义（契约 §3.7/DURABLE_WAIT_TIMEOUT_MS）：这是「等待持久
        // ACK 的总超时上限」，不是派发总墙钟——长任务（harness 默认 300s、
        // 长 HTTP）在执行期间持续有心跳/审计流量，按空闲判定：任何入站
        // 事件或输入传输完成都重置计时。彻底静默超过上限才按失联处理
        // （远程进入 Resume 对账，与 contract 的「按失联处理进入排空」
        // 一致）；连续两次空闲（重挂后仍无活跃证据）才硬失败，防无限
        // 占用槽位。本地会话另有 10s 心跳看门狗（pool::session_driver），
        // 空闲上限只对远程链路生效。
        let idle_limit = Duration::from_millis(DURABLE_WAIT_TIMEOUT_MS);
        let mut last_progress = tokio::time::Instant::now();
        // 100ms 轮询节拍：只用于检查「取消排空截止」与「agent 生命周期
        // 上限」这两个时间型条件。真正的取消触发靠提交通知（commits），
        // 不靠轮询——轮询只保证截止时间到点时能被及时处理。
        let mut cancel_tick = tokio::time::interval(Duration::from_millis(100));
        let mut commits = self.attempt.backend.subscribe_commits();
        commits.mark_changed(); // Also check a cancellation committed before subscribing.
        let mut error: Option<crate::journal::JournalError> = None;
        loop {
            if let Some(err) = error.take() {
                transfer.abort();
                if !transfer_done {
                    let _ = (&mut transfer).await;
                }
                return Err(err);
            }
            // 取消后排空有截止时间（P2-8）：持续心跳但不响应 Cancel 的
            // 执行器（用户脚本/执行器 bug）不能无限占用 x_max 槽位与协调
            // 任务——本地池 draining 期间看门狗停摆、idle 判定被 cancel_sent
            // 跳过，没有这道闸几个这样的派发就能停摆全部本地执行。本地会话
            // 直接 Recycle（单执行器一会话）；远程路由已在 CancelDispatch
            // 移除，这里只释放派发方，agent 侧有自己的收割截止。
            if self.cancel_sent
                && self
                    .cancel_sent_at
                    .is_some_and(|at| at.elapsed() > CANCEL_DRAIN_TIMEOUT)
            {
                transfer.abort();
                if !transfer_done {
                    let _ = (&mut transfer).await;
                }
                if !self.remote_replay {
                    let _ = link.commands.send(ToSession::Recycle).await;
                }
                return Err(invalid(
                    "executor did not stop after cancel within drain deadline",
                ));
            }
            if self.remote_replay && !self.cancel_sent && last_progress.elapsed() > idle_limit {
                transfer.abort();
                if !transfer_done {
                    let _ = (&mut transfer).await;
                }
                self.idle_strikes += 1;
                self.session_healthy = false;
                if self.idle_strikes >= 2 {
                    // 重挂后仍然彻底静默：执行器已失联且对账无法恢复，
                    // 硬失败（journal_driver 按授权操作状态包装 uncertain）。
                    return Err(invalid("dispatch idle timeout after reconnect; giving up"));
                }
                return Err(JournalError::LinkClosed(
                    "dispatch idle timeout; awaiting reconciliation".into(),
                ));
            }
            tokio::select! {
                result = &mut transfer, if !transfer_done => {
                    transfer_done = true;
                    last_progress = tokio::time::Instant::now();
                    self.idle_strikes = 0;
                    match result {
                        Ok(Ok(())) => {},
                        Ok(Err(InputTransferError::LinkClosed)) => {
                            self.session_healthy = false;
                            return Err(JournalError::LinkClosed("input transfer link closed".into()));
                        }
                        Ok(Err(InputTransferError::Invalid(reason))) => {
                            self.session_healthy = false;
                            return Err(invalid(format!("input transfer failed: {reason}")));
                        }
                        Err(error) => {
                            self.session_healthy = false;
                            return Err(invalid(format!("input transfer stopped: {error}")));
                        }
                    }
                }
                event = link.events.recv() => {
                    match event {
                        Some(FromSession::Incoming(message)) => {
                            // 任何入站帧（审计/ACK/结果/心跳）都是活跃证据，
                            // 也复位空闲计数——「连续两次」应为相邻空闲。
                            last_progress = tokio::time::Instant::now();
                            self.idle_strikes = 0;
                            match self.handle(*message, link).await {
                                Ok(Some(())) => {
                                    // 任务完成（ResultCommitted 已发）。
                                    transfer.abort();
                                    if !transfer_done { let _ = (&mut transfer).await; }
                                    return Ok(());
                                }
                                Ok(None) => {}
                                Err(err) => {
                                    error = Some(err);
                                    if !self.cancel_sent {
                                        // 协议/会话故障：回收会话。
                                        self.session_healthy = false;
                                    }
                                }
                            }
                        }
                        Some(FromSession::Dead(reason)) => {
                            let cancelled = self.cancel_sent;
                            transfer.abort();
                            if !transfer_done { let _ = (&mut transfer).await; }
                            if cancelled {
                                return Err(invalid(format!("executor stopped after cancel: {reason}")));
                            }
                            self.session_healthy = false;
                            return Err(JournalError::LinkClosed(format!("executor session dead: {reason}")));
                        }
                        None => {
                            transfer.abort();
                            if !transfer_done { let _ = (&mut transfer).await; }
                            self.session_healthy = false;
                            return Err(JournalError::LinkClosed("executor session closed".into()));
                        }
                    }
                }
                state_changed = async {
                    tokio::select! {
                        result = commits.changed() => result.is_ok(),
                        _ = cancel_tick.tick() => false,
                    }
                } => {
                    // 持久取消观察：run 终态即发 Cancel 并排空回收。
                    // 提交通知才读取状态；100ms 定时器只检查截止时间和排空。
                    // 不为每个派发定时抢状态锁，也不复制整个 run。
                    if !self.cancel_sent {
                        let run_id = self.attempt.run_id.clone();
                        let terminal = state_changed && self
                            .attempt
                            .backend
                            .inspect(|s| {
                                s.runs.get(&run_id).is_none_or(|run| run.terminal())
                            })
                            .await;
                        let deadline_exceeded = self.agent_deadline
                            .is_some_and(|deadline| tokio::time::Instant::now() >= deadline);
                        if terminal || deadline_exceeded {
                            if deadline_exceeded {
                                tracing::warn!(dispatch = %self.attempt.dispatch_id,
                                    "agent lifecycle deadline reached; draining executor");
                            }
                            self.cancel_sent = true;
                            self.cancel_sent_at = Some(tokio::time::Instant::now());
                            self.session_healthy = false; // 取消后进程整体回收
                            let dispatch = self.attempt.dispatch_id.clone();
                            let _ = link
                                .commands
                                .send(ToSession::CancelDispatch(dispatch))
                                .await;
                        }
                    }
                }
            }
        }
    }

    /// 读取该 run 的当前已提交状态快照。
    ///
    /// 派发期所有裁决（准备是否已提交、授权是否已发出、是否已终态）都以
    /// 这份快照为准——它是 journal 的权威视图，不是内存缓存。
    async fn snapshot(&self) -> Result<Run> {
        self.attempt
            .backend
            .run_snapshot(&self.attempt.run_id)
            .await?
            .ok_or_else(|| invalid("run missing"))
    }

    /// 处理一条来自执行器的消息。Ok(Some(())) = 派发完成。
    async fn handle(&mut self, message: Message, link: &mut DispatchLink) -> Result<Option<()>> {
        match message {
            Message::Accepted { dispatch_id, .. } => {
                if dispatch_id != self.attempt.dispatch_id {
                    return Err(invalid("accepted for foreign dispatch"));
                }
                Ok(None)
            }
            Message::AuditBatch {
                dispatch_id,
                records,
                ..
            } => {
                if dispatch_id != self.attempt.dispatch_id {
                    return Err(invalid("audit for foreign dispatch"));
                }
                self.handle_audit(records, link).await?;
                if let Some(pending) = self.pending_result.take() {
                    if self.durable_seq >= pending.last_audit_seq {
                        self.apply_result(pending, link).await?;
                        return Ok(Some(()));
                    }
                    self.pending_result = Some(pending);
                }
                Ok(None)
            }
            Message::Result {
                dispatch_id,
                result_id,
                last_audit_seq,
                outcome,
            } => {
                if dispatch_id != self.attempt.dispatch_id {
                    return Err(invalid("result for foreign dispatch"));
                }
                if self.durable_seq >= last_audit_seq {
                    let pending = PendingResult {
                        result_id,
                        last_audit_seq,
                        outcome,
                    };
                    self.apply_result(pending, link).await?;
                    return Ok(Some(()));
                }
                if self.pending_result.is_some() {
                    return Err(invalid("duplicate result"));
                }
                self.pending_result = Some(PendingResult {
                    result_id,
                    last_audit_seq,
                    outcome,
                });
                Ok(None)
            }
            Message::RequestOperation {
                ref transfer_id, ..
            } => {
                // 单流承载（v3）：执行器按「块 → InputReady → 请求」的顺序
                // 提交，请求到达时块必然已齐；不齐即传输损坏，明确拒绝。
                if !self.op_transfer_complete(transfer_id) {
                    return Err(invalid(
                        "operation request arrived before its transfer completed",
                    ));
                }
                self.handle_request_operation(message, link).await?;
                Ok(None)
            }
            Message::TransferChunk {
                dispatch_id,
                transfer_id,
                offset,
                bytes,
                digest,
            } => {
                if dispatch_id != self.attempt.dispatch_id {
                    return Err(invalid("transfer chunk for foreign dispatch"));
                }
                // T12：bytes 已是内存中的原始字节。
                let expected = hex_sha(&bytes);
                if expected != digest {
                    return Err(invalid("transfer chunk digest mismatch"));
                }
                let received: u64 = self.op_transfers.values().map(|e| e.buf.len() as u64).sum();
                // 三重预算都在**写入缓冲之前**判定：单块偏移越界、累计字节
                // 超限、新建条目数超限——先判后收，避免畸形对端靠「先占住
                // 内存再报错」耗尽主进程预算。
                if offset
                    .checked_add(bytes.len() as u64)
                    .is_none_or(|end| end > OPERATION_TRANSFER_LIMIT)
                    || received.saturating_add(bytes.len() as u64) > OPERATION_TRANSFER_LIMIT
                    || (!self.op_transfers.contains_key(&transfer_id)
                        && self.op_transfers.len() >= OPERATION_TRANSFER_COUNT)
                {
                    return Err(invalid("operation transfer exceeds input budget"));
                }
                let entry = self.op_transfers.entry(transfer_id).or_insert(OpTransfer {
                    buf: Arc::new(Vec::new()),
                    ready: None,
                });
                let end = offset + bytes.len() as u64;
                let received = entry.buf.len() as u64;
                if end <= received {
                    // 幂等重复：完全落在已接收前缀且字节相同（重连后全量重发，T07）。
                    if entry.buf[offset as usize..end as usize] == bytes[..] {
                        return Ok(None);
                    }
                    return Err(invalid("duplicate transfer chunk"));
                }
                if offset != received {
                    // 缺口/跨边界重叠：单流按序到达下直接拒绝。
                    return Err(invalid("duplicate transfer chunk"));
                }
                if let Some((total, _)) = &entry.ready {
                    if end > *total {
                        return Err(invalid("operation transfer exceeds input budget"));
                    }
                }
                Arc::make_mut(&mut entry.buf).extend_from_slice(&bytes);
                Ok(None)
            }
            Message::InputReady {
                dispatch_id,
                transfer_id,
                total_bytes,
                digest,
            } => {
                if dispatch_id != self.attempt.dispatch_id
                    || total_bytes > OPERATION_TRANSFER_LIMIT
                    || (!self.op_transfers.contains_key(&transfer_id)
                        && self.op_transfers.len() >= OPERATION_TRANSFER_COUNT)
                {
                    return Err(invalid("invalid operation transfer identity or size"));
                }
                let entry = self.op_transfers.entry(transfer_id).or_insert(OpTransfer {
                    buf: Arc::new(Vec::new()),
                    ready: None,
                });
                // 收齐声明：整值长度与摘要在此刻才被信任（此前只有分块）。
                entry.ready = Some((total_bytes, digest));
                Ok(None)
            }
            Message::ObservabilityBatch { dispatch_id, lines } => {
                let Some(store) = &self.observations else {
                    return Ok(None);
                };
                let batch_dispatch =
                    dispatch_id.unwrap_or_else(|| self.attempt.dispatch_id.clone());
                // 单流承载（v3）：观测批先于 Result 提交（同一发送队列），
                // 到达序即提交序；dispatch_id 归属回退仍保留——防中继/重传
                // 路径的迟到副本错记到当前节点（错记比丢弃更糟）。
                let (run_id, node_id, attempt) = if batch_dispatch == self.attempt.dispatch_id {
                    (
                        self.attempt.run_id.clone(),
                        self.attempt.node_id.clone(),
                        self.task_attempt,
                    )
                } else {
                    match resolve_dispatch(&self.attempt.backend, &batch_dispatch).await {
                        Some(found) => found,
                        None => {
                            tracing::warn!(
                                stale = %batch_dispatch,
                                count = lines.len(),
                                "unresolvable observability batch dropped"
                            );
                            return Ok(None);
                        }
                    }
                };
                let logger = store.logger(run_id, batch_dispatch);
                for line in lines {
                    logger.emit(flow_engine::nodelog::LogLine {
                        node_id: node_id.clone(),
                        attempt,
                        level: level_of(&line.level),
                        stream: stream_of(&line.stream),
                        message: line.message,
                    });
                }
                Ok(None)
            }
            Message::Stopped {
                dispatch_id,
                sealed,
                ..
            } => {
                if dispatch_id != self.attempt.dispatch_id {
                    return Ok(None);
                }
                // 取消排空收到停止声明：缺口按 incomplete 交由调用方包装。
                let _ = sealed;
                self.session_healthy = false;
                Err(invalid("executor stopped after cancel"))
            }
            Message::Reject { reason } => Err(invalid(format!("executor rejected: {reason}"))),
            Message::Heartbeat { .. } => Ok(None),
            other => Err(invalid(format!(
                "unexpected message from executor: {}",
                other.type_name()
            ))),
        }
    }

    /// 操作请求传输是否已齐（连续前缀长度与整值摘要校验）。
    fn op_transfer_complete(&self, transfer_id: &str) -> bool {
        let Some(entry) = self.op_transfers.get(transfer_id) else {
            return false;
        };
        let Some((total, digest)) = &entry.ready else {
            return false;
        };
        entry.buf.len() as u64 == *total
            && hex::encode(sha2::Sha256::digest(entry.buf.as_slice())) == *digest
    }

    /// 审计批次：连续性 + 同序号同内容幂等 + 单事务持久化 + AuditAck。
    async fn handle_audit(
        &mut self,
        records: Vec<flow_engine::execution_protocol::AuditRecord>,
        link: &mut DispatchLink,
    ) -> Result<()> {
        tracing::info!(
            count = records.len(),
            first = records.first().map(|r| r.audit_seq),
            "ipc: AuditBatch"
        );
        let mut new_records = Vec::new();
        for record in records {
            let digest = hex_sha(
                &serde_json::to_vec(&wire_record(
                    record.audit_seq,
                    &record.kind,
                    &record.payload,
                ))
                .map_err(|e| invalid(e.to_string()))?,
            );
            if record.audit_seq <= self.durable_seq {
                // 幂等重传：同序号同原始内容放行，异内容协议错误。
                match self.committed.get(&record.audit_seq) {
                    Some(previous) if *previous == digest => continue,
                    _ => return Err(invalid("audit retransmit with different content")),
                }
            }
            if record.audit_seq != self.durable_seq + new_records.len() as u64 + 1 {
                // 连续性闸门：批内必须首尾相接地接在已持久前缀之后。
                // 有缺口就意味着中间有审计丢了——此时落盘会让 journal 出现
                // 无法解释的空洞，恢复期无法裁决，因此整批拒绝。
                return Err(invalid("audit gap/duplicate"));
            }
            new_records.push((record, digest));
        }
        if !new_records.is_empty() {
            let dispatch_id = self.attempt.dispatch_id.clone();
            let run_id = self.attempt.run_id.clone();
            let node_id = self.attempt.node_id.clone();
            let prepared = new_records
                .iter()
                .map(|(record, digest)| (record.clone(), digest.clone()))
                .collect::<Vec<_>>();
            self.attempt
                .backend
                .internal_for_run(&self.attempt.run_id, move |state| {
                    let run = state.runs.get(&run_id).ok_or_else(|| fj("run missing"))?;
                    let node = run
                        .nodes
                        .get(&node_id)
                        .ok_or_else(|| fj("dispatch missing"))?;
                    if node.dispatch_id != dispatch_id {
                        return Err(fj("stale dispatch"));
                    }
                    let mut events = Vec::new();
                    for (record, _) in &prepared {
                        let kind = event_kind(&record.kind)
                            .ok_or_else(|| fj(format!("unknown audit kind {}", record.kind)))?;
                        let mut event = crate::journal_commands::node_event(
                            run,
                            &node_id,
                            kind,
                            record.payload.clone(),
                            false,
                        );
                        event.dispatch_id = Some(dispatch_id.clone());
                        event.audit_seq = record.audit_seq;
                        events.push(event);
                    }
                    Ok((events, ()))
                })
                .await?;
            for (record, digest) in new_records {
                // 已持久前缀推进到本批最后一条；摘要入库供同序号重传比对。
                self.durable_seq = record.audit_seq;
                self.durable_bytes += record_bytes(record.audit_seq, &record.kind, &record.payload);
                self.committed.insert(record.audit_seq, digest);
            }
        }
        // AuditAck 仅随连续持久前缀前进；commit_lsn 取 durable 位置。
        let commit_lsn = self.attempt.backend.journal.durable_lsn();
        // Ack 丢失会让执行器永远不知道审计已持久——失败必须留痕。
        if link
            .commands
            .send(ToSession::Send(Box::new(Message::AuditAck {
                dispatch_id: self.attempt.dispatch_id.clone(),
                durable_audit_seq: self.durable_seq,
                durable_bytes: self.durable_bytes,
                commit_lsn,
            })))
            .await
            .is_err()
        {
            tracing::warn!(
                dispatch = %self.attempt.dispatch_id,
                "audit ack dropped: session link closed"
            );
        }
        Ok(())
    }

    /// 结果屏障满足后的单事务接受。
    async fn apply_result(
        &mut self,
        pending: PendingResult,
        link: &mut DispatchLink,
    ) -> Result<()> {
        let result_id = pending.result_id.clone();
        match pending.outcome {
            ResultOutcome::Success { output, branch } => {
                self.attempt
                    .finish(
                        EventKind::NodeCompleted,
                        json!({"output": output, "branch": branch}),
                    )
                    .await?;
            }
            ResultOutcome::Failure {
                error,
                uncertain_operation,
            } => {
                // 与一期 journal_driver 错误包装同构的失败终态。
                let snapshot = self.snapshot().await?;
                let record = &snapshot.nodes[&self.attempt.node_id];
                let uncertain = uncertain_operation
                    || record
                        .operation
                        .as_ref()
                        .is_some_and(|o| o.outcome.is_none());
                // 「已授权但没有 Outcome」= 外部副作用的状态未知。这类失败
                // 不能包装成普通失败（那会让调度器自动重试，等于把「可能
                // 已经扣过款」当成「肯定没扣」），必须进 uncertain 等人工
                // 裁决（C4）。
                let message = if uncertain && !error.starts_with("external outcome uncertain") {
                    format!("external outcome uncertain: {error}")
                } else {
                    error
                };
                if uncertain {
                    self.attempt
                        .finish(
                            EventKind::WaitRegistered,
                            json!({"wait": Wait::Uncertain, "integrity": "unknown"}),
                        )
                        .await?;
                } else {
                    let mut payload = crate::journal_execution::failure_evidence(
                        &message,
                        record.prepared.is_none(),
                        &snapshot.input,
                        &self.predecessors,
                    );
                    let policy = self.node.retry();
                    // 自动重试的三个前提：准备已提交（输入确定）、没有外部
                    // 操作授权（无副作用）、节点类型是纯本地计算（script/
                    // condition）。缺一不可——否则重试就是重复副作用。
                    if record.prepared.is_some()
                        && record.operation.is_none()
                        && matches!(
                            self.node.kind(),
                            Some(NodeType::Script | NodeType::Condition)
                        )
                        && record.attempt < policy.max_attempts.min(100)
                    {
                        // 退避时刻由主进程派生并写入载荷：重试时机是平台
                        // 决策，不能交给执行器自选（否则退避策略形同虚设）。
                        let delay = policy.backoff_ms.min(i64::MAX as u64) as i64;
                        payload["retry_wake_at"] =
                            json!(chrono::Utc::now().timestamp_millis().saturating_add(delay));
                    }
                    self.attempt.finish(EventKind::NodeFailed, payload).await?;
                }
            }
            ResultOutcome::Wait { wait } => {
                // wire 的 WaitRequest.kind 是闭集枚举（postcard 变体索引，
                // 非法变体在解码层即拒）；这里映射进 journal 词汇表，
                // 不再有裸字符串比对。
                let kind = match wait.kind {
                    WaitRequestKind::Delay => WaitKind::Delay,
                    WaitRequestKind::Signal => WaitKind::Signal,
                    WaitRequestKind::Child => WaitKind::Child,
                };
                match kind {
                    WaitKind::Delay | WaitKind::Signal => {
                        self.attempt
                            .finish(
                                EventKind::WaitRegistered,
                                json!({"wait": match kind {
                                    WaitKind::Delay => Wait::Delay {
                                        wake_at: wait.wake_at.ok_or_else(|| invalid("delay missing wake_at"))?,
                                        output: wait.output.clone(),
                                    },
                                    _ => Wait::Signal { output: wait.output.clone() },
                                }}),
                            )
                            .await?;
                    }
                    WaitKind::Child => {
                        let workflow_id = wait
                            .workflow_id
                            .clone()
                            .ok_or_else(|| invalid("child wait missing workflow"))?;
                        self.register_child_run(&workflow_id, wait.input_mapping.clone())
                            .await?;
                    }
                    // Uncertain/Retry 由主进程派生，不是执行器可请求的等待类别。
                    _ => return Err(invalid("wait kind not requestable")),
                }
            }
        }
        // 结果已提交：回执使执行器释放槽位（重复派发返回原提交）。
        fault::checkpoint_for("result_committed:before", &self.attempt.node_id).await;
        let _ = link
            .commands
            .send(ToSession::Send(Box::new(Message::ResultCommitted {
                dispatch_id: self.attempt.dispatch_id.clone(),
                result_id,
            })))
            .await;
        fault::checkpoint_for("result_committed:after", &self.attempt.node_id).await;
        Ok(())
    }

    /// sub_workflow：同一结果接受事务创建子 run + 登记父子等待
    /// （一期 `start_child` 同构）。
    async fn register_child_run(
        &mut self,
        workflow_id: &str,
        input_mapping: Option<Value>,
    ) -> Result<()> {
        let run = self.snapshot().await?;
        let record = &run.nodes[&self.attempt.node_id];
        let prepared: Prepared = record
            .prepared
            .clone()
            .ok_or_else(|| invalid("child registration before preparation"))?;
        // Avoid putting an arbitrarily large mapping in the Result control frame.
        let input_mapping = match input_mapping {
            Some(mapping) => Some(mapping),
            None => {
                let root = self.attempt.backend.journal.root().to_path_buf();
                let upper = self.attempt.backend.journal.durable_lsn();
                let params = prepared.params.clone();
                let params = tokio::task::spawn_blocking(move || {
                    flow_journal::value::materialize(
                        &root,
                        upper,
                        &params,
                        flow_journal::VALUE_BUDGET,
                    )
                })
                .await
                .map_err(|e| invalid(e.to_string()))??;
                params.get("input_mapping").cloned()
            }
        };
        let backend = self.attempt.backend.clone();
        let frozen = backend
            .inspect(|state| {
                child_definition(state, &run, workflow_id, &self.attempt.node_id).cloned()
            })
            .await?;
        let definition = backend
            .cached_definition(
                &format!("child:{workflow_id}@{}:{}", frozen.version, frozen.checksum),
                &frozen.definition,
            )
            .await?;
        let (input, staged) = if let Some(mapping) = input_mapping {
            crate::journal_commands::validate_run_input(&definition, &mapping)?;
            let value = backend
                .journal
                .prepare_json(mapping, flow_journal::VALUE_BUDGET)
                .await?;
            (value.stored().clone(), Some(value))
        } else {
            let input = prepared.input.clone();
            if definition.input_schema.is_some() {
                let root = backend.journal.root().to_path_buf();
                let upper = backend.journal.durable_lsn();
                let value = input.clone();
                let actual = tokio::task::spawn_blocking(move || {
                    flow_journal::value::materialize(
                        &root,
                        upper,
                        &value,
                        flow_journal::VALUE_BUDGET,
                    )
                })
                .await
                .map_err(|e| invalid(e.to_string()))??;
                crate::journal_commands::validate_run_input(&definition, &actual)?;
            }
            (input, None)
        };
        let run_id = self.attempt.run_id.clone();
        let node_id = self.attempt.node_id.clone();
        let dispatch_id = self.attempt.dispatch_id.clone();
        let workflow_id = workflow_id.to_string();
        backend
            .internal_for_run_with_value(&self.attempt.run_id, staged, move |state| {
                let parent = &state.runs[&run_id];
                if parent.terminal() {
                    return Err(fj("parent terminal"));
                }
                if parent.depth >= flow_engine::MAX_SUB_WORKFLOW_DEPTH {
                    return Err(fj("child depth exceeded"));
                }
                // R_max 是主进程在飞 run 状态数的上限（不是协议量）：子 run
                // 也要占主进程状态，必须在同一事务里判定，否则并发注册会
                // 冲破上限。
                if state
                    .runs
                    .values()
                    .filter(|r| !r.terminal())
                    .count()
                    >= crate::journal::R_MAX
                {
                    return Err(flow_journal::Error::Limit(format!(
                        "R_max={}",
                        crate::journal::R_MAX
                    )));
                }
                let definition = child_definition(state, parent, &workflow_id, &node_id)?;
                if definition.version != frozen.version || definition.checksum != frozen.checksum {
                    return Err(flow_journal::Error::Conflict("child definition changed before registration".into()));
                }
                let child_id = crate::journal_commands::id();
                let child = flow_engine::journal_state::Run {
                    run_id: child_id.clone(),
                    workflow_id: workflow_id.clone(),
                    workflow_version: definition.version,
                    definition: definition.definition.clone(),
                    input,
                    source: "sub_workflow".into(),
                    source_detail: None,
                    parent: Some(flow_engine::journal_state::Parent {
                        run_id: run_id.clone(),
                        node_id: node_id.clone(),
                    }),
                    depth: parent.depth + 1,
                    created_at: crate::journal_commands::now(),
                    status: RunStatus::Running,
                    output: None,
                    error: None,
                    last_run_seq: 0,
                    nodes: BTreeMap::new(),
                    execution: parent.execution.clone().map(|mut e| { e.path.push(node_id.clone()); e }),
                    signals: parent.execution.clone().map(|mut e| { e.path.push(node_id.clone()); e.signal_inputs() }).unwrap_or_default(),
                };
                let mut created = flow_journal::Event::new(
                    EventKind::RunStarted,
                    serde_json::to_value(&child)?,
                );
                created.run_id = Some(child_id.clone());
                created.run_seq = 1;
                let node = &parent.nodes[&node_id];
                if node.dispatch_id != dispatch_id {
                    return Err(fj("stale dispatch"));
                }
                // 幂等闸门（I20 的契约精神）：同一 dispatch 的 child 等待结果
                // 已提交时，重复提交原样返回，不再创建第二个子 run——子流程
                // 可含外部副作用节点，重复创建不是可重入操作。
                if node.attempts[&dispatch_id].result.is_some() {
                    return Ok((vec![], ()));
                }
                let sealed = node.attempts[&node.dispatch_id].audit_seq;
                let wait_event = crate::journal_commands::node_event(
                    parent,
                    &node_id,
                    EventKind::WaitRegistered,
                    json!({"wait": Wait::Child { child_run_id: child_id }, "sealed_through": sealed.to_string()}),
                    true,
                );
                Ok((vec![created, wait_event], ()))
            })
            .await?;
        // 子 run 创建后交回调度器（defer 由 journal_driver 的 notify 处理）。
        let backend = self.attempt.backend.clone();
        let run_id = self.attempt.run_id.clone();
        let node_id = self.attempt.node_id.clone();
        let child_ids: Vec<String> = backend
            .inspect(|s| {
                s.runs
                    .values()
                    .filter(|r| {
                        r.parent
                            .as_ref()
                            .is_some_and(|p| p.run_id == run_id && p.node_id == node_id)
                    })
                    .map(|r| r.run_id.clone())
                    .collect()
            })
            .await;
        for child in child_ids {
            backend.defer_run(child).await;
        }
        Ok(())
    }

    /// RequestOperation：指纹校验 + 请求入库 + 同事务 Intent/Authorized +
    /// 许可下发（AuditAck 永不授权）。
    async fn handle_request_operation(
        &mut self,
        message: Message,
        link: &mut DispatchLink,
    ) -> Result<()> {
        if self
            .snapshot()
            .await?
            .execution
            .as_ref()
            .is_some_and(|e| e.regression.is_some())
        {
            return Err(invalid("external operations are disabled in regression"));
        }
        let Message::RequestOperation {
            dispatch_id,
            operation_id,
            request_fingerprint,
            transfer_id,
            ..
        } = message
        else {
            // 调用方按变体路由；走错路径按协议违规拒绝而非 panic。
            return Err(invalid("operation handler on non-operation message"));
        };
        if dispatch_id != self.attempt.dispatch_id {
            return Err(invalid("operation request for foreign dispatch"));
        }
        // 传输键与操作身份必须同源：执行器按 TransferKey::OperationRequest
        // 构造两者，错位说明对端协议实现漂移——拒绝而不是猜。
        if transfer_id != TransferKey::OperationRequest(operation_id.clone()).to_wire() {
            return Err(invalid("operation transfer id does not match operation"));
        }
        let request_bytes: Arc<Vec<u8>> = {
            let entry = self
                .op_transfers
                .get(&transfer_id)
                .ok_or_else(|| invalid("operation transfer not started"))?;
            let (total, digest) = entry
                .ready
                .clone()
                .ok_or_else(|| invalid("operation transfer incomplete"))?;
            if entry.buf.len() as u64 != total || hex_sha(&entry.buf) != digest {
                return Err(invalid("operation transfer digest mismatch"));
            }
            entry.buf.clone()
        };
        // 完整请求校验：主进程不得只校验未经核实的摘要。
        if flow_journal::codec::digest(&request_bytes) != request_fingerprint {
            return Err(invalid("operation fingerprint mismatch"));
        }
        // 请求入库（journal 级值，不占 audit_seq；与一期 store_json 同构）。
        // 小值内联、大值存为 journal 引用：内联阈值保证控制帧不会因为
        // 外部请求体而膨胀（值字节本身走 data 通道传输）。
        let stored = if request_bytes.len() <= flow_journal::INLINE_BYTES {
            StoredValue::Inline(
                serde_json::from_slice(&request_bytes).map_err(|e| invalid(e.to_string()))?,
            )
        } else {
            let mut reader = request_bytes.as_slice();
            StoredValue::Ref(
                self.attempt
                    .backend
                    .store_stream(&mut reader, ValueCodec::Json)
                    .await?,
            )
        };
        // 取当前提交状态（准备/授权串行裁决在 internal 事务内完成）。
        let _ = self.snapshot().await?;
        let operation = flow_engine::journal_state::Operation {
            operation_id: operation_id.clone(),
            fingerprint: request_fingerprint.clone(),
            permit_id: crate::journal_commands::id(),
            request: stored,
            outcome: None,
        };
        let run_id = self.attempt.run_id.clone();
        let node_id = self.attempt.node_id.clone();
        let dispatch = self.attempt.dispatch_id.clone();
        let operation_value = serde_json::to_value(&operation)?;
        fault::checkpoint("auth_persist:before").await;
        // 授权事务：OperationIntent 与 OperationAuthorized 必须同一事务提交
        // （C3）。分成两条事务会留下「有意图、未必授权」的中间态，恢复期
        // 将无法区分「请求没发出」与「请求已发出但没记录授权」。
        let authorize = self
            .attempt
            .backend
            .internal_for_run(&self.attempt.run_id, move |state| {
                let run = state.runs.get(&run_id).ok_or_else(|| fj("run missing"))?;
                let node = &run.nodes[&node_id];
                if run.terminal() || node.dispatch_id != dispatch {
                    return Err(fj("operation cancelled/stale"));
                }
                if node.prepared.is_none() {
                    return Err(fj("operation before committed preparation"));
                }
                if node.operation.is_some() {
                    return Err(fj(
                        "operation already authorized; outcome must be reconciled, not resent",
                    ));
                }
                let mut intent = crate::journal_commands::node_event(
                    run,
                    &node_id,
                    EventKind::OperationIntent,
                    json!({"operation": operation_value}),
                    true,
                );
                intent.dispatch_id = Some(dispatch.clone());
                let mut authorized = crate::journal_commands::node_event(
                    run,
                    &node_id,
                    EventKind::OperationAuthorized,
                    json!({"operation": operation_value}),
                    true,
                );
                authorized.dispatch_id = Some(dispatch.clone());
                authorized.run_seq += 1;
                Ok((vec![intent, authorized], ()))
            })
            .await;
        fault::checkpoint("auth_persist:after").await;
        match authorize {
            Ok(_) => {
                // 解析凭证（不进入授权事实）。
                fault::checkpoint("permit_send:before").await;
                let request_value: Value =
                    serde_json::from_slice(&request_bytes).unwrap_or(Value::Null);
                if self.node.is_harness_agent() {
                    let timeout = request_value["timeout_ms"]
                        .as_u64()
                        .unwrap_or(flow_engine::exec::DEFAULT_HARNESS_TIMEOUT_MS)
                        .clamp(1, flow_engine::exec::HARNESS_TIMEOUT_MAX_MS);
                    self.agent_deadline = Some(
                        tokio::time::Instant::now()
                            + Duration::from_millis(
                                timeout + flow_engine::execution_protocol::contract::DRAIN_GRACE_MS,
                            ),
                    );
                }
                let credential = request_value["credential"]["secret_ref"]
                    .as_str()
                    .and_then(flow_engine::secrets::get_secret)
                    .filter(|v| !v.is_empty());
                // Reserve capacity before taking the decision lock. Enqueue under
                // that same lock: a cancellation committed before this point must
                // prevent delivery, not merely stop a later scheduling tick.
                let permit = link
                    .commands
                    .clone()
                    .reserve_owned()
                    .await
                    .map_err(|_| invalid("operation permit session closed"))?;
                self.attempt
                    .backend
                    .internal_for_run(&self.attempt.run_id, |state| {
                        let run = state
                            .runs
                            .get(&self.attempt.run_id)
                            .ok_or_else(|| fj("run missing"))?;
                        let node = &run.nodes[&self.attempt.node_id];
                        if run.terminal()
                            || node.dispatch_id != self.attempt.dispatch_id
                            || node
                                .operation
                                .as_ref()
                                .is_none_or(|op| op.permit_id != operation.permit_id)
                        {
                            return Err(fj("operation cancelled/stale before permit delivery"));
                        }
                        permit.send(ToSession::Send(Box::new(Message::OperationPermit {
                            dispatch_id: self.attempt.dispatch_id.clone(),
                            operation_id,
                            permit_id: operation.permit_id.clone(),
                            request_fingerprint,
                            credential,
                        })));
                        Ok((vec![], ()))
                    })
                    .await?;
                fault::checkpoint("permit_send:after").await;
                Ok(())
            }
            Err(error) => {
                // 取消先提交或身份无效：不发 Permit，保存证据并取消派发。
                self.cancel_sent = true;
                self.session_healthy = false;
                let _ = link
                    .commands
                    .send(ToSession::CancelDispatch(self.attempt.dispatch_id.clone()))
                    .await;
                Err(error)
            }
        }
    }
}

/// 输入传输（master→executor，data 通道）：v2 起所有声明的值字节都走
/// 这里——Ref 从 journal 流式读出，Inline 值序列化后直接分块；再小也走。
enum ByteSource {
    Journal(flow_journal::ValueRef),
    Memory(Vec<u8>),
}

#[derive(Debug)]
/// 输入传输的失败原因。
///
/// 区分「链路断了」与「数据坏了」：前者可由远程模式走 Resume 对账恢复，
/// 后者必须直接失败——坏数据重传一次还是坏，重试没有意义。
enum InputTransferError {
    /// 传输内容非法（编码失败、摘要不符、journal 读失败）。
    Invalid(String),
    /// 命令通道已关闭（会话死亡或被回收）。
    LinkClosed,
}

/// StoredValue → 控制帧声明：Ref 保留 journal 身份（输出回引），
/// Inline 只声明存在（字节走 data 通道）。
fn task_value(stored: &StoredValue) -> flow_engine::execution_protocol::TaskValue {
    match stored {
        StoredValue::Ref(reference) => {
            flow_engine::execution_protocol::TaskValue::Ref(reference.clone())
        }
        StoredValue::Inline(_) => flow_engine::execution_protocol::TaskValue::Local,
    }
}

/// StoredValue → 可读字节源。
///
/// Ref 走 journal 流式读出（主进程不把大值整个读进内存），Inline 序列化
/// 成内存字节。失败返回 `Err(())`（调用方转成 `InputTransferError::Invalid`）。
fn byte_source(stored: &StoredValue) -> std::result::Result<ByteSource, ()> {
    match stored {
        StoredValue::Ref(reference) => Ok(ByteSource::Journal(reference.clone())),
        StoredValue::Inline(value) => serde_json::to_vec(value)
            .map(ByteSource::Memory)
            .map_err(|_| ()),
    }
}

/// 把一次派发所需的全部输入字节分块送到执行器。
///
/// 传输与事件循环并行：本协程只管发字节，主循环只管收审计/结果，二者
/// 互不阻塞——否则大输入的传输延迟会拖住审计确认，触发执行器侧的窗口
/// 停滞。
///
/// 顺序敏感：每个值的 `TransferChunk` 必须全部发完后才发它的 `InputReady`，
/// 执行器据此判断材料齐备；因此这里在成功读完并通过摘要校验后才发布
/// readiness。
#[allow(clippy::too_many_arguments)]
async fn transfer_inputs(
    session: tokio::sync::mpsc::Sender<ToSession>,
    dispatch_id: String,
    backend: Arc<JournalBackend>,
    input: StoredValue,
    predecessors: BTreeMap<String, StoredValue>,
    prepared: Option<Prepared>,
    previous_outcome: Option<Value>,
) -> std::result::Result<(), InputTransferError> {
    let root = backend.journal.root().to_path_buf();
    let upper = backend.journal.durable_lsn();
    // 声明与 ExecuteTask.inputs 对齐：run 输入、每个前驱输出、已提交准备
    // 参数、恢复路径的历史 Outcome 体。
    let mut transfers: Vec<(String, std::result::Result<ByteSource, ()>)> = Vec::new();
    transfers.push((TransferKey::RunInput.to_wire(), byte_source(&input)));
    for (node_id, value) in &predecessors {
        transfers.push((
            TransferKey::NodeInput(node_id.clone()).to_wire(),
            byte_source(value),
        ));
    }
    if let Some(prepared) = &prepared {
        transfers.push((
            TransferKey::PreparedParams.to_wire(),
            byte_source(&prepared.params),
        ));
    }
    if let Some(outcome) = &previous_outcome {
        // previous_outcome 是 StoredValue 的序列化（{"type":..,"value":{..}}）。
        // body_raw 在内层 value 里——与执行器侧 materialize_inputs 的
        // pointer("/value/body_raw") 取法对齐；取错层级会漏发 op-body 传输，
        // 执行器死等 InputReady（材料不齐）直到 deadline。
        if let Some(raw) = outcome
            .pointer("/value/body_raw")
            .and_then(|v| serde_json::from_value::<flow_journal::ValueRef>(v.clone()).ok())
        {
            transfers.push((
                TransferKey::OperationBody(raw.output_id.clone()).to_wire(),
                Ok(ByteSource::Journal(raw)),
            ));
        }
    }
    for (transfer_id, source) in transfers {
        let source =
            source.map_err(|_| InputTransferError::Invalid("input encoding failed".into()))?;
        let ready = match source {
            ByteSource::Memory(bytes) => {
                let mut cursor = std::io::Cursor::new(bytes);
                transfer_one(&session, &dispatch_id, &transfer_id, &mut cursor).await?
            }
            ByteSource::Journal(reference) => {
                let root = root.clone();
                let reference = reference.clone();
                // journal 读值是阻塞 IO：放进 spawn_blocking 执行，经 256 KiB
                // 有界管道喂给异步发送侧。有界是必须的——值可达数百 MiB，
                // 无界缓冲会让主进程内存占用与值大小成正比。
                let (reader, writer) = tokio::io::duplex(256 * 1024);
                let produce = tokio::task::spawn_blocking(move || {
                    use std::io::Write;
                    let mut bridge = tokio_util::io::SyncIoBridge::new(writer);
                    let result = flow_journal::value::read_value(
                        &root,
                        upper,
                        &reference,
                        true,
                        &mut bridge,
                    );
                    let _ = bridge.flush();
                    result
                });
                let mut reader = reader;
                let send = transfer_one(&session, &dispatch_id, &transfer_id, &mut reader).await;
                // Close the bounded pipe before joining its blocking producer.
                drop(reader);
                let production = produce.await;
                let ready = send?;
                production
                    .map_err(|e| InputTransferError::Invalid(e.to_string()))?
                    .map_err(|e| InputTransferError::Invalid(e.to_string()))?;
                ready
            }
        };
        // Publish readiness only after read_value's final integrity check succeeds.
        session
            .send(ToSession::Send(Box::new(ready)))
            .await
            .map_err(|_| InputTransferError::LinkClosed)?;
    }
    Ok(())
}

/// 单值分块传输：边读边发（有界管道 → 主进程内存有界）。
async fn transfer_one(
    session: &tokio::sync::mpsc::Sender<ToSession>,
    dispatch_id: &str,
    transfer_id: &str,
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> std::result::Result<Message, InputTransferError> {
    use tokio::io::AsyncReadExt;
    // 固定 256 KiB 块（与 contract 的 TRANSFER_CHUNK_BYTES 一致）：主进程
    // 不按值边界重切，避免与执行器的块大小约定漂移。
    let mut buffer = vec![0u8; TRANSFER_CHUNK_BYTES];
    let mut offset = 0u64;
    let mut hash = sha2::Sha256::new();
    loop {
        let n = reader
            .read(&mut buffer)
            .await
            .map_err(|e| InputTransferError::Invalid(e.to_string()))?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        // 每块自带摘要：执行器逐块校验，损坏只影响该块而不是整条传输。
        session
            .send(ToSession::Send(Box::new(Message::TransferChunk {
                dispatch_id: dispatch_id.into(),
                transfer_id: transfer_id.into(),
                offset,
                bytes: buffer[..n].to_vec(),
                digest: hex_sha(&buffer[..n]),
            })))
            .await
            .map_err(|_| InputTransferError::LinkClosed)?;
        offset += n as u64;
    }
    Ok(Message::InputReady {
        dispatch_id: dispatch_id.into(),
        transfer_id: transfer_id.into(),
        total_bytes: offset,
        digest: hex_sha_result(&hash),
    })
}

/// 单个字节值的 SHA-256 十六进制摘要。
fn hex_sha(bytes: &[u8]) -> String {
    hex::encode(sha2::Sha256::digest(bytes))
}

/// 增量摘要的终值（用于整值摘要，避免把整段读进内存再算一次）。
fn hex_sha_result(hash: &sha2::Sha256) -> String {
    use sha2::Digest;
    hex::encode(hash.clone().finalize())
}

/// 审计记录的规范化 wire 表示（幂等比较的输入）。
///
/// 摘要对象必须是**原始内容**的确定编码：同序号同内容重传必须得到同一
/// 摘要（幂等放行），同序号异内容必须得到不同摘要（拒绝）。
fn wire_record(audit_seq: u64, kind: &str, payload: &Value) -> Value {
    json!({"audit_seq": audit_seq.to_string(), "kind": kind, "payload": payload})
}

/// wire 上的审计种类 → journal 事件种类。
///
/// 闭集映射：未列出的种类一律拒绝（`None`），不做「未知即忽略」——忽略
/// 会让执行器的审计与 journal 事实静默错位。
fn event_kind(kind: &str) -> Option<EventKind> {
    Some(match kind {
        "input_prepared" => EventKind::InputPrepared,
        "value_chunk" => EventKind::ValueChunk,
        "value_published" => EventKind::ValuePublished,
        "operation_outcome" => EventKind::OperationOutcome,
        _ => return None,
    })
}

/// 反查派发归属 `(run_id, node_id, attempt)`。
///
/// 观测尾批晚于 Result 到达时（单流下帧序 = 提交序，但观测与结果分属不同队列），批里的 dispatch_id
/// 是唯一可靠归属。dispatch 索引按 DispatchStarted 事件投影，每个 dispatch
/// 携带自己的尝试号（含重试前的历史派发——见 backend-e2e
/// `retry_attempts_are_offloaded_and_old_dispatch_audit_is_isolated` 回归）；
/// 查无索引行的批直接丢弃，不回落节点当前值——错归属比丢弃更糟。
async fn resolve_dispatch(
    backend: &JournalBackend,
    dispatch_id: &str,
) -> Option<(String, String, u32)> {
    match backend.dispatch_owner(dispatch_id).await {
        Ok(owner) => owner,
        Err(error) => {
            tracing::warn!(%error, "observation dispatch lookup failed");
            None
        }
    }
}

/// wire 上的日志级别 → journal 日志级别（未知级别按 info 处理）。
fn level_of(level: &str) -> flow_engine::log_types::LogLevel {
    match level {
        "error" => flow_engine::log_types::LogLevel::Error,
        "warn" => flow_engine::log_types::LogLevel::Warn,
        "debug" => flow_engine::log_types::LogLevel::Debug,
        _ => flow_engine::log_types::LogLevel::Info,
    }
}

/// wire 上的输出流 → journal 日志流（未知流按 stdout 处理）。
fn stream_of(stream: &str) -> flow_engine::log_types::LogStream {
    match stream {
        "stderr" => flow_engine::log_types::LogStream::Stderr,
        "engine" => flow_engine::log_types::LogStream::Engine,
        _ => flow_engine::log_types::LogStream::Stdout,
    }
}

/// 解析子流程应使用的定义版本。
///
/// 两条路径：
/// - 固定上下文执行（execution 快照存在）：子流程版本被**钉死**在父 run
///   的 bindings 里，必须与钉死值一致，否则父 run 的可复现性被破坏。
/// - 普通执行：取该 workflow 最新的已发布版本。
///
/// 找不到定义即报错：子 run 绝不能绑到「猜出来的」定义上。
fn child_definition<'a>(
    state: &'a flow_engine::journal_state::State,
    parent: &'a Run,
    workflow_id: &str,
    node_id: &str,
) -> flow_journal::Result<&'a flow_engine::journal_state::DefinitionVersion> {
    Ok(if let Some(execution) = &parent.execution {
        let binding = format!(
            "{}@{}/{}",
            parent.workflow_id, parent.workflow_version, node_id
        );
        let key = execution
            .bindings
            .get(&binding)
            .ok_or_else(|| fj("child dependency not pinned"))?;
        if !key.starts_with(&format!("{workflow_id}@")) {
            return Err(fj("child differs from pinned dependency"));
        }
        execution
            .definitions
            .get(key)
            .ok_or_else(|| fj("pinned child definition missing"))?
    } else {
        state
            .workflows
            .get(workflow_id)
            .and_then(|w| w.versions.values().rev().find(|v| v.published))
            .ok_or_else(|| fj("child published definition missing"))?
    })
}

#[cfg(test)]
mod transfer_budget_tests {
    use super::*;

    /// 传输失败必须关管道，且坏值绝不能被发布为「已就绪」。
    #[tokio::test]
    async fn journal_transfer_closes_pipe_on_failure_and_never_publishes_bad_value() {
        let root = tempfile::tempdir().unwrap();
        let backend = JournalBackend::open(root.path(), flow_journal::JournalOptions::default())
            .await
            .unwrap();
        let bytes = vec![b'x'; 2 * 1024 * 1024];
        let reference = flow_journal::value::store_stream(
            &backend.journal,
            &mut bytes.as_slice(),
            ValueCodec::Bytes,
        )
        .await
        .unwrap();
        let (sender, receiver) = mpsc::channel(16);
        drop(receiver);
        let closed = tokio::time::timeout(
            Duration::from_secs(5),
            transfer_inputs(
                sender,
                "d".into(),
                backend.clone(),
                StoredValue::Ref(reference.clone()),
                BTreeMap::new(),
                None,
                None,
            ),
        )
        .await
        .unwrap();
        assert!(matches!(closed, Err(InputTransferError::LinkClosed)));
        let (sender, mut receiver) = mpsc::channel(16);
        let mut bad = reference;
        bad.digest = "0".repeat(64);
        let corrupt = tokio::time::timeout(
            Duration::from_secs(5),
            transfer_inputs(
                sender,
                "d".into(),
                backend.clone(),
                StoredValue::Ref(bad),
                BTreeMap::new(),
                None,
                None,
            ),
        )
        .await
        .unwrap();
        assert!(matches!(corrupt, Err(InputTransferError::Invalid(_))));
        while let Some(ToSession::Send(message)) = receiver.recv().await {
            assert!(!matches!(*message, Message::InputReady { .. }));
        }
        backend.close().await.unwrap();
    }

    /// 畸形传输必须在**保留**（进入 op_transfers 表）之前就被拒绝，
    /// 避免攻击者/异常对端用无效条目占住内存预算。
    #[tokio::test]
    async fn malformed_transfers_are_bounded_before_retention() {
        let root =
            std::env::temp_dir().join(format!("flow-transfer-limit-{}", uuid::Uuid::now_v7()));
        let backend = JournalBackend::open(&root, flow_journal::JournalOptions::default())
            .await
            .unwrap();
        let attempt = Attempt {
            backend: backend.clone(),
            run_id: "r".into(),
            node_id: "n".into(),
            dispatch_id: "d".into(),
        };
        let node = serde_json::from_value(
            json!({"id":"n", "type":"http_call", "params":{"url":"http://unused"}}),
        )
        .unwrap();
        let mut runner = DispatchRunner::runner(attempt, node, BTreeMap::new(), None);
        let (commands, _rx) = mpsc::channel(8);
        let (_tx, events) = mpsc::channel(8);
        let mut link = DispatchLink { commands, events };
        let ready = |dispatch: &str, id: &str, total_bytes| Message::InputReady {
            dispatch_id: dispatch.into(),
            transfer_id: id.into(),
            total_bytes,
            digest: String::new(),
        };
        assert!(runner
            .handle(ready("foreign", "t", 0), &mut link)
            .await
            .is_err());
        assert!(runner
            .handle(ready("d", "t", u64::MAX), &mut link)
            .await
            .is_err());
        assert!(runner.op_transfers.is_empty());
        let chunk = |id: &str, offset, bytes: &[u8]| Message::TransferChunk {
            dispatch_id: "d".into(),
            transfer_id: id.into(),
            offset,
            bytes: bytes.to_vec(),
            digest: hex_sha(bytes),
        };
        assert!(runner
            .handle(chunk("t", u64::MAX, b"x"), &mut link)
            .await
            .is_err());
        let half = vec![b'x'; OPERATION_TRANSFER_LIMIT as usize / 2];
        runner
            .handle(chunk("a", 0, &half), &mut link)
            .await
            .unwrap();
        runner
            .handle(chunk("b", 0, &half), &mut link)
            .await
            .unwrap();
        assert!(runner.handle(chunk("c", 0, b"x"), &mut link).await.is_err());
        assert_eq!(runner.op_transfers.len(), 2);
        runner.op_transfers.clear();
        for i in 0..OPERATION_TRANSFER_COUNT {
            runner
                .handle(ready("d", &i.to_string(), 0), &mut link)
                .await
                .unwrap();
        }
        assert!(runner
            .handle(ready("d", "overflow", 0), &mut link)
            .await
            .is_err());
        assert_eq!(runner.op_transfers.len(), OPERATION_TRANSFER_COUNT);
        backend.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
