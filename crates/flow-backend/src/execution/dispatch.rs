//! 主进程侧 IPC 派发编排（I05–I08）：审计持久确认、操作许可、结果屏障、
//! 取消排空与持久等待登记。
//!
//! 全部持久事实仍经一期 `Attempt`/`JournalBackend::internal` 单行事务写入，
//! journal 记录与进程内执行逐字节同构：InputPrepared/ValueChunk/
//! ValuePublished/OperationIntent/OperationAuthorized/OperationOutcome/
//! NodeCompleted/NodeFailed/WaitRegistered 及 sealed_through 屏障语义不变。

use crate::journal::JournalError;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use sha2::Digest as _;
use tokio::sync::mpsc;

use crate::journal::JournalBackend;
use crate::journal_execution::Attempt;
use flow_engine::execution_protocol::contract::{DURABLE_WAIT_TIMEOUT_MS, TRANSFER_CHUNK_BYTES};
use flow_engine::execution_protocol::message::{Message, ResultOutcome};
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
fn invalid(s: impl Into<String>) -> crate::journal::JournalError {
    crate::journal::JournalError::Journal(flow_journal::Error::Invalid(s.into()))
}
/// internal() 闭包内的错误类型（flow_journal::Error）。
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
    pub commands: mpsc::Sender<ToSession>,
    pub events: mpsc::Receiver<FromSession>,
}

impl IpcDispatcher {
    pub fn new(pool: Arc<ExecutorPool>) -> Self {
        Self { pool }
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
            events: std::mem::replace(&mut lease.events, mpsc::channel(1).1),
        };
        let outcome = runner.drive(&mut link).await;
        // 归还真实事件流（供空闲会话复用）。
        lease.events = std::mem::replace(&mut link.events, mpsc::channel(1).1);
        self.pool.release(lease, outcome.is_ok()).await;
        outcome
    }
}

/// T09 确定性故障注入：测试在命名检查点注册 hold 开关；未注册时
/// [`fault::checkpoint`] 立即返回（生产路径零行为差异）。用于在持久化
/// 前后、Permit 发送前后、ResultCommitted 前后确定性断线/取消——代替
/// 依赖 sleep 的竞态复现。注册表全局唯一：使用注入的测试必须串行场景。
pub mod fault {
    use std::collections::HashMap;
    use std::sync::OnceLock;
    use tokio::sync::watch;

    static REGISTRY: OnceLock<std::sync::Mutex<HashMap<String, watch::Receiver<bool>>>> =
        OnceLock::new();

    /// 注册检查点（初始 hold=true：到达即暂停，直至发送端置 false 释放）。
    /// 返回各检查点的发送端；重复注册同名点以最后一次为准。
    pub fn register(points: &[&str]) -> HashMap<String, watch::Sender<bool>> {
        let registry = REGISTRY
            .get_or_init(|| std::sync::Mutex::new(HashMap::<String, watch::Receiver<bool>>::new()));
        let mut guard = registry.lock().unwrap_or_else(|poison| poison.into_inner());
        let mut senders = HashMap::new();
        for point in points {
            let (sender, receiver) = watch::channel(true);
            guard.insert((*point).to_string(), receiver);
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
        if let Some(mut receiver) = receiver {
            tracing::debug!(name, "fault checkpoint holding");
            let _ = receiver.wait_for(|hold| !hold).await;
        }
    }
}

const OPERATION_TRANSFER_LIMIT: u64 = 8 * 1024 * 1024;
const OPERATION_TRANSFER_COUNT: usize = 16;

struct OpTransfer {
    /// 连续接收前缀（T07）：单流按序到达，无需块树与排序装配。
    buf: Vec<u8>,
    ready: Option<(u64, String)>,
}

struct PendingResult {
    result_id: String,
    last_audit_seq: u64,
    outcome: ResultOutcome,
}

pub(crate) struct DispatchRunner {
    attempt: Attempt,
    node: Node,
    predecessors: BTreeMap<String, StoredValue>,
    observations: Option<ObservationStore>,
    durable_seq: u64,
    durable_bytes: u64,
    /// 已提交记录的确定性编码摘要（同序号同内容幂等；异内容拒绝）。
    committed: BTreeMap<u64, String>,
    pending_result: Option<PendingResult>,
    op_transfers: BTreeMap<String, OpTransfer>,
    cancel_sent: bool,
    session_healthy: bool,
    /// 本次派发的尝试序号（观测日志归属用；在 drive_link 读记录时对齐）。
    task_attempt: u32,
    remote_replay: bool,
    /// 连续空闲超时次数（远程链路）：首次按失联走 Resume 对账，第二次
    /// 硬失败——防「静默执行器 ↔ 重挂」无限循环占用槽位。
    idle_strikes: u32,
}

impl DispatchRunner {
    fn new(
        attempt: Attempt,
        node: Node,
        predecessors: BTreeMap<String, StoredValue>,
        observations: Option<ObservationStore>,
    ) -> Self {
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
            session_healthy: true,
            task_attempt: 1,
            remote_replay: false,
            idle_strikes: 0,
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
            return Err(invalid("stale dispatch"));
        }
        self.task_attempt = record.attempt;
        let previous_outcome = record
            .operation
            .as_ref()
            .and_then(|o| o.outcome.clone())
            .map(|v| serde_json::to_value(&v))
            .transpose()?;
        let node_value = serde_json::to_value(&self.node)?;
        // node 定义是控制帧唯一允许的变长项：超限在这里拒绝（可读错误），
        // 而不是等 transport 编码失败把整条会话判死。
        flow_journal::codec::bounded_json(
            &node_value,
            flow_engine::execution_protocol::CONTROL_MAX_FRAME / 2,
        )?;
        // `previous_outcome` 同样进控制帧（恢复路径的必要描述符）。今天它的
        // 生产者都有界（stderr_tail ≤512 字节、body_raw 是 ValueRef），但这里
        // 补上同一道闸：将来任何新生产者都不可能把控制帧撑爆。
        if let Some(previous) = &previous_outcome {
            flow_journal::codec::bounded_json(
                previous,
                flow_engine::execution_protocol::CONTROL_MAX_FRAME / 2,
            )?;
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
        if !resume {
            link.commands
                .send(ToSession::Send(Box::new(Message::Execute {
                    command_id: command_id.clone(),
                    dispatch_id: self.attempt.dispatch_id.clone(),
                    task,
                })))
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
                    let _ = link
                        .commands
                        .send(ToSession::Send(Box::new(Message::OperationPermit {
                            dispatch_id: self.attempt.dispatch_id.clone(),
                            operation_id: operation.operation_id.clone(),
                            permit_id: operation.permit_id.clone(),
                            request_fingerprint: operation.fingerprint.clone(),
                            credential,
                        })))
                        .await;
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
        let mut cancel_tick = tokio::time::interval(Duration::from_millis(100));
        let mut error: Option<crate::journal::JournalError> = None;
        loop {
            if let Some(err) = error.take() {
                transfer.abort();
                if !transfer_done {
                    let _ = (&mut transfer).await;
                }
                return Err(err);
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
                            // 任何入站帧（审计/ACK/结果/心跳）都是活跃证据。
                            last_progress = tokio::time::Instant::now();
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
                _ = cancel_tick.tick() => {
                    // 持久取消观察：run 终态即发 Cancel 并排空回收。
                    // 只读终态判定，不 clone 整个 run（大 run 下每 100ms 全量
                    // 深拷贝是纯浪费）。
                    if !self.cancel_sent {
                        let run_id = self.attempt.run_id.clone();
                        let terminal = self
                            .attempt
                            .backend
                            .inspect(|s| {
                                s.runs.get(&run_id).is_some_and(|run| run.terminal())
                            })
                            .await;
                        if terminal {
                            self.cancel_sent = true;
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

    async fn snapshot(&self) -> Result<Run> {
        self.attempt
            .backend
            .inspect(|s| s.runs.get(&self.attempt.run_id).cloned())
            .await
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
            Message::RequestOperation { .. } => {
                // 单流承载（v3）：执行器按「块 → InputReady → 请求」的顺序
                // 提交，请求到达时块必然已齐；不齐即传输损坏，明确拒绝。
                let incomplete = match &message {
                    Message::RequestOperation { transfer_id, .. } => {
                        !self.op_transfer_complete(transfer_id)
                    }
                    _ => unreachable!(),
                };
                if incomplete {
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
                let raw = bytes.clone();
                let expected = hex_sha(&raw);
                if expected != digest {
                    return Err(invalid("transfer chunk digest mismatch"));
                }
                let received: u64 = self.op_transfers.values().map(|e| e.buf.len() as u64).sum();
                if offset
                    .checked_add(raw.len() as u64)
                    .is_none_or(|end| end > OPERATION_TRANSFER_LIMIT)
                    || received.saturating_add(raw.len() as u64) > OPERATION_TRANSFER_LIMIT
                    || (!self.op_transfers.contains_key(&transfer_id)
                        && self.op_transfers.len() >= OPERATION_TRANSFER_COUNT)
                {
                    return Err(invalid("operation transfer exceeds input budget"));
                }
                let entry = self.op_transfers.entry(transfer_id).or_insert(OpTransfer {
                    buf: Vec::new(),
                    ready: None,
                });
                let end = offset + raw.len() as u64;
                let received = entry.buf.len() as u64;
                if end <= received {
                    // 幂等重复：完全落在已接收前缀且字节相同（重连后全量重发，T07）。
                    if entry.buf[offset as usize..end as usize] == raw[..] {
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
                entry.buf.extend_from_slice(&raw);
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
                    buf: Vec::new(),
                    ready: None,
                });
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
        entry.buf.len() as u64 == *total && hex::encode(sha2::Sha256::digest(&entry.buf)) == *digest
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
                .internal(move |state| {
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
                self.durable_seq = record.audit_seq;
                self.durable_bytes += record_bytes(record.audit_seq, &record.kind, &record.payload);
                self.committed.insert(record.audit_seq, digest);
            }
        }
        // AuditAck 仅随连续持久前缀前进；commit_lsn 取 durable 位置。
        let commit_lsn = self.attempt.backend.journal.durable_lsn();
        let _ = link
            .commands
            .send(ToSession::Send(Box::new(Message::AuditAck {
                dispatch_id: self.attempt.dispatch_id.clone(),
                durable_audit_seq: self.durable_seq,
                durable_bytes: self.durable_bytes,
                commit_lsn,
            })))
            .await;
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
                let message = if uncertain && !error.starts_with("external outcome uncertain") {
                    format!("external outcome uncertain: {error}")
                } else {
                    error
                };
                if uncertain {
                    self.attempt
                        .finish(
                            EventKind::WaitRegistered,
                            json!({"wait": Wait{kind: WaitKind::Uncertain, wake_at: None, child_run_id: None, output: None}, "integrity": "unknown"}),
                        )
                        .await?;
                } else {
                    let mut payload = json!({
                        "error": message,
                        "preparation_failed": record.prepared.is_none(),
                        "original_input": snapshot.input,
                        "predecessors": self.predecessors.clone(),
                        "integrity": "incomplete",
                    });
                    let policy = self.node.retry();
                    if record.prepared.is_some()
                        && record.operation.is_none()
                        && matches!(
                            self.node.kind(),
                            Some(NodeType::Script | NodeType::Condition)
                        )
                        && record.attempt < policy.max_attempts.min(100)
                    {
                        let delay = policy.backoff_ms.min(i64::MAX as u64) as i64;
                        payload["retry_wake_at"] =
                            json!(chrono::Utc::now().timestamp_millis().saturating_add(delay));
                    }
                    self.attempt.finish(EventKind::NodeFailed, payload).await?;
                }
            }
            ResultOutcome::Wait { wait } => {
                // wire 的 WaitRequest.kind 是字符串（冻结契约）；在这里一次性
                // 解析进词汇表 enum，之后不再有裸字符串比对。
                let kind = match wait.kind.as_str() {
                    "delay" => WaitKind::Delay,
                    "signal" => WaitKind::Signal,
                    "child" => WaitKind::Child,
                    other => return Err(invalid(format!("unknown wait kind {other}"))),
                };
                match kind {
                    WaitKind::Delay | WaitKind::Signal => {
                        self.attempt
                            .finish(
                                EventKind::WaitRegistered,
                                json!({"wait": Wait{
                                    kind,
                                    wake_at: wait.wake_at,
                                    child_run_id: None,
                                    output: wait.output.clone(),
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
        fault::checkpoint("result_committed:before").await;
        let _ = link
            .commands
            .send(ToSession::Send(Box::new(Message::ResultCommitted {
                dispatch_id: self.attempt.dispatch_id.clone(),
                result_id,
            })))
            .await;
        fault::checkpoint("result_committed:after").await;
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
                    flow_journal::value::materialize(&root, upper, &params, 8 * 1024 * 1024)
                })
                .await
                .map_err(|e| invalid(e.to_string()))??;
                params.get("input_mapping").cloned()
            }
        };
        let input = if let Some(mapping) = input_mapping {
            let bytes = flow_journal::codec::bounded_json(&mapping, 8 * 1024 * 1024)?;
            if bytes.len() <= flow_journal::INLINE_BYTES {
                StoredValue::Inline(mapping)
            } else {
                let mut reader = bytes.as_slice();
                StoredValue::Ref(
                    flow_journal::value::store_stream(
                        &self.attempt.backend.journal,
                        &mut reader,
                        ValueCodec::Json,
                    )
                    .await?,
                )
            }
        } else {
            prepared.input.clone()
        };
        let backend = self.attempt.backend.clone();
        let run_id = self.attempt.run_id.clone();
        let node_id = self.attempt.node_id.clone();
        let dispatch_id = self.attempt.dispatch_id.clone();
        let workflow_id = workflow_id.to_string();
        backend
            .internal(move |state| {
                let parent = &state.runs[&run_id];
                if parent.terminal() {
                    return Err(fj("parent terminal"));
                }
                if parent.depth >= flow_engine::MAX_SUB_WORKFLOW_DEPTH {
                    return Err(fj("child depth exceeded"));
                }
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
                let definition = state
                    .workflows
                    .get(&workflow_id)
                    .and_then(|w| w.versions.values().rev().find(|v| v.published))
                    .ok_or_else(|| fj("child published definition missing"))?;
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
                let sealed = node.attempts[&node.dispatch_id].audit_seq;
                let wait_event = crate::journal_commands::node_event(
                    parent,
                    &node_id,
                    EventKind::WaitRegistered,
                    json!({"wait": Wait{kind: WaitKind::Child, wake_at: None, child_run_id: Some(child_id), output: None}, "sealed_through": sealed.to_string()}),
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
        let Message::RequestOperation {
            dispatch_id,
            operation_id,
            request_fingerprint,
            transfer_id,
            ..
        } = message
        else {
            unreachable!()
        };
        if dispatch_id != self.attempt.dispatch_id {
            return Err(invalid("operation request for foreign dispatch"));
        }
        // 传输键与操作身份必须同源：执行器按 TransferKey::OperationRequest
        // 构造两者，错位说明对端协议实现漂移——拒绝而不是猜。
        if transfer_id != TransferKey::OperationRequest(operation_id.clone()).to_wire() {
            return Err(invalid("operation transfer id does not match operation"));
        }
        let request_bytes: Vec<u8> = {
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
        let stored = if request_bytes.len() <= flow_journal::INLINE_BYTES {
            StoredValue::Inline(
                serde_json::from_slice(&request_bytes).map_err(|e| invalid(e.to_string()))?,
            )
        } else {
            let mut reader = request_bytes.as_slice();
            StoredValue::Ref(
                flow_journal::value::store_stream(
                    &self.attempt.backend.journal,
                    &mut reader,
                    ValueCodec::Json,
                )
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
        let authorize = self
            .attempt
            .backend
            .internal(move |state| {
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
                let credential = request_value["credential"]["secret_ref"]
                    .as_str()
                    .and_then(flow_engine::secrets::get_secret)
                    .filter(|v| !v.is_empty());
                let _ = link
                    .commands
                    .send(ToSession::Send(Box::new(Message::OperationPermit {
                        dispatch_id: self.attempt.dispatch_id.clone(),
                        operation_id,
                        permit_id: operation.permit_id.clone(),
                        request_fingerprint,
                        credential,
                    })))
                    .await;
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
enum InputTransferError {
    Invalid(String),
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

fn byte_source(stored: &StoredValue) -> std::result::Result<ByteSource, ()> {
    match stored {
        StoredValue::Ref(reference) => Ok(ByteSource::Journal(reference.clone())),
        StoredValue::Inline(value) => serde_json::to_vec(value)
            .map(ByteSource::Memory)
            .map_err(|_| ()),
    }
}

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

fn hex_sha(bytes: &[u8]) -> String {
    hex::encode(sha2::Sha256::digest(bytes))
}

fn hex_sha_result(hash: &sha2::Sha256) -> String {
    use sha2::Digest;
    hex::encode(hash.clone().finalize())
}

fn wire_record(audit_seq: u64, kind: &str, payload: &Value) -> Value {
    json!({"audit_seq": audit_seq.to_string(), "kind": kind, "payload": payload})
}

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
/// 是唯一可靠归属。当前派发命中的是节点的 `dispatch_id` 字段；更旧的
/// （重试前的）只能命中 attempts 账本，历史尝试号无处可查，用节点当前
/// 值——归属正确，号偏保守。
async fn resolve_dispatch(
    backend: &JournalBackend,
    dispatch_id: &str,
) -> Option<(String, String, u32)> {
    backend
        .inspect(|state| {
            for (run_id, run) in &state.runs {
                for (node_id, node) in &run.nodes {
                    if node.dispatch_id == dispatch_id || node.attempts.contains_key(dispatch_id) {
                        return Some((run_id.clone(), node_id.clone(), node.attempt));
                    }
                }
            }
            None
        })
        .await
}

fn level_of(level: &str) -> flow_engine::log_types::LogLevel {
    match level {
        "error" => flow_engine::log_types::LogLevel::Error,
        "warn" => flow_engine::log_types::LogLevel::Warn,
        "debug" => flow_engine::log_types::LogLevel::Debug,
        _ => flow_engine::log_types::LogLevel::Info,
    }
}

fn stream_of(stream: &str) -> flow_engine::log_types::LogStream {
    match stream {
        "stderr" => flow_engine::log_types::LogStream::Stderr,
        "engine" => flow_engine::log_types::LogStream::Engine,
        _ => flow_engine::log_types::LogStream::Stdout,
    }
}

#[cfg(test)]
mod transfer_budget_tests {
    use super::*;

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
