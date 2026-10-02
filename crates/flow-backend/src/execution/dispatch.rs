//! 主进程侧 IPC 派发编排（I05–I08）：审计持久确认、操作许可、结果屏障、
//! 取消排空与持久等待登记。
//!
//! 全部持久事实仍经一期 `Attempt`/`JournalBackend::internal` 单行事务写入，
//! journal 记录与进程内执行逐字节同构：InputPrepared/ValueChunk/
//! ValuePublished/OperationIntent/OperationAuthorized/OperationOutcome/
//! NodeCompleted/NodeFailed/WaitRegistered 及 sealed_through 屏障语义不变。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::{json, Value};
use sha2::Digest as _;
use tokio::sync::mpsc;

use crate::journal::JournalBackend;
use crate::journal_execution::Attempt;
use flow_engine::execution_protocol::contract::{DURABLE_WAIT_TIMEOUT_MS, TRANSFER_CHUNK_BYTES};
use flow_engine::execution_protocol::message::{Message, ResultOutcome};
use flow_engine::execution_protocol::record_bytes;
use flow_engine::journal_state::{Prepared, Run, Wait};
use flow_engine::observation::ObservationStore;
use flow_engine::{Node, NodeType};
use flow_journal::value::ValueCodec;
use flow_journal::{EventKind, StoredValue};

use super::pool::{ExecutorPool, FromSession, ToSession};

type Result<T> = std::result::Result<T, crate::journal::JournalError>;
fn invalid(s: impl Into<String>) -> crate::journal::JournalError {
    crate::journal::JournalError::Journal(flow_journal::Error::Invalid(s.into()))
}
/// internal() 闭包内的错误类型（flow_journal::Error）。
fn fj(s: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Invalid(s.into())
}

/// IPC 派发器：journal_driver 调度循环的执行端口（替换进程内 execute）。
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

struct OpTransfer {
    chunks: BTreeMap<u64, Vec<u8>>,
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
    /// RequestOperation 先于其 data 块到达时挂起（双通道乱序，二期 §3.6）。
    pending_operation_request: Option<Message>,
    cancel_sent: bool,
    session_healthy: bool,
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
            pending_operation_request: None,
            cancel_sent: false,
            session_healthy: true,
        }
    }

    /// 在给定链路上驱动派发；链路死亡返回 [`LINK_DEAD`] 标记错误，
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
        // 读取当前 run 快照，构造 Execute 任务面。
        let run = self.snapshot().await?;
        let record = run
            .nodes
            .get(&self.attempt.node_id)
            .ok_or_else(|| invalid("dispatch record missing"))?
            .clone();
        if record.dispatch_id != self.attempt.dispatch_id {
            return Err(invalid("stale dispatch"));
        }
        let prepared = record
            .prepared
            .as_ref()
            .map(serde_json::to_value)
            .transpose()?;
        let previous_outcome = record
            .operation
            .as_ref()
            .and_then(|o| o.outcome.clone())
            .map(|v| serde_json::to_value(&v))
            .transpose()?;
        let task = flow_engine::execution_protocol::ExecuteTask {
            run_id: self.attempt.run_id.clone(),
            node_id: self.attempt.node_id.clone(),
            node_execution_id: record.node_execution_id.clone(),
            attempt: record.attempt,
            node: serde_json::to_value(&self.node)?,
            input: run.input.clone(),
            predecessors: self.predecessors.clone().into_iter().collect(),
            prepared,
            previous_outcome: previous_outcome.clone(),
            observability: self.observations.is_some(),
        };
        let command_id = format!("cmd-{}", uuid::Uuid::now_v7());
        if !resume {
            link.commands
                .send(ToSession::Send(Message::Execute {
                    command_id: command_id.clone(),
                    dispatch_id: self.attempt.dispatch_id.clone(),
                    task,
                }))
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
                        .send(ToSession::Send(Message::OperationPermit {
                            dispatch_id: self.attempt.dispatch_id.clone(),
                            operation_id: operation.operation_id.clone(),
                            permit_id: operation.permit_id.clone(),
                            request_fingerprint: operation.fingerprint.clone(),
                            credential,
                        }))
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
        let transfer = tokio::spawn(transfer_inputs(
            transfer_session,
            transfer_backend,
            transfer_input,
            transfer_preds,
            transfer_prepared,
            transfer_outcome,
        ));
        // 主事件循环。
        let deadline = tokio::time::Instant::now() + Duration::from_millis(DURABLE_WAIT_TIMEOUT_MS);
        let mut cancel_tick = tokio::time::interval(Duration::from_millis(100));
        let mut error: Option<crate::journal::JournalError> = None;
        loop {
            if let Some(err) = error.take() {
                let _ = transfer.await;
                return Err(err);
            }
            tokio::select! {
                event = link.events.recv() => {
                    match event {
                        Some(FromSession::Incoming(message)) => {
                            match self.handle(message, link).await {
                                Ok(Some(())) => {
                                    // 任务完成（ResultCommitted 已发）。
                                    let _ = transfer.await;
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
                            let _ = transfer.await;
                            if cancelled {
                                return Err(invalid(format!("executor stopped after cancel: {reason}")));
                            }
                            self.session_healthy = false;
                            return Err(invalid(format!("link-dead: executor session dead: {reason}")));
                        }
                        None => {
                            let _ = transfer.await;
                            self.session_healthy = false;
                            return Err(invalid("link-dead: executor session closed"));
                        }
                    }
                }
                _ = cancel_tick.tick() => {
                    // 持久取消观察：run 终态即发 Cancel 并排空回收。
                    if !self.cancel_sent {
                        if let Ok(run) = self.snapshot().await {
                            if run.terminal() {
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
            if tokio::time::Instant::now() > deadline && !self.cancel_sent {
                self.session_healthy = false;
                let _ = transfer.await;
                return Err(invalid("dispatch deadline exceeded"));
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
                // 尝试推进结果屏障。
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
                // 屏障未满足时保留有界描述，继续读 data（不阻塞读取）。
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
                // 大请求的 data 块可能未齐：未齐时挂起，块到齐后重试。
                let transfer_id = match &message {
                    Message::RequestOperation {
                        transfer_id: Some(id),
                        request: None,
                        ..
                    } => id.clone(),
                    _ => String::new(),
                };
                if !transfer_id.is_empty() && !self.op_transfer_complete(&transfer_id) {
                    self.pending_operation_request = Some(message);
                    return Ok(None);
                }
                self.handle_request_operation(message, link).await?;
                Ok(None)
            }
            Message::TransferChunk {
                transfer_id,
                offset,
                bytes,
                digest,
            } => {
                let raw = STANDARD
                    .decode(&bytes)
                    .map_err(|_| invalid("transfer chunk base64"))?;
                let expected = hex_sha(&raw);
                if expected != digest {
                    return Err(invalid("transfer chunk digest mismatch"));
                }
                let entry = self.op_transfers.entry(transfer_id).or_insert(OpTransfer {
                    chunks: BTreeMap::new(),
                    ready: None,
                });
                if entry.chunks.insert(offset, raw).is_some() {
                    return Err(invalid("duplicate transfer chunk"));
                }
                self.try_pending_operation(link).await?;
                Ok(None)
            }
            Message::InputReady {
                transfer_id,
                total_bytes,
                digest,
            } => {
                let entry = self.op_transfers.entry(transfer_id).or_insert(OpTransfer {
                    chunks: BTreeMap::new(),
                    ready: None,
                });
                entry.ready = Some((total_bytes, digest));
                self.try_pending_operation(link).await?;
                Ok(None)
            }
            Message::ObservabilityBatch { dispatch_id, lines } => {
                if let Some(store) = &self.observations {
                    let logger = store.logger(
                        self.attempt.run_id.clone(),
                        dispatch_id.unwrap_or_else(|| self.attempt.dispatch_id.clone()),
                    );
                    for line in lines {
                        logger.emit(flow_engine::nodelog::LogLine {
                            node_id: self.attempt.node_id.clone(),
                            attempt: 1,
                            level: level_of(&line.level),
                            stream: stream_of(&line.stream),
                            message: line.message,
                        });
                    }
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

    /// 操作请求传输是否已齐（块数与摘要校验）。
    fn op_transfer_complete(&self, transfer_id: &str) -> bool {
        let Some(entry) = self.op_transfers.get(transfer_id) else {
            return false;
        };
        let Some((total, digest)) = &entry.ready else {
            return false;
        };
        let received: u64 = entry.chunks.values().map(|c| c.len() as u64).sum();
        if received != *total {
            return false;
        }
        let mut assembled = Vec::with_capacity(*total as usize);
        let mut expected = 0u64;
        for (offset, chunk) in &entry.chunks {
            if *offset != expected {
                return false;
            }
            expected += chunk.len() as u64;
            assembled.extend_from_slice(chunk);
        }
        hex_sha(&assembled) == *digest
    }

    /// 挂起的 RequestOperation 在块齐后重试。
    async fn try_pending_operation(&mut self, link: &mut DispatchLink) -> Result<()> {
        let ready = match &self.pending_operation_request {
            Some(Message::RequestOperation { transfer_id, .. }) => transfer_id
                .as_ref()
                .map(|id| self.op_transfer_complete(id))
                .unwrap_or(true),
            _ => false,
        };
        if ready {
            if let Some(message) = self.pending_operation_request.take() {
                self.handle_request_operation(message, link).await?;
            }
        }
        Ok(())
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
            .send(ToSession::Send(Message::AuditAck {
                dispatch_id: self.attempt.dispatch_id.clone(),
                durable_audit_seq: self.durable_seq,
                durable_bytes: self.durable_bytes,
                commit_lsn,
            }))
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
                            json!({"wait": Wait{kind: "uncertain".into(), wake_at: None, child_run_id: None, output: None}, "integrity": "unknown"}),
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
            ResultOutcome::Wait { wait } => match wait.kind.as_str() {
                "delay" | "signal" => {
                    self.attempt
                        .finish(
                            EventKind::WaitRegistered,
                            json!({"wait": Wait{
                                kind: wait.kind.clone(),
                                wake_at: wait.wake_at,
                                child_run_id: None,
                                output: wait.output.clone(),
                            }}),
                        )
                        .await?;
                }
                "child" => {
                    let workflow_id = wait
                        .workflow_id
                        .clone()
                        .ok_or_else(|| invalid("child wait missing workflow"))?;
                    self.register_child_run(&workflow_id, wait.input_mapping.clone())
                        .await?;
                }
                other => return Err(invalid(format!("unknown wait kind {other}"))),
            },
        }
        // 结果已提交：回执使执行器释放槽位（重复派发返回原提交）。
        let _ = link
            .commands
            .send(ToSession::Send(Message::ResultCommitted {
                dispatch_id: self.attempt.dispatch_id.clone(),
                result_id,
            }))
            .await;
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
                    >= 1000
                {
                    return Err(flow_journal::Error::Limit("R_max=1000".into()));
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
                    status: "running".into(),
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
                    json!({"wait": Wait{kind: "child".into(), wake_at: None, child_run_id: Some(child_id), output: None}, "sealed_through": sealed.to_string()}),
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
            request,
            transfer_id,
            ..
        } = message
        else {
            unreachable!()
        };
        if dispatch_id != self.attempt.dispatch_id {
            return Err(invalid("operation request for foreign dispatch"));
        }
        // 组装请求字节（inline 或 transfer）。
        let request_bytes: Vec<u8> = match request {
            Some(StoredValue::Inline(value)) => {
                flow_journal::codec::bounded_json(&value, 8 * 1024 * 1024)?
            }
            _ => {
                let id = transfer_id.ok_or_else(|| invalid("operation request missing"))?;
                let entry = self
                    .op_transfers
                    .get(&id)
                    .ok_or_else(|| invalid("operation transfer not started"))?;
                let (total, digest) = entry
                    .ready
                    .clone()
                    .ok_or_else(|| invalid("operation transfer incomplete"))?;
                let mut bytes = Vec::with_capacity(total as usize);
                let mut expected = 0u64;
                for (offset, chunk) in &entry.chunks {
                    if *offset != expected {
                        return Err(invalid("operation transfer gap"));
                    }
                    expected += chunk.len() as u64;
                    bytes.extend_from_slice(chunk);
                }
                if bytes.len() as u64 != total || hex_sha(&bytes) != digest {
                    return Err(invalid("operation transfer digest mismatch"));
                }
                bytes
            }
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
        match authorize {
            Ok(_) => {
                // 解析凭证（不进入授权事实）。
                let request_value: Value =
                    serde_json::from_slice(&request_bytes).unwrap_or(Value::Null);
                let credential = request_value["credential"]["secret_ref"]
                    .as_str()
                    .and_then(flow_engine::secrets::get_secret)
                    .filter(|v| !v.is_empty());
                let _ = link
                    .commands
                    .send(ToSession::Send(Message::OperationPermit {
                        dispatch_id: self.attempt.dispatch_id.clone(),
                        operation_id,
                        permit_id: operation.permit_id.clone(),
                        request_fingerprint,
                        credential,
                    }))
                    .await;
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

/// 输入传输（master→executor，data 通道）：Ref 值从 journal 流式读出分块。
#[allow(clippy::too_many_arguments)]
async fn transfer_inputs(
    session: tokio::sync::mpsc::Sender<ToSession>,
    backend: Arc<JournalBackend>,
    input: StoredValue,
    predecessors: BTreeMap<String, StoredValue>,
    prepared: Option<Prepared>,
    previous_outcome: Option<Value>,
) {
    let root = backend.journal.root().to_path_buf();
    let upper = backend.journal.durable_lsn();
    let mut transfers: Vec<(String, StoredValue)> = Vec::new();
    if let StoredValue::Ref(_) = &input {
        transfers.push(("in:run-input".into(), input.clone()));
    }
    for (node_id, value) in &predecessors {
        if let StoredValue::Ref(_) = value {
            transfers.push((format!("in:{node_id}"), value.clone()));
        }
    }
    if let Some(prepared) = &prepared {
        if let StoredValue::Ref(_) = &prepared.params {
            transfers.push(("in:prepared-params".into(), prepared.params.clone()));
        }
    }
    if let Some(outcome) = &previous_outcome {
        if let Some(raw) = outcome
            .get("body_raw")
            .and_then(|v| serde_json::from_value::<flow_journal::ValueRef>(v.clone()).ok())
        {
            transfers.push((format!("op-body:{}", raw.output_id), StoredValue::Ref(raw)));
        }
    }
    for (transfer_id, value) in transfers {
        let StoredValue::Ref(reference) = value else {
            continue;
        };
        let root = root.clone();
        let reference = reference.clone();
        let (reader, writer) = tokio::io::duplex(256 * 1024);
        let produce = tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let mut bridge = tokio_util::io::SyncIoBridge::new(writer);
            let result =
                flow_journal::value::read_value(&root, upper, &reference, true, &mut bridge);
            let _ = bridge.flush();
            result
        });
        let mut reader = reader;
        let send = transfer_one(&session, &transfer_id, &mut reader).await;
        let production = produce.await;
        if send.is_err() || production.is_err() {
            return;
        }
    }
}

/// 单值分块传输：边读边发（有界管道 → 主进程内存有界）。
async fn transfer_one(
    session: &tokio::sync::mpsc::Sender<ToSession>,
    transfer_id: &str,
    reader: &mut (impl tokio::io::AsyncRead + Unpin),
) -> std::result::Result<(), ()> {
    use tokio::io::AsyncReadExt;
    let mut buffer = vec![0u8; TRANSFER_CHUNK_BYTES];
    let mut offset = 0u64;
    let mut hash = sha2::Sha256::new();
    loop {
        let n = reader.read(&mut buffer).await.map_err(|_| ())?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        session
            .send(ToSession::Send(Message::TransferChunk {
                transfer_id: transfer_id.into(),
                offset,
                bytes: STANDARD.encode(&buffer[..n]),
                digest: hex_sha(&buffer[..n]),
            }))
            .await
            .map_err(|_| ())?;
        offset += n as u64;
    }
    session
        .send(ToSession::Send(Message::InputReady {
            transfer_id: transfer_id.into(),
            total_bytes: offset,
            digest: hex_sha_result(&hash),
        }))
        .await
        .map_err(|_| ())?;
    Ok(())
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

fn level_of(level: &str) -> flow_engine::event::LogLevel {
    match level {
        "error" => flow_engine::event::LogLevel::Error,
        "warn" => flow_engine::event::LogLevel::Warn,
        "debug" => flow_engine::event::LogLevel::Debug,
        _ => flow_engine::event::LogLevel::Info,
    }
}

fn stream_of(stream: &str) -> flow_engine::event::LogStream {
    match stream {
        "stderr" => flow_engine::event::LogStream::Stderr,
        "engine" => flow_engine::event::LogStream::Engine,
        _ => flow_engine::event::LogStream::Stdout,
    }
}
