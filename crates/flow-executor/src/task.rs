//! 单次节点派发的子进程内执行（二期 §3.8，I04）。
//!
//! 一次 Execute 在同一子进程内完成准备与执行：模板、script、condition
//! 的全部用户 JS 都在本进程运行，主进程不再求值任何用户表达式。准备与
//! 执行共用一套 audit_seq、窗口、取消与结果协议，不为 prepare/execute
//! 分别派发。

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Deserialize as _;
use serde_json::{json, Value};
use sha2::Digest as _;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use flow_engine::execution_protocol::contract::{bounded_control_text, DURABLE_WAIT_TIMEOUT_MS};
use flow_engine::execution_protocol::message::{
    ExecuteTask, Message, ResultOutcome, TaskValue, WaitKind, WaitRequest,
};
use flow_engine::execution_protocol::TransferKey;
use flow_engine::journal_state::Prepared;
use flow_engine::model::Node;
use flow_engine::{NodeType, HTTP_METHODS};
use flow_journal::value::ValueCodec;
use flow_journal::StoredValue;

use crate::audit::AuditStream;
use crate::observe::{ObservationBridge, ObservationClose};
use crate::transfer::{store_bytes, store_value, IncomingTransfers};

/// 输入面预算（与一期 materialize 8 MiB 一致）。
pub const INPUT_BUDGET: usize = 8 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    /// 业务失败（脚本/模板/HTTP 状态码/参数）：必须以失败 Result 回主进程，
    /// 由主进程按一期语义写 NodeFailed/重试，不走 Reject 回收。
    #[error("{0}")]
    Business(String),
    #[error("audit: {0}")]
    Audit(#[from] crate::audit::AuditError),
    #[error("transfer: {0}")]
    Transfer(#[from] crate::transfer::TransferError),
    #[error("journal encode: {0}")]
    Encode(#[from] flow_journal::Error),
    #[error("task input invalid: {0}")]
    Invalid(String),
    #[error("task cancelled")]
    Cancelled,
    #[error("master channel closed: {0}")]
    Closed(String),
    #[error("external outcome uncertain: {0}")]
    Uncertain(String),
    #[error("http failure: {0}")]
    Http(String),
}

impl From<serde_json::Error> for TaskError {
    fn from(error: serde_json::Error) -> Self {
        TaskError::Invalid(error.to_string())
    }
}

/// 任务结束方式（运行时据此决定是否释放/回收进程）。
#[derive(Debug)]
pub enum TaskEnd {
    /// ResultCommitted 已收到：槽位可复用。
    Committed,
    /// 取消/失败/通道断开：尽力封口后进程应退出，槽位整体回收。
    Recycle,
}

/// 运行一个派发：从 Accepted 到 ResultCommitted（或取消退出）。
pub struct TaskRunner {
    pub command_id: String,
    pub dispatch_id: String,
    pub task: ExecuteTask,
    pub outbound: mpsc::Sender<Message>,
    pub mail: mpsc::Receiver<Message>,
    pub ack_tx: watch::Sender<(u64, u64)>,
    pub cancel: CancellationToken,
    pub journal_id: String,
    pub window_bytes: u64,
    pub remote_replay: bool,
    /// 本任务全部输入/重传装配（生命周期覆盖整个派发）。
    pub transfers: IncomingTransfers,
    /// 已发出的唯一 Result 帧（回执丢失时按节奏幂等重发）。
    pub last_result: Option<Message>,
    /// 装配阶段重建的输入面（StoredValue 形态：Ref 保身份供输出回引，
    /// Local 已物化为 Inline）。resume 重建 Prepared、fresh prepare 都用它。
    pub face: Option<InputFace>,
}

/// v2 装配产物：输入面的 StoredValue 重建（值字节全部来自 data 通道）。
pub struct InputFace {
    pub input: StoredValue,
    pub predecessors: BTreeMap<String, StoredValue>,
    pub prepared_params: Option<StoredValue>,
}

impl TaskRunner {
    async fn receive_until(
        &mut self,
        deadline: tokio::time::Instant,
        context: &str,
    ) -> Result<Message, TaskError> {
        tokio::select! {
            message = self.mail.recv() => message.ok_or_else(|| TaskError::Closed("mailbox closed".into())),
            _ = self.cancel.cancelled() => Err(TaskError::Cancelled),
            _ = tokio::time::sleep_until(deadline), if self.remote_replay => Err(TaskError::Closed(format!("{context} timeout"))),
        }
    }
    pub async fn run(mut self) -> TaskEnd {
        let result = self.execute().await;
        match result {
            Ok(()) => TaskEnd::Committed,
            Err(error) => {
                tracing::warn!(dispatch = %self.dispatch_id, %error, "executor task stopped");
                eprintln!("flow-executor: task {} stopped: {error}", self.dispatch_id);
                // 尽力通知主进程后进入回收（主进程宽限期后 kill/reap）。
                let _ = self
                    .outbound
                    .send(Message::Reject {
                        reason: bounded_control_text(format!("executor task failed: {error}")),
                    })
                    .await;
                // 给在途帧（含 Reject）一个短暂落地窗口，再退出。
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                TaskEnd::Recycle
            }
        }
    }

    async fn execute(&mut self) -> Result<(), TaskError> {
        if let Some(runtime) = self
            .task
            .node
            .pointer("/_flow_execution/runtime")
            .and_then(Value::as_str)
        {
            if runtime != flow_engine::RUNTIME_ID {
                return Err(TaskError::Invalid("executor runtime mismatch".into()));
            }
        }
        self.outbound
            .send(Message::Accepted {
                command_id: self.command_id.clone(),
                dispatch_id: self.dispatch_id.clone(),
            })
            .await
            .map_err(|e| TaskError::Closed(e.to_string()))?;
        let node: Node = serde_json::from_value(self.task.node.clone()).map_err(TaskError::from)?;
        let mut audit = AuditStream::new(
            self.dispatch_id.clone(),
            self.outbound.clone(),
            self.window_bytes,
            self.ack_tx.subscribe(),
        );
        audit.configure(self.remote_replay, self.cancel.clone());
        // 观测桥（未启用时静默丢弃）。尾批必须在 Result 之前入队，否则会被
        // 复用会话的下一个派发错认领（见 ObservationClose）。
        let (logger, obs_close) = if self.task.observability {
            let (logger, _loss, close) =
                ObservationBridge::start(self.dispatch_id.clone(), self.outbound.clone());
            (logger, Some(close))
        } else {
            (ObservationBridge::disabled_logger(), None)
        };
        // 每次尝试一条开始行 + 一条终态行：没有它们，script/condition 节点
        // （脚本不打 console 时）在观测日志里整段静默，用户看不到节点在跑。
        // start/end 是零工作量的流程节点，不产生叙事。
        let narrative = narrative_kind(node.kind());
        if let Some(label) = narrative {
            logger.info(format!(
                "开始执行：{label}{}",
                attempt_label(self.task.attempt)
            ));
        }
        // 1) 输入装配（Ref 走 transfer；inline 直接可用）。
        let inputs = self
            .materialize_inputs(Duration::from_millis(DURABLE_WAIT_TIMEOUT_MS))
            .await?;
        // 2) 准备（已有提交 InputPrepared 则跳过求值）；业务失败统一在尾部
        // 转失败 Result，不走 Reject 回收。
        let prepared: Prepared = match &self.task.prepared {
            Some(meta) => {
                let face = self
                    .face
                    .as_ref()
                    .ok_or_else(|| TaskError::Invalid("inputs not materialized".into()))?;
                Prepared {
                    input: face.input.clone(),
                    predecessors: face.predecessors.clone(),
                    params: face
                        .prepared_params
                        .clone()
                        .ok_or_else(|| TaskError::Invalid("prepared params missing".into()))?,
                    engine_build: meta.engine_build.clone(),
                    node_semantics: meta.node_semantics,
                }
            }
            None => match self.prepare(&node, &inputs, &mut audit).await {
                Ok(prepared) => {
                    let prepared_seq = audit.next_seq() - 1;
                    // §3.8：等 InputPrepared 对应 AuditAck 后继续已被 Execute
                    // 授权的纯计算；外部节点另需 OperationPermit。
                    audit.await_durable(prepared_seq, &self.cancel).await?;
                    prepared
                }
                Err(TaskError::Business(error)) => {
                    if narrative.is_some() {
                        logger.error(format!("执行失败：{error}"));
                    }
                    drain_observations(logger, obs_close).await;
                    self.emit_failure(&mut audit, error, false).await?;
                    return Ok(());
                }
                Err(other) => return Err(other),
            },
        };
        // 3) 执行（按节点类型）；业务失败/未定外部结果转失败 Result
        // （同一派发；uncertain 由主进程依操作状态登记）。
        let attempt_logger = logger.clone();
        let started = std::time::Instant::now();
        let outcome = match self
            .execute_node(&node, &prepared, &inputs, &mut audit, logger)
            .await
        {
            Ok(outcome) => outcome,
            Err(TaskError::Business(error)) => ResultOutcome::Failure {
                error,
                uncertain_operation: false,
            },
            Err(TaskError::Uncertain(error)) => {
                // 授权后调用未得到可验证 Outcome：失败 Result 携带 uncertain
                // 标记，主进程按一期规则登记 uncertain 等待。
                attempt_logger.error(format!("外部操作结果未确定，等待裁决：{error}"));
                drain_observations(attempt_logger, obs_close).await;
                self.emit_failure(
                    &mut audit,
                    format!("external outcome uncertain: {error}"),
                    true,
                )
                .await?;
                return Ok(());
            }
            Err(other) => return Err(other),
        };
        // Wait 不打终态行：等待中的节点由各自的等待说明（delay/人工任务/
        // 子工作流）交代去向，恢复由 WaitResolved 叙事。
        if narrative.is_some() {
            match &outcome {
                ResultOutcome::Success { .. } => {
                    attempt_logger.info(format!("执行完成（{}）", elapsed_label(started.elapsed())))
                }
                ResultOutcome::Failure { error, .. } => {
                    attempt_logger.error(format!("执行失败：{error}"));
                }
                ResultOutcome::Wait { .. } => {}
            }
        }
        drain_observations(attempt_logger, obs_close).await;
        self.emit_result(&mut audit, outcome).await
    }

    async fn emit_failure(
        &mut self,
        audit: &mut AuditStream,
        error: String,
        uncertain: bool,
    ) -> Result<(), TaskError> {
        self.emit_result(
            audit,
            ResultOutcome::Failure {
                error,
                uncertain_operation: uncertain,
            },
        )
        .await
    }

    // Result can reference values at the end of the audit stream. A single flush
    // only fills the window; keep accepting ACKs until the entire prefix is durable.
    async fn emit_result(
        &mut self,
        audit: &mut AuditStream,
        mut outcome: ResultOutcome,
    ) -> Result<(), TaskError> {
        // 控制帧预算由常量保证：inline 值编码 ≤ INLINE_BYTES(64 KiB)，
        // CONTROL_MAX_FRAME(128 KiB) − 值编码 − 信封恒有余量，无需降级。
        // 自由文本（Failure.error）仍按 bounded_control_text 定长化。
        if let ResultOutcome::Failure { error, .. } = &mut outcome {
            *error = bounded_control_text(std::mem::take(error));
        }
        let last_audit_seq = audit.next_seq().saturating_sub(1);
        if self.remote_replay {
            tokio::time::timeout(
                Duration::from_millis(DURABLE_WAIT_TIMEOUT_MS),
                audit.await_durable(last_audit_seq, &self.cancel),
            )
            .await
            .map_err(|_| TaskError::Closed("final audit acknowledgement timeout".into()))??;
        } else {
            audit.await_durable(last_audit_seq, &self.cancel).await?;
        }
        let result_id = uuid::Uuid::now_v7().to_string();
        let result_message = Message::Result {
            dispatch_id: self.dispatch_id.clone(),
            result_id: result_id.clone(),
            last_audit_seq,
            outcome,
        };
        self.outbound
            .send(result_message.clone())
            .await
            .map_err(|e| TaskError::Closed(e.to_string()))?;
        if self.remote_replay {
            self.last_result = Some(result_message);
            tokio::time::timeout(
                Duration::from_millis(DURABLE_WAIT_TIMEOUT_MS),
                self.wait_committed(&result_id),
            )
            .await
            .map_err(|_| TaskError::Closed("ResultCommitted timeout".into()))?
        } else {
            self.wait_committed(&result_id).await
        }
    }

    async fn wait_committed(&mut self, result_id: &str) -> Result<(), TaskError> {
        let mut last_send = None::<tokio::time::Instant>;
        loop {
            let message = tokio::select! {
                message = self.mail.recv() => message,
                _ = self.cancel.cancelled() => return Err(TaskError::Cancelled),
                _ = tokio::time::sleep(Duration::from_millis(
                    flow_engine::execution_protocol::ACK_RETRANSMIT_MS / 2 + 1,
                )), if self.remote_replay => {
                    // 结果回执可能在上联/中继中丢失：周期性重发同一
                    // result_id 的 Result（主进程幂等返回原提交）。
                    if last_send.is_none_or(|at| {
                        at.elapsed()
                            >= Duration::from_millis(
                                flow_engine::execution_protocol::ACK_RETRANSMIT_MS,
                            )
                    }) {
                        if let Some(result) = self.last_result.clone() {
                            let _ = self.outbound.try_send(result);
                        }
                        last_send = Some(tokio::time::Instant::now());
                    }
                    continue;
                }
            };
            let Some(message) = message else {
                return Err(TaskError::Closed("mailbox closed".into()));
            };
            match message {
                Message::ResultCommitted {
                    dispatch_id,
                    result_id: committed_id,
                } => {
                    if dispatch_id == self.dispatch_id && committed_id == *result_id {
                        return Ok(());
                    }
                }
                Message::Cancel { dispatch_id, .. } if dispatch_id == self.dispatch_id => {
                    // 取消先到：不再等待提交确认，进入回收。
                    return Err(TaskError::Cancelled);
                }
                Message::AuditAck { .. } | Message::Heartbeat { .. } => continue,
                message @ Message::TransferChunk { .. } if self.remote_replay => {
                    self.transfers.feed(&message)?;
                }
                message @ Message::InputReady { .. } if self.remote_replay => {
                    self.transfers.input_ready(&message)?;
                }
                other => {
                    return Err(TaskError::Invalid(format!(
                        "unexpected message while waiting commit: {}",
                        other.type_name()
                    )))
                }
            }
        }
    }

    /// 装配输入面（v2）：声明的每个值字节都走 data 通道——Ref 保留身份
    /// 供输出回引，Local 物化为 Inline。重建的 StoredValue 面存入
    /// [`Self::face`]，resume 重建 Prepared / fresh prepare 复用。
    async fn materialize_inputs(&mut self, timeout: Duration) -> Result<TaskInputs, TaskError> {
        let mut budget = INPUT_BUDGET;
        let mut ids: Vec<String> = vec![TransferKey::RunInput.to_wire()];
        for (node_id, _) in &self.task.predecessors {
            ids.push(TransferKey::NodeInput(node_id.clone()).to_wire());
        }
        if self.task.prepared.is_some() {
            ids.push(TransferKey::PreparedParams.to_wire());
        }
        if let Some(outcome) = &self.task.previous_outcome {
            if let Some(raw) = outcome.pointer("/value/body_raw") {
                if let Some(output_id) = raw.get("output_id").and_then(Value::as_str) {
                    ids.push(TransferKey::OperationBody(output_id.to_owned()).to_wire());
                }
            }
        }
        let deadline = tokio::time::Instant::now() + timeout;
        while ids.iter().any(|id| !self.transfers.is_ready(id)) {
            let message = self.receive_until(deadline, "input transfer").await?;
            match message {
                Message::TransferChunk { .. } => self.transfers.feed(&message)?,
                Message::InputReady { .. } => self.transfers.input_ready(&message)?,
                Message::Cancel { dispatch_id, .. } if dispatch_id == self.dispatch_id => {
                    return Err(TaskError::Cancelled)
                }
                Message::AuditAck {
                    durable_audit_seq,
                    durable_bytes,
                    ..
                } => {
                    let _ = self.ack_tx.send((durable_audit_seq, durable_bytes));
                }
                _ => continue,
            }
        }
        // 物化（预算内）并重建 StoredValue 面；Bytes 编码引用不能进 JS
        // 输入面。
        let mut decode = |tv: &TaskValue, id: &str| -> Result<(StoredValue, Value), TaskError> {
            let bytes = self
                .transfers
                .take(id)
                .ok_or_else(|| TaskError::Invalid(format!("transfer {id} missing")))?;
            if bytes.len() > budget {
                return Err(TaskError::Invalid("aggregate input exceeds 8 MiB".into()));
            }
            budget -= bytes.len();
            match tv {
                TaskValue::Ref(reference) => {
                    let value = match reference.codec {
                        ValueCodec::Json => serde_json::from_slice(&bytes)?,
                        ValueCodec::Utf8 => Value::String(
                            String::from_utf8(bytes)
                                .map_err(|e| TaskError::Invalid(e.to_string()))?,
                        ),
                        ValueCodec::Bytes => {
                            return Err(TaskError::Invalid(
                                "binary value in JSON input face".into(),
                            ))
                        }
                    };
                    Ok((StoredValue::Ref(reference.clone()), value))
                }
                TaskValue::Local => {
                    let value: Value = serde_json::from_slice(&bytes)?;
                    Ok((StoredValue::Inline(value.clone()), value))
                }
            }
        };
        let (input_stored, input) = decode(&self.task.input, &TransferKey::RunInput.to_wire())?;
        let mut nodes = serde_json::Map::new();
        let mut predecessors = BTreeMap::new();
        for (node_id, value) in &self.task.predecessors {
            let (stored, decoded) =
                decode(value, &TransferKey::NodeInput(node_id.clone()).to_wire())?;
            predecessors.insert(node_id.clone(), stored);
            nodes.insert(node_id.clone(), decoded);
        }
        let prepared_params = match &self.task.prepared {
            Some(meta) => Some(decode(&meta.params, &TransferKey::PreparedParams.to_wire())?.0),
            None => None,
        };
        self.face = Some(InputFace {
            input: input_stored,
            predecessors,
            prepared_params,
        });
        Ok(TaskInputs {
            input,
            nodes: Value::Object(nodes),
        })
    }

    /// 一期 `Attempt::prepare` 的子进程等价实现：无模板直接提交实际输入；
    /// 有模板则展开（opaque 参数回填），产出 InputPrepared 审计事实。
    async fn prepare(
        &mut self,
        node: &Node,
        inputs: &TaskInputs,
        audit: &mut AuditStream,
    ) -> Result<Prepared, TaskError> {
        let (face_input, predecessors) = {
            let face = self
                .face
                .as_ref()
                .ok_or_else(|| TaskError::Invalid("inputs not materialized".into()))?;
            (face.input.clone(), face.predecessors.clone())
        };
        let determinism = self.determinism();
        let templates = serde_json::to_string(&node.params)?.contains("${");
        let params = if !templates {
            node.params.clone()
        } else {
            let mut params = node.params.clone();
            let mut opaque = Vec::new();
            if let Some(map) = params.as_object_mut() {
                for key in node.kind().map(NodeType::opaque_params).unwrap_or_default() {
                    if let Some(value) = map.remove(key) {
                        opaque.push((key.to_string(), value));
                    }
                }
            }
            let expanded = tokio::task::spawn_blocking({
                let params = params.clone();
                let input = inputs.input.clone();
                let nodes = inputs.nodes.clone();
                move || {
                    flow_engine::expr::expand_templates_recorded(
                        &params,
                        &input,
                        &nodes,
                        Duration::from_secs(2),
                        determinism,
                    )
                }
            })
            .await
            .map_err(|e| TaskError::Invalid(e.to_string()))?
            .map_err(|e| TaskError::Invalid(e.to_string()))?;
            let mut expanded = expanded;
            if let Some(map) = expanded.as_object_mut() {
                for (key, value) in opaque {
                    map.insert(key, value);
                }
            }
            expanded
        };
        let stored_params = store_value(audit, &self.journal_id, &params, INPUT_BUDGET).await?;
        if matches!(stored_params, StoredValue::Ref(_)) {
            // Freshly prepared bytes are already here. Do not wait for a transfer
            // which is only sent by the master on a resumed attempt.
            self.transfers.insert_ready(
                TransferKey::PreparedParams.to_wire(),
                flow_journal::codec::bounded_json(&params, INPUT_BUDGET)?,
            );
        }
        let prepared = Prepared {
            input: face_input,
            predecessors,
            params: stored_params,
            engine_build: flow_engine::RUNTIME_ID.into(),
            node_semantics: if determinism.is_some() { 2 } else { 1 },
        };
        audit.push(
            "input_prepared",
            json!({"prepared": serde_json::to_value(&prepared)?}),
        )?;
        Ok(prepared)
    }

    /// 节点执行：与一期 `journal_driver::execute` 逐分支对齐。
    async fn execute_node(
        &mut self,
        node: &Node,
        prepared: &Prepared,
        inputs: &TaskInputs,
        audit: &mut AuditStream,
        logger: flow_engine::NodeLogger,
    ) -> Result<ResultOutcome, TaskError> {
        let kind = node
            .kind()
            .ok_or_else(|| TaskError::Invalid("unknown node type".into()))?;
        match kind {
            NodeType::Start => Ok(ResultOutcome::Success {
                output: prepared.input.clone(),
                branch: None,
            }),
            NodeType::End => {
                let output = self.join_end_outputs(prepared, audit).await?;
                Ok(ResultOutcome::Success {
                    output,
                    branch: None,
                })
            }
            NodeType::Script | NodeType::Condition => {
                self.compute(node, kind, prepared, inputs, audit, logger)
                    .await
            }
            kind @ (NodeType::HttpCall | NodeType::Email | NodeType::Llm) => {
                self.http_node(node, kind, prepared, inputs, audit, &logger)
                    .await
            }
            NodeType::Harness => {
                self.harness_node(node, prepared, inputs, audit, &logger)
                    .await
            }
            NodeType::Delay => {
                let params = materialize_prepared_params(self, prepared).await?;
                let ms = params["ms"]
                    .as_u64()
                    .or_else(|| params["ms"].as_str().and_then(|s| s.parse().ok()))
                    .ok_or_else(|| TaskError::Business("delay ms required".into()))?;
                let wake = chrono::Utc::now()
                    .timestamp_millis()
                    .checked_add(
                        i64::try_from(ms)
                            .map_err(|_| TaskError::Business("delay overflow".into()))?,
                    )
                    .ok_or_else(|| TaskError::Business("delay overflow".into()))?;
                let wake_at = chrono::DateTime::from_timestamp_millis(wake)
                    .map(|at| {
                        at.with_timezone(&chrono::Local)
                            .format("%Y-%m-%d %H:%M:%S")
                            .to_string()
                    })
                    .unwrap_or_else(|| wake.to_string());
                logger.info(format!("delay {ms}ms，等待至 {wake_at}"));
                Ok(ResultOutcome::Wait {
                    wait: WaitRequest {
                        kind: WaitKind::Delay,
                        wake_at: Some(wake),
                        output: Some(StoredValue::Inline(json!({"slept_ms": ms}))),
                        workflow_id: None,
                        input_mapping: None,
                    },
                })
            }
            NodeType::HumanTask => {
                logger.info("等待人工信号（run.signal）");
                Ok(ResultOutcome::Wait {
                    wait: WaitRequest {
                        kind: WaitKind::Signal,
                        wake_at: None,
                        output: None,
                        workflow_id: None,
                        input_mapping: None,
                    },
                })
            }
            NodeType::Input => {
                // 逐字段解析：run 输入里已有（非 null）的值优先，否则用字段默认值；
                // 有字段两者皆无 → 挂起等用户填写，交付的对象成为本节点输出
                let params = materialize_prepared_params(self, prepared).await?;
                // 建图期已校验 fields 为数组；走到这里还不是数组，只可能是
                // ${} 模板展开出了非数组——不能静默放空，按业务错误失败
                let Some(fields) = params["fields"].as_array() else {
                    return Err(TaskError::Business(
                        "input 节点的 fields 必须是数组（模板展开后亦然）".into(),
                    ));
                };
                let run_input = inputs.input.as_object();
                let mut resolved = serde_json::Map::new();
                let mut waiting = false;
                for field in fields {
                    let Some(key) = field.get("key").and_then(Value::as_str) else {
                        continue;
                    };
                    let value = run_input
                        .and_then(|o| o.get(key))
                        .filter(|v| !v.is_null())
                        .or_else(|| field.get("default"));
                    match value {
                        Some(v) => {
                            resolved.insert(key.to_string(), v.clone());
                        }
                        None => waiting = true,
                    }
                }
                if !waiting {
                    logger.info("输入字段已齐，直接放行");
                    return Ok(ResultOutcome::Success {
                        output: store_value(
                            audit,
                            &self.journal_id,
                            &Value::Object(resolved),
                            INPUT_BUDGET,
                        )
                        .await?,
                        branch: None,
                    });
                }
                logger.info("输入字段未齐，等待用户填写（run.signal 交付）");
                Ok(ResultOutcome::Wait {
                    wait: WaitRequest {
                        kind: WaitKind::Signal,
                        wake_at: None,
                        output: None,
                        workflow_id: None,
                        input_mapping: None,
                    },
                })
            }
            NodeType::SubWorkflow => {
                let params = materialize_prepared_params(self, prepared).await?;
                let workflow_id = params["workflow_id"]
                    .as_str()
                    .ok_or_else(|| TaskError::Business("child workflow required".into()))?
                    .to_owned();
                logger.info(format!("启动子工作流 {workflow_id}"));
                Ok(ResultOutcome::Wait {
                    wait: WaitRequest {
                        kind: WaitKind::Child,
                        wake_at: None,
                        output: None,
                        workflow_id: Some(workflow_id),
                        // The mapping is already durable in InputPrepared.params.
                        input_mapping: None,
                    },
                })
            }
        }
    }

    /// 一期 join_values 的等价实现：单值直通；多值按键序组合原始 JSON。
    async fn join_end_outputs(
        &mut self,
        prepared: &Prepared,
        audit: &mut AuditStream,
    ) -> Result<StoredValue, TaskError> {
        let mut values: Vec<(String, StoredValue)> = prepared
            .predecessors
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        values.sort_by(|a, b| a.0.cmp(&b.0));
        if values.len() == 1 {
            return Ok(values.pop().unwrap().1);
        }
        if values.is_empty() {
            return Ok(StoredValue::Inline(Value::Null));
        }
        let mut composed: Vec<u8> = b"{".to_vec();
        for (index, (key, value)) in values.iter().enumerate() {
            if index > 0 {
                composed.push(b',');
            }
            composed.extend_from_slice(&serde_json::to_vec(key)?);
            composed.push(b':');
            match value {
                StoredValue::Inline(inline) => {
                    composed.extend_from_slice(&flow_journal::codec::bounded_json(
                        inline,
                        flow_journal::INLINE_BYTES,
                    )?);
                }
                StoredValue::Ref(reference) => {
                    if reference.codec != ValueCodec::Json {
                        return Err(TaskError::Invalid(
                            "non-JSON reference in JSON composition".into(),
                        ));
                    }
                    let bytes = self
                        .take_transfer(&TransferKey::NodeInput(key.clone()).to_wire())
                        .await?;
                    composed.extend_from_slice(&bytes);
                }
            }
        }
        composed.push(b'}');
        store_bytes(audit, &self.journal_id, &composed, ValueCodec::Json).await
    }

    /// script/condition：纯计算（一期 `Attempt::compute` 等价）。
    async fn compute(
        &mut self,
        _node: &Node,
        kind: NodeType,
        prepared: &Prepared,
        inputs: &TaskInputs,
        audit: &mut AuditStream,
        logger: flow_engine::NodeLogger,
    ) -> Result<ResultOutcome, TaskError> {
        // 输入面必须与提交的准备一致：用 prepared 里的原始 StoredValue 重新
        // 物化（Ref 已在装配阶段就位）。
        let mut budget = INPUT_BUDGET;
        let params_value = self
            .materialize_stored(
                &prepared.params,
                &TransferKey::PreparedParams.to_wire(),
                &mut budget,
            )
            .await?;
        let code = match kind {
            NodeType::Script => params_value["code"].as_str().unwrap_or("").to_owned(),
            NodeType::Condition => format!(
                "return ({});",
                params_value["expr"].as_str().unwrap_or("false")
            ),
            _ => unreachable!(),
        };
        let _ = inputs;
        let timeout = Duration::from_millis(
            params_value["timeout_ms"]
                .as_u64()
                .unwrap_or(2000)
                .min(30_000),
        );
        let input = self
            .materialize_stored(
                &prepared.input,
                &TransferKey::RunInput.to_wire(),
                &mut budget,
            )
            .await?;
        let mut nodes = serde_json::Map::new();
        for (node_id, value) in &prepared.predecessors {
            nodes.insert(
                node_id.clone(),
                self.materialize_stored(
                    value,
                    &TransferKey::NodeInput(node_id.clone()).to_wire(),
                    &mut budget,
                )
                .await?,
            );
        }
        let logger = logger.clone();
        let determinism = self.determinism();
        let result = tokio::task::spawn_blocking(move || {
            flow_engine::expr::eval_body_recorded(
                &code,
                &input,
                &Value::Object(nodes),
                timeout,
                &logger,
                determinism,
            )
        })
        .await
        .map_err(|e| TaskError::Business(e.to_string()))?
        .map_err(|e| TaskError::Business(e.to_string()))?;
        let branch = (kind == NodeType::Condition).then(|| flow_engine::exec::truthy(&result));
        let output = store_value(audit, &self.journal_id, &result, 8 * 1024 * 1024).await?;
        Ok(ResultOutcome::Success { output, branch })
    }

    fn determinism(&self) -> Option<flow_engine::expr::JsDeterminism> {
        let context = self.task.node.get("_flow_execution")?;
        let digest = flow_journal::codec::digest(context["seed"].as_str()?.as_bytes());
        Some(flow_engine::expr::JsDeterminism {
            clock_ms: context["clock_ms"].as_i64()?,
            seed: u32::from_str_radix(&digest[..8], 16).ok()?,
        })
    }

    async fn take_transfer(&mut self, transfer_id: &str) -> Result<Vec<u8>, TaskError> {
        // 输入装配在 execute 前完成；此处从会话级缓冲取。TaskRunner 的
        // transfers 在 materialize_inputs 中局部持有，这里简化为重新等待
        // InputReady（已就位时立即返回）。
        let deadline = tokio::time::Instant::now() + Duration::from_millis(DURABLE_WAIT_TIMEOUT_MS);
        loop {
            if let Some(bytes) = self.transfers.take(transfer_id) {
                return Ok(bytes);
            }
            let message = self.receive_until(deadline, "transfer wait").await?;
            match message {
                Message::TransferChunk { .. } => self.transfers.feed(&message)?,
                Message::InputReady { .. } => self.transfers.input_ready(&message)?,
                Message::Cancel { dispatch_id, .. } if dispatch_id == self.dispatch_id => {
                    return Err(TaskError::Cancelled)
                }
                _ => continue,
            }
        }
    }

    async fn materialize_stored(
        &mut self,
        stored: &StoredValue,
        transfer_id: &str,
        budget: &mut usize,
    ) -> Result<Value, TaskError> {
        match stored {
            StoredValue::Inline(value) => Ok(value.clone()),
            StoredValue::Ref(reference) => {
                let bytes = self.take_transfer(transfer_id).await?;
                if bytes.len() > *budget {
                    return Err(TaskError::Invalid("aggregate input exceeds 8 MiB".into()));
                }
                *budget -= bytes.len();
                match reference.codec {
                    ValueCodec::Json => Ok(serde_json::from_slice(&bytes)?),
                    ValueCodec::Utf8 => Ok(Value::String(
                        String::from_utf8(bytes).map_err(|e| TaskError::Invalid(e.to_string()))?,
                    )),
                    ValueCodec::Bytes => {
                        Err(TaskError::Invalid("binary value in JSON input face".into()))
                    }
                }
            }
        }
    }

    /// http/email：请求构造→许可→调用→Outcome→输出（I07 执行器侧）。
    async fn http_node(
        &mut self,
        _node: &Node,
        kind: NodeType,
        prepared: &Prepared,
        _inputs: &TaskInputs,
        audit: &mut AuditStream,
        logger: &flow_engine::NodeLogger,
    ) -> Result<ResultOutcome, TaskError> {
        // 恢复路径：已有 Outcome 直接派生输出，不重发请求。
        if let Some(outcome_value) = self
            .task
            .previous_outcome
            .clone()
            .filter(|_| !self.is_regression())
        {
            logger.info("已有捕获的 operation outcome，不重复发起请求（恢复）");
            let outcome: StoredValue =
                serde_json::from_value(outcome_value).map_err(TaskError::from)?;
            let output = self.http_output(&outcome, kind, audit).await?;
            return Ok(ResultOutcome::Success {
                output,
                branch: None,
            });
        }
        let mut budget = INPUT_BUDGET;
        let mut params = self
            .materialize_stored(
                &prepared.params,
                &TransferKey::PreparedParams.to_wire(),
                &mut budget,
            )
            .await?;
        if kind == NodeType::Email {
            let recipients = match &params["to"] {
                Value::Array(list) => list
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", "),
                other => other.as_str().unwrap_or("").to_owned(),
            };
            logger.info(format!("→ email to {recipients}"));
            let name = params["api_key"]
                .as_str()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| TaskError::Business("api_key secret reference required".into()))?
                .to_owned();
            let (url, body) = flow_engine::exec::email_request(&params)
                .map_err(|e| TaskError::Business(e.message))?;
            params = json!({
                "method": "POST",
                "url": url,
                "body": body,
                "timeout_ms": params["timeout_ms"],
                "credential": {"scheme": "bearer", "secret_ref": name},
            });
        }
        if kind == NodeType::Llm {
            params["method"] = json!("POST");
        }
        let method = params["method"].as_str().unwrap_or("GET").to_uppercase();
        if !HTTP_METHODS.contains(&method.as_str()) {
            return Err(TaskError::Invalid("invalid HTTP method".into()));
        }
        let url = params["url"]
            .as_str()
            .ok_or_else(|| TaskError::Business("HTTP URL missing".into()))?
            .to_owned();
        let mut request = json!({
            "method": method,
            "url": url,
            "headers": params.get("headers").cloned().unwrap_or(json!({})),
            "body": params.get("body").cloned().unwrap_or(Value::Null),
            "proxy": params.get("proxy").cloned().unwrap_or(Value::Null),
            "credential": params.get("credential").cloned().unwrap_or(Value::Null),
        });
        if kind == NodeType::Llm {
            request["adapter"] = json!("llm");
        }
        if self.is_regression() {
            let outcome = self.fixture_outcome(&request)?;
            let output = self.http_output(&outcome, kind, audit).await?;
            return Ok(ResultOutcome::Success {
                output,
                branch: None,
            });
        }
        let http_method = reqwest::Method::from_bytes(method.as_bytes())
            .map_err(|e| TaskError::Invalid(e.to_string()))?;
        // proxy：非法 URL 是配置错误（授权前构建请求时就该失败的最晚一刻）
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never());
        if let Some(proxy) = params["proxy"].as_str().filter(|s| !s.trim().is_empty()) {
            builder = builder.proxy(
                reqwest::Proxy::all(proxy)
                    .map_err(|e| TaskError::Business(format!("invalid proxy URL: {e}")))?,
            );
        }
        let client = builder
            .build()
            .map_err(|e| TaskError::Invalid(e.to_string()))?;
        let build_outbound = |credential: Option<String>| -> Result<reqwest::Request, TaskError> {
            let mut request = client
                .request(
                    http_method.clone(),
                    reqwest::Url::parse(&url).map_err(|e| TaskError::Invalid(e.to_string()))?,
                )
                .timeout(Duration::from_millis(
                    params["timeout_ms"]
                        .as_u64()
                        .unwrap_or(flow_engine::exec::DEFAULT_HTTP_TIMEOUT_MS)
                        .clamp(1, flow_engine::exec::HTTP_TIMEOUT_MAX_MS),
                ));
            if let Some(headers) = params["headers"].as_object() {
                for (key, value) in headers {
                    request = request.header(
                        key,
                        value
                            .as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| value.to_string()),
                    );
                }
            }
            if !params["body"].is_null() {
                request = request.json(&params["body"]);
            }
            if let Some(secret) = credential {
                request = request.bearer_auth(secret);
            }
            request
                .build()
                .map_err(|e| TaskError::Invalid(e.to_string()))
        };
        // T7：畸形请求（换行 header、非法 URL/方法/proxy）必须在授权前失败。
        // 授权即承诺「副作用可能已发生」，永远发不出去的请求不该进 uncertain——
        // 这条不变量原先由 master 侧进程内实现保证，删除后由执行器自己守住。
        build_outbound(None)?;
        let credential = self.authorize_operation(request).await?;
        let outbound_request = build_outbound(credential)?;
        // 展示面用的 URL（日志行）：query 里的敏感参数值脱敏，发起请求用的
        // 永远是原始 `url`。
        let url_label = flow_engine::redact_url(&url);
        logger.info(format!("→ {method} {url_label}"));
        let started = std::time::Instant::now();
        let mut response = match client.execute(outbound_request).await {
            Ok(response) => response,
            Err(error) => {
                logger.error(format!(
                    "请求 {url_label} 失败（{}ms）：{error}",
                    started.elapsed().as_millis()
                ));
                // 授权后调用失败：Outcome 未知，交主进程按 uncertain 处理。
                return Err(TaskError::Uncertain(error.to_string()));
            }
        };
        if response
            .content_length()
            .is_some_and(|length| length > flow_journal::MAX_VALUE_BYTES)
        {
            return Err(TaskError::Uncertain(
                "response exceeds capture limit; no complete outcome retained".into(),
            ));
        }
        let status = response.status().as_u16();
        // 重名响应头（Set-Cookie 等）按 RFC 7230 语义合并为逗号分隔，
        // 不能让后者覆盖前者——headers 是审计证据的保真面，丢多值即失真。
        let mut headers: serde_json::Map<String, Value> = serde_json::Map::new();
        for (name, value) in response.headers() {
            let value = value.to_str().unwrap_or("");
            headers
                .entry(name.to_string())
                .and_modify(|existing| {
                    if let Value::String(joined) = existing {
                        joined.push_str(", ");
                        joined.push_str(value);
                    }
                })
                .or_insert_with(|| json!(value));
        }
        let mut raw: Vec<u8> = Vec::new();
        while let Some(bytes) = response
            .chunk()
            .await
            .map_err(|e| TaskError::Uncertain(e.to_string()))?
        {
            if raw.len() as u64 + bytes.len() as u64 > flow_journal::MAX_VALUE_BYTES {
                return Err(TaskError::Uncertain(
                    "response exceeds capture limit; no complete outcome retained".into(),
                ));
            }
            raw.extend_from_slice(&bytes);
        }
        // 响应元信息一行：状态码、耗时、体量；非 2xx/3xx 升级 warn
        let response_line = format!(
            "← HTTP {status}（{}ms，body {}B）",
            started.elapsed().as_millis(),
            raw.len()
        );
        if status >= 400 {
            logger.warn(response_line);
        } else {
            logger.info(response_line);
        }
        let raw_stored = store_bytes(audit, &self.journal_id, &raw, ValueCodec::Bytes).await?;
        // 捕获的原始字节留在本地装配器，供输出派生阶段复用（不重读 journal）。
        let body_ref = match &raw_stored {
            StoredValue::Ref(reference) => serde_json::to_value(reference)?,
            StoredValue::Inline(value) => value.clone(),
        };
        let body_output_id = match &raw_stored {
            StoredValue::Ref(reference) => reference.output_id.clone(),
            StoredValue::Inline(_) => String::new(),
        };
        if !body_output_id.is_empty() {
            self.transfers.insert_ready(
                TransferKey::OperationBody(body_output_id.clone()).to_wire(),
                raw,
            );
        }
        let outcome = StoredValue::inline(
            json!({"status": status, "headers": headers, "body_raw": body_ref}),
        )?;
        let outcome_seq = audit.push(
            "operation_outcome",
            json!({"outcome": serde_json::to_value(&outcome)?}),
        )?;
        audit.await_durable(outcome_seq, &self.cancel).await?;
        let output = self.http_output(&outcome, kind, audit).await?;
        Ok(ResultOutcome::Success {
            output,
            branch: None,
        })
    }

    /// 外部操作的请求发送与许可等待（http/harness 共用）：RequestOperation →
    /// OperationPermit（取消/断线唤醒）。返回许可携带的 credential（harness 不用）。
    async fn authorize_operation(&mut self, request: Value) -> Result<Option<String>, TaskError> {
        if self.is_regression() {
            return Err(TaskError::Business(
                "external operations are disabled in regression".into(),
            ));
        }
        let request_bytes = flow_journal::codec::bounded_json(&request, INPUT_BUDGET)?;
        let fingerprint = flow_journal::codec::digest(&request_bytes);
        let operation_id = uuid::Uuid::now_v7().to_string();
        // v2：请求字节一律走 data 通道（再小也走），控制帧只带身份与指纹。
        let transfer_id = TransferKey::OperationRequest(operation_id.clone()).to_wire();
        {
            let mut offset = 0u64;
            for chunk in request_bytes.chunks(flow_engine::execution_protocol::TRANSFER_CHUNK_BYTES)
            {
                self.outbound
                    .send(Message::TransferChunk {
                        dispatch_id: self.dispatch_id.clone(),
                        transfer_id: transfer_id.clone(),
                        offset,
                        bytes: chunk.to_vec(),
                        digest: hex::encode(sha2::Sha256::digest(chunk)),
                    })
                    .await
                    .map_err(|e| TaskError::Closed(e.to_string()))?;
                offset += chunk.len() as u64;
            }
            self.outbound
                .send(Message::InputReady {
                    dispatch_id: self.dispatch_id.clone(),
                    transfer_id: transfer_id.clone(),
                    total_bytes: request_bytes.len() as u64,
                    digest: hex::encode(sha2::Sha256::digest(&request_bytes)),
                })
                .await
                .map_err(|e| TaskError::Closed(e.to_string()))?;
        }
        self.outbound
            .send(Message::RequestOperation {
                dispatch_id: self.dispatch_id.clone(),
                command_id: TransferKey::OperationRequest(operation_id.clone()).to_wire(),
                operation_id: operation_id.clone(),
                request_fingerprint: fingerprint.clone(),
                transfer_id,
            })
            .await
            .map_err(|e| TaskError::Closed(e.to_string()))?;
        let permit_deadline =
            tokio::time::Instant::now() + Duration::from_millis(DURABLE_WAIT_TIMEOUT_MS);
        let credential = loop {
            let message = self.receive_until(permit_deadline, "permit").await?;
            match message {
                Message::OperationPermit {
                    dispatch_id,
                    operation_id: permitted,
                    request_fingerprint,
                    credential,
                    ..
                } => {
                    if dispatch_id != self.dispatch_id
                        || permitted != operation_id
                        || request_fingerprint != fingerprint
                    {
                        return Err(TaskError::Invalid("permit identity mismatch".into()));
                    }
                    break credential;
                }
                Message::Cancel { dispatch_id, .. } if dispatch_id == self.dispatch_id => {
                    return Err(TaskError::Cancelled)
                }
                Message::AuditAck {
                    durable_audit_seq,
                    durable_bytes,
                    ..
                } => {
                    let _ = self.ack_tx.send((durable_audit_seq, durable_bytes));
                }
                _ => continue,
            }
        };
        Ok(credential)
    }

    /// harness：子进程执行（许可→spawn→Outcome→输出），与 http_node 同一条授权路径。
    /// 退出码非 0 → outcome 留存、节点判死；超时/spawn 后 IO 失败 → Uncertain
    /// （进程死前副作用未知）；spawn 失败 → 记录 spawn_error outcome 后 Business。
    async fn harness_node(
        &mut self,
        _node: &Node,
        prepared: &Prepared,
        _inputs: &TaskInputs,
        audit: &mut AuditStream,
        logger: &flow_engine::NodeLogger,
    ) -> Result<ResultOutcome, TaskError> {
        // 恢复路径：已有 Outcome 直接派生输出，不重跑进程。
        if let Some(outcome_value) = self
            .task
            .previous_outcome
            .clone()
            .filter(|_| !self.is_regression())
        {
            logger.info("已有捕获的 operation outcome，不重复启动进程（恢复）");
            let outcome: StoredValue =
                serde_json::from_value(outcome_value).map_err(TaskError::from)?;
            let output = self.harness_output(&outcome, audit).await?;
            return Ok(ResultOutcome::Success {
                output,
                branch: None,
            });
        }
        let mut budget = INPUT_BUDGET;
        let params = self
            .materialize_stored(
                &prepared.params,
                &TransferKey::PreparedParams.to_wire(),
                &mut budget,
            )
            .await?;
        let (command, args, workdir, timeout_ms, prompt) =
            flow_engine::exec::harness_request(&params)
                .map_err(|e| TaskError::Business(e.message))?;
        // 用户可配 timeout_ms 同样设上限：超限的等待会无限占用执行槽位
        // （本地/远程都没有对应 wall-clock 预算）。
        let timeout_ms = timeout_ms.clamp(1, flow_engine::exec::HARNESS_TIMEOUT_MAX_MS);
        let request = json!({
            "command": command,
            "args": args,
            "workdir": workdir,
            "timeout_ms": timeout_ms,
            "prompt": prompt,
        });
        if self.is_regression() {
            let outcome = self.fixture_outcome(&request)?;
            let output = self.harness_output(&outcome, audit).await?;
            return Ok(ResultOutcome::Success {
                output,
                branch: None,
            });
        }
        self.authorize_operation(request).await?;
        // prompt 可能含敏感内容，不进日志；只记命令行与超时。
        logger.info(format!(
            "→ harness {} {}（timeout {timeout_ms}ms）",
            command,
            args.join(" ")
        ));
        let started = std::time::Instant::now();
        let mut child =
            match flow_engine::exec::harness_command(&command, &args, workdir.as_deref()).spawn() {
                Ok(child) => child,
                Err(error) => {
                    logger.error(format!("harness 启动失败：{error}"));
                    // 进程从未启动：结果确定。记录 Outcome 后按业务失败收口
                    let outcome = StoredValue::inline(json!({"spawn_error": error.to_string()}))?;
                    let outcome_seq = audit.push(
                        "operation_outcome",
                        json!({"outcome": serde_json::to_value(&outcome)?}),
                    )?;
                    audit.await_durable(outcome_seq, &self.cancel).await?;
                    return Err(TaskError::Business(format!(
                        "harness spawn failed: {error}"
                    )));
                }
            };
        let output = capture_harness(
            &mut child,
            prompt.as_bytes(),
            Duration::from_millis(timeout_ms),
            &self.cancel,
        )
        .await?;
        let exit_code = output
            .status
            .code()
            .map(|code| code.to_string())
            .unwrap_or_else(|| "signal".into());
        logger.info(format!(
            "← exit {exit_code}（{}ms，stdout {}B）",
            started.elapsed().as_millis(),
            output.stdout.len()
        ));
        let raw_stored =
            store_bytes(audit, &self.journal_id, &output.stdout, ValueCodec::Bytes).await?;
        // 捕获的原始字节留在本地装配器，供输出派生阶段复用（不重读 journal）。
        let body_ref = match &raw_stored {
            StoredValue::Ref(reference) => serde_json::to_value(reference)?,
            StoredValue::Inline(value) => value.clone(),
        };
        let body_output_id = match &raw_stored {
            StoredValue::Ref(reference) => reference.output_id.clone(),
            StoredValue::Inline(_) => String::new(),
        };
        if !body_output_id.is_empty() {
            self.transfers.insert_ready(
                TransferKey::OperationBody(body_output_id.clone()).to_wire(),
                output.stdout,
            );
        }
        // 字段名沿用 http 的 body_raw：恢复路径按这个名字重传原始字节
        let outcome = StoredValue::inline(json!({
            "exit_code": output.status.code(),
            "body_raw": body_ref,
            "stderr": flow_engine::exec::stderr_tail(&String::from_utf8_lossy(&output.stderr)),
        }))?;
        let outcome_seq = audit.push(
            "operation_outcome",
            json!({"outcome": serde_json::to_value(&outcome)?}),
        )?;
        audit.await_durable(outcome_seq, &self.cancel).await?;
        let output = self.harness_output(&outcome, audit).await?;
        Ok(ResultOutcome::Success {
            output,
            branch: None,
        })
    }

    fn is_regression(&self) -> bool {
        self.task.node.pointer("/_flow_execution/regression") == Some(&Value::Bool(true))
    }

    fn fixture_outcome(&self, request: &Value) -> Result<StoredValue, TaskError> {
        let expected = self
            .task
            .node
            .pointer("/_flow_execution/fixture_fingerprint")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                TaskError::Business("missing regression fixture; no external call performed".into())
            })?;
        let actual = flow_journal::codec::fingerprint(request, INPUT_BUDGET)?;
        if actual != expected {
            return Err(TaskError::Business(format!("regression request differs from recorded fixture (expected {expected}, actual {actual}); no external call performed")));
        }
        serde_json::from_value(
            self.task
                .previous_outcome
                .clone()
                .ok_or_else(|| TaskError::Business("regression outcome missing".into()))?,
        )
        .map_err(Into::into)
    }

    /// 一期 `Attempt::harness_output` 等价：Outcome → 节点输出（重跑同一派生）。
    async fn harness_output(
        &mut self,
        outcome: &StoredValue,
        audit: &mut AuditStream,
    ) -> Result<StoredValue, TaskError> {
        let outcome_value = match outcome {
            StoredValue::Inline(value) => value.clone(),
            StoredValue::Ref(_) => return Err(TaskError::Invalid("outcome must be inline".into())),
        };
        if let Some(error) = outcome_value["spawn_error"].as_str() {
            return Err(TaskError::Business(format!(
                "harness spawn failed: {error}"
            )));
        }
        let exit_code = outcome_value["exit_code"].as_i64().map(|c| c as i32);
        let stderr = outcome_value["stderr"].as_str().unwrap_or("").to_owned();
        let raw: flow_journal::ValueRef =
            serde_json::from_value(outcome_value["body_raw"].clone()).map_err(TaskError::from)?;
        if raw.total_bytes > 8 * 1024 * 1024 {
            return Err(TaskError::Business(
                "integration decoding exceeds 8 MiB; raw outcome retained".into(),
            ));
        }
        let stdout = self.raw_bytes(&raw).await?;
        let output = flow_engine::exec::harness_output(
            exit_code,
            String::from_utf8_lossy(&stdout).into_owned(),
            stderr,
        )
        .map_err(|e| TaskError::Business(format!("{}; outcome retained", e.message)))?;
        store_value(audit, &self.journal_id, &output, 8 * 1024 * 1024).await
    }

    /// 一期 `Attempt::http_output` 等价：状态/头/体组合或 email 解析。
    async fn http_output(
        &mut self,
        outcome: &StoredValue,
        kind: NodeType,
        audit: &mut AuditStream,
    ) -> Result<StoredValue, TaskError> {
        let outcome_value = match outcome {
            StoredValue::Inline(value) => value.clone(),
            StoredValue::Ref(_) => return Err(TaskError::Invalid("outcome must be inline".into())),
        };
        let status = outcome_value["status"]
            .as_u64()
            .ok_or_else(|| TaskError::Invalid("invalid HTTP outcome".into()))?;
        let raw: flow_journal::ValueRef =
            serde_json::from_value(outcome_value["body_raw"].clone()).map_err(TaskError::from)?;
        if kind == NodeType::Email {
            if status >= 400 {
                return Err(TaskError::Business(format!(
                    "HTTP {status}; outcome retained"
                )));
            }
            if raw.total_bytes > 8 * 1024 * 1024 {
                return Err(TaskError::Business(
                    "integration decoding exceeds 8 MiB; raw outcome retained".into(),
                ));
            }
            let bytes = self
                .take_transfer(&TransferKey::OperationBody(raw.output_id.clone()).to_wire())
                .await
                .ok();
            // 恢复路径：body 不在本进程时需要主进程送回；v1 由
            // previous_outcome 附带 raw bytes（见下）。
            let bytes = match bytes {
                Some(bytes) => bytes,
                None => return Err(TaskError::Invalid("outcome body bytes unavailable".into())),
            };
            // 2xx 但非 JSON 的代理响应是业务层约定破裂，不是基础设施
            // 故障——outcome 已留存，按业务失败收口（Reject/回收路径会把
            // 平台故障放大成 uncertain 人工裁决）。
            let response: Value = serde_json::from_slice(&bytes).map_err(|e| {
                TaskError::Business(format!(
                    "email response must be JSON: {e}; outcome retained"
                ))
            })?;
            let output = json!({"status": status, "id": response["id"]});
            return store_value(audit, &self.journal_id, &output, 8 * 1024 * 1024).await;
        }
        // HTTP 输出：body 为完整 JSON 时原样嵌入，否则转义为文本。
        // 结构字节（{ , : }）由 flow_journal::value::ObjectWriter 的
        // serde_json Formatter 拥有，这里只提供完整值。
        let body = self.raw_bytes(&raw).await?;
        if kind == NodeType::Llm {
            if body.len() > INPUT_BUDGET {
                return Err(TaskError::Business(
                    "model response exceeds 8 MiB; raw outcome retained".into(),
                ));
            }
            let response: Value = serde_json::from_slice(&body).map_err(|e| {
                TaskError::Business(format!(
                    "model response must be JSON: {e}; outcome retained"
                ))
            })?;
            if let Some(schema) = self.task.node.pointer("/params/response_schema") {
                flow_engine::schema::validate(schema, &response).map_err(|e| {
                    TaskError::Business(format!("model response contract: {e}; outcome retained"))
                })?;
            }
        }
        let valid_json = {
            let mut de = serde_json::Deserializer::from_slice(&body);
            serde::de::IgnoredAny::deserialize(&mut de).is_ok() && de.end().is_ok()
        };
        let mut composed = Vec::with_capacity(body.len() + 256);
        {
            let mut object = flow_journal::value::ObjectWriter::begin(&mut composed)?;
            object.key("status")?;
            object.value(&status)?;
            object.key("headers")?;
            object.value(&outcome_value["headers"])?;
            object.key("body")?;
            if valid_json {
                object.raw_value_bytes(&body)?;
            } else {
                serde_json::to_writer(
                    object.value_writer(),
                    &String::from_utf8_lossy(&body).into_owned(),
                )?;
            }
            object.finish()?;
        }
        let output = store_bytes(audit, &self.journal_id, &composed, ValueCodec::Json).await?;
        if status >= 400 {
            return Err(TaskError::Business(format!(
                "HTTP {status}; outcome retained"
            )));
        }
        Ok(output)
    }

    async fn raw_bytes(
        &mut self,
        reference: &flow_journal::ValueRef,
    ) -> Result<Vec<u8>, TaskError> {
        // 当前进程刚捕获的响应：从内存取；恢复路径由主进程随
        // previous_outcome 重传（transfer id 约定见 TransferKey::OperationBody）。
        self.take_transfer(&TransferKey::OperationBody(reference.output_id.clone()).to_wire())
            .await
    }
}

struct TaskInputs {
    input: Value,
    nodes: Value,
}

async fn materialize_prepared_params(
    runner: &mut TaskRunner,
    prepared: &Prepared,
) -> Result<Value, TaskError> {
    let mut budget = INPUT_BUDGET;
    runner
        .materialize_stored(
            &prepared.params,
            &TransferKey::PreparedParams.to_wire(),
            &mut budget,
        )
        .await
}

/// 排空观测尾批：在每个发 Result 控制帧的出口前调用。`logger` 必须是
/// 最后一个克隆——drop 它关闭通道，转发任务才能 flush 完退出（顺序契约
/// 见 [`ObservationClose`]）。
async fn drain_observations(logger: flow_engine::NodeLogger, close: Option<ObservationClose>) {
    drop(logger);
    if let Some(close) = close {
        close.close().await;
    }
}

/// 节点类型的叙事名：观测日志的开始/终态行用它拼出人话。
fn narrative_kind(kind: Option<NodeType>) -> Option<&'static str> {
    match kind? {
        NodeType::Script => Some("脚本"),
        NodeType::Condition => Some("条件判断"),
        NodeType::HttpCall => Some("HTTP 请求"),
        NodeType::Email => Some("发送邮件"),
        NodeType::Harness => Some("本地命令"),
        NodeType::Llm => Some("模型调用"),
        NodeType::Delay => Some("延时等待"),
        NodeType::HumanTask => Some("人工任务"),
        NodeType::Input => Some("输入收集"),
        NodeType::SubWorkflow => Some("子工作流"),
        NodeType::Start | NodeType::End => None,
    }
}

/// 重试尝试的归属后缀；第 1 次尝试不标（避免每次执行都带冗余括号）。
fn attempt_label(attempt: u32) -> String {
    if attempt > 1 {
        format!("（第 {attempt} 次尝试）")
    } else {
        String::new()
    }
}

fn elapsed_label(elapsed: Duration) -> String {
    let ms = elapsed.as_millis();
    if ms < 1000 {
        format!("{ms}ms")
    } else {
        format!("{:.1}s", elapsed.as_secs_f64())
    }
}

#[allow(dead_code)]
fn unused() {}

/// Capture all three pipes concurrently under one deadline. Partial output is never success.
async fn capture_harness(
    child: &mut tokio::process::Child,
    prompt: &[u8],
    timeout: Duration,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<std::process::Output, TaskError> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| TaskError::Invalid("harness stdin unavailable".into()))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| TaskError::Invalid("harness stdout unavailable".into()))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| TaskError::Invalid("harness stderr unavailable".into()))?;
    let capture = async {
        let input = async {
            match stdin.write_all(prompt).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {}
                Err(e) => return Err(e),
            }
            drop(stdin);
            Ok::<_, std::io::Error>(())
        };
        let out = async {
            let mut bytes = Vec::new();
            let mut buffer = [0; 8192];
            loop {
                let n = stdout.read(&mut buffer).await?;
                if n == 0 {
                    break;
                }
                if bytes.len() as u64 + n as u64 > flow_journal::MAX_VALUE_BYTES {
                    return Err(std::io::Error::other(
                        "harness stdout exceeds capture limit; result incomplete",
                    ));
                }
                bytes.extend_from_slice(&buffer[..n]);
            }
            Ok::<_, std::io::Error>(bytes)
        };
        let err = async {
            let mut tail = std::collections::VecDeque::with_capacity(512);
            let mut buffer = [0; 8192];
            loop {
                let n = stderr.read(&mut buffer).await?;
                if n == 0 {
                    break;
                }
                for byte in &buffer[..n] {
                    if tail.len() == 512 {
                        tail.pop_front();
                    }
                    tail.push_back(*byte);
                }
            }
            Ok::<_, std::io::Error>(tail.into_iter().collect::<Vec<_>>())
        };
        let (_, stdout, stderr, status) = tokio::try_join!(input, out, err, child.wait())?;
        Ok::<_, std::io::Error>(std::process::Output {
            status,
            stdout,
            stderr,
        })
    };
    let result = tokio::select! {
        result = tokio::time::timeout(timeout, capture) => match result {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(error)) => Err(TaskError::Uncertain(format!("harness capture: {error}"))),
            Err(_) => Err(TaskError::Uncertain("harness deadline exceeded; result incomplete".into())),
        },
        _ = cancel.cancelled() => Err(TaskError::Uncertain("harness cancelled; result incomplete".into())),
    };
    if result.is_err() {
        let _ = child.start_kill();
        let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
    }
    result
}

#[cfg(all(test, unix))]
mod harness_capture_tests {
    use super::*;
    async fn capture(
        script: &str,
        input: &[u8],
        timeout: Duration,
    ) -> Result<std::process::Output, TaskError> {
        let mut child =
            flow_engine::exec::harness_command("/bin/sh", &["-c".into(), script.into()], None)
                .spawn()
                .unwrap();
        let result = capture_harness(
            &mut child,
            input,
            timeout,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;
        assert!(child.try_wait().unwrap().is_some(), "child must be reaped");
        result
    }
    #[tokio::test]
    async fn blocked_stdin_obeys_deadline_and_reaps() {
        assert!(matches!(
            capture(
                "exec sleep 30",
                &vec![0; 1024 * 1024],
                Duration::from_millis(50)
            )
            .await,
            Err(TaskError::Uncertain(_))
        ));
    }
    #[tokio::test]
    async fn drains_output_while_writing_prompt() {
        let output = capture(
            "head -c 131072 /dev/zero; cat",
            &vec![b'x'; 131072],
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        assert_eq!(output.stdout.len(), 262144);
    }
    #[tokio::test]
    async fn stdout_overflow_never_returns_truncated_success() {
        assert!(matches!(
            capture("exec yes", b"", Duration::from_secs(3)).await,
            Err(TaskError::Uncertain(_))
        ));
    }
    #[tokio::test]
    async fn stderr_capture_keeps_only_tail() {
        let output = capture(
            "head -c 1048576 /dev/zero >&2; printf tail >&2",
            b"",
            Duration::from_secs(3),
        )
        .await
        .unwrap();
        assert_eq!(output.stderr.len(), 512);
        assert!(output.stderr.ends_with(b"tail"));
    }
}
