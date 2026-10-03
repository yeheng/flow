//! 单次节点派发的子进程内执行（二期 §3.8，I04）。
//!
//! 一次 Execute 在同一子进程内完成准备与执行：模板、script、condition
//! 的全部用户 JS 都在本进程运行，主进程不再求值任何用户表达式。准备与
//! 执行共用一套 audit_seq、窗口、取消与结果协议，不为 prepare/execute
//! 分别派发。

use std::collections::BTreeMap;
use std::time::Duration;

use base64::Engine as _;
use serde::Deserialize as _;
use serde_json::{json, Value};
use sha2::Digest as _;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use flow_engine::execution_protocol::contract::DURABLE_WAIT_TIMEOUT_MS;
use flow_engine::execution_protocol::message::{ExecuteTask, Message, ResultOutcome, WaitRequest};
use flow_engine::journal_state::Prepared;
use flow_engine::model::Node;
use flow_engine::{NodeType, HTTP_METHODS};
use flow_journal::value::ValueCodec;
use flow_journal::StoredValue;

use crate::audit::AuditStream;
use crate::observe::ObservationBridge;
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
    /// 本任务全部输入/重传装配（生命周期覆盖整个派发）。
    pub transfers: IncomingTransfers,
    /// 已发出的唯一 Result 帧（回执丢失时按节奏幂等重发）。
    pub last_result: Option<Message>,
}

impl TaskRunner {
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
                        reason: format!("executor task failed: {error}"),
                    })
                    .await;
                // 给在途帧（含 Reject）一个短暂落地窗口，再退出。
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                TaskEnd::Recycle
            }
        }
    }

    async fn execute(&mut self) -> Result<(), TaskError> {
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
        // 观测桥（未启用时静默丢弃）。
        let logger = if self.task.observability {
            let (logger, _loss) =
                ObservationBridge::start(self.dispatch_id.clone(), self.outbound.clone());
            logger
        } else {
            ObservationBridge::disabled_logger()
        };
        // 1) 输入装配（Ref 走 transfer；inline 直接可用）。
        let inputs = self
            .materialize_inputs(Duration::from_millis(DURABLE_WAIT_TIMEOUT_MS))
            .await?;
        // 2) 准备（已有提交 InputPrepared 则跳过求值）；业务失败统一在尾部
        // 转失败 Result，不走 Reject 回收。
        let prepared: Prepared = match &self.task.prepared {
            Some(value) => serde_json::from_value(value.clone()).map_err(TaskError::from)?,
            None => match self.prepare(&node, &inputs, &mut audit).await {
                Ok(prepared) => {
                    let prepared_seq = audit.next_seq() - 1;
                    // §3.8：等 InputPrepared 对应 AuditAck 后继续已被 Execute
                    // 授权的纯计算；外部节点另需 OperationPermit。
                    audit.await_durable(prepared_seq, &self.cancel).await?;
                    prepared
                }
                Err(TaskError::Business(error)) => {
                    self.emit_failure(&mut audit, error, false).await?;
                    return Ok(());
                }
                Err(other) => return Err(other),
            },
        };
        // 3) 执行（按节点类型）；业务失败/未定外部结果转失败 Result
        // （同一派发；uncertain 由主进程依操作状态登记）。
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
        // 4) 封口并发送唯一 Result，等待 ResultCommitted。
        audit.flush().await?;
        let last_audit_seq = audit.next_seq().saturating_sub(1);
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
        self.last_result = Some(result_message);
        let deadline = Duration::from_millis(DURABLE_WAIT_TIMEOUT_MS);
        match tokio::time::timeout(deadline, self.wait_committed(&result_id)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(TaskError::Closed("ResultCommitted timeout".into())),
        }
    }

    /// 业务失败结果：封口发送 Failure 并等 ResultCommitted（与主进程
    /// NodeFailed/重试语义对接；uncertain 由主进程依操作状态判定）。
    async fn emit_failure(
        &mut self,
        audit: &mut AuditStream,
        error: String,
        uncertain: bool,
    ) -> Result<(), TaskError> {
        audit.flush().await?;
        let last_audit_seq = audit.next_seq().saturating_sub(1);
        let result_id = uuid::Uuid::now_v7().to_string();
        let result_message = Message::Result {
            dispatch_id: self.dispatch_id.clone(),
            result_id: result_id.clone(),
            last_audit_seq,
            outcome: ResultOutcome::Failure {
                error,
                uncertain_operation: uncertain,
            },
        };
        self.outbound
            .send(result_message.clone())
            .await
            .map_err(|e| TaskError::Closed(e.to_string()))?;
        self.last_result = Some(result_message);
        match tokio::time::timeout(
            Duration::from_millis(DURABLE_WAIT_TIMEOUT_MS),
            self.wait_committed(&result_id),
        )
        .await
        {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(TaskError::Closed("ResultCommitted timeout".into())),
        }
    }

    async fn wait_committed(&mut self, result_id: &str) -> Result<(), TaskError> {
        let mut last_send = None::<tokio::time::Instant>;
        loop {
            let message = tokio::select! {
                message = self.mail.recv() => message,
                _ = tokio::time::sleep(Duration::from_millis(
                    flow_engine::execution_protocol::ACK_RETRANSMIT_MS / 2 + 1,
                )) => {
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
                other => {
                    return Err(TaskError::Invalid(format!(
                        "unexpected message while waiting commit: {}",
                        other.type_name()
                    )))
                }
            }
        }
    }

    /// 装配 run 输入与前驱输出（inline 即用，Ref 等 transfer）。
    async fn materialize_inputs(&mut self, timeout: Duration) -> Result<TaskInputs, TaskError> {
        let mut budget = INPUT_BUDGET;
        // 收集需要 transfer 的引用：run 输入、前驱输出、已提交准备参数、
        // 恢复路径的历史 Outcome 原始体。
        let mut refs: Vec<String> = Vec::new();
        if let StoredValue::Ref(_) = &self.task.input {
            refs.push("in:run-input".into());
        }
        for (node_id, value) in &self.task.predecessors {
            if let StoredValue::Ref(_) = value {
                refs.push(format!("in:{node_id}"));
            }
        }
        if let Some(prepared) = &self.task.prepared {
            if let Some(params) = prepared.get("params") {
                if params.get("type").and_then(Value::as_str) == Some("ref") {
                    refs.push("in:prepared-params".into());
                }
            }
        }
        if let Some(outcome) = &self.task.previous_outcome {
            if let Some(raw) = outcome.pointer("/value/body_raw") {
                if let Some(output_id) = raw.get("output_id").and_then(Value::as_str) {
                    refs.push(format!("op-body:{output_id}"));
                }
            }
        }
        let deadline = tokio::time::Instant::now() + timeout;
        while refs.iter().any(|id| !self.transfers.is_ready(id)) {
            let message = tokio::time::timeout_at(deadline, self.mail.recv())
                .await
                .map_err(|_| TaskError::Closed("input transfer timeout".into()))?
                .ok_or_else(|| TaskError::Closed("mailbox closed".into()))?;
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
        // 物化为 JSON（预算内）。Bytes 编码引用不能进 JS 输入面。
        let mut decode = |stored: &StoredValue, id: &str| -> Result<Value, TaskError> {
            match stored {
                StoredValue::Inline(value) => Ok(value.clone()),
                StoredValue::Ref(reference) => {
                    let bytes = self
                        .transfers
                        .take(id)
                        .ok_or_else(|| TaskError::Invalid(format!("transfer {id} missing")))?;
                    let n = bytes.len();
                    if n > budget {
                        return Err(TaskError::Invalid("aggregate input exceeds 8 MiB".into()));
                    }
                    budget -= n;
                    match reference.codec {
                        ValueCodec::Json => Ok(serde_json::from_slice(&bytes)?),
                        ValueCodec::Utf8 => Ok(Value::String(
                            String::from_utf8(bytes)
                                .map_err(|e| TaskError::Invalid(e.to_string()))?,
                        )),
                        ValueCodec::Bytes => {
                            Err(TaskError::Invalid("binary value in JSON input face".into()))
                        }
                    }
                }
            }
        };
        let input = decode(&self.task.input, "in:run-input")?;
        let mut nodes = serde_json::Map::new();
        for (node_id, value) in &self.task.predecessors {
            nodes.insert(node_id.clone(), decode(value, &format!("in:{node_id}"))?);
        }
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
        let predecessors: BTreeMap<String, StoredValue> =
            self.task.predecessors.iter().cloned().collect();
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
                    flow_engine::expr::expand_templates_bounded(
                        &params,
                        &input,
                        &nodes,
                        Duration::from_secs(2),
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
        let params = store_value(audit, &self.journal_id, &params, 8 * 1024 * 1024).await?;
        let prepared = Prepared {
            input: self.task.input.clone(),
            predecessors,
            params,
            engine_build: crate::EXECUTOR_BUILD.into(),
            node_semantics: 1,
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
            kind @ (NodeType::HttpCall | NodeType::Llm | NodeType::Email) => {
                self.http_node(node, kind, prepared, inputs, audit).await
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
                Ok(ResultOutcome::Wait {
                    wait: WaitRequest {
                        kind: "delay".into(),
                        wake_at: Some(wake),
                        output: Some(StoredValue::Inline(json!({"slept_ms": ms}))),
                        workflow_id: None,
                        input_mapping: None,
                    },
                })
            }
            NodeType::HumanTask => Ok(ResultOutcome::Wait {
                wait: WaitRequest {
                    kind: "signal".into(),
                    wake_at: None,
                    output: None,
                    workflow_id: None,
                    input_mapping: None,
                },
            }),
            NodeType::SubWorkflow => {
                let params = materialize_prepared_params(self, prepared).await?;
                let workflow_id = params["workflow_id"]
                    .as_str()
                    .ok_or_else(|| TaskError::Business("child workflow required".into()))?
                    .to_owned();
                let input_mapping = params.get("input_mapping").cloned();
                Ok(ResultOutcome::Wait {
                    wait: WaitRequest {
                        kind: "child".into(),
                        wake_at: None,
                        output: None,
                        workflow_id: Some(workflow_id),
                        input_mapping,
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
                    let bytes = self.take_transfer(&format!("in:{key}")).await?;
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
            .materialize_stored(&prepared.params, "in:prepared-params", &mut budget)
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
            .materialize_stored(&prepared.input, "in:run-input", &mut budget)
            .await?;
        let mut nodes = serde_json::Map::new();
        for (node_id, value) in &prepared.predecessors {
            nodes.insert(
                node_id.clone(),
                self.materialize_stored(value, &format!("in:{node_id}"), &mut budget)
                    .await?,
            );
        }
        let logger = logger.clone();
        let result = tokio::task::spawn_blocking(move || {
            flow_engine::expr::eval_body_bounded(
                &code,
                &input,
                &Value::Object(nodes),
                timeout,
                &logger,
            )
        })
        .await
        .map_err(|e| TaskError::Business(e.to_string()))?
        .map_err(|e| TaskError::Business(e.to_string()))?;
        let branch = (kind == NodeType::Condition).then(|| flow_engine::exec::truthy(&result));
        let output = store_value(audit, &self.journal_id, &result, 8 * 1024 * 1024).await?;
        Ok(ResultOutcome::Success { output, branch })
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
            let message = tokio::time::timeout_at(deadline, self.mail.recv())
                .await
                .map_err(|_| TaskError::Closed("transfer wait timeout".into()))?
                .ok_or_else(|| TaskError::Closed("mailbox closed".into()))?;
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

    /// http/llm/email：请求构造→许可→调用→Outcome→输出（I07 执行器侧）。
    async fn http_node(
        &mut self,
        _node: &Node,
        kind: NodeType,
        prepared: &Prepared,
        _inputs: &TaskInputs,
        audit: &mut AuditStream,
    ) -> Result<ResultOutcome, TaskError> {
        // 恢复路径：已有 Outcome 直接派生输出，不重发请求。
        if let Some(outcome_value) = self.task.previous_outcome.clone() {
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
            .materialize_stored(&prepared.params, "in:prepared-params", &mut budget)
            .await?;
        if matches!(kind, NodeType::Llm | NodeType::Email) {
            let name = params["api_key"]
                .as_str()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| TaskError::Business("api_key secret reference required".into()))?
                .to_owned();
            let (url, body) = if kind == NodeType::Llm {
                flow_engine::exec::llm_request(&params)
            } else {
                flow_engine::exec::email_request(&params)
            }
            .map_err(|e| TaskError::Business(e.message))?;
            params = json!({
                "method": "POST",
                "url": url,
                "body": body,
                "timeout_ms": params["timeout_ms"],
                "credential": {"scheme": "bearer", "secret_ref": name},
            });
        }
        let method = params["method"].as_str().unwrap_or("GET").to_uppercase();
        if !HTTP_METHODS.contains(&method.as_str()) {
            return Err(TaskError::Invalid("invalid HTTP method".into()));
        }
        let url = params["url"]
            .as_str()
            .ok_or_else(|| TaskError::Business("HTTP URL missing".into()))?
            .to_owned();
        let request = json!({
            "method": method,
            "url": url,
            "headers": params.get("headers").cloned().unwrap_or(json!({})),
            "body": params.get("body").cloned().unwrap_or(Value::Null),
            "credential": params.get("credential").cloned().unwrap_or(Value::Null),
        });
        let request_bytes = flow_journal::codec::bounded_json(&request, INPUT_BUDGET)?;
        let fingerprint = flow_journal::codec::digest(&request_bytes);
        let operation_id = uuid::Uuid::now_v7().to_string();
        // 大请求走 data 分块；小请求 inline。
        let inline =
            if request_bytes.len() <= flow_engine::execution_protocol::CONTROL_MAX_FRAME / 2 {
                Some(StoredValue::Inline(request.clone()))
            } else {
                None
            };
        let transfer_id = if inline.is_none() {
            let id = format!("op:{operation_id}");
            let mut offset = 0u64;
            for chunk in request_bytes.chunks(flow_engine::execution_protocol::TRANSFER_CHUNK_BYTES)
            {
                self.outbound
                    .send(Message::TransferChunk {
                        dispatch_id: self.dispatch_id.clone(),
                        transfer_id: id.clone(),
                        offset,
                        bytes: base64::engine::general_purpose::STANDARD.encode(chunk),
                        digest: hex::encode(sha2::Sha256::digest(chunk)),
                    })
                    .await
                    .map_err(|e| TaskError::Closed(e.to_string()))?;
                offset += chunk.len() as u64;
            }
            self.outbound
                .send(Message::InputReady {
                    dispatch_id: self.dispatch_id.clone(),
                    transfer_id: id.clone(),
                    total_bytes: request_bytes.len() as u64,
                    digest: hex::encode(sha2::Sha256::digest(&request_bytes)),
                })
                .await
                .map_err(|e| TaskError::Closed(e.to_string()))?;
            Some(id)
        } else {
            None
        };
        self.outbound
            .send(Message::RequestOperation {
                dispatch_id: self.dispatch_id.clone(),
                command_id: format!("op:{operation_id}"),
                operation_id: operation_id.clone(),
                request_fingerprint: fingerprint.clone(),
                request: inline,
                transfer_id,
            })
            .await
            .map_err(|e| TaskError::Closed(e.to_string()))?;
        // 等待许可（取消/断线唤醒）。
        let credential = loop {
            let message = tokio::time::timeout(
                Duration::from_millis(DURABLE_WAIT_TIMEOUT_MS),
                self.mail.recv(),
            )
            .await
            .map_err(|_| TaskError::Closed("permit timeout".into()))?
            .ok_or_else(|| TaskError::Closed("mailbox closed".into()))?;
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
        // 执行外部调用（执行器进程内）。
        let http_method = reqwest::Method::from_bytes(method.as_bytes())
            .map_err(|e| TaskError::Invalid(e.to_string()))?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|e| TaskError::Invalid(e.to_string()))?;
        let mut request = client
            .request(
                http_method,
                reqwest::Url::parse(&url).map_err(|e| TaskError::Invalid(e.to_string()))?,
            )
            .timeout(Duration::from_millis(
                params["timeout_ms"].as_u64().unwrap_or(30_000),
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
        let outbound_request = request
            .build()
            .map_err(|e| TaskError::Invalid(e.to_string()))?;
        let mut response = match client.execute(outbound_request).await {
            Ok(response) => response,
            Err(error) => {
                // 授权后调用失败：Outcome 未知，交主进程按 uncertain 处理。
                return Err(TaskError::Uncertain(error.to_string()));
            }
        };
        let status = response.status().as_u16();
        let headers: serde_json::Map<String, Value> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), json!(v.to_str().unwrap_or(""))))
            .collect();
        let mut raw: Vec<u8> = Vec::new();
        while let Some(bytes) = response
            .chunk()
            .await
            .map_err(|e| TaskError::Uncertain(e.to_string()))?
        {
            if raw.len() as u64 + bytes.len() as u64 > flow_journal::MAX_VALUE_BYTES {
                return Err(TaskError::Http(
                    "captured value size; prefix retained".into(),
                ));
            }
            raw.extend_from_slice(&bytes);
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
            self.transfers
                .insert_ready(format!("op-body:{body_output_id}"), raw);
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

    /// 一期 `Attempt::http_output` 等价：状态/头/体组合或 llm/email 解析。
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
        if matches!(kind, NodeType::Llm | NodeType::Email) {
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
                .take_transfer(&format!("op-body:{}", raw.output_id))
                .await
                .ok();
            // 恢复路径：body 不在本进程时需要主进程送回；v1 由
            // previous_outcome 附带 raw bytes（见下）。
            let bytes = match bytes {
                Some(bytes) => bytes,
                None => return Err(TaskError::Invalid("outcome body bytes unavailable".into())),
            };
            let response: Value = serde_json::from_slice(&bytes)?;
            let output = if kind == NodeType::Llm {
                let content = response
                    .pointer("/choices/0/message/content")
                    .ok_or_else(|| {
                        TaskError::Business(
                            "LLM response missing choices[0].message.content; raw outcome retained"
                                .into(),
                        )
                    })?
                    .clone();
                json!({"content": content, "model": response["model"], "usage": response["usage"]})
            } else {
                json!({"status": status, "id": response["id"]})
            };
            return store_value(audit, &self.journal_id, &output, 8 * 1024 * 1024).await;
        }
        // HTTP 输出：body 为完整 JSON 时原样嵌入，否则转义为文本。
        let mut prefix = flow_journal::codec::bounded_json(
            &json!({"status": status, "headers": outcome_value["headers"]}),
            flow_journal::INLINE_BYTES,
        )?;
        prefix.pop();
        prefix.extend_from_slice(b",\"body\":");
        let body = self.raw_bytes(&raw).await?;
        let valid_json = {
            let mut de = serde_json::Deserializer::from_slice(&body);
            serde::de::IgnoredAny::deserialize(&mut de).is_ok() && de.end().is_ok()
        };
        let mut composed = prefix;
        if valid_json {
            composed.extend_from_slice(&body);
        } else {
            let text = serde_json::to_string(&String::from_utf8_lossy(&body).into_owned())?;
            composed.extend_from_slice(text.as_bytes());
        }
        composed.push(b'}');
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
        // previous_outcome 重传（transfer id 约定 op-body:<output_id>）。
        self.take_transfer(&format!("op-body:{}", reference.output_id))
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
        .materialize_stored(&prepared.params, "in:prepared-params", &mut budget)
        .await
}

#[allow(dead_code)]
fn unused() {}
