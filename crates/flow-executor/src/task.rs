//! 单次节点派发的子进程内执行（二期 §3.8，I04）。
//!
//! 一次 Execute 在同一子进程内完成准备与执行：模板、script、condition
//! 的全部用户 JS 都在本进程运行，主进程不再求值任何用户表达式。准备与
//! 执行共用一套 audit_seq、窗口、取消与结果协议，不为 prepare/execute
//! 分别派发。
//!
//! 本模块守住的边界：执行器只回答「这个节点做了什么、得到了什么」，
//! 不回答「这个节点该不该做」。授权（外部副作用）、提交（事实落盘）与
//! 恢复裁决都在主进程；执行器能给出的最强断言只有三种——成功、业务
//! 失败、外部结果未知（Uncertain），三者对主进程的后续处理完全不同，
//! 因此它们必须严格区分，不能互相冒充。
//!
//! 另一个核心不变量是「授权前不发请求」：外部副作用一旦获授权即视为
//! 可能已经发生，所以任何**发不出去**的请求（畸形 URL、非法 header、
//! 缺凭据）必须在授权之前失败，绝不能降级成 Uncertain 去人工裁决。

use std::collections::BTreeMap;
use std::sync::Arc;
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
///
/// 它同时是传输装配侧的硬上限（`transfer.rs` 按 `in:` 前缀强制执行）与
/// 本模块物化侧的累计预算：超出即判任务输入非法，而不是让单个节点把
/// 执行器内存吃光。
pub const INPUT_BUDGET: usize = 8 * 1024 * 1024;

/// 单次派发内可能出现的失败类型。
///
/// 分类本身就是契约：`Business` 走失败 Result（主进程按业务语义写
/// NodeFailed/重试），其余变体属于平台故障面，走 Reject/回收，绝不与
/// 业务失败互相冒充。
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    /// 业务失败（脚本/模板/HTTP 状态码/参数）：必须以失败 Result 回主进程，
    /// 由主进程按一期语义写 NodeFailed/重试，不走 Reject 回收。
    #[error("{0}")]
    Business(String),
    /// 审计流故障（窗口满、通道关闭、取消）：持久窗口无法推进，
    /// 本派发的审计前缀不再可信，只能整派失败回收。
    #[error("audit: {0}")]
    Audit(#[from] crate::audit::AuditError),
    /// 传输装配故障（摘要不符、缺口、超预算）：输入面不可信，不得继续执行。
    #[error("transfer: {0}")]
    Transfer(#[from] crate::transfer::TransferError),
    /// 值编码失败（超预算、深度超限）：产出面无法落账。
    #[error("journal encode: {0}")]
    Encode(#[from] flow_journal::Error),
    /// 派发内容本身不合法（身份缺失、节点类型未知、协议错位）。
    ///
    /// 属于主进程调度错误，不是用户工作流的失败面。
    #[error("task input invalid: {0}")]
    Invalid(String),
    /// 派发被取消：主进程已明确放弃这次执行。
    #[error("task cancelled")]
    Cancelled,
    /// 与主进程的通道关闭：失联证据，按回收处理。
    #[error("master channel closed: {0}")]
    Closed(String),
    /// 外部操作的结果未知：授权已发出但拿不到可验证 Outcome。
    ///
    /// 不得自动重试（副作用可能已发生），只能交主进程登记 uncertain
    /// 等待人工裁决。
    #[error("external outcome uncertain: {0}")]
    Uncertain(String),
    /// HTTP 传输层失败：请求已发出但响应不可得。
    #[error("http failure: {0}")]
    Http(String),
}

/// serde_json 解析失败归入输入非法（而非业务失败）：节点参数是主进程下发的
/// 结构化数据，解析不出来说明派发损坏，重试没有意义。
impl From<serde_json::Error> for TaskError {
    fn from(error: serde_json::Error) -> Self {
        TaskError::Invalid(error.to_string())
    }
}

/// reqwest 的 Display 只有顶层概述；代理 CONNECT、DNS、TLS 的原因在 source 链。
fn http_error_message(error: reqwest::Error) -> String {
    // URL 单独以脱敏后的标签记录，避免错误链重新带出 query 凭据。
    let error = error.without_url();
    let mut message = error.to_string();
    let mut source = std::error::Error::source(&error);
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    flow_engine::nodelog::redact_text(&message)
}

/// 任务结束方式（运行时据此决定是否释放/回收进程）。
#[derive(Debug)]
pub enum TaskEnd {
    /// ResultCommitted 已收到：槽位可复用。
    Committed,
    /// Agent hosts mutate process-wide state and can spawn tool descendants.
    /// Commit normally, then retire the entire managed executor session.
    CommittedRecycle,
    /// 取消/失败/通道断开：尽力封口后进程应退出，槽位整体回收。
    Recycle,
}

/// 运行一个派发：从 Accepted 到 ResultCommitted（或取消退出）。
///
/// 持有一个派发所需的全部上下文：任务载荷、进出站通道、取消令牌、审计
/// 窗口额度、输入装配器。生命周期 = 一次派发，结束后随对象释放。
pub struct TaskRunner {
    /// 主进程给本次派发的命令身份（回执按它对账）。
    pub command_id: String,
    /// 本次派发的唯一标识：所有进出站帧都按它归属。
    pub dispatch_id: String,
    /// 派发载荷（节点定义、输入声明、准备元数据、恢复用 outcome）。
    pub task: ExecuteTask,
    /// 出站通道：执行器→主进程。
    pub outbound: mpsc::Sender<Message>,
    /// 入站邮箱：主进程→本派发（传输块、许可、ACK、取消）。
    pub mail: mpsc::Receiver<Message>,
    /// 审计持久确认的最新位置（由运行时转发主进程的 AuditAck）。
    pub ack_tx: watch::Sender<(u64, u64)>,
    /// 取消令牌：主进程取消或看门狗到期时唤醒所有等待点。
    pub cancel: CancellationToken,
    /// 审计记录归属的 journal（由握手身份固定，决定值写到哪个库）。
    pub journal_id: String,
    /// 持久窗口额度 W：待发送与未确认记录共用的字节预算。
    pub window_bytes: u64,
    /// 是否需要应用层重放（远端 agent 上联形态）。
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
///
/// 与 `TaskInputs` 的区别：这里保留 `Ref` 身份（输出回引要用同一个
/// output_id），而 `TaskInputs` 是已解码的 JSON 视图（给 JS 用）。
pub struct InputFace {
    /// run 输入面（StoredValue 形态）。
    pub input: StoredValue,
    /// 前驱节点输出面（按节点 id）。
    pub predecessors: BTreeMap<String, StoredValue>,
    /// 已提交的准备参数（resume 时由主进程重传）。
    pub prepared_params: Option<StoredValue>,
}

impl TaskRunner {
    /// 等待一条入站消息，直到 `deadline`。
    ///
    /// 超时只在远端重放形态生效：本地形态的等待由主进程的取消驱动
    /// （主进程挂掉会关通道，邮箱随即 EOF），远端上联的链路可能既不
    /// 断也不给回应，需要自设上限避免槽位被永久占住。
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
    /// 派发主流程：执行并在结束后决定进程去留。
    ///
    /// 返回 `TaskEnd` 而非自行退出，是因为「能否复用本执行器」是会话
    /// 层的判断：只有拿到 ResultCommitted 才说明主进程已持久提交，
    /// 槽位才可安全复用；其余情况一律整体回收。
    pub async fn run(mut self) -> TaskEnd {
        // agent 模式会改写进程级状态并派生工具后代，提交后必须整体退役。
        let agent = self.node_is_agent_mode();
        let result = self.execute().await;
        match result {
            Ok(()) if agent => TaskEnd::CommittedRecycle,
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

    /// 执行一次派发：接受 → 装配输入 → 准备 → 按类型执行 → 发 Result。
    ///
    /// 顺序敏感：Accepted 必须先发（主进程据此确认派发被接手），输入
    /// 装配必须早于准备（模板展开要用到输入面），准备产出的 InputPrepared
    /// 必须先持久（§3.8：已被 Execute 授权的纯计算要等它落账后才继续）。
    ///
    /// # Errors
    /// 返回平台故障类错误（通道关闭、取消、审计/传输不可信），调用方
    /// 据此走 Reject 回收；业务失败在内部转成失败 Result，不外泄。
    async fn execute(&mut self) -> Result<(), TaskError> {
        // 运行时身份门禁：派发里的 _flow_execution.runtime 与本执行器不符，
        // 说明派发是给别的语义版本准备的。宁可不执行，也不能用错的语义
        // 解释同一份节点定义。
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

    /// 以失败 Outcome 收口本次派发。
    ///
    /// `uncertain` 只在外部结果未知时为真：主进程据此登记「等人工
    /// 裁决」，而不是按普通失败重试（重试可能重复副作用）。
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

    /// 发出最终 Result：先确保审计前缀已持久，再发 Result 并等提交回执。
    ///
    /// 「Result 可引用审计流末尾的值」意味着 Result 里的值引用必须落在
    /// 已持久的前缀内，所以必须等 `last_audit_seq` 被确认后才发——顺序
    /// 反了会得到悬空引用。
    ///
    /// 远端重放形态下还要重发同一 `result_id` 的 Result（主进程幂等返回
    /// 原提交），否则回执丢失会留下一个永远等不到确认的派发。
    ///
    /// # Errors
    /// 通道关闭或等待超时（远端形态）时返回 `Closed`/`Cancelled`。
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
            // 远端形态：等待确认自带上限，避免链路半死时无限占住槽位。
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

    /// 等待主进程确认 Result 已提交。
    ///
    /// 只认「同一 dispatch_id + 同一 result_id」的 ResultCommitted：串台
    /// 的回执等于没提交。取消先到时立即放弃等待并进入回收（主进程已
    /// 放弃这次执行，再等也没有意义）。
    ///
    /// # Errors
    /// 邮箱关闭、取消先到，或收到协议错位的消息时返回错误。
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
        // 只等「声明里要用的」那几个 transfer：提前到齐不代表可以开工，
        // 必须等齐所有必需值才进准备阶段。
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
        //
        // 闭包持有 &mut self 是刻意的：take 会从装配器里取走已装配的
        // 字节（此后不可再次 take），顺序敏感的取用必须串行化。
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
                            String::from_utf8(bytes.to_vec())
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
    ///
    /// 模板展开在 `spawn_blocking` 里跑：用户表达式可能长时间占用 CPU，
    /// 放回主执行线程会阻塞心跳与通道收发，心跳一停主进程即判失联。
    ///
    /// # Errors
    /// 模板展开失败、参数编码超限时返回 `Invalid`。
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
        // 含 "${" 才展开：无模板时直接提交实际输入，避免一次无谓的
        // 表达式求值（求值会改变用户可见的确定性上下文开销）。
        let templates = serde_json::to_string(&node.params)?.contains("${");
        let params = if !templates {
            node.params.clone()
        } else {
            let mut params = node.params.clone();
            let mut opaque = Vec::new();
            // opaque 参数（如密钥名）不能进模板展开：展开会把字面量替换
            // 成值，密钥引用必须在展开后原样回填。
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
            engine_build: flow_engine::BUILD_ID.into(),
            node_semantics: if determinism.is_some() { 2 } else { 1 },
        };
        audit.push(
            "input_prepared",
            json!({"prepared": serde_json::to_value(&prepared)?}),
        )?;
        Ok(prepared)
    }

    /// 节点执行：与一期 `journal_driver::execute` 逐分支对齐。
    ///
    /// 每个分支只负责「算出什么」，不负责「谁来提交」：等待类节点返回
    /// `Wait`（由主进程挂起 run），业务失败以 `Err(TaskError::Business)`
    /// 冒泡，外部结果未知以 `Err(TaskError::Uncertain)` 冒泡——三类结果
    /// 在主进程的处理路径完全不同。
    ///
    /// # Errors
    /// 见上；另含参数缺失等确定性业务失败。
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
                // ms 允许字符串形态：模板展开后常得到字符串，配置期校验
                // 只覆盖字面量。
                let ms = params["ms"]
                    .as_u64()
                    .or_else(|| params["ms"].as_str().and_then(|s| s.parse().ok()))
                    .ok_or_else(|| TaskError::Business("delay ms required".into()))?;
                // 唤醒时刻用绝对毫秒：跨进程/跨重启后仍可比，时区只影响
                // 展示（本地时区字符串），不影响语义。
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
                    // 缺 key 的字段无法交付，静默跳过（不算「待填写」）：
                    // 否则一个坏字段会让节点永远挂起。
                    let Some(key) = field.get("key").and_then(Value::as_str) else {
                        continue;
                    };
                    // null 视为未提供：JSON 里 null 与「没这个键」语义相同。
                    let value = run_input
                        .and_then(|o| o.get(key))
                        .filter(|v| !v.is_null())
                        .or_else(|| field.get("default"));
                    match value {
                        Some(v) => {
                            resolved.insert(key.to_string(), v.clone());
                        }
                        // 缺字段只记「待填写」，不提前返回：其余字段能解析
                        // 的先解析，交付对象才是完整的。
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
        // 按键排序保证组合结果的字节稳定：journal 里的输出字节必须与
        // 一期进程内路径逐字节一致，顺序不能取决于哈希迭代序。
        values.sort_by(|a, b| a.0.cmp(&b.0));
        if values.len() == 1 {
            return Ok(values.pop().unwrap().1);
        }
        if values.is_empty() {
            return Ok(StoredValue::Inline(Value::Null));
        }
        // 手工拼 JSON：需要把 Ref 的原始字节原样嵌入（结构字节由
        // flow_journal::value::ObjectWriter 的 serde_json Formatter 拥有，
        // 这里只提供完整值）。
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
    ///
    /// 用户代码在 `spawn_blocking` 里求值并受 `timeout_ms` 约束（上限
    /// 30s，防止一次节点永久占住执行槽）。
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
            // 条件节点复用同一套脚本求值器：只多包一层 return，让两条
            // 路径共用超时、确定性上下文与日志。
            NodeType::Condition => format!(
                "return ({});",
                params_value["expr"].as_str().unwrap_or("false")
            ),
            _ => {
                return Err(TaskError::Invalid(format!(
                    "compute dispatched for unsupported node kind: {kind:?}"
                )))
            }
        };
        let _ = inputs;
        // 超时上限 30s：再长的时间会让一个失控脚本长期霸占执行槽，
        // 而执行槽是全局有限资源。
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
        // 闭包 move 进阻塞线程：求值期间不持有 &mut self（否则任务无法
        // 响应取消），日志与确定性上下文都在闭包内自带。
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
        // 条件节点的分支由宿主的 truthy 判定（与 JS 真值语义对齐），
        // 脚本节点不带分支。
        let branch = (kind == NodeType::Condition).then(|| flow_engine::exec::truthy(&result));
        let output = store_value(audit, &self.journal_id, &result, 8 * 1024 * 1024).await?;
        Ok(ResultOutcome::Success { output, branch })
    }

    /// 取出本派发的确定性上下文（时钟与随机种子）。
    ///
    /// 缺失即 `None`：调用方按「无固定上下文」求值。这不是错误，因为
    /// 旧运行快照可能没有该字段。
    fn determinism(&self) -> Option<flow_engine::expr::JsDeterminism> {
        let context = self.task.node.get("_flow_execution")?;
        // 种子先摘要再取前 8 位十六进制：把任意长度字符串压成 u32，
        // 且分布均匀（直接截断字符串前缀会偏向短串）。
        let digest = flow_journal::codec::digest(context["seed"].as_str()?.as_bytes());
        Some(flow_engine::expr::JsDeterminism {
            clock_ms: context["clock_ms"].as_i64()?,
            seed: u32::from_str_radix(&digest[..8], 16).ok()?,
        })
    }

    /// 取一个 transfer 的装配字节，必要时等主进程送来。
    async fn take_transfer(&mut self, transfer_id: &str) -> Result<Arc<Vec<u8>>, TaskError> {
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

    /// 把 StoredValue 物化成 JSON 值，并计入本次物化的字节预算。
    ///
    /// 预算跨多次调用累计：一次节点的输入面总量有上限，避免多个大值
    /// 各自合法但合计超限。
    ///
    /// # Errors
    /// transfer 缺失、字节超预算或编码非法时返回 `Invalid`。
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
                        String::from_utf8(bytes.to_vec())
                            .map_err(|e| TaskError::Invalid(e.to_string()))?,
                    )),
                    ValueCodec::Bytes => {
                        Err(TaskError::Invalid("binary value in JSON input face".into()))
                    }
                }
            }
        }
    }

    /// http/email：请求构造→许可→调用→Outcome→输出（I07 执行器侧）。
    ///
    /// 外部副作用的三段式：先构造请求（不发），再取授权许可（主进程落
    /// OperationIntent/Authorized 的同一事务），最后才真正发出。恢复
    /// 路径（已有捕获的 outcome）不重复发请求。
    ///
    /// # Errors
    /// 业务失败（缺参数、状态码非 2xx）以 `Business` 冒泡；授权后拿不
    /// 到可验证响应以 `Uncertain` 冒泡（副作用可能已发生）。
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
            // 收件人列表压平成一行，仅用于日志展示；实际请求体由
            // email_request 构造。
            let recipients = match &params["to"] {
                Value::Array(list) => list
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", "),
                other => other.as_str().unwrap_or("").to_owned(),
            };
            logger.info(format!("→ email to {recipients}"));
            // 这里取的是密钥**名称**（secret_ref），不是密钥值：值要等
            // 授权许可回传，密钥材料绝不进节点参数与审计。
            let name = params["api_key"]
                .as_str()
                .filter(|s| !s.trim().is_empty())
                .ok_or_else(|| TaskError::Business("api_key secret reference required".into()))?
                .to_owned();
            let (url, body) = flow_engine::exec::email_request(&params)
                .map_err(|e| TaskError::Business(e.message))?;
            // 改写成统一形态的 HTTP 请求：邮件服务只是「一个 HTTP 端点 +
            // bearer 凭据」，复用同一条授权与 Outcome 路径。
            params = json!({
                "method": "POST",
                "url": url,
                "body": body,
                "timeout_ms": params["timeout_ms"],
                "credential": {"scheme": "bearer", "secret_ref": name},
            });
        }
        if kind == NodeType::Llm {
            // 原生模型节点固定 POST（请求体是供应商 JSON）。
            params["method"] = json!("POST");
        }
        let method = params["method"].as_str().unwrap_or("GET").to_uppercase();
        // 方法白名单：不在清单内即拒绝，不做「猜测意图」——发出去的
        // 方法一旦猜错，副作用后果无法撤回。
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
            // 回归模式：零外部调用，只放行与录制基线逐字节一致的请求。
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
            // 不跟随重定向、不自动重试：两者都会在授权之外多发请求，
            // 让「授权=一次副作用」的记账失真。
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
        // 带凭据重建：密钥值只在授权许可里出现，不进请求指纹与审计。
        let outbound_request = build_outbound(credential)?;
        // 展示面用的 URL（日志行）：query 里的敏感参数值脱敏，发起请求用的
        // 永远是原始 `url`。
        let url_label = flow_engine::redact_url(&url);
        logger.info(format!("→ {method} {url_label}"));
        let started = std::time::Instant::now();
        let mut response = match client.execute(outbound_request).await {
            Ok(response) => response,
            Err(error) => {
                let error = http_error_message(error);
                logger.error(format!(
                    "请求 {url_label} 失败（{}ms）：{error}",
                    started.elapsed().as_millis()
                ));
                // 授权后调用失败：Outcome 未知，交主进程按 uncertain 处理。
                return Err(TaskError::Uncertain(error));
            }
        };
        if response
            .content_length()
            .is_some_and(|length| length > flow_journal::MAX_VALUE_BYTES)
        {
            // 响应超捕获上限：完整 outcome 留不下来，等价于结果未知。
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
        // 流式读取并累计限额：Content-Length 缺失或撒谎时，只有边读
        // 边判才能拦住超限响应。
        while let Some(bytes) = response
            .chunk()
            .await
            .map_err(|e| TaskError::Uncertain(http_error_message(e)))?
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
            // 原始体登记进装配器：派生输出时按 output_id 取回，避免再读
            // 一次 journal（恢复路径由主进程重传同一 output_id）。
            self.transfers.insert_ready(
                TransferKey::OperationBody(body_output_id.clone()).to_wire(),
                raw,
            );
        }
        let outcome = StoredValue::inline(
            json!({"status": status, "headers": headers, "body_raw": body_ref}),
        )?;
        // Outcome 必须先落账（持久）再派生输出：它是「请求确实发生过且
        // 结果是什么」的权威证据，恢复路径完全依赖它。
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
    ///
    /// 请求字节走 data 通道分块发送（再小也走），控制帧只带身份与指纹：
    /// 控制帧有确定性上界（C6），值字节不能借道它。
    ///
    /// 许可回验三重身份：dispatch_id、operation_id 与请求指纹必须全部
    /// 匹配，否则拒绝——指纹不匹配意味着许可对应的不是这次请求。
    ///
    /// # Errors
    /// 身份错配返回 `Invalid`；取消返回 `Cancelled`；通道关闭返回 `Closed`。
    async fn authorize_operation(&mut self, request: Value) -> Result<Option<String>, TaskError> {
        if self.is_regression() {
            // 回归模式禁止一切外部操作：协调器也会独立拒绝许可，双保险
            // 防止回归意外发请求。
            return Err(TaskError::Business(
                "external operations are disabled in regression".into(),
            ));
        }
        let request_bytes = flow_journal::codec::bounded_json(&request, INPUT_BUDGET)?;
        // 指纹 = 请求字节摘要：授权与执行两侧对同一摘要，许可才对得上
        // 这一次具体的请求。
        let fingerprint = flow_journal::codec::digest(&request_bytes);
        let operation_id = uuid::Uuid::now_v7().to_string();
        // v2：请求字节一律走 data 通道（再小也走），控制帧只带身份与指纹。
        let transfer_id = TransferKey::OperationRequest(operation_id.clone()).to_wire();
        {
            let mut offset = 0u64;
            // 分块发送并逐块带摘要：接收端据此发现传输损坏。
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
            // InputReady 声明整值长度与摘要：接收端据此判断装配完成。
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
                    // 三重身份校验：串台的许可必须被拒，否则会对错误的
                    // 请求执行副作用。
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
                    // 许可等待期间也要推进审计确认，否则审计窗口会被
                    // 自己产生的记录堵死。
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
    ///
    /// # Errors
    /// 见上；另含参数缺失（`Business`）与平台故障（`Uncertain`）。
    async fn harness_node(
        &mut self,
        _node: &Node,
        prepared: &Prepared,
        _inputs: &TaskInputs,
        audit: &mut AuditStream,
        logger: &flow_engine::NodeLogger,
    ) -> Result<ResultOutcome, TaskError> {
        // 恢复路径：已有 Outcome 直接派生输出，不重跑进程。两种模式的
        // outcome 形状不同（agent 带 agent_error/agent 元数据），按节点
        // mode 选派生函数——mode 是保存期校验过的字面量。
        if let Some(outcome_value) = self
            .task
            .previous_outcome
            .clone()
            .filter(|_| !self.is_regression())
        {
            logger.info("已有捕获的 operation outcome，不重复启动进程（恢复）");
            let outcome: StoredValue =
                serde_json::from_value(outcome_value).map_err(TaskError::from)?;
            let output = if self.node_is_agent_mode() {
                self.agent_output_from_outcome(&outcome, audit).await?
            } else {
                self.harness_output(&outcome, audit).await?
            };
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
        if self.node_is_agent_mode() {
            return self.agent_node(&params, audit, logger).await;
        }
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
        // ManagedChild 绑定进程组/作业：执行器崩溃或取消时后代进程不会
        // 变成孤儿继续占用机器。
        let mut child = match flow_engine::process::ManagedChild::spawn_harness(
            &mut flow_engine::exec::harness_command(&command, &args, workdir.as_deref()),
        ) {
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

    /// 是否为回归模式（派发带 `_flow_execution.regression`）。
    ///
    /// 回归模式下一律零外部调用：结果只能来自录制基线。
    fn is_regression(&self) -> bool {
        self.task.node.pointer("/_flow_execution/regression") == Some(&Value::Bool(true))
    }

    /// mode 是否为 agent。`mode` 是保存期校验过的字面量（禁止模板），
    /// 原始节点参数即可判定，不需要等模板展开。
    fn node_is_agent_mode(&self) -> bool {
        self.task.node["type"] == "harness"
            && self
                .task
                .node
                .get("params")
                .and_then(|params| params.get("mode"))
                .and_then(Value::as_str)
                .is_some_and(|mode| mode.trim() == "agent")
    }

    /// 内联 harness agent。沿用统一授权、Outcome 与回归协议；本次任务
    /// 完成后回收整个 executor，不复用 host 的全局状态或工具后代。
    ///
    /// agent 是**一次性执行器**：它会改写进程级环境并派生工具后代，宿主
    /// 状态不得流向下一个任务。
    ///
    /// # Errors
    /// 配置缺失、凭据缺失等确定性失败以 `Business` 收口；超时或取消
    /// 以 `Uncertain` 收口（工具副作用可能已发生）。
    async fn agent_node(
        &mut self,
        params: &Value,
        audit: &mut AuditStream,
        logger: &flow_engine::NodeLogger,
    ) -> Result<ResultOutcome, TaskError> {
        let agent_params =
            flow_engine::exec::agent_params(params).map_err(|e| TaskError::Business(e.message))?;
        let timeout_ms = agent_params
            .timeout_ms
            .clamp(1, flow_engine::exec::HARNESS_TIMEOUT_MAX_MS);
        // 配置缺失/畸形发生在授权之前：请求指纹必须包含冻结配置，没有它
        // 就无法授权，也就没有可落账的 operation——直接 Business。
        let config = flow_engine::exec::frozen_agent_config(&self.task.node)
            .map_err(TaskError::Invalid)?
            .ok_or_else(|| {
                TaskError::Business(
                    "harness agent 配置未随派发下发（[harness.agent] 未配置，或 master \
                     版本过旧）；无法构造授权请求"
                        .into(),
                )
            })?;
        let request = flow_engine::exec::agent_request(
            &flow_engine::exec::AgentParams {
                prompt: agent_params.prompt.clone(),
                workdir: agent_params.workdir.clone(),
                api_key: agent_params.api_key.clone(),
                timeout_ms,
            },
            &config,
        );
        if self.is_regression() {
            // 0 runner / 0 网络：指纹比较用 baseline 冻结的配置构造。
            let outcome = self.fixture_outcome(&request)?;
            let output = self.agent_output_from_outcome(&outcome, audit).await?;
            return Ok(ResultOutcome::Success {
                output,
                branch: None,
            });
        }
        // workdir 运行期校验（授权前，仅对显式配置的路径）：保存期只查了
        // 字面量；存在性与 canonicalize 是本机事实，remote 模式下只有
        // executor 看得到。缺省（未配置）走 roots 下的临时沙箱，授权后创建。
        let configured_workdir = match &agent_params.workdir {
            Some(configured) => {
                let canonical = std::path::Path::new(configured)
                    .canonicalize()
                    .map_err(|e| {
                        TaskError::Business(format!(
                            "harness agent workdir 不可用（{configured}）：{e}；请清空 workdir 使用默认临时工作目录，或填写执行器所在机器上已存在的绝对目录，发布后启动新运行"
                        ))
                    })?;
                if !canonical.is_dir() {
                    return Err(TaskError::Business(format!(
                        "harness agent workdir 不是目录：{}",
                        canonical.display()
                    )));
                }
                Some(canonical)
            }
            None => None,
        };
        let credential = self.authorize_operation(request).await?;
        // 生命周期截止时间从授权后起算：覆盖装配、回合、清理与落账，
        // 避免任一阶段无限延长节点占用。
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        // 看门狗独立于 Tokio：宿主同步代码堵死运行时时，协作式超时与
        // 心跳都会失效，只有独立线程能保证执行器最终退出。
        //
        // Independent of Tokio progress and heartbeats. If synchronous host code
        // blocks the runtime, the managed process still exits within a bounded grace.
        let _watchdog = flow_engine::process::AgentWatchdog::start(
            Duration::from_millis(timeout_ms),
            self.cancel.clone(),
        )
        .map_err(|error| TaskError::Uncertain(format!("agent watchdog unavailable: {error}")))?;
        // 沙箱根：flow 主目录下 `harness/flow-harness-<uuid>`（FLOW_HOME →
        // ~/.flow；与配置/journal 同住，不散落系统 temp）。home 不可用的
        // 退化环境回退系统 temp。运行结束整个 roots 被收走（见下）。
        let harness_root = flow_config::flow_home().unwrap_or_else(std::env::temp_dir);
        let roots = harness_root
            .join("harness")
            .join(format!("flow-harness-{}", uuid::Uuid::now_v7().simple()));
        // prompt/密钥不进日志：只记模型、工作目录与超时（缺省时目录是临时
        // 沙箱，标注 default 以示来源）。
        let default_workdir = configured_workdir.is_none();
        let workdir = match configured_workdir {
            Some(canonical) => canonical,
            None => {
                let scratch = roots.join("workdir");
                std::fs::create_dir_all(&scratch).map_err(|e| {
                    TaskError::Business(format!("harness agent 临时沙箱创建失败：{e}"))
                })?;
                scratch.canonicalize().map_err(|e| {
                    TaskError::Business(format!("harness agent 临时沙箱不可用：{e}"))
                })?
            }
        };
        logger.info(format!(
            "→ harness agent（model {}，workdir {}{}，timeout {timeout_ms}ms）",
            config.model,
            workdir.display(),
            if default_workdir {
                "（默认临时沙箱）"
            } else {
                ""
            }
        ));
        let Some(api_key) = credential.filter(|key| !key.trim().is_empty()) else {
            // 授权成功但凭据缺失：确定性失败，落 outcome B 让恢复派生稳定。
            let outcome = StoredValue::inline(json!({
                "agent_error": format!(
                    "密钥 {} 未配置或为空（设置页密钥管理；环境兜底 FLOW_SECRET_*）",
                    agent_params.api_key
                ),
            }))?;
            return self
                .commit_agent_failure(
                    outcome,
                    audit,
                    format!("harness agent credential missing: {}", agent_params.api_key),
                )
                .await;
        };
        let cancel = self.cancel.clone();
        let result = tokio::select! {
            biased;
            // biased：取消优先于回合完成。回合刚完成与取消同时就绪时，
            // 选取消会让已完成的工作被判 Uncertain——但工具副作用确实
            // 已经发生，保守方向是对的。
            _ = cancel.cancelled() => Err(TaskError::Uncertain("harness agent cancelled; result incomplete".into())),
            result = tokio::time::timeout_at(deadline, self.agent_run(
                &config,
                &agent_params.prompt,
                &api_key,
                &workdir,
                &roots,
                audit,
                logger,
            )) => result.unwrap_or_else(|_| Err(TaskError::Uncertain(
                "harness agent lifecycle deadline exceeded; result incomplete".into(),
            ))),
        };
        // 成功/失败/超时/取消都收走临时根；Windows 上文件锁可能让删除失败，
        // 重试一次，仍失败只能残留（在 flow 主目录 harness/ 下，可人工清理）。
        if std::fs::remove_dir_all(&roots).is_err() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = std::fs::remove_dir_all(&roots);
        }
        result
    }

    /// 授权后的内联执行段：直接调用 flow-harness-agent-host 的 headless 装配（无
    /// runner 子进程、无 stdin/stdout 协议）。确定性失败（临时根不可建、
    /// 装配失败）落封闭 outcome；调用方统一约束总截止时间与取消。
    /// 超时/取消传播 Uncertain，随后回收整个执行器会话。
    ///
    /// # Errors
    /// 装配失败以 `Business`（经 `commit_agent_failure` 落 outcome）收口；
    /// 回合失败以 `Uncertain` 收口。
    #[allow(clippy::too_many_arguments)]
    async fn agent_run(
        &mut self,
        config: &flow_engine::AgentRunConfig,
        prompt: &str,
        api_key: &str,
        workdir: &std::path::Path,
        roots: &std::path::Path,
        audit: &mut AuditStream,
        logger: &flow_engine::NodeLogger,
    ) -> Result<ResultOutcome, TaskError> {
        std::fs::create_dir_all(roots.join("tmp"))
            .map_err(|e| TaskError::Business(format!("harness agent 临时目录创建失败：{e}")))?;
        let started = std::time::Instant::now();
        // typed 配置直入（不经 CONGA_* env）；key 走参数。装配失败（代理
        // 非法/规格空）是确定性失败 → outcome B。
        let spec = flow_harness_agent_host::headless::HeadlessSpec {
            base_url: config.base_url.clone(),
            model: config.model.clone(),
            api: config.api.clone(),
            proxy: config.proxy.clone(),
            max_turns: config.max_turns.map(|v| v as usize),
            max_tool_calls_per_turn: config.max_tool_calls_per_turn.map(|v| v as usize),
            max_tokens: config.max_tokens.map(|v| v as usize),
            tool_timeout_secs: config.tool_timeout_secs,
        };
        let session = match flow_harness_agent_host::headless::build_headless_with(
            api_key,
            &spec,
            roots,
            workdir,
            flow_harness_agent_host::permission::Mode::FullAuto,
        )
        .await
        {
            Ok(session) => session,
            Err(error) => {
                let outcome = StoredValue::inline(json!({ "agent_error": error.to_string() }))?;
                return self
                    .commit_agent_failure(
                        outcome,
                        audit,
                        format!("harness agent assembly failed: {error}"),
                    )
                    .await;
            }
        };
        let session_id = session.host.session().current_id();
        // The caller bounds assembly, the turn, cleanup and outcome publication
        // with one deadline. Dropping this future is always followed by retiring
        // the entire executor session, including tool process groups.
        //
        // 进度投影只写入有界观测通道：不记录 prompt、模型正文或工具参数
        // （详见 `agent_progress.rs`）。
        let mut progress = crate::agent_progress::AgentProgress::new(logger);
        let turn = session
            .host
            .run_turn(prompt, |event| progress.on_event(event))
            .await;
        // 清理宿主会话资源：不做会让下一次任务继承上个会话的状态。
        flow_harness_agent_host::session_cleanup::cleanup_session_resources(&session_id).await;
        let summary = turn
            .map_err(|error| TaskError::Uncertain(format!("harness agent turn failed: {error}")))?;
        let parsed = crate::agent_summary::summarize(&summary);
        logger.info(format!(
            "← agent {}（{}ms，{} turns）",
            if parsed.exit_code == 0 {
                "完成"
            } else {
                "失败"
            },
            started.elapsed().as_millis(),
            progress.rounds()
        ));
        // error 行同时进 stderr（A6）：Business 文案不只剩退出码。
        let mut stderr = String::new();
        if let Some(error) = &parsed.error {
            stderr.push_str(&format!("[agent] {error}"));
        }
        let raw_stored = store_bytes(
            audit,
            &self.journal_id,
            parsed.text.as_bytes(),
            ValueCodec::Bytes,
        )
        .await?;
        let body_ref = match &raw_stored {
            StoredValue::Ref(reference) => serde_json::to_value(reference)?,
            StoredValue::Inline(value) => value.clone(),
        };
        if let StoredValue::Ref(reference) = &raw_stored {
            if !reference.output_id.is_empty() {
                self.transfers.insert_ready(
                    TransferKey::OperationBody(reference.output_id.clone()).to_wire(),
                    parsed.text.as_bytes().to_vec(),
                );
            }
        }
        let agent_meta = json!({
            "model": config.model,
            "stop_reason": parsed.stop_reason,
            "elapsed_ms": started.elapsed().as_millis() as u64,
        });
        let outcome = StoredValue::inline(json!({
            "exit_code": parsed.exit_code,
            "body_raw": body_ref,
            "stderr": stderr,
            "agent": agent_meta,
        }))?;
        let outcome_seq = audit.push(
            "operation_outcome",
            json!({"outcome": serde_json::to_value(&outcome)?}),
        )?;
        audit.await_durable(outcome_seq, &self.cancel).await?;
        let output = self.agent_output_from_outcome(&outcome, audit).await?;
        Ok(ResultOutcome::Success {
            output,
            branch: None,
        })
    }

    /// agent 模式的 Outcome → 节点输出（恢复与首跑共用同一派生）。
    /// 封闭形状：`spawn_error` / `agent_error` / 正常三选一。
    ///
    /// # Errors
    /// `spawn_error`/`agent_error` 归为 `Business`；原始文本超限归为
    /// `Business`（outcome 已留存）。
    async fn agent_output_from_outcome(
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
                "harness agent spawn failed: {error}"
            )));
        }
        if let Some(error) = outcome_value["agent_error"].as_str() {
            return Err(TaskError::Business(format!("harness agent error: {error}")));
        }
        let exit_code = outcome_value["exit_code"].as_i64().map(|code| code as i32);
        let stderr = outcome_value["stderr"].as_str().unwrap_or("").to_owned();
        let agent = outcome_value.get("agent").cloned().unwrap_or(Value::Null);
        let raw: flow_journal::ValueRef =
            serde_json::from_value(outcome_value["body_raw"].clone()).map_err(TaskError::from)?;
        if raw.total_bytes > 8 * 1024 * 1024 {
            return Err(TaskError::Business(
                "agent text exceeds 8 MiB; raw outcome retained".into(),
            ));
        }
        let stdout = self.raw_bytes(&raw).await?;
        let output = flow_engine::exec::agent_output(
            exit_code,
            String::from_utf8_lossy(&stdout).into_owned(),
            stderr,
            agent,
        )
        .map_err(|e| TaskError::Business(format!("{}; outcome retained", e.message)))?;
        store_value(audit, &self.journal_id, &output, 8 * 1024 * 1024).await
    }

    /// 落一个封闭失败 outcome（B/C 形状）并以 Business 收口；恢复路径经
    /// [`Self::agent_output_from_outcome`] 重放同一失败文案。
    ///
    /// 先落 outcome 再报错：恢复期才能重放出同样的失败，而不是变成
    /// 一次「结果未知」的人工裁决。
    async fn commit_agent_failure(
        &mut self,
        outcome: StoredValue,
        audit: &mut AuditStream,
        message: String,
    ) -> Result<ResultOutcome, TaskError> {
        let outcome_seq = audit.push(
            "operation_outcome",
            json!({"outcome": serde_json::to_value(&outcome)?}),
        )?;
        audit.await_durable(outcome_seq, &self.cancel).await?;
        Err(TaskError::Business(message))
    }

    /// 回归模式：比对请求指纹并放行录制的 outcome，绝不发起外部调用。
    ///
    /// # Errors
    /// 缺 fixture 或指纹不匹配时返回 `Business`——请求与基线不同就
    /// 不能复用基线结果。
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
    ///
    /// # Errors
    /// 非 0 退出码、文本超限归为 `Business`（outcome 已留存）。
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
    ///
    /// # Errors
    /// outcome 形状非法归为 `Invalid`；状态码非 2xx、响应体非 JSON
    /// （email/llm）、文本超限归为 `Business`（outcome 已留存）。
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
            // 用 Deserializer 判「整个 body 是且仅是一个 JSON 值」：
            // serde_json::from_slice 会拒绝尾随内容，这里要的是同样的严格性。
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
                // JSON 体原样嵌入（保留供应商结构）；非 JSON 转义为文本。
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
            // 4xx/5xx 是业务失败而非平台故障：outcome 已落账，可按失败
            // 重试策略处理，不需要人工裁决。
            return Err(TaskError::Business(format!(
                "HTTP {status}; outcome retained"
            )));
        }
        Ok(output)
    }

    /// 取回已捕获的原始字节（按 output_id 定位）。
    ///
    /// # Errors
    /// 字节不在本进程且主进程未重传时返回 `Closed`（等待超时）。
    async fn raw_bytes(
        &mut self,
        reference: &flow_journal::ValueRef,
    ) -> Result<Arc<Vec<u8>>, TaskError> {
        // 当前进程刚捕获的响应：从内存取；恢复路径由主进程随
        // previous_outcome 重传（transfer id 约定见 TransferKey::OperationBody）。
        self.take_transfer(&TransferKey::OperationBody(reference.output_id.clone()).to_wire())
            .await
    }
}

/// 已物化的输入面（JSON 视图，给 JS 求值用）。
///
/// 与 [`InputFace`] 的 StoredValue 视图并存：前者给用户代码，后者给
/// 输出回引（需要保留 Ref 身份）。
struct TaskInputs {
    /// run 输入（JSON 值）。
    input: Value,
    /// 前驱节点输出（按节点 id 的 JSON 对象）。
    nodes: Value,
}

/// 物化已提交的准备参数（各节点类型的公共入口）。
///
/// # Errors
/// 参数缺失、超预算或编码非法时返回 `Invalid`。
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

/// 人类可读的耗时标签：亚秒用毫秒，之上用一位小数秒。
fn elapsed_label(elapsed: Duration) -> String {
    let ms = elapsed.as_millis();
    if ms < 1000 {
        format!("{ms}ms")
    } else {
        format!("{:.1}s", elapsed.as_secs_f64())
    }
}

// 三条管道必须并发推进：若先写完 prompt 再读 stdout，子进程写满管道
// 缓冲就会与写 stdin 互锁（死锁），且子进程退出前读不到任何输出。任一
// 分支失败或超时都判 Uncertain——进程可能已产生副作用。
/// Capture all three pipes concurrently under one deadline. Partial output is never success.
async fn capture_harness(
    child: &mut flow_engine::process::ManagedChild,
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
                // BrokenPipe：子进程可能已退出（提前失败或不读 stdin），
                // 这不是 harness 自身的错误，按「写到这里为止」处理。
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
            // stderr 只保留尾部 512 字节：崩溃现场（panic 栈、错误摘要）
            // 通常在最后，前面的大段噪声对排障无用。
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
            // 采集失败/超时/取消一律 Uncertain：子进程可能已经产生
            // 副作用，绝不能当成普通失败去重试。
            Ok(Err(error)) => Err(TaskError::Uncertain(format!("harness capture: {error}"))),
            Err(_) => Err(TaskError::Uncertain("harness deadline exceeded; result incomplete".into())),
        },
        _ = cancel.cancelled() => Err(TaskError::Uncertain("harness cancelled; result incomplete".into())),
    };
    if result.is_err() {
        // 失败路径必须杀掉子进程并回收：否则它会变成孤儿继续占用机器
        // （ManagedChild 的 kill_on_drop 只覆盖 drop，不覆盖这一路径）。
        let _ = child.start_kill();
        let _ = tokio::time::timeout(Duration::from_secs(2), child.wait()).await;
    }
    result
}

#[cfg(all(test, unix))]
mod harness_capture_tests {
    use super::*;
    /// 测试辅助：以 `/bin/sh -c` 跑一段脚本并采集其输出。
    async fn capture(
        script: &str,
        input: &[u8],
        timeout: Duration,
    ) -> Result<std::process::Output, TaskError> {
        let mut child = flow_engine::process::ManagedChild::spawn_harness(
            &mut flow_engine::exec::harness_command("/bin/sh", &["-c".into(), script.into()], None),
        )
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
    /// 写 stdin 被子进程阻塞时仍须按截止时间收口，并把子进程回收掉。
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
    /// 写 prompt 的同时必须读 stdout，否则管道缓冲会互锁。
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
    /// stdout 溢出捕获上限时不得返回「截断的成功」。
    #[tokio::test]
    async fn stdout_overflow_never_returns_truncated_success() {
        assert!(matches!(
            capture("exec yes", b"", Duration::from_secs(3)).await,
            Err(TaskError::Uncertain(_))
        ));
    }
    /// stderr 只保留尾部 512 字节。
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
