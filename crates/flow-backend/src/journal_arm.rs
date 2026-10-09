//! journal 后端的 `AnyBackend` 适配层（M1：v2 接入 flow-server 公共契约）。
//!
//! 职责：把 v2 journal 事实（`JournalBackend` 的已提交 State + journal 事件）
//! 映射到 v1 公共面的 DTO（`flow_dto` 的 WorkflowVersion / RunRecord / …）
//! 与事件模型（`flow_engine::Envelope`）。语义立场：
//!
//! - **journal 是唯一权威**：所有写走 journal 命令（幂等键 = 命令身份），
//!   读从已提交 State 折影；SQLite 投影只服务 journal_v2 开发 RPC，这里不用。
//! - **不编造**：journal 事件没有墙钟时间——映射出的 Envelope.ts 是 Unix
//!   epoch（v1 契约要求该字段；前端仅作展示）。run 的 ended_at 同理取 None。
//!   v2 的权威时间语义是 LSN 与 run_seq，不是墙钟。
//! - **只映射前端消费的事件类型**（run/node 生命周期 + 信号）：审计事实
//!   （InputPrepared / OperationIntent / ValueChunk / …）不出现在 v1 事件面。
//!
//! 已知的 v1→v2 语义差异（重试范围、子 run id、request_id）见
//! DESIGN.md §14 附录与 SQLITE_V1_TO_V2_MIGRATION.md。

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde_json::Value;

use crate::journal::{CommandReceipt, JournalBackend, JournalError};
use crate::{resolve_runnable_definition, VersionSource};
use crate::{BackendError, CreateRun, CreatedRun, SignalAck, SignalRequest};
use flow_dto::{
    NodeTemplate, NodeTemplateSummary, RunRecord, RunStats, Schedule, Webhook, WorkflowRunStats,
    WorkflowSummary, WorkflowVersion, STATUS_DRAFT, STATUS_PUBLISHED,
};
use flow_engine::fold::{NodeRecord, NodeState, RunPhase, RunState};
use flow_engine::journal_state::{Run as JournalRun, Workflow as JournalWorkflow};
use flow_engine::{Envelope, Event as EngineEvent};
use flow_journal::page::{Cursor, Filter};
use flow_journal::tail::TailReader;
use flow_journal::{Event as JournalEvent, EventKind, StoredValue, MAX_LINE_BYTES};
use uuid::Uuid;

/// run 输入/输出值的解析预算（与 run_start 的 store 预算一致）。
const VALUE_BUDGET: usize = 8 * 1024 * 1024;
/// v1 Envelope 的占位时间戳：journal 事件不携带墙钟（见模块注释）。
const EPOCH: DateTime<Utc> = DateTime::<Utc>::UNIX_EPOCH;
/// 订阅空闲轮询间隔（与 journal_v2 的 run.subscribe 一致）。
const SUBSCRIBE_POLL: std::time::Duration = std::time::Duration::from_millis(50);
/// 订阅单轮扫描的字节预算（有界，防单轮垄断）。
const SUBSCRIBE_SCAN_BYTES: u64 = 4 * 1024 * 1024;
/// 单页事件数上限（与 journal_v2 的分页上限一致）。
const PAGE_LIMIT: usize = 256;
/// 订阅单轮最多连续页数（批间让出，长回放不垄断任务）。
const POLL_PAGES: usize = 4;

type Result<T> = std::result::Result<T, BackendError>;
/// 订阅单轮扫描的产出（阻塞侧 → 异步侧）与两种轮询载荷。
type PollOutput<T> = std::result::Result<T, flow_journal::Error>;
type RunPollBatch = (
    Vec<Envelope>,
    HashMap<String, u32>,
    u64,
    bool,
    Option<Cursor>,
);
type GlobalPollBatch = (TailReader, Vec<Envelope>, HashMap<String, u32>);

fn internal(err: impl std::fmt::Display) -> BackendError {
    BackendError::Internal(err.to_string())
}

fn map_journal_error(error: JournalError) -> BackendError {
    match error {
        JournalError::Journal(flow_journal::Error::Invalid(message)) => {
            BackendError::Invalid(message)
        }
        JournalError::Journal(flow_journal::Error::Conflict(message)) => {
            BackendError::Conflict(message)
        }
        JournalError::Journal(other) => internal(other),
        JournalError::Projection(message) => internal(format!("projection: {message}")),
        JournalError::LinkClosed(message) => internal(message),
        // 已提交但投影可见性等待超时：命令本身成功了。写路径经 [`receipt`]
        // 归一为成功；走到这里的是读路径误用——按内部错误上报，不吞。
        JournalError::CommittedNotVisible(receipt) => {
            internal(format!("committed but not visible: {receipt:?}"))
        }
    }
}

/// 命令回执的错误归一：`CommittedNotVisible` 视为成功（拿回执继续）。
fn receipt(result: std::result::Result<CommandReceipt, JournalError>) -> Result<CommandReceipt> {
    match result {
        Ok(receipt) => Ok(receipt),
        Err(JournalError::CommittedNotVisible(receipt)) => Ok(receipt),
        Err(error) => Err(map_journal_error(error)),
    }
}

fn fresh_request_id() -> String {
    Uuid::now_v7().to_string()
}

fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|ts| ts.with_timezone(&Utc))
        .map_err(|e| internal(format!("invalid timestamp {raw}: {e}")))
}

/// journal payload 的 u64 字段兼容两种编码（纯数字 / 十进制字符串——
/// reducer 的 `number()` 走字符串，serde 结构体字段走数字）。
fn u64_of(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|s| s.parse().ok()))
}

/// StoredValue → JSON（阻塞读；值内容受 durable_lsn 上界约束）。
fn materialize_at(root: &std::path::Path, upper: u64, stored: &StoredValue, limit: usize) -> Value {
    flow_journal::value::materialize(root, upper, stored, limit).unwrap_or(Value::Null)
}

fn materialize(backend: &JournalBackend, stored: &StoredValue, limit: usize) -> Result<Value> {
    flow_journal::value::materialize(
        backend.journal.root(),
        backend.journal.durable_lsn(),
        stored,
        limit,
    )
    .map_err(|e| internal(format!("value materialization: {e}")))
}

// ---- workflow ----

pub(crate) async fn create_workflow(backend: &JournalBackend, name: &str) -> Result<String> {
    let receipt = receipt(backend.workflow_create(name, None).await)?;
    receipt.result["workflow_id"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| internal("workflow.create receipt missing workflow_id"))
}

pub(crate) async fn update_workflow(
    backend: &JournalBackend,
    workflow_id: &str,
    definition: &Value,
) -> Result<i64> {
    if !backend.state().await.workflows.contains_key(workflow_id) {
        return Err(BackendError::WorkflowNotFound(workflow_id.to_string()));
    }
    let receipt = receipt(
        backend
            .workflow_update(workflow_id, definition.clone(), None)
            .await,
    )?;
    receipt.result["version"]
        .as_u64()
        .map(|v| v as i64)
        .ok_or_else(|| internal("workflow.update receipt missing version"))
}

pub(crate) async fn publish(
    backend: &JournalBackend,
    workflow_id: &str,
    version: i64,
) -> Result<()> {
    let state = backend.state().await;
    let workflow = state
        .workflows
        .get(workflow_id)
        .ok_or_else(|| BackendError::WorkflowNotFound(workflow_id.to_string()))?;
    let stored = workflow
        .versions
        .get(&(version as u64))
        .ok_or_else(|| BackendError::VersionNotFound(workflow_id.to_string(), version))?;
    if stored.published {
        // 重复发布幂等成功（v1 UPDATE 语义相同）
        return Ok(());
    }
    drop(state);
    receipt(
        backend
            .workflow_publish(workflow_id, version as u64, None)
            .await,
    )?;
    Ok(())
}

fn workflow_version(
    backend: &JournalBackend,
    workflow_id: &str,
    version: &flow_engine::journal_state::DefinitionVersion,
) -> Result<WorkflowVersion> {
    Ok(WorkflowVersion {
        workflow_id: workflow_id.to_string(),
        version: version.version as i64,
        definition: materialize(backend, &version.definition, MAX_LINE_BYTES)?,
        checksum: version.checksum.clone(),
        status: if version.published {
            STATUS_PUBLISHED.into()
        } else {
            STATUS_DRAFT.into()
        },
        created_at: parse_ts(&version.created_at)?,
    })
}

pub(crate) async fn get_version(
    backend: &JournalBackend,
    workflow_id: &str,
    version: Option<i64>,
) -> Result<WorkflowVersion> {
    let state = backend.state().await;
    let workflow = state
        .workflows
        .get(workflow_id)
        .ok_or_else(|| BackendError::WorkflowNotFound(workflow_id.to_string()))?;
    match version {
        Some(v) => {
            let stored = workflow
                .versions
                .get(&(v as u64))
                .ok_or_else(|| BackendError::VersionNotFound(workflow_id.to_string(), v))?;
            workflow_version(backend, workflow_id, stored)
        }
        None => {
            let (_, latest) = workflow
                .versions
                .last_key_value()
                .ok_or_else(|| BackendError::WorkflowNotFound(workflow_id.to_string()))?;
            workflow_version(backend, workflow_id, latest)
        }
    }
}

pub(crate) async fn latest_published(
    backend: &JournalBackend,
    workflow_id: &str,
) -> Result<Option<i64>> {
    Ok(backend
        .state()
        .await
        .workflows
        .get(workflow_id)
        .and_then(|w| w.versions.iter().rev().find(|(_, v)| v.published))
        .map(|(v, _)| *v as i64))
}

pub(crate) async fn list_versions(
    backend: &JournalBackend,
    workflow_id: &str,
) -> Result<Vec<WorkflowVersion>> {
    let state = backend.state().await;
    let workflow = state
        .workflows
        .get(workflow_id)
        .ok_or_else(|| BackendError::WorkflowNotFound(workflow_id.to_string()))?;
    let mut versions = Vec::new();
    for (_, stored) in workflow.versions.iter().rev() {
        versions.push(workflow_version(backend, workflow_id, stored)?);
    }
    Ok(versions)
}

fn workflow_summary(workflow: &JournalWorkflow) -> Result<WorkflowSummary> {
    Ok(WorkflowSummary {
        workflow_id: workflow.workflow_id.clone(),
        name: workflow.name.clone(),
        latest_version: workflow.versions.last_key_value().map_or(0, |(v, _)| *v) as i64,
        published_version: workflow
            .versions
            .iter()
            .rev()
            .find(|(_, v)| v.published)
            .map(|(v, _)| *v as i64),
        created_at: parse_ts(&workflow.created_at)?,
    })
}

pub(crate) async fn list_workflows(backend: &JournalBackend) -> Result<Vec<WorkflowSummary>> {
    let state = backend.state().await;
    let mut summaries = Vec::new();
    for workflow in state.workflows.values() {
        summaries.push(workflow_summary(workflow)?);
    }
    // v1 按 created_at 倒序
    summaries.sort_by_key(|s| std::cmp::Reverse(s.created_at));
    Ok(summaries)
}

pub(crate) async fn delete_workflow(backend: &JournalBackend, workflow_id: &str) -> Result<()> {
    let state = backend.state().await;
    if !state.workflows.contains_key(workflow_id) {
        return Err(BackendError::WorkflowNotFound(workflow_id.to_string()));
    }
    let runs = state
        .runs
        .values()
        .filter(|r| r.workflow_id == workflow_id)
        .count();
    if runs > 0 {
        // v1 语义：已有 run 记录的工作流拒绝删除（事件日志不允许变成孤儿）
        return Err(BackendError::Conflict(format!(
            "workflow {workflow_id} 已有 {runs} 条 run 记录，拒绝删除"
        )));
    }
    drop(state);
    receipt(backend.workflow_delete(workflow_id, None).await)?;
    Ok(())
}

/// 「只有 published 可执行 + 创建前校验」的单一规则复用：journal 臂实现
/// [`VersionSource`]，与 sqlite / pg 走同一份 `resolve_runnable_definition`。
struct JournalVersionSource(Arc<JournalBackend>);

#[async_trait::async_trait]
impl VersionSource for JournalVersionSource {
    async fn get_version_by_ref(
        &self,
        workflow_id: &str,
        version: Option<i64>,
    ) -> Result<WorkflowVersion> {
        get_version(&self.0, workflow_id, version).await
    }

    async fn latest_published_version(&self, workflow_id: &str) -> Result<Option<i64>> {
        latest_published(&self.0, workflow_id).await
    }
}

pub(crate) async fn create_run(
    backend: &Arc<JournalBackend>,
    spec: CreateRun,
) -> Result<CreatedRun> {
    let (version, _definition) = resolve_runnable_definition(
        &JournalVersionSource(backend.clone()),
        &spec.workflow_id,
        spec.version,
    )
    .await?;
    let request_id = fresh_request_id();
    let receipt = receipt(
        backend
            .run_start(
                &spec.workflow_id,
                Some(version as u64),
                spec.input,
                &spec.source,
                spec.source_detail.as_deref(),
                Some(&request_id),
            )
            .await,
    )?;
    let run_id = receipt.result["run_id"]
        .as_str()
        .ok_or_else(|| internal("run.start receipt missing run_id"))?
        .to_string();
    let workflow_version = receipt.result["workflow_version"]
        .as_u64()
        .ok_or_else(|| internal("run.start receipt missing workflow_version"))?
        as i64;
    Ok(CreatedRun {
        run_id,
        workflow_version,
    })
}

// ---- run 读面 ----

fn run_record(backend: &JournalBackend, run: &JournalRun) -> Result<RunRecord> {
    Ok(RunRecord {
        id: run.run_id.clone(),
        workflow_id: run.workflow_id.clone(),
        workflow_version: run.workflow_version as i64,
        status: run.status.clone(),
        input: materialize(backend, &run.input, VALUE_BUDGET)?,
        output: run
            .output
            .as_ref()
            .map(|stored| materialize(backend, stored, VALUE_BUDGET))
            .transpose()?,
        error: run.error.clone(),
        source: run.source.clone(),
        source_detail: run.source_detail.clone(),
        started_at: parse_ts(&run.created_at)?,
        // journal 不记录终态墙钟（权威时间是 LSN/run_seq）——ended_at 无从
        // 取证，不编造。前端时长列以 created_at 为准。
        ended_at: None,
    })
}

pub(crate) async fn get_run(backend: &JournalBackend, run_id: &str) -> Result<RunRecord> {
    let state = backend.state().await;
    let run = state
        .runs
        .get(run_id)
        .ok_or_else(|| BackendError::RunNotFound(run_id.to_string()))?;
    run_record(backend, run)
}

pub(crate) async fn list_runs(
    backend: &JournalBackend,
    workflow_id: Option<&str>,
    status: Option<&str>,
    source: Option<&str>,
    before_run_id: Option<&str>,
    limit: i64,
) -> Result<Vec<RunRecord>> {
    let state = backend.state().await;
    // 游标 run 的 created_at：返回严格更旧的记录（v1 的 started_at < 子查询）
    let cursor_time = before_run_id
        .and_then(|id| state.runs.get(id))
        .map(|r| r.created_at.clone());
    let mut selected: Vec<&JournalRun> = state
        .runs
        .values()
        .filter(|run| {
            workflow_id.is_none_or(|id| run.workflow_id == id)
                && status.is_none_or(|s| run.status == s)
                && source.is_none_or(|s| run.source == s)
                && cursor_time.as_ref().is_none_or(|t| run.created_at < *t)
        })
        .collect();
    selected.sort_by_key(|r| std::cmp::Reverse(r.created_at.clone()));
    selected
        .into_iter()
        .take(limit.max(0) as usize)
        .map(|run| run_record(backend, run))
        .collect()
}

pub(crate) async fn run_stats(
    backend: &JournalBackend,
    workflow_id: Option<&str>,
) -> Result<RunStats> {
    let state = backend.state().await;
    let mut stats = RunStats {
        total: 0,
        by_status: BTreeMap::new(),
        by_workflow: Vec::new(),
    };
    let mut grouped: BTreeMap<String, WorkflowRunStats> = BTreeMap::new();
    for run in state
        .runs
        .values()
        .filter(|r| workflow_id.is_none_or(|id| r.workflow_id == id))
    {
        stats.total += 1;
        *stats.by_status.entry(run.status.clone()).or_insert(0) += 1;
        if workflow_id.is_none() {
            let entry = grouped
                .entry(run.workflow_id.clone())
                .or_insert(WorkflowRunStats {
                    workflow_id: run.workflow_id.clone(),
                    total: 0,
                    by_status: BTreeMap::new(),
                });
            entry.total += 1;
            *entry.by_status.entry(run.status.clone()).or_insert(0) += 1;
        }
    }
    stats.by_workflow = grouped.into_values().collect();
    Ok(stats)
}

/// 单写者进程：非终态 run 即本进程正在驱动（执行循环随服务启动）。
pub(crate) async fn is_live(backend: &JournalBackend, run_id: &str) -> bool {
    backend
        .state()
        .await
        .runs
        .get(run_id)
        .is_some_and(|run| !run.terminal())
}

/// journal Run（v2 折影）→ 引擎 RunState（v1 时间线契约）。
async fn run_state(backend: &JournalBackend, run: &JournalRun) -> Result<RunState> {
    let mut records = HashMap::new();
    // 子 run 归属映射：v2 用 uuid v7 + parent 关系（派生式已废弃），但等待
    // 解除后 wait 清空会丢 child_run_id——从全部 run 的 parent 字段反查
    // （折影派生、确定性；v1 时间线要求终态节点仍携带 child_run_id）。
    let child_of: HashMap<(String, String), String> = backend
        .inspect(|state| {
            state
                .runs
                .values()
                .filter_map(|r| {
                    r.parent
                        .as_ref()
                        .map(|p| ((p.run_id.clone(), p.node_id.clone()), r.run_id.clone()))
                })
                .collect()
        })
        .await;
    for (node_id, node) in &run.nodes {
        let state = match node.status.as_str() {
            "succeeded" => NodeState::Completed {
                attempt: node.attempt.max(1),
            },
            "failed" => NodeState::Failed {
                attempt: node.attempt.max(1),
                error: node.error.clone().unwrap_or_else(|| "failed".into()),
                retryable: false,
            },
            "skipped" => NodeState::Skipped {
                // 折影保留的跳过原因（upstream_failed / upstream_skipped /
                // branch_not_taken …；事件里携带，见 NodeSkipped 事件）
                reason: node.skip_reason.clone().unwrap_or_else(|| "skipped".into()),
            },
            // pending（尚未派发）→ v1 Pending；waiting（业务等待）在 v1 折叠
            // 里就是 Running——同词同义
            "pending" => NodeState::Pending,
            _ => NodeState::Running {
                attempt: node.attempt.max(1),
            },
        };
        records.insert(
            node_id.clone(),
            NodeRecord {
                state,
                output: node
                    .output
                    .as_ref()
                    .map(|stored| materialize(backend, stored, VALUE_BUDGET))
                    .transpose()?,
                started_at: None,
                ended_at: None,
                duration_ms: None,
                last_signal: None,
                child_run_id: node
                    .wait
                    .as_ref()
                    .and_then(|w| w.child_run_id.clone())
                    .or_else(|| {
                        child_of
                            .get(&(run.run_id.clone(), node_id.clone()))
                            .cloned()
                    }),
                input: node
                    .prepared
                    .as_ref()
                    .map(|prepared| {
                        // v1 语义：输入面快照在写入侧脱敏（时间线展示值）。
                        // journal 的 prepared.params 是原始数据面——这里补
                        // 同规则脱敏，两个展示面才不会一个骗客户端。
                        Ok(flow_engine::redact_value(&materialize(
                            backend,
                            &prepared.params,
                            VALUE_BUDGET,
                        )?))
                    })
                    .transpose()?,
            },
        );
    }
    let phase = match run.status.as_str() {
        "succeeded" => RunPhase::Succeeded,
        "failed" => RunPhase::Failed,
        "cancelled" => RunPhase::Cancelled,
        _ => RunPhase::Running,
    };
    Ok(RunState {
        records,
        phase,
        fatal_error: run.error.clone(),
        output: run
            .output
            .as_ref()
            .map(|stored| materialize(backend, stored, VALUE_BUDGET))
            .transpose()?,
        workflow_id: Some(run.workflow_id.clone()),
        workflow_version: Some(run.workflow_version as i64),
        input: materialize(backend, &run.input, VALUE_BUDGET)?,
        last_seq: run.last_run_seq,
        depth: run.depth,
        started_at: Some(parse_ts(&run.created_at)?),
        ended_at: None,
    })
}

pub(crate) async fn snapshot(backend: &JournalBackend, run_id: &str) -> Result<RunState> {
    let state = backend.state().await;
    let run = state
        .runs
        .get(run_id)
        .ok_or_else(|| BackendError::RunNotFound(run_id.to_string()))?;
    run_state(backend, run).await
}

// ---- 信号 / 取消 ----

pub(crate) async fn signal(backend: &JournalBackend, req: SignalRequest) -> Result<SignalAck> {
    let state = backend.state().await;
    let run = state
        .runs
        .get(&req.run_id)
        .ok_or_else(|| BackendError::RunNotFound(req.run_id.clone()))?;
    if run.terminal() {
        return Err(BackendError::Conflict(format!(
            "run {} 已终态，不能交付信号",
            req.run_id
        )));
    }
    let node = run.nodes.get(&req.node_id).ok_or_else(|| {
        BackendError::Invalid(format!("节点 {} 不存在或未在等待信号", req.node_id))
    })?;
    // v1 契约：副作用节点崩溃残留（uncertain）用 payload.action 裁决
    // （retry / succeeded / failed）——桥接到 v2 的 run.adjudicate 决策面。
    if node.wait.as_ref().is_some_and(|w| w.kind == "uncertain") {
        let operation_id = node
            .operation
            .as_ref()
            .map(|op| op.operation_id.clone())
            .ok_or_else(|| {
                BackendError::Invalid(format!(
                    "节点 {} 的不确定操作缺少 operation_id，无法裁决",
                    req.node_id
                ))
            })?;
        let action = req
            .payload
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("succeeded");
        let (decision, output, reason) = match action {
            "retry" => (
                "retry",
                Value::Null,
                "manual retry via run.signal".to_string(),
            ),
            "failed" => (
                "failed",
                Value::Null,
                req.payload
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("manual failure via run.signal")
                    .to_string(),
            ),
            _ => (
                "accept_output",
                req.payload.get("output").cloned().unwrap_or(Value::Null),
                "manual acceptance via run.signal".to_string(),
            ),
        };
        drop(state);
        let request_id = req.signal_id.clone().unwrap_or_else(fresh_request_id);
        receipt(
            backend
                .run_adjudicate(
                    &req.run_id,
                    &req.node_id,
                    &operation_id,
                    &reason,
                    output,
                    decision,
                    &request_id,
                )
                .await,
        )?;
        return Ok(SignalAck {
            // v1 非 pg 契约：只回显客户端提供的 id，不伪造服务端生成的键
            signal_id: req.signal_id.clone(),
            status: "applied".into(),
            delivered: true,
            event_seq: None,
            error: None,
        });
    }
    if node.wait.as_ref().is_none_or(|w| w.kind != "signal") {
        return Err(BackendError::Invalid(format!(
            "节点 {} 不在 signal 等待中",
            req.node_id
        )));
    }
    drop(state);
    // v1 契约允许省略 signal_id；幂等键缺省由服务端生成（重试需带同一 id）
    let request_id = req.signal_id.clone().unwrap_or_else(fresh_request_id);
    receipt(
        backend
            .run_signal(&req.run_id, &req.node_id, req.payload, Some(&request_id))
            .await,
    )?;
    Ok(SignalAck {
        signal_id: req.signal_id.clone(),
        status: "applied".into(),
        delivered: true,
        event_seq: None,
        error: None,
    })
}

pub(crate) async fn cancel(
    backend: &JournalBackend,
    run_id: &str,
    signal_id: Option<String>,
) -> Result<SignalAck> {
    let state = backend.state().await;
    let run = state
        .runs
        .get(run_id)
        .ok_or_else(|| BackendError::RunNotFound(run_id.to_string()))?;
    if run.terminal() {
        return Err(BackendError::Conflict(format!("run {run_id} 已是终态")));
    }
    drop(state);
    let request_id = signal_id.clone().unwrap_or_else(fresh_request_id);
    receipt(backend.run_cancel(run_id, Some(&request_id)).await)?;
    Ok(SignalAck {
        signal_id,
        status: "applied".into(),
        delivered: true,
        event_seq: None,
        error: None,
    })
}

// ---- 触发器（schedules / webhooks）----

/// journal 配置事实 → v1 Schedule DTO。input 的 Null 映射为 None（v1 的
/// Option 语义；run_start 对 schedule 的 input 比较用 Value 等价，Null ==
/// 缺省一致）。
fn schedule_from_config(config: &Value) -> Result<Schedule> {
    let input = if config["input"].is_null() {
        None
    } else {
        Some(config["input"].clone())
    };
    Ok(Schedule {
        id: config["id"]
            .as_str()
            .ok_or_else(|| internal("schedule config missing id"))?
            .to_string(),
        workflow_id: config["workflow_id"]
            .as_str()
            .ok_or_else(|| internal("schedule config missing workflow_id"))?
            .to_string(),
        cron_expr: config["cron_expr"]
            .as_str()
            .ok_or_else(|| internal("schedule config missing cron_expr"))?
            .to_string(),
        input,
        enabled: config["enabled"].as_bool().unwrap_or(false),
        created_at: parse_ts(
            config["created_at"]
                .as_str()
                .ok_or_else(|| internal("schedule config missing created_at"))?,
        )?,
    })
}

fn webhook_from_config(config: &Value) -> Result<Webhook> {
    Ok(Webhook {
        token: config["token"]
            .as_str()
            .ok_or_else(|| internal("webhook config missing token"))?
            .to_string(),
        workflow_id: config["workflow_id"]
            .as_str()
            .ok_or_else(|| internal("webhook config missing workflow_id"))?
            .to_string(),
        enabled: config["enabled"].as_bool().unwrap_or(false),
        created_at: parse_ts(
            config["created_at"]
                .as_str()
                .ok_or_else(|| internal("webhook config missing created_at"))?,
        )?,
    })
}

pub(crate) async fn create_schedule(
    backend: &JournalBackend,
    workflow_id: &str,
    cron_expr: &str,
    input: Option<&Value>,
    enabled: bool,
) -> Result<Schedule> {
    if !backend.state().await.workflows.contains_key(workflow_id) {
        return Err(BackendError::WorkflowNotFound(workflow_id.to_string()));
    }
    let id = fresh_request_id();
    let patch = serde_json::json!({
        "workflow_id": workflow_id,
        "cron_expr": cron_expr,
        "input": input.cloned().unwrap_or(Value::Null),
        "enabled": enabled,
        "created_at": chrono::Utc::now().to_rfc3339(),
    });
    // config_change 的回执就是合并后的完整配置（含服务端补的 id 键）
    let receipt = receipt(
        backend
            .config_change(
                "schedule.change",
                EventKind::ScheduleChanged,
                &id,
                patch,
                Some(&fresh_request_id()),
            )
            .await,
    )?;
    schedule_from_config(&receipt.result)
}

pub(crate) async fn list_schedules(
    backend: &JournalBackend,
    workflow_id: Option<&str>,
) -> Result<Vec<Schedule>> {
    let state = backend.state().await;
    let mut schedules: Vec<Schedule> = state
        .schedules
        .values()
        .filter(|config| workflow_id.is_none_or(|id| config["workflow_id"] == id))
        .map(schedule_from_config)
        .collect::<Result<_>>()?;
    schedules.sort_by_key(|s| std::cmp::Reverse(s.created_at));
    Ok(schedules)
}

pub(crate) async fn update_schedule(
    backend: &JournalBackend,
    id: &str,
    cron_expr: Option<&str>,
    input: Option<Option<Value>>,
    enabled: Option<bool>,
) -> Result<()> {
    if !backend.state().await.schedules.contains_key(id) {
        return Err(BackendError::ScheduleNotFound(id.to_string()));
    }
    let mut patch = serde_json::Map::new();
    if let Some(cron_expr) = cron_expr {
        patch.insert("cron_expr".into(), Value::String(cron_expr.to_string()));
    }
    if let Some(input) = input {
        patch.insert("input".into(), input.unwrap_or(Value::Null));
    }
    if let Some(enabled) = enabled {
        patch.insert("enabled".into(), Value::Bool(enabled));
    }
    if patch.is_empty() {
        // v1 语义：无字段更新也要确认 schedule 存在（前置检查已做）
        return Ok(());
    }
    receipt(
        backend
            .config_change(
                "schedule.change",
                EventKind::ScheduleChanged,
                id,
                Value::Object(patch),
                Some(&fresh_request_id()),
            )
            .await,
    )?;
    Ok(())
}

pub(crate) async fn delete_schedule(backend: &JournalBackend, id: &str) -> Result<()> {
    if !backend.state().await.schedules.contains_key(id) {
        return Err(BackendError::ScheduleNotFound(id.to_string()));
    }
    receipt(
        backend
            .config_change(
                "schedule.change",
                EventKind::ScheduleChanged,
                id,
                serde_json::json!({"deleted": true}),
                Some(&fresh_request_id()),
            )
            .await,
    )?;
    Ok(())
}

/// 触发去重：journal 的触发身份就是命令身份（scope = run.start:schedule:<id>，
/// request_id = fire_at 键）。已提交过该触发点 → false；否则 true。
/// 与 `trigger_start` 的内部幂等配合，并发双检也只会产生一个 run。
pub(crate) async fn try_insert_fire(
    backend: &JournalBackend,
    schedule_id: &str,
    fire_at: chrono::DateTime<chrono::Utc>,
) -> Result<bool> {
    let scope = format!("run.start:schedule:{schedule_id}");
    let request_id = fire_at.to_rfc3339();
    let existing = backend
        .command_status(&scope, &request_id)
        .await
        .map_err(map_journal_error)?;
    Ok(existing.is_none())
}

/// journal 不预登记触发权（命令身份即去重），无需撤销。
pub(crate) async fn delete_fire(
    _backend: &JournalBackend,
    _schedule_id: &str,
    _fire_at: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    Ok(())
}

pub(crate) async fn create_webhook(backend: &JournalBackend, workflow_id: &str) -> Result<Webhook> {
    if !backend.state().await.workflows.contains_key(workflow_id) {
        return Err(BackendError::WorkflowNotFound(workflow_id.to_string()));
    }
    let token = Uuid::now_v7().simple().to_string();
    let patch = serde_json::json!({
        "workflow_id": workflow_id,
        "enabled": true,
        "created_at": chrono::Utc::now().to_rfc3339(),
    });
    let receipt = receipt(
        backend
            .config_change(
                "webhook.change",
                EventKind::WebhookChanged,
                &token,
                patch,
                Some(&fresh_request_id()),
            )
            .await,
    )?;
    webhook_from_config(&receipt.result)
}

pub(crate) async fn list_webhooks(
    backend: &JournalBackend,
    workflow_id: Option<&str>,
) -> Result<Vec<Webhook>> {
    let state = backend.state().await;
    let mut webhooks: Vec<Webhook> = state
        .webhooks
        .values()
        .filter(|config| workflow_id.is_none_or(|id| config["workflow_id"] == id))
        .map(webhook_from_config)
        .collect::<Result<_>>()?;
    webhooks.sort_by_key(|w| std::cmp::Reverse(w.created_at));
    Ok(webhooks)
}

pub(crate) async fn get_webhook(backend: &JournalBackend, token: &str) -> Result<Option<Webhook>> {
    let state = backend.state().await;
    match state.webhooks.get(token) {
        Some(config) => Ok(Some(webhook_from_config(config)?)),
        None => Ok(None),
    }
}

pub(crate) async fn set_webhook_enabled(
    backend: &JournalBackend,
    token: &str,
    enabled: bool,
) -> Result<()> {
    if !backend.state().await.webhooks.contains_key(token) {
        return Err(BackendError::WebhookNotFound(token.to_string()));
    }
    receipt(
        backend
            .config_change(
                "webhook.change",
                EventKind::WebhookChanged,
                token,
                serde_json::json!({"enabled": enabled}),
                Some(&fresh_request_id()),
            )
            .await,
    )?;
    Ok(())
}

pub(crate) async fn delete_webhook(backend: &JournalBackend, token: &str) -> Result<()> {
    if !backend.state().await.webhooks.contains_key(token) {
        return Err(BackendError::WebhookNotFound(token.to_string()));
    }
    receipt(
        backend
            .config_change(
                "webhook.change",
                EventKind::WebhookChanged,
                token,
                serde_json::json!({"deleted": true}),
                Some(&fresh_request_id()),
            )
            .await,
    )?;
    Ok(())
}

// ---- 可复用节点模板（TemplateChanged 事实）----

fn template_from_config(config: &Value) -> Result<NodeTemplate> {
    Ok(NodeTemplate {
        id: config["id"]
            .as_str()
            .ok_or_else(|| internal("template missing id"))?
            .to_string(),
        name: config["name"]
            .as_str()
            .ok_or_else(|| internal("template missing name"))?
            .to_string(),
        category: config["category"].as_str().map(str::to_string),
        nodes: config["nodes"].clone(),
        edges: config["edges"].clone(),
        created_at: parse_ts(
            config["created_at"]
                .as_str()
                .ok_or_else(|| internal("template missing created_at"))?,
        )?,
        updated_at: parse_ts(
            config["updated_at"]
                .as_str()
                .ok_or_else(|| internal("template missing updated_at"))?,
        )?,
    })
}

/// journal 命令的 Conflict 在模板面只有名字冲突一种来源 → TemplateNameTaken。
fn map_template_error(error: BackendError, name: &str) -> BackendError {
    match error {
        BackendError::Conflict(_) => BackendError::TemplateNameTaken(name.to_string()),
        other => other,
    }
}

pub(crate) async fn template_create(
    backend: &JournalBackend,
    name: &str,
    category: Option<&str>,
    nodes: &Value,
    edges: &Value,
) -> Result<NodeTemplate> {
    let id = fresh_request_id();
    let patch = serde_json::json!({
        "name": name,
        "category": category,
        "nodes": nodes,
        "edges": edges,
        "created_at": chrono::Utc::now().to_rfc3339(),
    });
    let receipt = receipt(
        backend
            .template_change(&id, patch, Some(&fresh_request_id()))
            .await,
    )
    .map_err(|error| map_template_error(error, name))?;
    template_from_config(&receipt.result)
}

pub(crate) async fn template_list(backend: &JournalBackend) -> Result<Vec<NodeTemplateSummary>> {
    let state = backend.state().await;
    let mut templates: Vec<NodeTemplateSummary> = state
        .templates
        .values()
        .map(|config| {
            Ok(NodeTemplateSummary {
                id: config["id"]
                    .as_str()
                    .ok_or_else(|| internal("template id"))?
                    .to_string(),
                name: config["name"]
                    .as_str()
                    .ok_or_else(|| internal("template name"))?
                    .to_string(),
                category: config["category"].as_str().map(str::to_string),
                node_count: config["nodes"].as_array().map_or(0, |n| n.len()) as i64,
                created_at: parse_ts(
                    config["created_at"]
                        .as_str()
                        .ok_or_else(|| internal("template created_at"))?,
                )?,
                updated_at: parse_ts(
                    config["updated_at"]
                        .as_str()
                        .ok_or_else(|| internal("template updated_at"))?,
                )?,
            })
        })
        .collect::<Result<_>>()?;
    templates.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(templates)
}

pub(crate) async fn template_get(backend: &JournalBackend, id: &str) -> Result<NodeTemplate> {
    let state = backend.state().await;
    let config = state
        .templates
        .get(id)
        .ok_or_else(|| BackendError::TemplateNotFound(id.to_string()))?;
    template_from_config(config)
}

pub(crate) async fn template_update(
    backend: &JournalBackend,
    id: &str,
    name: Option<&str>,
    category: Option<Option<&str>>,
    nodes: Option<&Value>,
    edges: Option<&Value>,
) -> Result<NodeTemplate> {
    if !backend.state().await.templates.contains_key(id) {
        return Err(BackendError::TemplateNotFound(id.to_string()));
    }
    let mut patch = serde_json::Map::new();
    if let Some(name) = name {
        patch.insert("name".into(), Value::String(name.to_string()));
    }
    if let Some(category) = category {
        patch.insert(
            "category".into(),
            category
                .map(|c| Value::String(c.to_string()))
                .unwrap_or(Value::Null),
        );
    }
    if let Some(nodes) = nodes {
        patch.insert("nodes".into(), nodes.clone());
    }
    if let Some(edges) = edges {
        patch.insert("edges".into(), edges.clone());
    }
    let receipt = receipt(
        backend
            .template_change(id, Value::Object(patch), Some(&fresh_request_id()))
            .await,
    )
    .map_err(|error| map_template_error(error, name.unwrap_or_default()))?;
    template_from_config(&receipt.result)
}

pub(crate) async fn template_delete(backend: &JournalBackend, id: &str) -> Result<bool> {
    if !backend.state().await.templates.contains_key(id) {
        return Ok(false);
    }
    receipt(
        backend
            .template_change(
                id,
                serde_json::json!({"deleted": true}),
                Some(&fresh_request_id()),
            )
            .await,
    )?;
    Ok(true)
}

// ---- 事件面：v2 journal 事件 → v1 Envelope ----

/// 单条 journal 事件 → v1 Envelope。attempt 从 DispatchStarted 事件增量追踪
/// （journal 事件不携带 attempt；终态事件的 attempt = 当前派发序，回放顺序
/// 保证映射正确）。不属于 v1 事件面的审计事实返回 None。
pub(crate) fn envelope(
    event: &JournalEvent,
    root: &std::path::Path,
    upper: u64,
    attempts: &mut HashMap<String, u32>,
) -> Option<Envelope> {
    if event.run_seq == 0 {
        return None;
    }
    let run_id = event.run_id.clone()?;
    let node_id = event.node_id.clone().or_else(|| {
        event
            .payload
            .get("node_id")
            .and_then(Value::as_str)
            .map(str::to_string)
    });
    let attempt_of = |node_id: &Option<String>| -> u32 {
        node_id
            .as_ref()
            .and_then(|id| attempts.get(id.as_str()).copied())
            .unwrap_or(0)
    };
    let payload = &event.payload;
    let engine_event = match event.kind {
        EventKind::RunStarted => EngineEvent::RunStarted {
            workflow_id: payload.get("workflow_id")?.as_str()?.to_string(),
            workflow_version: u64_of(payload.get("workflow_version")?)? as i64,
            input: {
                let stored: StoredValue = serde_json::from_value(payload["input"].clone()).ok()?;
                materialize_at(root, upper, &stored, VALUE_BUDGET)
            },
            depth: u64_of(payload.get("depth").unwrap_or(&Value::Null)).unwrap_or(0) as u32,
        },
        EventKind::DispatchStarted => {
            let node_id = node_id?;
            let attempt =
                u64_of(payload.get("attempt").unwrap_or(&Value::Null)).unwrap_or(1) as u32;
            attempts.insert(node_id.clone(), attempt);
            EngineEvent::NodeStarted {
                node_id,
                attempt,
                child_run_id: None,
                input: None,
            }
        }
        EventKind::NodeCompleted => {
            let node_id = node_id?;
            let attempt = attempt_of(&Some(node_id.clone()));
            let output = {
                let stored: StoredValue = serde_json::from_value(payload["output"].clone()).ok()?;
                materialize_at(root, upper, &stored, VALUE_BUDGET)
            };
            EngineEvent::NodeCompleted {
                node_id,
                attempt,
                output,
                // journal 事件无耗时墙钟：duration 置 0（展示面缺省）
                duration_ms: 0,
            }
        }
        EventKind::NodeFailed => {
            let node_id = node_id?;
            let attempt = attempt_of(&Some(node_id.clone()));
            EngineEvent::NodeFailed {
                node_id,
                attempt,
                error: payload
                    .get("error")
                    .and_then(Value::as_str)
                    .unwrap_or("failed")
                    .to_string(),
                // v2 的纯计算重试（retry_wake_at）在 v1 词汇里就是 retryable
                retryable: payload.get("retry_wake_at").is_some(),
            }
        }
        EventKind::NodeSkipped => EngineEvent::NodeSkipped {
            node_id: node_id?,
            reason: payload
                .get("reason")
                .and_then(Value::as_str)
                .unwrap_or("skipped")
                .to_string(),
        },
        // 等待解除即节点成功（human 信号 / delay 到点 / 子 run 完成）：v2 用
        // WaitResolved 落账，v1 折叠里没有等待事件——映射成 node_completed，
        // 前端实时视图才能翻转节点状态（kind=retry 的等待不经此事件，由
        // 新的 DispatchStarted 继续，不产生假的完成）。
        EventKind::WaitResolved => {
            let node_id = node_id?;
            let attempt = attempt_of(&Some(node_id.clone()));
            let output = {
                let stored: StoredValue = serde_json::from_value(payload["output"].clone()).ok()?;
                materialize_at(root, upper, &stored, VALUE_BUDGET)
            };
            EngineEvent::NodeCompleted {
                node_id,
                attempt,
                output,
                duration_ms: 0,
            }
        }
        EventKind::SignalReceived => {
            let node_id = node_id?;
            let payload_value = {
                let stored: StoredValue =
                    serde_json::from_value(payload["payload"].clone()).ok()?;
                materialize_at(root, upper, &stored, VALUE_BUDGET)
            };
            EngineEvent::SignalReceived {
                node_id,
                payload: payload_value,
            }
        }
        EventKind::RunCompleted => {
            let output = {
                let stored: StoredValue = serde_json::from_value(payload["output"].clone()).ok()?;
                materialize_at(root, upper, &stored, VALUE_BUDGET)
            };
            EngineEvent::RunCompleted { output }
        }
        EventKind::RunFailed => EngineEvent::RunFailed {
            error: payload
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("failed")
                .to_string(),
        },
        EventKind::RunCancelled => EngineEvent::RunCancelled {},
        _ => return None,
    };
    Some(Envelope {
        seq: event.run_seq,
        ts: EPOCH,
        run_id,
        event: engine_event,
    })
}

/// run.events：journal 分页全量读取后映射（阻塞部分走 spawn_blocking）。
pub(crate) async fn read_events(
    backend: &JournalBackend,
    run_id: &str,
    from_seq: Option<u64>,
) -> Result<Vec<Envelope>> {
    if !backend.state().await.runs.contains_key(run_id) {
        return Err(BackendError::RunNotFound(run_id.to_string()));
    }
    let root = backend.journal.root().to_path_buf();
    let identity = backend.journal.id().to_string();
    let upper = backend.journal.durable_lsn();
    let run_id = run_id.to_string();
    let from_seq = from_seq.unwrap_or(0);
    let envelopes = tokio::task::spawn_blocking(
        move || -> std::result::Result<Vec<Envelope>, flow_journal::Error> {
            let mut cursor = Cursor::first(identity.clone(), run_id.clone(), Filter::Events, upper);
            let mut envelopes = Vec::new();
            let mut attempts = HashMap::new();
            loop {
                let page = flow_journal::page::page(
                    &root,
                    &identity,
                    &run_id,
                    Filter::Events,
                    cursor,
                    upper,
                    PAGE_LIMIT,
                )?;
                let next = page.next_cursor.clone();
                for positioned in &page.events {
                    if let Some(envelope) = envelope(&positioned.event, &root, upper, &mut attempts)
                    {
                        if envelope.seq >= from_seq {
                            envelopes.push(envelope);
                        }
                    }
                }
                match next {
                    Some(cursor_next) => cursor = cursor_next,
                    None => break,
                }
            }
            Ok(envelopes)
        },
    )
    .await
    .map_err(|e| internal(format!("event scan: {e}")))?
    .map_err(|e| BackendError::Internal(format!("event scan: {e}")))?;
    Ok(envelopes)
}

// ---- 订阅：journal 尾读 → v1 Envelope 流 ----

/// 订阅状态机：buffer 缓存本轮已映射事件，poll 补充。
struct Subscription {
    backend: Arc<JournalBackend>,
    /// 指定 run：回放全部 + 追尾到终态结束；None：全局纯实时增量。
    run: Option<String>,
    cursor: Option<Cursor>,
    tail: Option<TailReader>,
    /// 全局订阅的起始 LSN：只有此界之后提交的事务流入（跳过历史——包括
    /// 订阅前已存在的 run；订阅后新建 run 的事件天然全部流入）。首轮
    /// poll 采集（= 订阅建立时刻的 durable_lsn）。
    start_lsn: u64,
    /// 单 run 订阅的去重水位：只放行 seq 严格大于它的已映射事件。cursor
    /// 在页耗尽（next_cursor=None）时无法前进——重读同一页是常态而非错误，
    /// 权威去重靠 run_seq（每 run 严格单调）。
    run_from_seq: u64,
    buffer: VecDeque<Envelope>,
    attempts: HashMap<String, u32>,
    terminal_seen: bool,
    finished: bool,
}

impl Subscription {
    /// 单轮拉取。返回本轮是否还有更多（false = 已追平，调用方决定休眠）。
    async fn poll(&mut self) -> bool {
        let root = self.backend.journal.root().to_path_buf();
        let identity = self.backend.journal.id().to_string();
        let upper = self.backend.journal.durable_lsn();
        match self.run.clone() {
            Some(run_id) => {
                let cursor = self.cursor.clone().unwrap_or_else(|| {
                    Cursor::first(identity.clone(), run_id.clone(), Filter::Events, upper)
                });
                let mut attempts = std::mem::take(&mut self.attempts);
                let from_seq = self.run_from_seq;
                let page_run_id = run_id.clone();
                let log_run_id = run_id.clone();
                let result = tokio::task::spawn_blocking(move || -> PollOutput<RunPollBatch> {
                    let mut cursor = cursor;
                    let mut events = Vec::new();
                    let mut more = true;
                    let mut watermark = from_seq;
                    for _ in 0..POLL_PAGES {
                        // page() 以 cursor.upper_lsn 为读上界（durable_lsn 只做
                        // 校验）——续读必须把界顶到本轮的 durable_lsn，否则
                        // 携旧界的 cursor 永远看不到新提交（追尾停摆）。
                        cursor.upper_lsn = upper;
                        let page = flow_journal::page::page(
                            &root,
                            &identity,
                            &page_run_id,
                            Filter::Events,
                            cursor.clone(),
                            upper,
                            PAGE_LIMIT,
                        )?;
                        // 页耗尽后 cursor 不前进，重读同一页是常态——以
                        // run_seq 水位去重（每 run 严格单调递增）
                        for positioned in &page.events {
                            if let Some(envelope) =
                                envelope(&positioned.event, &root, upper, &mut attempts)
                            {
                                if envelope.seq > watermark {
                                    watermark = envelope.seq;
                                    events.push(envelope);
                                }
                            }
                        }
                        match page.next_cursor {
                            Some(next) => cursor = next,
                            None => {
                                more = false;
                                break;
                            }
                        }
                    }
                    Ok((events, attempts, watermark, more, Some(cursor)))
                })
                .await;
                match result {
                    Ok(Ok((events, attempts, watermark, more, cursor))) => {
                        self.attempts = attempts;
                        self.run_from_seq = watermark;
                        self.cursor = cursor;
                        for envelope in events {
                            if envelope.event.is_run_terminal() {
                                self.terminal_seen = true;
                            }
                            self.buffer.push_back(envelope);
                        }
                        if !more && self.terminal_seen {
                            // 单 run 订阅契约：终态且追平 → 流自然结束
                            self.finished = true;
                        }
                        // v1 契约：订阅不存在的 run 立即静默结束（不报错、
                        // 不永久等待）。从未见过事件的空轮询 + State 里无此
                        // run → 结束（v2 的 run 创建即提交 RunStarted，存在
                        // 的 run 必有可回放事件）。
                        if !more && self.buffer.is_empty() && self.run_from_seq == 0 {
                            let missing = !self.backend.state().await.runs.contains_key(&run_id);
                            if missing {
                                self.finished = true;
                            }
                        }
                        more
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(%error, run_id = %log_run_id, "journal subscription poll failed; closing");
                        self.finished = true;
                        false
                    }
                    Err(error) => {
                        tracing::warn!(%error, run_id = %log_run_id, "journal subscription task failed; closing");
                        self.finished = true;
                        false
                    }
                }
            }
            None => {
                let mut tail = self
                    .tail
                    .take()
                    .unwrap_or_else(|| TailReader::new(&root, &identity));
                let start_lsn = self.start_lsn;
                let mut attempts = std::mem::take(&mut self.attempts);
                let result = tokio::task::spawn_blocking(move || -> PollOutput<GlobalPollBatch> {
                    let mut events = Vec::new();
                    let mut bytes = 0u64;
                    while bytes < SUBSCRIBE_SCAN_BYTES {
                        let Some((tx, location)) = tail.next(upper)? else {
                            break;
                        };
                        bytes += location.bytes;
                        // 全局订阅是纯实时增量：起始 LSN 之前的历史
                        // （含订阅前已存在的 run）不流入——从订阅时刻起
                        // 只看新提交的事实，与 v1 broadcast 语义一致
                        if tx.lsn <= start_lsn {
                            continue;
                        }
                        for event in &tx.events {
                            if event.run_id.is_none() || event.run_seq == 0 {
                                continue;
                            }
                            if let Some(envelope) = envelope(event, &root, upper, &mut attempts) {
                                events.push(envelope);
                            }
                        }
                    }
                    Ok((tail, events, attempts))
                })
                .await;
                match result {
                    Ok(Ok((tail, events, attempts))) => {
                        self.tail = Some(tail);
                        self.attempts = attempts;
                        let caught_up = events.is_empty();
                        for envelope in events {
                            self.buffer.push_back(envelope);
                        }
                        !caught_up
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "journal global subscription failed; closing");
                        self.finished = true;
                        false
                    }
                    Err(error) => {
                        tracing::warn!(%error, "journal global subscription task failed; closing");
                        self.finished = true;
                        false
                    }
                }
            }
        }
    }
}

pub(crate) fn subscribe(
    backend: Arc<JournalBackend>,
    run_id: Option<String>,
) -> futures::stream::BoxStream<'static, Envelope> {
    // 全局订阅的基线在**订阅建立时刻**采集（流是惰性的，首条 next 可能
    // 晚于订阅调用——那时再取基线会漏掉其间提交的事实）。durable_lsn 是
    // 同步读，这里的语义等价 v1 broadcast「注册接收器即从现在开始」。
    let start_lsn = match &run_id {
        None => backend.journal.durable_lsn(),
        Some(_) => 0,
    };
    futures::stream::unfold(
        Subscription {
            backend,
            run: run_id,
            cursor: None,
            tail: None,
            start_lsn,
            run_from_seq: 0,
            buffer: VecDeque::new(),
            attempts: HashMap::new(),
            terminal_seen: false,
            finished: false,
        },
        |mut state| async move {
            loop {
                if let Some(envelope) = state.buffer.pop_front() {
                    return Some((envelope, state));
                }
                if state.finished {
                    return None;
                }
                let more = state.poll().await;
                if !more && !state.finished && state.buffer.is_empty() {
                    tokio::time::sleep(SUBSCRIBE_POLL).await;
                }
            }
        },
    )
    .boxed()
}
