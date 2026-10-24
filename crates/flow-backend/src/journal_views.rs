//! Materialized product views of committed V2 state. No alternate command or event protocol.
//!
//! 本模块是产品面的物化读面：把已提交的当前行翻译成 DTO。
//! 它守护的核心不变量是「读面不产生新事实」——这里只有读，没有第二条
//! 写入路径，因此读到的状态与权威库严格一致。
//! 需要它，是因为产品面需要跨运行的聚合视图（列表、统计、时间线），
//! 而这些视图不能由 journal 的分页事实直接拼出，也不能另起一套协议。
use crate::journal::JournalBackend;
use crate::ViewError;
use chrono::{DateTime, Utc};
use flow_dto::{
    NodeTemplate, NodeTemplateSummary, RunRecord, RunStats, Schedule, Webhook, WorkflowRunStats,
    WorkflowSummary, WorkflowVersion, STATUS_DRAFT, STATUS_PUBLISHED,
};
use flow_engine::journal_state::{Run as JournalRun, WaitKind, Workflow as JournalWorkflow};
use flow_journal::VALUE_BUDGET;
use flow_journal::{StoredValue, MAX_LINE_BYTES};
use serde_json::{json, Value};
use std::collections::BTreeMap;
pub type Result<T> = std::result::Result<T, ViewError>;

/// 内部错误归类：读面故障统一为 Unavailable，与「不存在」「超限」区分开。
fn internal(error: impl std::fmt::Display) -> ViewError {
    ViewError::Unavailable(error.to_string())
}

/// 存储错误归类：响应过大单独成类，其余归为内部故障。
fn storage(error: crate::StorageError) -> ViewError {
    if matches!(error, crate::StorageError::ResponseTooLarge) {
        ViewError::Limit("RESPONSE_TOO_LARGE".into())
    } else {
        internal(error)
    }
}

/// 解析 RFC3339 时间戳并统一到 UTC。
fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|ts| ts.with_timezone(&Utc))
        .map_err(|e| internal(format!("invalid timestamp {raw}: {e}")))
}

/// 把一条已存版本记录物化成产品面 DTO（定义正文在此刻才读出）。
fn workflow_version(
    root: &std::path::Path,
    upper: u64,
    workflow_id: &str,
    version: &flow_engine::journal_state::DefinitionVersion,
) -> Result<WorkflowVersion> {
    Ok(WorkflowVersion {
        workflow_id: workflow_id.to_string(),
        version: version.version as i64,
        definition: flow_journal::value::materialize(
            root,
            upper,
            &version.definition,
            MAX_LINE_BYTES,
        )?,
        checksum: version.checksum.clone(),
        status: if version.published {
            STATUS_PUBLISHED.into()
        } else {
            STATUS_DRAFT.into()
        },
        created_at: parse_ts(&version.created_at)?,
    })
}

/// 读一个工作流的指定版本（缺省为最新已发布版本）。
pub async fn get_version(
    backend: &JournalBackend,
    workflow_id: &str,
    version: Option<i64>,
) -> Result<WorkflowVersion> {
    if version.is_some_and(|v| v < 1) {
        // 版本号从 1 开始：0 不是合法版本，报「不存在」比报错更贴近调用方预期。
        return Err(ViewError::NotFound(format!("版本不存在：{workflow_id}")));
    }
    let stored = backend
        .definition_version(workflow_id, version.map(|v| v as u64))
        .await
        .map_err(internal)?
        .ok_or_else(|| ViewError::NotFound(format!("版本不存在：{workflow_id}")))?;
    let root = backend.journal.root().to_path_buf();
    let upper = backend.journal.durable_lsn();
    let workflow_id = workflow_id.to_owned();
    tokio::task::spawn_blocking(move || workflow_version(&root, upper, &workflow_id, &stored))
        .await
        .map_err(internal)?
}

/// 查工作流最新的已发布版本号（无发布版本时返回 None）。
pub async fn latest_published(backend: &JournalBackend, workflow_id: &str) -> Result<Option<i64>> {
    Ok(backend
        .inspect(|state| {
            state
                .workflows
                .get(workflow_id)
                .and_then(|w| w.versions.iter().rev().find(|(_, v)| v.published))
                .map(|(v, _)| *v as i64)
        })
        .await)
}

/// 列出工作流的全部版本（含定义正文）。
pub async fn list_versions(
    backend: &JournalBackend,
    workflow_id: &str,
) -> Result<Vec<WorkflowVersion>> {
    let stored = backend
        .definition_versions(workflow_id)
        .await
        .map_err(definition_error)?;
    // 先按总量预留预算，再去读正文文件：避免读完才发现超限而浪费 IO。
    reserve_values(stored.iter().map(|v| &v.definition))?;
    let root = backend.journal.root().to_path_buf();
    let upper = backend.journal.durable_lsn();
    let workflow_id = workflow_id.to_owned();
    tokio::task::spawn_blocking(move || {
        stored
            .iter()
            .map(|v| workflow_version(&root, upper, &workflow_id, v))
            .collect()
    })
    .await
    .map_err(internal)?
}

/// Metadata reads never open definition value files or clone execution history.
pub async fn version_metadata(backend: &JournalBackend, workflow_id: &str) -> Result<Vec<Value>> {
    backend
        .definition_versions(workflow_id)
        .await
        .map_err(definition_error)?
        .into_iter()
        .map(|v| {
            Ok(json!({
                "version": v.version as i64,
                "status": if v.published { STATUS_PUBLISHED } else { STATUS_DRAFT },
                "checksum": v.checksum,
                "created_at": parse_ts(&v.created_at)?,
            }))
        })
        .collect()
}

/// 把后端错误映射成读面错误分类（NotFound / Limit / 其它）。
fn definition_error(error: crate::journal::JournalError) -> ViewError {
    match error {
        crate::journal::JournalError::Journal(flow_journal::Error::NotFound(message)) => {
            ViewError::NotFound(message)
        }
        crate::journal::JournalError::Journal(flow_journal::Error::Limit(_)) => {
            ViewError::Limit("RESPONSE_TOO_LARGE".into())
        }
        other => internal(other),
    }
}

/// Reserve the complete encoded business-value size before reading value files.
///
/// 预算必须在开文件之前一次性预留完：读了半个响应才发现超限，
/// 已经消耗的 IO 与内存都白费，而且响应可能已经被部分写出。
fn reserve_values<'a>(values: impl IntoIterator<Item = &'a StoredValue>) -> Result<()> {
    let mut remaining = VALUE_BUDGET;
    for value in values {
        let size = match value {
            StoredValue::Ref(r) => usize::try_from(r.total_bytes).map_err(internal)?,
            StoredValue::Inline(v) => flow_journal::codec::bounded_json(v, remaining)?.len(),
        };
        remaining = remaining
            .checked_sub(size)
            .ok_or_else(|| ViewError::Limit("RESPONSE_TOO_LARGE".into()))?;
    }
    Ok(())
}

/// 把工作流当前行翻译成摘要 DTO（不读定义正文）。
fn workflow_summary(value: &Value) -> Result<WorkflowSummary> {
    let workflow: JournalWorkflow = serde_json::from_value(value.clone()).map_err(internal)?;
    Ok(WorkflowSummary {
        workflow_id: workflow.workflow_id.clone(),
        name: workflow.name.clone(),
        latest_version: value["latest_version"].as_u64().unwrap_or(0) as i64,
        published_version: value["published_version"].as_u64().map(|v| v as i64),
        created_at: parse_ts(&workflow.created_at)?,
    })
}

/// 列出全部未删除的工作流摘要。
pub async fn list_workflows(backend: &JournalBackend) -> Result<Vec<WorkflowSummary>> {
    backend
        .store
        .ordered("workflow", None, false)
        .await
        .map_err(storage)?
        .into_iter()
        // 已删除的工作流不出现在产品列表里，但行仍保留在权威库中。
        .filter(|v| v["deleted"] != true)
        .map(|v| workflow_summary(&v))
        .collect()
}

/// 把一个 run 的当前记录翻译成产品面 DTO（含输入/输出正文）。
fn run_record(root: &std::path::Path, upper: u64, run: &JournalRun) -> Result<RunRecord> {
    let materialize =
        |value: &StoredValue| flow_journal::value::materialize(root, upper, value, VALUE_BUDGET);
    Ok(RunRecord {
        id: run.run_id.clone(),
        workflow_id: run.workflow_id.clone(),
        workflow_version: run.workflow_version as i64,
        status: run.status,
        input: materialize(&run.input)?,
        output: run.output.as_ref().map(materialize).transpose()?,
        error: run.error.clone(),
        source: run.source.clone(),
        source_detail: run.source_detail.clone(),
        started_at: parse_ts(&run.created_at)?,
        // journal 不记录终态墙钟（权威时间是 LSN/run_seq）——ended_at 无从
        // 取证，不编造。前端时长列以 created_at 为准。
        ended_at: None,
    })
}

/// 读单个 run 的产品面记录。
pub async fn get_run(backend: &JournalBackend, run_id: &str) -> Result<RunRecord> {
    let (_, value) = backend
        .store
        .get_summary("run", run_id)
        .await
        .map_err(storage)?;
    let run = serde_json::from_value(
        value.ok_or_else(|| ViewError::NotFound(format!("run 不存在：{run_id}")))?,
    )
    .map_err(internal)?;
    Ok(materialize_runs(backend, vec![run]).await?.remove(0))
}

/// 批量物化 run 的输入/输出正文。
///
/// 先把整批的编码后尺寸算清楚，超预算就直接拒绝；正文读取放在
/// blocking 任务里，避免阻塞异步运行时。
async fn materialize_runs(
    backend: &JournalBackend,
    runs: Vec<JournalRun>,
) -> Result<Vec<RunRecord>> {
    // Account for the entire response before allocating any referenced business values.
    let mut bytes = 0usize;
    for run in &runs {
        for value in std::iter::once(&run.input).chain(run.output.iter()) {
            let n = match value {
                StoredValue::Ref(r) => usize::try_from(r.total_bytes).map_err(internal)?,
                StoredValue::Inline(v) => flow_journal::codec::bounded_json(v, VALUE_BUDGET)?.len(),
            };
            bytes = bytes
                .checked_add(n)
                .ok_or_else(|| ViewError::Limit("RESPONSE_TOO_LARGE".into()))?;
            if bytes > VALUE_BUDGET {
                return Err(ViewError::Limit("RESPONSE_TOO_LARGE".into()));
            }
        }
    }
    let root = backend.journal.root().to_path_buf();
    let upper = backend.journal.durable_lsn();
    tokio::task::spawn_blocking(move || runs.iter().map(|r| run_record(&root, upper, r)).collect())
        .await
        .map_err(internal)?
}

/// 按工作流/状态/来源分页列出 run。
pub async fn list_runs(
    backend: &JournalBackend,
    workflow_id: Option<&str>,
    status: Option<&str>,
    source: Option<&str>,
    before_run_id: Option<&str>,
    limit: i64,
) -> Result<Vec<RunRecord>> {
    let (_, values) = backend
        .store
        .runs_page(flow_store::store::RunsQuery {
            workflow_id,
            status,
            source,
            before_run_id,
            limit: limit.max(0) as usize,
        })
        .await
        .map_err(storage)?;
    materialize_runs(
        backend,
        values
            .into_iter()
            .map(serde_json::from_value)
            .collect::<std::result::Result<_, _>>()
            .map_err(internal)?,
    )
    .await
}

/// 统计 run 数量（按状态与工作流分组）。
pub async fn run_stats(backend: &JournalBackend, workflow_id: Option<&str>) -> Result<RunStats> {
    let mut stats = RunStats {
        total: 0,
        by_status: BTreeMap::new(),
        by_workflow: Vec::new(),
    };
    let mut grouped: BTreeMap<String, WorkflowRunStats> = BTreeMap::new();
    for (workflow, status, count) in backend
        .store
        .run_counts(workflow_id)
        .await
        .map_err(storage)?
    {
        stats.total += count;
        *stats.by_status.entry(status.clone()).or_insert(0) += count;
        if workflow_id.is_none() {
            let entry = grouped.entry(workflow.clone()).or_insert(WorkflowRunStats {
                workflow_id: workflow,
                total: 0,
                by_status: BTreeMap::new(),
            });
            entry.total += count;
            *entry.by_status.entry(status).or_insert(0) += count;
        }
    }
    stats.by_workflow = grouped.into_values().collect();
    Ok(stats)
}

/// 判断 run 是否仍在进行（未终态）。
pub async fn is_live(backend: &JournalBackend, run_id: &str) -> bool {
    backend
        .inspect(|state| state.runs.get(run_id).is_some_and(|run| !run.terminal()))
        .await
}

/// 把定时器配置行翻译成 DTO。
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

/// 把 webhook 配置行翻译成 DTO。
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

/// 列出定时器配置（可按工作流过滤）。
pub async fn list_schedules(
    backend: &JournalBackend,
    workflow_id: Option<&str>,
) -> Result<Vec<Schedule>> {
    backend
        .store
        .ordered("schedule", workflow_id, false)
        .await
        .map_err(storage)?
        .iter()
        .map(schedule_from_config)
        .collect()
}

/// 列出 webhook 配置（可按工作流过滤）。
pub async fn list_webhooks(
    backend: &JournalBackend,
    workflow_id: Option<&str>,
) -> Result<Vec<Webhook>> {
    backend
        .store
        .ordered("webhook", workflow_id, false)
        .await
        .map_err(storage)?
        .iter()
        .map(webhook_from_config)
        .collect()
}

/// 把模板配置行翻译成完整 DTO（含节点/边片段）。
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

/// 列出节点模板摘要（列表视图不读完整片段）。
pub async fn template_list(backend: &JournalBackend) -> Result<Vec<NodeTemplateSummary>> {
    let configs = backend
        .store
        .ordered("template", None, true)
        .await
        .map_err(storage)?;
    configs
        .iter()
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
        .collect()
}

/// 读单个节点模板的完整内容。
pub async fn template_get(backend: &JournalBackend, id: &str) -> Result<NodeTemplate> {
    backend
        .inspect(|state| {
            let config = state
                .templates
                .get(id)
                .ok_or_else(|| ViewError::NotFound(format!("节点模板不存在：{id}")))?;
            template_from_config(config)
        })
        .await
}

/// One committed-state snapshot supplies status, node states, waiting and child relationships.
pub async fn timeline(backend: &JournalBackend, run_id: &str) -> Result<Value> {
    let (lsn, run) = backend
        .run_snapshot_with_cursor(run_id)
        .await
        .map_err(internal)?;
    let run = run.ok_or_else(|| ViewError::NotFound(format!("run 不存在：{run_id}")))?;
    let children = backend.run_children(run_id, lsn).await.map_err(internal)?;
    reserve_values(
        std::iter::once(&run.definition)
            .chain(run.output.iter())
            .chain(
                run.nodes
                    .values()
                    .flat_map(|n| n.output.iter().chain(n.prepared.iter().map(|p| &p.params))),
            ),
    )?;
    let root = backend.journal.root().to_path_buf();
    let upper = backend.journal.durable_lsn();
    let journal_id = backend.journal.id().to_owned();
    tokio::task::spawn_blocking(move || {
    let materialize = |v: &StoredValue| flow_journal::value::materialize(&root, upper, v, VALUE_BUDGET).map_err(internal);
    let definition: flow_engine::Definition =
        serde_json::from_value(materialize(&run.definition)?)
            .map_err(internal)?;
    let mut nodes = Vec::new();
    for spec in &definition.nodes {
        let node = run.nodes.get(&spec.id);
        let child = children.get(&spec.id);
        let status = node.map_or("pending", |n| n.status.as_str());
        // 展示态与业务状态分开：retrying 与 running 在产品面要区分，
        // 但底层状态都还是 waiting。
        let display_state = match status {
            "succeeded" => "completed",
            "waiting" => {
                if node
                    .and_then(|n| n.wait.as_ref())
                    .is_some_and(|w| w.kind() == WaitKind::Retry)
                {
                    "retrying"
                } else {
                    "running"
                }
            }
            other => other,
        };
        let output = node
            .and_then(|n| n.output.as_ref())
            // 节点正文可能含密钥等敏感内容，展示前统一脱敏。
            .map(|v| materialize(v).map(|v| flow_engine::nodelog::redact_snapshot(&v)))
            .transpose()?;
        let input = node
            .and_then(|n| n.prepared.as_ref())
            .map(|p| {
                materialize(&p.params).map(|v| flow_engine::nodelog::redact_snapshot(&v))
            })
            .transpose()?;
        nodes.push(json!({"id":spec.id,"name":spec.name,"type":spec.node_type,"state":display_state,
            "attempts":node.map_or(0, |n| n.attempt),"started_at":null,"ended_at":null,"duration_ms":null,
            "input":input,"output":output,"error":node.and_then(|n| n.error.as_ref()).map(|e| flow_engine::nodelog::redact_text(e)),
            "reason":node.and_then(|n| n.skip_reason.as_ref()),
            "wait":node.and_then(|n| n.wait.as_ref()),
            "operation_id":node.and_then(|n| n.operation.as_ref()).map(|o| &o.operation_id),
            "child_run_id":child}));
    }
    Ok(
        json!({"run_id":run.run_id,"status":run.status,"workflow_id":run.workflow_id,
        "workflow_version":run.workflow_version,"started_at":run.created_at,"ended_at":null,
        "output":run.output.as_ref().map(materialize).transpose()?,
        "fatal_error":run.error.as_ref().map(|e| flow_engine::nodelog::redact_text(e)),"last_seq":run.last_run_seq,"nodes":nodes,
        "snapshot_cursor":{"journal_id":journal_id,"lsn":lsn.to_string()}}),
    )
    }).await.map_err(internal)?
}

#[cfg(test)]
mod budget_tests {
    use super::*;
    /// 累计预算必须在开文件之前判定：两个半预算的值也必须被拒绝。
    #[test]
    fn cumulative_values_are_rejected_before_opening_files() {
        let value = StoredValue::Ref(flow_journal::value::ValueRef {
            journal_id: "j".into(),
            output_id: "absent".into(),
            codec: flow_journal::value::ValueCodec::Json,
            version: 1,
            chunk_count: 1,
            total_bytes: VALUE_BUDGET as u64 / 2 + 1,
            digest: "a".repeat(64),
        });
        assert!(reserve_values([&value]).is_ok());
        assert!(reserve_values([&value, &value])
            .unwrap_err()
            .to_string()
            .contains("RESPONSE_TOO_LARGE"));
    }
}
