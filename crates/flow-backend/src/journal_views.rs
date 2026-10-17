//! Materialized product views of committed V2 state. No alternate command or event protocol.
use crate::journal::JournalBackend;
use crate::ViewError;
use chrono::{DateTime, Utc};
use flow_dto::{
    NodeTemplate, NodeTemplateSummary, RunRecord, RunStats, Schedule, Webhook, WorkflowRunStats,
    WorkflowSummary, WorkflowVersion, STATUS_DRAFT, STATUS_PUBLISHED,
};
use flow_engine::journal_state::{Run as JournalRun, WaitKind, Workflow as JournalWorkflow};
use flow_journal::{StoredValue, MAX_LINE_BYTES};
use serde_json::{json, Value};
use std::collections::BTreeMap;
const VALUE_BUDGET: usize = 8 * 1024 * 1024;
pub type Result<T> = std::result::Result<T, ViewError>;
fn internal(error: impl std::fmt::Display) -> ViewError {
    ViewError::Internal(error.to_string())
}
fn parse_ts(raw: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|ts| ts.with_timezone(&Utc))
        .map_err(|e| internal(format!("invalid timestamp {raw}: {e}")))
}

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
        )
        .map_err(internal)?,
        checksum: version.checksum.clone(),
        status: if version.published {
            STATUS_PUBLISHED.into()
        } else {
            STATUS_DRAFT.into()
        },
        created_at: parse_ts(&version.created_at)?,
    })
}

pub async fn get_version(
    backend: &JournalBackend,
    workflow_id: &str,
    version: Option<i64>,
) -> Result<WorkflowVersion> {
    let stored = backend
        .inspect(|state| {
            let workflow = state
                .workflows
                .get(workflow_id)
                .ok_or_else(|| ViewError::WorkflowNotFound(workflow_id.into()))?;
            match version {
                Some(v) => workflow
                    .versions
                    .get(&(v as u64))
                    .cloned()
                    .ok_or_else(|| ViewError::VersionNotFound(workflow_id.into(), v)),
                None => workflow
                    .versions
                    .last_key_value()
                    .map(|(_, v)| v.clone())
                    .ok_or_else(|| ViewError::WorkflowNotFound(workflow_id.into())),
            }
        })
        .await?;
    let root = backend.journal.root().to_path_buf();
    let upper = backend.journal.durable_lsn();
    let workflow_id = workflow_id.to_owned();
    tokio::task::spawn_blocking(move || workflow_version(&root, upper, &workflow_id, &stored))
        .await
        .map_err(internal)?
}

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

pub async fn list_versions(
    backend: &JournalBackend,
    workflow_id: &str,
) -> Result<Vec<WorkflowVersion>> {
    let stored = backend
        .inspect(|state| {
            state
                .workflows
                .get(workflow_id)
                .map(|w| w.versions.values().rev().cloned().collect::<Vec<_>>())
                .ok_or_else(|| ViewError::WorkflowNotFound(workflow_id.into()))
        })
        .await?;
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
        .inspect(|state| {
            let workflow = state
                .workflows
                .get(workflow_id)
                .ok_or_else(|| ViewError::WorkflowNotFound(workflow_id.into()))?;
            workflow
                .versions
                .values()
                .rev()
                .map(|v| {
                    Ok(json!({
                        "version": v.version as i64,
                        "status": if v.published { STATUS_PUBLISHED } else { STATUS_DRAFT },
                        "checksum": v.checksum,
                        "created_at": parse_ts(&v.created_at)?,
                    }))
                })
                .collect()
        })
        .await
}

/// Reserve the complete encoded business-value size before reading value files.
fn reserve_values<'a>(values: impl IntoIterator<Item = &'a StoredValue>) -> Result<()> {
    let mut remaining = VALUE_BUDGET;
    for value in values {
        let size = match value {
            StoredValue::Ref(r) => usize::try_from(r.total_bytes).map_err(internal)?,
            StoredValue::Inline(v) => flow_journal::codec::bounded_json(v, remaining)
                .map_err(internal)?
                .len(),
        };
        remaining = remaining
            .checked_sub(size)
            .ok_or_else(|| internal("RESPONSE_TOO_LARGE"))?;
    }
    Ok(())
}

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

pub async fn list_workflows(backend: &JournalBackend) -> Result<Vec<WorkflowSummary>> {
    backend
        .store
        .ordered("workflow", None, false)
        .await
        .map_err(internal)?
        .into_iter()
        .map(|v| workflow_summary(&v))
        .collect()
}

fn run_record(root: &std::path::Path, upper: u64, run: &JournalRun) -> Result<RunRecord> {
    let materialize = |value: &StoredValue| {
        flow_journal::value::materialize(root, upper, value, VALUE_BUDGET).map_err(internal)
    };
    Ok(RunRecord {
        id: run.run_id.clone(),
        workflow_id: run.workflow_id.clone(),
        workflow_version: run.workflow_version as i64,
        status: run.status.as_str().to_owned(),
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

pub async fn get_run(backend: &JournalBackend, run_id: &str) -> Result<RunRecord> {
    let (_, value) = backend
        .store
        .get_summary("run", run_id)
        .await
        .map_err(internal)?;
    let run =
        serde_json::from_value(value.ok_or_else(|| ViewError::RunNotFound(run_id.to_owned()))?)
            .map_err(internal)?;
    Ok(materialize_runs(backend, vec![run]).await?.remove(0))
}

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
                StoredValue::Inline(v) => flow_journal::codec::bounded_json(v, VALUE_BUDGET)
                    .map_err(internal)?
                    .len(),
            };
            bytes = bytes
                .checked_add(n)
                .ok_or_else(|| internal("RESPONSE_TOO_LARGE"))?;
            if bytes > VALUE_BUDGET {
                return Err(internal("RESPONSE_TOO_LARGE"));
            }
        }
    }
    let root = backend.journal.root().to_path_buf();
    let upper = backend.journal.durable_lsn();
    tokio::task::spawn_blocking(move || runs.iter().map(|r| run_record(&root, upper, r)).collect())
        .await
        .map_err(internal)?
}

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
        .map_err(internal)?;
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
        .map_err(internal)?
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

pub async fn is_live(backend: &JournalBackend, run_id: &str) -> bool {
    backend
        .inspect(|state| state.runs.get(run_id).is_some_and(|run| !run.terminal()))
        .await
}

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

pub async fn list_schedules(
    backend: &JournalBackend,
    workflow_id: Option<&str>,
) -> Result<Vec<Schedule>> {
    backend
        .store
        .ordered("schedule", workflow_id, false)
        .await
        .map_err(internal)?
        .iter()
        .map(schedule_from_config)
        .collect()
}

pub async fn list_webhooks(
    backend: &JournalBackend,
    workflow_id: Option<&str>,
) -> Result<Vec<Webhook>> {
    backend
        .store
        .ordered("webhook", workflow_id, false)
        .await
        .map_err(internal)?
        .iter()
        .map(webhook_from_config)
        .collect()
}

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

pub async fn template_list(backend: &JournalBackend) -> Result<Vec<NodeTemplateSummary>> {
    let configs = backend
        .store
        .ordered("template", None, true)
        .await
        .map_err(internal)?;
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

pub async fn template_get(backend: &JournalBackend, id: &str) -> Result<NodeTemplate> {
    backend
        .inspect(|state| {
            let config = state
                .templates
                .get(id)
                .ok_or_else(|| ViewError::TemplateNotFound(id.into()))?;
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
    let run = run.ok_or_else(|| ViewError::RunNotFound(run_id.into()))?;
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
            .map(|v| materialize(v).map(|v| flow_engine::redact_value(&v)))
            .transpose()?;
        let input = node
            .and_then(|n| n.prepared.as_ref())
            .map(|p| {
                materialize(&p.params).map(|v| flow_engine::redact_value(&v))
            })
            .transpose()?;
        nodes.push(json!({"id":spec.id,"name":spec.name,"type":spec.node_type,"state":display_state,
            "attempts":node.map_or(0, |n| n.attempt),"started_at":null,"ended_at":null,"duration_ms":null,
            "input":input,"output":output,"error":node.and_then(|n| n.error.as_ref()),
            "reason":node.and_then(|n| n.skip_reason.as_ref()),
            "wait":node.and_then(|n| n.wait.as_ref()),
            "operation_id":node.and_then(|n| n.operation.as_ref()).map(|o| &o.operation_id),
            "child_run_id":child}));
    }
    Ok(
        json!({"run_id":run.run_id,"status":run.status,"workflow_id":run.workflow_id,
        "workflow_version":run.workflow_version,"started_at":run.created_at,"ended_at":null,
        "output":run.output.as_ref().map(materialize).transpose()?,
        "fatal_error":run.error,"last_seq":run.last_run_seq,"nodes":nodes,
        "snapshot_cursor":{"journal_id":journal_id,"lsn":lsn.to_string()}}),
    )
    }).await.map_err(internal)?
}

#[cfg(test)]
mod budget_tests {
    use super::*;
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
