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

fn materialize(backend: &JournalBackend, stored: &StoredValue, limit: usize) -> Result<Value> {
    flow_journal::value::materialize(
        backend.journal.root(),
        backend.journal.durable_lsn(),
        stored,
        limit,
    )
    .map_err(|e| internal(format!("value materialization: {e}")))
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

pub async fn get_version(
    backend: &JournalBackend,
    workflow_id: &str,
    version: Option<i64>,
) -> Result<WorkflowVersion> {
    let state = backend.state().await;
    let workflow = state
        .workflows
        .get(workflow_id)
        .ok_or_else(|| ViewError::WorkflowNotFound(workflow_id.to_string()))?;
    match version {
        Some(v) => {
            let stored = workflow
                .versions
                .get(&(v as u64))
                .ok_or_else(|| ViewError::VersionNotFound(workflow_id.to_string(), v))?;
            workflow_version(backend, workflow_id, stored)
        }
        None => {
            let (_, latest) = workflow
                .versions
                .last_key_value()
                .ok_or_else(|| ViewError::WorkflowNotFound(workflow_id.to_string()))?;
            workflow_version(backend, workflow_id, latest)
        }
    }
}

pub async fn latest_published(backend: &JournalBackend, workflow_id: &str) -> Result<Option<i64>> {
    Ok(backend
        .state()
        .await
        .workflows
        .get(workflow_id)
        .and_then(|w| w.versions.iter().rev().find(|(_, v)| v.published))
        .map(|(v, _)| *v as i64))
}

pub async fn list_versions(
    backend: &JournalBackend,
    workflow_id: &str,
) -> Result<Vec<WorkflowVersion>> {
    let state = backend.state().await;
    let workflow = state
        .workflows
        .get(workflow_id)
        .ok_or_else(|| ViewError::WorkflowNotFound(workflow_id.to_string()))?;
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

pub async fn list_workflows(backend: &JournalBackend) -> Result<Vec<WorkflowSummary>> {
    backend
        .projection
        .ordered("workflow", None, false)
        .await
        .map_err(internal)?
        .into_iter()
        .map(|v| workflow_summary(&serde_json::from_value(v).map_err(internal)?))
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
        .projection
        .get("run", run_id)
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
        .projection
        .runs_page(flow_store::projection::RunsQuery {
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
        .projection
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
        .projection
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
        .projection
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
        .projection
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
    let state = backend.state().await;
    let config = state
        .templates
        .get(id)
        .ok_or_else(|| ViewError::TemplateNotFound(id.to_string()))?;
    template_from_config(config)
}

/// One committed-state snapshot supplies status, node states, waiting and child relationships.
pub async fn timeline(backend: &JournalBackend, run_id: &str) -> Result<Value> {
    let state = backend.state().await;
    let run = state
        .runs
        .get(run_id)
        .ok_or_else(|| ViewError::RunNotFound(run_id.into()))?;
    let definition: flow_engine::Definition =
        serde_json::from_value(materialize(backend, &run.definition, VALUE_BUDGET)?)
            .map_err(internal)?;
    let mut nodes = Vec::new();
    for spec in &definition.nodes {
        let node = run.nodes.get(&spec.id);
        let child = state.runs.values().find(|child| {
            child
                .parent
                .as_ref()
                .is_some_and(|p| p.run_id == run.run_id && p.node_id == spec.id)
        });
        let status = node.map_or("pending", |n| n.status.as_str());
        let display_state = match status {
            "succeeded" => "completed",
            "waiting" => {
                if node
                    .and_then(|n| n.wait.as_ref())
                    .is_some_and(|w| w.kind == WaitKind::Retry)
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
            .map(|v| materialize(backend, v, VALUE_BUDGET).map(|v| flow_engine::redact_value(&v)))
            .transpose()?;
        let input = node
            .and_then(|n| n.prepared.as_ref())
            .map(|p| {
                materialize(backend, &p.params, VALUE_BUDGET).map(|v| flow_engine::redact_value(&v))
            })
            .transpose()?;
        nodes.push(json!({"id":spec.id,"name":spec.name,"type":spec.node_type,"state":display_state,
            "attempts":node.map_or(0, |n| n.attempt),"started_at":null,"ended_at":null,"duration_ms":null,
            "input":input,"output":output,"error":node.and_then(|n| n.error.as_ref()),
            "reason":node.and_then(|n| n.skip_reason.as_ref()),
            "wait":node.and_then(|n| n.wait.as_ref()),
            "operation_id":node.and_then(|n| n.operation.as_ref()).map(|o| &o.operation_id),
            "child_run_id":child.map(|c| &c.run_id)}));
    }
    Ok(
        json!({"run_id":run.run_id,"status":run.status,"workflow_id":run.workflow_id,
        "workflow_version":run.workflow_version,"started_at":run.created_at,"ended_at":null,
        "output":run.output.as_ref().map(|v| materialize(backend,v,VALUE_BUDGET)).transpose()?,
        "fatal_error":run.error,"last_seq":run.last_run_seq,"nodes":nodes,
        "snapshot_cursor":{"journal_id":backend.journal.id(),"lsn":state.applied_lsn.to_string()}}),
    )
}
