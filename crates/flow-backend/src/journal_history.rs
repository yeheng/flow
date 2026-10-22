//! Current-state loading and entity changes for one atomic SQLite commit.
use crate::journal::{audit_rows, project_rows, JournalError};
use flow_engine::journal_state::{Run, State};
use flow_journal::{EventKind, StoredValue, Transaction, ValueRef};
use flow_store::store::{EntityChange, Store};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
type Result<T> = std::result::Result<T, JournalError>;
fn db(e: flow_store::store::Error) -> JournalError {
    JournalError::Storage(e.to_string())
}

/// Normal recovery loads current control metadata and active executions only.
/// Accepted history, completed executions and old attempts are never replayed here.
pub(crate) async fn load(index: &Store, identity: &str, upper: u64) -> Result<State> {
    if upper > 0 && !index.domain_mode().await.map_err(db)? {
        return Err(JournalError::Storage(
            "fact-only database is not a coordinator database".into(),
        ));
    }
    let mut state = State {
        version: 1,
        journal_id: identity.into(),
        applied_lsn: upper,
        compact_audits: true,
        ..Default::default()
    };
    for kind in [
        "workflow",
        "schedule",
        "webhook",
        "template",
        "legacy",
        "schedule_fire",
        "run_workflow",
    ] {
        let mut after = String::new();
        loop {
            let keys = index
                .current_keys(kind, None, &after, upper, 256)
                .await
                .map_err(db)?;
            if keys.is_empty() {
                break;
            }
            for key in &keys {
                let value = index
                    .get_summary(kind, key)
                    .await
                    .map_err(db)?
                    .1
                    .ok_or_else(|| {
                        JournalError::Storage("missing current control entity".into())
                    })?;
                match kind {
                    "workflow" => {
                        let mut workflow: flow_engine::journal_state::Workflow =
                            serde_json::from_value(value)?;
                        let mut after_version = String::new();
                        loop {
                            let versions = index
                                .current_keys(
                                    "workflow_version",
                                    Some(key),
                                    &after_version,
                                    upper,
                                    256,
                                )
                                .await
                                .map_err(db)?;
                            if versions.is_empty() {
                                break;
                            }
                            for version in &versions {
                                let row = index
                                    .current_at("workflow_version", version, upper)
                                    .await
                                    .map_err(db)?
                                    .ok_or_else(|| {
                                        JournalError::Storage("missing definition version".into())
                                    })?;
                                let version: flow_engine::journal_state::DefinitionVersion =
                                    serde_json::from_value(row)?;
                                workflow.versions.insert(version.version, version);
                            }
                            after_version = versions.last().unwrap().clone();
                        }
                        state.workflows.insert(key.clone(), workflow);
                    }
                    "schedule" => {
                        state.schedules.insert(key.clone(), value);
                    }
                    "webhook" => {
                        state.webhooks.insert(key.clone(), value);
                    }
                    "template" => {
                        state.templates.insert(key.clone(), value);
                    }
                    "legacy" => {
                        state.legacy.insert(key.clone(), value);
                    }
                    "schedule_fire" => {
                        state
                            .schedule_fires
                            .insert(key.clone(), serde_json::from_value(value)?);
                    }
                    "run_workflow" => {
                        state.run_workflows.insert(key.clone());
                    }
                    _ => unreachable!(),
                }
            }
            after = keys.last().unwrap().clone();
        }
    }
    if let Some(meta) = index.get("engine", "epoch").await.map_err(db)?.1 {
        state.master_epoch = meta["epoch"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| JournalError::Storage("invalid master epoch".into()))?;
    }
    let mut after = String::new();
    loop {
        let ids = index.active_run_ids(&after).await.map_err(db)?;
        if ids.is_empty() {
            break;
        }
        for id in &ids {
            if state.runs.len() >= crate::journal::R_MAX {
                return Err(JournalError::Storage("active run capacity exceeded".into()));
            }
            if let Some(run) = run_at(index, id, upper, false).await? {
                state.runs.insert(id.clone(), run);
            }
        }
        after = ids.last().unwrap().clone();
    }
    Ok(state)
}

pub(crate) async fn run_at(index: &Store, id: &str, upper: u64, full: bool) -> Result<Option<Run>> {
    let (lsn, value) = if full {
        index.get("run", id).await.map_err(db)?
    } else {
        index.get_execution(id).await.map_err(db)?
    };
    if lsn != upper {
        return Err(JournalError::Storage(
            "current run read crossed decision boundary".into(),
        ));
    }
    value
        .map(serde_json::from_value)
        .transpose()
        .map_err(JournalError::from)
}

/// Load cold current objects before preview, while the coordinator owns the decision lock.
pub(crate) async fn prepare(index: &Store, state: &mut State, tx: &Transaction) -> Result<()> {
    let upper = state.applied_lsn;
    let ids: BTreeSet<_> = tx
        .events
        .iter()
        .filter_map(|e| e.run_id.as_deref())
        .collect();
    for id in ids {
        if !state.runs.contains_key(id) {
            if let Some(run) = run_at(index, id, upper, false).await? {
                state.runs.insert(id.into(), run);
            }
        }
    }
    let mut descriptors = BTreeMap::<String, Option<ValueRef>>::new();
    fn refs(value: &Value, found: &mut BTreeMap<String, Option<ValueRef>>) {
        if value.get("journal_id").is_some()
            && value.get("output_id").is_some()
            && value.get("digest").is_some()
        {
            if let Ok(reference) = serde_json::from_value::<ValueRef>(value.clone()) {
                found.insert(reference.output_id.clone(), Some(reference));
                return;
            }
        }
        if value.get("type").and_then(Value::as_str) == Some("ref") {
            if let Ok(StoredValue::Ref(r)) = serde_json::from_value(value.clone()) {
                found.insert(r.output_id.clone(), Some(r));
                return;
            }
        }
        match value {
            Value::Object(m) => {
                for v in m.values() {
                    refs(v, found);
                }
            }
            Value::Array(a) => {
                for v in a {
                    refs(v, found);
                }
            }
            _ => {}
        }
    }
    let fresh: BTreeSet<_> = tx
        .events
        .iter()
        .filter(|e| e.kind == EventKind::DispatchStarted)
        .filter_map(|e| e.dispatch_id.as_deref())
        .collect();
    for event in &tx.events {
        if let (Some(run), Some(node), Some(dispatch)) = (
            event.run_id.as_deref(),
            event
                .node_id
                .as_deref()
                .or_else(|| event.payload["node_id"].as_str()),
            event.dispatch_id.as_deref(),
        ) {
            if let Some(node_state) = state.runs.get_mut(run).and_then(|r| r.nodes.get_mut(node)) {
                if !fresh.contains(dispatch) && !node_state.attempts.contains_key(dispatch) {
                    let key = serde_json::to_string(&(run, node, dispatch))?;
                    if let Some(value) = index
                        .current_at("run_attempt", &key, upper)
                        .await
                        .map_err(db)?
                    {
                        node_state
                            .attempts
                            .insert(dispatch.into(), serde_json::from_value(value)?);
                    }
                }
            }
        }
        refs(&event.payload, &mut descriptors);
    }
    for (id, expected) in descriptors {
        if let Some(expected) = &expected {
            if state
                .values
                .check(&StoredValue::Ref(expected.clone()))
                .is_ok()
            {
                continue;
            }
        }
        if let Some(record) = index.get("published", &id).await.map_err(db)?.1 {
            let lsn = record["lsn"]
                .as_str()
                .and_then(|n| n.parse::<u64>().ok())
                .ok_or_else(|| JournalError::Storage("invalid publication prefix".into()))?;
            if lsn <= upper {
                let descriptor: ValueRef = serde_json::from_value(record["value"].clone())?;
                // A mismatched reference remains a reducer error; user JSON can also contain
                // objects that happen to look like descriptors, so do not reject it here.
                if expected.as_ref().is_none_or(|r| r == &descriptor) {
                    state.values.remember(descriptor)?;
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn index_rows(
    state: &State,
    tx: &Transaction,
) -> flow_journal::Result<Vec<EntityChange>> {
    let mut rows = audit_rows(tx)?;
    rows.extend(project_rows(state, tx)?);
    for event in &tx.events {
        if event.kind == EventKind::Command {
            let mut record: flow_engine::journal_state::CommandRecord =
                serde_json::from_value(event.payload.clone())?;
            record.lsn = tx.lsn;
            if let Some(id) = &record.request_id {
                rows.push(EntityChange {
                    kind: "command".into(),
                    key: flow_engine::journal_state::command_key(&record.scope, id),
                    value: Some(serde_json::to_value(record)?),
                });
            }
        }
        if event.kind == EventKind::MasterEpochStarted {
            rows.push(EntityChange {
                kind: "engine".into(),
                key: "epoch".into(),
                value: Some(json!({"epoch":state.master_epoch.to_string()})),
            });
        }
        if event.kind == EventKind::ScheduleChanged {
            if let Some(id) = event.payload["id"].as_str() {
                rows.push(EntityChange {
                    kind: "schedule_fire".into(),
                    key: id.into(),
                    value: state
                        .schedule_fires
                        .get(id)
                        .map(serde_json::to_value)
                        .transpose()?,
                });
            }
        }
        if event.kind == EventKind::Command {
            if let Some(key) = event.payload["scope"]
                .as_str()
                .and_then(|s| s.strip_prefix("run.start:schedule:"))
            {
                if let Some(fire) = state.schedule_fires.get(key) {
                    rows.push(EntityChange {
                        kind: "schedule_fire".into(),
                        key: key.into(),
                        value: Some(serde_json::to_value(fire)?),
                    });
                }
            }
        }
        if event.kind == EventKind::RunStarted {
            if let Some(workflow) = event.payload["workflow_id"].as_str() {
                rows.push(EntityChange {
                    kind: "run_workflow".into(),
                    key: workflow.into(),
                    value: Some(json!({"workflow_id":workflow})),
                });
            }
            if let (Some(parent), Some(node), Some(child)) = (
                event.payload["parent"]["run_id"].as_str(),
                event.payload["parent"]["node_id"].as_str(),
                event.run_id.as_deref(),
            ) {
                rows.push(EntityChange {
                    kind: "child".into(),
                    key: serde_json::to_string(&(parent, node, child))?,
                    value: Some(json!({"lsn":format!("{:020}",tx.lsn)})),
                });
            }
        }
        if event.kind == EventKind::DispatchStarted {
            if let (Some(dispatch), Some(run), Some(node)) =
                (&event.dispatch_id, &event.run_id, &event.node_id)
            {
                rows.push(EntityChange {
                    kind: "dispatch".into(),
                    key: dispatch.clone(),
                    value: Some(
                        json!({"run_id":run,"node_id":node,"attempt":event.payload["attempt"]}),
                    ),
                });
            }
        }
    }
    Ok(rows)
}

pub(crate) fn trim(state: &mut State) {
    state.runs.retain(|_, run| !run.terminal());
    for run in state.runs.values_mut() {
        for node in run.nodes.values_mut() {
            node.attempts.retain(|id, _| *id == node.dispatch_id);
        }
    }
    state.values.trim_cache();
}
