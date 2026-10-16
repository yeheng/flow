//! Versioned, disposable execution index. Journal facts remain the sole authority.
use crate::journal::{audit_rows, project_rows, JournalError};
use flow_engine::journal_state::{Run, State};
use flow_journal::{EventKind, StoredValue, Transaction, ValueRef};
use flow_store::projection::{ProjectedRow, Projector};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
type Result<T> = std::result::Result<T, JournalError>;
fn db(e: sqlx::Error) -> JournalError {
    JournalError::Projection(e.to_string())
}

pub(crate) async fn run_at(
    index: &Projector,
    id: &str,
    upper: u64,
    full: bool,
) -> Result<Option<Run>> {
    let Some(value) = index.history_at("run", id, upper).await.map_err(db)? else {
        return Ok(None);
    };
    let mut run: Run = serde_json::from_value(value)?;
    let mut after = String::new();
    loop {
        let keys = index
            .history_keys("run_node", Some(id), &after, upper, 256)
            .await
            .map_err(db)?;
        if keys.is_empty() {
            break;
        }
        for key in &keys {
            let (_, node_id): (String, String) = serde_json::from_str(key)?;
            if let Some(value) = index.history_at("run_node", key, upper).await.map_err(db)? {
                let mut node: flow_engine::journal_state::Node = serde_json::from_value(value)?;
                let attempt_key = serde_json::to_string(&(id, &node_id, &node.dispatch_id))?;
                if let Some(attempt) = index
                    .history_at("run_attempt", &attempt_key, upper)
                    .await
                    .map_err(db)?
                {
                    node.attempts
                        .insert(node.dispatch_id.clone(), serde_json::from_value(attempt)?);
                }
                run.nodes.insert(node_id, node);
            }
        }
        after = keys.last().unwrap().clone();
    }
    if full {
        let mut after = String::new();
        loop {
            let keys = index
                .history_keys("run_attempt", Some(id), &after, upper, 256)
                .await
                .map_err(db)?;
            if keys.is_empty() {
                break;
            }
            for key in &keys {
                let (_, node, dispatch): (String, String, String) = serde_json::from_str(key)?;
                if let Some(node) = run.nodes.get_mut(&node) {
                    if let Some(value) = index
                        .history_at("run_attempt", key, upper)
                        .await
                        .map_err(db)?
                    {
                        node.attempts
                            .insert(dispatch, serde_json::from_value(value)?);
                    }
                }
            }
            after = keys.last().unwrap().clone();
        }
    }
    Ok(Some(run))
}

/// Read older evidence at exactly the reducer prefix, never the index's latest revision.
pub(crate) async fn prepare(index: &Projector, state: &mut State, tx: &Transaction) -> Result<()> {
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
                        .history_at("run_attempt", &key, upper)
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
        if matches!(
            event.kind,
            EventKind::ValueChunk | EventKind::ValuePublished
        ) {
            if let Some(id) = event.payload["output_id"].as_str() {
                descriptors.insert(id.into(), None);
            }
        }
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
                .ok_or_else(|| JournalError::Projection("invalid publication prefix".into()))?;
            if lsn <= upper {
                let descriptor: ValueRef = serde_json::from_value(record["value"].clone())?;
                // A mismatched reference remains a reducer error; user JSON can also contain
                // objects that happen to look like descriptors, so do not reject it here.
                if expected.as_ref().is_none_or(|r| r == &descriptor) {
                    state.values.remember_verified(descriptor)?;
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn index_rows(
    state: &State,
    tx: &Transaction,
) -> flow_journal::Result<Vec<ProjectedRow>> {
    let mut rows = audit_rows(tx)?;
    for row in project_rows(state, tx)? {
        let value = serde_json::to_value(row.value)?;
        let digest = flow_journal::codec::digest(&serde_json::to_vec(&value)?);
        rows.push(ProjectedRow {
            kind: "history".into(),
            key: serde_json::to_string(&(row.kind, row.key, format!("{:020}", tx.lsn), digest))?,
            value: Some(value),
        });
    }
    for event in &tx.events {
        if event.kind == EventKind::ValuePublished {
            if let Some(id) = event.payload["output_id"].as_str() {
                rows.push(ProjectedRow {
                    kind: "published".into(),
                    key: id.into(),
                    value: Some(json!({"lsn":tx.lsn.to_string(),"value":event.payload})),
                });
            }
        }
        if event.kind == EventKind::RunStarted {
            if let (Some(parent), Some(node), Some(child)) = (
                event.payload["parent"]["run_id"].as_str(),
                event.payload["parent"]["node_id"].as_str(),
                event.run_id.as_deref(),
            ) {
                rows.push(ProjectedRow {
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
                rows.push(ProjectedRow {
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
