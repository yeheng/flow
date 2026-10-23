//! Candidate runs share the normal executor, but never receive external-operation permits.
use crate::journal::{CommandReceipt, JournalBackend, JournalError};
use flow_engine::journal_state::{OperationFixture, Regression, Run};
use flow_journal::{Error, StoredValue};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

impl JournalBackend {
    pub async fn run_regression(
        &self,
        baseline: &str,
        workflow: Option<&str>,
        version: Option<u64>,
        request_id: &str,
    ) -> Result<CommandReceipt, JournalError> {
        self.run_regression_with_fixtures(baseline, workflow, version, BTreeMap::new(), request_id)
            .await
    }

    pub async fn run_regression_with_fixtures(
        &self,
        baseline: &str,
        workflow: Option<&str>,
        version: Option<u64>,
        overrides: BTreeMap<String, OperationFixture>,
        request_id: &str,
    ) -> Result<CommandReceipt, JournalError> {
        let root = self
            .run_snapshot(baseline)
            .await?
            .ok_or_else(|| Error::Invalid("baseline run not found".into()))?;
        if !root.terminal() {
            return Err(Error::Conflict("baseline must be terminal".into()).into());
        }
        let mut execution = root
            .execution
            .clone()
            .ok_or_else(|| Error::Invalid("baseline has no recorded execution context".into()))?;
        let mut fixtures = execution
            .regression
            .as_ref()
            .map(|r| r.fixtures.clone())
            .unwrap_or_default();
        let mut signals = execution
            .regression
            .as_ref()
            .map(|r| r.signals.clone())
            .unwrap_or_default();
        let mut todo = vec![root.clone()];
        let mut visited = BTreeSet::new();
        while let Some(run) = todo.pop() {
            if !visited.insert(run.run_id.clone()) {
                continue;
            }
            if visited.len() > 1000 {
                return Err(Error::Limit("baseline child closure exceeds 1000 runs".into()).into());
            }
            if !run.terminal() {
                return Err(Error::Conflict("baseline has unfinished child runs".into()).into());
            }
            let context = run
                .execution
                .as_ref()
                .ok_or_else(|| Error::Invalid("baseline child context missing".into()))?;
            let definition = self.cached_definition(&run.run_id, &run.definition).await?;
            for (id, node) in &run.nodes {
                if let Some(operation) = &node.operation {
                    if let Some(outcome) = &operation.outcome {
                        fixtures.insert(
                            context.node_key(id),
                            OperationFixture {
                                fingerprint: operation.fingerprint.clone(),
                                outcome: outcome.clone(),
                            },
                        );
                    }
                }
                if definition.node_type(id).is_some_and(|k| {
                    matches!(
                        k,
                        flow_engine::NodeType::HumanTask | flow_engine::NodeType::Input
                    )
                }) {
                    if let Some(output) = &node.output {
                        signals.insert(context.node_key(id), output.clone());
                    }
                }
            }
            for (_, child) in self
                .run_children(&run.run_id, self.journal.durable_lsn())
                .await?
            {
                todo.push(
                    self.run_snapshot(&child)
                        .await?
                        .ok_or_else(|| Error::Invalid("baseline child missing".into()))?,
                );
            }
        }
        if overrides.len() > 1000 {
            return Err(Error::Limit("maximum 1000 fixture overrides".into()).into());
        }
        let mut override_keys = execution
            .regression
            .as_ref()
            .map(|r| r.overrides.clone())
            .unwrap_or_default();
        for (path, fixture) in overrides {
            let node_path: Vec<String> = serde_json::from_str(&path)
                .map_err(|_| Error::Invalid("fixture paths must be JSON string arrays".into()))?;
            if node_path.is_empty()
                || serde_json::to_string(&node_path)? != path
                || fixture.fingerprint.len() != 64
            {
                return Err(
                    Error::Invalid("invalid fixture path or request fingerprint".into()).into(),
                );
            }
            override_keys.insert(path.clone());
            fixtures.insert(path, fixture);
        }
        execution.regression = Some(Regression {
            baseline: baseline.into(),
            fixtures,
            signals,
            overrides: override_keys,
        });
        let input = flow_journal::value::materialize(
            self.journal.root(),
            self.journal.durable_lsn(),
            &root.input,
            flow_journal::VALUE_BUDGET,
        )?;
        self.start_with_regression(
            workflow.unwrap_or(&root.workflow_id),
            version,
            input,
            "manual",
            Some(&format!("regression:{baseline}")),
            Some(request_id),
            Some(execution),
            None,
            None,
        )
        .await
    }

    pub async fn compare_runs(
        &self,
        baseline: &str,
        candidate: &str,
    ) -> Result<Value, JournalError> {
        let mut differences = Vec::new();
        let mut truncated = false;
        let mut complete = true;
        let mut visited = 0;
        let mut pairs = vec![(baseline.to_owned(), candidate.to_owned(), String::new())];
        while let Some((left_id, right_id, prefix)) = pairs.pop() {
            visited += 1;
            if visited > 1000 {
                return Err(Error::Limit("comparison exceeds 1000 run pairs".into()).into());
            }
            let left = self
                .run_snapshot(&left_id)
                .await?
                .ok_or_else(|| Error::Invalid("baseline run not found".into()))?;
            let right = self
                .run_snapshot(&right_id)
                .await?
                .ok_or_else(|| Error::Invalid("candidate run not found".into()))?;
            complete &= left.terminal() && right.terminal();
            if left.status != right.status {
                record_difference(
                    &mut differences,
                    &mut truncated,
                    json!({"path":format!("{prefix}status"),"expected":left.status,"actual":right.status}),
                );
            }
            if left.error != right.error {
                record_difference(
                    &mut differences,
                    &mut truncated,
                    json!({"path":format!("{prefix}error"),"expected":left.error,"actual":right.error}),
                );
            }
            if value_digest(left.output.as_ref())? != value_digest(right.output.as_ref())? {
                record_difference(
                    &mut differences,
                    &mut truncated,
                    json!({"path":format!("{prefix}output"),"expected":left.output,"actual":right.output}),
                );
            }
            let keys: BTreeSet<_> = left.nodes.keys().chain(right.nodes.keys()).collect();
            for key in keys {
                let expected_request = request_fingerprint(&left, key);
                let actual_request = request_fingerprint(&right, key);
                if expected_request != actual_request {
                    record_difference(
                        &mut differences,
                        &mut truncated,
                        json!({"path":format!("{prefix}nodes.{key}.request"),"expected":expected_request,"actual":actual_request}),
                    );
                }
                let a = left.nodes.get(key);
                let b = right.nodes.get(key);
                let equal = match (a, b) {
                    (Some(a), Some(b)) => {
                        a.status == b.status
                            && a.branch == b.branch
                            && a.error == b.error
                            && value_digest(a.output.as_ref())? == value_digest(b.output.as_ref())?
                    }
                    _ => false,
                };
                if !equal {
                    record_difference(
                        &mut differences,
                        &mut truncated,
                        json!({"path":format!("{prefix}nodes.{key}"),"expected_status":a.map(|n| n.status),"actual_status":b.map(|n| n.status),"expected_output":a.and_then(|n| n.output.clone()),"actual_output":b.and_then(|n| n.output.clone())}),
                    );
                }
            }
            let upper = self.journal.durable_lsn();
            let a = self.run_children(&left_id, upper).await?;
            let b = self.run_children(&right_id, upper).await?;
            for key in a.keys().chain(b.keys()).collect::<BTreeSet<_>>() {
                match (a.get(key), b.get(key)) {
                    (Some(left), Some(right)) => pairs.push((
                        left.clone(),
                        right.clone(),
                        format!("{prefix}children.{key}."),
                    )),
                    _ => record_difference(
                        &mut differences,
                        &mut truncated,
                        json!({"path":format!("{prefix}children.{key}"),"expected":a.get(key),"actual":b.get(key)}),
                    ),
                }
            }
        }
        Ok(
            json!({"baseline":baseline,"candidate":candidate,"complete":complete,"passed":complete && differences.is_empty(),"differences":differences,"truncated":truncated}),
        )
    }
}

fn request_fingerprint<'a>(run: &'a Run, node: &str) -> Option<&'a str> {
    if run
        .nodes
        .get(node)
        .and_then(|n| n.error.as_deref())
        .is_some_and(|e| e.starts_with("regression request differs"))
    {
        return None;
    }
    run.nodes
        .get(node)
        .and_then(|n| n.operation.as_ref())
        .map(|o| o.fingerprint.as_str())
        .or_else(|| {
            let e = run.execution.as_ref()?;
            e.regression
                .as_ref()?
                .fixtures
                .get(&e.node_key(node))
                .map(|f| f.fingerprint.as_str())
        })
}

fn value_digest(value: Option<&StoredValue>) -> Result<Option<String>, flow_journal::Error> {
    value
        .map(|v| match v {
            StoredValue::Inline(v) => Ok(flow_journal::codec::digest(
                &flow_journal::codec::bounded_json(v, flow_journal::INLINE_BYTES)?,
            )),
            StoredValue::Ref(r) => Ok(r.digest.clone()),
        })
        .transpose()
}

fn record_difference(differences: &mut Vec<Value>, truncated: &mut bool, value: Value) {
    if differences.len() < 64 {
        differences.push(value);
    } else {
        *truncated = true;
    }
}
