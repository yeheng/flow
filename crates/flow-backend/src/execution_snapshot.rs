use crate::journal::{JournalBackend, JournalError};
use flow_engine::journal_state::{DefinitionVersion, ExecutionSnapshot};
use flow_journal::{Error, StoredValue};
use std::collections::BTreeMap;

impl JournalBackend {
    pub(crate) async fn freeze_execution(
        &self,
        workflow: &str,
        version: Option<u64>,
    ) -> Result<(u64, StoredValue, Option<ExecutionSnapshot>), JournalError> {
        let workflows = self.inspect(|s| s.workflows.clone()).await;
        let root = workflows
            .get(workflow)
            .filter(|w| !w.deleted)
            .ok_or_else(|| Error::NotFound("workflow not found".into()))?;
        let selected = version
            .or_else(|| {
                root.versions
                    .values()
                    .rev()
                    .find(|v| v.published)
                    .map(|v| v.version)
            })
            .ok_or_else(|| Error::Invalid("no published version".into()))?;
        let root = root
            .versions
            .get(&selected)
            .ok_or_else(|| Error::NotFound("workflow version not found".into()))?
            .clone();
        if !root.published {
            return Err(Error::Invalid("version not published".into()).into());
        }
        let mut definitions: BTreeMap<String, DefinitionVersion> = BTreeMap::new();
        let mut bindings = BTreeMap::new();
        let mut todo = vec![(workflow.to_owned(), root.clone())];
        while let Some((id, version)) = todo.pop() {
            let key = format!("{id}@{}", version.version);
            if definitions.contains_key(&key) {
                continue;
            }
            if definitions.len() >= 128 {
                return Err(
                    Error::Limit("dependency closure exceeds 128 definitions".into()).into(),
                );
            }
            let def = self
                .cached_definition(
                    &format!("dependency:{key}:{}", version.checksum),
                    &version.definition,
                )
                .await?;
            if definitions.is_empty() && def.semantics == 1 {
                return Ok((selected, root.definition, None));
            }
            definitions.insert(key.clone(), version);
            for node in &def.nodes {
                if node.kind() != Some(flow_engine::NodeType::SubWorkflow) {
                    continue;
                }
                let child = node.params["workflow_id"]
                    .as_str()
                    .filter(|s| !s.contains("${"))
                    .ok_or_else(|| {
                        Error::Invalid("semantics 2 requires a static child workflow_id".into())
                    })?;
                let workflow = workflows
                    .get(child)
                    .filter(|w| !w.deleted)
                    .ok_or_else(|| Error::Invalid("child workflow not found".into()))?;
                let version = match node.params.get("version") {
                    Some(v) => workflow.versions.get(&v.as_u64().ok_or_else(|| {
                        Error::Invalid("child version must be an integer".into())
                    })?),
                    None => workflow.versions.values().rev().find(|v| v.published),
                }
                .filter(|v| v.published)
                .ok_or_else(|| Error::Invalid("child published definition missing".into()))?;
                let child_key = format!("{child}@{}", version.version);
                bindings.insert(format!("{key}/{}", node.id), child_key);
                todo.push((child.into(), version.clone()));
            }
        }
        Ok((
            selected,
            root.definition,
            Some(ExecutionSnapshot {
                runtime: flow_engine::RUNTIME_ID.into(),
                clock_ms: chrono::Utc::now().timestamp_millis(),
                seed: crate::journal_commands::id(),
                path: Vec::new(),
                definitions,
                bindings,
                regression: None,
                correlation_id: None,
                agent: self.harness_agent_config(),
            }),
        ))
    }
}
