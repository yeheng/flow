use crate::journal::{JournalBackend, JournalError};
use flow_engine::journal_state::ExecutionSnapshot;
use flow_journal::{Error, StoredValue};
use std::collections::BTreeMap;

impl JournalBackend {
    pub(crate) async fn freeze_execution(
        &self,
        workflow: &str,
        version: Option<u64>,
    ) -> Result<(u64, StoredValue, Option<ExecutionSnapshot>), JournalError> {
        let _metadata = self.workflow_metadata.read().await;
        let root = self.published_definition(workflow, version).await?;
        let selected = root.version;
        let root_key = format!("{workflow}@{selected}");
        let mut definitions = BTreeMap::from([(root_key.clone(), root.clone())]);
        let mut bindings = BTreeMap::new();
        let mut todo = vec![root_key.clone()];
        while let Some(key) = todo.pop() {
            let version = &definitions[&key];
            let def = self
                .cached_definition(
                    &format!("dependency:{key}:{}", version.checksum),
                    &version.definition,
                )
                .await?;
            if key == root_key && def.semantics == 1 {
                return Ok((selected, root.definition, None));
            }
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
                let selected = node
                    .params
                    .get("version")
                    .map(|v| {
                        v.as_u64().ok_or_else(|| {
                            Error::Invalid("child version must be an integer".into())
                        })
                    })
                    .transpose()?;
                let version = self.published_definition(child, selected).await.map_err(
                    |error| match error {
                        JournalError::Journal(Error::NotFound(_)) => JournalError::Journal(
                            Error::Invalid("child published definition missing".into()),
                        ),
                        other => other,
                    },
                )?;
                let child_key = format!("{child}@{}", version.version);
                let unseen = !definitions.contains_key(&child_key);
                if unseen && definitions.len() >= 128 {
                    return Err(
                        Error::Limit("dependency closure exceeds 128 definitions".into()).into(),
                    );
                }
                let child_version = unseen.then_some(version);
                bindings.insert(format!("{key}/{}", node.id), child_key.clone());
                if let Some(version) = child_version {
                    definitions.insert(child_key.clone(), version);
                    todo.push(child_key);
                }
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

    /// Caller holds workflow_metadata read guard across the complete dependency
    /// closure. Old explicitly pinned versions are read cold, not cached forever.
    async fn published_definition(
        &self,
        workflow: &str,
        version: Option<u64>,
    ) -> Result<flow_engine::journal_state::DefinitionVersion, JournalError> {
        let selected = self
            .inspect_committed(|state| {
                let metadata = state
                    .workflows
                    .get(workflow)
                    .filter(|w| !w.deleted)
                    .ok_or_else(|| Error::NotFound("workflow not found".into()))?;
                version
                    .or_else(|| {
                        metadata
                            .versions
                            .values()
                            .rev()
                            .find(|v| v.published)
                            .map(|v| v.version)
                    })
                    .ok_or_else(|| Error::Invalid("no published version".into()))
            })
            .await?;
        let definition = self
            .definition_version(workflow, Some(selected))
            .await?
            .ok_or_else(|| Error::NotFound("workflow version not found".into()))?;
        if !definition.published {
            return Err(Error::Invalid("version not published".into()).into());
        }
        Ok(definition)
    }
}
