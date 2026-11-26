//! Managed install/uninstall orchestration. Detached workers retain reservations
//! until filesystem work and process reaping have actually finished.
use crate::{
    journal::JournalBackend,
    plugin_bundle::{self, BundleError, SealedBundle},
    plugin_operation::{Operation, OperationError, Phase},
};
use flow_config::plugin_fs::RootLease;
use serde_json::{json, Value};
use std::{fs, path::PathBuf, sync::Arc};

type Result<T> = std::result::Result<T, OperationError>;

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?
}
fn root() -> Result<PathBuf> {
    flow_config::plugin_fs::plugins_root()
        .ok_or_else(|| OperationError::Invalid("plugin root unavailable".into()))
}
fn reservation(error: &str) -> OperationError {
    if error == "host is shutting down" {
        OperationError::Invalid(error.into())
    } else {
        OperationError::Conflict(error.into())
    }
}
fn cleanup(op: &Operation) -> Result<()> {
    if let Err(error) = op.cleanup() {
        if matches!(&error, OperationError::Io(error) if error.kind() == std::io::ErrorKind::InvalidInput)
        {
            return Err(error);
        }
        tracing::warn!(%error, operation_id=%op.receipt.operation_id, "plugin cleanup pending");
    }
    Ok(())
}

fn attempt<T>(op: &mut Operation, work: impl FnOnce(&mut Operation) -> Result<T>) -> Result<T> {
    match work(op) {
        Ok(value) => Ok(value),
        Err(error) => {
            op.reject(&error)?;
            Err(error)
        }
    }
}
async fn reject(mut op: Operation, error: OperationError) -> Result<Value> {
    blocking(move || {
        op.reject(&error)?;
        Err(error)
    })
    .await
}

impl JournalBackend {
    pub async fn plugin_deployment(
        &self,
        plugin_id: &str,
    ) -> std::result::Result<Value, crate::journal::JournalError> {
        let _catalog = self.plugin_catalog.read().await;
        let id = plugin_id.to_owned();
        let defaults = self.plugin_defaults();
        let (package, bundle_digest, revision, draining) = tokio::task::spawn_blocking(move || {
            use crate::journal::JournalError;

            let package =
                crate::plugin_registry::installed_snapshot_cached_with_defaults(&defaults)
                    .rows
                    .into_iter()
                    .filter_map(|row| row.plugin)
                    .find(|plugin| plugin.manifest.plugin.id == id)
                    .ok_or_else(|| {
                        flow_journal::Error::NotFound("installed plugin not found".into())
                    })?;
            let root = package
                .source
                .parent()
                .ok_or_else(|| JournalError::Storage("plugin directory has no root".into()))?;
            let draining = crate::plugin_operation::is_draining(root, &id)
                .map_err(|error| JournalError::Storage(error.to_string()))?;
            let revision = flow_config::plugin_fs::publication_revision()
                .map_err(|error| JournalError::Storage(error.to_string()))?;
            let marker = package.source.join(plugin_bundle::MARKER);
            let digest = match fs::read(marker) {
                Ok(bytes) => Some(
                    serde_json::from_slice::<crate::plugin_operation::Identity>(&bytes)?
                        .bundle_digest,
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(JournalError::Storage(error.to_string())),
            };
            Ok::<_, JournalError>((package, digest, revision, draining))
        })
        .await
        .map_err(|error| crate::journal::JournalError::Storage(error.to_string()))??;
        let node_types = package.manifest.plugin.node_types;
        let manager = self.plugin_remote_manager.read().unwrap().upgrade();
        let mode = if manager.is_some() { "remote" } else { "local" };
        let agents = match manager {
            Some(manager) if !node_types.is_empty() => {
                manager
                    .plugin_readiness(
                        plugin_id,
                        &package.content_hash,
                        &node_types,
                        revision.as_deref(),
                    )
                    .await
            }
            _ => Default::default(),
        };
        Ok(json!({
            "plugin_id": plugin_id,
            "content_hash": package.content_hash,
            "bundle_digest": bundle_digest,
            "node_types": node_types,
            "draining": draining,
            "publication_revision": revision,
            "mode": mode,
            "agents": agents,
        }))
    }

    async fn plugin_blockers(&self, plugin_id: &str) -> Result<Vec<String>> {
        let (mut blockers, legacy) = self
            .inspect_committed(|state| {
                let mut blockers = Vec::new();
                let mut legacy = Vec::new();
                for run in state.runs.values().filter(|run| !run.terminal()) {
                    match &run.plugin_bindings {
                        Some(bindings)
                            if bindings
                                .values()
                                .any(|binding| binding.plugin_id == plugin_id) =>
                        {
                            blockers.push(run.run_id.clone());
                        }
                        Some(_) if run.execution.is_some() => {}
                        Some(_) => legacy.push(run.clone()),
                        None => legacy.push(run.clone()),
                    }
                }
                Ok((blockers, legacy))
            })
            .await
            .map_err(|error| OperationError::Io(std::io::Error::other(error)))?;
        for run in legacy {
            let frozen_ceiling = run.policy.as_ref().is_some_and(|policy| {
                policy
                    .plugin_ceilings
                    .iter()
                    .any(|ceiling| ceiling.plugin_id == plugin_id)
            });
            if frozen_ceiling || self.legacy_run_uses_plugin(&run, plugin_id).await? {
                blockers.push(run.run_id);
            }
        }
        Ok(blockers)
    }

    async fn legacy_run_uses_plugin(
        &self,
        run: &flow_domain::journal_state::Run,
        plugin_id: &str,
    ) -> Result<bool> {
        let mut definitions = vec![&run.definition];
        if let Some(snapshot) = &run.execution {
            definitions.extend(
                snapshot
                    .definitions
                    .values()
                    .map(|version| &version.definition),
            );
        }
        for (index, stored) in definitions.into_iter().enumerate() {
            let key = format!("plugin-drain:{}:{index}", run.run_id);
            let definition = self
                .cached_frozen_definition(&key, stored)
                .await
                .map_err(|error| OperationError::Io(std::io::Error::other(error)))?;
            for node in &definition.nodes {
                // Semantics 1 resolves children at execution time.
                if (run.execution.is_none()
                    && node.kind() == Some(flow_domain::NodeType::SubWorkflow))
                    || crate::policy::plugin_id_for_node(node, run).as_deref() == Some(plugin_id)
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    pub async fn plugin_install(
        self: &Arc<Self>,
        source: String,
        replace: bool,
        request_id: String,
    ) -> Result<Value> {
        let backend = self.clone();
        tokio::spawn(async move { backend.install_worker(source, replace, request_id).await })
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?
    }
    async fn install_worker(
        self: Arc<Self>,
        source: String,
        replace: bool,
        request_id: String,
    ) -> Result<Value> {
        let source_path = PathBuf::from(&source);
        let capture_source = source_path.clone();
        let prepare_id = request_id.clone();
        let (root, arguments, id, replay) = blocking(move || {
            let root = root()?;
            let arguments = json!({"source":source,"replace":replace});
            let prior = Operation::lookup(&root, "plugins.install", &prepare_id, &arguments)?;
            let replay = prior
                .as_ref()
                .map(|receipt| receipt.replay())
                .transpose()?
                .flatten();
            let id = match prior {
                Some(receipt) => receipt.plugin_id,
                None => plugin_bundle::bounded_manifest(&source_path)?.1.plugin.id,
            };
            Ok((root, arguments, id, replay))
        })
        .await?;
        if let Some(result) = replay {
            return Ok(result);
        }
        let _reservation = self.reserve_plugin_operation(&id).map_err(reservation)?;
        let running = self
            .services
            .lock()
            .await
            .get(&id)
            .is_some_and(|service| service.alive());
        if running {
            return Err(OperationError::Conflict(
                "service already running; stop before replace".into(),
            ));
        }
        let catalog = self.plugin_catalog.write().await;
        let mut op = blocking(move || {
            Operation::reconcile_pending(&root, &id)?;
            Operation::begin(&root, "plugins.install", &request_id, &arguments, &id)
        })
        .await?;
        drop(catalog);
        if let Some(result) = op.replay()? {
            return Ok(result);
        }
        let source_path = capture_source;
        // Capture outside the exclusive lease so unrelated starts and lists proceed.
        op = blocking(move || {
            attempt(&mut op, |op| {
                if op.receipt.phase == Phase::Capturing {
                    if op.stage().try_exists()? {
                        fs::remove_dir_all(op.stage())?;
                    }
                    let bundle = plugin_bundle::capture(&source_path, &op.stage(), op.root())?;
                    if bundle.manifest.plugin.id != op.receipt.plugin_id {
                        return Err(OperationError::Conflict(
                            "source plugin id changed during capture".into(),
                        ));
                    }
                    op.seal(bundle.bundle_digest)?;
                }
                Ok(())
            })?;
            Ok(op)
        })
        .await?;
        let running = self
            .services
            .lock()
            .await
            .get(&op.receipt.plugin_id)
            .is_some_and(|service| service.alive());
        if running {
            return reject(
                op,
                OperationError::Conflict("service already running; stop before replace".into()),
            )
            .await;
        }
        let _catalog = self.plugin_catalog.write().await;
        let (op, changes_bundle) = blocking(move || {
            let mut op = op;
            let _lease = RootLease::exclusive(op.root())?;
            let changes_bundle = attempt(&mut op, |op| {
                validate_target(op)?;
                let old = op.target_identity()?;
                let new_digest = op
                    .receipt
                    .new_identity
                    .as_ref()
                    .map(|identity| &identity.bundle_digest);
                Ok(
                    old.as_ref().map(|identity| &identity.bundle_digest) != new_digest
                        && (old.is_none() || replace),
                )
            })?;
            Ok((op, changes_bundle))
        })
        .await?;
        let plugin_id = op.receipt.plugin_id.clone();
        if changes_bundle {
            let blockers = self.plugin_blockers(&plugin_id).await?;
            if !blockers.is_empty() {
                return drain(op, blockers).await;
            }
        }
        let mut op = op;
        let defaults = self.plugin_defaults();
        let grants = self.inspect(|state| state.plugin_grants.clone()).await;
        blocking(move || {
            let _lease = RootLease::exclusive(op.root())?;
            attempt(&mut op, |op| {
                if op.receipt.phase != Phase::Sealed {
                    let result = op.recover()?;
                    cleanup(op)?;
                    return Ok(result);
                }
                let bundle = plugin_bundle::inspect(&op.stage())?;
                if op.receipt.new_identity.as_ref().map(|i| &i.bundle_digest)
                    != Some(&bundle.bundle_digest)
                {
                    return Err(OperationError::Conflict(
                        "plugin operation superseded".into(),
                    ));
                }
                let snapshot = crate::plugin_scan::scan_unlocked(op.root(), &defaults)?;
                for row in &snapshot.rows {
                    if row.id == op.receipt.plugin_id && row.source != op.live() {
                        return Err(OperationError::Conflict("duplicate plugin id".into()));
                    }
                    if let Some(plugin) = &row.plugin {
                        if plugin.manifest.plugin.id != op.receipt.plugin_id
                            && plugin
                                .manifest
                                .plugin
                                .node_types
                                .iter()
                                .any(|node| bundle.manifest.plugin.node_types.contains(node))
                        {
                            return Err(OperationError::Conflict(format!(
                                "node type overlaps plugin {}",
                                plugin.manifest.plugin.id
                            )));
                        }
                    }
                }
                for node in &bundle.manifest.plugin.node_types {
                    if let Some(owner) = defaults.get(node) {
                        if owner != &op.receipt.plugin_id {
                            return Err(OperationError::Conflict(format!(
                                "node type overlaps plugin {owner}"
                            )));
                        }
                    }
                }
                validate_target(op)?;
                let old = op.target_identity()?;
                if !snapshot.rows.iter().any(|row| row.source == op.live())
                    && snapshot.rows.len() >= 64
                {
                    return Err(BundleError::Limit("plugin limit reached".into()).into());
                }
                let key = flow_domain::journal_state::PluginGrant::key(
                    &op.receipt.plugin_id,
                    &bundle.content_hash,
                );
                let grant_state = if grants.contains_key(&key) {
                    "granted"
                } else if grants
                    .values()
                    .any(|grant| grant.plugin_id == op.receipt.plugin_id)
                {
                    "hash_changed"
                } else {
                    "pending"
                };
                let same = old
                    .as_ref()
                    .is_some_and(|old| old.bundle_digest == bundle.bundle_digest);
                if old.is_some() && !replace && !same {
                    return Err(OperationError::Conflict("plugin already installed".into()));
                }
                let result = install_result(&bundle, grant_state, old.is_some() && !same, same);
                if !same || crate::plugin_operation::is_draining(op.root(), &op.receipt.plugin_id)? {
                    op.record_drain(old.as_ref())?;
                }
                let result = if same {
                    op.complete_unchanged(result)?
                } else {
                    op.install_intent(old, result)?;
                    op.recover()?
                };
                cleanup(op)?;
                if let Err(error) = op.clear_draining() {
                    tracing::warn!(%error, plugin_id = %op.receipt.plugin_id, "could not clear plugin drain marker");
                }
                Ok(result)
            })
        })
        .await
    }

    pub async fn plugin_uninstall(
        self: &Arc<Self>,
        plugin_id: String,
        stop_first: bool,
        request_id: String,
    ) -> Result<Value> {
        let backend = self.clone();
        tokio::spawn(async move {
            backend
                .uninstall_worker(plugin_id, stop_first, request_id)
                .await
        })
        .await
        .map_err(|e| std::io::Error::other(e.to_string()))?
    }
    async fn uninstall_worker(
        self: Arc<Self>,
        plugin_id: String,
        stop_first: bool,
        request_id: String,
    ) -> Result<Value> {
        let prepare_id = request_id.clone();
        let (root, arguments, id, replay) = blocking(move || {
            crate::plugin_operation::validate_id(&plugin_id)?;
            let root = root()?;
            let arguments = json!({"plugin_id":plugin_id,"stop_first":stop_first});
            let prior = Operation::lookup(&root, "plugins.uninstall", &prepare_id, &arguments)?;
            let replay = prior
                .as_ref()
                .map(|receipt| receipt.replay())
                .transpose()?
                .flatten();
            Ok((root, arguments, plugin_id, replay))
        })
        .await?;
        if let Some(result) = replay {
            return Ok(result);
        }
        let _reservation = self.reserve_plugin_operation(&id).map_err(reservation)?;
        let _catalog = self.plugin_catalog.write().await;
        let mut op = blocking(move || {
            Operation::reconcile_pending(&root, &id)?;
            Operation::begin(&root, "plugins.uninstall", &request_id, &arguments, &id)
        })
        .await?;
        if let Some(result) = op.replay()? {
            return Ok(result);
        }
        // An intent already fixed its result and must not stop a later service generation.
        if op.receipt.phase == Phase::RemoveIntent {
            return blocking(move || {
                let _lease = RootLease::exclusive(op.root())?;
                let result = op.recover()?;
                cleanup(&op)?;
                Ok(result)
            })
            .await;
        }
        let (prepared, installed) = blocking(move || {
            let _lease = RootLease::shared(op.root())?;
            let installed = attempt(&mut op, |op| {
                validate_target(op)?;
                Ok(op.live().try_exists()?)
            })?;
            Ok((op, installed))
        })
        .await?;
        op = prepared;
        if installed {
            let blockers = self.plugin_blockers(&op.receipt.plugin_id).await?;
            if !blockers.is_empty() {
                return drain(op, blockers).await;
            }
        }
        let running = self
            .services
            .lock()
            .await
            .get(&op.receipt.plugin_id)
            .is_some_and(|service| service.alive());
        if running && !stop_first {
            return reject(op, OperationError::Conflict("service running".into())).await;
        }
        let service = self.services.lock().await.remove(&op.receipt.plugin_id);
        let stopped_instance = service.as_ref().map(|service| service.instance.clone());
        if let Some(service) = service {
            self.service_ui
                .lock()
                .await
                .retain(|_, (instance, _)| instance != &service.instance);
            service.shutdown().await;
        }
        let running = self
            .services
            .lock()
            .await
            .get(&op.receipt.plugin_id)
            .is_some_and(|service| service.alive());
        if running {
            return reject(op, OperationError::Conflict("service running".into())).await;
        }
        blocking(move || {
            let _lease = RootLease::exclusive(op.root())?;
            attempt(&mut op, |op| {
                validate_target(op)?;
                let reload = plugin_bundle::bounded_manifest(&op.live())
                    .map(|(_, m)| !m.plugin.node_types.is_empty())
                    .unwrap_or(true);
                let old = op.target_identity()?;
                let result = json!({"plugin_id":op.receipt.plugin_id,"removed":old.is_some(),
                "already_absent":old.is_none(),"stopped_instance":stopped_instance,
                "in_flight_runs_unaffected":true,"executor_reload_required":old.is_some() && reload,
                "grants_retained":true,"trash":old.as_ref().map(|_| op.trash())});
                let result = if let Some(old) = old {
                    op.record_drain(Some(&old))?;
                    op.remove_intent(old, result)?;
                    op.recover()?
                } else {
                    op.complete_unchanged(result)?
                };
                cleanup(op)?;
                if let Err(error) = op.clear_draining() {
                    tracing::warn!(%error, plugin_id = %op.receipt.plugin_id, "could not clear plugin drain marker");
                }
                Ok(result)
            })
        })
        .await
    }
}

async fn drain(mut op: Operation, run_ids: Vec<String>) -> Result<Value> {
    blocking(move || {
        op.mark_draining()?;
        let error = OperationError::Conflict(format!(
            "plugin is draining; active runs: {}",
            run_ids.join(", ")
        ));
        op.reject(&error)?;
        Err(error)
    })
    .await
}

fn install_result(
    bundle: &SealedBundle,
    grant_state: &str,
    replaced: bool,
    idempotent: bool,
) -> Value {
    json!({"plugin_id":bundle.manifest.plugin.id,"version":bundle.manifest.plugin.version,
        "content_hash":bundle.content_hash,"bundle_digest":bundle.bundle_digest,
        "node_types":bundle.manifest.plugin.node_types,
        "service":bundle.manifest.service.is_some(),"grant_state":grant_state,
        "entry_executable":bundle.entry_executable,
        "executor_reload_required":!bundle.manifest.plugin.node_types.is_empty(),
        "replaced":replaced,"idempotent":idempotent})
}
fn validate_target(op: &Operation) -> Result<()> {
    let path = op.live();
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || path.canonicalize()?.parent() != Some(op.root())
    {
        return Err(OperationError::Invalid(
            "plugin target must be a direct regular child directory".into(),
        ));
    }
    if let Ok((_, manifest)) = plugin_bundle::bounded_manifest(&path) {
        if manifest.plugin.id != op.receipt.plugin_id {
            return Err(OperationError::Invalid(format!(
                "plugin id does not match directory: source={}",
                path.display()
            )));
        }
    }
    Ok(())
}
