//! Durable mutation intent. Callers own the plugin reservation and publication lease.
//! No method here starts/stops processes or writes journal grants.
use crate::plugin_bundle::{self, BundleError, MARKER};
use flow_config::plugin_fs::RootLease;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

#[derive(Debug, thiserror::Error)]
pub enum OperationError {
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    Bundle(#[from] BundleError),
    #[error("{0}")]
    Json(#[from] serde_json::Error),
}
type Result<T> = std::result::Result<T, OperationError>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Capturing,
    Sealed,
    MoveOldIntent,
    PublishIntent,
    RemoveIntent,
    Completed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Identity {
    pub generation: String,
    pub bundle_digest: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Receipt {
    pub operation_id: String,
    pub staging: PathBuf,
    pub trash: PathBuf,
    pub plugin_id: String,
    pub fingerprint: String,
    pub method: String,
    pub phase: Phase,
    pub new_identity: Option<Identity>,
    /// None = target was absent. Used only once a mutation intent is durable.
    pub old_identity: Option<Identity>,
    pub result: Option<Value>,
    #[serde(default)]
    failure: Option<(i32, String)>,
}

impl Receipt {
    pub fn replay(&self) -> Result<Option<Value>> {
        if let Some((code, message)) = &self.failure {
            return Err(match code {
                -32602 => OperationError::Invalid(message.clone()),
                -32012 => OperationError::Conflict(message.clone()),
                -32021 => BundleError::Limit(message.clone()).into(),
                _ => io::Error::other(message.clone()).into(),
            });
        }
        if self.phase == Phase::Completed {
            return self.result.clone().map(Some).ok_or_else(missing);
        }
        Ok(None)
    }
}

/// A stable per-request worker lock survives receipt renames. It is distinct from
/// the short root `.ops.lock`; it is never removed, just like `.publish.lock`.
pub struct Operation {
    root: PathBuf,
    key: String,
    _worker: fs::File,
    pub receipt: Receipt,
}
impl Operation {
    /// Used under a shared publication lease by service admission after restart.
    pub fn has_unfinished(root: &Path, plugin_id: &str) -> Result<bool> {
        let _ops = RootLease::operations(root)?;
        let directory = root.join(".ops");
        if !directory.try_exists()? {
            return Ok(false);
        }
        let metadata = fs::symlink_metadata(&directory)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(OperationError::Invalid(
                ".ops must be a regular directory".into(),
            ));
        }
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            let key = path.file_name().unwrap_or_default().to_string_lossy();
            if key.len() != 64 || !key.bytes().all(|b| b.is_ascii_hexdigit()) {
                continue;
            }
            let receipt: Receipt = serde_json::from_slice(&fs::read(path)?)?;
            if receipt.plugin_id == plugin_id && receipt.phase != Phase::Completed {
                return Ok(true);
            }
        }
        Ok(false)
    }
    /// Reconcile durable rename intents before accepting another managed
    /// mutation. Capturing/sealed requests need their original retry; active
    /// workers are skipped and continue to fence their IDs through the log.
    pub fn reconcile_pending(root: &Path, plugin_id: &str) -> Result<()> {
        if !root.exists() {
            return Ok(());
        }
        let root = root.canonicalize()?;
        let pending = {
            let _ops = RootLease::operations(&root)?;
            let directory = root.join(".ops");
            match fs::symlink_metadata(&directory) {
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error.into()),
                Ok(metadata) if !metadata.is_dir() || metadata.file_type().is_symlink() => {
                    return Err(OperationError::Invalid(
                        ".ops must be a regular directory".into(),
                    ))
                }
                _ => (),
            }
            let mut pending = Vec::new();
            for entry in fs::read_dir(directory)? {
                let path = entry?.path();
                let key = path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned();
                if key.len() != 64 || !key.bytes().all(|b| b.is_ascii_hexdigit()) {
                    continue;
                }
                let receipt: Receipt = serde_json::from_slice(&fs::read(path)?)?;
                if receipt.plugin_id == plugin_id
                    && matches!(
                        receipt.phase,
                        Phase::MoveOldIntent | Phase::PublishIntent | Phase::RemoveIntent
                    )
                {
                    pending.push(key);
                }
            }
            pending
        };
        for key in pending {
            let worker_path = root.join(".ops").join(format!("{key}.lock"));
            let metadata = fs::symlink_metadata(&worker_path)?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(OperationError::Invalid(
                    "operation lock must be a regular file".into(),
                ));
            }
            let worker = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(worker_path)?;
            match worker.try_lock_exclusive() {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Err(error.into()),
                Ok(()) => (),
            }
            let receipt: Receipt = {
                let _ops = RootLease::operations(&root)?;
                serde_json::from_slice(&fs::read(root.join(".ops").join(&key))?)?
            };
            let mut operation = Self {
                root: root.clone(),
                key,
                _worker: worker,
                receipt,
            };
            let _lease = RootLease::exclusive(&root)?;
            match operation.recover() {
                Ok(_) => {
                    if let Err(error) = operation.cleanup() {
                        tracing::warn!(%error, "recovered plugin cleanup pending");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, operation_id=%operation.receipt.operation_id, "plugin intent remains unresolved")
                }
            }
        }
        Ok(())
    }
    /// Look up identity before opening an install source: sealed/completed retries
    /// must also work when that source has been changed or removed.
    pub fn lookup(
        root: &Path,
        method: &str,
        request_id: &str,
        arguments: &Value,
    ) -> Result<Option<Receipt>> {
        if !root.exists() {
            return Ok(None);
        }
        let _ops = RootLease::operations(root)?;
        let key = hex::encode(Sha256::digest(format!("{method}\0{request_id}").as_bytes()));
        let path = root.join(".ops").join(key);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let receipt: Receipt = serde_json::from_slice(&bytes)?;
        let fingerprint = hex::encode(Sha256::digest(serde_json::to_vec(arguments)?));
        if receipt.fingerprint != fingerprint || receipt.method != method {
            return Err(OperationError::Conflict(
                "same request identity with different arguments".into(),
            ));
        }
        if receipt.phase == Phase::Completed {
            plugin_bundle::sync_file(&fs::File::open(&path)?)?;
            plugin_bundle::sync_dir(path.parent().ok_or_else(missing)?)?;
        }
        Ok(Some(receipt))
    }
    pub fn begin(
        root: &Path,
        method: &str,
        request_id: &str,
        arguments: &Value,
        plugin_id: &str,
    ) -> Result<Self> {
        validate_id(plugin_id)?;
        if !matches!(method, "plugins.install" | "plugins.uninstall")
            || request_id.is_empty()
            || request_id.len() > 128
        {
            return Err(OperationError::Invalid(
                "invalid plugin operation identity".into(),
            ));
        }
        create_durable_root(root)?;
        let root = root.canonicalize()?;
        // Fail before creating any intent if directory sync is unsupported.
        plugin_bundle::sync_dir(&root)?;
        let _ops = RootLease::operations(&root)?;
        for name in [".ops", ".staging", ".trash"] {
            let path = root.join(name);
            match fs::create_dir(&path) {
                Ok(()) => (),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    let metadata = fs::symlink_metadata(&path)?;
                    if !metadata.is_dir() || metadata.file_type().is_symlink() {
                        return Err(OperationError::Invalid(format!(
                            "{name} must be a regular directory"
                        )));
                    }
                }
                Err(error) => return Err(error.into()),
            }
            plugin_bundle::sync_dir(&path)?;
        }
        plugin_bundle::sync_dir(&root)?;
        let key = hex::encode(Sha256::digest(format!("{method}\0{request_id}").as_bytes()));
        let fingerprint = hex::encode(Sha256::digest(serde_json::to_vec(arguments)?));
        let path = root.join(".ops").join(&key);
        let receipt = if path.exists() {
            let receipt: Receipt = serde_json::from_slice(&fs::read(&path)?)?;
            if receipt.fingerprint != fingerprint
                || receipt.method != method
                || receipt.plugin_id != plugin_id
            {
                return Err(OperationError::Conflict(
                    "same request identity with different arguments".into(),
                ));
            }
            receipt
        } else {
            // An unresolved earlier operation fences this ID across master restarts.
            for entry in fs::read_dir(root.join(".ops"))? {
                let path = entry?.path();
                let name = path.file_name().unwrap_or_default().to_string_lossy();
                if name.len() != 64 || !name.bytes().all(|b| b.is_ascii_hexdigit()) {
                    continue;
                }
                let old: Receipt = serde_json::from_slice(&fs::read(path)?)?;
                if old.plugin_id == plugin_id && old.phase != Phase::Completed {
                    return Err(OperationError::Conflict(format!(
                        "unfinished plugin operation {}; retry its original request",
                        old.operation_id
                    )));
                }
            }
            let operation_id = uuid::Uuid::now_v7().to_string();
            Receipt {
                staging: PathBuf::from(".staging").join(&operation_id),
                trash: PathBuf::from(".trash").join(&operation_id),
                operation_id,
                plugin_id: plugin_id.into(),
                fingerprint,
                method: method.into(),
                phase: Phase::Capturing,
                new_identity: None,
                old_identity: None,
                result: None,
                failure: None,
            }
        };
        let lock_path = root.join(".ops").join(format!("{key}.lock"));
        if let Ok(metadata) = fs::symlink_metadata(&lock_path) {
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(OperationError::Invalid(
                    "operation worker lock must be a regular file".into(),
                ));
            }
        }
        let worker = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        worker.try_lock_exclusive().map_err(|e| {
            if e.kind() == io::ErrorKind::WouldBlock {
                OperationError::Conflict("plugin operation in progress".into())
            } else {
                e.into()
            }
        })?;
        plugin_bundle::sync_file(&worker)?;
        let operation = Self {
            root,
            key,
            _worker: worker,
            receipt,
        };
        operation.save_unlocked()?;
        Ok(operation)
    }
    pub fn stage(&self) -> PathBuf {
        self.root.join(&self.receipt.staging)
    }
    pub fn trash(&self) -> PathBuf {
        self.root.join(&self.receipt.trash)
    }
    pub fn live(&self) -> PathBuf {
        self.root.join(&self.receipt.plugin_id)
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    /// Best effort after durable completion. Only operation-owned generations may
    /// be removed, never a live path and never another request's remnants.
    pub fn cleanup(&self) -> Result<()> {
        if self.receipt.phase != Phase::Completed {
            return Err(missing());
        }
        for (path, expected) in [
            (self.stage(), &self.receipt.new_identity),
            (self.trash(), &self.receipt.old_identity),
        ] {
            let metadata = match fs::symlink_metadata(&path) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error.into()),
            };
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || path.canonicalize()?.parent()
                    != Some(path.parent().ok_or_else(missing)?.canonicalize()?.as_path())
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsafe plugin cleanup path: {}", path.display()),
                )
                .into());
            }
            require_identity(&path, expected.as_ref().ok_or_else(missing)?)?;
            fs::remove_dir_all(&path)?;
            plugin_bundle::sync_dir(path.parent().ok_or_else(missing)?)?;
        }
        Ok(())
    }
    pub fn replay(&self) -> Result<Option<Value>> {
        self.receipt.replay()
    }
    /// Rejections before any live intent are terminal outcomes, not unresolved
    /// transactions that fence the plugin forever. Retain request fingerprints.
    pub fn reject(&mut self, error: &OperationError) -> Result<()> {
        if matches!(error, OperationError::Conflict(message) if message.contains("evidence missing") || message.contains("superseded"))
        {
            return Ok(());
        }
        if !matches!(self.receipt.phase, Phase::Capturing | Phase::Sealed) {
            return Ok(());
        }
        let code = match error {
            OperationError::Invalid(_) | OperationError::Bundle(BundleError::Invalid(_)) => -32602,
            OperationError::Conflict(_) => -32012,
            OperationError::Bundle(BundleError::Limit(_)) => -32021,
            _ => -32603,
        };
        self.receipt.failure = Some((code, error.to_string()));
        self.receipt.phase = Phase::Completed;
        self.save()?;
        if self.stage().try_exists()? {
            if let Some(expected) = &self.receipt.new_identity {
                require_identity(&self.stage(), expected)?;
            }
            let metadata = fs::symlink_metadata(self.stage())?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(superseded());
            }
            fs::remove_dir_all(self.stage())?;
            plugin_bundle::sync_dir(&self.root.join(".staging"))?;
        }
        Ok(())
    }
    pub fn seal(&mut self, bundle_digest: String) -> Result<()> {
        if self.receipt.phase != Phase::Capturing {
            return Err(superseded());
        }
        let identity = Identity {
            generation: uuid::Uuid::now_v7().to_string(),
            bundle_digest,
        };
        if plugin_bundle::digest(&self.stage())? != identity.bundle_digest {
            return Err(superseded());
        }
        atomic_json(&self.stage().join(MARKER), &identity)?;
        self.receipt.new_identity = Some(identity);
        self.receipt.phase = Phase::Sealed;
        self.save()
    }
    /// Caller holds the exclusive root lease. Legacy targets gain a durable marker.
    pub fn target_identity(&self) -> Result<Option<Identity>> {
        identity(&self.live(), true)
    }

    /// Fix the result and expected target before the first live rename.
    pub fn install_intent(&mut self, old: Option<Identity>, result: Value) -> Result<()> {
        if self.receipt.phase != Phase::Sealed {
            return Err(superseded());
        }
        self.publication_fence()?;
        self.receipt.old_identity = old;
        self.receipt.result = Some(result);
        self.receipt.phase = if self.receipt.old_identity.is_some() {
            Phase::MoveOldIntent
        } else {
            Phase::PublishIntent
        };
        self.save()
    }
    pub fn remove_intent(&mut self, old: Identity, result: Value) -> Result<()> {
        if self.receipt.phase != Phase::Capturing {
            return Err(superseded());
        }
        self.publication_fence()?;
        self.receipt.old_identity = Some(old);
        self.receipt.result = Some(result);
        self.receipt.phase = Phase::RemoveIntent;
        self.save()
    }
    /// For an absent uninstall or a fresh same-bundle install. No live mutation.
    pub fn complete_unchanged(&mut self, result: Value) -> Result<Value> {
        if !matches!(self.receipt.phase, Phase::Capturing | Phase::Sealed) {
            return Err(superseded());
        }
        self.receipt.result = Some(result);
        self.complete()
    }
    /// Caller holds the exclusive root lease. Sealed work never rereads a source.
    pub fn recover(&mut self) -> Result<Value> {
        if let Some(result) = self.replay()? {
            return Ok(result);
        }
        match self.receipt.phase {
            Phase::MoveOldIntent => {
                let old = self.receipt.old_identity.as_ref().ok_or_else(missing)?;
                if matches_identity(&self.trash(), old)? {
                    if self.live().try_exists()? {
                        return Err(superseded());
                    }
                } else {
                    require_identity(&self.live(), old)?;
                    if self.trash().try_exists()? {
                        return Err(superseded());
                    }
                    durable_rename(&self.live(), &self.trash())?;
                }
                self.receipt.phase = Phase::PublishIntent;
                self.save()?;
                self.publish()
            }
            Phase::PublishIntent => self.publish(),
            Phase::RemoveIntent => {
                let old = self.receipt.old_identity.as_ref().ok_or_else(missing)?;
                // Trash is affirmative evidence of this removal. A later live
                // generation is never touched, even if its bytes are identical.
                if !matches_identity(&self.trash(), old)? {
                    require_identity(&self.live(), old)?;
                    if self.trash().try_exists()? {
                        return Err(superseded());
                    }
                    durable_rename(&self.live(), &self.trash())?;
                }
                self.complete()
            }
            _ => Err(missing()),
        }
    }
    fn publish(&mut self) -> Result<Value> {
        let new = self.receipt.new_identity.as_ref().ok_or_else(missing)?;
        if matches_identity(&self.live(), new)? {
            return self.complete();
        }
        if self.live().try_exists()? {
            return Err(superseded());
        }
        require_identity(&self.stage(), new)?;
        if let Err(error) = durable_rename(&self.stage(), &self.live()) {
            // If rename happened and only its directory sync failed, keep the
            // published evidence for recovery. Never overwrite that directory.
            if !self.live().try_exists()? {
                if let Some(old) = self.receipt.old_identity.clone() {
                    require_identity(&self.trash(), &old)?;
                    // This existing phase also safely describes rollback: either
                    // the old generation is still in trash or it is back at live.
                    self.receipt.phase = Phase::MoveOldIntent;
                    self.save()?;
                    if let Err(rollback) = durable_rename(&self.trash(), &self.live()) {
                        return Err(io::Error::other(format!(
                            "{error}; rollback failed: {rollback}; trash={}",
                            self.trash().display()
                        ))
                        .into());
                    }
                }
            }
            return Err(error);
        }
        require_identity(&self.live(), new)?;
        self.complete()
    }
    fn complete(&mut self) -> Result<Value> {
        let result = self.receipt.result.clone().ok_or_else(missing)?;
        // A previous process may have died after rename but before syncing its
        // parents. Reaffirm rename durability before recording completion.
        for directory in [
            &self.root,
            &self.root.join(".staging"),
            &self.root.join(".trash"),
        ] {
            plugin_bundle::sync_dir(directory)?;
        }
        if matches!(
            self.receipt.phase,
            Phase::PublishIntent | Phase::RemoveIntent
        ) {
            atomic_json(
                &self.root.join(flow_config::plugin_fs::PUBLICATION_REVISION),
                &self.receipt.operation_id,
            )?;
        }
        self.receipt.phase = Phase::Completed;
        self.save()?;
        crate::plugin_scan::invalidate(&self.root);
        Ok(result)
    }
    fn save(&self) -> Result<()> {
        let _ops = RootLease::operations(&self.root)?;
        self.save_unlocked()
    }
    /// Caller holds the exclusive publication lease. An orphaned rename intent
    /// must be reconciled before another ID can consume its type/capacity slot.
    /// Capturing/sealed preparations for other IDs may still run concurrently.
    fn publication_fence(&self) -> Result<()> {
        let _ops = RootLease::operations(&self.root)?;
        for entry in fs::read_dir(self.root.join(".ops"))? {
            let path = entry?.path();
            let key = path.file_name().unwrap_or_default().to_string_lossy();
            if key.len() != 64 || !key.bytes().all(|b| b.is_ascii_hexdigit()) {
                continue;
            }
            let receipt: Receipt = serde_json::from_slice(&fs::read(path)?)?;
            if receipt.operation_id != self.receipt.operation_id
                && matches!(
                    receipt.phase,
                    Phase::MoveOldIntent | Phase::PublishIntent | Phase::RemoveIntent
                )
            {
                return Err(OperationError::Conflict(format!(
                    "unfinished plugin operation {} for {}; retry its original request",
                    receipt.operation_id, receipt.plugin_id
                )));
            }
        }
        Ok(())
    }
    fn save_unlocked(&self) -> Result<()> {
        atomic_json(&self.root.join(".ops").join(&self.key), &self.receipt)
    }
}

pub fn validate_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 128
        || id.starts_with('.')
        || !plugin_bundle::normal_path(id)
        || Path::new(id).components().count() != 1
        || id.contains(['/', '\\'])
    {
        return Err(OperationError::Invalid(
            "plugin_id must be a single visible directory name".into(),
        ));
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct DrainMarker {
    generation: Option<String>,
    operation: String,
}

/// Fence new runs until publication is durable, including across a restart in
/// the middle of renaming the live directory.
pub(crate) fn is_draining(root: &Path, id: &str) -> Result<bool> {
    validate_id(id)?;
    let bytes = match fs::read(root.join(".draining").join(id)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let marker: DrainMarker = serde_json::from_slice(&bytes)?;
    if marker.operation.len() != 64 || !marker.operation.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(OperationError::Invalid(
            "invalid plugin drain receipt".into(),
        ));
    }
    let receipt: Receipt =
        serde_json::from_slice(&fs::read(root.join(".ops").join(marker.operation))?)?;
    if receipt.plugin_id != id {
        return Err(OperationError::Invalid(
            "plugin drain receipt belongs to another plugin".into(),
        ));
    }
    if receipt.phase == Phase::Completed && receipt.result.is_some() {
        return Ok(false);
    }
    let current = match fs::read(root.join(id).join(MARKER)) {
        Ok(bytes) => Some(serde_json::from_slice::<Identity>(&bytes)?.generation),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    Ok(current.is_none() || marker.generation == current)
}

impl Operation {
    pub(crate) fn mark_draining(&self) -> Result<()> {
        let _lease = flow_config::plugin_fs::RootLease::exclusive(self.root())?;
        self.record_drain(self.target_identity()?.as_ref())
    }

    /// Hold the exclusive publication lease when recording a drain.
    pub(crate) fn record_drain(&self, target: Option<&Identity>) -> Result<()> {
        let directory = self.root().join(".draining");
        create_durable_root(&directory)?;
        let marker = DrainMarker {
            generation: target.map(|identity| identity.generation.clone()),
            operation: self.key.clone(),
        };
        atomic_json(&directory.join(&self.receipt.plugin_id), &marker)
    }

    pub(crate) fn clear_draining(&self) -> Result<()> {
        let directory = self.root().join(".draining");
        match fs::remove_file(directory.join(&self.receipt.plugin_id)) {
            Ok(()) => Ok(plugin_bundle::sync_dir(&directory)?),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

fn identity(path: &Path, create: bool) -> Result<Option<Identity>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(superseded());
    }
    let marker = path.join(MARKER);
    let digest = plugin_bundle::digest(path)?;
    match fs::symlink_metadata(&marker) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(superseded());
            }
            let mut id: Identity = serde_json::from_slice(&fs::read(&marker)?)?;
            if id.bundle_digest != digest {
                if !create {
                    return Err(superseded());
                }
                // A fresh operation may adopt a manually edited legacy tree.
                // Recovery never takes this branch and checks the sealed digest.
                id.bundle_digest = digest;
                atomic_json(&marker, &id)?;
            }
            Ok(Some(id))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound && create => {
            let id = Identity {
                generation: uuid::Uuid::now_v7().to_string(),
                bundle_digest: digest,
            };
            atomic_json(&marker, &id)?;
            Ok(Some(id))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Err(missing()),
        Err(error) => Err(error.into()),
    }
}
fn matches_identity(path: &Path, expected: &Identity) -> Result<bool> {
    Ok(identity(path, false)?.as_ref() == Some(expected))
}
fn require_identity(path: &Path, expected: &Identity) -> Result<()> {
    if matches_identity(path, expected)? {
        Ok(())
    } else if !path.try_exists()? {
        Err(missing())
    } else {
        Err(superseded())
    }
}
fn missing() -> OperationError {
    OperationError::Conflict("plugin recovery evidence missing".into())
}
fn create_durable_root(root: &Path) -> Result<()> {
    let absolute = std::path::absolute(root)?;
    let mut missing = Vec::new();
    let mut existing = absolute.as_path();
    while !existing.try_exists()? {
        missing.push(existing.to_path_buf());
        existing = existing
            .parent()
            .ok_or_else(|| io::Error::other("plugin root has no existing ancestor"))?;
    }
    fs::create_dir_all(&absolute)?;
    // Persist each newly created directory and the ancestor entry linking it.
    for directory in missing {
        plugin_bundle::sync_dir(&directory)?;
    }
    plugin_bundle::sync_dir(existing)?;
    Ok(())
}
fn superseded() -> OperationError {
    OperationError::Conflict("plugin operation superseded".into())
}
fn durable_rename(from: &Path, to: &Path) -> Result<()> {
    #[cfg(test)]
    RENAME_FAILURE.with(|failures| {
        if failures.borrow_mut().pop_front().unwrap_or(false) {
            Err(io::Error::other("injected plugin rename failure"))
        } else {
            Ok(())
        }
    })?;
    fs::rename(from, to).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("rename {} -> {}: {error}", from.display(), to.display()),
        )
    })?;
    plugin_bundle::sync_dir(from.parent().ok_or_else(missing)?)?;
    plugin_bundle::sync_dir(to.parent().ok_or_else(missing)?)?;
    Ok(())
}
#[cfg(test)]
thread_local! { static RENAME_FAILURE: std::cell::RefCell<std::collections::VecDeque<bool>> = const { std::cell::RefCell::new(std::collections::VecDeque::new()) }; }
fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::now_v7()));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.flush()?;
    plugin_bundle::sync_file(&file)?;
    fs::rename(&temporary, path)?;
    plugin_bundle::sync_dir(path.parent().ok_or_else(missing)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn source(parent: &Path, name: &str) -> PathBuf {
        let path = parent.join(name);
        fs::create_dir(&path).unwrap();
        fs::write(path.join("plugin.toml"), "[plugin]\nid = \"demo\"\nversion = \"1\"\nentry = \"entry\"\nprotocol = 2\nnode_types = [\"custom\"]\n[methods]\ninvoke = \"invoke\"\n").unwrap();
        fs::write(path.join("entry"), b"executable").unwrap();
        fs::write(path.join("ui.html"), b"old ui").unwrap();
        path
    }
    fn begin(root: &Path, request: &str, method: &str) -> Operation {
        Operation::begin(root, method, request, &json!({"request":request}), "demo").unwrap()
    }
    fn sealed(root: &Path, source: &Path, request: &str) -> Operation {
        let mut op = begin(root, request, "plugins.install");
        let bundle = plugin_bundle::capture(source, &op.stage(), root).unwrap();
        op.seal(bundle.bundle_digest).unwrap();
        op
    }
    #[test]
    fn orphaned_rename_intent_fences_other_ids_before_they_consume_capacity() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins");
        let source = source(temp.path(), "source");
        let mut old = sealed(&root, &source, "old");
        let _lease = RootLease::exclusive(&root).unwrap();
        old.install_intent(None, json!({"ok":true})).unwrap();
        drop(old);
        let mut other =
            Operation::begin(&root, "plugins.uninstall", "other", &json!({}), "other").unwrap();
        let identity = Identity {
            generation: "unpublished".into(),
            bundle_digest: "unpublished".into(),
        };
        assert!(other
            .remove_intent(identity, json!({"ok":true}))
            .unwrap_err()
            .to_string()
            .contains("unfinished plugin operation"));
        assert!(!root.join("other").exists());
    }

    #[test]
    fn failed_publication_rolls_back_only_its_own_old_generation() {
        for rollback_fails in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("plugins");
            fs::create_dir(&root).unwrap();
            let old_source = source(temp.path(), "old");
            fs::rename(old_source, root.join("demo")).unwrap();
            let new_source = source(temp.path(), "new");
            fs::write(new_source.join("ui.html"), b"new ui").unwrap();
            let mut op = sealed(&root, &new_source, "rollback");
            let _lease = RootLease::exclusive(&root).unwrap();
            let old = op.target_identity().unwrap();
            op.install_intent(old, json!({"ok":true})).unwrap();
            RENAME_FAILURE.with(|fault| *fault.borrow_mut() = [false, true, rollback_fails].into());
            let error = op.recover().unwrap_err();
            RENAME_FAILURE.with(|fault| fault.borrow_mut().clear());
            assert_eq!(op.live().exists(), !rollback_fails);
            if rollback_fails {
                assert!(error.to_string().contains("rollback failed"));
                assert!(op.trash().join("entry").exists());
            } else {
                assert_eq!(fs::read(op.live().join("ui.html")).unwrap(), b"old ui");
            }
            drop(op);
            let mut retry = begin(&root, "rollback", "plugins.install");
            assert_eq!(retry.recover().unwrap(), json!({"ok":true}));
            assert_eq!(fs::read(retry.live().join("ui.html")).unwrap(), b"new ui");
        }
    }

    #[test]
    fn persistence_failures_at_each_publication_boundary_remain_recoverable() {
        let mut failures = 0;
        for boundary in 0..12 {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("plugins");
            let source = source(temp.path(), "source");
            let mut op = sealed(&root, &source, "durability");
            let _lease = RootLease::exclusive(&root).unwrap();
            plugin_bundle::SYNC_FAILURE.with(|fault| fault.set(Some(boundary)));
            let outcome = op
                .install_intent(None, json!({"ok":true}))
                .and_then(|_| op.recover());
            plugin_bundle::SYNC_FAILURE.with(|fault| fault.set(None));
            if outcome.is_err() {
                failures += 1;
            }
            drop(op);
            let mut retry = begin(&root, "durability", "plugins.install");
            if retry.receipt.phase == Phase::Sealed {
                retry.install_intent(None, json!({"ok":true})).unwrap();
            }
            assert_eq!(retry.recover().unwrap(), json!({"ok":true}));
            assert!(retry.live().join("entry").is_file());
            assert_eq!(retry.receipt.phase, Phase::Completed);
        }
        assert!(
            failures >= 6,
            "must exercise both file and parent-directory synchronization failures"
        );
    }

    #[test]
    fn published_intent_replays_after_source_disappears() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins");
        let source = source(temp.path(), "source");
        let mut op = sealed(&root, &source, "publish");
        let _lease = RootLease::exclusive(&root).unwrap();
        op.install_intent(None, json!({"installed":true})).unwrap();
        // Simulate process death after the live rename, before completed.
        fs::rename(op.stage(), op.live()).unwrap();
        drop(op);
        fs::remove_dir_all(source).unwrap();
        let mut retry = begin(&root, "publish", "plugins.install");
        assert_eq!(retry.recover().unwrap(), json!({"installed":true}));
        drop(retry);
        let replay = begin(&root, "publish", "plugins.install");
        assert_eq!(replay.replay().unwrap(), Some(json!({"installed":true})));
    }
    #[test]
    fn removal_recovery_never_removes_a_later_generation() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins");
        fs::create_dir(&root).unwrap();
        let source = source(temp.path(), "source");
        fs::rename(&source, root.join("demo")).unwrap();
        let mut op = begin(&root, "remove", "plugins.uninstall");
        let _lease = RootLease::exclusive(&root).unwrap();
        let old = op.target_identity().unwrap().unwrap();
        op.remove_intent(old, json!({"removed":true})).unwrap();
        fs::rename(op.live(), op.trash()).unwrap();
        // A later same-ID install with exactly the same package bytes.
        let later = source_fixture(temp.path());
        plugin_bundle::capture(&later, &op.live(), &root).unwrap();
        let new_identity = op.target_identity().unwrap().unwrap();
        drop(op);
        let mut retry = begin(&root, "remove", "plugins.uninstall");
        assert_eq!(retry.recover().unwrap(), json!({"removed":true}));
        assert_eq!(retry.target_identity().unwrap(), Some(new_identity));
    }
    fn source_fixture(parent: &Path) -> PathBuf {
        source(parent, "later")
    }
    #[test]
    fn replace_resumes_after_old_directory_move_using_only_sealed_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins");
        fs::create_dir(&root).unwrap();
        let old_source = source(temp.path(), "old");
        fs::rename(old_source, root.join("demo")).unwrap();
        let new_source = source(temp.path(), "new");
        fs::write(new_source.join("ui.html"), b"new ui").unwrap();
        let mut op = sealed(&root, &new_source, "replace");
        let _lease = RootLease::exclusive(&root).unwrap();
        let old = op.target_identity().unwrap();
        op.install_intent(old, json!({"replaced":true})).unwrap();
        fs::rename(op.live(), op.trash()).unwrap();
        drop(op);
        fs::write(new_source.join("ui.html"), b"mutated after sealing").unwrap();
        let mut retry = begin(&root, "replace", "plugins.install");
        retry.recover().unwrap();
        assert_eq!(fs::read(retry.live().join("ui.html")).unwrap(), b"new ui");
    }
    #[test]
    fn missing_evidence_and_superseded_generation_fail_closed() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins");
        let source = source(temp.path(), "source");
        let mut op = sealed(&root, &source, "missing");
        let _lease = RootLease::exclusive(&root).unwrap();
        op.install_intent(None, json!({"ok":true})).unwrap();
        fs::remove_dir_all(op.stage()).unwrap();
        assert!(op
            .recover()
            .unwrap_err()
            .to_string()
            .contains("evidence missing"));
        plugin_bundle::capture(&source, &op.live(), &root).unwrap();
        op.target_identity().unwrap(); // Assign another generation, even though bytes match.
        assert!(op.recover().unwrap_err().to_string().contains("superseded"));
        assert!(op.live().exists());
    }
    #[test]
    fn receipts_fence_same_id_and_parameter_conflicts_before_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins");
        let op = begin(&root, "first", "plugins.install");
        assert!(Operation::begin(&root, "plugins.install", "first", &json!({}), "demo").is_err());
        drop(op);
        let error = Operation::begin(&root, "plugins.install", "first", &json!({}), "demo")
            .err()
            .unwrap();
        assert!(error.to_string().contains("different arguments"));
        let error = Operation::begin(&root, "plugins.uninstall", "second", &json!({}), "demo")
            .err()
            .unwrap();
        assert!(error.to_string().contains("unfinished plugin operation"));
        assert!(!root.join("demo").exists());
    }
    #[test]
    fn bundle_digest_detects_ui_changes_without_changing_grant_identity() {
        let temp = tempfile::tempdir().unwrap();
        let source = source(temp.path(), "source");
        let old = plugin_bundle::inspect(&source).unwrap();
        fs::write(source.join("ui.html"), b"new ui").unwrap();
        let new = plugin_bundle::inspect(&source).unwrap();
        assert_eq!(old.content_hash, new.content_hash);
        assert_ne!(old.bundle_digest, new.bundle_digest);
    }
    #[cfg(unix)]
    #[test]
    fn capture_rejects_symlinks_reserved_markers_and_recursive_sources() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins");
        fs::create_dir(&root).unwrap();
        let source = source(temp.path(), "source");
        std::os::unix::fs::symlink("entry", source.join("link")).unwrap();
        assert!(plugin_bundle::capture(&source, &root.join("stage"), &root).is_err());
        fs::remove_file(source.join("link")).unwrap();
        fs::write(source.join(MARKER), b"reserved").unwrap();
        assert!(plugin_bundle::capture(&source, &root.join("stage"), &root).is_err());
        assert!(plugin_bundle::capture(temp.path(), &root.join("stage"), &root).is_err());
    }
}
