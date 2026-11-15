//! Strict capture for managed installs. Legacy discovery deliberately uses looser rules.
use flow_config::plugin_manifest::{parse, PluginManifest};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, Read, Write},
    path::{Component, Path},
};

pub(crate) const MARKER: &str = ".flow-install.json";
pub(crate) const MANIFEST_LIMIT: u64 = 262144;
const FILE_LIMIT: u64 = 256 * 1024 * 1024;
const TREE_LIMIT: u64 = 512 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum BundleError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Limit(String),
    #[error("{0}")]
    Io(#[from] io::Error),
}
pub(crate) type Result<T> = std::result::Result<T, BundleError>;

pub struct SealedBundle {
    pub manifest: PluginManifest,
    pub content_hash: String,
    pub bundle_digest: String,
    pub entry_executable: bool,
}

pub(crate) fn normal_path(value: &str) -> bool {
    !value.is_empty()
        && Path::new(value)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
}

pub(crate) fn bounded_manifest(directory: &Path) -> Result<(Vec<u8>, PluginManifest)> {
    let mut bytes = Vec::new();
    fs::File::open(directory.join("plugin.toml"))?
        .take(MANIFEST_LIMIT + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MANIFEST_LIMIT {
        return Err(BundleError::Limit("manifest exceeds 256 KiB".into()));
    }
    let manifest =
        parse(std::str::from_utf8(&bytes).map_err(|e| BundleError::Invalid(e.to_string()))?)
            .map_err(|_| BundleError::Invalid("invalid plugin manifest".into()))?;
    Ok((bytes, manifest))
}

/// The destination must be a fresh operation-owned path on the plugin filesystem.
pub fn capture(source: &Path, destination: &Path, root: &Path) -> Result<SealedBundle> {
    let metadata = fs::symlink_metadata(source)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(BundleError::Invalid(
            "source must be a regular directory".into(),
        ));
    }
    let source = source.canonicalize()?;
    let root = root.canonicalize()?;
    // An ancestor would recursively copy the staging directory into itself.
    if source.starts_with(&root) || root.starts_with(&source) {
        return Err(BundleError::Invalid(
            "source and plugin root must not contain each other".into(),
        ));
    }
    fs::create_dir(destination)?;
    let outcome = (|| {
        copy_tree(&source, destination, &mut Budget::default())?;
        let bundle = inspect(destination)?;
        sync_dir(destination)?;
        sync_dir(
            destination
                .parent()
                .ok_or_else(|| io::Error::other("missing staging parent"))?,
        )?;
        Ok(bundle)
    })();
    if outcome.is_err() {
        // No live path has been touched. Preserve the original error; a leftover hidden
        // operation path can be removed by its owner on retry.
        let _ = fs::remove_dir_all(destination);
    }
    outcome
}

#[derive(Default)]
struct Budget {
    files: usize,
    bytes: u64,
}
impl Budget {
    fn file(&mut self, len: u64) -> Result<()> {
        self.files += 1;
        self.bytes = self
            .bytes
            .checked_add(len)
            .ok_or_else(|| BundleError::Limit("bundle size overflow".into()))?;
        if self.files > 4096 || len > FILE_LIMIT || self.bytes > TREE_LIMIT {
            return Err(BundleError::Limit(
                "bundle exceeds file count or size limits".into(),
            ));
        }
        Ok(())
    }
}

fn copy_tree(source: &Path, destination: &Path, budget: &mut Budget) -> Result<()> {
    for entry in sorted_entries(source)? {
        let metadata = fs::symlink_metadata(&entry)?;
        let name = entry
            .file_name()
            .ok_or_else(|| io::Error::other("missing file name"))?;
        if name == MARKER {
            return Err(BundleError::Invalid(
                "source contains reserved .flow-install.json".into(),
            ));
        }
        let target = destination.join(name);
        if metadata.is_dir() {
            fs::create_dir(&target)?;
            copy_tree(&entry, &target, budget)?;
            fs::set_permissions(&target, metadata.permissions())?;
            sync_dir(&target)?;
        } else if metadata.is_file() {
            budget.file(metadata.len())?;
            // Bound the actual read as well as metadata: a source file may grow during capture.
            let mut source = fs::File::open(&entry)?.take(FILE_LIMIT + 1);
            let mut target = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&target)?;
            let bytes = io::copy(&mut source, &mut target)?;
            if bytes > FILE_LIMIT {
                return Err(BundleError::Limit("file exceeds 256 MiB".into()));
            }
            if bytes > metadata.len() {
                budget.bytes += bytes - metadata.len();
                if budget.bytes > TREE_LIMIT {
                    return Err(BundleError::Limit("bundle exceeds 512 MiB".into()));
                }
            }
            target.set_permissions(metadata.permissions())?;
            target.flush()?;
            sync_file(&target)?;
        } else {
            return Err(BundleError::Invalid(
                "bundle contains non-regular entry".into(),
            ));
        }
    }
    Ok(())
}

pub fn inspect(directory: &Path) -> Result<SealedBundle> {
    let (bytes, manifest) = bounded_manifest(directory)?;
    if manifest.plugin.id.len() > 64 || !normal_path(&manifest.plugin.entry) {
        return Err(BundleError::Invalid(
            "install requires id <= 64 bytes and a normal relative entry path".into(),
        ));
    }
    let entry = directory.join(&manifest.plugin.entry);
    let metadata = fs::symlink_metadata(&entry)?;
    if !metadata.is_file() || !entry.canonicalize()?.starts_with(directory.canonicalize()?) {
        return Err(BundleError::Invalid(
            "entry must be a contained regular file".into(),
        ));
    }
    if manifest.plugin.protocol >> 16 != flow_plugin_sdk::PROTOCOL_VERSION >> 16 {
        return Err(BundleError::Invalid("protocol major mismatch".into()));
    }
    if manifest.plugin.node_types.iter().any(|name| {
        flow_engine::NodeType::parse(name).is_some_and(|kind| {
            !matches!(
                kind,
                flow_engine::NodeType::Harness | flow_engine::NodeType::Llm
            )
        })
    }) {
        return Err(BundleError::Invalid(
            "node_types contains builtin type".into(),
        ));
    }
    if let Some(ui) = manifest.service.as_ref().and_then(|s| s.web_ui.as_ref()) {
        let ui = directory.join(ui);
        let metadata = fs::symlink_metadata(&ui)?;
        if !metadata.is_file() || metadata.len() > MANIFEST_LIMIT {
            return Err(BundleError::Invalid(
                "web-ui must be a regular file under 256 KiB".into(),
            ));
        }
    }
    let mut hash = Sha256::new();
    hash.update(bytes);
    hash_file(&entry, &mut hash)?;
    #[cfg(unix)]
    let executable = {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o100 != 0
    };
    #[cfg(not(unix))]
    let executable = true;
    Ok(SealedBundle {
        manifest,
        content_hash: hex::encode(hash.finalize()),
        bundle_digest: digest(directory)?,
        entry_executable: executable,
    })
}

/// Includes directory entries, relative paths, executable bits and every byte,
/// including web-ui. The host generation marker is deliberately excluded.
pub(crate) fn digest(directory: &Path) -> Result<String> {
    let mut hash = Sha256::new();
    hash.update(b"flow-plugin-bundle-v1\0");
    digest_tree(directory, directory, &mut hash)?;
    Ok(hex::encode(hash.finalize()))
}
fn digest_tree(root: &Path, directory: &Path, hash: &mut Sha256) -> Result<()> {
    for path in sorted_entries(directory)? {
        if path == root.join(MARKER) {
            continue;
        }
        let metadata = fs::symlink_metadata(&path)?;
        let relative = path
            .strip_prefix(root)
            .map_err(|e| BundleError::Invalid(e.to_string()))?;
        let relative = relative.as_os_str().as_encoded_bytes();
        hash.update((relative.len() as u64).to_le_bytes());
        hash.update(relative);
        #[cfg(unix)]
        let mode = {
            use std::os::unix::fs::PermissionsExt;
            metadata.permissions().mode() & 0o111
        };
        #[cfg(not(unix))]
        let mode = 0u32;
        hash.update(mode.to_le_bytes());
        if metadata.is_dir() {
            hash.update(b"d");
            digest_tree(root, &path, hash)?;
        } else if metadata.is_file() {
            hash.update(b"f");
            hash.update(metadata.len().to_le_bytes());
            hash_file(&path, hash)?;
        } else if metadata.file_type().is_symlink() {
            // Legacy trees may contain links. Hash the link itself, never its target;
            // new captures reject links before reaching this code.
            hash.update(b"l");
            let link = fs::read_link(&path)?;
            let link = link.as_os_str().as_encoded_bytes();
            hash.update((link.len() as u64).to_le_bytes());
            hash.update(link);
        } else {
            #[cfg(unix)]
            {
                // Old trees may contain sockets/devices/FIFOs. Recovery identifies
                // their directory entries without opening a device or blocking on
                // a pipe. Strict new captures still reject every special entry.
                use std::os::unix::fs::MetadataExt;
                hash.update(b"s");
                hash.update((metadata.mode() & u32::from(libc::S_IFMT)).to_le_bytes());
                hash.update(metadata.rdev().to_le_bytes());
            }
            #[cfg(not(unix))]
            return Err(BundleError::Invalid(
                "bundle contains non-regular entry".into(),
            ));
        }
    }
    Ok(())
}
fn hash_file(path: &Path, hash: &mut Sha256) -> io::Result<()> {
    let mut file = fs::File::open(path)?;
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            return Ok(());
        }
        hash.update(&buffer[..n]);
    }
}
fn sorted_entries(path: &Path) -> io::Result<Vec<std::path::PathBuf>> {
    let mut entries = fs::read_dir(path)?
        .map(|e| e.map(|e| e.path()))
        .collect::<io::Result<Vec<_>>>()?;
    entries.sort();
    Ok(entries)
}
pub(crate) fn sync_dir(path: &Path) -> io::Result<()> {
    sync_file(&fs::File::open(path)?)
}
pub(crate) fn sync_file(file: &fs::File) -> io::Result<()> {
    #[cfg(test)]
    SYNC_FAILURE.with(|fault| {
        if let Some(remaining) = fault.get() {
            if remaining == 0 {
                fault.set(None);
                return Err(io::Error::other("injected plugin persistence failure"));
            }
            fault.set(Some(remaining - 1));
        }
        Ok(())
    })?;
    file.sync_all()
}
#[cfg(test)]
thread_local! { pub(crate) static SYNC_FAILURE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) }; }

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn oversize_file_is_rejected_before_copy_and_partial_stage_is_removed() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("plugins");
        let source = temp.path().join("source");
        fs::create_dir(&root).unwrap();
        fs::create_dir(&source).unwrap();
        let file = fs::File::create(source.join("huge")).unwrap();
        file.set_len(FILE_LIMIT + 1).unwrap();
        let stage = root.join("stage");
        assert!(matches!(
            capture(&source, &stage, &root),
            Err(BundleError::Limit(_))
        ));
        assert!(!stage.exists());
    }
    #[test]
    fn normal_paths_reject_traversal_absolute_and_empty_names() {
        for path in ["", "/bin/entry", "../entry", "bin/../entry", "./entry"] {
            assert!(!normal_path(path), "{path}");
        }
        assert!(normal_path("bin/entry"));
    }
}
