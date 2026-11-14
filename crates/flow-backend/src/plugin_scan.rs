//! One immutable discovery result shared by list, routing and validation.
use crate::plugin_registry::DiscoveredPlugin;
use flow_config::plugin_manifest::{parse, FieldDecl, PluginManifest};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{self, Read},
    path::{Path, PathBuf},
};

pub struct ScanRow {
    pub id: String,
    pub directory_name: String,
    pub source: PathBuf,
    pub load_state: &'static str,
    pub load_error: Option<String>,
    pub entry_contained: bool,
    pub entry_symlink: bool,
    pub entry_oversize: bool,
    pub manifest: Option<PluginManifest>,
    pub plugin: Option<DiscoveredPlugin>,
}
impl ScanRow {
    pub fn annotate(&self, mut value: Value) -> Value {
        value["id"] = json!(self.id);
        value["source"] = json!(self.source);
        value["directory_name"] = json!(self.directory_name);
        value["id_matches_directory"] = json!(self.id == self.directory_name);
        value["load_state"] = json!(self.load_state);
        value["load_error"] = json!(self.load_error);
        value["entry_contained"] = json!(self.entry_contained);
        value["entry_symlink"] = json!(self.entry_symlink);
        value["entry_oversize"] = json!(self.entry_oversize);
        value["service"] = json!(self.manifest.as_ref().is_some_and(|m| m.service.is_some()));
        value
    }
}
pub struct PluginSnapshot {
    pub rows: Vec<ScanRow>,
}
impl PluginSnapshot {
    pub fn dynamic_types(&self) -> BTreeSet<String> {
        self.rows
            .iter()
            .filter_map(|r| r.plugin.as_ref())
            .flat_map(|p| p.manifest.plugin.node_types.iter().cloned())
            .filter(|name| flow_engine::NodeType::parse(name).is_none())
            .collect()
    }
    pub fn forms(&self) -> BTreeMap<String, Vec<FieldDecl>> {
        self.rows
            .iter()
            .filter_map(|r| r.plugin.as_ref())
            .flat_map(|p| p.manifest.ui.params_forms.iter())
            .filter(|f| flow_engine::NodeType::parse(&f.node_type).is_none())
            .map(|f| (f.node_type.clone(), f.fields.clone()))
            .collect()
    }
}
/// Blocking, caller must already own a publication lease. `defaults` describes actual executor defaults.
pub fn scan_unlocked(
    root: &Path,
    defaults: &BTreeMap<String, String>,
) -> io::Result<PluginSnapshot> {
    let mut directories = std::fs::read_dir(root)?
        .map(|e| e.map(|e| e.path()))
        .collect::<io::Result<Vec<_>>>()?;
    directories.retain(|p| {
        p.is_dir()
            && !p
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .starts_with('.')
    });
    directories.sort();
    let mut rows = Vec::new();
    for directory in directories {
        let path = directory.join("plugin.toml");
        if !path.is_file() {
            continue;
        }
        let name = directory
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        let mut row = ScanRow {
            id: name.clone(),
            directory_name: name,
            source: directory.clone(),
            load_state: "broken",
            load_error: None,
            entry_contained: false,
            entry_symlink: false,
            entry_oversize: false,
            manifest: None,
            plugin: None,
        };
        let result = (|| -> Result<DiscoveredPlugin, String> {
            let mut bytes = Vec::new();
            std::fs::File::open(&path)
                .map_err(|e| e.to_string())?
                .take(262145)
                .read_to_end(&mut bytes)
                .map_err(|e| e.to_string())?;
            if bytes.len() > 262144 {
                return Err("manifest exceeds 256 KiB".into());
            }
            let manifest = parse(std::str::from_utf8(&bytes).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            row.id = manifest.plugin.id.clone();
            row.manifest = Some(manifest.clone());
            if manifest.plugin.protocol >> 16 != flow_plugin_sdk::PROTOCOL_VERSION >> 16 {
                return Err("protocol major mismatch".into());
            }
            if manifest.plugin.node_types.iter().any(|name| {
                flow_engine::NodeType::parse(name).is_some_and(|kind| {
                    !matches!(
                        kind,
                        flow_engine::NodeType::Harness | flow_engine::NodeType::Llm
                    )
                })
            }) {
                return Err("node_types contains builtin type".into());
            }
            let entry = PathBuf::from(&manifest.plugin.entry);
            let entry = if entry.is_absolute() {
                entry
            } else {
                directory.join(entry)
            };
            let metadata = std::fs::metadata(&entry).map_err(|e| e.to_string())?;
            if !metadata.is_file() {
                return Err("entry is not a file".into());
            }
            row.entry_symlink = std::fs::symlink_metadata(&entry)
                .map_err(|e| e.to_string())?
                .file_type()
                .is_symlink();
            row.entry_contained = entry
                .canonicalize()
                .map_err(|e| e.to_string())?
                .starts_with(directory.canonicalize().map_err(|e| e.to_string())?);
            row.entry_oversize = metadata.len() > 256 * 1024 * 1024;
            let mut hash = Sha256::new();
            hash.update(&bytes);
            let mut file = std::fs::File::open(entry).map_err(|e| e.to_string())?;
            let mut buffer = [0u8; 65536];
            loop {
                let n = file.read(&mut buffer).map_err(|e| e.to_string())?;
                if n == 0 {
                    break;
                }
                hash.update(&buffer[..n]);
            }
            Ok(DiscoveredPlugin {
                manifest,
                source: directory,
                content_hash: hex::encode(hash.finalize()),
            })
        })();
        match result {
            Ok(plugin) => {
                row.load_state = "loaded";
                row.plugin = Some(plugin);
            }
            Err(e) => row.load_error = Some(e),
        }
        rows.push(row);
    }
    let mut counts = BTreeMap::new();
    for row in &rows {
        if row.manifest.is_some() {
            *counts.entry(row.id.clone()).or_insert(0usize) += 1;
        }
    }
    let mut occupied = defaults.clone();
    for row in &mut rows {
        if counts.get(&row.id).is_some_and(|n| *n > 1) {
            row.load_state = "conflict";
            row.load_error = Some("duplicate plugin id".into());
            row.plugin = None;
            continue;
        }
        let Some(plugin) = &row.plugin else {
            continue;
        };
        if let Some(other) = plugin
            .manifest
            .plugin
            .node_types
            .iter()
            .filter_map(|n| occupied.get(n))
            .find(|id| *id != &row.id)
        {
            row.load_state = "broken";
            row.load_error = Some(format!("node type overlaps plugin {other}"));
            row.plugin = None;
            continue;
        }
        for name in &plugin.manifest.plugin.node_types {
            occupied.insert(name.clone(), row.id.clone());
        }
    }
    Ok(PluginSnapshot { rows })
}
pub fn scan(root: &Path, defaults: &BTreeMap<String, String>) -> io::Result<PluginSnapshot> {
    let _lease = flow_config::plugin_fs::RootLease::shared(root)?;
    scan_unlocked(root, defaults)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn plugin(root: &Path, directory: &str, id: &str, node: &str) {
        let path = root.join(directory);
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("entry"), b"plugin bytes").unwrap();
        std::fs::write(path.join("plugin.toml"),format!("[plugin]\nid={id:?}\nversion=\"1\"\nentry=\"entry\"\nprotocol=2\nnode_types=[{node:?}]\n[methods]\ninvoke=\"invoke\"\n")).unwrap();
    }
    #[test]
    fn duplicate_ids_disable_both_and_different_ids_keep_first_owner() {
        let root = flow_test_support::io::TempDir::new("plugin-scan-conflicts");
        plugin(root.path(), "a", "duplicate", "first-node");
        plugin(root.path(), "b", "duplicate", "second-node");
        plugin(root.path(), "c", "first", "shared-node");
        plugin(root.path(), "d", "second", "shared-node");
        let snapshot = scan(root.path(), &BTreeMap::new()).unwrap();
        assert_eq!(
            snapshot
                .rows
                .iter()
                .map(|r| r.load_state)
                .collect::<Vec<_>>(),
            vec!["conflict", "conflict", "loaded", "broken"]
        );
        assert_eq!(
            snapshot.dynamic_types(),
            BTreeSet::from(["shared-node".into()])
        );
        let loaded = snapshot.rows[2].plugin.as_ref().unwrap();
        assert_eq!(
            loaded.content_hash,
            crate::plugin_registry::content_hash(
                &std::fs::read(root.path().join("c/plugin.toml")).unwrap(),
                b"plugin bytes"
            )
        );
    }
    #[test]
    fn malformed_and_oversized_manifests_are_visible_and_hidden_directories_ignored() {
        let root = flow_test_support::io::TempDir::new("plugin-scan-broken");
        plugin(root.path(), "a", "valid", "custom-node");
        plugin(root.path(), ".hidden", "hidden", "hidden-node");
        std::fs::create_dir(root.path().join("broken")).unwrap();
        std::fs::write(root.path().join("broken/plugin.toml"), b"invalid").unwrap();
        std::fs::create_dir(root.path().join("large")).unwrap();
        std::fs::write(root.path().join("large/plugin.toml"), vec![b'x'; 262145]).unwrap();
        let snapshot = scan(
            root.path(),
            &BTreeMap::from([("custom-node".into(), "default-owner".into())]),
        )
        .unwrap();
        assert_eq!(snapshot.rows.len(), 3);
        assert!(snapshot.rows.iter().all(|r| r.plugin.is_none()));
        assert!(snapshot.rows[0]
            .load_error
            .as_ref()
            .unwrap()
            .contains("default-owner"));
        assert!(snapshot.rows[2]
            .load_error
            .as_ref()
            .unwrap()
            .contains("256 KiB"));
    }
    #[cfg(unix)]
    #[test]
    fn legacy_external_entry_is_annotated_without_removing_its_route() {
        let root = flow_test_support::io::TempDir::new("plugin-scan-legacy");
        plugin(root.path(), "legacy", "legacy", "legacy-node");
        let entry = root.path().join("legacy/entry");
        std::fs::remove_file(&entry).unwrap();
        std::fs::write(root.path().join("external"), b"legacy").unwrap();
        std::os::unix::fs::symlink("../external", entry).unwrap();
        let snapshot = scan(root.path(), &BTreeMap::new()).unwrap();
        let row = &snapshot.rows[0];
        assert!(row.plugin.is_some());
        assert!(row.entry_symlink);
        assert!(!row.entry_contained);
    }
}
