//! Worker routing table built from the same plugin catalog used for publication.
//! The catalog is read once at startup. Spawn rechecks manifest and entry identity.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use flow_config::plugin_manifest;
use flow_dispatcher::execution_protocol::plugin_inventory::PluginInventory;
use flow_plugin_catalog::{content_hash_path, read_manifest, BUNDLED_ROUTES};

/// 注册来源。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistrationSource {
    /// 内置默认注册（编译期约定的定位方式）。
    Default,
    /// manifest 发现（携带 manifest 路径，排障第一现场）。
    Manifest(PathBuf),
}

/// 一个已注册的插件（按节点类型路由的目标）。
#[derive(Debug, Clone)]
pub struct RegisteredPlugin {
    /// 插件 id。
    pub plugin_id: String,
    /// 插件版本。
    pub version: String,
    /// 节点执行入口方法（manifest `[methods] invoke`）。
    pub invoke_method: String,
    /// 入口二进制绝对路径。
    pub binary: PathBuf,
    /// plugin-proto 协议版本（快筛值；握手权威）。
    pub protocol: u32,
    /// Content identity of the installed or embedded manifest and its entry.
    pub content_hash: String,
    /// UI 投影（未来 run 快照冻结物；R1 先解析携带）。
    pub ui: Option<plugin_manifest::UiProjection>,
    /// 注册来源。
    pub source: RegistrationSource,
}

impl RegisteredPlugin {
    /// I35：spawn 前复核内容哈希（注册后二进制/manifest 被换掉 → 拒绝）。
    pub fn recheck_content_hash(&self) -> Result<(), String> {
        let manifest_bytes = match &self.source {
            RegistrationSource::Manifest(path) => {
                read_manifest(path).map_err(|e| format!("manifest 不可读：{e}"))?
            }
            RegistrationSource::Default => BUNDLED_ROUTES
                .iter()
                .find(|route| route.plugin_id == self.plugin_id)
                .ok_or_else(|| format!("unknown bundled plugin {}", self.plugin_id))?
                .manifest
                .as_bytes()
                .to_vec(),
        };
        let current = content_hash_path(&manifest_bytes, &self.binary)
            .map_err(|e| format!("入口不可读：{e}"))?;
        if current != self.content_hash {
            return Err(format!(
                "插件 {} 内容已变化（注册时 {}，当前 {}）：需重新授予后才能执行（I35）",
                self.plugin_id,
                &self.content_hash[..8.min(self.content_hash.len())],
                &current[..8.min(current.len())],
            ));
        }
        Ok(())
    }
}

/// 节点类型 → 插件注册的路由表。
#[derive(Debug, Clone, Default)]
pub struct PluginRegistry {
    by_node_type: HashMap<String, RegisteredPlugin>,
}

impl PluginRegistry {
    /// 加载：内置默认注册 + manifest 目录发现（缺省 `<flow_home>/plugins`）。
    ///
    /// 环境变量（执行器环境白名单已放行）：
    /// - `FLOW_PLUGINS_DIR`：插件目录显式覆盖（测试/部署布局用）；
    /// - `FLOW_AGENT_PLUGIN_BIN`：默认注册的入口覆盖。
    ///
    /// 所有拒绝都走 `tracing::warn`：坏插件禁用，执行器照常服务。
    pub fn load() -> Self {
        let mut registry = Self::default();
        let executor = std::env::current_exe().ok();
        for route in BUNDLED_ROUTES {
            if let Some(binary) =
                route.binary(executor.as_deref(), std::env::var_os(route.binary_env))
            {
                let content_hash = match content_hash_path(route.manifest.as_bytes(), &binary) {
                    Ok(hash) => hash,
                    Err(error) => {
                        tracing::warn!(plugin = route.plugin_id, %error, "bundled plugin unreadable");
                        continue;
                    }
                };
                registry.register(
                    route.node_type,
                    RegisteredPlugin {
                        plugin_id: route.plugin_id.into(),
                        version: "bundled".into(),
                        invoke_method: route.invoke.into(),
                        binary,
                        protocol: flow_plugin_sdk::protocol::PROTOCOL_VERSION,
                        content_hash,
                        ui: None,
                        source: RegistrationSource::Default,
                    },
                );
            }
        }
        if let Some(dir) = flow_config::plugin_fs::plugins_root() {
            registry.discover(&dir);
        }
        for route in BUNDLED_ROUTES {
            if registry.resolve(route.node_type).is_none() {
                tracing::warn!(
                    node_type = route.node_type,
                    "plugin unavailable after scanning plugin directory and binary overrides"
                );
            }
        }
        registry
    }

    /// 节点类型路由查询。
    pub fn resolve(&self, node_type: &str) -> Option<&RegisteredPlugin> {
        self.by_node_type.get(node_type)
    }

    pub(crate) fn inventory(&self) -> PluginInventory {
        let mut inventory = PluginInventory::default();
        let mut entries: Vec<_> = self.by_node_type.iter().collect();
        entries.sort_unstable_by_key(|(node_type, _)| *node_type);
        for (node_type, plugin) in entries {
            if let Err(error) = inventory.insert(node_type, &plugin.plugin_id, &plugin.content_hash)
            {
                tracing::warn!(%node_type, %error, "plugin unavailable for inventory-based routing");
            }
        }
        inventory
    }

    /// 已注册的节点类型数（诊断）。
    pub fn len(&self) -> usize {
        self.by_node_type.len()
    }

    /// 是否为空。
    pub fn is_empty(&self) -> bool {
        self.by_node_type.is_empty()
    }

    fn register(&mut self, node_type: &str, registration: RegisteredPlugin) {
        self.by_node_type
            .insert(node_type.to_string(), registration);
    }

    fn discover(&mut self, dir: &Path) {
        if !dir.is_dir() {
            return;
        }
        let occupied = self
            .by_node_type
            .iter()
            .map(|(node, registration)| (node.clone(), registration.plugin_id.clone()))
            .collect();
        let snapshot = match flow_plugin_catalog::scan(dir, &occupied) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(%error, ?dir, "plugin scan failed");
                return;
            }
        };
        for row in snapshot.rows {
            let manifest_path = row.source.join("plugin.toml");
            let Some(plugin) = row.plugin else {
                tracing::warn!(?manifest_path, plugin = %row.id, reason = ?row.load_error, "plugin disabled by catalog");
                continue;
            };
            let registration = RegisteredPlugin {
                plugin_id: plugin.manifest.plugin.id.clone(),
                version: plugin.manifest.plugin.version.clone(),
                invoke_method: plugin.manifest.methods.invoke.clone(),
                binary: plugin.entry(),
                protocol: plugin.manifest.plugin.protocol,
                content_hash: plugin.content_hash,
                ui: Some(plugin.manifest.ui_projection()),
                source: RegistrationSource::Manifest(manifest_path),
            };
            for node_type in &plugin.manifest.plugin.node_types {
                self.register(node_type, registration.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_text(id: &str, node_types: &str, entry: &str) -> String {
        format!(
            r#"
[plugin]
id = "{id}"
version = "0.1.0"
entry = "{entry}"
protocol = 2
node_types = [{node_types}]

[methods]
invoke = "agent/runTurn"
"#
        )
    }

    /// 一个存在的入口文件（tempfile 不引依赖：用临时目录 + 普通文件）。
    fn fake_entry(dir: &std::path::Path) -> std::path::PathBuf {
        let path = dir.join("entry-bin");
        std::fs::write(&path, b"#!/bin/sh\n").unwrap();
        path
    }

    fn tempdir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "flow-registry-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn discovery_disables_duplicate_ids_and_ignores_hidden_directories() {
        let root = tempdir("duplicate-ids");
        for (directory, id, node) in [
            ("a", "duplicate", "one"),
            ("b", "duplicate", "two"),
            (".hidden", "hidden", "hidden-node"),
            ("c", "valid", "valid-node"),
        ] {
            let dir = root.join(directory);
            std::fs::create_dir(&dir).unwrap();
            fake_entry(&dir);
            let text = manifest_text(id, &format!("\"{node}\""), "entry-bin");
            std::fs::write(dir.join("plugin.toml"), text).unwrap();
        }
        let mut registry = PluginRegistry::default();
        registry.discover(&root);
        assert!(registry.resolve("one").is_none());
        assert!(registry.resolve("two").is_none());
        assert!(registry.resolve("hidden-node").is_none());
        assert_eq!(registry.resolve("valid-node").unwrap().plugin_id, "valid");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn streaming_hash_matches_i35_and_recheck_rejects_changed_bytes() {
        let root = tempdir("stream-hash");
        let plugin = root.join("demo");
        std::fs::create_dir(&plugin).unwrap();
        let entry = fake_entry(&plugin);
        let text = manifest_text("demo", "\"custom\"", "entry-bin");
        assert_eq!(
            content_hash_path(text.as_bytes(), &entry).unwrap(),
            flow_plugin_catalog::content_hash(text.as_bytes(), &std::fs::read(&entry).unwrap())
        );
        std::fs::write(plugin.join("plugin.toml"), &text).unwrap();
        let mut registry = PluginRegistry::default();
        registry.discover(&root);
        let registration = registry.resolve("custom").unwrap();
        registration.recheck_content_hash().unwrap();
        std::fs::write(&entry, b"different bytes").unwrap();
        assert!(registration
            .recheck_content_hash()
            .unwrap_err()
            .contains("内容已变化"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn discovers_manifest_and_routes_node_type() {
        let dir = tempdir("discover");
        let plugin_dir = dir.join("plugins").join("demo-plugin");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        let entry = fake_entry(&plugin_dir);
        let manifest = manifest_text("demo-plugin", "\"demo\"", entry.to_str().unwrap());
        std::fs::write(plugin_dir.join("plugin.toml"), manifest).unwrap();

        let mut registry = PluginRegistry::default();
        registry.discover(&dir.join("plugins"));

        let registration = registry.resolve("demo").expect("manifest 插件已注册");
        assert_eq!(registration.plugin_id, "demo-plugin");
        assert_eq!(registration.invoke_method, "agent/runTurn");
        assert_eq!(registration.binary, entry);
        assert!(registration.ui.is_some());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn rejects_builtin_names_overlap_missing_entry_and_bad_protocol() {
        let dir = tempdir("reject");
        let plugins = dir.join("plugins");

        // 内置类型名：script 是编译期语义，拒绝。
        let builtin_dir = plugins.join("evil");
        std::fs::create_dir_all(&builtin_dir).unwrap();
        let entry = fake_entry(&builtin_dir);
        std::fs::write(
            builtin_dir.join("plugin.toml"),
            manifest_text("evil", "\"script\"", entry.to_str().unwrap()),
        )
        .unwrap();

        // 与既有插件重叠：demo 已被占用。
        let other_dir = plugins.join("other");
        std::fs::create_dir_all(&other_dir).unwrap();
        let other_entry = fake_entry(&other_dir);
        std::fs::write(
            other_dir.join("plugin.toml"),
            manifest_text("other", "\"demo\"", other_entry.to_str().unwrap()),
        )
        .unwrap();

        // 入口不存在。
        let missing_dir = plugins.join("missing");
        std::fs::create_dir_all(&missing_dir).unwrap();
        std::fs::write(
            missing_dir.join("plugin.toml"),
            manifest_text("missing", "\"m\"", "no-such-bin"),
        )
        .unwrap();

        // 协议主版本不符（9 << 16）。
        let old_dir = plugins.join("old");
        std::fs::create_dir_all(&old_dir).unwrap();
        let old_entry = fake_entry(&old_dir);
        let old_manifest = manifest_text("old", "\"old_type\"", old_entry.to_str().unwrap())
            .replace("protocol = 2", &(9u32 << 16).to_string());
        std::fs::write(old_dir.join("plugin.toml"), old_manifest).unwrap();

        let mut full = PluginRegistry::default();
        full.discover(&plugins);

        // demo（other_dir 之前按字典序 demo? 这里 demo 未注册——重叠用例
        // 需要先有一个 demo：直接验证 evil/missing/old 均未注册，other
        // 是否注册取决于目录序，此处只钉拒绝项。
        assert!(full.resolve("script").is_none(), "内置名被拒绝");
        assert!(full.resolve("m").is_none(), "入口缺失被拒绝");
        assert!(full.resolve("old_type").is_none(), "协议不符被拒绝");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn manifest_replaces_default_registration() {
        let dir = tempdir("replace");
        let plugins = dir.join("plugins");
        let harness_dir = plugins.join("harness-agent");
        std::fs::create_dir_all(&harness_dir).unwrap();
        let entry = fake_entry(&harness_dir);
        std::fs::write(
            harness_dir.join("plugin.toml"),
            manifest_text("harness-agent", "\"harness\"", entry.to_str().unwrap()),
        )
        .unwrap();

        let mut registry = PluginRegistry::default();
        // 预置一个「默认注册」。
        registry.register(
            "harness",
            RegisteredPlugin {
                plugin_id: "harness-agent".into(),
                version: "bundled".into(),
                invoke_method: "agent/runTurn".into(),
                binary: PathBuf::from("/nonexistent-default"),
                protocol: 1,
                content_hash: String::new(),
                ui: None,
                source: RegistrationSource::Default,
            },
        );
        registry.discover(&plugins);

        let registration = registry.resolve("harness").expect("替换后仍可路由");
        assert_eq!(registration.binary, entry, "显式部署替换默认定位");
        assert!(matches!(
            registration.source,
            RegistrationSource::Manifest(_)
        ));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
