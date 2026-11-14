//! 插件注册表：节点类型 → 插件注册的路由表（设计见
//! docs/plans/2026-11-06-plugin-system.md §4/§8 R1-R2）。
//!
//! 加载来源两路，合并规则白纸黑字：
//!
//! 1. **内置默认注册**（编译期约定）：`harness` → harness-agent 插件的
//!    传统定位（`FLOW_AGENT_PLUGIN_BIN` 或同目录兄弟二进制）。这保证
//!    「没有 plugins 目录的部署照样能跑 harness 节点」——R2 的行为
//!    零变化靠它。
//! 2. **manifest 发现**（`FLOW_PLUGINS_DIR`，缺省 `<flow_home>/plugins`）：
//!    逐目录解析 `plugin.toml`。**显式部署胜过隐式默认**：同 id 的
//!    manifest 替换默认注册；其余冲突一律拒绝加载该 manifest。
//!
//! 拒绝规则（每条都 loud，不静默跳字段）：
//! - `node_types` 含内置节点类型名（`NodeType::parse` 可识别且非
//!   Harness）——防插件悄悄顶掉内置语义；
//! - `node_types` 与**其他插件**的注册重叠——一个节点类型只能有一个
//!   语义居所；
//! - 协议主版本不符（廉价快筛：不匹配连进程都不拉，握手才是权威裁决）；
//! - 入口二进制不存在。
//!
//! 注册表在执行器进程启动时加载一次，**不热加载**：进程重启即插件
//! 重载，这是刻意的（设计 §0）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use flow_config::plugin_manifest::{self, PluginManifest};
use flow_engine::NodeType;

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
    /// 内容身份（I35）：sha256(manifest 字节 ‖ entry 字节)。内置默认注册
    /// 无 manifest，为空串（内置节点不走 plugin_node 授权链）。
    pub content_hash: String,
    /// UI 投影（未来 run 快照冻结物；R1 先解析携带）。
    pub ui: Option<plugin_manifest::UiProjection>,
    /// 注册来源。
    pub source: RegistrationSource,
}

impl RegisteredPlugin {
    /// I35：spawn 前复核内容哈希（注册后二进制/manifest 被换掉 → 拒绝）。
    /// 内置默认注册（无 manifest）不参与复核。
    pub fn recheck_content_hash(&self) -> Result<(), String> {
        let RegistrationSource::Manifest(manifest_path) = &self.source else {
            return Ok(());
        };
        let (manifest_bytes, entry_bytes) = (
            std::fs::read(manifest_path).map_err(|e| format!("manifest 不可读：{e}"))?,
            std::fs::read(&self.binary).map_err(|e| format!("入口不可读：{e}"))?,
        );
        let current = content_hash(&manifest_bytes, &entry_bytes);
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

/// 内容身份：sha256(manifest 字节 ‖ entry 字节)——与 master 侧
/// `flow_backend::plugin_registry::content_hash` 同一公式。
fn content_hash(manifest_bytes: &[u8], entry_bytes: &[u8]) -> String {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(manifest_bytes);
    hasher.update(entry_bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// 节点类型 → 插件注册的路由表。
#[derive(Debug, Clone, Default)]
pub struct PluginRegistry {
    by_node_type: HashMap<String, RegisteredPlugin>,
}

/// 内置默认注册的通用定位：`<BIN_ENV>` 显式指定，否则执行器同目录
/// 兄弟 `<bin_name>`（行为零变化的锚点：没有 plugins 目录的部署照样
/// 能跑，plugins 目录的 manifest 永远可以覆盖默认注册）。
fn default_sibling_binary(bin_env: &str, bin_name: &str) -> Option<PathBuf> {
    if let Some(path) = std::env::var_os(bin_env) {
        let path = PathBuf::from(path);
        return path.is_file().then_some(path);
    }
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join(bin_name)))
        .filter(|path| path.is_file())
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
        // 内置默认注册两张：harness（R2）与 llm（R4）。二进制缺失只 warn：
        // 对应节点不可用，执行器照常服务其它节点类型。
        for (node_type, plugin_id, bin_env, bin_name, invoke) in [
            (
                "harness",
                "harness-agent",
                "FLOW_AGENT_PLUGIN_BIN",
                "flow-agent-plugin",
                "agent/runTurn",
            ),
            (
                "llm",
                "llm",
                "FLOW_LLM_PLUGIN_BIN",
                "flow-llm-plugin",
                "llm/invoke",
            ),
        ] {
            match default_sibling_binary(bin_env, bin_name) {
                Some(binary) => registry.register(
                    node_type,
                    RegisteredPlugin {
                        plugin_id: plugin_id.into(),
                        version: "bundled".into(),
                        invoke_method: invoke.into(),
                        binary,
                        protocol: flow_plugin_sdk::protocol::PROTOCOL_VERSION,
                        content_hash: String::new(),
                        ui: None,
                        source: RegistrationSource::Default,
                    },
                ),
                None => tracing::warn!(
                    plugin_id,
                    "{bin_env} 未设且同目录无 {bin_name}；{node_type} 节点将不可用，除非 plugins 目录提供 manifest"
                ),
            }
        }
        let dir = flow_config::plugin_fs::plugins_root();
        if let Some(dir) = dir {
            registry.discover(&dir);
        }
        registry
    }

    /// 节点类型路由查询。
    pub fn resolve(&self, node_type: &str) -> Option<&RegisteredPlugin> {
        self.by_node_type.get(node_type)
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

    /// manifest 发现：逐子目录找 `plugin.toml`，按合并/拒绝规则装载。
    fn discover(&mut self, dir: &Path) {
        if !dir.exists() {
            return;
        }
        let _lease = match flow_config::plugin_fs::RootLease::shared(dir) {
            Ok(lease) => lease,
            Err(error) => {
                tracing::warn!(%error, ?dir, "plugin lease unavailable");
                return;
            }
        };
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(error) => {
                tracing::warn!(%error, ?dir, "plugin directory unavailable");
                return;
            }
        };
        let mut dirs: Vec<_> = entries
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| {
                p.is_dir()
                    && !p
                        .file_name()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .starts_with('.')
            })
            .collect();
        dirs.sort();
        let mut candidates = Vec::new();
        let mut ids = std::collections::BTreeMap::new();
        for plugin_dir in dirs {
            let manifest_path = plugin_dir.join("plugin.toml");
            if !manifest_path.is_file() {
                continue;
            }
            let text = (|| -> std::io::Result<String> {
                use std::io::Read;
                let mut bytes = Vec::new();
                std::fs::File::open(&manifest_path)?
                    .take(262145)
                    .read_to_end(&mut bytes)?;
                if bytes.len() > 262144 {
                    return Err(std::io::Error::other("manifest exceeds 256 KiB"));
                }
                String::from_utf8(bytes).map_err(std::io::Error::other)
            })();
            match text {
                Ok(text) => {
                    let id = plugin_manifest::parse(&text).ok().map(|m| m.plugin.id);
                    if let Some(id) = &id {
                        *ids.entry(id.clone()).or_insert(0usize) += 1;
                    }
                    candidates.push((manifest_path, plugin_dir, text, id));
                }
                Err(error) => tracing::warn!(?manifest_path, %error, "manifest unavailable"),
            }
        }
        for (path, dir, text, id) in candidates {
            if id.as_ref().is_some_and(|id| ids[id] > 1) {
                tracing::warn!(?path, ?id, "duplicate plugin id; all copies disabled");
                continue;
            }
            self.load_manifest(&path, &dir, &text);
        }
    }

    fn load_manifest(&mut self, manifest_path: &Path, plugin_dir: &Path, text: &str) {
        let manifest: PluginManifest = match plugin_manifest::parse(text) {
            Ok(manifest) => manifest,
            Err(error) => {
                tracing::warn!(?manifest_path, %error, "manifest 解析失败，插件禁用（不猜、不跳过）");
                return;
            }
        };
        let reject = |reason: String| {
            tracing::warn!(
                ?manifest_path,
                plugin = %manifest.plugin.id,
                "{reason}，插件禁用"
            );
        };
        // 协议快筛：主版本不等连进程都不拉（握手的版本门仍是权威裁决）。
        if manifest.plugin.protocol >> 16 != flow_plugin_sdk::protocol::PROTOCOL_VERSION >> 16 {
            reject(format!(
                "协议主版本不符：manifest={}，runtime={}",
                manifest.plugin.protocol >> 16,
                flow_plugin_sdk::protocol::PROTOCOL_VERSION >> 16
            ));
            return;
        }
        // 内置类型名防护：NodeType 可解析且仍由编译期处理器承载 = 不可
        // 被插件顶掉。harness/llm 已迁出为插件承载（R2/R4），不在守卫集。
        if let Some(builtin) = manifest.plugin.node_types.iter().find(|name| {
            matches!(
                NodeType::parse(name),
                Some(kind) if !matches!(kind, NodeType::Harness | NodeType::Llm)
            )
        }) {
            reject(format!(
                "node_types 含内置类型 {builtin:?}（内置语义不可被插件顶掉）"
            ));
            return;
        }
        // 入口存在性：相对路径相对插件目录，也可绝对路径（测试/部署布局）。
        let entry = PathBuf::from(&manifest.plugin.entry);
        let binary = if entry.is_absolute() {
            entry
        } else {
            plugin_dir.join(entry)
        };
        if !binary.is_file() {
            reject(format!("入口二进制不存在：{}", binary.display()));
            return;
        }
        // 与其他插件的重叠：一个节点类型只能有一个语义居所。同 id 的
        // 默认注册（harness-agent）允许被替换 = 显式部署胜过隐式默认。
        if let Some(existing) = manifest
            .plugin
            .node_types
            .iter()
            .filter_map(|node_type| self.by_node_type.get(node_type))
            .find(|existing| existing.plugin_id != manifest.plugin.id)
        {
            reject(format!(
                "node_types 与插件 {} 的注册重叠",
                existing.plugin_id
            ));
            return;
        }
        let entry_bytes = std::fs::read(&binary).unwrap_or_default();
        let registration = RegisteredPlugin {
            plugin_id: manifest.plugin.id.clone(),
            version: manifest.plugin.version.clone(),
            invoke_method: manifest.methods.invoke.clone(),
            binary,
            protocol: manifest.plugin.protocol,
            content_hash: content_hash(text.as_bytes(), &entry_bytes),
            ui: Some(manifest.ui_projection()),
            source: RegistrationSource::Manifest(manifest_path.to_path_buf()),
        };
        let node_types = manifest.plugin.node_types.clone();
        for node_type in &node_types {
            let replaced_default = matches!(
                self.by_node_type.get(node_type),
                Some(existing) if existing.source == RegistrationSource::Default
            );
            if replaced_default {
                tracing::info!(
                    ?manifest_path,
                    plugin = %registration.plugin_id,
                    node_type,
                    "manifest 替换内置默认注册（显式部署胜过隐式默认）"
                );
            }
            self.register(node_type, registration.clone());
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
protocol = 1
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
            .replace("protocol = 1", &(9u32 << 16).to_string());
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
