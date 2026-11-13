//! 插件注册（master 侧）：manifest 发现、动态节点类型与参数校验。
//!
//! 与运行时注册表（flow-plugin-runtime/plugin_registry）的分工：
//! - 运行时按 manifest **路由派发**（spawn 插件进程）；
//! - master 按同一份 manifest **接受/校验定义**（publish 期动态类型逃生）、
//!   合并 `nodetypes.list`、暴露 `plugins.list`。
//!
//! 两侧同源（plugins 目录 + 内置 harness 副本），规则一致：显式部署
//! 胜过内置副本；解析失败 loud。

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use flow_config::plugin_manifest::ParamsFormDecl;
use flow_config::plugin_manifest::{parse, FieldDecl, PluginManifest, Widget};
use serde_json::{json, Value};

/// 发现结果：全部可读 manifest（解析失败的记为 broken，不静默消失）。
#[derive(Debug, Clone)]
pub struct DiscoveredPlugin {
    pub manifest: PluginManifest,
    /// 发现来源目录（诊断）。
    pub source: std::path::PathBuf,
    /// 内容身份（I35）：sha256(manifest 字节 ‖ entry 字节)。插件身份键从
    /// (id, version) 升级为 (id, content_hash)——升级/换二进制必须重新授予。
    pub content_hash: String,
}

/// 计算插件内容哈希：manifest 原文字节 + 入口二进制字节（不可读 = 拒绝，
/// 由调用方处置）。
pub fn content_hash(manifest_bytes: &[u8], entry_bytes: &[u8]) -> String {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(manifest_bytes);
    hasher.update(entry_bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// 扫描 plugins 目录（FLOW_PLUGINS_DIR → <flow_home>/plugins）。
///
/// harness 的内置副本**不**在这里注入：发现函数只看显式部署；内置
/// 兜底仅 harness 配置解析使用（plugin_config），动态类型/列表只认
/// 已部署 manifest——没部署的插件不该出现在调色板里。
pub fn discover_plugins() -> Vec<DiscoveredPlugin> {
    let Some(base) = std::env::var_os("FLOW_PLUGINS_DIR")
        .map(std::path::PathBuf::from)
        .or_else(|| flow_config::flow_home().map(|home| home.join("plugins")))
    else {
        return Vec::new();
    };
    let Ok(entries) = std::fs::read_dir(&base) else {
        return Vec::new();
    };
    let mut found = Vec::new();
    let mut dirs: Vec<_> = entries
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    dirs.sort();
    for plugin_dir in dirs {
        let manifest_path = plugin_dir.join("plugin.toml");
        if !manifest_path.is_file() {
            continue;
        }
        match std::fs::read(&manifest_path)
            .map_err(|e| e.to_string())
            .and_then(|bytes| {
                let text = String::from_utf8(bytes.clone()).map_err(|e| e.to_string())?;
                let manifest = parse(&text).map_err(|e| e.to_string())?;
                let entry = resolve_entry(&plugin_dir, &manifest.plugin.entry)
                    .ok_or_else(|| format!("入口不存在：{}", manifest.plugin.entry))?;
                let entry_bytes = std::fs::read(&entry).map_err(|e| e.to_string())?;
                let content_hash = content_hash(&bytes, &entry_bytes);
                Ok(DiscoveredPlugin {
                    manifest,
                    source: plugin_dir,
                    content_hash,
                })
            }) {
            Ok(plugin) => found.push(plugin),
            Err(error) => tracing::warn!(
                ?manifest_path,
                %error,
                "插件 manifest 解析失败，不进列表/不接受其节点类型"
            ),
        }
    }
    found
}

/// 入口路径解析：相对插件目录或绝对路径（与 runtime 侧同规）。
fn resolve_entry(plugin_dir: &std::path::Path, entry: &str) -> Option<std::path::PathBuf> {
    let path = std::path::Path::new(entry);
    if path.is_absolute() {
        Some(path.to_path_buf())
    } else {
        Some(plugin_dir.join(path))
    }
}

/// 动态节点类型集合：已部署 manifest 声明且**不在**编译枚举内的类型名。
pub fn dynamic_node_types() -> BTreeSet<String> {
    discover_plugins()
        .into_iter()
        .flat_map(|plugin| plugin.manifest.plugin.node_types)
        .filter(|name| flow_engine::NodeType::parse(name).is_none())
        .collect()
}

/// 各动态类型的参数表单（校验与 listing 共用）。
pub fn dynamic_params_forms() -> BTreeMap<String, Vec<FieldDecl>> {
    let mut forms = BTreeMap::new();
    for plugin in discover_plugins() {
        for form in plugin.manifest.ui.params_forms {
            if flow_engine::NodeType::parse(&form.node_type).is_none() {
                forms.insert(form.node_type.clone(), form.fields);
            }
        }
    }
    forms
}

/// 动态节点的参数校验（publish 期）：按 manifest 表单校验已声明字段。
///
/// 规则与 engine `validate_params` 同精神：`${}` 模板放行（执行期展开
/// 后才可判定）；未声明字段放行（节点参数可携带 join/retry 等元数据，
/// 与内置类型一致）。类型级错误在发布时拒绝——这是「坏参数在发布时
/// 爆，而不是凌晨三点的 run 里」的 manifest 版。
pub fn validate_dynamic_params(
    node: &flow_engine::Node,
    forms: &BTreeMap<String, Vec<FieldDecl>>,
) -> Result<(), String> {
    let Some(fields) = forms.get(&node.node_type) else {
        return Ok(());
    };
    for field in fields {
        let Some(value) = node.params.get(&field.key) else {
            if field.required {
                return Err(format!(
                    "节点 {}（{}）缺少参数 {}",
                    node.id, node.node_type, field.key
                ));
            }
            continue;
        };
        // 模板放行：展开后才知道真值（与引擎同一条规则）。
        if value.as_str().is_some_and(|text| text.contains("${")) {
            continue;
        }
        let reject = |reason: String| {
            Err(format!(
                "节点 {}（{}）的参数 {} {reason}",
                node.id, node.node_type, field.key
            ))
        };
        match &field.widget {
            Widget::Text { .. } | Widget::Code { .. } => {
                if field.required && value.as_str().is_none_or(|text| text.trim().is_empty()) {
                    return reject("必填项不能为空".into());
                }
            }
            Widget::Number { min, max } => {
                let Some(number) = value.as_f64() else {
                    return reject("必须是数值".into());
                };
                if let Some(min) = min {
                    if number < *min {
                        return reject(format!("低于下限 {min}"));
                    }
                }
                if let Some(max) = max {
                    if number > *max {
                        return reject(format!("超过上限 {max}"));
                    }
                }
            }
            Widget::Boolean => {
                if value.as_bool().is_none() {
                    return reject("必须是布尔值".into());
                }
            }
            Widget::Enum { values } => {
                let Some(text) = value.as_str() else {
                    return reject("必须是字符串".into());
                };
                if !values.iter().any(|allowed| allowed == text) {
                    return reject(format!(
                        "值 {text:?} 不在枚举闭集内（允许 {}）",
                        values.join(" | ")
                    ));
                }
            }
            Widget::Secret => {
                if field.required && value.as_str().is_none_or(|text| text.trim().is_empty()) {
                    return reject("必填引用不能为空".into());
                }
            }
        }
    }
    Ok(())
}

/// manifest 条目 → NodeTypeDesc 形状（listing 合并用）。
///
/// 端口缺省单入单出（插件节点暂不声明多端口；需要那天在 manifest [ui]
/// 加 ports 声明，宿主版本）。params_schema 由表单字段转 JSON-Schema 子集
/// （x-widget/x-secret 与既有 ModalFields 渲染约定一致）。
pub fn manifest_to_node_type_desc(manifest: &PluginManifest, form: &ParamsFormDecl) -> Value {
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();
    for field in &form.fields {
        let mut property = serde_json::Map::new();
        match &field.widget {
            Widget::Text { max_len } => {
                property.insert("type".into(), json!("string"));
                let _ = max_len;
            }
            Widget::Code { format, .. } => {
                property.insert("type".into(), json!("string"));
                property.insert("x-widget".into(), json!("code"));
                property.insert(
                    "x-format".into(),
                    json!(match format {
                        flow_config::plugin_manifest::CodeFormat::Text => "text",
                        flow_config::plugin_manifest::CodeFormat::Js => "js",
                        flow_config::plugin_manifest::CodeFormat::Json => "json",
                        flow_config::plugin_manifest::CodeFormat::Shell => "shell",
                    }),
                );
            }
            Widget::Number { min, max } => {
                property.insert("type".into(), json!("number"));
                if let Some(min) = min {
                    property.insert("minimum".into(), json!(min));
                }
                if let Some(max) = max {
                    property.insert("maximum".into(), json!(max));
                }
            }
            Widget::Boolean => {
                property.insert("type".into(), json!("boolean"));
            }
            Widget::Enum { values } => {
                property.insert("type".into(), json!("string"));
                property.insert("enum".into(), json!(values));
            }
            Widget::Secret => {
                property.insert("type".into(), json!("string"));
                property.insert("x-secret".into(), json!(true));
            }
        }
        if let Some(default) = &field.default {
            property.insert("default".into(), json!(default));
        }
        if field.required {
            required.push(field.key.clone());
        }
        properties.insert(field.key.clone(), Value::Object(property));
    }
    json!({
        "type": form.node_type,
        "label": if manifest.plugin.name.is_empty() {
            form.node_type.clone()
        } else {
            manifest.plugin.name.clone()
        },
        "category": manifest.ui.palette.as_ref()
            .map(|palette| palette.category.clone())
            .unwrap_or_else(|| "plugin".into()),
        "ports": [{"id": "in", "label": "入"}, {"id": "out", "label": "出"}],
        "params_schema": {
            "type": "object",
            "required": required,
            "properties": Value::Object(properties),
        },
        // 插件标记：前端/诊断可区分「编译内置」与「插件提供」。
        "x-plugin": manifest.plugin.id,
    })
}

/// `plugins.list` 的条目形状。
pub fn plugin_list_entry(plugin: &DiscoveredPlugin) -> Value {
    plugin_list_entry_with_grant(plugin, None, false)
}

/// 带授予状态的列表条目（grant = 当前内容哈希命中的授予记录；
/// hash_mismatch = 该插件存在旧哈希的授予 → 升级后待重授予）。
pub fn plugin_list_entry_with_grant(
    plugin: &DiscoveredPlugin,
    grant: Option<&flow_engine::journal_state::PluginGrant>,
    hash_mismatch: bool,
) -> Value {
    let manifest = &plugin.manifest;
    json!({
        "id": manifest.plugin.id,
        "name": if manifest.plugin.name.is_empty() {
            manifest.plugin.id.clone()
        } else {
            manifest.plugin.name.clone()
        },
        "version": manifest.plugin.version,
        "node_types": manifest.plugin.node_types,
        "invoke": manifest.methods.invoke,
        "protocol": manifest.plugin.protocol,
        "config_fields": manifest.config_fields.iter().map(field_summary).collect::<Vec<_>>(),
        "ui": serde_json::to_value(manifest.ui_projection()).unwrap_or(Value::Null),
        "source": plugin.source.display().to_string(),
        // 09 §7：内容身份与授予状态（UI 授予页的唯一数据源）。
        "content_hash": plugin.content_hash,
        "permissions": manifest.permissions_projection().unwrap_or_else(|| json!({
            "legacy": true,
        })),
        "grant": grant.map(|grant| json!({
            "content_hash": grant.content_hash,
            "capabilities": grant.capabilities.iter()
                .map(flow_dto::permission::Capability::to_json).collect::<Vec<_>>(),
            "legacy_confirmed": grant.legacy_confirmed,
            "granted_at": grant.granted_at,
        })),
        "grant_state": if grant.is_some() {
            "granted"
        } else if hash_mismatch {
            "hash_changed"
        } else {
            "pending"
        },
    })
}

fn field_summary(field: &FieldDecl) -> Value {
    let widget = match &field.widget {
        Widget::Text { .. } => "text",
        Widget::Code { .. } => "code",
        Widget::Number { .. } => "number",
        Widget::Boolean => "boolean",
        Widget::Enum { .. } => "enum",
        Widget::Secret => "secret",
    };
    json!({
        "key": field.key,
        "type": widget,
        "required": field.required,
        "default": field.default,
    })
}

pub fn plugins_for_definition(
    definition: &Value,
) -> Vec<flow_engine::journal_state::AgentPluginSnapshot> {
    let nodes = definition["nodes"].as_array().cloned().unwrap_or_default();
    let mut wanted: BTreeSet<String> = BTreeSet::new();
    for node in nodes {
        if let Some(node_type) = node["type"].as_str() {
            if flow_engine::NodeType::parse(node_type).is_none() {
                wanted.insert(node_type.to_string());
            }
        }
    }
    discover_plugins()
        .into_iter()
        .filter(|plugin| {
            plugin
                .manifest
                .plugin
                .node_types
                .iter()
                .any(|node_type| wanted.contains(node_type))
        })
        .map(|plugin| {
            let DiscoveredPlugin {
                manifest,
                source: _,
                content_hash,
            } = plugin;
            let ui = serde_json::to_value(manifest.ui_projection()).ok();
            flow_engine::journal_state::AgentPluginSnapshot {
                id: manifest.plugin.id.clone(),
                version: manifest.plugin.version.clone(),
                ui,
                permissions: manifest.permissions_projection(),
                content_hash: Some(content_hash),
            }
        })
        .collect()
}

/// 插件的授予身份：内容哈希 + 声明能力（legacy 插件无声明）。
pub struct PluginIdentity {
    pub plugin_id: String,
    pub content_hash: String,
    pub legacy: bool,
    pub capabilities: Option<Vec<flow_config::plugin_manifest::ManifestCapability>>,
}

/// manifest 权限段的来源标记（内置副本 vs 部署目录）。
pub enum ManifestPermissionsSource {
    Bundled,
    Discovered,
}

/// 按节点类型反查插件 id（动态类型 + 内置 llm/harness）。
pub fn bundled_or_discovered_by_type(node_type: &str) -> Option<String> {
    if let Some(found) = discover_plugins().into_iter().find(|plugin| {
        plugin
            .manifest
            .plugin
            .node_types
            .iter()
            .any(|t| t == node_type)
    }) {
        return Some(found.manifest.plugin.id);
    }
    match node_type {
        "llm" => Some("llm".into()),
        "harness" => Some("harness-agent".into()),
        _ => None,
    }
}

/// 按插件 id 取授予身份：部署目录优先（发现结果带内容哈希）；内置副本
/// （harness 嵌入 manifest / llm 嵌入 manifest + 入口二进制）兜底——两者
/// 必须算出与执行器侧一致的哈希（manifest 字节 ‖ entry 字节）。
pub fn bundled_or_discovered(plugin_id: &str) -> Option<PluginIdentity> {
    if let Some(found) = discover_plugins()
        .into_iter()
        .find(|plugin| plugin.manifest.plugin.id == plugin_id)
    {
        let legacy = matches!(
            found.manifest.permissions,
            flow_config::plugin_manifest::ManifestPermissions::Legacy
        );
        let capabilities = match &found.manifest.permissions {
            flow_config::plugin_manifest::ManifestPermissions::Legacy => None,
            flow_config::plugin_manifest::ManifestPermissions::Declared(section) => {
                Some(section.capabilities.clone())
            }
        };
        return Some(PluginIdentity {
            plugin_id: plugin_id.to_string(),
            content_hash: found.content_hash,
            legacy,
            capabilities,
        });
    }
    // 内置兜底：嵌入 manifest 文本 + 入口二进制（env 或同目录兄弟文件）。
    let (manifest_text, entry_env, sibling) = match plugin_id {
        "harness-agent" => (
            include_str!("../../../plugins/harness/plugin/plugin.toml"),
            "FLOW_AGENT_PLUGIN_BIN",
            "flow-agent-plugin",
        ),
        "llm" => (
            include_str!("../../../plugins/llm/plugin/plugin.toml"),
            "FLOW_LLM_PLUGIN_BIN",
            "flow-llm-plugin",
        ),
        _ => return None,
    };
    let manifest = match parse(manifest_text) {
        Ok(manifest) => manifest,
        Err(error) => {
            tracing::warn!(plugin = plugin_id, %error, "内置 manifest 解析失败");
            return None;
        }
    };
    // 入口定位：env → current_exe 同目录 → 上溯两级（cargo test 的测试
    // 二进制在 target/debug/deps 下，兄弟二进制在 target/debug）。
    let entry = std::env::var_os(entry_env)
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::current_exe().ok().and_then(|exe| {
                let name = std::path::Path::new(sibling);
                std::iter::successors(exe.parent(), |parent| parent.parent())
                    .take(3)
                    .map(|dir| dir.join(name))
                    .find(|candidate| candidate.is_file())
            })
        });
    let entry_bytes = entry.and_then(|path| std::fs::read(path).ok())?;
    let content_hash = content_hash(manifest_text.as_bytes(), &entry_bytes);
    let legacy = matches!(
        manifest.permissions,
        flow_config::plugin_manifest::ManifestPermissions::Legacy
    );
    let capabilities = match &manifest.permissions {
        flow_config::plugin_manifest::ManifestPermissions::Legacy => None,
        flow_config::plugin_manifest::ManifestPermissions::Declared(section) => {
            Some(section.capabilities.clone())
        }
    };
    Some(PluginIdentity {
        plugin_id: plugin_id.to_string(),
        content_hash,
        legacy,
        capabilities,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(node_type: &str, params: Value) -> flow_engine::Node {
        serde_json::from_value(json!({
            "id": "n1",
            "type": node_type,
            "params": params,
        }))
        .unwrap()
    }

    fn forms() -> BTreeMap<String, Vec<FieldDecl>> {
        let text = r#"
[plugin]
id = "echo-plugin"
version = "0.1.0"
entry = "bin/x"
protocol = 1
node_types = ["echo"]

[methods]
invoke = "node/invoke"

[[ui.params]]
for = "echo"
[[ui.params.field]]
key = "text"
type = "code"
format = "text"
required = true
[[ui.params.field]]
key = "times"
type = "number"
min = 1
max = 5
default = "1"
[[ui.params.field]]
key = "mode"
type = "enum"
values = ["upper", "lower"]
default = "upper"
"#;
        let manifest = parse(text).unwrap();
        let mut forms = BTreeMap::new();
        for form in manifest.ui.params_forms {
            forms.insert(form.node_type.clone(), form.fields);
        }
        forms
    }

    #[test]
    fn dynamic_params_validation_table() {
        let forms = forms();
        // 模板放行 + 合法值通过。
        assert!(validate_dynamic_params(
            &node("echo", json!({"text": "${input.x}", "times": 3})),
            &forms
        )
        .is_ok());
        // 必填缺失 / 空白。
        assert!(validate_dynamic_params(&node("echo", json!({})), &forms)
            .unwrap_err()
            .contains("text"));
        assert!(
            validate_dynamic_params(&node("echo", json!({"text": "  "})), &forms)
                .unwrap_err()
                .contains("空")
        );
        // 数值边界与类型。
        assert!(
            validate_dynamic_params(&node("echo", json!({"text": "a", "times": 6})), &forms)
                .unwrap_err()
                .contains("上限")
        );
        assert!(
            validate_dynamic_params(&node("echo", json!({"text": "a", "times": "3"})), &forms)
                .unwrap_err()
                .contains("数值")
        );
        // 枚举闭集（拒绝不回退）。
        assert!(validate_dynamic_params(
            &node("echo", json!({"text": "a", "mode": "title"})),
            &forms
        )
        .unwrap_err()
        .contains("闭集"));
        // 未声明字段放行（join/retry 元数据与内置类型同规则）。
        assert!(validate_dynamic_params(
            &node("echo", json!({"text": "a", "join": "all"})),
            &forms
        )
        .is_ok());
    }

    #[test]
    fn manifest_entry_becomes_node_type_desc() {
        let text = r#"
[plugin]
id = "echo-plugin"
name = "Echo"
version = "0.1.0"
entry = "bin/x"
protocol = 1
node_types = ["echo"]

[methods]
invoke = "node/invoke"

[[ui.params]]
for = "echo"
[[ui.params.field]]
key = "text"
type = "code"
format = "text"
required = true
"#;
        let manifest = parse(text).unwrap();
        let desc = manifest_to_node_type_desc(&manifest, &manifest.ui.params_forms[0]);
        assert_eq!(desc["type"], "echo");
        assert_eq!(desc["label"], "Echo");
        assert_eq!(desc["category"], "plugin");
        assert_eq!(desc["x-plugin"], "echo-plugin");
        assert_eq!(desc["params_schema"]["required"][0], "text");
        assert_eq!(
            desc["params_schema"]["properties"]["text"]["x-widget"],
            "code"
        );
    }
}
