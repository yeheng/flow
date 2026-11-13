//! 插件配置三源合一（**单一入口**，R3）。
//!
//! 设计（docs/plans/2026-11-06-plugin-system.md §5）：插件配置的合并
//! 只许发生在这里——manifest 声明（default/闭集/边界）+ 用户值
//!（[harness.agent] 配置段）+ secret 引用（节点级，不在此层）。插件
//! 永远只见最终快照，不见合并过程；执行器结构性禁读 manifest，它收到的
//! 就是 run 快照里冻结的 resolved 值。
//!
//! manifest 来源两级：**plugins 目录的显式部署胜过随包内置副本**（与
//! 执行器注册表同一条规则）；两者皆无 = 未部署（harness 配置面退回
//! typed 校验路径，warn 一次——隐式缺省可以存在，但必须 loud）。
//!
//! 停用判定在调用方：值对象为空 = 未启用（manifest 里 base_url/model
//! 必填，空值集永远不可能解析通过，由调用方先行判定，本层只做纯合并）。

use flow_config::plugin_manifest::{FieldDecl, PluginManifest, Widget};
use flow_config::HarnessAgentConfig;
use serde_json::{json, Value};

/// 随包内置的 harness-agent manifest 副本（编译期嵌入；显式部署可覆盖）。
const EMBEDDED_HARNESS_MANIFEST: &str = include_str!("../../../plugins/harness/plugin/plugin.toml");

/// 已安装插件的运行时形态：resolved 配置 + 快照冻结用的声明元数据。
#[derive(Debug, Clone, PartialEq)]
pub struct InstalledAgentPlugin {
    /// 三源合一后的运行配置（随 run 快照冻结，派发时下发）。
    pub run_config: flow_dto::AgentRunConfig,
    /// 插件 id（快照冻结：历史渲染的展示归属）。
    pub plugin_id: String,
    /// 插件版本（快照冻结：诊断展示；渲染永不回读插件目录）。
    pub version: String,
    /// UI 投影（快照冻结的本体：palette/params/summary_keys）。
    pub ui: Option<Value>,
    /// 权限需求投影（09 §7.2）。None = legacy-unrestricted。
    pub permissions: Option<Value>,
    /// 内容身份（I35）。typed 注入路径可能无法定位二进制 → None（诊断用）。
    pub content_hash: Option<String>,
}

/// harness-agent 的 manifest：plugins 目录显式部署 → 内置副本。
///
/// # Errors
/// 显式部署的 manifest 解析失败 = 启动失败（部署坏了要让运维看见，
/// 不静默回落内置副本——那会把坏部署变成幽灵配置）。
/// harness-agent 的 (权限投影, 内容哈希)：部署目录优先，缺省回嵌入文本 +
/// 入口二进制（env FLOW_AGENT_PLUGIN_BIN 或同目录兄弟文件）。
pub fn harness_agent_identity(
    manifest: &PluginManifest,
    manifest_text: Option<&str>,
) -> (Option<Value>, Option<String>) {
    let permissions = manifest.permissions_projection();
    let content_hash = manifest_text.and_then(|text| {
        let entry = std::env::var_os("FLOW_AGENT_PLUGIN_BIN")
            .map(std::path::PathBuf::from)
            .or_else(|| {
                std::env::current_exe()
                    .ok()
                    .and_then(|exe| exe.parent().map(|dir| dir.join("flow-agent-plugin")))
            });
        let bytes = entry.and_then(|path| std::fs::read(path).ok());
        bytes.map(|bytes| crate::plugin_registry::content_hash(text.as_bytes(), &bytes))
    });
    (permissions, content_hash)
}

pub fn harness_agent_manifest() -> Result<PluginManifest, String> {
    let base = std::env::var_os("FLOW_PLUGINS_DIR")
        .map(std::path::PathBuf::from)
        .or_else(|| flow_config::flow_home().map(|home| home.join("plugins")));
    if let Some(manifest) = manifest_from(base.as_deref()) {
        return manifest;
    }
    parse_embedded()
}

/// 从指定 plugins 根发现 harness-agent manifest（测试注入目录用，
/// 不碰全局环境变量——并行测试与全局 env 是经典赛跑）。
fn manifest_from(base: Option<&std::path::Path>) -> Option<Result<PluginManifest, String>> {
    let path = base?.join("harness-agent").join("plugin.toml");
    if !path.is_file() {
        return None;
    }
    let text = std::fs::read_to_string(&path)
        .map_err(|error| format!("manifest 不可读 {}：{error}", path.display()))
        .ok()?;
    Some(
        flow_config::plugin_manifest::parse(&text)
            .map_err(|error| format!("manifest 解析失败 {}：{error}", path.display())),
    )
}

fn parse_embedded() -> Result<PluginManifest, String> {
    flow_config::plugin_manifest::parse(EMBEDDED_HARNESS_MANIFEST)
        .map_err(|error| format!("内置 manifest 副本损坏（编译期漂移）：{error}"))
}

/// 三源合一的合并函数：manifest 声明 + 用户值 → resolved 快照。
///
/// 规则（表驱动测试钉死，改任何一条先改测试）：
/// - 未知值键 = 拒绝（配置面以 manifest 为准，不静默吞）；
/// - default 先垫底，用户值覆盖；
/// - `required` = 存在且非空白；
/// - number：必须是数值且落在 min..=max；
/// - enum：必须命中 values 闭集（拒绝不是回退 default——静默改值比
///   报错危险得多）；
/// - secret：值是引用（非空字符串），明文永不进配置层。
///
/// # Errors
/// 任一规则不过即 Err，消息面向配置编辑者（键名 + 原因）。
pub fn resolve_plugin_config(
    plugin_id: &str,
    fields: &[FieldDecl],
    values: &Value,
) -> Result<Value, String> {
    let values = values
        .as_object()
        .ok_or_else(|| format!("插件 {plugin_id} 的配置值必须是对象"))?;
    for key in values.keys() {
        if !fields.iter().any(|field| &field.key == key) {
            return Err(format!(
                "插件 {plugin_id} 的未知配置键 {key:?}（配置面以 manifest 为准）"
            ));
        }
    }
    let mut resolved = serde_json::Map::new();
    for field in fields {
        let provided = values.get(&field.key);
        if let Some(value) = provided {
            resolved.insert(field.key.clone(), validate_value(plugin_id, field, value)?);
            continue;
        }
        let Some(default) = &field.default else {
            if field.required {
                return Err(format!("插件 {plugin_id} 缺少必填配置 {:?}", field.key));
            }
            continue;
        };
        // default 也过同一校验：声明与规则不一致要在启动期爆，不留给 run。
        let value = Value::String(default.clone());
        resolved.insert(field.key.clone(), validate_value(plugin_id, field, &value)?);
    }
    Ok(Value::Object(resolved))
}

fn validate_value(plugin_id: &str, field: &FieldDecl, value: &Value) -> Result<Value, String> {
    let reject = |reason: String| Err(format!("插件 {plugin_id} 的配置 {:?} {reason}", field.key));
    match &field.widget {
        Widget::Text { .. } | Widget::Code { .. } => {
            let text = value
                .as_str()
                .ok_or_else(|| "必须是字符串".to_string())
                .and_then(|text| {
                    field
                        .required
                        .then_some(text.trim())
                        .filter(|t| !t.is_empty())
                        .map(|_| text.to_string())
                        .ok_or_else(|| "必填项不能为空白".to_string())
                });
            text.map(Value::String).or_else(reject)
        }
        Widget::Number { min, max } => {
            let number = match value.as_f64() {
                Some(number) => number,
                None => return reject("必须是数值".into()),
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
            Ok(value.clone())
        }
        Widget::Boolean => value
            .as_bool()
            .map(Value::Bool)
            .ok_or_else(|| "必须是布尔值".to_string())
            .or_else(reject),
        Widget::Enum { values } => {
            let text = value.as_str().ok_or_else(|| "必须是字符串".to_string())?;
            if !values.contains(&text.to_string()) {
                return reject(format!(
                    "值 {text:?} 不在枚举闭集内（允许 {}）",
                    values.join(" | ")
                ));
            }
            Ok(Value::String(text.to_string()))
        }
        Widget::Secret => {
            let text = value
                .as_str()
                .filter(|text| !text.trim().is_empty())
                .ok_or_else(|| "必须是引用（非空字符串）".to_string())?;
            if field.required && text.trim().is_empty() {
                return reject("必填引用不能为空".into());
            }
            Ok(Value::String(text.to_string()))
        }
    }
}

/// `[harness.agent]`（用户值）→ resolved 插件配置。
///
/// 空配置 = 未启用（返回 None）；有值 = 过 manifest 闭集校验 + 既有
/// 跨字段校验（成对/端点形态在 `HarnessAgentConfig::validate`，两道门
/// 都过才装）。
///
/// # Errors
/// manifest 部署损坏或值违反闭集/边界时 Err（启动期显式失败）。
pub fn harness_agent_plugin(
    config: &HarnessAgentConfig,
) -> Result<Option<InstalledAgentPlugin>, String> {
    config.validate()?;
    let values = harness_agent_values(config);
    if values.as_object().is_none_or(|map| map.is_empty()) {
        return Ok(None);
    }
    let manifest = harness_agent_manifest()?;
    let resolved = resolve_plugin_config(&manifest.plugin.id, &manifest.config_fields, &values)?;
    let run_config: flow_dto::AgentRunConfig = serde_json::from_value(resolved)
        .map_err(|error| format!("resolved 配置与 AgentRunConfig 形状不符：{error}"))?;
    let (permissions, content_hash) = harness_agent_identity(&manifest, None);
    Ok(Some(InstalledAgentPlugin {
        run_config,
        plugin_id: manifest.plugin.id.clone(),
        version: manifest.plugin.version.clone(),
        ui: serde_json::to_value(manifest.ui_projection()).ok(),
        permissions,
        content_hash,
    }))
}

/// typed 配置 → 值对象（空串/None 一律省略：显式空串在 typed 层已是关闭语义）。
fn harness_agent_values(config: &HarnessAgentConfig) -> Value {
    let mut values = serde_json::Map::new();
    let mut push = |key: &str, value: Option<String>| {
        if let Some(value) = value.filter(|v| !v.trim().is_empty()) {
            values.insert(key.to_string(), Value::String(value));
        }
    };
    push("base_url", config.base_url.clone());
    push("model", config.model.clone());
    push("api", config.api.clone());
    push("proxy", config.proxy.clone());
    for (key, value) in [
        ("max_turns", config.max_turns),
        ("max_tool_calls_per_turn", config.max_tool_calls_per_turn),
        ("max_tokens", config.max_tokens),
        ("tool_timeout_secs", config.tool_timeout_secs),
    ] {
        if let Some(value) = value {
            values.insert(key.to_string(), json!(value));
        }
    }
    Value::Object(values)
}

/// 已 resolved 的 typed 配置 → 安装形态（直接注入路径：e2e/dev 用，
/// 元数据尽力从 manifest 补齐——注入与声明两条路汇到同一形态）。
///
/// # Errors
/// manifest 部署损坏时 Err（元数据补不齐宁可失败：快照冻结不写半截）。
pub fn installed_from_run_config(
    run_config: flow_dto::AgentRunConfig,
) -> Result<InstalledAgentPlugin, String> {
    let manifest = harness_agent_manifest()?;
    let (permissions, content_hash) = harness_agent_identity(&manifest, None);
    Ok(InstalledAgentPlugin {
        run_config,
        plugin_id: manifest.plugin.id.clone(),
        version: manifest.plugin.version.clone(),
        ui: serde_json::to_value(manifest.ui_projection()).ok(),
        permissions,
        content_hash,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_config::plugin_manifest;

    fn harness_fields() -> Vec<FieldDecl> {
        parse_embedded()
            .expect("内置 manifest 可解析")
            .config_fields
    }

    /// 表驱动：默认垫底、枚举拒绝、边界拒绝、未知键拒绝、必填非空。
    #[test]
    fn resolution_table() {
        let fields = harness_fields();
        let plugin = "harness-agent";

        // 默认垫底：只给必填，api 取 manifest default。
        let resolved = resolve_plugin_config(
            plugin,
            &fields,
            &json!({"base_url": "http://llm.local", "model": "m"}),
        )
        .unwrap();
        assert_eq!(resolved["api"], "openai");
        assert!(resolved.get("proxy").is_none(), "可选未给 = 缺省不出现");

        // 枚举拒绝（不静默回退 default）。
        let error = resolve_plugin_config(
            plugin,
            &fields,
            &json!({"base_url": "http://x", "model": "m", "api": "gemini"}),
        )
        .unwrap_err();
        assert!(
            error.contains("gemini") && error.contains("闭集"),
            "{error}"
        );

        // 数值边界：min/max 都咬。
        for (key, bad) in [
            ("max_turns", json!(0)),
            ("max_turns", json!(201)),
            ("max_tokens", json!(1_000_001)),
            ("tool_timeout_secs", json!(0)),
        ] {
            let error = resolve_plugin_config(
                plugin,
                &fields,
                &json!({"base_url": "http://x", "model": "m", key: bad}),
            )
            .unwrap_err();
            assert!(error.contains(key), "{key}: {error}");
        }

        // 未知键：配置面以 manifest 为准。
        let error = resolve_plugin_config(
            plugin,
            &fields,
            &json!({"base_url": "http://x", "model": "m", "temperature": 0.7}),
        )
        .unwrap_err();
        assert!(error.contains("temperature"), "{error}");

        // 必填缺失与空白。
        assert!(
            resolve_plugin_config(plugin, &fields, &json!({"base_url": "http://x"}))
                .unwrap_err()
                .contains("model")
        );
        assert!(
            resolve_plugin_config(plugin, &fields, &json!({"base_url": "   ", "model": "m"}))
                .unwrap_err()
                .contains("空白")
        );

        // 数值类型强校验（字符串数字不接受——TOML 值本来就是类型的）。
        assert!(resolve_plugin_config(
            plugin,
            &fields,
            &json!({"base_url": "http://x", "model": "m", "max_turns": "8"})
        )
        .is_err());
    }

    /// 内置 manifest 的闭集/边界与 flow-config 的 typed 校验一致
    /// （两处漂移在这里爆：一侧改了另一侧没跟）。
    #[test]
    fn embedded_manifest_bounds_match_typed_validation() {
        let fields = harness_fields();
        let find = |key: &str| {
            fields
                .iter()
                .find(|field| field.key == key)
                .unwrap_or_else(|| panic!("{key} 未声明"))
        };
        for (key, max) in [
            ("max_turns", 200.0),
            ("max_tool_calls_per_turn", 100.0),
            ("max_tokens", 1_000_000.0),
            ("tool_timeout_secs", 86_400.0),
        ] {
            match &find(key).widget {
                Widget::Number {
                    min,
                    max: field_max,
                } => {
                    assert_eq!(*min, Some(1.0), "{key} min");
                    assert_eq!(
                        *field_max,
                        Some(max),
                        "{key} max（与 HarnessAgentConfig::validate 漂移）"
                    );
                }
                other => panic!("{key} 应为 number：{other:?}"),
            }
        }
        match &find("api").widget {
            Widget::Enum { values } => {
                assert_eq!(values, &vec!["openai".to_string(), "anthropic".to_string()])
            }
            other => panic!("api 应为 enum：{other:?}"),
        }
    }

    /// happy path：typed 配置 → resolved（default 补齐）→ AgentRunConfig。
    #[test]
    fn harness_agent_plugin_resolves_and_freezes_metadata() {
        let config = HarnessAgentConfig {
            base_url: Some("http://llm.local".into()),
            model: Some("m".into()),
            api: None,
            proxy: None,
            max_turns: Some(8),
            max_tool_calls_per_turn: None,
            max_tokens: None,
            tool_timeout_secs: None,
        };
        let installed = harness_agent_plugin(&config)
            .expect("解析成功")
            .expect("启用");
        assert_eq!(installed.run_config.api, "openai", "default 垫底");
        assert_eq!(installed.run_config.max_turns, Some(8));
        assert_eq!(installed.plugin_id, "harness-agent");
        let ui = installed.ui.as_ref().expect("UI 投影冻结");
        assert_eq!(ui["palette"]["category"], "ai");
        assert_eq!(ui["run_view"]["summary_keys"][0], "agent.model");

        // 空配置 = 未启用。
        assert!(harness_agent_plugin(&HarnessAgentConfig::default())
            .unwrap()
            .is_none());

        // typed 校验先行：base_url 有 model 无 → 成对报错。
        assert!(harness_agent_plugin(&HarnessAgentConfig {
            base_url: Some("http://x".into()),
            ..Default::default()
        })
        .is_err());
    }

    /// 显式部署胜过内置副本：目录里的 manifest 才是解析依据。
    #[test]
    fn discovered_manifest_wins_over_embedded() {
        let dir = std::env::temp_dir().join(format!(
            "flow-plugin-config-{}-{}",
            std::process::id(),
            line!()
        ));
        let plugin_dir = dir.join("harness-agent");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        // 收紧 max_turns 上限：内置副本是 200，部署副本 3。
        let manifest = EMBEDDED_HARNESS_MANIFEST.replace("max = 200", "max = 3");
        std::fs::write(plugin_dir.join("plugin.toml"), manifest).unwrap();

        let fields = parse_discovered_at(&dir);
        match &fields
            .iter()
            .find(|field| field.key == "max_turns")
            .unwrap()
            .widget
        {
            Widget::Number { max, .. } => assert_eq!(*max, Some(3.0), "部署副本必须胜出"),
            other => panic!("{other:?}"),
        }
        // 部署副本的收紧边界真实生效：8 在内置合法、在部署非法。
        let error = resolve_plugin_config(
            "harness-agent",
            &fields,
            &json!({"base_url": "http://x", "model": "m", "max_turns": 8}),
        )
        .unwrap_err();
        assert!(error.contains("上限"), "{error}");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    fn parse_discovered_at(dir: &std::path::Path) -> Vec<FieldDecl> {
        match manifest_from(Some(dir)) {
            Some(Ok(manifest)) => manifest.config_fields,
            other => panic!("发现路径必须命中部署副本：{other:?}"),
        }
    }

    /// manifest 解析器出口与投影序列化的冒烟（include_str 副本健康）。
    #[test]
    fn embedded_manifest_parses() {
        let manifest = plugin_manifest::parse(EMBEDDED_HARNESS_MANIFEST).expect("内置副本合法");
        assert_eq!(manifest.plugin.id, "harness-agent");
        assert_eq!(manifest.methods.invoke, "agent/runTurn");
    }
}
