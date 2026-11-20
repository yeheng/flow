//! 主进程的权限适配层：请求派生、策略冻结、插件清单与身份校验。
//! 裁决由 `flow-policy` 计算，事实提交由 Journal 的命令与派发入口负责。

pub mod derive;

use std::collections::{BTreeMap, BTreeSet};

use derive::normalize_provider;
use flow_dto::permission::{Approval, Capability, ExecutionMode, FrozenPolicy, NodePermissions};
use flow_engine::journal_state::{AgentPluginSnapshot, PluginBinding, Run};
pub use flow_policy::{check, check_plugin_needs, CheckInput, Evaluation, PluginNeedsInput};

// ---------------------------------------------------------------------------
// G2：策略冻结与静态门
// ---------------------------------------------------------------------------

/// G2：冻结根 run 的策略快照（执行模式 + workflow/node 能力及期限）。
///
/// legacy 定义（无权限块）冻结为 `legacy=true` 的空能力集：不自动生成全
/// 能力；静态门另在启动时拒绝含副作用节点的 legacy 定义。`plugin_ceilings`
/// 是该定义引用的插件授予层（无授予记录的插件不出现在层里 → G3 拒绝）。
pub fn freeze_root(
    definition: &flow_engine::Definition,
    plugin_ceilings: Vec<flow_dto::permission::PluginCeiling>,
) -> FrozenPolicy {
    freeze(definition, None, plugin_ceilings)
}

/// G2：冻结子 run 的策略。子工作流拥有独立冻结策略（自己的权限块），
/// 调用方节点的有效层作为额外上限逐层 AND（§3.5）：不继承父节点凭据、
/// 不自动提升能力；父 Plan 则子也只读。
pub fn freeze_child(
    definition: &flow_engine::Definition,
    parent: &FrozenPolicy,
    parent_run: &str,
    parent_node: &str,
    plugin_ceilings: Vec<flow_dto::permission::PluginCeiling>,
) -> FrozenPolicy {
    freeze(
        definition,
        Some((parent, parent_run, parent_node)),
        plugin_ceilings,
    )
}

fn freeze(
    definition: &flow_engine::Definition,
    parent: Option<(&FrozenPolicy, &str, &str)>,
    plugin_ceilings: Vec<flow_dto::permission::PluginCeiling>,
) -> FrozenPolicy {
    use flow_dto::permission::{CeilingLayer, WorkflowPermissions};
    let mut ceilings = Vec::new();
    let mut parent_plan = false;
    let mut parent_bypass = false;
    if let Some((parent, run, node)) = parent {
        parent_plan = parent.mode == ExecutionMode::Plan;
        parent_bypass = parent.mode == ExecutionMode::Bypass;
        ceilings.extend(parent.ceilings.iter().cloned());
        let source = |layer: &str| format!("parent:{run}/{node}:{layer}");
        // legacy 父 run 没有任何能力：子 run 的外部操作一律被空上限拒绝。
        let workflow_allow = if parent.legacy {
            Vec::new()
        } else {
            parent.workflow.allow.clone()
        };
        ceilings.push(CeilingLayer {
            source: source("workflow"),
            allow: workflow_allow,
        });
        if let Some(NodePermissions::Restrict { allow }) = parent.nodes.get(node) {
            ceilings.push(CeilingLayer {
                source: source("node"),
                allow: allow.clone(),
            });
        }
    }
    let (legacy, workflow) = match &definition.permissions {
        Some(permissions) => (false, permissions.clone()),
        None => (
            true,
            WorkflowPermissions {
                version: flow_dto::permission::PERMISSION_SCHEMA_VERSION,
                mode: ExecutionMode::Execute,
                allow: Vec::new(),
            },
        ),
    };
    let mode = if parent_plan {
        ExecutionMode::Plan
    } else if parent_bypass {
        ExecutionMode::Bypass
    } else {
        workflow.mode
    };
    let nodes = definition
        .nodes
        .iter()
        .filter_map(|node| {
            node.permissions
                .clone()
                .map(|permissions| (node.id.clone(), permissions))
        })
        .collect();
    FrozenPolicy::sealed(mode, legacy, workflow, nodes, ceilings, plugin_ceilings)
}

pub(crate) struct FrozenPlugins {
    pub bindings: BTreeMap<String, PluginBinding>,
    pub ceilings: Vec<flow_dto::permission::PluginCeiling>,
    pub projections: Vec<AgentPluginSnapshot>,
    pub draining: BTreeSet<String>,
}

/// Resolve runtime identity and grants from one discovery result for the run closure.
pub(crate) async fn freeze_plugins(
    backend: &crate::journal::JournalBackend,
    types: BTreeSet<String>,
    agent_plugin_id: Option<&str>,
) -> Result<FrozenPlugins, crate::journal::JournalError> {
    use flow_dto::permission::{Action, Capability, PluginCeiling, ResourcePattern};
    if types.is_empty() {
        return Ok(FrozenPlugins {
            bindings: BTreeMap::new(),
            ceilings: Vec::new(),
            projections: Vec::new(),
            draining: BTreeSet::new(),
        });
    }
    let agent_plugin_id = agent_plugin_id.map(str::to_owned);
    let defaults = backend.plugin_defaults();
    let (bindings, identities, projections, draining) = tokio::task::spawn_blocking(move || {
        let snapshot = crate::plugin_registry::installed_snapshot_with_defaults(&defaults);
        let plugins: Vec<_> = snapshot
            .rows
            .into_iter()
            .filter_map(|row| row.plugin)
            .collect();
        let mut bindings = BTreeMap::new();
        let mut identities = BTreeMap::new();
        let mut projections = BTreeMap::new();
        for node_type in types {
            let installed = plugins
                .iter()
                .find(|plugin| plugin.manifest.plugin.node_types.contains(&node_type));
            let id = match flow_engine::NodeType::parse(&node_type) {
                Some(flow_engine::NodeType::Llm) => Some("llm"),
                Some(flow_engine::NodeType::Harness) => agent_plugin_id.as_deref(),
                _ => installed.map(|plugin| plugin.manifest.plugin.id.as_str()),
            };
            let Some(id) = id else { continue };
            let Some(identity) =
                crate::plugin_registry::bundled_or_discovered_with_plugins(id, &plugins)
            else {
                continue;
            };
            let required = identity
                .capabilities
                .iter()
                .flatten()
                .filter(|entry| entry.required)
                .map(|entry| entry.capability.clone())
                .collect();
            if let Some(plugin) = installed.filter(|plugin| plugin.manifest.plugin.id == id) {
                if flow_engine::NodeType::parse(&node_type).is_none() {
                    projections
                        .entry(id.to_owned())
                        .or_insert_with(|| AgentPluginSnapshot {
                            id: id.to_owned(),
                            version: plugin.manifest.plugin.version.clone(),
                            ui: serde_json::to_value(plugin.manifest.ui_projection()).ok(),
                            permissions: plugin.manifest.permissions_projection(),
                            content_hash: Some(identity.content_hash.clone()),
                        });
                }
            }
            bindings.insert(
                node_type,
                PluginBinding {
                    plugin_id: id.to_owned(),
                    content_hash: identity.content_hash.clone(),
                    required,
                    unrestricted: identity.legacy,
                },
            );
            identities.insert(id.to_owned(), identity);
        }
        let mut draining = BTreeSet::new();
        if let Some(root) = flow_config::plugin_fs::plugins_root() {
            for id in identities.keys() {
                if crate::plugin_operation::is_draining(&root, id)? {
                    draining.insert(id.clone());
                }
            }
        }
        Ok::<_, crate::plugin_operation::OperationError>((
            bindings,
            identities,
            projections.into_values().collect(),
            draining,
        ))
    })
    .await
    .map_err(|error| crate::journal::JournalError::Storage(error.to_string()))?
    .map_err(|error| crate::journal::JournalError::Storage(error.to_string()))?;
    let state_ceilings = backend
        .inspect(|state| {
            identities
                .iter()
                .filter_map(|(plugin_id, identity)| {
                    // I35：只认 (plugin_id, 当前内容哈希) 的精确授予——
                    // 二进制或 manifest 变化后旧授予不再适用（换内容必须
                    // 重新授予）。
                    let grant = state
                        .plugin_grants
                        .get(&flow_engine::journal_state::PluginGrant::key(
                            plugin_id,
                            &identity.content_hash,
                        ))
                        .cloned()
                        // 测试兜底：进程内测试没有启动期授予时，内置插件按其
                        // 声明能力给一个瞬态上限（不落 journal；生产路径恒有
                        // 启动期自动授予，走不到这里）。
                        .or_else(|| {
                            crate::journal_commands::tests_auto_govern().then(|| {
                                flow_engine::journal_state::PluginGrant {
                                    plugin_id: identity.plugin_id.clone(),
                                    content_hash: identity.content_hash.clone(),
                                    capabilities: identity
                                        .capabilities
                                        .clone()
                                        .unwrap_or_default()
                                        .into_iter()
                                        .map(|entry| {
                                            let mut cap = entry.capability;
                                            cap.approval = Approval::Auto;
                                            cap
                                        })
                                        .collect(),
                                    legacy_confirmed: identity.legacy,
                                    granted_at: String::new(),
                                    by: flow_dto::permission::Principal::System,
                                }
                            })
                        })?;
                    let allow = if grant.legacy_confirmed {
                        Action::ALL
                            .iter()
                            .map(|action| Capability {
                                id: format!("legacy-{action}"),
                                pattern: ResourcePattern::Any { of: *action },
                                approval: Approval::Ask,
                                expires_at: None,
                            })
                            .collect()
                    } else {
                        grant.capabilities.clone()
                    };
                    let content_hash = grant.content_hash.clone();
                    Some(PluginCeiling {
                        plugin_id: plugin_id.clone(),
                        content_hash,
                        allow,
                    })
                })
                .collect::<Vec<_>>()
        })
        .await;
    Ok(FrozenPlugins {
        bindings,
        ceilings: state_ceilings,
        projections,
        draining,
    })
}

/// 节点操作对应的插件身份（G3/G5 评估输入；与 freeze 的解析同源）。
pub fn plugin_id_for_node(node: &flow_engine::Node, run: &Run) -> Option<String> {
    let node_type = node.plugin_type()?;
    if let Some(bindings) = &run.plugin_bindings {
        return bindings
            .get(node_type)
            .map(|binding| binding.plugin_id.clone());
    }
    match node.kind() {
        Some(flow_engine::NodeType::Llm) => Some("llm".into()),
        Some(flow_engine::NodeType::Harness) => run
            .execution
            .as_ref()?
            .agent_plugin
            .as_ref()
            .map(|plugin| plugin.id.clone()),
        _ => crate::plugin_registry::bundled_or_discovered_by_type(node_type),
    }
}

pub fn plugin_needs_for_type(
    run: &Run,
    node_type: &str,
) -> Option<(String, Vec<Capability>, bool)> {
    if let Some(bindings) = &run.plugin_bindings {
        let binding = bindings.get(node_type)?;
        return Some((
            binding.plugin_id.clone(),
            binding.required.clone(),
            binding.unrestricted,
        ));
    }
    let plugin_id = crate::plugin_registry::bundled_or_discovered_by_type(node_type)?;
    let identity = crate::plugin_registry::bundled_or_discovered(&plugin_id)?;
    let needs = identity
        .capabilities
        .as_ref()
        .map(|entries| {
            entries
                .iter()
                .filter(|entry| entry.required)
                .map(|entry| entry.capability.clone())
                .collect()
        })
        .unwrap_or_default();
    Some((plugin_id, needs, identity.legacy))
}

/// G4/I35：执行器上报的插件身份（id + 内容哈希）必须与 master 侧一致且
/// 与冻结授予同哈希——远程执行器二进制与授予快照脱钩时拒绝执行。
pub fn verify_plugin_identity(
    expected_id: &str,
    expected_hash: &str,
    reported: &serde_json::Value,
) -> Result<(), String> {
    let id = reported["id"].as_str().unwrap_or_default();
    let hash = reported["content_hash"].as_str().unwrap_or_default();
    if id != expected_id {
        return Err(format!(
            "插件身份不符：执行器上报 {id:?}，策略冻结 {expected_id:?}"
        ));
    }
    if !expected_hash.is_empty() && hash != expected_hash {
        return Err(format!(
            "插件 {expected_id} 内容哈希与授予时不符（执行器 {}，授予 {}）：需重新授予后才能执行（I35）",
            &hash[..8.min(hash.len())],
            &expected_hash[..8.min(expected_hash.len())],
        ));
    }
    Ok(())
}

/// 测试钩子（FLOW_TEST_AUTO_GOVERN=1）：legacy 定义按**运行期已知事实**
/// 合成精确权限块——节点字面参数 + 冻结的 agent 配置。产品语义不受影响
///（生产不设此变量）：legacy 定义照旧被静态门拒绝。
///
/// 供存量 e2e 使用（它们测的是节点语义而不是权限）；模板化的目标无法
/// 静态派生（host/收件人/model 含 `${}` 的节点不生成能力 → 该节点在
/// 受管语义下被拒——这类用例需要显式权限）。
pub fn auto_govern_for_tests(
    definition: &flow_engine::Definition,
    agent: Option<&flow_dto::AgentRunConfig>,
) -> flow_engine::Definition {
    use flow_dto::permission::{Capability, ResourcePattern, WorkflowPermissions};
    let mut allow: Vec<Capability> = Vec::new();
    let mut push = |pattern: ResourcePattern| {
        let id = format!("auto-{}", allow.len());
        allow.push(Capability {
            id,
            pattern,
            approval: Approval::Auto,
            expires_at: None,
        });
    };
    for node in &definition.nodes {
        let params = &node.params;
        match node.kind() {
            Some(flow_engine::NodeType::HttpCall) => {
                if let Some(url) = params["url"].as_str() {
                    if let Some(prefix) = static_origin(url) {
                        push(ResourcePattern::Net {
                            methods: crate::HTTP_METHODS.iter().map(|m| m.to_string()).collect(),
                            url_prefix: prefix,
                        });
                    }
                }
                if let Some(name) = params["credential"]["secret_ref"].as_str() {
                    push(ResourcePattern::Secret {
                        names: vec![name.to_string()],
                    });
                }
            }
            Some(flow_engine::NodeType::Email) => {
                let endpoint = params["endpoint"]
                    .as_str()
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or("https://api.resend.com/emails");
                if let Some(prefix) = static_origin(endpoint) {
                    push(ResourcePattern::Net {
                        methods: vec!["POST".to_string()],
                        url_prefix: prefix,
                    });
                }
                if let Some(to) = params["to"].as_str().filter(|to| !to.contains("${")) {
                    let recipients: Vec<String> = to
                        .split(',')
                        .map(str::trim)
                        .filter(|r| !r.is_empty())
                        .map(str::to_string)
                        .collect();
                    if !recipients.is_empty() {
                        push(ResourcePattern::SendEmail { recipients });
                    }
                }
                if let Some(name) = params["api_key"].as_str() {
                    push(ResourcePattern::Secret {
                        names: vec![name.to_string()],
                    });
                }
            }
            Some(flow_engine::NodeType::Llm) => {
                let provider = params["url"].as_str().filter(|u| !u.contains("${"));
                let model = params["body"]["model"]
                    .as_str()
                    .filter(|m| !m.is_empty() && !m.contains("${"));
                if let (Some(provider), Some(model)) = (provider, model) {
                    push(ResourcePattern::LlmCall {
                        providers: vec![normalize_provider(provider)],
                        models: vec![model.to_string()],
                    });
                }
                if let Some(name) = params["credential"]["secret_ref"].as_str() {
                    push(ResourcePattern::Secret {
                        names: vec![name.to_string()],
                    });
                }
            }
            Some(flow_engine::NodeType::Harness) if node.is_harness_agent() => {
                // agent 的 LlmCall 来自冻结配置（运行期才知道端点/模型/密钥名）。
                if let Some(agent) = agent.filter(|a| a.enabled()) {
                    push(ResourcePattern::LlmCall {
                        providers: vec![normalize_provider(&agent.base_url)],
                        models: vec![agent.model.clone()],
                    });
                    if let Some(name) = params["api_key"].as_str().filter(|n| !n.trim().is_empty())
                    {
                        push(ResourcePattern::Secret {
                            names: vec![name.to_string()],
                        });
                    }
                }
            }
            Some(flow_engine::NodeType::Harness) => {
                let Some(command) = params["command"].as_str() else {
                    continue;
                };
                if !command.starts_with('/') || command.contains("${") {
                    continue;
                }
                let argv: Vec<String> = params["args"]
                    .as_array()
                    .map(|items| {
                        items
                            .iter()
                            .map(|v| {
                                v.as_str()
                                    .map(str::to_string)
                                    .unwrap_or_else(|| v.to_string())
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                if argv.iter().any(|arg| arg.contains("${")) {
                    continue;
                }
                let basename = command.rsplit('/').next().unwrap_or(command);
                let shell = ["sh", "bash", "zsh", "dash", "ksh"].contains(&basename);
                if shell && argv.len() == 2 && argv[0] == "-c" {
                    push(ResourcePattern::Shell {
                        executable: Some(command.to_string()),
                        script_digest: flow_dto::permission::sha256_hex(argv[1].as_bytes()),
                    });
                } else {
                    push(ResourcePattern::Exec {
                        executable: command.to_string(),
                        argv_exact: argv,
                    });
                }
            }
            _ => {
                // G4：动态插件节点同样受管。Declared manifest 的 required 能力
                // 合成进 workflow 层（manifest 专用变体 Any/ExecProgram 无法
                // 在 workflow 层表达，跳过——这类用例需显式权限）；Legacy
                // manifest 需求不可枚举，不注入（派发层的测试钩子免裁）。
                if let Some(plugin_id) =
                    crate::plugin_registry::bundled_or_discovered_by_type(&node.node_type)
                {
                    if let Some(identity) =
                        crate::plugin_registry::bundled_or_discovered(&plugin_id)
                    {
                        if let Some(capabilities) = identity.capabilities.as_ref() {
                            for entry in capabilities.iter().filter(|entry| entry.required) {
                                if !entry.capability.pattern.is_manifest_only() {
                                    push(entry.capability.pattern.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    let mut data = definition.editable();
    data.permissions = Some(WorkflowPermissions {
        version: flow_dto::permission::PERMISSION_SCHEMA_VERSION,
        mode: ExecutionMode::Execute,
        allow,
    });
    // 动态（插件声明）类型参与校验：echo 节点不是未知类型。
    let dynamic = crate::plugin_registry::dynamic_node_types();
    flow_engine::journal_state::validate_definition_with_dynamic(
        &serde_json::to_value(&data).expect("definition serializable"),
        &dynamic,
    )
    .expect("auto-governed definition must validate")
}

/// URL 的源站前缀；authority 含模板时 None（无法静态派生）。
fn static_origin(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?;
    if host.contains("${") {
        return None;
    }
    let port = parsed
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    Some(format!("{}://{host}{port}/", parsed.scheme()))
}

/// 定义的权限内容摘要（workflow 层 + 各节点层）；legacy 定义为 None。
/// 变化即在新版本事务里记 WorkflowPermissionsChanged（谁在何时改了上限）。
pub fn definition_permissions_digest(definition: &flow_engine::Definition) -> Option<String> {
    let workflow = definition.permissions.as_ref()?;
    let nodes: std::collections::BTreeMap<&str, &NodePermissions> = definition
        .nodes
        .iter()
        .filter_map(|node| node.permissions.as_ref().map(|p| (node.id.as_str(), p)))
        .collect();
    Some(flow_dto::permission::canonical_digest(&serde_json::json!({
        "workflow": workflow,
        "nodes": nodes,
    })))
}

/// 启动静态门（G2 前置）：legacy 定义含副作用节点（内置副作用或动态
/// 插件节点，方案已确认决策 2）→ 拒绝启动并提示迁移；受管定义里的动态
/// 插件节点要求插件已部署且已授予（G4，逐次裁决在派发层）。
///
/// # Errors
/// 返回面向用户的拒绝原因（RPC `-32020`）。
pub fn start_gate(
    definition: &flow_engine::Definition,
    plugin_ceilings: &[flow_dto::permission::PluginCeiling],
    bindings: Option<&BTreeMap<String, PluginBinding>>,
) -> Result<(), String> {
    if definition
        .permissions
        .as_ref()
        .is_some_and(|p| p.mode == ExecutionMode::Bypass)
    {
        return Ok(());
    }
    match &definition.permissions {
        None => {
            let side_effects: Vec<&str> = definition
                .side_effect_nodes()
                .iter()
                .map(|node| node.id.as_str())
                .collect();
            if side_effects.is_empty() {
                return Ok(());
            }
            Err(format!(
                "工作流未声明权限块（legacy），含副作用节点 {}（含动态插件节点）：需由管理员迁移（添加 \
                 permissions 并显式授予能力）后才能启动新 run",
                side_effects.join(", ")
            ))
        }
        Some(_permissions) => {
            // G4：动态插件节点走授予链路（manifest 声明 ⊆ 授予 ∩ workflow \
            // ∩ node，派发时逐次裁决）；启动门只把未部署/未授予的挑出来，
            // fail-fast（-32020 列出节点）。
            let mut missing: Vec<String> = Vec::new();
            for node in definition.nodes.iter().filter(|n| n.kind().is_none()) {
                let plugin_id = match bindings {
                    Some(bindings) => bindings
                        .get(&node.node_type)
                        .map(|binding| binding.plugin_id.clone()),
                    None => crate::plugin_registry::bundled_or_discovered_by_type(&node.node_type),
                };
                match plugin_id {
                    None => missing.push(format!(
                        "节点 {} 的插件类型 {} 未部署（manifest 缺失或已下架）",
                        node.id, node.node_type
                    )),
                    Some(plugin_id) => {
                        if !plugin_ceilings
                            .iter()
                            .any(|ceiling| ceiling.plugin_id == plugin_id)
                        {
                            missing.push(format!(
                                "节点 {} 依赖的插件 {plugin_id} 未授予（permissions.grant 后才能运行）",
                                node.id
                            ));
                        }
                    }
                }
            }
            if !missing.is_empty() {
                return Err(missing.join("；"));
            }
            // Plan + harness agent：工具已受管（变更性工具本地 + master 双重
            // 提前拒绝），允许启动。
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_dto::permission::{OperationSpec, Principal};
    use serde_json::json;

    #[test]
    fn bypass_is_inherited_by_child_workflows_and_start_gate_accepts_it() {
        let definition: flow_engine::Definition = serde_json::from_value(serde_json::json!({
            "nodes": [], "edges": [], "permissions": {"version":1,"mode":"bypass","allow":[]}
        }))
        .unwrap();
        assert!(start_gate(&definition, &[], None).is_ok());
        let parent = freeze_root(&definition, vec![]);
        let child: flow_engine::Definition =
            serde_json::from_value(json!({"nodes": [], "edges": []})).unwrap();
        let frozen = freeze_child(&child, &parent, "parent-run", "node", vec![]);
        assert_eq!(frozen.mode, ExecutionMode::Bypass);
        let request = CheckInput {
            specs: &[OperationSpec::Http {
                method: "POST".into(),
                url: "https://example.com".into(),
                headers_digest: "0".repeat(64),
                body_digest: "0".repeat(64),
            }],
            actor: &Principal::Node {
                run_id: "r".into(),
                node_id: "n".into(),
            },
            node_id: "n",
            request_fingerprint: "fingerprint",
            plugin_id: None,
        };
        assert!(matches!(
            check(&request, &frozen, &[], chrono::Utc::now()),
            Evaluation::Allow { .. }
        ));
    }

    #[test]
    fn plugin_identity_verification_binds_content_hash() {
        let reported = json!({"id": "demo", "content_hash": "a".repeat(64)});
        assert!(verify_plugin_identity("demo", &"a".repeat(64), &reported).is_ok());
        // 哈希不符（换二进制未重授予）→ 拒绝。
        let mismatch = verify_plugin_identity("demo", &"b".repeat(64), &reported)
            .expect_err("hash mismatch must fail");
        assert!(mismatch.contains("I35"), "{mismatch}");
        // id 不符 → 拒绝。
        let id_mismatch = verify_plugin_identity("other", &"a".repeat(64), &reported)
            .expect_err("id mismatch must fail");
        assert!(id_mismatch.contains("身份不符"), "{id_mismatch}");
        // 期望哈希未知（无冻结 ceiling）：不拦截（交给层缺失拒绝）。
        assert!(verify_plugin_identity("demo", "", &reported).is_ok());
    }
}
