//! master 内唯一的权限评估器（G3/G5 共用，§4）。
//!
//! 「检查」分布在六处，「裁决」只在这里。本模块是**纯函数**：输入完整
//! 请求、冻结策略、Run 精确授权与显式时间，输出 Allow/Deny/Ask；不读
//! 隐式时钟、不做 IO——同一输入必得同一结果（I31），regression 与重放
//! 因此不需要重新问人。
//!
//! 分层语义（§3.4）：
//! - 各层（插件授予 → workflow → node → 祖先上限）取 **AND**，任一层没有
//!   匹配即 Deny；
//! - 层内多项匹配取 **OR** 合并能力，但匹配项中任一 Ask 都要求询问；
//! - 顺序无关：不按 pattern 具体度或 id 排序改变授权；
//! - 审批与 Run 精确授权不能越过能力上限或 Plan（I27）。

pub mod derive;

use chrono::{DateTime, Utc};

use derive::normalize_provider;
use flow_dto::permission::{
    Approval, Capability, ExecutionMode, FrozenPolicy, NodePermissions, OperationSpec, Principal,
    RunApproval,
};

/// 评估结论（步骤 0–4 的纯函数结果；步骤 5 的事务执行在 dispatch/命令层）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Evaluation {
    /// 全部能力满足且无需询问（matched = `层:能力 id`）。
    Allow { matched: Vec<String> },
    /// 任一能力上限不满足（I27：审批不能越过）。matched 记录已满足的层。
    Deny { reason: String, matched: Vec<String> },
    /// 能力满足但至少一层要求询问，且无精确 Run 授权免除。
    Ask { matched: Vec<String> },
}

/// 需求集合 + 身份输入。`specs` 非空（§3.6：多个必需动作构成同一次外部
/// 操作的授权需求集合，任一 Deny 则整次拒绝）。
pub struct CheckInput<'a> {
    pub specs: &'a [OperationSpec],
    pub actor: &'a Principal,
    pub node_id: &'a str,
    /// 操作请求字节的摘要（I11 指纹；参与 Run 授权 digest）。
    pub request_fingerprint: &'a str,
    /// 执行该操作的插件身份（P1 G4/G5）；宿主直接执行的请求为 None。
    pub plugin_id: Option<&'a str>,
}

/// 纯函数评估入口。
pub fn check(
    input: &CheckInput<'_>,
    policy: &FrozenPolicy,
    run_approvals: &[RunApproval],
    evaluated_at: DateTime<Utc>,
) -> Evaluation {
    if input.specs.is_empty() {
        return Evaluation::Deny {
            reason: "空需求集合不能授权".into(),
            matched: vec![],
        };
    }
    // 步骤 0：Plan 是不可提升的只读上限，在能力检查前生效。
    if policy.mode == ExecutionMode::Plan
        && input.specs.iter().any(|spec| spec.action().is_mutating())
    {
        return Evaluation::Deny {
            reason: "plan 模式禁止变更性操作（只读上限不可被能力提升越过）".into(),
            matched: vec![],
        };
    }
    // legacy 定义：没有权限块就没有任何能力（不自动生成全能力）。
    if policy.legacy {
        return Evaluation::Deny {
            reason: "legacy 工作流未声明权限块；迁移（添加 permissions）后才能执行外部操作"
                .into(),
            matched: vec![],
        };
    }
    let mut matched: Vec<String> = Vec::new();
    let mut ask = false;
    // 步骤 1：插件授予硬上限（宿主直接执行的请求没有插件层）。
    if let Some(plugin_id) = input.plugin_id {
        let Some(ceiling) = policy
            .plugin_ceilings
            .iter()
            .find(|c| c.plugin_id == plugin_id)
        else {
            return Evaluation::Deny {
                reason: format!("插件 {plugin_id} 无授予记录（待授予状态拒绝执行）"),
                matched,
            };
        };
        let layer = format!("plugin:{plugin_id}");
        if !evaluate_layer(&ceiling.allow, input.specs, evaluated_at, &layer, &mut matched, &mut ask)
        {
            return deny(matched, &layer);
        }
    }
    // 步骤 2：workflow 上限层。
    if !evaluate_layer(
        &policy.workflow.allow,
        input.specs,
        evaluated_at,
        "workflow",
        &mut matched,
        &mut ask,
    ) {
        return deny(matched, "workflow");
    }
    // 步骤 3：node 层（Inherit 继承；Restrict 也必须匹配）。
    if let Some(NodePermissions::Restrict { allow }) = policy.nodes.get(input.node_id) {
        if !evaluate_layer(allow, input.specs, evaluated_at, "node", &mut matched, &mut ask) {
            return deny(matched, "node");
        }
    }
    // 子工作流：祖先上限层逐层 AND 相交（不继承凭据，只收上限）。
    for ceiling in &policy.ceilings {
        if !evaluate_layer(
            &ceiling.allow,
            input.specs,
            evaluated_at,
            &ceiling.source,
            &mut matched,
            &mut ask,
        ) {
            return deny(matched, &ceiling.source);
        }
    }
    // 步骤 4：任一匹配层要求 Ask → 检查同 run/node 的精确 Run 授权。
    if ask {
        let digest = flow_dto::permission::run_approval_digest(
            input.actor,
            input.specs,
            input.request_fingerprint,
            &policy.digest,
        );
        let approved = run_approvals.iter().any(|approval| {
            approval.node_id == input.node_id
                && approval.digest == digest
                && approval.expires_at.is_none_or(|at| at > evaluated_at)
        });
        if !approved {
            return Evaluation::Ask { matched };
        }
        matched.push("run-approval".into());
    }
    Evaluation::Allow { matched }
}

/// 组装 Deny：reason 标注未满足的层（拒绝时记录未满足的层）。
fn deny(matched: Vec<String>, layer: &str) -> Evaluation {
    Evaluation::Deny {
        reason: format!("{layer} 能力上限未满足"),
        matched,
    }
}

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
    freeze(definition, Some((parent, parent_run, parent_node)), plugin_ceilings)
}

fn freeze(
    definition: &flow_engine::Definition,
    parent: Option<(&FrozenPolicy, &str, &str)>,
    plugin_ceilings: Vec<flow_dto::permission::PluginCeiling>,
) -> FrozenPolicy {
    use flow_dto::permission::{CeilingLayer, WorkflowPermissions};
    let mut ceilings = Vec::new();
    let mut parent_plan = false;
    if let Some((parent, run, node)) = parent {
        parent_plan = parent.mode == ExecutionMode::Plan;
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

/// 定义引用的插件授予层：按节点类型解析插件 id（llm → "llm" 插件；
/// harness agent → 快照元数据；动态类型 → 已部署 manifest），再从授予
/// 记录取能力。无授予 → 无层（G3 按「待授予」拒绝）。legacy 确认的授予
/// 展开为全动作族级 Any（无限制执行的显式确认，§7.1）。
pub async fn plugin_ceilings(
    backend: &crate::journal::JournalBackend,
    definition: &flow_engine::Definition,
    agent_plugin_id: Option<&str>,
) -> Vec<flow_dto::permission::PluginCeiling> {
    use flow_dto::permission::{Action, Capability, PluginCeiling, ResourcePattern};
    let mut wanted: Vec<String> = Vec::new();
    for node in &definition.nodes {
        match node.kind() {
            Some(flow_engine::NodeType::Llm) => wanted.push("llm".into()),
            Some(flow_engine::NodeType::Harness)
                if node.is_harness_agent() && agent_plugin_id.is_some() =>
            {
                wanted.push(agent_plugin_id.expect("checked").to_string())
            }
            _ => {
                if node.kind().is_none() {
                    if let Some(plugin) =
                        crate::plugin_registry::bundled_or_discovered_by_type(&node.node_type)
                    {
                        wanted.push(plugin);
                    }
                }
            }
        }
    }
    wanted.sort();
    wanted.dedup();
    let state_ceilings = backend
        .inspect(|state| {
            wanted
                .iter()
                .filter_map(|plugin_id| {
                    // 首选精确内容哈希；否则该插件任意最新授予（哈希执法在
                    // 执行器上报侧，见 P1 协议）。
                    let identity = crate::plugin_registry::bundled_or_discovered(plugin_id);
                    let grant = state
                        .plugin_grants
                        .values()
                        .filter(|grant| grant.plugin_id == *plugin_id)
                        .max_by_key(|grant| grant.granted_at.clone())
                        .cloned()
                        .or_else(|| {
                            identity.as_ref().and_then(|identity| {
                                state
                                    .plugin_grants
                                    .get(&flow_engine::journal_state::PluginGrant::key(
                                        plugin_id,
                                        &identity.content_hash,
                                    ))
                                    .cloned()
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
                    let content_hash = identity
                        .map(|identity| identity.content_hash)
                        .unwrap_or(grant.content_hash.clone());
                    Some(PluginCeiling {
                        plugin_id: plugin_id.clone(),
                        content_hash,
                        allow,
                    })
                })
                .collect::<Vec<_>>()
        })
        .await;
    state_ceilings
}

/// 节点操作对应的插件身份（G3/G5 评估输入；与 freeze 的解析同源）。
pub fn plugin_id_for_node(
    node: &flow_engine::Node,
    agent_plugin_id: Option<&str>,
) -> Option<String> {
    match node.kind() {
        Some(flow_engine::NodeType::Llm) => Some("llm".into()),
        Some(flow_engine::NodeType::Harness) if node.is_harness_agent() => {
            agent_plugin_id.map(str::to_string)
        }
        _ => node
            .kind()
            .is_none()
            .then(|| {
                crate::plugin_registry::bundled_or_discovered_by_type(&node.node_type)
            })
            .flatten(),
    }
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
    // 只含动态插件节点的定义保持 legacy：legacy 门只拦内置副作用节点，动态
    // 节点照旧运行（G4 接入前 plugin_node 无授权步，受管反而会被门拒绝）。
    let has_dynamic = definition.nodes.iter().any(|node| node.kind().is_none());
    let has_builtin_side_effect = definition
        .nodes
        .iter()
        .any(|node| node.kind().is_some_and(flow_engine::NodeType::has_side_effect));
    if has_dynamic && !has_builtin_side_effect {
        return definition.clone();
    }
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
                            methods: crate::HTTP_METHODS
                            .iter()
                            .map(|m| m.to_string())
                            .collect(),
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
                                v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string())
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
                // 动态插件节点：调用不经 G3（plugin_node 无授权步），受管工作流
                // 会被静态门拒绝（fail-closed，接入 G4 前不伪装已受管）。
                // 这里不注入能力，让 legacy 路径保持现状（未受管执行）。
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
    let port = parsed.port().map(|port| format!(":{port}")).unwrap_or_default();
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

/// 启动静态门（G2 前置）：legacy 定义含副作用节点 → 拒绝启动并提示迁移；
/// 受管定义里首版尚不支持的组合 → 拒绝（fail-closed，不能伪装已受管）。
///
/// # Errors
/// 返回面向用户的拒绝原因（RPC `-32020`）。
pub fn start_gate(definition: &flow_engine::Definition) -> Result<(), String> {
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
                "工作流未声明权限块（legacy），含副作用节点 {}：需由管理员迁移（添加 \
                 permissions 并显式授予能力）后才能启动新 run",
                side_effects.join(", ")
            ))
        }
        Some(permissions) => {
            // 动态插件节点的调用不经 G3（plugin_node 无授权步）：插件授予
            // 链路接入前不能伪装已受管，fail-closed（G4 待接入）。
            if let Some(node) = definition.nodes.iter().find(|n| n.kind().is_none()) {
                return Err(format!(
                    "动态插件节点 {}（{}）尚未接入插件授予，权限工作流中不能执行",
                    node.id, node.node_type
                ));
            }
            // Plan + harness agent：工具已受管（变更性工具本地 + master 双重
            // 提前拒绝），允许启动。
            Ok(())
        }
    }
}

/// 评估一层能力：全部 spec 都必须有非过期匹配项。
///
/// 返回 false = 本层不满足（任一 spec 无匹配）。`matched` 追加本层命中项
/// （`层:id`）；`ask` 在任一命中项要求 Ask 时置真（OR 合并能力、AND 询问）。
fn evaluate_layer(
    layer: &[Capability],
    specs: &[OperationSpec],
    now: DateTime<Utc>,
    layer_name: &str,
    matched: &mut Vec<String>,
    ask: &mut bool,
) -> bool {
    for spec in specs {
        let mut hit = false;
        for capability in layer {
            // 过期项在显式 evaluated_at 下视为未匹配（§3.4）。
            let live = capability.expires_at.is_none_or(|at| at > now);
            if live && capability.pattern.matches(spec) {
                hit = true;
                let label = format!("{layer_name}:{}", capability.id);
                if !matched.contains(&label) {
                    matched.push(label);
                }
                if capability.approval == Approval::Ask {
                    *ask = true;
                }
            }
        }
        if !hit {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_dto::permission::{
        parse_capability, parse_workflow_permissions, CeilingLayer, PluginCeiling,
        WorkflowPermissions,
    };
    use serde_json::json;
    use std::collections::BTreeMap;

    fn net_cap(id: &str, prefix: &str, approval: &str) -> Capability {
        parse_capability(&json!({
            "id": id, "action": "net", "methods": ["GET", "POST"],
            "url_prefix": prefix, "approval": approval
        }))
        .unwrap()
    }

    fn http_spec(url: &str) -> OperationSpec {
        OperationSpec::Http {
            method: "POST".into(),
            url: url.into(),
            headers_digest: "0".repeat(64),
            body_digest: "0".repeat(64),
        }
    }

    fn secret_spec(name: &str) -> OperationSpec {
        OperationSpec::Secret {
            secret_name: name.into(),
        }
    }

    fn workflow(mode: ExecutionMode, allow: Vec<Capability>) -> WorkflowPermissions {
        WorkflowPermissions {
            version: 1,
            mode,
            allow,
        }
    }

    fn frozen(
        mode: ExecutionMode,
        allow: Vec<Capability>,
        nodes: BTreeMap<String, NodePermissions>,
        ceilings: Vec<CeilingLayer>,
        plugins: Vec<PluginCeiling>,
    ) -> FrozenPolicy {
        FrozenPolicy::sealed(mode, false, workflow(mode, allow), nodes, ceilings, plugins)
    }

    fn policy(allow: Vec<Capability>) -> FrozenPolicy {
        frozen(ExecutionMode::Execute, allow, BTreeMap::new(), vec![], vec![])
    }

    fn actor() -> Principal {
        Principal::Node {
            run_id: "r".into(),
            node_id: "n".into(),
        }
    }

    static ACTOR: std::sync::LazyLock<Principal> = std::sync::LazyLock::new(actor);

    fn input<'a>(specs: &'a [OperationSpec], fingerprint: &'a str) -> CheckInput<'a> {
        CheckInput {
            specs,
            actor: &ACTOR,
            node_id: "n",
            request_fingerprint: fingerprint,
            plugin_id: None,
        }
    }

    fn check_now(specs: &[OperationSpec], policy: &FrozenPolicy, approvals: &[RunApproval]) -> Evaluation {
        check(&input(specs, "fingerprint"), policy, approvals, Utc::now())
    }

    fn restrict(allow: Vec<Capability>) -> BTreeMap<String, NodePermissions> {
        BTreeMap::from([("n".to_string(), NodePermissions::Restrict { allow })])
    }

    #[test]
    fn unmatched_is_denied_not_asked() {
        // 未授予的动作直接 Deny：点批准不能获得能力（§3.4）。
        match check_now(&[http_spec("https://api.example.com/x")], &policy(vec![]), &[]) {
            Evaluation::Deny { reason, .. } => assert!(reason.contains("workflow"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
    }

    #[test]
    fn node_auto_cannot_override_workflow_ask() {
        // workflow Ask + node Auto：最终仍需询问（重叠与排序无关）。
        let p = frozen(
            ExecutionMode::Execute,
            vec![net_cap("w", "https://api.example.com/", "ask")],
            restrict(vec![net_cap("n1", "https://api.example.com/", "auto")]),
            vec![],
            vec![],
        );
        match check_now(&[http_spec("https://api.example.com/x")], &p, &[]) {
            Evaluation::Ask { matched } => {
                assert_eq!(matched, vec!["workflow:w".to_string(), "node:n1".to_string()]);
            }
            other => panic!("expected ask, got {other:?}"),
        }
    }

    #[test]
    fn node_restrict_intersects_and_empty_restrict_denies() {
        let allow = vec![
            net_cap("a", "https://a.example.com/", "auto"),
            net_cap("b", "https://b.example.com/", "auto"),
        ];
        let p = frozen(
            ExecutionMode::Execute,
            allow.clone(),
            restrict(vec![net_cap("na", "https://a.example.com/", "auto")]),
            vec![],
            vec![],
        );
        assert!(matches!(
            check_now(&[http_spec("https://a.example.com/x")], &p, &[]),
            Evaluation::Allow { .. }
        ));
        match check_now(&[http_spec("https://b.example.com/x")], &p, &[]) {
            Evaluation::Deny { reason, .. } => assert!(reason.contains("node"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
        // Restrict 空列表 = 禁止所有外部能力。
        let p = frozen(ExecutionMode::Execute, allow, restrict(vec![]), vec![], vec![]);
        assert!(matches!(
            check_now(&[http_spec("https://a.example.com/x")], &p, &[]),
            Evaluation::Deny { .. }
        ));
    }

    #[test]
    fn multi_action_needs_all_specs_allowed() {
        // Http + Secret 组合：Secret 未授予 → 整次 Deny（I37 的评估侧）。
        let p = policy(vec![net_cap("w", "https://api.example.com/", "auto")]);
        let specs = [http_spec("https://api.example.com/x"), secret_spec("k")];
        match check_now(&specs, &p, &[]) {
            Evaluation::Deny { reason, .. } => assert!(reason.contains("workflow"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
    }

    #[test]
    fn plan_denies_mutating_before_capabilities() {
        // Plan 上限在能力检查前生效：即使能力全给也 Deny。
        let p = frozen(
            ExecutionMode::Plan,
            vec![net_cap("w", "https://api.example.com/", "auto")],
            BTreeMap::new(),
            vec![],
            vec![],
        );
        match check_now(&[http_spec("https://api.example.com/x")], &p, &[]) {
            Evaluation::Deny { reason, .. } => assert!(reason.contains("plan"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
        // 只读动作不受 Plan 影响（仍需能力匹配）。
        let read_only = parse_workflow_permissions(&json!({
            "version": 1, "mode": "plan",
            "allow": [{"id":"r","action":"file_read","subtree":"src","approval":"auto"}]
        }))
        .unwrap();
        let p = frozen(
            ExecutionMode::Plan,
            read_only.allow,
            BTreeMap::new(),
            vec![],
            vec![],
        );
        let read = [OperationSpec::FileRead {
            path: "src/a.rs".into(),
        }];
        assert!(matches!(check_now(&read, &p, &[]), Evaluation::Allow { .. }));
    }

    #[test]
    fn legacy_policy_denies_everything() {
        let legacy = FrozenPolicy::sealed(
            ExecutionMode::Execute,
            true,
            workflow(ExecutionMode::Execute, vec![]),
            BTreeMap::new(),
            vec![],
            vec![],
        );
        match check_now(&[secret_spec("k")], &legacy, &[]) {
            Evaluation::Deny { reason, .. } => assert!(reason.contains("legacy"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
    }

    #[test]
    fn expired_capability_is_unmatched() {
        let past = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
        let cap = parse_capability(&json!({
            "id":"net-exp", "action":"net", "methods":["POST"],
            "url_prefix":"https://api.example.com/", "approval":"auto",
            "expires_at": past
        }))
        .unwrap();
        assert!(matches!(
            check_now(&[http_spec("https://api.example.com/x")], &policy(vec![cap]), &[]),
            Evaluation::Deny { .. }
        ));
    }

    #[test]
    fn run_approval_exempts_ask_only_for_exact_digest() {
        let p = policy(vec![net_cap("w", "https://api.example.com/", "ask")]);
        let specs = [http_spec("https://api.example.com/x")];
        // 无授权：Ask。
        assert!(matches!(check_now(&specs, &p, &[]), Evaluation::Ask { .. }));
        // 精确授权：免再次询问。
        let digest =
            flow_dto::permission::run_approval_digest(&actor(), &specs, "fingerprint", &p.digest);
        let approval = |digest: String, expires_at| RunApproval {
            node_id: "n".into(),
            actor: actor(),
            digest,
            expires_at,
            by: Principal::LocalAdmin,
        };
        let approvals = vec![approval(digest.clone(), None)];
        match check_now(&specs, &p, &approvals) {
            Evaluation::Allow { matched } => assert!(matched.contains(&"run-approval".to_string())),
            other => panic!("expected allow, got {other:?}"),
        }
        // 内容变化（不同 URL）→ 新摘要 → 仍 Ask；不能挪给下一次调用。
        let other = [http_spec("https://api.example.com/other")];
        assert!(matches!(check_now(&other, &p, &approvals), Evaluation::Ask { .. }));
        // 指纹变化（同 URL 不同 body/cwd）→ 仍 Ask。
        assert!(matches!(
            check(&input(&specs, "fingerprint-2"), &p, &approvals, Utc::now()),
            Evaluation::Ask { .. }
        ));
        // 授权过期 → 重新询问。
        let expired = vec![approval(digest, Some(Utc::now() - chrono::Duration::hours(1)))];
        assert!(matches!(check_now(&specs, &p, &expired), Evaluation::Ask { .. }));
    }

    #[test]
    fn evaluation_is_deterministic_regardless_of_order() {
        // 同层多项匹配 OR 合并、命中项与顺序无关（I31）。
        let a = net_cap("a", "https://a.example.com/", "auto");
        let b = net_cap("b", "https://a.example.com/", "auto");
        let ids = |e: Evaluation| match e {
            Evaluation::Allow { mut matched } => {
                matched.sort();
                matched
            }
            other => panic!("expected allow, got {other:?}"),
        };
        let spec = [http_spec("https://a.example.com/x")];
        let one = ids(check_now(&spec, &policy(vec![a.clone(), b.clone()]), &[]));
        let two = ids(check_now(&spec, &policy(vec![b, a]), &[]));
        assert_eq!(one, two);
        assert_eq!(one.len(), 2);
    }

    #[test]
    fn plugin_ceiling_denies_when_ungranted() {
        let p = frozen(
            ExecutionMode::Execute,
            vec![net_cap("w", "https://api.example.com/", "auto")],
            BTreeMap::new(),
            vec![],
            vec![PluginCeiling {
                plugin_id: "llm".into(),
                content_hash: "abc".into(),
                allow: vec![],
            }],
        );
        let specs = [http_spec("https://api.example.com/x")];
        let mut with_plugin = input(&specs, "f");
        with_plugin.plugin_id = Some("llm");
        match check(&with_plugin, &p, &[], Utc::now()) {
            Evaluation::Deny { reason, .. } => assert!(reason.contains("plugin:llm"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
        // 无授予记录的插件同样拒绝。
        with_plugin.plugin_id = Some("other");
        match check(&with_plugin, &p, &[], Utc::now()) {
            Evaluation::Deny { reason, .. } => assert!(reason.contains("待授予"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
    }

    #[test]
    fn ancestor_ceilings_intersect() {
        // 子工作流：父只授予 a → b 被父层拒绝。
        let p = frozen(
            ExecutionMode::Execute,
            vec![
                net_cap("a", "https://a.example.com/", "auto"),
                net_cap("b", "https://b.example.com/", "auto"),
            ],
            BTreeMap::new(),
            vec![CeilingLayer {
                source: "parent:r0/call:workflow".into(),
                allow: vec![net_cap("pa", "https://a.example.com/", "auto")],
            }],
            vec![],
        );
        assert!(matches!(
            check_now(&[http_spec("https://a.example.com/x")], &p, &[]),
            Evaluation::Allow { .. }
        ));
        match check_now(&[http_spec("https://b.example.com/x")], &p, &[]) {
            Evaluation::Deny { reason, .. } => assert!(reason.contains("parent:r0"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
    }

    #[test]
    fn digest_binds_mode_and_layers() {
        let a = policy(vec![net_cap("w", "https://a.example.com/", "auto")]);
        let b = frozen(
            ExecutionMode::Plan,
            vec![net_cap("w", "https://a.example.com/", "auto")],
            BTreeMap::new(),
            vec![],
            vec![],
        );
        assert_ne!(a.digest, b.digest);
        assert!(a.digest_is_consistent());
    }
}
