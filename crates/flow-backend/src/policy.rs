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
    Deny {
        reason: String,
        matched: Vec<String>,
    },
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
            reason: "legacy 工作流未声明权限块；迁移（添加 permissions）后才能执行外部操作".into(),
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
        if !evaluate_layer(
            &ceiling.allow,
            input.specs,
            evaluated_at,
            &layer,
            &mut matched,
            &mut ask,
        ) {
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
        if !evaluate_layer(
            allow,
            input.specs,
            evaluated_at,
            "node",
            &mut matched,
            &mut ask,
        ) {
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
// G4：动态插件节点的需求包含评估（09 §7）
// ---------------------------------------------------------------------------

/// G4 评估输入：插件 manifest 声明的 required 能力是**模式**而非具体请求。
/// `unrestricted = true` 表示 manifest 未声明（legacy-unrestricted）：需求
/// 不可枚举，只能逐次人工确认。
pub struct PluginNeedsInput<'a> {
    pub plugin_id: &'a str,
    pub node_id: &'a str,
    pub actor: &'a Principal,
    /// manifest 声明的 required 能力（Declared 时非空可空；Legacy 时为空）。
    pub needs: &'a [Capability],
    /// 请求指纹：节点参数 + manifest 需求的 canonical 摘要（Run 授权用它
    /// 绑定「精确重复」）。
    pub request_fingerprint: &'a str,
    /// manifest 是否未声明（legacy-unrestricted，已由管理员确认授予）。
    pub unrestricted: bool,
}

/// G4：动态插件节点的授权评估。问题从「层覆盖具体请求」变为
/// 「层覆盖 manifest 声明的需求模式」：层间 AND、层内 OR、命中项 Ask
/// 即需询问，与 [`check`] 同构；区别仅在匹配语义用
/// [`ResourcePattern::covers`](flow_dto::permission::ResourcePattern::covers)。
/// Run 精确授权以空 specs + 请求指纹派生摘要（无具体请求可枚举）。
pub fn check_plugin_needs(
    input: &PluginNeedsInput<'_>,
    policy: &FrozenPolicy,
    run_approvals: &[RunApproval],
    evaluated_at: DateTime<Utc>,
) -> Evaluation {
    // 插件进程的副作用面不可枚举：Plan 下无法证明只读，一律拒绝。
    if policy.mode == ExecutionMode::Plan {
        return Evaluation::Deny {
            reason: "plan 模式禁止插件节点（无法证明只读）".into(),
            matched: vec![],
        };
    }
    if policy.legacy {
        return Evaluation::Deny {
            reason: "legacy 工作流未声明权限块；迁移（添加 permissions）后才能执行外部操作".into(),
            matched: vec![],
        };
    }
    let Some(ceiling) = policy
        .plugin_ceilings
        .iter()
        .find(|c| c.plugin_id == input.plugin_id)
    else {
        return Evaluation::Deny {
            reason: format!("插件 {} 无授予记录（待授予状态拒绝执行）", input.plugin_id),
            matched: vec![],
        };
    };
    // legacy-unrestricted：需求不可枚举，节点/workflow 层约束不了它；
    // 已由管理员确认授予 → 逐次人工确认（Run 精确授权可免同指纹重复）。
    if input.unrestricted {
        let digest = flow_dto::permission::run_approval_digest(
            input.actor,
            &[],
            input.request_fingerprint,
            &policy.digest,
        );
        let approved = run_approvals.iter().any(|approval| {
            approval.node_id == input.node_id
                && approval.digest == digest
                && approval.expires_at.is_none_or(|at| at > evaluated_at)
        });
        let label = format!("plugin:{}:legacy-confirmed", input.plugin_id);
        if approved {
            return Evaluation::Allow {
                matched: vec![label, "run-approval".into()],
            };
        }
        return Evaluation::Ask {
            matched: vec![label],
        };
    }
    let mut matched: Vec<String> = Vec::new();
    let mut ask = false;
    let mut layers: Vec<(&[Capability], String)> = Vec::new();
    layers.push((&ceiling.allow, format!("plugin:{}", input.plugin_id)));
    layers.push((policy.workflow.allow.as_slice(), "workflow".to_string()));
    if let Some(NodePermissions::Restrict { allow }) = policy.nodes.get(input.node_id) {
        layers.push((allow.as_slice(), "node".to_string()));
    }
    for ceiling in &policy.ceilings {
        layers.push((ceiling.allow.as_slice(), ceiling.source.clone()));
    }
    for (allow, layer) in &layers {
        if !covers_layer(
            allow,
            input.needs,
            evaluated_at,
            layer,
            &mut matched,
            &mut ask,
        ) {
            return deny(matched, layer);
        }
    }
    if ask {
        let digest = flow_dto::permission::run_approval_digest(
            input.actor,
            &[],
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

/// [`evaluate_layer`] 的模式版：全部需求都被层内某个未过期能力覆盖才算
/// 满足；命中 Ask 即询问（层内 OR、层间 AND、跨层 OR 询问）。
fn covers_layer(
    layer: &[Capability],
    needs: &[Capability],
    now: DateTime<Utc>,
    layer_name: &str,
    matched: &mut Vec<String>,
    ask: &mut bool,
) -> bool {
    for need in needs {
        let mut hit = false;
        for capability in layer {
            let live = capability.expires_at.is_none_or(|at| at > now);
            if live && capability.pattern.covers(&need.pattern) {
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
                    // I35：授予以 (plugin_id, content_hash) 为键——二进制或
                    // manifest 变化后旧授予不再适用（换内容必须重新授予）。
                    // identity 不可得时（目录不可读）才退回该插件任意最新授予，
                    // 由执行器上报哈希侧再核验。
                    let identity = crate::plugin_registry::bundled_or_discovered(plugin_id);
                    // I35：只认 (plugin_id, 当前内容哈希) 的精确授予——
                    // 二进制或 manifest 变化后旧授予不再适用（换内容必须
                    // 重新授予）。
                    let grant = identity
                        .as_ref()
                        .and_then(|identity| {
                            state
                                .plugin_grants
                                .get(&flow_engine::journal_state::PluginGrant::key(
                                    plugin_id,
                                    &identity.content_hash,
                                ))
                                .cloned()
                        })
                        // 测试兜底：进程内测试没有启动期授予时，内置插件按其
                        // 声明能力给一个瞬态上限（不落 journal；生产路径恒有
                        // 启动期自动授予，走不到这里）。
                        .or_else(|| {
                            crate::journal_commands::tests_auto_govern()
                                .then(|| {
                                    identity.as_ref().map(|identity| {
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
                                })
                                .flatten()
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
            .then(|| crate::plugin_registry::bundled_or_discovered_by_type(&node.node_type))
            .flatten(),
    }
}

/// G4：动态插件节点的需求来源（master 侧 manifest，不信任执行器自报）：
/// `(plugin_id, required 能力, 是否 legacy-unrestricted)`。插件未部署返回
/// `None`（派发层按能力拒绝收口）。
pub fn plugin_needs_for_type(node_type: &str) -> Option<(String, Vec<Capability>, bool)> {
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
) -> Result<(), String> {
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
                match crate::plugin_registry::bundled_or_discovered_by_type(&node.node_type) {
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
        parse_capability, parse_workflow_permissions, CeilingLayer, PluginCeiling, ResourcePattern,
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
        frozen(
            ExecutionMode::Execute,
            allow,
            BTreeMap::new(),
            vec![],
            vec![],
        )
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

    fn check_now(
        specs: &[OperationSpec],
        policy: &FrozenPolicy,
        approvals: &[RunApproval],
    ) -> Evaluation {
        check(&input(specs, "fingerprint"), policy, approvals, Utc::now())
    }

    fn restrict(allow: Vec<Capability>) -> BTreeMap<String, NodePermissions> {
        BTreeMap::from([("n".to_string(), NodePermissions::Restrict { allow })])
    }

    #[test]
    fn unmatched_is_denied_not_asked() {
        // 未授予的动作直接 Deny：点批准不能获得能力（§3.4）。
        match check_now(
            &[http_spec("https://api.example.com/x")],
            &policy(vec![]),
            &[],
        ) {
            Evaluation::Deny { reason, .. } => assert!(reason.contains("workflow"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
    }

    #[test]
    fn proxied_request_requires_net_capability_for_proxy_origin() {
        // 带代理的请求是两条需求（目标 + 代理 origin）：只授目标 → Deny。
        let target_only = policy(vec![net_cap("t", "https://api.example.com/", "auto")]);
        let proxied = vec![
            http_spec("https://api.example.com/x"),
            OperationSpec::Http {
                method: "POST".into(),
                url: "http://proxy.local:8080/".into(),
                headers_digest: "0".repeat(64),
                body_digest: "0".repeat(64),
            },
        ];
        match check_now(&proxied, &target_only, &[]) {
            Evaluation::Deny { reason, .. } => {
                assert!(reason.contains("workflow"), "{reason}")
            }
            other => panic!("expected deny without proxy capability, got {other:?}"),
        }
        // 目标与代理都授 → Allow，且 matched 记录两条能力。
        let both = policy(vec![
            net_cap("t", "https://api.example.com/", "auto"),
            net_cap("p", "http://proxy.local:8080/", "auto"),
        ]);
        match check_now(&proxied, &both, &[]) {
            Evaluation::Allow { matched } => {
                assert!(matched.contains(&"workflow:t".to_string()));
                assert!(matched.contains(&"workflow:p".to_string()));
            }
            other => panic!("expected allow with both capabilities, got {other:?}"),
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
                assert_eq!(
                    matched,
                    vec!["workflow:w".to_string(), "node:n1".to_string()]
                );
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
        let p = frozen(
            ExecutionMode::Execute,
            allow,
            restrict(vec![]),
            vec![],
            vec![],
        );
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
        assert!(matches!(
            check_now(&read, &p, &[]),
            Evaluation::Allow { .. }
        ));
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
            check_now(
                &[http_spec("https://api.example.com/x")],
                &policy(vec![cap]),
                &[]
            ),
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
        assert!(matches!(
            check_now(&other, &p, &approvals),
            Evaluation::Ask { .. }
        ));
        // 指纹变化（同 URL 不同 body/cwd）→ 仍 Ask。
        assert!(matches!(
            check(&input(&specs, "fingerprint-2"), &p, &approvals, Utc::now()),
            Evaluation::Ask { .. }
        ));
        // 授权过期 → 重新询问。
        let expired = vec![approval(
            digest,
            Some(Utc::now() - chrono::Duration::hours(1)),
        )];
        assert!(matches!(
            check_now(&specs, &p, &expired),
            Evaluation::Ask { .. }
        ));
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

    // -----------------------------------------------------------------
    // G4：check_plugin_needs 真值表
    // -----------------------------------------------------------------

    fn need_cap(id: &str, pattern: ResourcePattern) -> Capability {
        Capability {
            id: id.to_string(),
            pattern,
            approval: Approval::Auto,
            expires_at: None,
        }
    }

    fn plugin_policy(allow: Vec<Capability>, grant: Vec<Capability>) -> FrozenPolicy {
        frozen(
            ExecutionMode::Execute,
            allow,
            BTreeMap::new(),
            vec![],
            vec![PluginCeiling {
                plugin_id: "demo".into(),
                content_hash: "0".repeat(64),
                allow: grant,
            }],
        )
    }

    fn needs_input<'a>(needs: &'a [Capability], fingerprint: &'a str) -> PluginNeedsInput<'a> {
        PluginNeedsInput {
            plugin_id: "demo",
            node_id: "n",
            actor: &ACTOR,
            needs,
            request_fingerprint: fingerprint,
            unrestricted: false,
        }
    }

    fn check_needs(
        needs: &[Capability],
        policy: &FrozenPolicy,
        approvals: &[RunApproval],
    ) -> Evaluation {
        check_plugin_needs(&needs_input(needs, "fp"), policy, approvals, Utc::now())
    }

    #[test]
    fn plugin_needs_covered_by_all_layers_allows() {
        let need = need_cap(
            "m-net",
            ResourcePattern::Net {
                methods: vec!["GET".into()],
                url_prefix: "https://api.example.com/v1/".into(),
            },
        );
        // workflow 层比需求更宽（多方法 + 更短前缀）；授予层与需求同宽。
        let policy = plugin_policy(
            vec![need_cap(
                "w-net",
                ResourcePattern::Net {
                    methods: vec!["GET".into(), "POST".into()],
                    url_prefix: "https://api.example.com/".into(),
                },
            )],
            vec![need.clone()],
        );
        match check_needs(&[need], &policy, &[]) {
            Evaluation::Allow { matched } => {
                assert!(
                    matched.contains(&"workflow:w-net".to_string()),
                    "{matched:?}"
                );
                assert!(
                    matched.contains(&"plugin:demo:m-net".to_string()),
                    "{matched:?}"
                );
            }
            other => panic!("expected allow, got {other:?}"),
        }
    }

    #[test]
    fn plugin_needs_workflow_gap_denies() {
        let need = need_cap(
            "m-net",
            ResourcePattern::Net {
                methods: vec!["POST".into()],
                url_prefix: "https://api.example.com/v1/".into(),
            },
        );
        // workflow 只给了 GET：覆盖不了 POST 需求。
        let policy = plugin_policy(
            vec![need_cap(
                "w-net",
                ResourcePattern::Net {
                    methods: vec!["GET".into()],
                    url_prefix: "https://api.example.com/".into(),
                },
            )],
            vec![need.clone()],
        );
        match check_needs(&[need], &policy, &[]) {
            Evaluation::Deny { reason, .. } => assert!(reason.contains("workflow"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
    }

    #[test]
    fn plugin_needs_ask_propagates_and_run_approval_exempts() {
        let need = need_cap(
            "m-net",
            ResourcePattern::Net {
                methods: vec!["GET".into()],
                url_prefix: "https://api.example.com/".into(),
            },
        );
        let ask_grant = Capability {
            approval: Approval::Ask,
            ..need.clone()
        };
        let policy = plugin_policy(vec![need.clone()], vec![ask_grant]);
        match check_needs(std::slice::from_ref(&need), &policy, &[]) {
            Evaluation::Ask { .. } => {}
            other => panic!("expected ask, got {other:?}"),
        }
        // Run 授权摘要 = (actor, 空 specs, fingerprint, policy digest)。
        let digest = flow_dto::permission::run_approval_digest(&ACTOR, &[], "fp", &policy.digest);
        let approvals = [RunApproval {
            node_id: "n".into(),
            actor: ACTOR.clone(),
            digest,
            expires_at: None,
            by: Principal::System,
        }];
        match check_needs(&[need], &policy, &approvals) {
            Evaluation::Allow { matched } => assert!(matched.contains(&"run-approval".to_string())),
            other => panic!("expected allow, got {other:?}"),
        }
    }

    #[test]
    fn plugin_needs_unrestricted_asks_every_dispatch() {
        let policy = plugin_policy(vec![], vec![]);
        let mut input = needs_input(&[], "fp");
        input.unrestricted = true;
        match check_plugin_needs(&input, &policy, &[], Utc::now()) {
            Evaluation::Ask { matched } => {
                assert_eq!(matched, vec!["plugin:demo:legacy-confirmed".to_string()])
            }
            other => panic!("expected ask, got {other:?}"),
        }
    }

    #[test]
    fn plugin_needs_plan_and_missing_grant_deny() {
        let need = need_cap(
            "m-net",
            ResourcePattern::Net {
                methods: vec!["GET".into()],
                url_prefix: "https://api.example.com/".into(),
            },
        );
        // Plan：不可证明只读 → 拒绝。
        let plan = frozen(
            ExecutionMode::Plan,
            vec![need.clone()],
            BTreeMap::new(),
            vec![],
            vec![PluginCeiling {
                plugin_id: "demo".into(),
                content_hash: "0".repeat(64),
                allow: vec![need.clone()],
            }],
        );
        match check_needs(std::slice::from_ref(&need), &plan, &[]) {
            Evaluation::Deny { reason, .. } => assert!(reason.contains("plan"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
        // 未授予：无 ceiling → 拒绝。
        let ungranted = policy(vec![need]);
        match check_needs(&[], &ungranted, &[]) {
            Evaluation::Deny { reason, .. } => assert!(reason.contains("授予"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
    }

    #[test]
    fn plugin_needs_expired_capability_does_not_cover() {
        let need = need_cap(
            "m-net",
            ResourcePattern::Net {
                methods: vec!["GET".into()],
                url_prefix: "https://api.example.com/".into(),
            },
        );
        let expired = Capability {
            expires_at: Some(Utc::now() - chrono::Duration::hours(1)),
            ..need.clone()
        };
        let policy = plugin_policy(vec![need.clone()], vec![expired]);
        match check_needs(&[need], &policy, &[]) {
            Evaluation::Deny { reason, .. } => assert!(reason.contains("plugin"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
    }

    #[test]
    fn plugin_needs_node_restrict_narrows_workflow() {
        let need = need_cap(
            "m-net",
            ResourcePattern::Net {
                methods: vec!["GET".into()],
                url_prefix: "https://api.example.com/".into(),
            },
        );
        // node Restrict 为空层：盖不住任何需求 → Deny（层间 AND）。
        let policy = frozen(
            ExecutionMode::Execute,
            vec![need.clone()],
            restrict(vec![]),
            vec![],
            vec![PluginCeiling {
                plugin_id: "demo".into(),
                content_hash: "0".repeat(64),
                allow: vec![need.clone()],
            }],
        );
        match check_needs(&[need], &policy, &[]) {
            Evaluation::Deny { reason, .. } => assert!(reason.contains("node"), "{reason}"),
            other => panic!("expected deny, got {other:?}"),
        }
    }

    #[test]
    fn plugin_needs_empty_declared_allows_without_asking() {
        // 声明了权限但无 required 能力：插件自我声明无外部动作 → 直接放行。
        let policy = plugin_policy(vec![], vec![]);
        match check_needs(&[], &policy, &[]) {
            Evaluation::Allow { matched } => assert!(matched.is_empty()),
            other => panic!("expected allow, got {other:?}"),
        }
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
