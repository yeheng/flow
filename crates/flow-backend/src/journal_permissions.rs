//! 权限模型的命令与读面（09 §6/§7.3）：人工审批（approve/deny）、Run 精确
//! 授权撤销、待审批收件箱与有效权限查询。
//!
//! 裁决仍只在一处（`policy::check`）：批准命令在提交时以当前时间**重新评估**
//! 冻结策略——能力已过期/上限不再满足则直接拒绝，不向外层默认策略回退
//! （§4）；审批永远不授予能力（I27），Run 授权只免重复询问。
//!
//! 并发语义：批准、TTL、取消都经同一决策锁串行竞争，折叠层只接受「当前
//! pending 的那一次请求实例」的裁决（I32）；同 hash 重复提交返回既有结论，
//! 换了 hash 拒绝。
use crate::journal::{CommandReceipt, JournalBackend, JournalError};
use flow_dto::permission::{
    ApprovalScope, Decision, DecisionRecord, Modifiers, NodePermissions, Principal, RunApproval,
};
use flow_engine::journal_state::{
    ApprovalState, ApprovalStatus, Operation, PermissionDecidedPayload, Run, State,
};
use flow_journal::{Event, EventKind, StoredValue};
use serde_json::{json, Value};

type Result<T> = std::result::Result<T, JournalError>;

/// 审批理由上限（与人工裁决同规）。
const REASON_MAX_BYTES: usize = 4096;
/// 收件箱里内联展示的请求正文上限；更大的正文只给引用（宿主只读展开）。
const PREVIEW_MAX_BYTES: usize = 16 * 1024;

fn conflict(message: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Conflict(message.into())
}

/// 旧版本权限摘要缓存（checksum → digest）：保存路径每次都要对比上一版，
/// 物化 Ref 定义是磁盘读，按 checksum 命中后只发生一次。
static PERMISSION_DIGEST_CACHE: std::sync::OnceLock<
    std::sync::Mutex<std::collections::HashMap<String, Option<String>>>,
> = std::sync::OnceLock::new();

impl JournalBackend {
    /// 计算某个定义版本的权限内容摘要（legacy = None），按 checksum 缓存。
    pub(crate) async fn version_permissions_digest(
        &self,
        version: &flow_engine::journal_state::DefinitionVersion,
    ) -> Result<Option<String>> {
        let cache = PERMISSION_DIGEST_CACHE.get_or_init(Default::default);
        if let Some(hit) = cache
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(&version.checksum)
        {
            return Ok(hit.clone());
        }
        let definition = self
            .cached_definition(
                &format!("permissions-digest:{}", version.checksum),
                &version.definition,
            )
            .await?;
        let digest = crate::policy::definition_permissions_digest(&definition);
        cache
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(version.checksum.clone(), digest.clone());
        Ok(digest)
    }
}

fn forbidden(message: impl Into<String>) -> flow_journal::Error {
    flow_journal::Error::Forbidden(message.into())
}

/// 一次人工裁决的参数（approve/deny 共用）。
pub struct ApprovalCommand<'a> {
    pub run_id: &'a str,
    pub node_id: &'a str,
    pub operation_id: &'a str,
    pub request_hash: &'a str,
    pub allow: bool,
    pub scope: ApprovalScope,
    pub reason: Option<&'a str>,
    pub by: Principal,
}

impl JournalBackend {
    /// `run.approve` / `run.deny`：裁决该节点**当前 pending** 的请求实例。
    ///
    /// Allow 同一事务写 PermissionDecided{by} +（scope=run 时）
    /// RunApprovalUpserted + OperationIntent/Authorized（节点级，I26）；
    /// 提交后由该派发的 runner 观察到授权并下发 Permit。Deny 只写裁决。
    ///
    /// # Errors
    /// 节点无 pending 询问 / 实例或 hash 不符 / 已被另一结论裁决 →
    /// `Conflict`；审批已过期 → `Forbidden`；参数非法 → `Invalid`。
    pub async fn run_decide(
        &self,
        command: ApprovalCommand<'_>,
        request_id: &str,
    ) -> Result<CommandReceipt> {
        if let Some(reason) = command.reason {
            if reason.len() > REASON_MAX_BYTES {
                return Err(flow_journal::Error::Invalid(format!(
                    "reason exceeds {REASON_MAX_BYTES} bytes"
                ))
                .into());
            }
        }
        if !command.allow && command.reason.is_none_or(|reason| reason.trim().is_empty()) {
            return Err(flow_journal::Error::Invalid("deny requires a reason".into()).into());
        }
        if !command.allow && command.scope == ApprovalScope::Run {
            return Err(flow_journal::Error::Invalid(
                "deny does not take an approval scope".into(),
            )
            .into());
        }
        let verb = if command.allow {
            "run.approve"
        } else {
            "run.deny"
        };
        let scope_name = format!("{verb}:{}", command.run_id);
        let request = json!({
            "run_id": command.run_id,
            "node_id": command.node_id,
            "operation_id": command.operation_id,
            "request_hash": command.request_hash,
            "scope": command.scope,
            "reason": command.reason,
            "by": command.by,
        });
        let run_id = command.run_id.to_string();
        self.command_for_run(&run_id, &scope_name, Some(request_id), &request, |state| {
            decide(state, &command)
        })
        .await
    }

    /// `run.approvals.revoke`：撤销一条 Run 精确授权（只影响之后的询问；
    /// 已经发生的副作用不撤销）。
    ///
    /// # Errors
    /// run/授权不存在 → `NotFound`。
    pub async fn run_approval_revoke(
        &self,
        run_id: &str,
        node_id: &str,
        digest: &str,
        by: Principal,
        request_id: &str,
    ) -> Result<CommandReceipt> {
        let request = json!({"run_id": run_id, "node_id": node_id, "digest": digest, "by": by});
        let scope = format!("run.approvals.revoke:{run_id}");
        self.command_for_run(run_id, &scope, Some(request_id), &request, |state| {
            let run = state
                .runs
                .get(run_id)
                .ok_or_else(|| flow_journal::Error::NotFound("run not found".into()))?;
            if !run
                .approvals
                .iter()
                .any(|a| a.node_id == node_id && a.digest == digest)
            {
                return Err(flow_journal::Error::NotFound(
                    "run approval not found".into(),
                ));
            }
            let mut event = Event::new(
                EventKind::RunApprovalRemoved,
                json!({"node_id": node_id, "digest": digest, "by": by}),
            );
            event.run_id = Some(run_id.to_string());
            event.run_seq = run.last_run_seq + 1;
            Ok((vec![event], json!({"revoked": true})))
        })
        .await
    }

    /// 待审批收件箱（`approvals.list`）：在飞 run 的节点审批状态。
    ///
    /// 只返回 master 持久请求生成的字段（插件不能覆盖）；secret 只以名称
    /// 出现在需求集合里，正文只在有界时内联。
    pub async fn approvals_view(&self, run_id: Option<&str>, all: bool) -> Value {
        self.inspect(|state| {
            let mut items = Vec::new();
            for run in state.runs.values() {
                if run_id.is_some_and(|id| id != run.run_id) {
                    continue;
                }
                if run_id.is_none() && run.terminal() {
                    continue;
                }
                for (node_id, node) in &run.nodes {
                    let Some(approval) = &node.approval else {
                        continue;
                    };
                    if !all && approval.status != ApprovalStatus::Pending {
                        continue;
                    }
                    items.push(approval_item(run, node_id, approval));
                }
            }
            // 截止时间近的排前（收件箱按紧迫度）。
            items.sort_by_key(|item| item["deadline"].as_i64().unwrap_or(i64::MAX));
            json!({"items": items, "server_time": chrono::Utc::now().timestamp_millis()})
        })
        .await
    }
}

impl JournalBackend {
    /// `permissions.effective`：`run_id` 给定时按该 run 的冻结快照回答；
    /// 否则按工作流的指定版本（缺省最新版本）回答。两者不混用（§7.3）。
    ///
    /// # Errors
    /// run / 工作流 / 版本不存在 → `NotFound`；参数缺失 → `Invalid`。
    pub async fn permissions_effective_view(
        &self,
        workflow_id: Option<&str>,
        version: Option<u64>,
        node_id: Option<&str>,
        run_id: Option<&str>,
    ) -> Result<Value> {
        if let Some(run_id) = run_id {
            let run = self
                .run_snapshot(run_id)
                .await?
                .ok_or_else(|| flow_journal::Error::NotFound("run not found".into()))?;
            let definition = self
                .cached_definition(&format!("effective:{run_id}"), &run.definition)
                .await?;
            let mut view = effective_permissions(&definition, run.policy.as_ref(), node_id);
            view["run_id"] = json!(run_id);
            view["frozen"] = json!(run.policy.is_some());
            view["approvals"] = serde_json::to_value(&run.approvals)?;
            return Ok(view);
        }
        let workflow_id = workflow_id
            .ok_or_else(|| flow_journal::Error::Invalid("workflow_id or run_id required".into()))?;
        let selected = self
            .inspect(|state| {
                let workflow = state.workflows.get(workflow_id).filter(|w| !w.deleted)?;
                match version {
                    Some(version) => workflow.versions.get(&version).cloned(),
                    None => workflow.versions.values().next_back().cloned(),
                }
            })
            .await
            .ok_or_else(|| flow_journal::Error::NotFound("workflow version not found".into()))?;
        let definition = self
            .cached_definition(
                &format!("effective-def:{}", selected.checksum),
                &selected.definition,
            )
            .await?;
        let mut view = effective_permissions(&definition, None, node_id);
        view["workflow_id"] = json!(workflow_id);
        view["version"] = json!(selected.version);
        Ok(view)
    }
}

/// 收件箱条目：卡片字段逐字来自持久请求。
fn approval_item(run: &Run, node_id: &str, approval: &ApprovalState) -> Value {
    let status = if run.terminal() && approval.status == ApprovalStatus::Pending {
        // 取消/终态后仍挂着的询问：派生为 cancelled（不改事实）。
        "cancelled"
    } else {
        approval.status.as_str()
    };
    let preview = match &approval.operation_request {
        Some(StoredValue::Inline(value)) => {
            let bytes = serde_json::to_vec(value)
                .map(|b| b.len())
                .unwrap_or(usize::MAX);
            if bytes <= PREVIEW_MAX_BYTES {
                value.clone()
            } else {
                json!({"truncated": true, "bytes": bytes})
            }
        }
        Some(StoredValue::Ref(reference)) => {
            json!({"truncated": true, "bytes": reference.total_bytes, "output_id": reference.output_id})
        }
        None => Value::Null,
    };
    json!({
        "run_id": run.run_id,
        "workflow_id": run.workflow_id,
        "workflow_version": run.workflow_version,
        "node_id": node_id,
        "gate": approval.gate,
        "status": status,
        "operation_id": approval.request.operation_id,
        "request_hash": approval.request.request_hash,
        "actor": approval.request.actor,
        "specs": approval.request.specs,
        "context": approval.request.context,
        "policy_digest": approval.request.policy_digest,
        "matched": approval.matched,
        "asked_at": approval.asked_at,
        "deadline": approval.deadline,
        "decision": approval.decision,
        "request_preview": preview,
    })
}

/// 裁决决策（命令锁内）：只接受当前 pending 实例；Allow 先重新评估。
fn decide(
    state: &State,
    command: &ApprovalCommand<'_>,
) -> flow_journal::Result<(Vec<Event>, Value)> {
    let run = state
        .runs
        .get(command.run_id)
        .ok_or_else(|| flow_journal::Error::NotFound("run not found".into()))?;
    let node = run
        .nodes
        .get(command.node_id)
        .ok_or_else(|| flow_journal::Error::NotFound("node not found".into()))?;
    let approval = node
        .approval
        .as_ref()
        .ok_or_else(|| conflict("node has no permission request"))?;
    if approval.request.operation_id != command.operation_id {
        return Err(conflict("not the current request instance"));
    }
    if approval.request.request_hash != command.request_hash {
        // 内容或派发变了：旧卡片不能批准新请求（I32）。
        return Err(conflict(
            "request hash mismatch: the request changed; refresh the approval card",
        ));
    }
    let wanted = if command.allow {
        ApprovalStatus::Allowed
    } else {
        ApprovalStatus::Denied
    };
    match approval.status {
        ApprovalStatus::Pending => {}
        // 同 hash 重复提交：返回既有结论（幂等）；相反结论 → 已被决定。
        status if status == wanted => {
            return Ok((
                vec![],
                json!({"decision": status.as_str(), "already_decided": true}),
            ));
        }
        status => {
            return Err(conflict(format!("request already {}", status.as_str())));
        }
    }
    if run.terminal() {
        return Err(conflict("run is terminal"));
    }
    if node.dispatch_id != approval.dispatch_id {
        return Err(conflict("request superseded by a new dispatch"));
    }
    let now = chrono::Utc::now();
    if now.timestamp_millis() >= approval.deadline {
        // 截止后由 TTL 路径写 System Deny；人工不能再裁决过期请求（I34）。
        return Err(forbidden("approval expired"));
    }
    let policy = run
        .policy
        .as_ref()
        .ok_or_else(|| forbidden("run has no frozen policy"))?;
    let reason = command.reason.map(str::to_owned);
    // 同事务多条 run 事实：run_seq 逐条 +1（折叠层要求严格连续）。
    let mut seq = run.last_run_seq;
    let node_event = |seq: &mut u64, kind: EventKind, payload: Value| {
        *seq += 1;
        let mut event = Event::new(kind, payload);
        event.run_id = Some(run.run_id.clone());
        event.node_id = Some(command.node_id.to_string());
        event.dispatch_id = Some(node.dispatch_id.clone());
        event.run_seq = *seq;
        event
    };
    let record =
        |decision: Decision, matched: Vec<String>, reason: Option<String>| DecisionRecord {
            operation_id: approval.request.operation_id.clone(),
            request_hash: approval.request.request_hash.clone(),
            evaluated_at: now,
            policy_revision: format!("{}:{}", policy.digest, run.approval_seq),
            decision,
            by: command.by.clone(),
            matched_capabilities: matched,
            modifiers: Modifiers::default(),
            scope: command.scope,
            reason,
        };
    let fingerprint = approval.request.context.request_fingerprint.clone();
    if !command.allow {
        let event = node_event(
            &mut seq,
            EventKind::PermissionDecided,
            serde_json::to_value(PermissionDecidedPayload {
                gate: approval.gate.clone(),
                record: record(Decision::Deny, approval.matched.clone(), reason),
                request_fingerprint: fingerprint,
                status: ApprovalStatus::Denied,
            })?,
        );
        return Ok((vec![event], json!({"decision": "denied"})));
    }
    // 批准时重新评估（能力期限相对提交时刻求值；过期/撤权直接拒绝，§4）。
    let evaluation = crate::policy::check(
        &crate::policy::CheckInput {
            specs: &approval.request.specs,
            actor: &approval.request.actor,
            node_id: command.node_id,
            request_fingerprint: &fingerprint,
            plugin_id: None,
        },
        policy,
        &run.approvals,
        now,
    );
    let matched = match evaluation {
        crate::policy::Evaluation::Deny { reason: denied, .. } => {
            let event = node_event(
                &mut seq,
                EventKind::PermissionDecided,
                serde_json::to_value(PermissionDecidedPayload {
                    gate: approval.gate.clone(),
                    record: record(
                        Decision::Deny,
                        vec![],
                        Some(format!("re-evaluation at approval denied: {denied}")),
                    ),
                    request_fingerprint: fingerprint,
                    status: ApprovalStatus::Denied,
                })?,
            );
            return Ok((vec![event], json!({"decision": "denied", "reason": denied})));
        }
        crate::policy::Evaluation::Allow { matched }
        | crate::policy::Evaluation::Ask { matched } => matched,
    };
    let mut events = vec![node_event(
        &mut seq,
        EventKind::PermissionDecided,
        serde_json::to_value(PermissionDecidedPayload {
            gate: approval.gate.clone(),
            record: record(Decision::Allow, matched, reason),
            request_fingerprint: fingerprint.clone(),
            status: ApprovalStatus::Allowed,
        })?,
    )];
    if command.scope == ApprovalScope::Run {
        // Run 授权：同事务登记，只免除同 run/node 下精确操作的再次询问。
        seq += 1;
        let mut upsert = Event::new(
            EventKind::RunApprovalUpserted,
            serde_json::to_value(RunApproval {
                node_id: command.node_id.to_string(),
                actor: approval.request.actor.clone(),
                digest: flow_dto::permission::run_approval_digest(
                    &approval.request.actor,
                    &approval.request.specs,
                    &fingerprint,
                    &policy.digest,
                ),
                expires_at: None,
                by: command.by.clone(),
            })?,
        );
        upsert.run_id = Some(run.run_id.clone());
        upsert.run_seq = seq;
        events.push(upsert);
    }
    if approval.gate == "node" {
        // G3：批准与授权同事务（C3 + I26），执行器在原 operation 身份下继续。
        let request = approval
            .operation_request
            .clone()
            .ok_or_else(|| conflict("node-level request body missing"))?;
        let operation = serde_json::to_value(Operation {
            operation_id: approval.request.operation_id.clone(),
            fingerprint,
            permit_id: crate::journal_commands::id(),
            request,
            outcome: None,
        })?;
        events.push(node_event(
            &mut seq,
            EventKind::OperationIntent,
            json!({"operation": operation}),
        ));
        events.push(node_event(
            &mut seq,
            EventKind::OperationAuthorized,
            json!({"operation": operation}),
        ));
    }
    Ok((
        events,
        json!({"decision": "allowed", "scope": command.scope}),
    ))
}

/// 有效权限（`permissions.effective`）：设计期按指定定义版本、运行期按冻结
/// 快照，不混用「当前配置」与「该次运行」（§7.3）。不返回任何 secret 值。
pub fn effective_permissions(
    definition: &flow_engine::Definition,
    policy: Option<&flow_dto::permission::FrozenPolicy>,
    node_id: Option<&str>,
) -> Value {
    let frozen = policy
        .cloned()
        .unwrap_or_else(|| crate::policy::freeze_root(definition, Vec::new()));
    let workflow_caps: Vec<Value> = frozen
        .workflow
        .allow
        .iter()
        .map(flow_dto::permission::Capability::to_json)
        .collect();
    let nodes: Vec<Value> = definition
        .nodes
        .iter()
        .filter(|node| node_id.is_none_or(|id| id == node.id))
        .map(|node| {
            let layer = frozen.nodes.get(&node.id);
            let (mode, caps) = match layer {
                Some(NodePermissions::Restrict { allow }) => (
                    "restrict",
                    allow
                        .iter()
                        .map(|cap| {
                            let mut item = cap.to_json();
                            // 派生状态：同 Action 的 workflow 能力是否存在 / 是否要求询问。
                            let peers: Vec<_> = frozen
                                .workflow
                                .allow
                                .iter()
                                .filter(|w| w.pattern.action() == cap.pattern.action())
                                .collect();
                            let derived = if peers.is_empty() {
                                "not_granted_by_workflow"
                            } else if cap.approval == flow_dto::permission::Approval::Ask
                                || peers
                                    .iter()
                                    .all(|w| w.approval == flow_dto::permission::Approval::Ask)
                            {
                                "requires_approval"
                            } else {
                                "auto"
                            };
                            item["derived"] = json!(derived);
                            item
                        })
                        .collect::<Vec<_>>(),
                ),
                _ => ("inherit", Vec::new()),
            };
            json!({
                "node_id": node.id,
                "node_type": node.node_type,
                "mode": mode,
                "allow": caps,
                "side_effect": node.kind().is_none_or(flow_engine::NodeType::has_side_effect),
            })
        })
        .collect();
    json!({
        "legacy": frozen.legacy,
        "mode": frozen.mode,
        "policy_digest": frozen.digest,
        "workflow": workflow_caps,
        "nodes": nodes,
        "ceilings": frozen.ceilings.iter().map(|c| json!({
            "source": c.source,
            "allow": c.allow.iter().map(flow_dto::permission::Capability::to_json).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "diagnostics": definition.permission_diagnostics(),
        "start_gate": crate::policy::start_gate(definition).err(),
    })
}

impl JournalBackend {
    /// `plugins.list`：已部署插件 + 内容身份 + 授予状态（UI 授予页的
    /// 唯一数据源；未部署的内置插件不出现在这里）。
    pub async fn plugins_view(&self) -> Vec<Value> {
        let plugins = crate::plugin_registry::discover_plugins();
        let grants = self.inspect(|state| state.plugin_grants.clone()).await;
        plugins
            .iter()
            .map(|plugin| {
                let key = flow_engine::journal_state::PluginGrant::key(
                    &plugin.manifest.plugin.id,
                    &plugin.content_hash,
                );
                let exact = grants.get(&key);
                let hash_mismatch = !exact.is_some()
                    && grants
                        .values()
                        .any(|grant| grant.plugin_id == plugin.manifest.plugin.id);
                crate::plugin_registry::plugin_list_entry_with_grant(plugin, exact, hash_mismatch)
            })
            .collect()
    }
}

/// 内置（bundled）插件：随产品分发的受信二进制，启动时自动授予其声明
/// 能力（幂等：同 (id, hash) 已有授予则跳过）。第三方插件必须显式授予。
pub const BUNDLED_PLUGIN_IDS: [&str; 2] = ["harness-agent", "llm"];

impl JournalBackend {
    /// `permissions.grant`：按当前发现/内置的插件内容身份写入授予事实。
    ///
    /// `capability_ids = None` = 全部声明能力；`Some(ids)` = 子集（required
    /// 缺失时警告由 UI 侧提示，服务端按交集记录）。legacy 插件用
    /// `legacy_confirm = true` 记录显式风险确认。
    ///
    /// # Errors
    /// 插件不存在 / 声明非 legacy 却要求 legacy 确认 → `NotFound`/`Invalid`。
    pub async fn plugin_grant(
        &self,
        plugin_id: &str,
        capability_ids: Option<Vec<String>>,
        legacy_confirm: bool,
        request_id: &str,
    ) -> Result<CommandReceipt> {
        let Some(identity) = crate::plugin_registry::bundled_or_discovered(plugin_id) else {
            return Err(
                flow_journal::Error::NotFound(format!("plugin {plugin_id} not deployed")).into(),
            );
        };
        let request = json!({
            "plugin_id": plugin_id,
            "content_hash": identity.content_hash,
            "capability_ids": capability_ids,
            "legacy_confirm": legacy_confirm,
        });
        let scope = format!("permissions.grant:{plugin_id}");
        self.command(&scope, Some(request_id), &request, move |state| {
            let grant = build_grant(&identity, &capability_ids, legacy_confirm, state)?;
            let mut event = Event::new(
                EventKind::PluginGrantUpserted,
                serde_json::to_value(&grant)?,
            );
            event.payload["granted_at"] = json!(crate::journal_commands::now());
            Ok((vec![event], serde_json::to_value(&grant)?))
        })
        .await
    }

    /// 启动期内置插件自动授予（幂等；不覆盖已有人工授予）。
    pub async fn ensure_bundled_grants(&self) {
        for plugin_id in BUNDLED_PLUGIN_IDS {
            let Some(identity) = crate::plugin_registry::bundled_or_discovered(plugin_id) else {
                continue;
            };
            let key =
                flow_engine::journal_state::PluginGrant::key(plugin_id, &identity.content_hash);
            let already = self
                .inspect(|state| state.plugin_grants.contains_key(&key))
                .await;
            if already {
                continue;
            }
            let request_id = format!("autogrant-{key}");
            match self
                .plugin_grant(plugin_id, None, identity.legacy, &request_id)
                .await
            {
                Ok(_) => tracing::info!(plugin = plugin_id, hash = %identity.content_hash,
                    "内置插件能力已自动授予（升级后哈希变化需重新授予）"),
                Err(error) => tracing::warn!(plugin = plugin_id, %error,
                    "内置插件自动授予失败；权限工作流中该插件将被拒绝"),
            }
        }
    }
}

/// 组装授予记录（声明 ∩ 请求子集）。
fn build_grant(
    identity: &crate::plugin_registry::PluginIdentity,
    capability_ids: &Option<Vec<String>>,
    legacy_confirm: bool,
    state: &State,
) -> flow_journal::Result<flow_engine::journal_state::PluginGrant> {
    if identity.legacy {
        if !legacy_confirm {
            return Err(flow_journal::Error::Invalid(format!(
                "插件 {} 未声明 [permissions]（legacy-unrestricted）：需显式 legacy_confirm",
                identity.plugin_id
            )));
        }
        return Ok(flow_engine::journal_state::PluginGrant {
            plugin_id: identity.plugin_id.clone(),
            content_hash: identity.content_hash.clone(),
            capabilities: Vec::new(),
            legacy_confirmed: true,
            granted_at: String::new(),
            by: Principal::LocalAdmin,
        });
    }
    if legacy_confirm {
        return Err(flow_journal::Error::Invalid(format!(
            "插件 {} 已声明 [permissions]：按能力列表授予，不需要 legacy 确认",
            identity.plugin_id
        )));
    }
    let declared = identity.capabilities.clone().unwrap_or_default();
    let selected: Vec<_> = match capability_ids {
        None => declared,
        Some(ids) => declared
            .into_iter()
            .filter(|cap| ids.contains(&cap.capability.id))
            .collect(),
    };
    let _ = state;
    // 授予层能力一律记为 auto：授予本身就是上限已批准，是否询问由
    // workflow/node 层决定（层间 AND：任一层 Ask 都会询问）。
    let capabilities = selected
        .into_iter()
        .map(|entry| {
            let mut cap = entry.capability;
            cap.approval = flow_dto::permission::Approval::Auto;
            cap
        })
        .collect();
    Ok(flow_engine::journal_state::PluginGrant {
        plugin_id: identity.plugin_id.clone(),
        content_hash: identity.content_hash.clone(),
        capabilities,
        legacy_confirmed: false,
        granted_at: String::new(),
        by: Principal::LocalAdmin,
    })
}
