//! 托管运行的纯权限评估器（G3/G4/G5 共用，§4）。
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

use chrono::{DateTime, Utc};
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
    if policy.mode == ExecutionMode::Bypass {
        return Evaluation::Allow {
            matched: vec!["workflow:bypass".into()],
        };
    }
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
    if policy.mode == ExecutionMode::Bypass {
        return Evaluation::Allow {
            matched: vec!["workflow:bypass".into()],
        };
    }
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

    #[test]
    fn bypass_allows_operations_and_plugins_without_grants_or_node_capabilities() {
        let policy = frozen(
            ExecutionMode::Bypass,
            vec![],
            restrict(vec![]),
            vec![CeilingLayer {
                source: "parent".into(),
                allow: vec![],
            }],
            vec![],
        );
        assert!(matches!(
            check_now(&[http_spec("https://example.com")], &policy, &[]),
            Evaluation::Allow { .. }
        ));
        assert!(matches!(
            check_now(&[], &policy, &[]),
            Evaluation::Allow { .. }
        ));
        assert!(matches!(
            check_needs(&[], &policy, &[]),
            Evaluation::Allow { .. }
        ));
        assert_eq!(ExecutionMode::parse("bypass"), Some(ExecutionMode::Bypass));
        let decoded: WorkflowPermissions =
            serde_json::from_value(serde_json::json!({"version":1,"mode":"bypass","allow":[]}))
                .unwrap();
        assert_eq!(decoded.mode, ExecutionMode::Bypass);
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
}
