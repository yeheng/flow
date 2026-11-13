//! 09 权限模型 P0 端到端：节点级 Allow/Ask/Deny、审批命令、TTL、重启恢复、
//! legacy 静态门、Plan、子工作流上限与挂起不占槽。
//!
//! 契约来源：docs/design/09-permission-model.md §4（决策算法）、§5（G2/G3）、
//! §6（审批流）、§12.1（I26–I34）。全部用本地 stub 与真实执行器进程。

use flow_e2e::e2e_test;
use flow_test_support::fixtures::{governed, http_def, http_def_full, timeline_node, StubHttp};
/// Net 能力条目（测试本地形状）。
fn net_cap(id: &str, method: &str, prefix: &str, approval: &str) -> Value {
    json!({"id": id, "action": "net", "methods": [method], "url_prefix": prefix,
           "approval": approval})
}

/// Secret 能力条目。
fn secret_cap(id: &str, name: &str, approval: &str) -> Value {
    json!({"id": id, "action": "secret", "names": [name], "approval": approval})
}
use flow_test_support::{
    approvals, approve, call_err, call_json, deny, publish_legacy_workflow, run_case_with_env,
    start_run, wait_pending_approval, wait_run_terminal, Ctx, TIMEOUT,
};
use serde_json::{json, Value};

/// start → http_call → end，带显式 permissions（复用 http_def 的节点形状）。
fn governed_http(permissions: Value) -> Value {
    let mut definition = http_def("POST", "https://placeholder.invalid/x");
    definition["semantics"] = json!(2);
    definition["permissions"] = permissions;
    definition
}

fn permissions(mode: &str, allow: Vec<Value>) -> Value {
    json!({"version": 1, "mode": mode, "allow": allow})
}

/// 取 run 的权限事件（kind 前缀过滤）。
async fn permission_events(client: &flow_test_support::Conn, run_id: &str) -> Vec<Value> {
    let events: Value = call_json(client, "run.events.view", json!({"run_id": run_id})).await;
    events["events"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|event| {
            event["event"]["kind"]
                .as_str()
                .is_some_and(|kind| kind.starts_with("permission") || kind == "policy_frozen")
        })
        .collect()
}

e2e_test!(
    auto_capability_executes_with_system_decision_recorded,
    |ctx: &mut Ctx| Box::pin(async move {
        let stub = StubHttp::fixed(200, json!({"ok": true})).await;
        let definition = governed_http(permissions(
            "execute",
            vec![net_cap("w-net", "POST", &stub.url("/x"), "auto")],
        ));
        // http_def 里 url 是 placeholder：换成 stub。
        let definition = rewrite_url(definition, &stub.url("/x"));
        let client = ctx.client().await;
        let (workflow_id, _) = publish_legacy_workflow(&client, "auto 执行", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
        assert_eq!(stub.requests().len(), 1);
        // auto 不是审计豁免：System 决策 + PolicyFrozen 都在（I26/I30）。
        let events = permission_events(&client, &run_id).await;
        let kinds: Vec<&str> = events
            .iter()
            .filter_map(|e| e["event"]["kind"].as_str())
            .collect();
        assert!(kinds.contains(&"policy_frozen"), "{events:?}");
        assert!(kinds.contains(&"permission_decided"), "{events:?}");
        let decided = events
            .iter()
            .find(|e| e["event"]["kind"] == json!("permission_decided"))
            .unwrap();
        assert_eq!(
            decided["event"]["payload"]["record"]["decision"],
            json!("allow")
        );
        assert_eq!(
            decided["event"]["payload"]["record"]["by"],
            json!({"kind": "system"})
        );
        assert_eq!(
            decided["event"]["payload"]["record"]["matched_capabilities"],
            json!(["workflow:w-net"])
        );
    })
);

e2e_test!(
    ask_suspends_until_approved_then_executes,
    |ctx: &mut Ctx| Box::pin(async move {
        let stub = StubHttp::fixed(200, json!({"ok": true})).await;
        let url = stub.url("/x");
        let definition = governed_http(permissions(
            "execute",
            vec![net_cap("w-net", "POST", &url, "ask")],
        ));
        let definition = rewrite_url(definition, &url);
        let client = ctx.client().await;
        let (workflow_id, _) = publish_legacy_workflow(&client, "ask 等待", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        // 挂起等待：run 仍是 running（业务等待不是平台故障），stub 零请求。
        let pending = wait_pending_approval(&client, &run_id, "call", TIMEOUT).await;
        assert_eq!(pending["gate"], json!("node"));
        assert_eq!(pending["specs"][0]["action"], json!("net"));
        assert!(pending["deadline"].as_i64().unwrap() > 0);
        let run = call_json(&client, "run.get.view", json!({"run_id": run_id})).await;
        assert_eq!(run["run"]["status"], json!("running"), "{run}");
        assert!(stub.requests().is_empty());
        // 批准 → 原请求继续执行（同 operation）。
        approve(
            &client,
            &run_id,
            "call",
            pending["operation_id"].as_str().unwrap(),
            pending["request_hash"].as_str().unwrap(),
            "once",
        )
        .await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
        assert_eq!(stub.requests().len(), 1);
        // timeline 的审批终态。
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(
            timeline_node(&timeline, "call")["approval"]["status"],
            json!("allowed")
        );
    })
);

e2e_test!(deny_fails_the_node_without_any_request, |ctx: &mut Ctx| {
    Box::pin(async move {
        let stub = StubHttp::fixed(200, json!({"ok": true})).await;
        let url = stub.url("/x");
        let definition = governed_http(permissions(
            "execute",
            vec![net_cap("w-net", "POST", &url, "ask")],
        ));
        let definition = rewrite_url(definition, &url);
        let client = ctx.client().await;
        let (workflow_id, _) = publish_legacy_workflow(&client, "ask 拒绝", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let pending = wait_pending_approval(&client, &run_id, "call", TIMEOUT).await;
        deny(
            &client,
            &run_id,
            "call",
            pending["operation_id"].as_str().unwrap(),
            pending["request_hash"].as_str().unwrap(),
            "误操作，不发布",
        )
        .await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("failed"), "{run}");
        // 零请求：Deny 时操作从未执行。
        assert!(stub.requests().is_empty());
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        assert_eq!(
            timeline_node(&timeline, "call")["approval"]["status"],
            json!("denied")
        );
        assert!(timeline_node(&timeline, "call")["error"]
            .as_str()
            .unwrap_or_default()
            .contains("permission denied"));
    })
});

e2e_test!(
    unmatched_capability_denies_without_asking,
    |ctx: &mut Ctx| Box::pin(async move {
        let stub = StubHttp::fixed(200, json!({"ok": true})).await;
        let url = stub.url("/x");
        // 能力只覆盖别的源站。
        let definition = governed_http(permissions(
            "execute",
            vec![net_cap("w-other", "POST", "https://other.invalid/", "auto")],
        ));
        let definition = rewrite_url(definition, &url);
        let client = ctx.client().await;
        let (workflow_id, _) = publish_legacy_workflow(&client, "未授予即拒绝", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("failed"), "{run}");
        assert!(stub.requests().is_empty());
        // 未授权动作直接 Deny（不进审批）：收件箱为空。
        let inbox = approvals(&client, Some(&run_id)).await;
        assert_eq!(inbox["items"].as_array().map(Vec::len), Some(0), "{inbox}");
    })
);

/// legacy 门用例显式关闭自动治理（其余 e2e 靠它跑存量语义）。
#[tokio::test]
async fn legacy_definition_with_side_effect_nodes_cannot_start() {
    run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[("FLOW_TEST_AUTO_GOVERN", "0")],
        |ctx| {
            Box::pin(async move {
                let stub = StubHttp::fixed(200, json!({"ok": true})).await;
                let definition = http_def("POST", &stub.url("/x"));
                let client = ctx.client().await;
                let (workflow_id, _) =
                    publish_legacy_workflow(&client, "legacy 副作用", definition).await;
                let error = call_err(
                    &client,
                    "run.start",
                    json!({"workflow_id": workflow_id, "input": {}}),
                )
                .await;
                assert_eq!(error.code(), -32020, "{error}");
                assert!(error.message().contains("call"), "{}", error.message());
                // 纯计算 legacy 照常运行。
                let (pure_wf, _) = publish_legacy_workflow(
                    &client,
                    "legacy 纯计算",
                    flow_test_support::fixtures::linear_def("return 1;"),
                )
                .await;
                let run_id = start_run(&client, &pure_wf, json!({})).await;
                let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
                assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
            })
        },
    )
    .await;
}

e2e_test!(plan_mode_denies_mutating_operation, |ctx: &mut Ctx| {
    Box::pin(async move {
        let stub = StubHttp::fixed(200, json!({"ok": true})).await;
        let url = stub.url("/x");
        let definition = governed_http(permissions(
            "plan",
            vec![net_cap("w-net", "POST", &url, "auto")],
        ));
        let definition = rewrite_url(definition, &url);
        let client = ctx.client().await;
        let (workflow_id, _) = publish_legacy_workflow(&client, "plan 只读", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("failed"), "{run}");
        assert!(stub.requests().is_empty());
        let timeline: Value = call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
        let error = timeline_node(&timeline, "call")["error"]
            .as_str()
            .unwrap_or_default();
        assert!(
            error.contains("permission denied") || error.contains("plan"),
            "{error}"
        );
    })
});

e2e_test!(
    approve_is_idempotent_and_rejects_changed_hash,
    |ctx: &mut Ctx| Box::pin(async move {
        let stub = StubHttp::fixed(200, json!({"ok": true})).await;
        let url = stub.url("/x");
        let definition = governed_http(permissions(
            "execute",
            vec![net_cap("w-net", "POST", &url, "ask")],
        ));
        let definition = rewrite_url(definition, &url);
        let client = ctx.client().await;
        let (workflow_id, _) = publish_legacy_workflow(&client, "幂等", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let pending = wait_pending_approval(&client, &run_id, "call", TIMEOUT).await;
        // 换 hash：拒绝（I32）。
        let error = call_err(
            &client,
            "run.approve",
            json!({"run_id": run_id, "node_id": "call",
                   "operation_id": pending["operation_id"],
                   "request_hash": "0".repeat(64), "scope": "once"}),
        )
        .await;
        assert_eq!(error.code(), -32012, "{error}");
        // 正确批准两次：第二次返回既有结论。
        let first = approve(
            &client,
            &run_id,
            "call",
            pending["operation_id"].as_str().unwrap(),
            pending["request_hash"].as_str().unwrap(),
            "once",
        )
        .await;
        assert_eq!(first["decision"], json!("allowed"), "{first}");
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
        // 同 hash 重复提交永远返回既有结论（I32：幂等，不因终态翻脸）。
        let repeated = call_json(
            &client,
            "run.approve",
            json!({"run_id": run_id, "node_id": "call",
                   "operation_id": pending["operation_id"],
                   "request_hash": pending["request_hash"], "scope": "once"}),
        )
        .await;
        assert_eq!(repeated["already_decided"], json!(true), "{repeated}");
    })
);

#[tokio::test]
async fn approval_ttl_expires_into_system_deny() {
    // 服务器以 1s TTL 启动：到期 = System Deny + 节点业务失败（I34）。
    run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[("FLOW_APPROVAL_TTL_MS", "1000")],
        |ctx| {
            Box::pin(async move {
                let stub = StubHttp::fixed(200, json!({"ok": true})).await;
                let url = stub.url("/x");
                let definition = governed_http(permissions(
                    "execute",
                    vec![net_cap("w-net", "POST", &url, "ask")],
                ));
                let definition = rewrite_url(definition, &url);
                let client = ctx.client().await;
                let (workflow_id, _) = publish_legacy_workflow(&client, "TTL", definition).await;
                let run_id = start_run(&client, &workflow_id, json!({})).await;
                let pending = wait_pending_approval(&client, &run_id, "call", TIMEOUT).await;
                assert_eq!(pending["status"], json!("pending"));
                let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
                assert_eq!(run["run"]["status"], json!("failed"), "{run}");
                assert!(stub.requests().is_empty(), "TTL 拒绝 = 零外部请求");
                let timeline: Value =
                    call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
                assert_eq!(
                    timeline_node(&timeline, "call")["approval"]["status"],
                    json!("expired"),
                    "{}",
                    timeline
                );
            })
        },
    )
    .await;
}

e2e_test!(
    restart_redispatches_with_same_operation_and_inherited_deadline,
    |ctx: &mut Ctx| Box::pin(async move {
        let stub = StubHttp::fixed(200, json!({"ok": true})).await;
        let url = stub.url("/x");
        let definition = governed_http(permissions(
            "execute",
            vec![net_cap("w-net", "POST", &url, "ask")],
        ));
        let definition = rewrite_url(definition, &url);
        let client = ctx.client().await;
        let (workflow_id, _) = publish_legacy_workflow(&client, "重启继承", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let first = wait_pending_approval(&client, &run_id, "call", TIMEOUT).await;
        // 主进程重启：执行器会话死亡 → 重派发（沿用原 operation_id）→ 重新询问。
        ctx.restart().await;
        let client = ctx.client().await;
        // 重派发需要重新 spawn 执行器（数百毫秒）：等待**重新询问**出现——
        // 同 operation_id、新 request_hash（dispatch 变化）。
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        let second = loop {
            let list = approvals(&client, Some(&run_id)).await;
            let found = list["items"].as_array().and_then(|items| {
                items.iter().find(|item| {
                    item["node_id"] == json!("call")
                        && item["operation_id"] == first["operation_id"]
                        && item["request_hash"] != first["request_hash"]
                        && item["status"] == json!("pending")
                })
            });
            if let Some(item) = found {
                break item.clone();
            }
            if tokio::time::Instant::now() > deadline {
                panic!("重启后未重新询问（仍见旧条目）：{list}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        };
        assert_eq!(
            second["operation_id"], first["operation_id"],
            "换 dispatch 沿用原 operation 身份（§6.1）"
        );
        // deadline 继承原询问（不得因重启刷新 TTL）。
        assert_eq!(second["deadline"], first["deadline"]);
        // 旧 hash 批准 → 冲突；新 hash 批准 → 执行。
        let error = call_err(
            &client,
            "run.approve",
            json!({"run_id": run_id, "node_id": "call",
                   "operation_id": first["operation_id"],
                   "request_hash": first["request_hash"], "scope": "once"}),
        )
        .await;
        assert_eq!(error.code(), -32012, "{error}");
        approve(
            &client,
            &run_id,
            "call",
            second["operation_id"].as_str().unwrap(),
            second["request_hash"].as_str().unwrap(),
            "once",
        )
        .await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
        assert_eq!(stub.requests().len(), 1, "批准后恰好一次外部请求");
    })
);

e2e_test!(
    run_scope_approval_exempts_the_exact_repeat_only,
    |ctx: &mut Ctx| Box::pin(async move {
        // 同一节点重发同一请求（裁决 retry 后）：scope=run 免再次询问；
        // 内容变化仍要问。
        let stub = StubHttp::fixed(200, json!({"ok": true})).await;
        let url = stub.url("/x");
        let definition = governed_http(permissions(
            "execute",
            vec![net_cap("w-net", "POST", &url, "ask")],
        ));
        let definition = rewrite_url(definition, &url);
        let client = ctx.client().await;
        let (workflow_id, _) = publish_legacy_workflow(&client, "Run 授权", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let pending = wait_pending_approval(&client, &run_id, "call", TIMEOUT).await;
        approve(
            &client,
            &run_id,
            "call",
            pending["operation_id"].as_str().unwrap(),
            pending["request_hash"].as_str().unwrap(),
            "run",
        )
        .await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
        // Run 授权投影已登记，且可撤销。
        let inbox = approvals(&client, Some(&run_id)).await;
        assert_eq!(inbox["items"].as_array().map(Vec::len), Some(0));
        let events: Value = call_json(&client, "run.events.view", json!({"run_id": run_id})).await;
        let upserted = events["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["event"]["kind"] == json!("run_approval_upserted"));
        assert!(upserted, "{events}");
    })
);

e2e_test!(
    secret_requires_a_matching_capability_and_is_never_leaked,
    |ctx: &mut Ctx| Box::pin(async move {
        // workflow 只授予 Net：带 credential.secret_ref 的请求在 Secret 层被拒，
        // 凭据零解析、零请求（I37 的评估侧验收）。
        let stub = StubHttp::fixed(200, json!({"ok": true})).await;
        let url = stub.url("/x");
        let mut definition = http_def_full(json!({
            "method": "POST", "url": url,
            "credential": {"scheme": "bearer", "secret_ref": "P0_TEST_KEY"}
        }));
        definition["semantics"] = json!(2);
        definition["permissions"] = permissions(
            "execute",
            vec![net_cap("w-net", "POST", &stub_root(&url), "auto")],
        );
        let client = ctx.client().await;
        // 密钥真值走 secrets store（与生产同一条交付路径）。
        call_json(
            &client,
            "secrets.set",
            json!({"name": "P0_TEST_KEY", "value": "shhhh"}),
        )
        .await;
        let (workflow_id, _) =
            publish_legacy_workflow(&client, "缺 Secret 能力", definition.clone()).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("failed"), "{run}");
        assert!(stub.requests().is_empty());
        // 补上 Secret 能力后成功（真值经 Permit 通道交付，事件里只有名字）。
        let mut definition = definition.clone();
        definition["permissions"]["allow"]
            .as_array_mut()
            .unwrap()
            .push(secret_cap("w-key", "P0_TEST_KEY", "auto"));
        let updated: Value = call_json(
            &client,
            "workflow.update",
            json!({"workflow_id": workflow_id, "definition": definition}),
        )
        .await;
        call_json(
            &client,
            "workflow.publish",
            json!({"workflow_id": workflow_id, "version": updated["version"]}),
        )
        .await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
        assert_eq!(stub.requests().len(), 1);
        let request = stub.requests()[0].clone();
        assert!(
            request.contains("Bearer shhhh"),
            "凭据真值应经 Permit 交付：{request}"
        );
        let events: Value = call_json(&client, "run.events.view", json!({"run_id": run_id})).await;
        let text = serde_json::to_string(&events).unwrap();
        assert!(!text.contains("shhhh"), "secret 真值不得进入任何事件");
    })
);

e2e_test!(
    parked_approval_does_not_block_the_only_executor_slot,
    |ctx: &mut Ctx| Box::pin(async move {
        // x_max=1：ask 节点挂起后交还槽位，并行 script 分支照常完成。
        let stub = StubHttp::fixed(200, json!({"ok": true})).await;
        let url = stub.url("/x");
        let definition = json!({
            "semantics": 2,
            "permissions": permissions("execute", vec![
                net_cap("w-net", "POST", &url, "ask"),
            ]),
            "nodes": [
                {"id": "start", "type": "start"},
                {"id": "call", "type": "http_call",
                 "params": {"method": "POST", "url": url}},
                {"id": "s0", "type": "script", "params": {"code": "return 42;"}},
                {"id": "end", "type": "end"}
            ],
            "edges": [
                {"from": "start", "to": "call"},
                {"from": "start", "to": "s0"},
                {"from": "call", "to": "end"},
                {"from": "s0", "to": "end"}
            ]
        });
        let client = ctx.client().await;
        let (workflow_id, _) = publish_legacy_workflow(&client, "挂起不占槽", definition).await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let pending = wait_pending_approval(&client, &run_id, "call", TIMEOUT).await;
        // s0 应能完成（等待 http 审批期间不占唯一槽位）。
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        loop {
            let timeline: Value =
                call_json(&client, "run.timeline", json!({"run_id": run_id})).await;
            if timeline_node(&timeline, "s0")["state"] == json!("completed") {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("挂起的审批阻塞了并行分支：{timeline}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        // http 节点仍在等待（收件箱 pending，stub 零请求）。
        assert_eq!(pending["status"], json!("pending"));
        assert!(stub.requests().is_empty());
    })
);

e2e_test!(
    child_policy_is_capped_by_the_parent_node,
    |ctx: &mut Ctx| Box::pin(async move {
        // 子工作流自己授予了更大能力，但父的 sub_workflow 节点 Restrict 只放行
        // 其他源站 → 子的 http 在父上限层被拒。
        let stub_a = StubHttp::fixed(200, json!({"ok": "a"})).await;
        let stub_b = StubHttp::fixed(200, json!({"ok": "b"})).await;
        let client = ctx.client().await;
        let mut child = http_def("POST", &stub_b.url("/x"));
        child["semantics"] = json!(2);
        child["permissions"] = permissions(
            "execute",
            vec![net_cap("c-net", "POST", &stub_b.url("/x"), "auto")],
        );
        let (child_wf, _) = publish_legacy_workflow(&client, "子流程宽", child).await;
        let mut parent = flow_test_support::fixtures::sub_def(&child_wf);
        parent["semantics"] = json!(2);
        parent["permissions"] = permissions(
            "execute",
            vec![net_cap("p-net", "POST", &stub_a.url("/"), "auto")],
        );
        for node in parent["nodes"].as_array_mut().unwrap() {
            if node["type"] == json!("sub_workflow") {
                node["permissions"] = json!({
                    "mode": "restrict",
                    "allow": [net_cap("p-sub", "POST", &stub_a.url("/"), "auto")]
                });
            }
        }
        let (parent_wf, _) = publish_legacy_workflow(&client, "父流程窄", parent).await;
        let run_id = start_run(&client, &parent_wf, json!({})).await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("failed"), "{run}");
        assert!(stub_b.requests().is_empty(), "父上限层拒绝：子不得发请求");
    })
);

e2e_test!(
    permissions_effective_reports_layers_and_diagnostics,
    |ctx: &mut Ctx| Box::pin(async move {
        let stub = StubHttp::fixed(200, json!({"ok": true})).await;
        let url = stub.url("/x");
        let mut definition = governed_http(permissions(
            "execute",
            vec![net_cap("w-net", "POST", "https://other.invalid/", "ask")],
        ));
        definition = rewrite_url(definition, &url);
        // 节点层 auto 覆盖不了 workflow ask。
        definition["nodes"][1]["permissions"] = json!({
            "mode": "restrict",
            "allow": [net_cap("n-net", "POST", &url, "auto")]
        });
        let client = ctx.client().await;
        let (workflow_id, _) = publish_legacy_workflow(&client, "effective", definition).await;
        let view: Value = call_json(
            &client,
            "permissions.effective",
            json!({"workflow_id": workflow_id}),
        )
        .await;
        assert_eq!(view["legacy"], json!(false));
        assert_eq!(view["workflow"][0]["id"], json!("w-net"));
        let node = view["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|n| n["node_id"] == json!("call"))
            .unwrap();
        assert_eq!(node["mode"], json!("restrict"));
        assert_eq!(node["allow"][0]["derived"], json!("requires_approval"));
        let diagnostics = serde_json::to_string(&view["diagnostics"]).unwrap();
        assert!(
            diagnostics.contains("n-net") && diagnostics.contains("仍需审批"),
            "{view}"
        );
    })
);

e2e_test!(workflow_permissions_change_is_audited, |ctx: &mut Ctx| {
    Box::pin(async move {
        let stub = StubHttp::fixed(200, json!({"ok": true})).await;
        let url = stub.url("/x");
        let definition = governed_http(permissions(
            "execute",
            vec![net_cap("w-net", "POST", &url, "auto")],
        ));
        let definition = rewrite_url(definition, &url);
        let client = ctx.client().await;
        let (workflow_id, version) =
            publish_legacy_workflow(&client, "权限变更审计", definition.clone()).await;
        // 改成 ask → 新版本带 WorkflowPermissionsChanged。
        let mut changed = definition.clone();
        changed["permissions"]["allow"][0]["approval"] = json!("ask");
        let updated: Value = call_json(
            &client,
            "workflow.update",
            json!({"workflow_id": workflow_id, "definition": changed}),
        )
        .await;
        assert_eq!(updated["version"].as_i64().unwrap(), version + 1);
        // 新权限生效验证（事件审计在折叠层单测覆盖）。
        call_json(
            &client,
            "workflow.publish",
            json!({"workflow_id": workflow_id, "version": updated["version"]}),
        )
        .await;
        let run_id = start_run(&client, &workflow_id, json!({})).await;
        let pending = wait_pending_approval(&client, &run_id, "call", TIMEOUT).await;
        approve(
            &client,
            &run_id,
            "call",
            pending["operation_id"].as_str().unwrap(),
            pending["request_hash"].as_str().unwrap(),
            "once",
        )
        .await;
        let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
        assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
    })
});

e2e_test!(
    proxy_egress_denied_without_net_capability_for_proxy_origin,
    |ctx: &mut Ctx| {
        Box::pin(async move {
            let target = StubHttp::fixed(200, json!({"ok": true})).await;
            let proxy = StubHttp::fixed(200, json!({"via": "proxy"})).await;
            // 只授目标源站：代理 origin 无能力 → Deny；目标与代理都零请求
            //（字节实际去向是代理主机，代理出站必须单独授权）。
            let definition = governed(
                &http_def_full(json!({
                    "method": "GET",
                    "url": target.url("/x"),
                    "proxy": proxy.url(""),
                })),
                json!([net_cap("w-net", "GET", &stub_root(&target.url("/x")), "auto")]),
            );
            let client = ctx.client().await;
            let (workflow_id, _) = publish_legacy_workflow(&client, "proxy 无能力", definition).await;
            let run_id = start_run(&client, &workflow_id, json!({})).await;
            let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
            assert_eq!(run["run"]["status"], json!("failed"), "{run}");
            assert!(
                run["run"]["error"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("permission denied"),
                "{run}"
            );
            assert!(target.requests().is_empty(), "目标零请求");
            assert!(proxy.requests().is_empty(), "代理零请求：Deny 先于任何出站");
            // 补上代理能力后同一形状成功（正向路径另由 http_calls 的显式代理
            // 用例覆盖，这里补齐最小对照）。
            let both = governed(
                &http_def_full(json!({
                    "method": "GET",
                    "url": target.url("/x"),
                    "proxy": proxy.url(""),
                })),
                json!([
                    net_cap("w-net", "GET", &stub_root(&target.url("/x")), "auto"),
                    net_cap("w-proxy", "GET", &stub_root(&proxy.url("")), "auto"),
                ]),
            );
            let (workflow_id, _) = publish_legacy_workflow(&client, "proxy 双能力", both).await;
            let run_id = start_run(&client, &workflow_id, json!({})).await;
            let run = wait_run_terminal(&client, &run_id, TIMEOUT).await;
            assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
            assert_eq!(proxy.requests().len(), 1, "代理收到一次请求");
        })
    }
);

/// 把 http_def 里的 placeholder URL 替换成真实 stub URL（保参数形状不变）。
fn rewrite_url(mut definition: Value, url: &str) -> Value {
    for node in definition["nodes"].as_array_mut().unwrap() {
        if node["type"] == json!("http_call") {
            node["params"]["url"] = json!(url);
        }
    }
    definition
}

/// `http://host:port/path` → `http://host:port/`（net 能力的源站前缀）。
fn stub_root(url: &str) -> String {
    let split = url.split_once("://").unwrap_or(("", url));
    let authority = split.1.split('/').next().unwrap_or_default();
    format!("{}://{authority}/", split.0)
}
