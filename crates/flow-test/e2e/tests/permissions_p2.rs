//! 09 权限模型 P2 端到端：API key 分档（§9）——审批权与写权分离。
//!
//! approve-only key 能裁决审批，但 workflow.update / permissions.grant 被拒。

use flow_test_support::fixtures::StubHttp;
use flow_test_support::permissions::sha256_hex;

/// Net 能力条目。
fn net_cap(id: &str, method: &str, prefix: &str, approval: &str) -> Value {
    json!({"id": id, "action": "net", "methods": [method], "url_prefix": prefix,
           "approval": approval})
}
use flow_test_support::{
    approve, call_err, call_json, publish_legacy_workflow, start_run, wait_pending_approval,
    TIMEOUT,
};
use serde_json::{json, Value};

/// 受管 ask 定义（http 节点 + ask Net 能力）。
fn governed_ask(url: &str) -> Value {
    json!({
        "semantics": 2,
        "permissions": {"version": 1, "mode": "execute", "allow": [
            net_cap("w-net", "POST", url, "ask"),
        ]},
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "call", "type": "http_call",
             "params": {"method": "POST", "url": url}},
            {"id": "e", "type": "end"}
        ],
        "edges": [
            {"from": "s", "to": "call"}, {"from": "call", "to": "e"}
        ]
    })
}

#[tokio::test]
async fn approve_only_key_cannot_write_workflows_but_can_approve() {
    // 明文 key 只在测试里可见；配置存 sha256。
    let approve_key_plain = "approve-only-key-0123456789abcdef";
    let approve_key_hash = sha256_hex(approve_key_plain.as_bytes());
    let config_dir = tempfile::tempdir().unwrap();
    let config_path = config_dir.path().join("flow.config.toml");
    std::fs::write(
        &config_path,
        format!(
            "[[api_keys]]\nid = \"approver\"\ntoken_hash = \"{approve_key_hash}\"\nscopes = [\"read\", \"approve\"]\n"
        ),
    )
    .unwrap();
    flow_test_support::run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[("FLOW_CONFIG", &config_path.display().to_string())],
        |ctx| {
            Box::pin(async move {
                let admin = ctx.client().await;
                let stub = StubHttp::fixed(200, json!({"ok": true})).await;
                let url = stub.url("/x");
                let definition = governed_ask(&url);
                let (workflow_id, _) =
                    publish_legacy_workflow(&admin, "P2 分档", definition.clone()).await;
                let run_id = start_run(&admin, &workflow_id, json!({})).await;
                let pending = wait_pending_approval(&admin, &run_id, "call", TIMEOUT).await;

                // approve-only key 走独立连接（同 WS 服务，_token 换 key）。
                let approver = flow_test_support::connect(ctx.addr(), approve_key_plain).await;
                // 读面放行（read scope）。
                let inbox: Value =
                    call_json(&approver, "approvals.list", json!({"run_id": run_id})).await;
                assert_eq!(inbox["items"].as_array().map(Vec::len), Some(1), "{inbox}");
                // 写 workflow：拒绝（缺 write scope）。
                let error = call_err(
                    &approver,
                    "workflow.update",
                    json!({"workflow_id": workflow_id, "definition": definition,
                           "request_id": "p2-key-1"}),
                )
                .await;
                assert_eq!(error.code(), -32020, "{error}");
                assert!(error.message().contains("approve"), "{}", error.message());
                // 授予插件：拒绝（admin only）。
                let error = call_err(
                    &approver,
                    "permissions.grant",
                    json!({"plugin_id": "llm", "request_id": "p2-key-2"}),
                )
                .await;
                assert_eq!(error.code(), -32020, "{error}");
                // 审批本身：放行，决策记录 by = ApiKey{id}。
                approve(
                    &approver,
                    &run_id,
                    "call",
                    pending["operation_id"].as_str().unwrap(),
                    pending["request_hash"].as_str().unwrap(),
                    "once",
                )
                .await;
                let run = flow_test_support::wait_run_terminal(&admin, &run_id, TIMEOUT).await;
                assert_eq!(run["run"]["status"], json!("succeeded"), "{run}");
                let events: Value =
                    call_json(&admin, "run.events.view", json!({"run_id": run_id})).await;
                let decided = events["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|e| {
                        e["event"]["kind"] == json!("permission_decided")
                            && e["event"]["payload"]["gate"] == json!("node")
                            && e["event"]["payload"]["record"]["decision"] == json!("allow")
                    })
                    .expect("allow decision recorded");
                assert_eq!(
                    decided["event"]["payload"]["record"]["by"],
                    json!({"kind": "api_key", "id": "approver"}),
                    "{}",
                    serde_json::to_string(decided).unwrap()
                );
                // 未知 token 仍是 -32001。
                let stranger =
                    flow_test_support::connect(ctx.addr(), "totally-wrong-token-0000000000000000")
                        .await;
                let error = call_err(&stranger, "approvals.list", json!({"run_id": run_id})).await;
                assert_eq!(error.code(), -32001, "{error}");
            })
        },
    )
    .await;
}

/// 哈希链（09 §10.2）：中段事务的 body+digest 被一致篡改（旧读侧逐帧校验
/// 会被绕过），verify 顺链必断。
#[tokio::test]
async fn hash_chain_detects_consistent_mid_history_tampering() {
    use flow_backend::journal::JournalBackend;
    let dir = tempfile::tempdir().unwrap();
    {
        let backend = JournalBackend::open(dir.path(), Default::default())
            .await
            .unwrap();
        // 三个事务：create + update ×2（直接走命令面，不需要执行器）。
        let created = backend.workflow_create("链", None).await.unwrap();
        let workflow_id = created.result["workflow_id"].as_str().unwrap().to_owned();
        let mut definition = governed_ask("https://tamper.invalid/x");
        for version in 0..2 {
            definition["permissions"]["allow"][0]["url_prefix"] =
                json!(format!("https://tamper.invalid/{version}"));
            backend
                .workflow_update(&workflow_id, definition.clone(), None)
                .await
                .unwrap();
        }
        backend.close().await.unwrap();
    }
    let db = dir.path().join(flow_journal::sqlite::DATABASE);
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        // 中段事务（lsn=2）整体重编码：改 events.body 里的名字并同步
        // transactions.digest/bytes——读侧逐帧复核（body==digest）仍通过，
        // 只有链能发现（§10.2 的目标威胁）。
        let lsn = "00000000000000000002";
        let rows: Vec<(i64, String)> = {
            let mut statement = conn
                .prepare("SELECT event_index, body FROM events WHERE lsn = ?1 ORDER BY event_index")
                .unwrap();
            let mapped = statement
                .query_map(rusqlite::params![lsn], |r| {
                    Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
                })
                .unwrap();
            mapped.collect::<Result<_, _>>().unwrap()
        };
        let count = rows.len();
        for (index, body) in rows {
            let tampered = body.replace("tamper.invalid", "tamped.invalid");
            conn.execute(
                "UPDATE events SET body = ?1 WHERE lsn = ?2 AND event_index = ?3",
                rusqlite::params![tampered, lsn, index],
            )
            .unwrap();
        }
        // 重构 transactions.digest：读回路径会重编码 body 计算——我们直接用
        // 读回器自己的编码器保持逐帧一致。
        let identity: String = conn
            .query_row(
                "SELECT journal_id FROM store_meta WHERE singleton = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        // 直接从 events 行重组 Transaction（读回器会先校验 digest，先改后读
        // 有鸡生蛋问题；这里手工构造以重编码出「篡改后的正确摘要」）。
        let tx_id: String = conn
            .query_row(
                "SELECT tx_id FROM transactions WHERE lsn = ?1",
                rusqlite::params![lsn],
                |r| r.get(0),
            )
            .unwrap();
        let events: Vec<flow_journal::Event> = {
            let mut statement = conn
                .prepare("SELECT body FROM events WHERE lsn = ?1 ORDER BY event_index")
                .unwrap();
            let mapped = statement
                .query_map(rusqlite::params![lsn], |r| r.get::<_, String>(0))
                .unwrap();
            mapped
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .into_iter()
                .map(|body| serde_json::from_str::<flow_journal::Event>(&body).unwrap())
                .collect()
        };
        let tx = flow_journal::Transaction {
            v: 2,
            journal_id: identity.clone(),
            lsn: 2,
            tx_id,
            events,
        };
        let body_bytes =
            flow_journal::codec::bounded_json(&tx, flow_journal::MAX_LINE_BYTES).unwrap();
        let digest = flow_journal::codec::digest(&body_bytes);
        conn.execute(
            "UPDATE transactions SET digest = ?1, bytes = ?2, event_count = ?3 WHERE lsn = ?4",
            rusqlite::params![digest, body_bytes.len() as i64, count as i64, lsn],
        )
        .unwrap();
        let _ = conn;
    }
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        let mut statement = conn
            .prepare("SELECT lsn, prev_digest, digest, chain_digest FROM transactions ORDER BY lsn")
            .unwrap();
        let rows = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })
            .unwrap();
        for row in rows.flatten() {
            eprintln!(
                "row {:?} prev={:.8} digest={:.8} chain={:.8}",
                row.0, row.1, row.2, row.3
            );
        }
    }
    let report = flow_journal::maintenance::verify(dir.path());
    assert!(report.is_err(), "中段一致篡改必须被链发现");
    let message = report.unwrap_err().to_string();
    assert!(message.contains("chain broken"), "{message}");
}

/// permissions.audit.page（09 §10.3）：能回答「谁批了什么」——批准事件的
/// record.by / matched_capabilities 可查，decision 过滤生效。
#[tokio::test]
async fn permissions_audit_page_answers_who_approved_what() {
    flow_test_support::run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[("FLOW_TEST_AUTO_GOVERN", "0")],
        |ctx| {
            Box::pin(async move {
                let client = ctx.client().await;
                let stub = StubHttp::fixed(200, json!({"ok": true})).await;
                let url = stub.url("/x");
                let definition = governed_ask(&url);
                let (workflow_id, _) =
                    publish_legacy_workflow(&client, "P2 审计", definition).await;
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
                let _ = flow_test_support::wait_run_terminal(&client, &run_id, TIMEOUT).await;

                let page: Value = flow_test_support::call_json(
                    &client,
                    "permissions.audit.page",
                    json!({"run_id": run_id, "decision": "allow"}),
                )
                .await;
                let text = serde_json::to_string(&page).unwrap();
                assert!(
                    text.contains("\"kind\":\"local_admin\""),
                    "决策记录应含 by=local_admin：{text}"
                );
                assert!(text.contains("w-net"), "应含命中的能力项：{text}");
                // deny 过滤：无 deny 记录 → 空。
                let deny_page: Value = flow_test_support::call_json(
                    &client,
                    "permissions.audit.page",
                    json!({"run_id": run_id, "decision": "deny"}),
                )
                .await;
                assert_eq!(
                    deny_page["events"].as_array().map(Vec::len),
                    Some(0),
                    "{deny_page}"
                );
                // run.audit.page 的 filter=permission 模式同样可用。
                let audit: Value = flow_test_support::call_json(
                    &client,
                    "run.audit.page",
                    json!({"run_id": run_id, "filter": "permission", "limit": 50}),
                )
                .await;
                let audit_text = serde_json::to_string(&audit).unwrap();
                assert!(audit_text.contains("permission_decided"), "{audit_text}");
            })
        },
    )
    .await;
}

/// P2 权限变更鉴权（09 §9：权限块变更仅 admin）：write-scope key 能改
/// 定义但不能改权限块（-32020）；admin key 可改；WorkflowPermissionsChanged
/// 的 by 记录真实主体（ApiKey{id}），不再写死 LocalAdmin。
#[tokio::test]
async fn permission_block_change_requires_admin_and_records_actor() {
    fn sha256_hex(bytes: &[u8]) -> String {
        use sha2::Digest;
        let digest = sha2::Sha256::digest(bytes);
        digest.iter().map(|b| format!("{b:02x}")).collect()
    }
    let writer_plain = "writer-key-0123456789abcdef";
    let admin_plain = "admin-key-0123456789abcdef";
    let config_dir = tempfile::tempdir().unwrap();
    let config_path = config_dir.path().join("flow.config.toml");
    std::fs::write(
        &config_path,
        format!(
            "[[api_keys]]\nid = \"writer\"\ntoken_hash = \"{}\"\nscopes = [\"read\", \"write\"]\n\
             [[api_keys]]\nid = \"boss\"\ntoken_hash = \"{}\"\nscopes = [\"admin\"]\n",
            sha256_hex(writer_plain.as_bytes()),
            sha256_hex(admin_plain.as_bytes()),
        ),
    )
    .unwrap();
    flow_test_support::run_case_with_env(
        env!("CARGO_BIN_EXE_flow-journal-server-e2e"),
        &[("FLOW_CONFIG", &config_path.display().to_string())],
        |ctx: &mut flow_test_support::Ctx| {
            Box::pin(async move {
                let admin = ctx.client().await;
                let writer = flow_test_support::connect(ctx.addr(), writer_plain).await;

                // admin 建一个受管工作流（带权限块）。
                let wf: Value = flow_test_support::call_json(
                    &admin,
                    "workflow.create",
                    json!({"name": "perm-change-gate"}),
                )
                .await;
                let workflow_id = wf["workflow_id"].as_str().unwrap().to_owned();
                let governed = || json!({
                    "semantics": 2,
                    "permissions": {"version": 1, "mode": "execute", "allow": [
                        net_cap("w-net", "POST", "https://api.example.com/", "auto"),
                    ]},
                    "nodes": [
                        {"id": "s", "type": "start"},
                        {"id": "c", "type": "http_call",
                         "params": {"method": "POST", "url": "https://api.example.com/x"}},
                        {"id": "e", "type": "end"}
                    ],
                    "edges": [{"from": "s", "to": "c"}, {"from": "c", "to": "e"}]
                });
                let saved: Value = flow_test_support::call_json(
                    &admin,
                    "workflow.update",
                    json!({"workflow_id": workflow_id, "definition": governed()}),
                )
                .await;
                let version = saved["version"].as_u64().unwrap_or(1);

                // write key：保存权限不变的新版本 → 允许（改动 params）。
                let mut unchanged = governed();
                unchanged["nodes"][1]["params"]["url"] = json!("https://api.example.com/y");
                let ok: Value = flow_test_support::call_json(
                    &writer,
                    "workflow.update",
                    json!({"workflow_id": workflow_id, "definition": unchanged}),
                )
                .await;
                assert!(ok["version"].as_u64().is_some(), "write key 可改非权限内容：{ok}");

                // write key：改权限块（auto → ask）→ -32020。

                let mut loosened = governed();
                loosened["permissions"]["allow"][0]["approval"] = json!("ask");
                let error = flow_test_support::call_err(
                    &writer,
                    "workflow.update",
                    json!({"workflow_id": workflow_id, "definition": loosened}),
                )
                .await;
                assert_eq!(error.code(), -32020, "非 admin 改权限块必须拒绝：{error}");
                assert!(
                    error.message().contains("admin"),
                    "拒绝原因点名 admin：{error}"
                );

                // admin key：改权限块（writer 被拒的同一份 loosened）→ 允许；
                // 事件 by = ApiKey{boss}（不再写死 LocalAdmin）。
                let mut loosened = governed();
                loosened["permissions"]["allow"][0]["approval"] = json!("ask");
                let changed: Value = flow_test_support::call_json(
                    &flow_test_support::connect(ctx.addr(), admin_plain).await,
                    "workflow.update",
                    json!({"workflow_id": workflow_id, "definition": loosened}),
                )
                .await;
                assert!(changed["version"].as_u64().is_some(), "{changed}");
                let events: Value = flow_test_support::call_json(
                    &admin,
                    "permissions.audit.page",
                    json!({"limit": 100}),
                )
                .await;
                let changed_events: Vec<&Value> = events["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|e| e["event"]["kind"] == json!("workflow_permissions_changed"))
                    .collect();
                assert!(
                    changed_events.iter().any(|e| {
                        e["event"]["payload"]["by"] == json!({"kind": "api_key", "id": "boss"})
                    }),
                    "by 必须记录真实主体（ApiKey{{boss}}）：{events:?}（初始版本 v{version} 由 local_admin 建立）"
                );
            })
        },
    )
    .await;
}
