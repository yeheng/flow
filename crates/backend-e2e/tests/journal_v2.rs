use flow_backend::journal::JournalBackend;
use jsonrpsee::RpcModule;
use serde_json::{json, Value};

// 用产品 flow-journal-server（而非 e2e 薄壳）：e2e 壳只从环境变量读配置、
// 不解析 --config；本用例验证已移除的配置在打开数据目录前就被拒绝。
#[test]
fn removed_cluster_config_is_rejected_before_opening_the_journal() {
    let dir = flow_test_support::io::TempDir::new("removed-cluster-config");
    let path = dir.path().join("flow.toml");
    std::fs::write(&path, "[journal.cluster]\ncluster_id='test'\nnode_id='a'\n").unwrap();
    let output = std::process::Command::new(flow_test_support::io::server_bin())
        .arg("--config")
        .arg(path)
        .env(
            "FLOW_JOURNAL_TOKEN",
            "removed-config-test-token-at-least-32bytes",
        )
        .env("FLOW_JOURNAL_DATA_DIR", dir.path().join("journal"))
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknown field `cluster`"));
    assert!(!dir.path().join("journal").exists());
}

#[tokio::test]
async fn removed_cluster_rpc_methods_return_method_not_found() {
    let dir = flow_test_support::io::TempDir::new("removed-cluster-rpc");
    let backend = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let token = "removed-method-test-token-at-least-32bytes";
    let module = flow_rpc::journal_v2::module(backend.clone(), token.into()).unwrap();
    for method in [
        "journal.cluster.status",
        "journal.cluster.nodes",
        "journal.cluster.remove",
    ] {
        let response = call(&module, method, json!({"_token":token})).await;
        assert_eq!(response["error"]["code"], -32601, "{method}: {response}");
    }
    backend.close().await.unwrap();
}

#[tokio::test]
async fn schedule_reactivation_and_legacy_creation_boundaries_are_durable() {
    let dir = flow_test_support::io::TempDir::new("schedule-reactivation");
    let backend = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let receipt = backend.workflow_create("reactivation", None).await.unwrap();
    let workflow = receipt.result["workflow_id"].as_str().unwrap();
    backend.workflow_update(workflow, json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}), None).await.unwrap();
    backend.workflow_publish(workflow, 1, None).await.unwrap();
    // Replay a legacy configuration with no active_since field.
    let created = chrono::Utc::now();
    backend.append(vec![flow_journal::Event::new(flow_journal::EventKind::ScheduleChanged,
        json!({"id":"legacy","workflow_id":workflow,"enabled":true,"cron_expr":"* * * * *","input":null,"created_at":created.to_rfc3339()}))]).await.unwrap();
    flow_rpc::journal_triggers::fire_due(&backend, created.with_timezone(&chrono::Local)).await;
    assert!(backend.state().await.unwrap().runs.is_empty());
    backend
        .config_change(
            "schedule.change",
            flow_journal::EventKind::ScheduleChanged,
            "legacy",
            json!({"enabled":false}),
            None,
        )
        .await
        .unwrap();
    backend
        .config_change(
            "schedule.change",
            flow_journal::EventKind::ScheduleChanged,
            "legacy",
            json!({"enabled":true}),
            None,
        )
        .await
        .unwrap();
    let state = backend.state().await.unwrap();
    let since = chrono::DateTime::parse_from_rfc3339(
        state.schedules["legacy"]["active_since"].as_str().unwrap(),
    )
    .unwrap();
    assert!(since >= created);
    let stale_fire = (created - chrono::Duration::minutes(1)).to_rfc3339();
    assert!(
        backend
            .trigger_start("schedule", "legacy", &stale_fire, json!(null))
            .await
            .is_err(),
        "the command decision must reject stale scan results under the state lock"
    );
    let now = chrono::Local::now();
    flow_rpc::journal_triggers::fire_due(&backend, now).await;
    assert!(backend.state().await.unwrap().runs.is_empty());
    backend
        .config_change(
            "schedule.change",
            flow_journal::EventKind::ScheduleChanged,
            "legacy",
            json!({"cron_expr":"*/2 * * * *"}),
            None,
        )
        .await
        .unwrap();
    let state = backend.state().await.unwrap();
    let changed = chrono::DateTime::parse_from_rfc3339(
        state.schedules["legacy"]["active_since"].as_str().unwrap(),
    )
    .unwrap();
    assert!(changed >= since);
    backend.close().await.unwrap();
}

async fn call(
    module: &RpcModule<flow_rpc::journal_v2::Context>,
    method: &str,
    params: Value,
) -> Value {
    let request = json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}).to_string();
    let (response, _) = module.raw_json_request(&request, 1).await.unwrap();
    serde_json::from_str(response.get()).unwrap()
}

#[tokio::test]
async fn v2_subscription_backfills_from_run_sequence_and_closes_after_terminal() {
    let root = std::env::temp_dir().join(format!("flow-v2-subscription-{}", uuid::Uuid::now_v7()));
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let created = backend.workflow_create("subscription", None).await.unwrap();
    let workflow = created.result["workflow_id"].as_str().unwrap();
    backend.workflow_update(workflow,json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}),None).await.unwrap();
    backend.workflow_publish(workflow, 1, None).await.unwrap();
    let receipt = backend
        .run_start(
            workflow,
            None,
            json!({"full":"value"}),
            "manual",
            None,
            None,
        )
        .await
        .unwrap();
    let run_id = receipt.result["run_id"].as_str().unwrap();
    let token = "s".repeat(32);
    let module = flow_rpc::journal_v2::module(backend.clone(), token.clone()).unwrap();
    let request=json!({"jsonrpc":"2.0","id":1,"method":"run.subscribe","params":{"run_id":run_id,"_token":token,"event_format":"v2","from_seq":"1"}}).to_string();
    let (_, mut stream) = module.raw_json_request(&request, 32).await.unwrap();
    start_ipc(&backend, 16).await;
    let mut seqs = Vec::new();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while let Some(message) = stream.recv().await {
            let message: Value = serde_json::from_str(message.get()).unwrap();
            let event = &message["params"]["result"]["event"];
            let seq = event["run_seq"].as_str().unwrap().parse::<u64>().unwrap();
            seqs.push(seq);
            if event["kind"] == "run_completed" {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(seqs.len() > 2);
    assert_eq!(seqs[0], 2);
    assert!(seqs.windows(2).all(|v| v[1] == v[0] + 1));
    drop(stream);
    drop(module);
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn triggers_commit_run_and_dedup_together_and_survive_config_deletion() {
    let root = std::env::temp_dir().join(format!("flow-v2-triggers-{}", uuid::Uuid::now_v7()));
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let created = backend.workflow_create("triggers", None).await.unwrap();
    let workflow = created.result["workflow_id"].as_str().unwrap();
    backend.workflow_update(workflow,json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}),None).await.unwrap();
    backend.workflow_publish(workflow, 1, None).await.unwrap();
    backend.config_change("schedule.change",flow_journal::EventKind::ScheduleChanged,"cron",json!({"workflow_id":workflow,"enabled":true,"cron_expr":"* * * * *","input":{"from":"cron"}}),None).await.unwrap();
    let now = chrono::Local::now() + chrono::Duration::minutes(2);
    tokio::join!(
        flow_rpc::journal_triggers::fire_due(&backend, now),
        flow_rpc::journal_triggers::fire_due(&backend, now)
    );
    assert_eq!(backend.state().await.unwrap().runs.len(), 1);
    backend
        .config_change(
            "webhook.change",
            flow_journal::EventKind::WebhookChanged,
            "hook",
            json!({"workflow_id":workflow,"enabled":true}),
            None,
        )
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        backend.trigger_start("webhook", "hook", "delivery", json!({"n":1})),
        backend.trigger_start("webhook", "hook", "delivery", json!({"n":1}))
    );
    let a = a.unwrap();
    assert_eq!(a.commit_cursor, b.unwrap().commit_cursor);
    backend
        .config_change(
            "webhook.change",
            flow_journal::EventKind::WebhookChanged,
            "hook",
            json!({"deleted":true}),
            None,
        )
        .await
        .unwrap();
    let retry = backend
        .trigger_start("webhook", "hook", "delivery", json!({"n":1}))
        .await
        .unwrap();
    assert_eq!(retry.commit_cursor, a.commit_cursor);
    assert!(backend
        .trigger_start("webhook", "hook", "delivery", json!({"n":2}))
        .await
        .is_err());
    assert!(backend
        .trigger_start("webhook", "hook", "new-delivery", json!({"n":1}))
        .await
        .is_err());
    assert_eq!(backend.state().await.unwrap().runs.len(), 2);
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn schedule_respects_activation_boundary_and_never_fires_early() {
    let dir = flow_test_support::io::TempDir::new("schedule-boundary");
    let backend = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let receipt = backend.workflow_create("boundary", None).await.unwrap();
    let workflow = receipt.result["workflow_id"].as_str().unwrap();
    backend.workflow_update(workflow, json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}), None).await.unwrap();
    backend.workflow_publish(workflow, 1, None).await.unwrap();
    backend
        .config_change(
            "schedule.change",
            flow_journal::EventKind::ScheduleChanged,
            "cron",
            json!({"workflow_id":workflow,"enabled":true,"cron_expr":"* * * * *","input":null}),
            None,
        )
        .await
        .unwrap();
    let now = chrono::Local::now();
    flow_rpc::journal_triggers::fire_due(&backend, now).await;
    assert!(
        backend.state().await.unwrap().runs.is_empty(),
        "creation must not backfill a historical slot"
    );
    let cron: cron_parser::Schedule = "* * * * *".parse().unwrap();
    let fire = cron.next_after(&now).unwrap();
    flow_rpc::journal_triggers::fire_due(&backend, fire - chrono::Duration::milliseconds(500))
        .await;
    assert!(
        backend.state().await.unwrap().runs.is_empty(),
        "must not fire before the scheduled instant"
    );
    tokio::join!(
        flow_rpc::journal_triggers::fire_due(&backend, fire),
        flow_rpc::journal_triggers::fire_due(&backend, fire)
    );
    assert_eq!(backend.state().await.unwrap().runs.len(), 1);
    backend.close().await.unwrap();
    drop(backend);
    let backend = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    flow_rpc::journal_triggers::fire_due(&backend, fire).await;
    assert_eq!(
        backend.state().await.unwrap().runs.len(),
        1,
        "restart must retain the fire identity"
    );
    backend.close().await.unwrap();
}

#[tokio::test]
async fn authenticated_commands_attach_and_are_atomically_visible() {
    let root = std::env::temp_dir().join(format!("flow-v2-rpc-{}", uuid::Uuid::now_v7()));
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let token = "a".repeat(32);
    let module = flow_rpc::journal_v2::module(backend.clone(), token.clone()).unwrap();
    let denied = call(&module, "workflow.create", json!({"name":"test"})).await;
    assert_eq!(denied["error"]["code"], -32001);
    assert!(backend.state().await.unwrap().workflows.is_empty());
    let params = json!({"_token":token,"name":"test","request_id":"same"});
    let first = call(&module, "workflow.create", params.clone()).await;
    let receipt = &first["result"];
    assert_eq!(receipt["visible"], true);
    assert_eq!(receipt["committed"], true);
    let status = call(
        &module,
        "command.status",
        json!({"_token":token,"scope":"workflow.create","request_id":"same"}),
    )
    .await;
    assert_eq!(status["result"]["commit_cursor"], receipt["commit_cursor"]);
    let duplicate = call(&module, "workflow.create", params).await;
    assert_eq!(
        duplicate["result"]["commit_cursor"],
        receipt["commit_cursor"]
    );
    assert_eq!(duplicate["result"]["visible"], true);
    assert_eq!(backend.state().await.unwrap().workflows.len(), 1);
    let denied = call(
        &module,
        "run.audit.page",
        json!({"run_id":"x","_token":"wrong"}),
    )
    .await;
    assert_eq!(denied["error"]["code"], -32001);
    drop(module);
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn value_download_checks_token_and_run_binding_and_streams_original_bytes() {
    let root = std::env::temp_dir().join(format!("flow-v2-download-{}", uuid::Uuid::now_v7()));
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let created = backend.workflow_create("download", None).await.unwrap();
    let workflow = created.result["workflow_id"].as_str().unwrap();
    backend.workflow_update(workflow,json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}),None).await.unwrap();
    backend.workflow_publish(workflow, 1, None).await.unwrap();
    let input = json!({"business_secret":"雪".repeat(60_000)});
    let created = backend
        .run_start(workflow, None, input.clone(), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap();
    let snapshot = backend.state().await.unwrap();
    let flow_journal::StoredValue::Ref(value) = &snapshot.runs[run_id].input else {
        panic!("expected reference")
    };
    let token = "b".repeat(32);
    let router = flow_rpc::journal_download::router(backend.clone(), token.clone()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let url = format!("http://{addr}/runs/{run_id}/values/{}", value.output_id);
    let client = reqwest::Client::new();
    assert_eq!(client.get(&url).send().await.unwrap().status(), 401);
    let response = client.get(&url).bearer_auth(&token).send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["cache-control"], "no-store");
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        serde_json::to_vec(&input).unwrap()
    );
    let wrong = format!("http://{addr}/runs/missing/values/{}", value.output_id);
    assert_eq!(
        client
            .get(wrong)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    server.abort();
    let _ = server.await;
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn slow_subscription_can_resume_by_pages_and_terminal_audit_stays_live() {
    let root = std::env::temp_dir().join(format!("flow-slow-reader-{}", uuid::Uuid::now_v7()));
    let b = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let w = b.workflow_create("slow", None).await.unwrap();
    let w = w.result["workflow_id"].as_str().unwrap();
    b.workflow_update(w,json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}),None).await.unwrap();
    b.workflow_publish(w, 1, None).await.unwrap();
    let r = b
        .run_start(w, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let id = r.result["run_id"].as_str().unwrap();
    let token = "q".repeat(32);
    let module = flow_rpc::journal_v2::module(b.clone(), token.clone()).unwrap();
    let request=json!({"jsonrpc":"2.0","id":1,"method":"run.subscribe","params":{"_token":token,"run_id":id,"event_format":"v2","from_seq":"0"}}).to_string();
    let (_, mut stream) = module.raw_json_request(&request, 1).await.unwrap();
    start_ipc(&b, 16).await;
    // Leave the one-message subscription buffer unread beyond the 2s send deadline.
    tokio::time::sleep(std::time::Duration::from_millis(2300)).await;
    let receipt = b.workflow_create("not blocked", None).await.unwrap();
    assert!(receipt.visible);
    let mut positions = std::collections::BTreeSet::new();
    while let Ok(Some(message)) =
        tokio::time::timeout(std::time::Duration::from_millis(100), stream.recv()).await
    {
        let v: Value = serde_json::from_str(message.get()).unwrap();
        let e = &v["params"]["result"];
        if let Some(seq) = e["event"]["run_seq"].as_str() {
            positions.insert(seq.parse::<u64>().unwrap());
        }
    }
    let mut cursor = Value::Null;
    loop {
        let v = call(
            &module,
            "run.events.page",
            json!({"_token":token,"run_id":id,"cursor":cursor,"limit":2}),
        )
        .await;
        for e in v["result"]["events"].as_array().unwrap() {
            positions.insert(
                e["event"]["run_seq"]
                    .as_str()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap(),
            );
        }
        cursor = v["result"]["next_cursor"].clone();
        if cursor.is_null() {
            break;
        }
    }
    let state = b.state().await.unwrap();
    let run = &state.runs[id];
    assert!(run.terminal());
    assert_eq!(
        positions.into_iter().collect::<Vec<_>>(),
        (1..=run.last_run_seq).collect::<Vec<_>>()
    );
    let node = &run.nodes["e"];
    let before = run.last_run_seq;
    let from = b.journal.durable_lsn();
    let request=json!({"jsonrpc":"2.0","id":2,"method":"run.subscribe","params":{"_token":token,"run_id":id,"event_format":"v2","mode":"audit","from_lsn":from.to_string()}}).to_string();
    let (_, mut audit) = module.raw_json_request(&request, 4).await.unwrap();
    let mut late = flow_journal::Event::new(
        flow_journal::EventKind::LateAudit,
        json!({"node_id":"e","evidence":"late"}),
    );
    late.run_id = Some(id.into());
    late.dispatch_id = Some(node.dispatch_id.clone());
    late.audit_seq = node.attempts[&node.dispatch_id].audit_seq + 1;
    b.append(vec![late]).await.unwrap();
    let message = tokio::time::timeout(std::time::Duration::from_secs(5), audit.recv())
        .await
        .unwrap()
        .unwrap();
    let v: Value = serde_json::from_str(message.get()).unwrap();
    assert_eq!(v["params"]["result"]["event"]["kind"], "late_audit");
    assert_eq!(b.state().await.unwrap().runs[id].last_run_seq, before);
    drop(audit);
    drop(stream);
    drop(module);
    b.close().await.unwrap();
    drop(b);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn delete_workflow_with_runs_preserves_history_and_blocks_new_runs() {
    let root = flow_test_support::io::TempDir::new("v2-delete-guard");
    let backend = JournalBackend::open(root.path(), Default::default())
        .await
        .unwrap();
    let token = "delete-guard-token-at-least-32-bytes";
    let module = flow_rpc::journal_v2::module(backend.clone(), token.into()).unwrap();
    let created = backend.workflow_create("history", None).await.unwrap();
    let workflow = created.result["workflow_id"].as_str().unwrap();
    backend.workflow_update(workflow, json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}), None).await.unwrap();
    backend.workflow_publish(workflow, 1, None).await.unwrap();
    let run = backend
        .run_start(workflow, None, Value::Null, "manual", None, None)
        .await
        .unwrap();
    let run = run.result["run_id"].as_str().unwrap();
    let before = backend.journal.durable_lsn();
    let deleted = call(
        &module,
        "workflow.delete",
        json!({"_token":token,"request_id":"delete","workflow_id":workflow}),
    )
    .await;
    assert!(deleted.get("error").is_none(), "{deleted}");
    assert!(backend.journal.durable_lsn() > before);
    assert!(backend.state().await.unwrap().workflows[workflow].deleted);
    assert!(flow_backend::journal_views::list_workflows(&backend)
        .await
        .unwrap()
        .is_empty());
    assert!(backend
        .run_start(workflow, None, Value::Null, "manual", None, None)
        .await
        .is_err());
    assert!(
        flow_backend::journal_views::get_version(&backend, workflow, Some(1))
            .await
            .is_ok()
    );
    let timeline = call(
        &module,
        "run.timeline",
        json!({"_token":token,"run_id":run}),
    )
    .await;
    assert!(timeline.get("error").is_none(), "{timeline}");
    let workflow = workflow.to_owned();
    backend.close().await.unwrap();
    drop(module);
    drop(backend);
    let recovered = JournalBackend::open(root.path(), Default::default())
        .await
        .unwrap();
    assert!(recovered.state().await.unwrap().workflows[&workflow].deleted);
    assert!(flow_backend::journal_views::list_workflows(&recovered)
        .await
        .unwrap()
        .is_empty());
    assert!(
        flow_backend::journal_views::get_version(&recovered, &workflow, Some(1))
            .await
            .is_ok()
    );
    assert!(recovered
        .run_start(&workflow, None, Value::Null, "manual", None, None)
        .await
        .is_err());
    let replacement = recovered.workflow_import("history", json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]}), true, None).await.unwrap();
    assert_eq!(replacement.result["created"], true);
    assert_ne!(replacement.result["workflow_id"], workflow);
    recovered.close().await.unwrap();
}

#[tokio::test]
async fn product_crud_replays_receipts_and_rejects_changed_arguments() {
    let root = flow_test_support::io::TempDir::new("v2-product-idempotence");
    let backend = JournalBackend::open(root.path(), Default::default())
        .await
        .unwrap();
    let token = "product-crud-token-at-least-32-bytes";
    let module = flow_rpc::journal_v2::module(backend.clone(), token.into()).unwrap();
    let workflow = backend
        .workflow_create("triggers", None)
        .await
        .unwrap()
        .result["workflow_id"]
        .clone();
    for (namespace, mut args) in [
        (
            "schedule",
            json!({"workflow_id":workflow,"cron":"* * * * *"}),
        ),
        ("webhook", json!({"workflow_id":workflow})),
        (
            "template",
            json!({"name":"reusable","nodes":[{"id":"n","type":"script","name":"Script","params":{"code":"return 1;"}}],"edges":[]}),
        ),
    ] {
        args["_token"] = json!(token);
        args["request_id"] = json!("create");
        let method = format!("{namespace}.create");
        let (first, duplicate) = tokio::join!(
            call(&module, &method, args.clone()),
            call(&module, &method, args.clone())
        );
        assert_eq!(first, duplicate, "concurrent duplicate of {method}");
        assert_eq!(first["result"]["committed"], true, "{first}");
        let status = call(
            &module,
            "command.status",
            json!({"_token":token,"scope":method,"request_id":"create"}),
        )
        .await;
        assert_eq!(status["result"], first["result"]);
        let mut changed = args.clone();
        if namespace == "template" {
            changed["name"] = json!("different");
        } else {
            changed["workflow_id"] = json!("different");
        }
        assert_eq!(
            call(&module, &method, changed).await["error"]["code"],
            -32012
        );
        let key_field = if namespace == "webhook" {
            "token"
        } else {
            "id"
        };
        let key = first["result"]["result"][key_field].clone();
        assert!(key.is_string(), "{first}");
        let mut update = json!({"_token":token,"request_id":"update", (key_field):key});
        let update_method = if namespace == "webhook" {
            "webhook.set_enabled".to_owned()
        } else {
            format!("{namespace}.update")
        };
        if namespace == "template" {
            update["category"] = json!("tools");
        } else {
            update["enabled"] = json!(false);
        }
        let updated = call(&module, &update_method, update.clone()).await;
        assert_eq!(updated["result"]["committed"], true, "{updated}");
        assert_eq!(updated, call(&module, &update_method, update).await);
        let delete = json!({"_token":token,"request_id":"delete", (key_field):key});
        let delete_method = format!("{namespace}.delete");
        let deleted = call(&module, &delete_method, delete.clone()).await;
        assert_eq!(deleted["result"]["result"]["deleted"], true, "{deleted}");
        assert_eq!(deleted, call(&module, &delete_method, delete).await);
        // Replaying a creation must return its original receipt even after deletion.
        assert_eq!(first, call(&module, &method, args).await);
    }
    let state = backend.state().await.unwrap();
    assert!(state.schedules.is_empty() && state.webhooks.is_empty() && state.templates.is_empty());
    backend.close().await.unwrap();
}

#[tokio::test]
async fn product_creation_is_visible_in_the_commit_receipt() {
    let root = flow_test_support::io::TempDir::new("v2-product-projection-delay");
    let backend = JournalBackend::open(root.path(), Default::default())
        .await
        .unwrap();
    let token = "product-delay-token-at-least-32-bytes";
    let module = flow_rpc::journal_v2::module(backend.clone(), token.into()).unwrap();
    let workflow = backend
        .workflow_create("delayed", None)
        .await
        .unwrap()
        .result["workflow_id"]
        .clone();
    let args =
        json!({"_token":token,"request_id":"stable","workflow_id":workflow,"cron":"* * * * *"});
    let response = call(&module, "schedule.create", args.clone()).await;
    let original = response["result"].clone();
    assert_eq!(original["visible"], true);
    assert_eq!(original["committed"], true);
    assert_eq!(original["request_id"], "stable");
    let status = call(
        &module,
        "command.status",
        json!({"_token":token,"scope":"schedule.create","request_id":"stable"}),
    )
    .await;
    assert_eq!(status["result"], original);
    let retry = call(&module, "schedule.create", args).await;
    assert_eq!(retry["result"]["commit_cursor"], original["commit_cursor"]);
    assert_eq!(retry["result"]["result"], original["result"]);
    assert_eq!(retry["result"]["visible"], true);
    assert_eq!(backend.state().await.unwrap().schedules.len(), 1);
    backend.close().await.unwrap();
}

/// 本地语义套件一律走真实 IPC 执行器（`cargo build --workspace --bins`
/// 先产出 flow-executor）；全平台同一入口（unix socketpair / 其余 loopback）。
async fn start_ipc(backend: &std::sync::Arc<flow_backend::journal::JournalBackend>, x_max: usize) {
    backend
        .start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
            flow_backend::execution::IpcOptions {
                executor: flow_engine::execution_protocol::contract::ExecutorInvocation::explicit(
                    flow_test_support::io::executor_bin(),
                ),
                x_max,
                tag: None,
            },
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn workflow_import_preflight_and_atomic_commit() {
    let root = flow_test_support::io::TempDir::new("workflow-atomic-import");
    let backend = JournalBackend::open(root.path(), Default::default())
        .await
        .unwrap();
    let token = "workflow-import-token-at-least-32-bytes";
    let module = flow_rpc::journal_v2::module(backend.clone(), token.into()).unwrap();
    let before = backend.journal.durable_lsn();
    let invalid = json!({"nodes":[],"edges":[]});
    let result = call(
        &module,
        "workflow.import",
        json!({"_token":token,"request_id":"bad","name":"bad","definition":invalid}),
    )
    .await;
    assert!(result.get("error").is_some(), "{result}");
    assert_eq!(backend.journal.durable_lsn(), before);
    assert!(backend.state().await.unwrap().workflows.is_empty());
    let definition = json!({"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]});
    let valid = call(
        &module,
        "workflow.validate",
        json!({"_token":token,"definition":definition}),
    )
    .await;
    assert_eq!(valid["result"]["valid"], true, "{valid}");
    assert_eq!(backend.journal.durable_lsn(), before);
    let args =
        json!({"_token":token,"request_id":"import","name":"imported","definition":definition});
    let imported = call(&module, "workflow.import", args.clone()).await;
    assert!(imported.get("error").is_none(), "{imported}");
    let receipt = &imported["result"];
    assert_eq!(receipt["result"]["status"], "published");
    assert_eq!(receipt["result"]["version"], 1);
    let after = backend.journal.durable_lsn();
    let replay = call(&module, "workflow.import", args).await;
    assert_eq!(replay["result"], imported["result"]);
    assert_eq!(backend.journal.durable_lsn(), after);
    let again = backend
        .workflow_import("imported", definition, true, Some("again"))
        .await
        .unwrap();
    assert_eq!(again.result["created"], false);
    assert_eq!(again.result["version"], 1);
    assert_eq!(backend.state().await.unwrap().workflows.len(), 1);
    backend.close().await.unwrap();
}

#[tokio::test]
async fn workflow_import_reports_missing_secrets_without_creating_records() {
    let root = flow_test_support::io::TempDir::new("workflow-import-missing-secret");
    let backend = JournalBackend::open(root.path(), Default::default())
        .await
        .unwrap();
    let definition = json!({"nodes":[{"id":"s","type":"start"},
        {"id":"agent","type":"harness","params":{"mode":"agent","prompt":"test","api_key":"WORKFLOW_IMPORT_MISSING_SECRET_TEST"}},
        {"id":"e","type":"end"}],"edges":[{"from":"s","to":"agent"},{"from":"agent","to":"e"}]});
    let before = backend.journal.durable_lsn();
    let validation = backend.workflow_validate(definition.clone()).unwrap();
    assert_eq!(validation["valid"], false);
    assert_eq!(
        validation["missing_secrets"],
        json!([{"node_id":"agent","name":"WORKFLOW_IMPORT_MISSING_SECRET_TEST"}])
    );
    assert!(backend
        .workflow_import("missing", definition, true, Some("missing"))
        .await
        .is_err());
    assert_eq!(backend.journal.durable_lsn(), before);
    assert!(backend.state().await.unwrap().workflows.is_empty());
    backend.close().await.unwrap();
}
