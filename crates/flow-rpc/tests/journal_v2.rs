use flow_backend::journal::JournalBackend;
use jsonrpsee::RpcModule;
use serde_json::{json, Value};

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
    backend.start_execution().await.unwrap();
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
    let now = chrono::Local::now();
    tokio::join!(
        flow_rpc::journal_triggers::fire_due(&backend, now),
        flow_rpc::journal_triggers::fire_due(&backend, now)
    );
    assert_eq!(backend.state().await.runs.len(), 1);
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
    assert_eq!(backend.state().await.runs.len(), 2);
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn authenticated_commands_attach_and_report_projection_timeout() {
    let root = std::env::temp_dir().join(format!("flow-v2-rpc-{}", uuid::Uuid::now_v7()));
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let token = "a".repeat(32);
    let module = flow_rpc::journal_v2::module(backend.clone(), token.clone()).unwrap();
    let denied = call(&module, "workflow.create", json!({"name":"test"})).await;
    assert_eq!(denied["error"]["code"], -32001);
    assert!(backend.state().await.workflows.is_empty());
    backend.pause_projection(true);
    let params = json!({"_token":token,"name":"test","request_id":"same"});
    let first = call(&module, "workflow.create", params.clone()).await;
    assert_eq!(
        first["error"]["code"],
        flow_rpc::journal_v2::COMMITTED_NOT_VISIBLE
    );
    let receipt = &first["error"]["data"];
    assert_eq!(receipt["committed"], true);
    let status = call(
        &module,
        "command.status",
        json!({"_token":token,"scope":"workflow.create","request_id":"same"}),
    )
    .await;
    assert_eq!(status["result"]["commit_cursor"], receipt["commit_cursor"]);
    backend.pause_projection(false);
    let duplicate = call(&module, "workflow.create", params).await;
    assert_eq!(
        duplicate["result"]["commit_cursor"],
        receipt["commit_cursor"]
    );
    assert_eq!(duplicate["result"]["visible"], true);
    assert_eq!(backend.state().await.workflows.len(), 1);
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
    let snapshot = backend.state().await;
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
