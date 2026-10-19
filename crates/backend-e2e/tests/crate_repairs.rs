//! Regressions for lifecycle and contract gaps found by the crate review.
use flow_backend::journal::JournalBackend;
use serde_json::{json, Value};
use std::time::Duration;

async fn install(b: &JournalBackend, definition: Value) -> String {
    let id = b.workflow_create("repair", None).await.unwrap().result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    b.workflow_update(&id, definition, None).await.unwrap();
    b.workflow_publish(&id, 1, None).await.unwrap();
    id
}
fn passthrough() -> Value {
    json!({"semantics":2,"nodes":[{"id":"s","type":"start"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"e"}]})
}

#[tokio::test]
async fn terminal_subscription_closes_after_unrelated_commits() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let wf = install(&b, passthrough()).await;
    let receipt = b
        .run_start(&wf, None, Value::Null, "manual", None, None)
        .await
        .unwrap();
    let id = receipt.result["run_id"].as_str().unwrap();
    b.run_cancel(id, None).await.unwrap();
    b.workflow_create("unrelated", None).await.unwrap();
    let token = "repair-test-token-at-least-32-characters";
    let methods: jsonrpsee::core::server::Methods =
        flow_rpc::journal_v2::module(b.clone(), token.into())
            .unwrap()
            .into();
    let request = json!({"jsonrpc":"2.0","id":1,"method":"run.subscribe","params":{"_token":token,"event_format":"v2","run_id":id,"from_seq":"0"}}).to_string();
    let (reply, mut events) = methods.raw_json_request(&request, 64).await.unwrap();
    assert!(!reply.get().contains("\"error\""), "{}", reply.get());
    let received = tokio::time::timeout(Duration::from_secs(2), async {
        let mut received = Vec::new();
        while let Some(event) = events.recv().await {
            received.push(serde_json::from_str::<Value>(event.get()).unwrap());
        }
        received
    })
    .await
    .expect("terminal subscription must release its sink after replay");
    let kinds: Vec<_> = received
        .iter()
        .filter_map(|e| e["params"]["result"]["event"]["kind"].as_str())
        .collect();
    assert_eq!(kinds, ["run_started", "run_cancelled"]);
    b.close().await.unwrap();
}

#[tokio::test]
async fn child_inputs_use_the_pinned_contract_before_run_creation() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    b.start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
        flow_backend::execution::IpcOptions {
            executor: flow_engine::execution_protocol::ExecutorInvocation::explicit(
                flow_test_support::io::executor_bin(),
            ),
            x_max: 2,
            tag: None,
        },
    ))
    .await
    .unwrap();
    let schema = json!({"type":"object","required":["n"],"properties":{"n":{"type":"integer"},"padding":{"type":"string"}},"additionalProperties":false});
    let mut child = passthrough();
    child["input_schema"] = schema;
    let child = install(&b, child).await;
    for inherited in [false, true] {
        for input in [
            json!({}),
            json!({"n":"wrong"}),
            json!({"n":1,"extra":true}),
            json!({"n":1}),
            json!({"n":1,"padding":"x".repeat(70_000)}),
        ] {
            let valid = input["n"] == 1 && input.get("extra").is_none();
            let mut params = json!({"workflow_id":child});
            if !inherited {
                params["input_mapping"] = input.clone();
            }
            let parent = install(&b, json!({"semantics":2,"nodes":[{"id":"s","type":"start"},{"id":"c","type":"sub_workflow","params":params},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"c"},{"from":"c","to":"e"}]})).await;
            let receipt = b
                .run_start(
                    &parent,
                    None,
                    if inherited {
                        input.clone()
                    } else {
                        Value::Null
                    },
                    "manual",
                    None,
                    None,
                )
                .await
                .unwrap();
            let id = receipt.result["run_id"].as_str().unwrap();
            let run = tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    let run = b.run_snapshot(id).await.unwrap().unwrap();
                    if run.terminal() {
                        break run;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(
                run.status == flow_engine::journal_state::RunStatus::Succeeded,
                valid,
                "{input}: {:?}",
                run.error
            );
            let children = b.store.children(id, b.journal.durable_lsn()).await.unwrap();
            assert_eq!(
                children.len(),
                usize::from(valid),
                "invalid inputs must not create a child"
            );
            if !valid {
                assert!(run.error.unwrap().contains("input contract"));
            }
        }
    }
    b.close().await.unwrap();
}

#[tokio::test]
async fn cli_recovers_an_accepted_write_after_losing_its_reply() {
    use futures::StreamExt;
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let wf = install(&b, passthrough()).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let backend = b.clone();
    let (accepted, committed) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
        let message = ws.next().await.unwrap().unwrap();
        let request: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
        let p = &request["params"];
        let key = p["request_id"].as_str().unwrap();
        let receipt = backend
            .run_start(
                p["workflow_id"].as_str().unwrap(),
                None,
                Value::Null,
                "manual",
                None,
                Some(key),
            )
            .await
            .unwrap();
        accepted
            .send((
                key.to_owned(),
                receipt.result["run_id"].as_str().unwrap().to_owned(),
            ))
            .unwrap();
        // Commit succeeded; terminate the socket without delivering the response.
    });
    let params = dir.path().join("request.json");
    std::fs::write(&params, json!({"workflow_id":wf}).to_string()).unwrap();
    let token = "repair-test-token-at-least-32-characters";
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::process::Command::new(flow_test_support::io::cli_bin())
            .env("FLOW_JOURNAL_TOKEN", token)
            .arg("--url")
            .arg(format!("ws://{addr}"))
            .args(["journal", "call", "run.start", "--params"])
            .arg(params)
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    peer.await.unwrap();
    assert!(!output.status.success());
    let (key, original) = committed.await.unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains(&key) && stderr.contains("run.start:manual:"),
        "{stderr}"
    );
    assert!(!stderr.contains(token));
    assert!(b
        .command_status("run.start:manual:", &key)
        .await
        .unwrap()
        .is_some());
    let (server, addr) =
        flow_rpc::journal_v2::serve(b.clone(), token.into(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
    let output = tokio::process::Command::new(flow_test_support::io::cli_bin())
        .env("FLOW_JOURNAL_TOKEN", token)
        .arg("--url")
        .arg(format!("ws://{addr}"))
        .arg("--request-id")
        .arg(&key)
        .args(["run", "start", &wf, "--detach"])
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), original);
    let rows = b
        .store
        .runs_page(flow_store::store::RunsQuery {
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap()
        .1;
    assert_eq!(rows.len(), 1, "retry must not create another run");
    server.stop().unwrap();
    server.stopped().await;
    b.close().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn harness_deadline_and_run_cancel_stop_descendants() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(&dir.path().join("data"), Default::default())
        .await
        .unwrap();
    b.start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
        flow_backend::execution::IpcOptions {
            executor: flow_engine::execution_protocol::ExecutorInvocation::explicit(
                flow_test_support::io::executor_bin(),
            ),
            x_max: 2,
            tag: None,
        },
    ))
    .await
    .unwrap();
    for cancel in [false, true] {
        let marker = dir.path().join(format!("escaped-{cancel}"));
        let ready = dir.path().join(format!("ready-{cancel}"));
        let shell = format!(
            "(sleep 0.7; touch '{}') & touch '{}'; wait",
            marker.display(),
            ready.display()
        );
        let wf = install(&b, json!({"semantics":2,"nodes":[{"id":"s","type":"start"},{"id":"h","type":"harness","params":{"command":"/bin/sh","args":["-c",shell],"prompt":"x","timeout_ms":if cancel {10_000}else{100}}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]})).await;
        let receipt = b
            .run_start(&wf, None, Value::Null, "manual", None, None)
            .await
            .unwrap();
        let id = receipt.result["run_id"].as_str().unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while !ready.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            if cancel {
                b.run_cancel(id, None).await.unwrap();
            }
            loop {
                let run = b.run_snapshot(id).await.unwrap().unwrap();
                if if cancel {
                    run.terminal()
                } else {
                    run.status == flow_engine::journal_state::RunStatus::AwaitingResume
                } {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert!(
            !marker.exists(),
            "harness descendant survived cancel={cancel}"
        );
    }
    b.close().await.unwrap();
}
