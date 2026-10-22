use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::{Run, RunStatus};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

async fn install(b: &JournalBackend, definition: Value) -> String {
    let id = b.workflow_create("contract", None).await.unwrap().result["workflow_id"]
        .as_str()
        .unwrap()
        .to_owned();
    b.workflow_update(&id, definition, None).await.unwrap();
    b.workflow_publish(&id, 1, None).await.unwrap();
    id
}
async fn start(b: &Arc<JournalBackend>) {
    b.start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
        flow_backend::execution::IpcOptions {
            executor: flow_engine::execution_protocol::contract::ExecutorInvocation::explicit(
                flow_test_support::io::executor_bin(),
            ),
            x_max: 4,
            tag: None,
        },
    ))
    .await
    .unwrap();
}
async fn until(b: &JournalBackend, id: &str, done: impl Fn(&Run) -> bool) -> Run {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let run = b.run_snapshot(id).await.unwrap().unwrap();
            if done(&run) {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
async fn run(b: &JournalBackend, wf: &str, input: Value) -> String {
    b.run_start(wf, None, input, "manual", None, None)
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned()
}
fn script(code: &str) -> Value {
    json!({"nodes":[{"id":"s","type":"start"},{"id":"n","type":"script","params":{"code":code}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]})
}

#[tokio::test]
async fn conditional_merge_executes_only_selected_branch_and_preserves_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    start(&b).await;
    let definition = json!({"nodes":[{"id":"s","type":"start"},{"id":"c","type":"condition","params":{"expr":"input.accept"}},{"id":"a","type":"script","params":{"code":"return 1;"}},{"id":"b","type":"script","params":{"code":"return 2;"}},{"id":"m","type":"script","params":{"code":"return {reached:true};"}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"c"},{"from":"c","to":"a","port":"true"},{"from":"c","to":"b","port":"false"},{"from":"a","to":"m"},{"from":"b","to":"m"},{"from":"m","to":"e"}]});
    let wf = install(&b, definition.clone()).await;
    for accept in [true, false] {
        let id = run(&b, &wf, json!({"accept":accept})).await;
        let r = until(&b, &id, Run::terminal).await;
        assert_eq!(r.status, RunStatus::Succeeded);
        assert_eq!(r.nodes["m"].attempt, 1);
        assert_eq!(
            r.nodes["e"].status,
            flow_engine::journal_state::NodeStatus::Succeeded
        );
    }
    let mut legacy = definition;
    legacy["semantics"] = json!(1);
    let wf = install(&b, legacy).await;
    let id = run(&b, &wf, json!({"accept":true})).await;
    let r = until(&b, &id, Run::terminal).await;
    assert_eq!(
        r.nodes["m"].status,
        flow_engine::journal_state::NodeStatus::Skipped
    );
    b.close().await.unwrap();
}

#[tokio::test]
async fn early_signal_and_child_dependencies_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let child = install(&b, script("return {version:1};")).await;
    let parent=install(&b,json!({"nodes":[{"id":"s","type":"start"},{"id":"h","type":"human_task"},{"id":"c","type":"sub_workflow","params":{"workflow_id":child}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"h"},{"from":"h","to":"c"},{"from":"c","to":"e"}]})).await;
    let id = run(&b, &parent, Value::Null).await;
    let receipt = b
        .run_signal(&id, "h", json!({"approved":true}), Some("early"))
        .await
        .unwrap();
    assert_eq!(receipt.result["status"], "queued");
    b.workflow_update(&child, script("return {version:2};"), None)
        .await
        .unwrap();
    b.workflow_publish(&child, 2, None).await.unwrap();
    b.close().await.unwrap();
    drop(b);
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    start(&b).await;
    let r = until(&b, &id, Run::terminal).await;
    assert_eq!(
        r.output,
        Some(flow_journal::StoredValue::Inline(json!({"version":1})))
    );
    assert!(r.signals.is_empty());
    assert_eq!(
        b.run_signal(&id, "h", json!({"approved":true}), Some("early"))
            .await
            .unwrap()
            .result["status"],
        "queued"
    );
    b.close().await.unwrap();
}

#[tokio::test]
async fn regression_reuses_recorded_time_randomness_and_reports_changes() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    start(&b).await;
    let wf = install(
        &b,
        script("return {time: Date.now(), zero: new Date().getTime(), random: Math.random()};"),
    )
    .await;
    let id = run(&b, &wf, Value::Null).await;
    let baseline = until(&b, &id, Run::terminal).await;
    assert_eq!(baseline.status, RunStatus::Succeeded);
    let candidate = b
        .run_regression(&id, None, None, "same")
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    until(&b, &candidate, Run::terminal).await;
    assert_eq!(
        b.compare_runs(&id, &candidate).await.unwrap()["passed"],
        true
    );
    b.workflow_update(&wf, script("return {changed:true};"), None)
        .await
        .unwrap();
    b.workflow_publish(&wf, 2, None).await.unwrap();
    let candidate = b
        .run_regression(&id, None, Some(2), "changed")
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    until(&b, &candidate, Run::terminal).await;
    assert_eq!(
        b.compare_runs(&id, &candidate).await.unwrap()["passed"],
        false
    );
    b.close().await.unwrap();
}

#[tokio::test]
async fn regression_never_reissues_http_and_rejects_changed_requests() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let http = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().fallback(axum::routing::any(move || {
                let c = count.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    axum::Json(json!({"ok":true}))
                }
            })),
        )
        .await
        .unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    start(&b).await;
    let definition = json!({"nodes":[{"id":"s","type":"start"},{"id":"h","type":"http_call","params":{"url":format!("http://{addr}/first")}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]});
    let wf = install(&b, definition.clone()).await;
    let baseline = run(&b, &wf, Value::Null).await;
    assert_eq!(
        until(&b, &baseline, Run::terminal).await.status,
        RunStatus::Succeeded
    );
    let candidate = b
        .run_regression(&baseline, None, None, "same")
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        until(&b, &candidate, Run::terminal).await.status,
        RunStatus::Succeeded
    );
    assert_eq!(
        b.compare_runs(&baseline, &candidate).await.unwrap()["passed"],
        true
    );
    let mut changed = definition;
    changed["nodes"][1]["params"]["url"] = json!(format!("http://{addr}/changed"));
    b.workflow_update(&wf, changed, None).await.unwrap();
    b.workflow_publish(&wf, 2, None).await.unwrap();
    let candidate = b
        .run_regression(&baseline, None, Some(2), "different")
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let failed = until(&b, &candidate, Run::terminal).await;
    assert_eq!(failed.status, RunStatus::Failed);
    assert!(failed
        .error
        .unwrap()
        .contains("differs from recorded fixture"));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let baseline_run = b.run_snapshot(&baseline).await.unwrap().unwrap();
    let operation = baseline_run.nodes["h"].operation.as_ref().unwrap();
    let mut request = flow_journal::value::materialize(
        b.journal.root(),
        b.journal.durable_lsn(),
        &operation.request,
        8 * 1024 * 1024,
    )
    .unwrap();
    request["url"] = json!(format!("http://{addr}/changed"));
    let overrides = std::collections::BTreeMap::from([(
        baseline_run.execution.as_ref().unwrap().node_key("h"),
        flow_engine::journal_state::OperationFixture {
            fingerprint: flow_journal::codec::fingerprint(&request, 8 * 1024 * 1024).unwrap(),
            outcome: operation.outcome.clone().unwrap(),
        },
    )]);
    let accepted = b
        .run_regression_with_fixtures(
            &baseline,
            None,
            Some(2),
            overrides.clone(),
            "explicit-fixture",
        )
        .await
        .unwrap();
    let candidate = accepted.result["run_id"].as_str().unwrap();
    assert_eq!(
        until(&b, candidate, Run::terminal).await.status,
        RunStatus::Succeeded
    );
    assert_eq!(
        b.run_regression_with_fixtures(&baseline, None, Some(2), overrides, "explicit-fixture")
            .await
            .unwrap()
            .result,
        accepted.result
    );
    assert!(
        b.compare_runs(&baseline, candidate).await.unwrap()["differences"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["path"] == "nodes.h.request")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    b.close().await.unwrap();
    http.abort();
}

#[tokio::test]
async fn evidence_bundle_is_closed_portable_and_rejects_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let b = JournalBackend::open(&data, Default::default())
        .await
        .unwrap();
    start(&b).await;
    let wf = install(&b, script("return input;")).await;
    let id = run(&b, &wf, json!({"large":"x".repeat(200_000)})).await;
    let expected = until(&b, &id, Run::terminal).await;
    b.close().await.unwrap();
    drop(b);
    let destination = dir.path().join("bundle");
    let manifest = flow_backend::bundle::export(&data, &id, &destination).unwrap();
    assert!(!manifest.values.is_empty());
    let (_, state) = flow_backend::bundle::inspect(&destination).unwrap();
    assert_eq!(state.runs[&id].output, expected.output);
    assert!(flow_backend::bundle::export(&data, &id, &destination).is_err());
    let path = destination.join("values").join(flow_journal::codec::digest(
        manifest.values.keys().next().unwrap().as_bytes(),
    ));
    std::fs::write(path, b"corrupt").unwrap();
    assert!(flow_backend::bundle::inspect(&destination)
        .unwrap_err()
        .to_string()
        .contains("checksum"));
}

#[tokio::test]
async fn invocation_uses_scoped_credentials_validates_contracts_and_protects_other_runs() {
    const ADMIN: &str = "management-credential-not-for-business-clients-123456";
    const KEY: &str = "business-invocation-credential-at-least-32-bytes";
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let mut definition = script("return input; /* private_engine_literal */");
    definition["input_schema"] = json!({"type":"object","required":["id"],"properties":{"id":{"type":"integer"}},"additionalProperties":false});
    definition["input_schema"]["description"] =
        json!(format!("private_engine_literal{}", "x".repeat(70_000)));
    definition["output_schema"] = definition["input_schema"].clone();
    let wf = install(&b, definition).await;
    b.config_change("webhook.change",flow_journal::EventKind::WebhookChanged,"business",json!({"workflow_id":wf,"enabled":true,"invoke_version":1,"invoke_key_hash":flow_journal::codec::digest(KEY.as_bytes())}),Some("hook")).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = b.clone();
    let http = tokio::spawn(async move {
        axum::serve(
            listener,
            flow_rpc::journal_triggers::router(server, ADMIN.into()),
        )
        .await
        .unwrap()
    });
    let client = reqwest::Client::new();
    let endpoint = format!("http://{addr}/invoke/business");
    let preflight = client
        .request(reqwest::Method::OPTIONS, &endpoint)
        .header("Origin", "https://business.example")
        .header(
            "Access-Control-Request-Headers",
            "Authorization, Idempotency-Key",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status(), 204);
    assert!(preflight.headers()["access-control-allow-headers"]
        .to_str()
        .unwrap()
        .contains("Authorization"));
    assert_eq!(
        client
            .post(&endpoint)
            .bearer_auth(ADMIN)
            .header("Idempotency-Key", "no")
            .json(&json!({"id":1}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(&endpoint)
            .bearer_auth(KEY)
            .header("Idempotency-Key", "invalid")
            .json(&json!({"id":"wrong"}))
            .send()
            .await
            .unwrap()
            .status(),
        400
    );
    let response = client
        .post(&endpoint)
        .bearer_auth(KEY)
        .header("Idempotency-Key", "accepted")
        .header("X-Correlation-Id", "order-1")
        .json(&json!({"id":1}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 202);
    let accepted: Value = response.json().await.unwrap();
    let duplicate: Value = client
        .post(&endpoint)
        .bearer_auth(KEY)
        .header("Idempotency-Key", "accepted")
        .header("X-Correlation-Id", "order-1")
        .json(&json!({"id":1}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(accepted["run_id"], duplicate["run_id"]);
    assert_eq!(accepted["run_token"], duplicate["run_token"]);
    assert_eq!(
        client
            .post(&endpoint)
            .bearer_auth(KEY)
            .header("Idempotency-Key", "accepted")
            .header("X-Correlation-Id", "order-1")
            .json(&json!({"id":2}))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    let id = accepted["run_id"].as_str().unwrap();
    let key = accepted["run_token"].as_str().unwrap();
    let other = run(&b, &wf, json!({"id":2})).await;
    assert_eq!(
        client
            .get(format!("http://{addr}/invocations/{other}"))
            .bearer_auth(key)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .post(format!("http://{addr}/hooks/business"))
            .bearer_auth(key)
            .header("Idempotency-Key", "admin-denied")
            .json(&json!({"id":1}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    start(&b).await;
    until(&b, id, Run::terminal).await;
    let result: Value = client
        .get(format!("http://{addr}/invocations/{id}"))
        .bearer_auth(key)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(result["output"], json!({"id":1}));
    assert_eq!(result["correlation_id"], "order-1");
    let events: Value = client
        .get(format!("http://{addr}/invocations/{id}/events"))
        .bearer_auth(key)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!events.to_string().contains("private_engine_literal"));
    assert!(events["events"]
        .as_array()
        .unwrap()
        .iter()
        .all(|e| e["event"]["payload"].get("definition").is_none()));
    let after: Value = client
        .get(format!(
            "http://{addr}/invocations/{id}/events?after_lsn={}",
            events["snapshot_cursor"]["lsn"].as_str().unwrap()
        ))
        .bearer_auth(key)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(after["events"].as_array().unwrap().is_empty());
    let private = b.run_snapshot(id).await.unwrap().unwrap().definition;
    let flow_journal::StoredValue::Ref(private) = private else {
        panic!("private definition must use a blob")
    };
    assert_eq!(
        client
            .get(format!(
                "http://{addr}/invocations/{id}/values/{}",
                private.output_id
            ))
            .bearer_auth(key)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        client
            .post(format!("http://{addr}/invocations/{id}/signals/n"))
            .bearer_auth(key)
            .header("Idempotency-Key", "forbidden")
            .json(&json!({"action":"retry"}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    b.close().await.unwrap();
    http.abort();
}

#[tokio::test]
async fn native_model_and_tool_steps_are_recorded_and_regress_without_calls() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener,axum::Router::new().fallback(axum::routing::any(move ||{let c=counter.clone();async move{c.fetch_add(1,Ordering::SeqCst);axum::Json(json!({"tool_calls":[{"name":"lookup","arguments":{"id":42}}],"usage":{"tokens":12}}))}}))).await.unwrap()
    });
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    start(&b).await;
    let wf=install(&b,json!({"nodes":[{"id":"s","type":"start"},{"id":"model","type":"llm","params":{"url":format!("http://{addr}/model"),"body":{"model":"test-model","messages":[{"role":"user","content":"lookup"}]},"response_schema":{"type":"object","required":["tool_calls"]}}},{"id":"tool","type":"http_call","params":{"method":"POST","url":format!("http://{addr}/tool"),"body":"${nodes.model.body.tool_calls[0].arguments}"}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"model"},{"from":"model","to":"tool"},{"from":"tool","to":"e"}]})).await;
    let id = run(&b, &wf, Value::Null).await;
    let baseline = until(&b, &id, Run::terminal).await;
    assert_eq!(baseline.status, RunStatus::Succeeded);
    for node in ["model", "tool"] {
        assert!(baseline.nodes[node]
            .operation
            .as_ref()
            .unwrap()
            .outcome
            .is_some());
    }
    let candidate = b
        .run_regression(&id, None, None, "recorded-model")
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    until(&b, &candidate, Run::terminal).await;
    assert_eq!(
        b.compare_runs(&id, &candidate).await.unwrap()["passed"],
        true
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    b.close().await.unwrap();
    server.abort();
}

#[tokio::test]
async fn unknown_operations_retry_only_with_explicit_repeatable_policy() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for repeatable in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let mut bytes = [0; 8192];
            let _ = first.read(&mut bytes).await.unwrap();
            drop(first);
            let (mut second, _) = listener.accept().await.unwrap();
            let _ = second.read(&mut bytes).await.unwrap();
            second.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}").await.unwrap();
        });
        let dir = tempfile::tempdir().unwrap();
        let b = JournalBackend::open(dir.path(), Default::default())
            .await
            .unwrap();
        start(&b).await;
        let wf=install(&b,json!({"nodes":[{"id":"s","type":"start"},{"id":"h","type":"http_call","params":{"url":format!("http://{addr}"),"effect_policy":if repeatable{"repeatable"}else{"manual"},"retry":{"max_attempts":2,"backoff_ms":20}}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]})).await;
        let id = run(&b, &wf, Value::Null).await;
        let r = until(&b, &id, |r| {
            if repeatable {
                r.terminal()
            } else {
                r.status == RunStatus::AwaitingResume
            }
        })
        .await;
        if repeatable {
            assert_eq!(r.status, RunStatus::Succeeded);
            assert_eq!(r.nodes["h"].attempt, 2);
        } else {
            assert_eq!(r.nodes["h"].attempt, 1);
            assert!(r.nodes["h"].operation.as_ref().unwrap().outcome.is_none());
        }
        b.close().await.unwrap();
        server.abort();
    }
}

#[tokio::test]
async fn incompatible_runtime_leaves_active_runs_untouched_and_starts_execution() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let wf = install(&b, script("return 1;")).await;
    let id = run(&b, &wf, Value::Null).await;
    let mut other = b.run_snapshot(&id).await.unwrap().unwrap();
    other.run_id = "different-runtime".into();
    other.execution.as_mut().unwrap().runtime = "0".repeat(64);
    let mut event = flow_journal::Event::new(
        flow_journal::EventKind::RunStarted,
        serde_json::to_value(other).unwrap(),
    );
    event.run_id = Some("different-runtime".into());
    event.run_seq = 1;
    b.append(vec![event]).await.unwrap();
    // 旧 runtime 的在途 run 不拒绝启动、不判死：执行照常启动，当前
    // runtime 的 run 正常执行；陈旧 run 原样保留（协调器跳过，不空转），
    // 用户可手动取消。拒绝启动会把桌面 app 整体带崩（setup hook 失败）。
    b.start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
        flow_backend::execution::IpcOptions {
            executor: flow_engine::execution_protocol::contract::ExecutorInvocation::explicit(
                flow_test_support::io::executor_bin(),
            ),
            x_max: 1,
            tag: None,
        },
    ))
    .await
    .unwrap();
    let r = until(&b, &id, Run::terminal).await;
    assert_eq!(
        r.status,
        RunStatus::Succeeded,
        "当前 runtime 的 run 应正常执行"
    );
    let stale = b.run_snapshot("different-runtime").await.unwrap().unwrap();
    assert_eq!(stale.status, RunStatus::Running, "陈旧 run 应原样保留");
    assert!(stale.nodes.is_empty(), "陈旧 run 不应被推进");
    // 用户逃生口：手动取消把陈旧 run 收口
    b.run_cancel("different-runtime", None).await.unwrap();
    let stale = b.run_snapshot("different-runtime").await.unwrap().unwrap();
    assert_eq!(stale.status, RunStatus::Cancelled);
    b.close().await.unwrap();
}

#[tokio::test]
async fn input_and_output_contracts_fail_at_the_correct_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    let mut definition = script("return 'wrong';");
    definition["input_schema"] = json!({"type":"integer"});
    definition["output_schema"] = json!({"type":"integer"});
    let wf = install(&b, definition).await;
    assert!(b
        .run_start(&wf, None, json!("wrong"), "manual", None, None)
        .await
        .unwrap_err()
        .to_string()
        .contains("expected integer"));
    let id = run(&b, &wf, json!(1)).await;
    start(&b).await;
    let r = until(&b, &id, Run::terminal).await;
    assert_eq!(r.status, RunStatus::Failed);
    assert!(r.error.unwrap().contains("output contract"));
    b.close().await.unwrap();
}

#[tokio::test]
async fn actual_cli_regression_returns_success_and_difference_exit_codes() {
    let dir = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(dir.path(), Default::default())
        .await
        .unwrap();
    start(&b).await;
    let wf = install(&b, script("return {ok:true};")).await;
    let baseline = run(&b, &wf, Value::Null).await;
    until(&b, &baseline, Run::terminal).await;
    let token = "cli-regression-administrator-token-32-bytes";
    let (server, addr) =
        flow_rpc::journal_v2::serve(b.clone(), token.into(), "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
    let command = |version: &str| {
        let mut command = tokio::process::Command::new(flow_test_support::io::cli_bin());
        command.env("FLOW_JOURNAL_TOKEN", token).args([
            "--url",
            &format!("ws://{addr}"),
            "journal",
            "regress",
            &baseline,
            "--version",
            version,
            "--timeout",
            "15",
        ]);
        command
    };
    let same = command("1").output().await.unwrap();
    assert!(
        same.status.success(),
        "{}",
        String::from_utf8_lossy(&same.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&same.stdout).unwrap()["passed"],
        true
    );
    b.workflow_update(&wf, script("return {changed:true};"), None)
        .await
        .unwrap();
    b.workflow_publish(&wf, 2, None).await.unwrap();
    let different = command("2").output().await.unwrap();
    assert_eq!(different.status.code(), Some(4));
    assert_eq!(
        serde_json::from_slice::<Value>(&different.stdout).unwrap()["passed"],
        false
    );
    server.stop().unwrap();
    server.stopped().await;
    b.close().await.unwrap();
}
