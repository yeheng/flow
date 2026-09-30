use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::Run;
use flow_journal::{JournalOptions, StoredValue};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-journal-exec-{}", uuid::Uuid::now_v7()))
}

#[tokio::test]
async fn pure_retry_preserves_preparation_and_original_deadline_across_restart() {
    let root = temp();
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    let workflow = install(&backend, json!({"nodes":[{"id":"s","type":"start"},{"id":"n","type":"script","params":{"code":"throw new Error('retry me');","retry":{"max_attempts":2,"backoff_ms":800}}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]})).await;
    let created = backend
        .run_start(
            &workflow,
            None,
            json!({"original":true}),
            "manual",
            None,
            None,
        )
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap();
    backend.start_execution().await.unwrap();
    let waiting = until(&backend, run_id, |r| {
        r.nodes
            .get("n")
            .is_some_and(|n| n.wait.as_ref().is_some_and(|w| w.kind == "retry"))
    })
    .await;
    let deadline = waiting.nodes["n"].wait.as_ref().unwrap().wake_at;
    let prepared = waiting.nodes["n"].prepared.clone();
    backend.close().await.unwrap();
    drop(backend);
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    assert_eq!(
        backend.state().await.runs[run_id].nodes["n"]
            .wait
            .as_ref()
            .unwrap()
            .wake_at,
        deadline
    );
    backend.start_execution().await.unwrap();
    let done = until(&backend, run_id, Run::terminal).await;
    assert_eq!(done.status, "failed");
    assert_eq!(done.nodes["n"].attempt, 2);
    assert_eq!(done.nodes["n"].prepared, prepared);
    assert_eq!(done.nodes["n"].attempts.len(), 2);
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn manual_resolution_binds_operation_and_never_fabricates_outcome() {
    use flow_journal::{Event, EventKind, StoredValue};
    let root = temp();
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let workflow=install(&backend,json!({"nodes":[{"id":"s","type":"start"},{"id":"h","type":"http_call","params":{"url":"http://invalid.invalid"}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]})).await;
    let created = backend
        .run_start(&workflow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap();
    let event = |kind, payload, run_seq, audit_seq| {
        let mut e = Event::new(kind, payload);
        e.run_id = Some(run_id.into());
        e.node_id = Some("h".into());
        e.dispatch_id = Some("dispatch".into());
        e.run_seq = run_seq;
        e.audit_seq = audit_seq;
        e
    };
    backend
        .append(vec![event(
            EventKind::DispatchStarted,
            json!({"node_execution_id":"execution","attempt":"1"}),
            2,
            0,
        )])
        .await
        .unwrap();
    let prepared = flow_engine::journal_state::Prepared {
        input: StoredValue::Inline(json!({})),
        predecessors: Default::default(),
        params: StoredValue::Inline(json!({"url":"http://invalid.invalid"})),
        engine_build: "test".into(),
        node_semantics: 1,
    };
    backend
        .append(vec![event(
            EventKind::InputPrepared,
            json!({"prepared":prepared}),
            0,
            1,
        )])
        .await
        .unwrap();
    let op = flow_engine::journal_state::Operation {
        operation_id: "operation".into(),
        fingerprint: flow_journal::codec::digest(b"{}"),
        permit_id: "permit".into(),
        request: StoredValue::Inline(json!({})),
        outcome: None,
    };
    backend
        .append(vec![
            event(EventKind::OperationIntent, json!({"operation":op}), 3, 0),
            event(
                EventKind::OperationAuthorized,
                json!({"operation":op}),
                4,
                0,
            ),
        ])
        .await
        .unwrap();
    backend.append(vec![event(EventKind::WaitRegistered,json!({"wait":{"kind":"uncertain","wake_at":null,"child_run_id":null},"sealed_through":"1","integrity":"unknown"}),5,0)]).await.unwrap();
    assert!(backend
        .run_adjudicate(
            run_id,
            "h",
            "other",
            "verified externally",
            json!({"accepted":true}),
            "decision"
        )
        .await
        .is_err());
    let receipt = backend
        .run_adjudicate(
            run_id,
            "h",
            "operation",
            "verified externally",
            json!({"accepted":true}),
            "decision",
        )
        .await
        .unwrap();
    let duplicate = backend
        .run_adjudicate(
            run_id,
            "h",
            "operation",
            "verified externally",
            json!({"accepted":true}),
            "decision",
        )
        .await
        .unwrap();
    assert_eq!(receipt.commit_cursor, duplicate.commit_cursor);
    backend.start_execution().await.unwrap();
    let done = until(&backend, run_id, Run::terminal).await;
    assert_eq!(done.status, "succeeded");
    assert!(done.nodes["h"]
        .operation
        .as_ref()
        .unwrap()
        .outcome
        .is_none());
    assert_eq!(done.nodes["h"].attempts["dispatch"].integrity, "unknown");
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}
async fn install(b: &JournalBackend, definition: Value) -> String {
    let created = b.workflow_create("test", None).await.unwrap();
    let id = created.result["workflow_id"].as_str().unwrap().to_string();
    b.workflow_update(&id, definition, None).await.unwrap();
    b.workflow_publish(&id, 1, None).await.unwrap();
    id
}
async fn until(b: &Arc<JournalBackend>, run_id: &str, predicate: impl Fn(&Run) -> bool) -> Run {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let run = b.state().await.runs[run_id].clone();
            if predicate(&run) {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn script_actual_values_and_delay_survive_restart() {
    let root = temp();
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    let workflow=install(&backend,json!({"nodes":[{"id":"s","type":"start"},{"id":"n","type":"script","params":{"code":"console.log('hello'); return {my_auth_token: input.my_auth_token, n: nodes.s.n + 1};"}},{"id":"d","type":"delay","params":{"ms":1000}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"n"},{"from":"n","to":"d"},{"from":"d","to":"e"}]})).await;
    let created = backend
        .run_start(
            &workflow,
            None,
            json!({"my_auth_token":"business secret","n":4}),
            "manual",
            None,
            Some("run-one"),
        )
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap();
    backend.start_execution().await.unwrap();
    let waiting = until(&backend, run_id, |r| {
        r.nodes.get("d").is_some_and(|n| n.wait.is_some())
    })
    .await;
    assert_eq!(
        waiting.nodes["n"].output,
        Some(StoredValue::Inline(
            json!({"my_auth_token":"business secret","n":5})
        ))
    );
    assert!(waiting.nodes["n"]
        .prepared
        .as_ref()
        .unwrap()
        .predecessors
        .contains_key("s"));
    let wake_at = waiting.nodes["d"].wait.as_ref().unwrap().wake_at;
    backend.close().await.unwrap();
    drop(backend);
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    assert_eq!(
        backend.state().await.runs[run_id].nodes["d"]
            .wait
            .as_ref()
            .unwrap()
            .wake_at,
        wake_at
    );
    backend.start_execution().await.unwrap();
    let done = until(&backend, run_id, |r| r.terminal()).await;
    assert_eq!(done.status, "succeeded");
    assert_eq!(
        done.output,
        Some(StoredValue::Inline(json!({"slept_ms":1000})))
    );
    assert_eq!(done.nodes["n"].attempt, 1);
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn child_completion_wakes_parent_and_signal_is_idempotent() {
    let root = temp();
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    let child=install(&backend,json!({"nodes":[{"id":"s","type":"start"},{"id":"h","type":"human_task"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]})).await;
    let parent=install(&backend,json!({"nodes":[{"id":"s","type":"start"},{"id":"c","type":"sub_workflow","params":{"workflow_id":child}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"c"},{"from":"c","to":"e"}]})).await;
    let created = backend
        .run_start(&parent, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap();
    backend.start_execution().await.unwrap();
    let waiting = until(&backend, run_id, |r| {
        r.nodes.get("c").is_some_and(|n| n.wait.is_some())
    })
    .await;
    let child_id = waiting.nodes["c"]
        .wait
        .as_ref()
        .unwrap()
        .child_run_id
        .as_ref()
        .unwrap();
    until(&backend, child_id, |r| {
        r.nodes.get("h").is_some_and(|n| n.wait.is_some())
    })
    .await;
    let receipt = backend
        .run_signal(child_id, "h", json!({"ok":true}), Some("signal-1"))
        .await
        .unwrap();
    let duplicate = backend
        .run_signal(child_id, "h", json!({"ok":true}), Some("signal-1"))
        .await
        .unwrap();
    assert_eq!(duplicate.commit_cursor, receipt.commit_cursor);
    let done = until(&backend, run_id, |r| r.terminal()).await;
    assert_eq!(done.status, "succeeded");
    assert_eq!(done.output, Some(StoredValue::Inline(json!({"ok":true}))));
    backend.close().await.unwrap();
    drop(backend);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn http_result_is_complete_and_never_resent_after_wait_restart() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let response = json!({"my_auth_token":"business","data":"雪".repeat(60_000)}).to_string();
    let expected = response.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let _ = socket.read(&mut request).await.unwrap();
            observed.fetch_add(1, Ordering::Relaxed);
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        response.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            for chunk in response.as_bytes().chunks(137) {
                socket.write_all(chunk).await.unwrap();
            }
        }
    });
    let root = temp();
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    let workflow=install(&backend,json!({"nodes":[{"id":"s","type":"start"},{"id":"h","type":"http_call","params":{"url":format!("http://{addr}")}},{"id":"n","type":"script","params":{"code":"return {token: nodes.h.body.my_auth_token, length: nodes.h.body.data.length};"}},{"id":"d","type":"delay","params":{"ms":1000}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"h"},{"from":"h","to":"n"},{"from":"n","to":"d"},{"from":"d","to":"e"}]})).await;
    let created = backend
        .run_start(&workflow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap();
    backend.start_execution().await.unwrap();
    let waiting = until(&backend, run_id, |r| {
        r.nodes.get("d").is_some_and(|n| n.wait.is_some())
    })
    .await;
    assert_eq!(
        waiting.nodes["n"].output,
        Some(StoredValue::Inline(
            json!({"token":"business","length":60_000})
        ))
    );
    let output = flow_journal::value::materialize(
        &root,
        backend.journal.durable_lsn(),
        waiting.nodes["h"].output.as_ref().unwrap(),
        1024 * 1024,
    )
    .unwrap();
    assert_eq!(
        output["body"],
        serde_json::from_str::<Value>(&expected).unwrap()
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    backend.close().await.unwrap();
    drop(backend);
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    backend.start_execution().await.unwrap();
    let done = until(&backend, run_id, |r| r.terminal()).await;
    assert_eq!(done.status, "succeeded");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    backend.close().await.unwrap();
    drop(backend);
    server.abort();
    std::fs::remove_dir_all(root).unwrap();
}
