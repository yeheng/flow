//! Non-power-loss phase-one acceptance. All datasets are disposable.
use flow_backend::journal::JournalBackend;
use serde_json::{json, Value};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
async fn install(b: &JournalBackend, nodes: Value, edges: Value) -> String {
    let r = b.workflow_create("acceptance", None).await.unwrap();
    let id = r.result["workflow_id"].as_str().unwrap().to_owned();
    b.workflow_update(&id, json!({"nodes":nodes,"edges":edges}), None)
        .await
        .unwrap();
    b.workflow_publish(&id, 1, None).await.unwrap();
    id
}
async fn terminal(b: &JournalBackend, id: &str) -> flow_engine::journal_state::Run {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            let r = b.state().await.runs[id].clone();
            if r.terminal() {
                return r;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
#[ignore = "30-second mixed business acceptance"]
async fn sustained_mixed_business() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let temp = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(temp.path(), Default::default())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = calls.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tokio::spawn(async move {
                let mut buf = [0; 4096];
                let _ = s.read(&mut buf).await;
                s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}").await.unwrap();
            });
        }
    });
    let mut workflows = Vec::new();
    for (kind, params) in [
        ("http_call", json!({"url":format!("http://{addr}")})),
        ("script", json!({"code":"return {ok:true};"})),
        ("delay", json!({"ms":100})),
        ("human_task", json!({})),
        ("human_task", json!({})),
    ] {
        workflows.push(install(&b,json!([{"id":"s","type":"start"},{"id":"n","type":kind,"params":params},{"id":"e","type":"end"}]),json!([{"from":"s","to":"n"},{"from":"n","to":"e"}])).await);
    }
    b.start_execution_with_limit(16).await.unwrap();
    let start = Instant::now();
    let before = b.journal.stats();
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..120 {
        tokio::time::sleep_until((start + Duration::from_millis(250 * i)).into()).await;
        let b = b.clone();
        let w = workflows[(i % 5) as usize].clone();
        tasks.spawn(async move {
            let now = Instant::now();
            let receipt = b
                .run_start(
                    &w,
                    None,
                    json!({"index":i}),
                    "manual",
                    None,
                    Some(&format!("mixed-{i}")),
                )
                .await
                .unwrap();
            let latency = now.elapsed().as_micros();
            assert!(receipt.visible);
            let id = receipt.result["run_id"].as_str().unwrap();
            if i % 5 == 4 {
                b.run_cancel(id, Some("cancel")).await.unwrap();
            }
            if i % 5 == 3 {
                tokio::time::timeout(Duration::from_secs(60), async {
                    loop {
                        if b.state().await.runs[id]
                            .nodes
                            .get("n")
                            .is_some_and(|n| n.status == "waiting")
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .unwrap();
                b.run_signal(id, "n", json!({"ok":true}), Some("signal"))
                    .await
                    .unwrap();
            }
            let r = terminal(&b, id).await;
            assert_eq!(
                r.status,
                if i % 5 == 4 { "cancelled" } else { "succeeded" },
                "{:?}",
                r.error
            );
            latency
        });
    }
    let mut latency = Vec::new();
    while let Some(r) = tasks.join_next().await {
        latency.push(r.unwrap());
    }
    latency.sort_unstable();
    let p99 = latency[latency.len() * 99 / 100];
    let after = b.journal.stats();
    println!("mixed 120 runs, offered 4 runs/s for 30s, 20% HTTP/script/delay/signal/cancel each; p99={p99}us; elapsed={:?}; transactions={}; encoded_bytes={}; peak_active={}",start.elapsed(),after.transactions-before.transactions,after.encoded_bytes-before.encoded_bytes,b.execution_peak());
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 24);
    assert!(p99 <= 250_000, "projected p99 exceeds 250ms: {p99}us");
    assert!(b.execution_peak() <= 16);
    b.close().await.unwrap();
    server.abort();
}
#[tokio::test]
async fn observation_failure_and_flood_do_not_change_business_result() {
    for mode in ["corrupt", "deleted", "unwritable", "flood"] {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let initial = JournalBackend::open(root, Default::default())
            .await
            .unwrap();
        initial.close().await.unwrap();
        drop(initial);
        if mode == "corrupt" {
            std::fs::write(root.join("observations/manifest.json"), b"corrupt").unwrap();
            std::fs::write(root.join("observations/00000001.jsonl"), b"broken\npartial").unwrap();
        }
        let b = JournalBackend::open(root, Default::default())
            .await
            .unwrap();
        let w=install(&b,json!([{"id":"s","type":"start"},{"id":"n","type":"script","params":{"code":"for(let i=0;i<10000;i++) console.log('observation'); return {ok:true};"}},{"id":"e","type":"end"}]),json!([{"from":"s","to":"n"},{"from":"n","to":"e"}])).await;
        if mode == "deleted" {
            std::fs::remove_dir_all(root.join("observations")).unwrap();
        }
        if mode == "unwritable" {
            std::fs::create_dir(root.join("observations/00000001.jsonl")).unwrap();
        }
        b.start_execution().await.unwrap();
        let receipt = b
            .run_start(&w, None, json!({}), "manual", None, None)
            .await
            .unwrap();
        let r = terminal(&b, receipt.result["run_id"].as_str().unwrap()).await;
        assert_eq!(r.status, "succeeded", "mode={mode} {:?}", r.error);
        assert_eq!(
            r.output,
            Some(flow_journal::StoredValue::Inline(json!({"ok":true})))
        );
        let store = b.observations.as_ref().unwrap();
        store.flush();
        let loss = store.loss();
        assert!(
            loss.queue_dropped + loss.storage_dropped > 0 || loss.history_incomplete,
            "{mode}"
        );
        b.close().await.unwrap();
        drop(b);
        assert!(flow_journal::maintenance::verify(root)
            .unwrap()
            .fault
            .is_none());
    }
}

#[tokio::test]
async fn cancelled_fanout_tree_finishes_after_restart() {
    let temp = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(temp.path(), Default::default())
        .await
        .unwrap();
    let child = install(
        &b,
        json!([{"id":"s","type":"start"},{"id":"h","type":"human_task"},{"id":"e","type":"end"}]),
        json!([{"from":"s","to":"h"},{"from":"h","to":"e"}]),
    )
    .await;
    let mut nodes = vec![
        json!({"id":"s","type":"start"}),
        json!({"id":"e","type":"end"}),
    ];
    let mut edges = Vec::new();
    for i in 0..64 {
        let id = format!("c{i}");
        nodes.push(json!({"id":id,"type":"sub_workflow","params":{"workflow_id":child}}));
        edges.push(json!({"from":"s","to":id}));
        edges.push(json!({"from":id,"to":"e"}));
    }
    let parent = install(&b, json!(nodes), json!(edges)).await;
    let r = b
        .run_start(&parent, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let parent = r.result["run_id"].as_str().unwrap().to_owned();
    b.start_execution_with_limit(4).await.unwrap();
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if b.state().await.runs.len() == 65 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    b.run_cancel(&parent, Some("tree-cancel")).await.unwrap();
    b.close().await.unwrap();
    drop(b);
    let b = JournalBackend::open(temp.path(), Default::default())
        .await
        .unwrap();
    b.start_execution_with_limit(1).await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if b.state()
                .await
                .runs
                .values()
                .all(|r| r.status == "cancelled")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(b.state().await.runs.len(), 65);
    b.close().await.unwrap();
}

#[tokio::test]
#[ignore = "subprocess crash fixture"]
async fn committed_projection_crash_child() {
    let root = std::path::PathBuf::from(std::env::var("FLOW_CRASH_FIXTURE").unwrap());
    let b = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    b.pause_projection(true);
    assert!(matches!(
        b.workflow_create("crash", Some("original")).await,
        Err(flow_backend::journal::JournalError::CommittedNotVisible(_))
    ));
    std::process::exit(70); // deliberate abrupt exit: no backend close or Rust drops
}
#[tokio::test]
async fn abrupt_exit_after_commit_before_projection_recovers_original_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let status = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "committed_projection_crash_child"])
        .env("FLOW_CRASH_FIXTURE", temp.path())
        .status()
        .await
        .unwrap();
    assert_eq!(status.code(), Some(70));
    let b = JournalBackend::open(temp.path(), Default::default())
        .await
        .unwrap();
    let original = b
        .command_status("workflow.create", "original")
        .await
        .unwrap()
        .unwrap();
    let retry = b.workflow_create("crash", Some("original")).await.unwrap();
    assert_eq!(original.commit_cursor, retry.commit_cursor);
    assert!(retry.visible);
    assert_eq!(b.state().await.workflows.len(), 1);
    b.close().await.unwrap();
}

#[tokio::test]
async fn aggregate_predecessors_and_oversize_output_fail_within_budget() {
    let temp = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(temp.path(), Default::default())
        .await
        .unwrap();
    let w=install(&b,json!([
        {"id":"s","type":"start"},
        {"id":"a","type":"script","params":{"code":"return 'x'.repeat(5*1024*1024);"}},
        {"id":"b","type":"script","params":{"code":"return 'y'.repeat(5*1024*1024);"}},
        {"id":"j","type":"script","params":{"code":"throw new Error('USER_CODE_RAN');"}},
        {"id":"e","type":"end"}
    ]),json!([{"from":"s","to":"a"},{"from":"s","to":"b"},{"from":"a","to":"j"},{"from":"b","to":"j"},{"from":"j","to":"e"}])).await;
    b.start_execution_with_limit(2).await.unwrap();
    let receipt = b
        .run_start(&w, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let r = terminal(&b, receipt.result["run_id"].as_str().unwrap()).await;
    assert_eq!(r.status, "failed");
    assert_eq!(r.nodes["a"].status, "succeeded");
    assert_eq!(r.nodes["b"].status, "succeeded");
    let error = format!("{:?}", r.error);
    assert!(!error.contains("USER_CODE_RAN"), "{error}");
    assert!(
        error.contains("budget") || error.contains("aggregate"),
        "{error}"
    );
    let w=install(&b,json!([{"id":"s","type":"start"},{"id":"n","type":"script","params":{"code":"return 'x'.repeat(9*1024*1024);"}},{"id":"e","type":"end"}]),json!([{"from":"s","to":"n"},{"from":"n","to":"e"}])).await;
    let r = b
        .run_start(&w, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run = terminal(&b, r.result["run_id"].as_str().unwrap()).await;
    assert_eq!(run.status, "failed");
    println!(
        "resource boundaries rejected aggregate 10 MiB predecessors and 9 MiB output; error={:?}",
        run.error
    );
    b.close().await.unwrap();
}

#[tokio::test]
async fn cancel_authorized_http_before_response_never_advances_or_resends() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let temp = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(temp.path(), Default::default())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted, mut incoming) = tokio::sync::mpsc::channel(1);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buf = [0; 4096];
        let _ = socket.read(&mut buf).await;
        let (tx, rx) = tokio::sync::oneshot::channel();
        accepted.send(tx).await.unwrap();
        let _ = rx.await;
        let _ = socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            .await;
        // Any retry would open a second connection to this listener.
        tokio::time::timeout(Duration::from_millis(500), listener.accept())
            .await
            .is_ok()
    });
    let w=install(&b,json!([{"id":"s","type":"start"},{"id":"h","type":"http_call","params":{"url":format!("http://{addr}")}},{"id":"e","type":"end"}]),json!([{"from":"s","to":"h"},{"from":"h","to":"e"}])).await;
    b.start_execution().await.unwrap();
    let r = b
        .run_start(&w, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let id = r.result["run_id"].as_str().unwrap().to_owned();
    let release = tokio::time::timeout(Duration::from_secs(10), incoming.recv())
        .await
        .unwrap()
        .unwrap();
    b.run_cancel(&id, Some("cancel-authorized")).await.unwrap();
    let _ = release.send(());
    assert!(!server.await.unwrap());
    b.close().await.unwrap();
    drop(b);
    let b = JournalBackend::open(temp.path(), Default::default())
        .await
        .unwrap();
    b.start_execution().await.unwrap();
    let r = terminal(&b, &id).await;
    assert_eq!(r.status, "cancelled");
    assert!(!r.nodes.contains_key("e"));
    b.close().await.unwrap();
}

#[tokio::test]
#[ignore = "subprocess executor crash fixture"]
async fn authorized_crash_child() {
    let root = std::path::PathBuf::from(std::env::var("FLOW_CRASH_FIXTURE").unwrap());
    let b = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    b.start_execution().await.unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn sigkill_after_external_request_recovers_uncertain_without_resend() {
    use tokio::io::AsyncReadExt;
    let temp = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(temp.path(), Default::default())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let w=install(&b,json!([{"id":"s","type":"start"},{"id":"h","type":"http_call","params":{"url":format!("http://{addr}")}},{"id":"e","type":"end"}]),json!([{"from":"s","to":"h"},{"from":"h","to":"e"}])).await;
    let r = b
        .run_start(&w, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let id = r.result["run_id"].as_str().unwrap().to_owned();
    b.close().await.unwrap();
    drop(b);
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "authorized_crash_child"])
        .env("FLOW_CRASH_FIXTURE", temp.path())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(10), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut bytes = [0; 4096];
    assert!(socket.read(&mut bytes).await.unwrap() > 0);
    child.kill().await.unwrap();
    let status = child.wait().await.unwrap();
    assert!(!status.success());
    drop(socket);
    let b = JournalBackend::open(temp.path(), Default::default())
        .await
        .unwrap();
    b.start_execution().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let s = b.state().await;
            let n = &s.runs[&id].nodes["h"];
            if n.wait.as_ref().is_some_and(|w| w.kind == "uncertain") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), listener.accept())
            .await
            .is_err()
    );
    assert!(b.state().await.runs[&id].nodes["h"]
        .operation
        .as_ref()
        .unwrap()
        .outcome
        .is_none());
    b.close().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
#[ignore = "requires isolated 32 MiB FLOW_ENOSPC_VOLUME disk image"]
async fn enospc_observations_and_backup_target_do_not_damage_authority() {
    use std::io::Write;
    let volume = std::path::PathBuf::from(std::env::var("FLOW_ENOSPC_VOLUME").unwrap());
    assert_eq!(volume, std::path::Path::new("/tmp/flow-acceptance-volume"));
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    let b = JournalBackend::open(root, Default::default())
        .await
        .unwrap();
    b.close().await.unwrap();
    drop(b);
    std::fs::remove_dir_all(root.join("observations")).unwrap();
    let logs = volume.join("observations");
    std::fs::create_dir_all(&logs).unwrap();
    std::os::unix::fs::symlink(&logs, root.join("observations")).unwrap();
    let b = JournalBackend::open(root, Default::default())
        .await
        .unwrap();
    let w=install(&b,json!([{"id":"s","type":"start"},{"id":"n","type":"script","params":{"code":"for(let i=0;i<1000;i++) console.log('full'); return {ok:true};"}},{"id":"e","type":"end"}]),json!([{"from":"s","to":"n"},{"from":"n","to":"e"}])).await;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_ne!(
            std::fs::metadata(&volume).unwrap().dev(),
            std::fs::metadata(volume.parent().unwrap()).unwrap().dev(),
            "requires an isolated mounted filesystem"
        );
    }
    let fill = volume.join("fill-business.bin");
    let mut file = std::fs::File::create(&fill).unwrap();
    let block = vec![0; 1024 * 1024];
    let mut filled = 0;
    loop {
        assert!(
            filled < 40 * 1024 * 1024,
            "refuse to fill an oversized filesystem"
        );
        match file.write_all(&block) {
            Ok(()) => filled += block.len(),
            Err(e) => {
                assert_eq!(e.raw_os_error(), Some(28));
                break;
            }
        }
    }
    while file.write_all(&block[..4096]).is_ok() {
        filled += 4096;
        assert!(
            filled < 40 * 1024 * 1024,
            "refuse to fill an oversized filesystem"
        );
    }
    assert!(filled < 40 * 1024 * 1024);
    b.start_execution().await.unwrap();
    let r = b
        .run_start(
            &w,
            None,
            json!({"large":"x".repeat(100_000)}),
            "manual",
            None,
            None,
        )
        .await
        .unwrap();
    let run = terminal(&b, r.result["run_id"].as_str().unwrap()).await;
    assert_eq!(run.status, "succeeded");
    b.observations.as_ref().unwrap().flush();
    assert!(b.observations.as_ref().unwrap().loss().storage_dropped > 0);
    let upper = b.journal.durable_lsn();
    let target = volume.join("failed-backup");
    assert!(flow_journal::maintenance::backup(root, &target, upper).is_err());
    assert!(!target.join("backup-manifest.json").exists());
    assert_eq!(b.journal.durable_lsn(), upper);
    assert!(flow_journal::maintenance::verify(root)
        .unwrap()
        .fault
        .is_none());
    drop(file);
    std::fs::remove_file(fill).unwrap();
    b.close().await.unwrap();
    println!("actual ENOSPC isolated to observation/backup volume; successful authoritative output preserved");
}

#[tokio::test]
#[ignore = "large HTTP decoding expansion acceptance"]
async fn http_without_length_expansion_hits_output_budget_and_retains_outcome() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let temp = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(temp.path(), Default::default())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let _ = socket.read(&mut request).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let block = [0u8; 65536];
        for _ in 0..720 {
            socket.write_all(&block).await.unwrap();
        } // 45 MiB -> 270 MiB escaped text
    });
    let w=install(&b,json!([{"id":"s","type":"start"},{"id":"h","type":"http_call","params":{"url":format!("http://{addr}")}},{"id":"e","type":"end"}]),json!([{"from":"s","to":"h"},{"from":"h","to":"e"}])).await;
    let r = b
        .run_start(&w, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    b.start_execution_with_limit(1).await.unwrap();
    let run = terminal(&b, r.result["run_id"].as_str().unwrap()).await;
    server.await.unwrap();
    assert_eq!(run.status, "failed");
    assert!(
        run.error.as_ref().unwrap().contains("captured value size"),
        "{:?}",
        run.error
    );
    assert!(run.nodes["h"].output.is_none());
    assert!(run.nodes["h"].operation.as_ref().unwrap().outcome.is_some());
    println!("45 MiB no-length response -> 270 MiB escaping rejected at 256 MiB; raw outcome retained; incomplete output unpublished");
    b.close().await.unwrap();
}

#[tokio::test]
#[ignore = "16 active HTTP tasks plus JS memory acceptance"]
async fn sixteen_active_tasks_and_simultaneous_release_are_bounded() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let temp = tempfile::tempdir().unwrap();
    let b = JournalBackend::open(temp.path(), Default::default())
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (ready, wait) = tokio::sync::oneshot::channel();
    let (release, go) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut sockets = Vec::new();
        for _ in 0..16 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            let _ = socket.read(&mut buf).await;
            sockets.push(socket);
        }
        ready.send(()).unwrap();
        go.await.unwrap();
        let mut writes = tokio::task::JoinSet::new();
        for mut socket in sockets {
            writes.spawn(async move {
                let body = format!("\"{}\"", "x".repeat(1024 * 1024));
                socket
                    .write_all(
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await
                    .unwrap();
                socket.write_all(body.as_bytes()).await.unwrap();
            });
        }
        while let Some(r) = writes.join_next().await {
            r.unwrap();
        }
    });
    let w=install(&b,json!([{"id":"s","type":"start"},{"id":"h","type":"http_call","params":{"url":format!("http://{addr}"),"timeout_ms":60000}},{"id":"j","type":"script","params":{"code":"return {bytes:nodes.h.body.length};"}},{"id":"e","type":"end"}]),json!([{"from":"s","to":"h"},{"from":"h","to":"j"},{"from":"j","to":"e"}])).await;
    let mut ids = Vec::new();
    for _ in 0..16 {
        let r = b
            .run_start(&w, None, json!({}), "manual", None, None)
            .await
            .unwrap();
        ids.push(r.result["run_id"].as_str().unwrap().to_owned());
    }
    b.start_execution_with_limit(16).await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), wait)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(b.execution_peak(), 16);
    let start = Instant::now();
    release.send(()).unwrap();
    for id in ids {
        let r = terminal(&b, &id).await;
        assert_eq!(r.status, "succeeded", "{:?}", r.error);
        assert_eq!(
            r.output,
            Some(flow_journal::StoredValue::Inline(
                json!({"bytes":1024*1024})
            ))
        );
    }
    server.await.unwrap();
    println!("16 active HTTP tasks, 1 MiB responses then JS, all released together; drain={:?}; peak_active={}",start.elapsed(),b.execution_peak());
    assert!(start.elapsed() < Duration::from_secs(30));
    b.close().await.unwrap();
}
