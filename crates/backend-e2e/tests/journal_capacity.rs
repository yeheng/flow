//! Explicit acceptance load, not part of the short regression suite.
use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::{NodeStatus, RunStatus};
use serde_json::json;
use std::time::{Duration, Instant};

#[tokio::test]
#[ignore = "1000-run phase-one acceptance load"]
async fn thousand_waiting_runs_release_slots_and_recover_bounded_dispatch() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("journal");
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let created = backend.workflow_create("capacity", None).await.unwrap();
    let workflow = created.result["workflow_id"].as_str().unwrap();
    backend.workflow_update(workflow,json!({"nodes":[{"id":"s","type":"start"},{"id":"h","type":"human_task"},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]}),None).await.unwrap();
    backend.workflow_publish(workflow, 1, None).await.unwrap();
    let mut runs = Vec::new();
    let mut latency = Vec::new();
    let start = Instant::now();
    for i in 0..1000 {
        let now = Instant::now();
        let receipt = backend
            .run_start(
                workflow,
                None,
                json!({"index":i}),
                "manual",
                None,
                Some(&format!("capacity-{i}")),
            )
            .await
            .unwrap();
        assert!(receipt.visible);
        latency.push(now.elapsed().as_micros());
        runs.push(receipt.result["run_id"].as_str().unwrap().to_owned());
    }
    assert!(backend
        .run_start(workflow, None, json!({}), "manual", None, None)
        .await
        .is_err());
    latency.sort_unstable();
    assert!(latency[990] < 250_000, "projected p99 us={}", latency[990]);
    start_ipc(&backend, 2).await;
    tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            if backend.state().await.unwrap().runs.values().all(|r| {
                r.nodes
                    .get("h")
                    .is_some_and(|n| n.status == NodeStatus::Waiting)
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert!(backend.execution_peak() <= 2);
    let elapsed = start.elapsed();
    backend.close().await.unwrap();
    drop(backend);
    let ready = temp.path().join("crash-ready");
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "capacity_crash_child"])
        .env("FLOW_CAPACITY_CRASH_ROOT", &root)
        .env("FLOW_CAPACITY_READY", &ready)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        while !ready.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    child.kill().await.unwrap();
    assert!(!child.wait().await.unwrap().success());
    let restart = Instant::now();
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    assert_eq!(backend.state().await.unwrap().runs.len(), 1000);
    assert!(restart.elapsed() < Duration::from_secs(60));
    start_ipc(&backend, 1).await;
    for run in &runs {
        backend
            .run_cancel(run, Some("capacity-cancel"))
            .await
            .unwrap();
    }
    assert!(backend
        .state()
        .await
        .unwrap()
        .runs
        .values()
        .all(|r| r.status == RunStatus::Cancelled));
    println!("1000 in-flight, A_max=2 peak; create projected p99={} us; create+wait={elapsed:?}; recovery+cancel={:?}",latency[990],restart.elapsed());
    backend.close().await.unwrap();
}

#[tokio::test]
#[ignore = "1 GiB original values / long-history acceptance load"]
async fn gib_history_checkpoint_and_streamed_verify_remain_bounded() {
    use tokio::io::AsyncReadExt;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("journal");
    let journal = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let journal = journal.clone();
        tasks.spawn(async move {
            let mut input = tokio::io::repeat(42).take(128 * 1024 * 1024);
            journal
                .store_stream(&mut input, flow_journal::value::ValueCodec::Bytes)
                .await
                .unwrap()
        });
    }
    let mut values = Vec::new();
    while let Some(value) = tasks.join_next().await {
        values.push(value.unwrap());
    }
    journal.close().await.unwrap();
    drop(journal);
    let full = Instant::now();
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let full_elapsed = full.elapsed();
    assert!(
        full_elapsed < Duration::from_secs(60),
        "full recovery {full_elapsed:?}"
    );
    let upper = backend.journal.durable_lsn();
    backend.close().await.unwrap();
    drop(backend);
    let fast = Instant::now();
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    let fast_elapsed = fast.elapsed();
    assert!(
        fast_elapsed < Duration::from_secs(60),
        "checkpoint recovery {fast_elapsed:?}"
    );
    for value in &values {
        flow_journal::value::read_value(
            &root,
            backend.journal.durable_lsn(),
            value,
            true,
            &mut std::io::sink(),
        )
        .unwrap();
    }
    // Read a complete 128 MiB object into a counting/validation sink, never a Vec.
    struct Verify(u64);
    impl std::io::Write for Verify {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            assert!(bytes.iter().all(|b| *b == 42));
            self.0 += bytes.len() as u64;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Verify(0);
    flow_journal::value::read_value(&root, upper, &values[0], true, &mut output).unwrap();
    assert_eq!(output.0, 128 * 1024 * 1024);
    println!("1 GiB original values; full recovery={full_elapsed:?}; checkpoint recovery={fast_elapsed:?}; streamed 128 MiB verified");
    backend.close().await.unwrap();
}

#[tokio::test]
#[ignore = "1000-run subprocess crash fixture"]
async fn capacity_crash_child() {
    let root = std::path::PathBuf::from(std::env::var("FLOW_CAPACITY_CRASH_ROOT").unwrap());
    let backend = JournalBackend::open(&root, Default::default())
        .await
        .unwrap();
    assert_eq!(backend.state().await.unwrap().runs.len(), 1000);
    std::fs::write(std::env::var("FLOW_CAPACITY_READY").unwrap(), b"loaded").unwrap();
    std::future::pending::<()>().await;
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
