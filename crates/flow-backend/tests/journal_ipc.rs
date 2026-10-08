//! 二期 IPC 集成回归（I08/I10）：真实 flow-executor 子进程走通
//! 模板/script/condition/HTTP/delay/human/sub_workflow、取消排空、
//! 进程回收、重启恢复与一期语义等价。

use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::Run;
use flow_journal::{JournalOptions, StoredValue};
use serde_json::{json, Value};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-journal-ipc-{}", uuid::Uuid::now_v7()))
}

/// 定位真实执行器（I09 规则的测试版，独立二进制形态）。
fn executor_bin() -> std::path::PathBuf {
    flow_test_support::io::executor_bin()
}

/// 独立 flow-executor 二进制（无子命令前缀）。
fn executor() -> flow_engine::execution_protocol::contract::ExecutorInvocation {
    flow_engine::execution_protocol::contract::ExecutorInvocation::explicit(executor_bin())
}

/// 强制 IPC 模式（绕过环境变量，测试内显式指定二进制）。
async fn ipc_backend_explicit(root: &std::path::Path) -> Arc<JournalBackend> {
    ipc_backend_tagged(root, None).await
}

async fn ipc_backend_tagged(root: &std::path::Path, tag: Option<String>) -> Arc<JournalBackend> {
    ipc_backend_sized(root, tag, 2).await
}

async fn ipc_backend_sized(
    root: &std::path::Path,
    tag: Option<String>,
    x_max: usize,
) -> Arc<JournalBackend> {
    let backend = JournalBackend::open(root, JournalOptions::default())
        .await
        .unwrap();
    backend
        .start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
            flow_backend::execution::IpcOptions {
                executor: executor(),
                x_max,
                tag,
            },
        ))
        .await
        .unwrap();
    backend
}

async fn install(b: &JournalBackend, definition: Value) -> String {
    let created = b.workflow_create("test", None).await.unwrap();
    let id = created.result["workflow_id"].as_str().unwrap().to_string();
    b.workflow_update(&id, definition, None).await.unwrap();
    b.workflow_publish(&id, 1, None).await.unwrap();
    id
}

async fn until(b: &Arc<JournalBackend>, run_id: &str, predicate: impl Fn(&Run) -> bool) -> Run {
    tokio::time::timeout(Duration::from_secs(30), async {
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

async fn start_run(backend: &JournalBackend, workflow: &str, input: Value) -> String {
    let created = backend
        .run_start(workflow, None, input, "manual", None, None)
        .await
        .unwrap();
    created.result["run_id"].as_str().unwrap().to_string()
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "off".into()),
        )
        .try_init();
}

#[tokio::test]
async fn script_condition_and_templates_execute_in_subprocess() {
    init_tracing();
    let root = temp();
    let backend = ipc_backend_explicit(&root).await;
    let workflow = install(
        &backend,
        json!({
            "nodes": [
                {"id":"s","type":"start"},
                {"id":"a","type":"script","params":{"code":"console.log('from child'); return {n: input.n + 1, name: input.name};"}},
                {"id":"d","type":"delay","params":{"ms": "${input.ms}"}},
                {"id":"c","type":"condition","params":{"expr":"input.n >= 10"}},
                {"id":"e","type":"end"},
                {"id":"x","type":"end"}
            ],
            "edges": [
                {"from":"s","to":"a"},
                {"from":"a","to":"d"},
                {"from":"d","to":"c"},
                {"from":"c","to":"e","port":"true"},
                {"from":"c","to":"x","port":"false"}
            ]
        }),
    )
    .await;
    let run_id = start_run(
        &backend,
        &workflow,
        json!({"n": 41, "name": "flow", "ms": 40}),
    )
    .await;
    let done = until(&backend, &run_id, Run::terminal).await;
    if done.status != "succeeded" {
        let mut report = String::new();
        for (id, n) in &done.nodes {
            report.push_str(&format!(
                "  {id}: status={} error={:?} wait={:?}\n",
                n.status,
                n.error,
                n.wait.as_ref().map(|w| w.kind.clone())
            ));
        }
        panic!("run failed: {:?}\n{}", done.error, report);
    }
    // script 在子进程执行。
    let script_output = materialize(&backend, &done, "a").await;
    assert_eq!(script_output["n"], 42);
    assert_eq!(script_output["name"], "flow");
    assert_eq!(done.nodes["c"].branch, Some(true));
    // 模板在子进程展开：delay 的 ms 来自 ${input.ms}。
    match &done.nodes["d"].prepared.as_ref().unwrap().params {
        StoredValue::Inline(value) => assert_eq!(value["ms"], 40),
        StoredValue::Ref(_) => panic!("small params must be inline"),
    }
    // end 的直接前驱是 condition：单值直通。
    let output = materialize(&backend, &done, "e").await;
    assert_eq!(output, json!(true));
    assert!(done.nodes["x"].status == "skipped");
    // 观测经 data 通道落地（console 行）。
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

async fn materialize(backend: &JournalBackend, run: &Run, node_id: &str) -> Value {
    let output = run.nodes[node_id].output.clone().unwrap();
    match output {
        StoredValue::Inline(value) => value,
        StoredValue::Ref(reference) => {
            let root = backend.journal.root().to_path_buf();
            let upper = backend.journal.durable_lsn();
            tokio::task::spawn_blocking(move || {
                flow_journal::value::materialize(
                    &root,
                    upper,
                    &StoredValue::Ref(reference),
                    8 * 1024 * 1024,
                )
            })
            .await
            .unwrap()
            .unwrap()
        }
    }
}

#[tokio::test]
async fn http_call_requires_permit_and_persists_outcome_via_ipc() {
    init_tracing();
    let root = temp();
    let backend = ipc_backend_explicit(&root).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hits_server = hits.clone();
    let server = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut socket, _) = listener.accept().await.unwrap();
        hits_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut request = Vec::new();
        let mut buffer = [0; 4096];
        loop {
            let n = socket.read(&mut buffer).await.unwrap();
            if n == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..n]);
            if request.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let body = json!({"echo": "ok"});
        let payload = body.to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{payload}",
            payload.len(),
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    let workflow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"h","type":"http_call","params":{"url": format!("http://{addr}/echo"), "method": "POST", "body": {"q": 1}}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]}),
    )
    .await;
    let run_id = start_run(&backend, &workflow, json!({})).await;
    let done = until(&backend, &run_id, Run::terminal).await;
    assert_eq!(done.status, "succeeded");
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    let output = materialize(&backend, &done, "e").await;
    assert_eq!(output["status"], 200);
    assert_eq!(output["body"]["echo"], "ok");
    // 操作授权与 Outcome 都在 journal（Intent+Authorized 同事务）。
    let record = &done.nodes["h"];
    let operation = record.operation.as_ref().expect("operation committed");
    assert!(operation.outcome.is_some());
    server.await.unwrap();
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn delay_and_signal_waits_release_executor_slots() {
    let root = temp();
    let backend = ipc_backend_explicit(&root).await;
    let workflow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"d","type":"delay","params":{"ms": 300}},
            {"id":"h","type":"human_task","params":{"prompt": "decide"}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"d"},{"from":"d","to":"h"},{"from":"h","to":"e"}]}),
    )
    .await;
    let run_id = start_run(&backend, &workflow, json!({})).await;
    // delay 等待登记后不占执行进程；human_task 等信号。
    let waiting = until(&backend, &run_id, |r| {
        r.nodes.get("h").is_some_and(|n| n.status == "waiting") || r.terminal()
    })
    .await;
    if waiting.terminal() {
        let mut report = String::new();
        for (id, n) in &waiting.nodes {
            report.push_str(&format!(
                "  {id}: status={} error={:?} wait={:?}\n",
                n.status,
                n.error,
                n.wait.as_ref().map(|w| w.kind.clone())
            ));
        }
        panic!("run terminal early: {:?}\n{}", waiting.error, report);
    }
    assert_eq!(waiting.nodes["d"].status, "succeeded");
    assert!(waiting.nodes["d"].wait.is_none());
    backend
        .run_signal(&run_id, "h", json!({"decision": true}), Some("signal"))
        .await
        .unwrap();
    let done = until(&backend, &run_id, Run::terminal).await;
    assert_eq!(done.status, "succeeded");
    let output = materialize(&backend, &done, "e").await;
    assert_eq!(output["decision"], true);
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn sub_workflow_child_created_and_parent_completes() {
    let root = temp();
    let backend = ipc_backend_explicit(&root).await;
    let child = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"n","type":"script","params":{"code":"return {child: true, n: input.n * 2};"}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),
    )
    .await;
    let parent = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"w","type":"sub_workflow","params":{"workflow_id": child, "input_mapping": {"n": "${input.base}"}}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"w"},{"from":"w","to":"e"}]}),
    )
    .await;
    let run_id = start_run(&backend, &parent, json!({"base": 21})).await;
    let done = until(&backend, &run_id, Run::terminal).await;
    assert_eq!(done.status, "succeeded");
    let output = materialize(&backend, &done, "e").await;
    assert_eq!(output["child"], true);
    assert_eq!(output["n"], 42);
    // 子 run 完成且登记了父子关系。
    let state = backend.state().await;
    let children: Vec<&Run> = state
        .runs
        .values()
        .filter(|r| r.parent.as_ref().is_some_and(|p| p.run_id == run_id))
        .collect();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].status, "succeeded");
    drop(state);
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn cancel_during_http_marks_uncertain_and_never_resends() {
    let root = temp();
    let backend = ipc_backend_explicit(&root).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let hits_server = hits.clone();
    tokio::spawn(async move {
        // 接受连接但不响应，制造取消竞争窗口。
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            hits_server.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            std::mem::forget(socket); // 挂起连接直到测试结束
        }
    });
    let workflow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"h","type":"http_call","params":{"url": format!("http://{addr}/hang"), "timeout_ms": 10000}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]}),
    )
    .await;
    let run_id = start_run(&backend, &workflow, json!({})).await;
    // 等待授权已提交（请求已发出）再取消。
    let _authorized = until(&backend, &run_id, |r| {
        r.nodes.get("h").is_some_and(|n| n.operation.is_some())
    })
    .await;
    backend.run_cancel(&run_id, None).await.unwrap();
    let cancelled = until(&backend, &run_id, |r| {
        r.status == "cancelled"
            && r.nodes.get("h").is_some_and(|n| {
                n.attempts
                    .get(n.dispatch_id.as_str())
                    .is_some_and(|a| a.sealed)
            })
    })
    .await;
    // 取消胜出：外部操作进入 uncertain（授权已提交、Outcome 缺失；
    // 终态 run 上的 WaitRegistered 记录封口但不改状态，与一期一致）。
    let node = &cancelled.nodes["h"];
    assert!(node.operation.as_ref().unwrap().outcome.is_none());
    let attempt = &node.attempts[node.dispatch_id.as_str()];
    assert!(attempt.sealed, "dispatch sealed after cancel");
    let result = attempt.result.as_ref().expect("sealed result event");
    assert_eq!(
        result.kind,
        flow_journal::EventKind::WaitRegistered,
        "payload: {}",
        result.payload
    );
    assert_eq!(
        result.payload["wait"]["kind"], "uncertain",
        "payload: {}",
        result.payload
    );
    let sent_once = hits.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(sent_once, 1);
    // 重启不重发：uncertain 保持。
    backend.close().await.unwrap();
    drop(backend);
    let backend = ipc_backend_explicit(&root).await;
    let still = until(&backend, &run_id, |r| r.status == "cancelled").await;
    assert_eq!(
        still.nodes["h"].operation.as_ref().unwrap().outcome,
        None,
        "no re-request after restart"
    );
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn killing_executor_mid_script_recovers_with_new_process() {
    let root = temp();
    let tag = format!("ipc-kill-{}", uuid::Uuid::now_v7().simple());
    let backend = ipc_backend_tagged(&root, Some(tag.clone())).await;
    let workflow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"hang","type":"script","params":{"code":"var t=Date.now(); while(Date.now()-t<30000){} return 1;","timeout_ms": 30000}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"hang"},{"from":"hang","to":"e"}]}),
    )
    .await;
    let run_id = start_run(&backend, &workflow, json!({})).await;
    // 只杀本测试标记的执行器，避免并行测试误伤。
    let victim = wait_for_tagged_executor(&tag).await;
    unsafe {
        libc::kill(victim as i32, libc::SIGKILL);
    }
    let done = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let run = backend.state().await.runs[&run_id].clone();
            if run.terminal() {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let done = match done {
        Ok(run) => run,
        Err(_) => {
            let run = backend.state().await.runs[&run_id].clone();
            let mut report = String::new();
            for (id, n) in &run.nodes {
                report.push_str(&format!(
                    "  {id}: status={} error={:?} wait={:?} attempt={}\n",
                    n.status,
                    n.error,
                    n.wait.as_ref().map(|w| w.kind.clone()),
                    n.attempt
                ));
            }
            panic!(
                "run not terminal after kill: {} {:?}\n{}",
                run.status, run.error, report
            );
        }
    };
    assert_eq!(done.status, "failed");
    // 新派发在健康会话上继续（新进程）：立即可判定的脚本。
    let quick = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"n","type":"script","params":{"code":"throw new Error('quick');"}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),
    )
    .await;
    let run2 = start_run(&backend, &quick, json!({})).await;
    let done2 = until(&backend, &run2, Run::terminal).await;
    assert_eq!(done2.status, "failed");
    assert_eq!(done2.nodes["n"].status, "failed");
    backend.close().await.unwrap();
    // 无遗留：本测试标记的执行器全部回收。
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if tagged_executor_pids(&tag).is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("no orphan executor processes");
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restart_reuses_committed_preparation_without_reevaluation() {
    let root = temp();
    let backend = ipc_backend_explicit(&root).await;
    let workflow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"n","type":"script","params":{"code":"throw new Error('boom');","retry":{"max_attempts":2,"backoff_ms":100}}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),
    )
    .await;
    let run_id = start_run(&backend, &workflow, json!({})).await;
    let waiting = until(&backend, &run_id, |r| {
        r.nodes
            .get("n")
            .is_some_and(|n| n.wait.as_ref().is_some_and(|w| w.kind == "retry"))
    })
    .await;
    let prepared = waiting.nodes["n"].prepared.clone();
    backend.close().await.unwrap();
    drop(backend);
    let backend = ipc_backend_explicit(&root).await;
    let done = until(&backend, &run_id, Run::terminal).await;
    // attempt 1 重试、attempt 2 耗尽重试后终失败；准备输入跨进程复用。
    assert_eq!(done.status, "failed");
    assert_eq!(done.nodes["n"].attempt, 2);
    assert_eq!(done.nodes["n"].prepared, prepared, "committed input reused");
    assert_eq!(done.nodes["n"].attempts.len(), 2);
    let first = &done.nodes["n"].attempts.values().next().unwrap();
    assert!(first.result.is_some(), "first attempt sealed");
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn large_output_chunks_cross_result_barrier() {
    let root = temp();
    let backend = ipc_backend_explicit(&root).await;
    let workflow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"n","type":"script","params":{"code":"var a=[]; for (var i=0;i<60000;i++){a.push('chunky-' + i);} return {rows: a};"}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),
    )
    .await;
    let run_id = start_run(&backend, &workflow, json!({})).await;
    let done = until(&backend, &run_id, Run::terminal).await;
    if done.status != "succeeded" {
        let mut report = String::new();
        for (id, n) in &done.nodes {
            report.push_str(&format!(
                "  {id}: status={} error={:?}\n",
                n.status, n.error
            ));
        }
        panic!("large output run failed: {:?}\n{}", done.error, report);
    }
    let output = materialize(&backend, &done, "e").await;
    assert_eq!(output["rows"].as_array().unwrap().len(), 60000);
    // 输出经分块存储（Ref）且完整性校验通过。
    let node_output = done.nodes["n"].output.clone().unwrap();
    assert!(matches!(node_output, StoredValue::Ref(_)));
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn journal_events_equivalent_between_in_process_and_ipc() {
    let definition = json!({"nodes":[
        {"id":"s","type":"start"},
        {"id":"n","type":"script","params":{"code":"return {n: input.n + 1};"}},
        {"id":"d","type":"delay","params":{"ms": 50}},
        {"id":"e","type":"end"}],
        "edges":[{"from":"s","to":"n"},{"from":"n","to":"d"},{"from":"d","to":"e"}]});
    let root_a = temp();
    let backend_a = JournalBackend::open(&root_a, JournalOptions::default())
        .await
        .unwrap();
    backend_a.start_execution().await.unwrap();
    let workflow_a = install(&backend_a, definition.clone()).await;
    let run_a = start_run(&backend_a, &workflow_a, json!({"n": 1})).await;
    let done_a = until(&backend_a, &run_a, Run::terminal).await;
    assert_eq!(done_a.status, "succeeded");
    backend_a.close().await.unwrap();

    let root_b = temp();
    let backend_b = ipc_backend_explicit(&root_b).await;
    let workflow_b = install(&backend_b, definition).await;
    let run_b = start_run(&backend_b, &workflow_b, json!({"n": 1})).await;
    let done_b = until(&backend_b, &run_b, Run::terminal).await;
    assert_eq!(done_b.status, "succeeded");
    backend_b.close().await.unwrap();

    // 状态等价：每节点状态/输出/attempt/audit_seq/等待结构一致。
    for node_id in ["s", "n", "d", "e"] {
        let a = &done_a.nodes[node_id];
        let b = &done_b.nodes[node_id];
        assert_eq!(a.status, b.status, "{node_id} status");
        assert_eq!(a.output, b.output, "{node_id} output");
        assert_eq!(a.attempt, b.attempt, "{node_id} attempt");
        assert_eq!(a.branch, b.branch);
        assert_eq!(a.wait.is_none(), b.wait.is_none());
        assert!(
            a.attempts[a.dispatch_id.as_str()].audit_seq > 0,
            "{node_id} has audit facts"
        );
        assert_eq!(
            a.attempts[a.dispatch_id.as_str()].audit_seq,
            b.attempts[b.dispatch_id.as_str()].audit_seq,
            "{node_id} audit sequence parity"
        );
        assert_eq!(
            a.prepared.as_ref().map(|p| &p.params),
            b.prepared.as_ref().map(|p| &p.params),
            "{node_id} prepared params identical"
        );
    }
    std::fs::remove_dir_all(root_a).unwrap();
    std::fs::remove_dir_all(root_b).unwrap();
}

/// 等待并返回带指定标记的执行器 PID（测试隔离：只杀自己的进程）。
async fn wait_for_tagged_executor(tag: &str) -> u32 {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if let Some(pid) = tagged_executor_pids(tag).first().cloned() {
                return pid;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("tagged executor process appeared")
}

fn tagged_executor_pids(tag: &str) -> Vec<u32> {
    std::process::Command::new("pgrep")
        .arg("-f")
        .arg(format!("flow-executor --tag {tag}"))
        .output()
        .ok()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .filter_map(|l| l.trim().parse::<u32>().ok())
                .filter(|pid| *pid != std::process::id())
                .collect()
        })
        .unwrap_or_default()
}

/// I10 性能矩阵：IPC 模式混合业务负载（script/delay/http/cancel 各 1/4），
/// 采集：吞吐、per-run 完成延迟分位、主进程 RSS、执行器进程数与峰值 RSS。
/// 运行：scripts/release.sh cargo test --release -p flow-backend --test journal_ipc -- --ignored --nocapture ipc_mixed
#[tokio::test]
#[ignore = "perf: scripts/release.sh cargo test --release -- --ignored --nocapture"]
async fn ipc_mixed_business_matrix() {
    use std::sync::atomic::AtomicU64;
    let root = temp();
    let tag = format!("perf-{}", uuid::Uuid::now_v7().simple());
    // X_max=4（契约默认）；取消负载的 HTTP 设短超时，让执行器自行封口
    // （uncertain 结果）而不是依赖 5s Draining 宽限 kill。
    let backend = ipc_backend_sized(&root, Some(tag.clone()), 4).await;
    // 挂起 HTTP 服务器（cancel 负载用）。
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hang_addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            std::mem::forget(socket);
        }
    });
    // 正常 HTTP 服务器。
    let ok_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ok_addr = ok_listener.local_addr().unwrap();
    tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        loop {
            let Ok((mut socket, _)) = ok_listener.accept().await else {
                return;
            };
            let mut buffer = [0; 4096];
            let mut seen = Vec::new();
            loop {
                let n = match socket.read(&mut buffer).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                seen.extend_from_slice(&buffer[..n]);
                if seen.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let body = "{\"ok\":true}";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    let script_flow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"n","type":"script","params":{"code":"return {n: input.n + 1};"}},
            {"id":"d","type":"delay","params":{"ms": 10}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"n"},{"from":"n","to":"d"},{"from":"d","to":"e"}]}),
    )
    .await;
    let delay_flow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"d","type":"delay","params":{"ms": 50}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"d"},{"from":"d","to":"e"}]}),
    )
    .await;
    let http_flow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"h","type":"http_call","params":{"url": format!("http://{ok_addr}/ok")}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]}),
    )
    .await;
    let cancel_flow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"h","type":"http_call","params":{"url": format!("http://{hang_addr}/hang"), "timeout_ms": 400}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]}),
    )
    .await;

    let runs = 48u64;
    let started = std::time::Instant::now();
    let latencies = Arc::new(std::sync::Mutex::new(Vec::<f64>::new()));
    let terminal_count = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for i in 0..runs {
        let (flow, input, cancel) = match i % 4 {
            0 => (script_flow.clone(), json!({"n": i}), false),
            1 => (delay_flow.clone(), json!({}), false),
            2 => (http_flow.clone(), json!({}), false),
            _ => (cancel_flow.clone(), json!({}), true),
        };
        let backend = backend.clone();
        let latencies = latencies.clone();
        let terminal_count = terminal_count.clone();
        handles.push(tokio::spawn(async move {
            let t0 = std::time::Instant::now();
            let created = backend
                .run_start(&flow, None, input, "manual", None, None)
                .await
                .unwrap();
            let run_id = created.result["run_id"].as_str().unwrap().to_string();
            if cancel {
                // 等授权提交后取消（制造真实取消竞争窗口）。
                loop {
                    let run = backend.state().await.runs[&run_id].clone();
                    let authorized = run.nodes.get("h").is_some_and(|n| n.operation.is_some());
                    let terminal = run.terminal();
                    if authorized || terminal {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                let _ = backend.run_cancel(&run_id, None).await;
            }
            let done = match tokio::time::timeout(Duration::from_secs(120), async {
                loop {
                    let run = backend.state().await.runs[&run_id].clone();
                    if run.terminal() {
                        return run;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            {
                Ok(run) => run,
                Err(_) => {
                    let run = backend.state().await.runs[&run_id].clone();
                    let mut report = format!("stuck run status={}\n", run.status);
                    for (id, n) in &run.nodes {
                        report.push_str(&format!(
                            "  {id}: {} err={:?} wait={:?} attempt={}\n",
                            n.status,
                            n.error,
                            n.wait.as_ref().map(|w| w.kind.clone()),
                            n.attempt
                        ));
                    }
                    panic!("run {run_id} not terminal in 120s: {report}");
                }
            };
            terminal_count.fetch_add(1, Ordering::Relaxed);
            assert!(
                matches!(done.status.as_str(), "succeeded" | "cancelled"),
                "unexpected status {}",
                done.status
            );
            latencies
                .lock()
                .unwrap()
                .push(t0.elapsed().as_secs_f64() * 1000.0);
        }));
    }
    for handle in handles {
        handle.await.unwrap();
    }
    let elapsed = started.elapsed();
    let mut samples = latencies.lock().unwrap().clone();
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let pct = |p: f64| -> f64 {
        let idx = ((samples.len() as f64 - 1.0) * p).round() as usize;
        samples[idx.min(samples.len() - 1)]
    };
    let master_rss = rss_kib(std::process::id()).unwrap_or(0);
    let executor_pids = tagged_executor_pids(&tag);
    let mut executor_peak = 0u64;
    for pid in &executor_pids {
        if let Some(rss) = rss_kib(*pid) {
            executor_peak = executor_peak.max(rss);
        }
    }
    eprintln!("ipc mixed matrix (release):");
    eprintln!(
        "  runs={} terminal={}",
        runs,
        terminal_count.load(Ordering::Relaxed)
    );
    eprintln!(
        "  wall={:.3}s ({:.1} run/s)",
        elapsed.as_secs_f64(),
        runs as f64 / elapsed.as_secs_f64()
    );
    eprintln!(
        "  latency ms: p50={:.1} p99={:.1} max={:.1}",
        pct(0.50),
        pct(0.99),
        pct(1.0)
    );
    eprintln!(
        "  master RSS {} KiB; executors alive={} peak_rss {} KiB",
        master_rss,
        executor_pids.len(),
        executor_peak
    );
    assert_eq!(terminal_count.load(Ordering::Relaxed), runs);
    assert!(
        master_rss < 512 * 1024,
        "master RSS bounded: {master_rss} KiB"
    );
    assert!(
        executor_peak < 256 * 1024,
        "executor RSS bounded: {executor_peak} KiB"
    );
    backend.close().await.unwrap();
    // 回收无遗留。
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if tagged_executor_pids(&tag).is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("no orphan executor processes after load");
    std::fs::remove_dir_all(root).unwrap();
}

fn rss_kib(pid: u32) -> Option<u64> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u64>()
        .ok()
}

#[tokio::test]
async fn result_larger_than_audit_window_commits_all_chunks() {
    let root = tempfile::tempdir().unwrap();
    let backend = ipc_backend_explicit(root.path()).await;
    let workflow = install(
        &backend,
        json!({"nodes":[
        {"id":"s","type":"start"},
        {"id":"n","type":"script","params":{"code":"return 'x'.repeat(3 * 1024 * 1024);"}},
        {"id":"e","type":"end"}],"edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),
    )
    .await;
    let run_id = start_run(&backend, &workflow, json!(null)).await;
    let done = until(&backend, &run_id, Run::terminal).await;
    assert_eq!(done.status, "succeeded", "{:?}", done.error);
    let output = materialize(&backend, &done, "n").await;
    assert_eq!(output.as_str().unwrap(), "x".repeat(3 * 1024 * 1024));
    backend.close().await.unwrap();
}

#[tokio::test]
async fn oversized_http_capture_is_uncertain_without_resend() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let root = temp();
    let backend = ipc_backend_explicit(&root).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        let _ = socket.read(&mut request).await.unwrap();
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            flow_journal::MAX_VALUE_BYTES + 1
        );
        socket.write_all(head.as_bytes()).await.unwrap();
        // Keep the body stream open: rejection must follow the declared size,
        // rather than EOF or the request timeout. Keep the listener to detect retries.
        (socket, listener)
    });
    let flow = install(&backend, json!({"nodes":[{"id":"s","type":"start"},{"id":"h","type":"http_call","params":{"url":format!("http://{addr}"),"timeout_ms":10000}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]})).await;
    let id = start_run(&backend, &flow, json!({})).await;
    let (_socket, listener) = server.await.unwrap();
    let run = tokio::time::timeout(
        Duration::from_secs(2),
        until(&backend, &id, |run| {
            run.nodes
                .get("h")
                .and_then(|node| node.wait.as_ref())
                .is_some_and(|wait| wait.kind == "uncertain")
        }),
    )
    .await
    .expect("capture limit must be enforced before waiting for the response body");
    assert!(run.nodes["h"].operation.as_ref().unwrap().outcome.is_none());
    assert!(run.nodes["h"].output.is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(200), listener.accept())
            .await
            .is_err()
    );
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
