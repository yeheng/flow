//! 桌面 app 启动预热：协调器就绪时预先拉起一个空闲执行器。
//!
//! 单独成文件（而非并入 `journal_ipc`）：后者的 `killing_executor_mid_script`
//! 依赖杀进程与派发之间的时序窗口，对同套件内并行用例的负载敏感；把预热
//! 用例放到独立测试二进制，避免新增调度扰动它的时序。

use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::{Run, RunStatus};
use flow_journal::JournalOptions;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-journal-prewarm-{}", uuid::Uuid::now_v7()))
}

fn executor() -> flow_engine::execution_protocol::contract::ExecutorInvocation {
    flow_engine::execution_protocol::contract::ExecutorInvocation::explicit(
        flow_test_support::io::executor_bin(),
    )
}

/// 预热形态：`prewarm=true` 时协调器在开始调度前拉起一个空闲执行器。
async fn backend_prewarmed(root: &std::path::Path, tag: &str) -> Arc<JournalBackend> {
    let backend = JournalBackend::open(root, JournalOptions::default())
        .await
        .unwrap();
    backend
        .start_execution_ipc_prewarm(
            flow_backend::execution::ExecutionMode::Ipc(flow_backend::execution::IpcOptions {
                executor: executor(),
                x_max: 2,
                tag: Some(tag.to_string()),
            }),
            true,
        )
        .await
        .unwrap();
    backend
}

async fn install(b: &JournalBackend, definition: Value) -> String {
    let created = b.workflow_create("prewarm", None).await.unwrap();
    let id = created.result["workflow_id"].as_str().unwrap().to_string();
    b.workflow_update(&id, definition, None).await.unwrap();
    b.workflow_publish(&id, 1, None).await.unwrap();
    id
}

async fn start_run(backend: &JournalBackend, workflow: &str, input: Value) -> String {
    let created = backend
        .run_start(workflow, None, input, "manual", None, None)
        .await
        .unwrap();
    created.result["run_id"].as_str().unwrap().to_string()
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
    .expect("condition timeout in until")
}

fn tagged_executor_pids(tag: &str) -> Vec<u32> {
    std::process::Command::new("pgrep")
        .arg("-f")
        .arg(format!("flow-executor.*--tag {tag}"))
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

/// 尚未派发任何 run 时预热执行器已就绪；首次 run 复用同一进程。
#[tokio::test]
async fn prewarm_spawns_idle_executor_and_first_run_reuses_it() {
    let root = temp();
    let tag = format!("prewarm-{}", uuid::Uuid::now_v7().simple());
    let backend = backend_prewarmed(&root, &tag).await;
    // 未启动任何 run：预热进程已经就绪，且只有一个。
    let warmed = wait_for_tagged_executor(&tag).await;
    assert_eq!(
        tagged_executor_pids(&tag),
        vec![warmed],
        "exactly one prewarmed executor (tag={tag})"
    );
    let workflow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"n","type":"script","params":{"code":"return 1;"}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),
    )
    .await;
    let run_id = start_run(&backend, &workflow, json!({})).await;
    let done = until(&backend, &run_id, Run::terminal).await;
    assert_eq!(done.status, RunStatus::Succeeded);
    // 首次派发复用预热进程，而不是再 spawn 一个（x_max 槽位未被超配）。
    assert_eq!(
        tagged_executor_pids(&tag),
        vec![warmed],
        "first run must reuse the prewarmed executor (tag={tag})"
    );
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
