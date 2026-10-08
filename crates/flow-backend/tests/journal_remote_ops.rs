//! 三期 R3-01：远程运维与容量验收。
//!
//! - agent drain：停止接新派发、取消在飞、回报完成并退出上联。
//! - 多 agent 隔离：一台 agent 下线（drain）后其余机器继续承接任务。
//! - 容量准入：slots=1 时并发任务串行完成（不超配、不死锁）。

use flow_agent::runtime::{serve_for_tests, AgentConfig};
use flow_backend::journal::JournalBackend;
use flow_engine::execution_protocol::transport::FrameTransport;
use flow_engine::journal_state::Run;
use flow_journal::JournalOptions;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-journal-r3-{}", uuid::Uuid::now_v7()))
}

/// 合并二进制自召唤形态：同一个 `flow` 文件 + `executor` 子命令前缀。
fn executor() -> flow_engine::execution_protocol::contract::ExecutorInvocation {
    flow_engine::execution_protocol::contract::ExecutorInvocation::merged(
        flow_test_support::io::flow_bin(),
    )
}

fn remote_options() -> flow_backend::execution::remote::RemoteOptions {
    flow_backend::execution::remote::RemoteOptions {
        control_addr: "127.0.0.1:0".into(),
        data_addr: "127.0.0.1:0".into(),
        ca_cert: "unused".into(),
        server_cert: "unused".into(),
        server_key: "unused".into(),
        attach_timeout_ms: 5_000,
    }
}

type Handles =
    Arc<tokio::sync::Mutex<std::collections::BTreeMap<String, flow_agent::pool::ExecutorHandle>>>;

fn agent_config(agent_id: &str, slots: u32) -> AgentConfig {
    AgentConfig {
        agent_id: agent_id.into(),
        control_addr: String::new(),
        data_addr: String::new(),
        ca_cert: "unused".into(),
        cert: "unused".into(),
        key: "unused".into(),
        executor: executor(),
        slots,
    }
}

async fn spawn_agent(
    manager: &Arc<flow_backend::execution::remote::AgentManager>,
    agent_id: &str,
    slots: u32,
    handles: Handles,
) {
    let (control_a, control_b) = tokio::io::duplex(256 * 1024);
    let (data_a, data_b) = tokio::io::duplex(256 * 1024);
    manager
        .attach_inprocess(agent_id.to_string(), control_a, data_a)
        .await;
    let config = agent_config(agent_id, slots);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let agent_id = agent_id.to_string();
    tokio::spawn(async move {
        use flow_engine::execution_protocol::Message;
        let mut transport = FrameTransport::spawn_streams(control_b, data_b, 128);
        let boot = flow_engine::execution_protocol::transport::fresh_boot_id();
        if transport
            .send(Message::AgentHello {
                agent_boot_id: boot.clone(),
                build: "sim".into(),
                capabilities: vec!["relay".into(), "resource_report".into(), "reconnect".into()],
                slots,
            })
            .await
            .is_err()
        {
            return;
        }
        let welcome = loop {
            match transport.recv().await {
                Ok((_, message)) => {
                    if let Message::AgentWelcome { .. } = message {
                        break message;
                    }
                }
                Err(_) => return,
            }
        };
        let Message::AgentWelcome {
            journal_id,
            master_epoch,
            link_session_id,
            data_credential,
            ..
        } = welcome
        else {
            return;
        };
        if transport
            .send(Message::DataBind {
                agent_boot_id: boot.clone(),
                link_session_id: link_session_id.clone(),
                credential: data_credential,
            })
            .await
            .is_err()
        {
            return;
        }
        let relay = Arc::new(parking_lot::Mutex::new(flow_agent::relay::Relay::new(
            agent_id,
            boot,
            link_session_id,
        )));
        let _ = serve_for_tests(
            &config,
            &journal_id,
            master_epoch,
            transport,
            relay,
            handles,
            shutdown,
        )
        .await;
    });
}

async fn install(b: &JournalBackend, definition: Value) -> String {
    let created = b.workflow_create("test", None).await.unwrap();
    let id = created.result["workflow_id"].as_str().unwrap().to_string();
    b.workflow_update(&id, definition, None).await.unwrap();
    b.workflow_publish(&id, 1, None).await.unwrap();
    id
}

async fn until(b: &Arc<JournalBackend>, run_id: &str, predicate: impl Fn(&Run) -> bool) -> Run {
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let run = b.state().await.runs[run_id].clone();
            if predicate(&run) {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition reached")
}

const FLOW: &str = r#"{"nodes":[
    {"id":"s","type":"start"},
    {"id":"n","type":"script","params":{"code":"return {n: input.n + 1};"}},
    {"id":"e","type":"end"}],
    "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}"#;

async fn run_and_finish(backend: &Arc<JournalBackend>, flow: &str, n: i64) -> Run {
    let created = backend
        .run_start(flow, None, json!({"n": n}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    until(backend, &run_id, Run::terminal).await
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "off".into()),
        )
        .try_init();
}

#[tokio::test]
async fn slots_bound_enforces_serial_dispatch() {
    init_tracing();
    let root = temp();
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    let manager = flow_backend::execution::remote::AgentManager::new(
        backend.clone(),
        backend.journal.id().into(),
        1,
    );
    backend
        .start_execution_remote_manager(manager.clone(), remote_options())
        .await
        .unwrap();
    let handles: Handles = Arc::new(tokio::sync::Mutex::new(Default::default()));
    spawn_agent(&manager, "agent-cap", 1, handles).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (entered, mut entries) = tokio::sync::mpsc::channel(4);
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let server_release = release.clone();
    let server = tokio::spawn(async move {
        let mut jobs = tokio::task::JoinSet::new();
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let entered = entered.clone();
            let release = server_release.clone();
            jobs.spawn(async move {
                let mut request = [0; 4096];
                let _ = socket.read(&mut request).await.unwrap();
                entered.send(()).await.unwrap();
                release.acquire().await.unwrap().forget();
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                    )
                    .await
                    .unwrap();
            });
        }
        while let Some(result) = jobs.join_next().await {
            result.unwrap();
        }
    });
    let flow = install(&backend, json!({"nodes":[{"id":"s","type":"start"},{"id":"n","type":"http_call","params":{"url":format!("http://{addr}")}},{"id":"e","type":"end"}],"edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]})).await;
    let first = {
        let b = backend.clone();
        let f = flow.clone();
        tokio::spawn(async move { run_and_finish(&b, &f, 1).await })
    };
    let second = {
        let b = backend.clone();
        let f = flow.clone();
        tokio::spawn(async move { run_and_finish(&b, &f, 2).await })
    };
    tokio::time::timeout(Duration::from_secs(10), entries.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(200), entries.recv())
            .await
            .is_err(),
        "second external operation began before the first released its slot"
    );
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), entries.recv())
        .await
        .unwrap()
        .unwrap();
    release.add_permits(1);
    let (r1, r2) = (first.await.unwrap(), second.await.unwrap());
    assert_eq!(r1.status, "succeeded", "{:?}", r1.error);
    assert_eq!(r2.status, "succeeded", "{:?}", r2.error);
    server.await.unwrap();
    manager.shutdown().await;
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn one_agent_offline_others_continue() {
    let root = temp();
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    let manager = flow_backend::execution::remote::AgentManager::new(
        backend.clone(),
        backend.journal.id().into(),
        1,
    );
    backend
        .start_execution_remote_manager(manager.clone(), remote_options())
        .await
        .unwrap();
    let handles_a: Handles = Arc::new(tokio::sync::Mutex::new(Default::default()));
    let handles_b: Handles = Arc::new(tokio::sync::Mutex::new(Default::default()));
    spawn_agent(&manager, "agent-a", 2, handles_a.clone()).await;
    spawn_agent(&manager, "agent-b", 2, handles_b.clone()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let flow = install(&backend, serde_json::from_str(FLOW).unwrap()).await;
    // 基线：两台均可承接。
    let warm = run_and_finish(&backend, &flow, 10).await;
    assert_eq!(warm.status, "succeeded", "{:?}", warm.error);
    // agent-a 下线（drain：回收其执行器并退出上联）。
    manager.drain("agent-a", 1000).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if handles_a.lock().await.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("agent-a executors recycled");
    // agent-b 继续承接新任务（单机故障不拖死全局）。
    let after = run_and_finish(&backend, &flow, 20).await;
    assert_eq!(after.status, "succeeded", "{:?}", after.error);
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn drain_stops_new_dispatch_and_recycles() {
    let root = temp();
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    let manager = flow_backend::execution::remote::AgentManager::new(
        backend.clone(),
        backend.journal.id().into(),
        1,
    );
    backend
        .start_execution_remote_manager(manager.clone(), remote_options())
        .await
        .unwrap();
    let handles: Handles = Arc::new(tokio::sync::Mutex::new(Default::default()));
    spawn_agent(&manager, "agent-drain", 1, handles.clone()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    manager.drain("agent-drain", 1000).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if handles.lock().await.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("drain recycles local executors");
    // drain 后新派发明确失败（不静默挂起/不自动切机）。
    let flow = install(&backend, serde_json::from_str(FLOW).unwrap()).await;
    let done = run_and_finish(&backend, &flow, 1).await;
    assert_eq!(
        done.status, "failed",
        "no agent after drain must fail loudly"
    );
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
