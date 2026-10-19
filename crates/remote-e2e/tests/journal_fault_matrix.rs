//! T09 授权/结果提交故障矩阵：确定性检查点注入（`dispatch::fault`），
//! 在持久化前后、Permit 发送前后、ResultCommitted 前后断线/取消；
//! 用真实 HTTP 计数外部副作用，用 run 终态与 journal 事实核对结果。
//! 全部场景串行执行（注入注册表全局唯一，见 dispatch::fault 文档）。

use flow_agent::runtime::{serve_for_tests, AgentConfig};
use flow_backend::journal::JournalBackend;
use flow_engine::execution_protocol::transport::FrameTransport;
use flow_engine::journal_state::{Run, RunStatus};
use flow_journal::JournalOptions;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-fault-{}", uuid::Uuid::now_v7()))
}

fn remote_options() -> flow_backend::execution::remote::RemoteOptions {
    flow_backend::execution::remote::RemoteOptions {
        control_addr: "127.0.0.1:0".into(),
        ca_cert: "ca.pem".into(),
        server_cert: "c.pem".into(),
        server_key: "k.pem".into(),
        attach_timeout_ms: 10_000,
        wss_addr: None,
        wss_agents: Vec::new(),
    }
}

fn agent_config(agent_id: &str, slots: u32) -> AgentConfig {
    AgentConfig {
        agent_id: agent_id.into(),
        control_addr: "unused".into(),
        ca_cert: Default::default(),
        cert: Default::default(),
        key: Default::default(),
        sni: None,
        executor: flow_engine::execution_protocol::ExecutorInvocation {
            program: flow_test_support::io::executor_bin(),
        },
        slots,
        wss_url: None,
        token: None,
    }
}

type Handles = Arc<tokio::sync::Mutex<BTreeMap<String, flow_agent::pool::ExecutorHandle>>>;

use std::collections::BTreeMap;

/// 与 ops 套件同构的模拟 agent：attach_inprocess + serve_for_tests +
/// 真实执行器子进程池。返回会话关闭句柄（shutdown token）。
async fn spawn_agent(
    manager: &Arc<flow_backend::execution::remote::AgentManager>,
    agent_id: &str,
    slots: u32,
    handles: Handles,
) -> tokio_util::sync::CancellationToken {
    let (control_a, control_b) = tokio::io::duplex(1024 * 1024);
    manager
        .attach_inprocess(agent_id.to_string(), control_a)
        .await;
    let config = agent_config(agent_id, slots);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let session_shutdown = shutdown.clone();
    let agent_id = agent_id.to_string();
    tokio::spawn(async move {
        use flow_engine::execution_protocol::Message;
        let mut transport = FrameTransport::spawn(control_b, 128);
        let boot = flow_engine::execution_protocol::transport::fresh_boot_id();
        if transport
            .send(Message::AgentHello {
                version: flow_engine::execution_protocol::PROTOCOL_VERSION,
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
                Ok(message) => {
                    if let Message::AgentWelcome { .. } = message {
                        break message;
                    }
                }
                Err(_) => return,
            }
        };
        let Message::AgentWelcome {
            version: flow_engine::execution_protocol::PROTOCOL_VERSION,
            journal_id,
            master_epoch,
            link_session_id,
            ..
        } = welcome
        else {
            return;
        };
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
            session_shutdown,
        )
        .await;
    });
    shutdown
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
            let run = b.state().await.unwrap().runs[run_id].clone();
            if predicate(&run) {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition reached")
}

/// 外部操作端点：计数每个真实到达的 HTTP 请求（副作用计数），立即 200。
async fn counting_http(counter: Arc<AtomicUsize>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            counter.fetch_add(1, Ordering::SeqCst);
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut request = [0u8; 4096];
            let _ = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut request)).await;
            let _ = socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
                .await;
        }
    });
    format!("http://{addr}/op")
}

fn http_flow(url: &str) -> Value {
    json!({"nodes":[
        {"id":"s","type":"start"},
        {"id":"n","type":"http_call","params":{"url": url, "timeout_ms": 20000}},
        {"id":"e","type":"end"}],
        "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]})
}

async fn open_backend(
    root: &std::path::Path,
) -> (
    Arc<JournalBackend>,
    Arc<flow_backend::execution::remote::AgentManager>,
) {
    let backend = JournalBackend::open(&root.join("data"), JournalOptions::default())
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
    (backend, manager)
}

/// 场景 A（T09 矩阵）：授权持久化**前**断线 → 外部调用必须从未发生
/// （未持久化不得 HTTP），run 不得 Succeeded。
#[tokio::test]
async fn matrix_disconnect_before_auth_persist_blocks_http() {
    flow_backend::execution::dispatch::fault::clear();
    let root = temp();
    std::fs::create_dir_all(&root).unwrap();
    let (backend, manager) = open_backend(&root).await;
    let handles: Handles = Arc::new(tokio::sync::Mutex::new(Default::default()));
    let shutdown = spawn_agent(&manager, "agent-fa", 1, handles).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let counter = Arc::new(AtomicUsize::new(0));
    let url = counting_http(counter.clone()).await;
    let flow = install(&backend, http_flow(&url)).await;

    let mut gates = flow_backend::execution::dispatch::fault::register(&["auth_persist:before"]);
    gates
        .get_mut("auth_persist:before")
        .unwrap()
        .send_replace(true);

    let created = backend
        .run_start(&flow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    // 等执行器到达授权检查点（确定性：检查点 hold 住请求处理）。
    tokio::time::sleep(Duration::from_millis(1500)).await;
    // 断线（杀 agent 会话）后释放检查点，并主动取消 run（不依赖
    // 120s durable-wait 超时走到终态）。
    shutdown.cancel();
    gates
        .get_mut("auth_persist:before")
        .unwrap()
        .send_replace(false);
    backend.run_cancel(&run_id, None).await.unwrap();

    let run = until(&backend, &run_id, Run::terminal).await;
    assert_ne!(run.status, RunStatus::Succeeded, "断线于持久化前不得成功");
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "未持久化授权不得触发外部 HTTP 调用"
    );
    manager.shutdown().await;
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

/// 场景 B（T09 矩阵）：授权已持久化、Permit 未发送时取消 → 不发 Permit、
/// 外部调用不发生、run 取消且授权事实保留（不透明重跑）。
#[tokio::test]
async fn matrix_cancel_between_persist_and_permit_keeps_http_zero() {
    flow_backend::execution::dispatch::fault::clear();
    let root = temp();
    std::fs::create_dir_all(&root).unwrap();
    let (backend, manager) = open_backend(&root).await;
    let handles: Handles = Arc::new(tokio::sync::Mutex::new(Default::default()));
    let _shutdown = spawn_agent(&manager, "agent-fb", 1, handles).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let counter = Arc::new(AtomicUsize::new(0));
    let url = counting_http(counter.clone()).await;
    let flow = install(&backend, http_flow(&url)).await;

    let mut gates = flow_backend::execution::dispatch::fault::register(&["permit_send:before"]);
    gates
        .get_mut("permit_send:before")
        .unwrap()
        .send_replace(true);

    let created = backend
        .run_start(&flow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    // 等待到达 permit 发送前（授权已持久化）。
    tokio::time::sleep(Duration::from_millis(1500)).await;
    backend.run_cancel(&run_id, None).await.unwrap();
    gates
        .get_mut("permit_send:before")
        .unwrap()
        .send_replace(false);
    let run = until(&backend, &run_id, |r| r.terminal()).await;
    assert_eq!(run.status, RunStatus::Cancelled, "{:?}", run.error);
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "取消先于 Permit 的外部调用必须为 0"
    );
    manager.shutdown().await;
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

/// 场景 C（T09 矩阵）：ResultCommitted 发送**前**断线 → 外部调用恰好一次
/// （无重复），run 不因断线而误 Succeeded；权威事实只由持久回执推进。
#[tokio::test]
async fn matrix_disconnect_before_result_committed_no_duplicate_side_effect() {
    flow_backend::execution::dispatch::fault::clear();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "flow_backend=debug,flow_agent=info,flow_executor=info".into()),
        )
        .try_init();
    let root = temp();
    std::fs::create_dir_all(&root).unwrap();
    let (backend, manager) = open_backend(&root).await;
    // slots=2：start 节点的回执被 hold 时不阻塞 http 节点拿到自己的执行器。
    let handles: Handles = Arc::new(tokio::sync::Mutex::new(Default::default()));
    let shutdown = spawn_agent(&manager, "agent-fc", 2, handles).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let counter = Arc::new(AtomicUsize::new(0));
    let url = counting_http(counter.clone()).await;
    let flow = install(&backend, http_flow(&url)).await;

    let mut gates =
        flow_backend::execution::dispatch::fault::register(&["result_committed:before"]);
    gates
        .get_mut("result_committed:before")
        .unwrap()
        .send_replace(true);

    let created = backend
        .run_start(&flow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    // 外部调用已完成（HTTP 已计数），结果回执被 hold。
    let http_done = tokio::time::timeout(Duration::from_secs(20), async {
        while counter.load(Ordering::SeqCst) < 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let run_now = backend.state().await.unwrap().runs[&run_id].clone();
    assert!(
        http_done.is_ok(),
        "external call must have happened exactly once; run status={:?} nodes={:?}",
        run_now.status,
        run_now
            .nodes
            .iter()
            .map(|(id, node)| (id.clone(), node.status, node.error.clone()))
            .collect::<Vec<_>>()
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();
    gates
        .get_mut("result_committed:before")
        .unwrap()
        .send_replace(false);
    let run = until(&backend, &run_id, Run::terminal).await;
    // 断线于回执前：run 不得 Succeeded（权威成功只由持久路径推进），
    // 且外部副作用恰好一次（不得重跑）。
    assert_ne!(
        run.status,
        RunStatus::Succeeded,
        "receipt lost at link death must not report success"
    );
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "外部调用恰好一次，断线不得导致重复执行"
    );
    manager.shutdown().await;
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
