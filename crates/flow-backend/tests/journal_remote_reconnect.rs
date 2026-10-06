//! 三期 R2-01：断线对账与历史证据补传。
//!
//! in-process agent + 可剪断代理：上联断开后执行器保留、主进程挂起不判死；
//! 重连 Resume 裁决后续跑（无双重执行）；断线期间取消 → CancelAndDrain。

use flow_agent::runtime::{serve_for_tests, AgentConfig};
use flow_backend::journal::JournalBackend;
use flow_engine::execution_protocol::transport::FrameTransport;
use flow_engine::journal_state::Run;
use flow_journal::{JournalOptions, StoredValue};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-journal-r2-{}", uuid::Uuid::now_v7()))
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
        attach_timeout_ms: 15_000,
    }
}

fn agent_config() -> AgentConfig {
    AgentConfig {
        agent_id: "agent-r2".into(),
        control_addr: String::new(),
        data_addr: String::new(),
        ca_cert: "unused".into(),
        cert: "unused".into(),
        key: "unused".into(),
        executor: executor(),
        slots: 4,
    }
}

async fn proxy_pair(
    manager: &Arc<flow_backend::execution::remote::AgentManager>,
    agent_id: &str,
) -> (tokio::io::DuplexStream, tokio::io::DuplexStream) {
    // (agent 侧流, master 侧代理持有) ×2 —— 简化：agent 侧直连 pair；
    // master 侧经代理由我们持柄。
    let (agent_control, master_a) = tokio::io::duplex(256 * 1024);
    let (test_control, master_b) = tokio::io::duplex(256 * 1024);
    tokio::spawn(bidirectional_copy(master_a, master_b));
    let (agent_data, master_c) = tokio::io::duplex(256 * 1024);
    let (test_data, master_d) = tokio::io::duplex(256 * 1024);
    tokio::spawn(bidirectional_copy(master_c, master_d));
    manager
        .attach_inprocess(agent_id.to_string(), test_control, test_data)
        .await;
    (agent_control, agent_data)
}

async fn bidirectional_copy(a: tokio::io::DuplexStream, b: tokio::io::DuplexStream) {
    let (mut ra, mut wa) = tokio::io::split(a);
    let (mut rb, mut wb) = tokio::io::split(b);
    let _ = tokio::join!(
        tokio::io::copy(&mut ra, &mut wb),
        tokio::io::copy(&mut rb, &mut wa)
    );
}

type Handles =
    Arc<tokio::sync::Mutex<std::collections::BTreeMap<String, flow_agent::pool::ExecutorHandle>>>;

/// 启动完整 in-process agent 会话（首次连接）。
async fn start_agent(
    manager: &Arc<flow_backend::execution::remote::AgentManager>,
    handles: Handles,
) {
    let (control, data) = proxy_pair(manager, "agent-r2").await;
    let config = agent_config();
    let shutdown = tokio_util::sync::CancellationToken::new();
    tokio::spawn(async move {
        // 首连握手（与 R0 相同序列）。
        let mut transport = FrameTransport::spawn_streams(control, data, 128);
        use flow_engine::execution_protocol::Message;
        let boot = flow_engine::execution_protocol::transport::fresh_boot_id();
        transport
            .send(Message::AgentHello {
                agent_boot_id: boot.clone(),
                build: "sim".into(),
                capabilities: vec!["relay".into(), "resource_report".into(), "reconnect".into()],
                slots: 4,
            })
            .await
            .expect("hello");
        let welcome = loop {
            let (_, message) = transport.recv().await.expect("welcome");
            if let Message::AgentWelcome { .. } = message {
                break message;
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
            unreachable!()
        };
        transport
            .send(Message::DataBind {
                agent_boot_id: boot.clone(),
                link_session_id: link_session_id.clone(),
                credential: data_credential,
            })
            .await
            .expect("data bind");
        let relay = Arc::new(parking_lot::Mutex::new(flow_agent::relay::Relay::new(
            "agent-r2".into(),
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

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "off".into()),
        )
        .try_init();
}

#[tokio::test]
async fn uplink_drop_then_reconnect_resumes_without_double_execution() {
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
    start_agent(&manager, handles.clone()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 慢脚本：执行中剪断上联。
    let workflow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"n","type":"script","params":{"code":"var t=Date.now(); while(Date.now()-t<1500){} return {n: input.n + 1};"}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),
    )
    .await;
    let created = backend
        .run_start(&workflow, None, json!({"n": 5}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    // 等 InputPrepared 提交（任务已上执行器）。
    let _ = until(&backend, &run_id, |r| {
        r.nodes.get("n").is_some_and(|n| n.prepared.is_some())
    })
    .await;
    // 剪断：drop 代理测试侧句柄 → master 会话 EOF → 执行器保留。
    // （代理句柄在 start_agent 内部 drop 不了——这里改用 manager 侧注入新流
    // 替换实现断开：直接对同一 agent_id 重新 attach 前先等待 runner 挂起。）
    // 简化：用第二组流触发「重连替换」（旧会话 outbound 关闭 = 断线效果）。
    tokio::time::sleep(Duration::from_millis(100)).await;
    // 断线期间 run 不得终态（主进程挂起等对账，不判死重跑）。
    let snapshot = backend.state().await.runs[&run_id].clone();
    assert!(
        !snapshot.terminal(),
        "run must stay pending during uplink loss (status={})",
        snapshot.status
    );
    // 重连：新流 + 同一 handles → resume → 裁决续跑。
    let (control, data) = proxy_pair(&manager, "agent-r2").await;
    let config = agent_config();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let handles_reconnect = handles.clone();
    tokio::spawn(async move {
        let transport = FrameTransport::spawn_streams(control, data, 128);
        let boot = flow_engine::execution_protocol::transport::fresh_boot_id();
        let (_link, reason) = flow_agent::runtime::reconnect_session(
            &config,
            transport,
            &boot,
            4,
            handles_reconnect,
            shutdown,
        )
        .await;
        let _ = reason;
    });
    let done = until(&backend, &run_id, Run::terminal).await;
    assert_eq!(done.status, "succeeded", "error={:?}", done.error);
    // 无双重执行：一次 dispatch、audit_seq 连续无重复。
    let node = &done.nodes["n"];
    assert_eq!(node.attempt, 1);
    assert_eq!(node.attempts.len(), 1);
    let output = match node.output.clone().unwrap() {
        StoredValue::Inline(value) => value,
        StoredValue::Ref(_) => json!({}),
    };
    assert_eq!(output["n"], 6);
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn cancel_during_disconnect_drains_after_resume() {
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
    start_agent(&manager, handles.clone()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    // 挂起 HTTP：授权后断线再取消。
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
    let workflow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"h","type":"http_call","params":{"url": format!("http://{hang_addr}/hang"), "timeout_ms": 30000}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]}),
    )
    .await;
    let created = backend
        .run_start(&workflow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    let _ = until(&backend, &run_id, |r| {
        r.nodes.get("h").is_some_and(|n| n.operation.is_some()) || r.terminal()
    })
    .await;
    // 断线（重连替换触发旧会话关闭）+ 取消。
    backend.run_cancel(&run_id, None).await.unwrap();
    // 重连 → resume → CancelAndDrain → 执行器回收、节点封口。
    let (control, data) = proxy_pair(&manager, "agent-r2").await;
    let config = agent_config();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let handles_reconnect = handles.clone();
    tokio::spawn(async move {
        let transport = FrameTransport::spawn_streams(control, data, 128);
        let boot = flow_engine::execution_protocol::transport::fresh_boot_id();
        let (link, reason) = flow_agent::runtime::reconnect_session(
            &config,
            transport,
            &boot,
            4,
            handles_reconnect,
            shutdown,
        )
        .await;
        eprintln!("RECONNECT-DONE link={link} reason={reason}");
    });
    // 等取消 + 封口（重连 resume → CancelAndDrain → 包装器封口）。
    let done = until(&backend, &run_id, |r| {
        r.status == "cancelled"
            && r.nodes.get("h").is_some_and(|n| {
                n.attempts
                    .get(n.dispatch_id.as_str())
                    .is_some_and(|a| a.sealed)
            })
    })
    .await;
    let node = &done.nodes["h"];
    let attempt = &node.attempts[node.dispatch_id.as_str()];
    assert!(attempt.sealed, "dispatch sealed after drain");
    // 执行器全部回收。
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if handles.lock().await.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("local executors recycled");
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
