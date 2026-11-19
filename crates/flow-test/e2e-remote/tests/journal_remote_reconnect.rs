//! 三期 R2-01：断线对账与历史证据补传。
//!
//! in-process agent + 可剪断代理：上联断开后执行器保留、主进程挂起不判死；
//! 重连 Resume 裁决后续跑（无双重执行）；断线期间取消 → CancelAndDrain。

use flow_agent::runtime::{serve_for_tests, AgentConfig};
use flow_backend::journal::JournalBackend;
use flow_engine::execution_protocol::{transport::FrameTransport, Message};
use flow_engine::journal_state::Run;
use flow_engine::journal_state::RunStatus;
use flow_journal::{JournalOptions, StoredValue};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-journal-r2-{}", uuid::Uuid::now_v7()))
}

/// 独立 flow-executor 二进制（agent 本机执行器池召唤它）。
fn executor() -> flow_engine::execution_protocol::contract::ExecutorInvocation {
    flow_engine::execution_protocol::contract::ExecutorInvocation::explicit(
        flow_test_support::io::executor_bin(),
    )
}

fn remote_options() -> flow_backend::execution::remote::RemoteOptions {
    flow_backend::execution::remote::RemoteOptions {
        control_addr: "127.0.0.1:0".into(),
        ca_cert: "unused".into(),
        server_cert: "unused".into(),
        server_key: "unused".into(),
        attach_timeout_ms: 15_000,
        wss_addr: None,
        wss_agents: Vec::new(),
    }
}

fn agent_config() -> AgentConfig {
    AgentConfig {
        agent_id: "agent-r2".into(),
        control_addr: String::new(),
        ca_cert: "unused".into(),
        cert: "unused".into(),
        key: "unused".into(),
        sni: None,
        executor: executor(),
        slots: 4,
        wss_url: None,
        token: None,
    }
}

async fn proxy_pair(
    manager: &Arc<flow_backend::execution::remote::AgentManager>,
    agent_id: &str,
) -> (tokio::io::DuplexStream, Vec<tokio::task::JoinHandle<()>>) {
    // agent 侧直连 pair；master 侧经代理由测试持柄（abort 代理即模拟断链）。
    let (agent_stream, master_a) = tokio::io::duplex(256 * 1024);
    let (test_stream, master_b) = tokio::io::duplex(256 * 1024);
    let proxy = tokio::spawn(bidirectional_copy(master_a, master_b));
    manager
        .attach_inprocess(agent_id.to_string(), test_stream)
        .await;
    (agent_stream, vec![proxy])
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
) -> (
    Vec<tokio::task::JoinHandle<()>>,
    tokio::task::JoinHandle<()>,
) {
    let (stream, proxies) = proxy_pair(manager, "agent-r2").await;
    (proxies, start_agent_on(stream, handles))
}

fn start_agent_on(
    stream: tokio::io::DuplexStream,
    handles: Handles,
) -> tokio::task::JoinHandle<()> {
    let config = agent_config();
    let shutdown = tokio_util::sync::CancellationToken::new();
    tokio::spawn(async move {
        // 首连握手（与 R0 相同序列；v4 单流无 DataBind）。
        let mut transport = FrameTransport::spawn(stream, 128);
        let boot = flow_engine::execution_protocol::transport::fresh_boot_id();
        transport
            .send(Message::AgentHello {
                version: flow_engine::execution_protocol::PROTOCOL_VERSION,
                agent_boot_id: boot.clone(),
                build: "sim".into(),
                capabilities: vec!["relay".into(), "resource_report".into(), "reconnect".into()],
                slots: 4,
            })
            .await
            .expect("hello");
        let welcome = loop {
            let message = transport.recv().await.expect("welcome");
            if let Message::AgentWelcome { .. } = message {
                break message;
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
            unreachable!()
        };
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
    })
}

async fn disconnect(
    proxies: Vec<tokio::task::JoinHandle<()>>,
    session: tokio::task::JoinHandle<()>,
) {
    for proxy in proxies {
        proxy.abort();
        let _ = proxy.await;
    }
    tokio::time::timeout(Duration::from_secs(3), session)
        .await
        .expect("old session observes actual EOF")
        .unwrap();
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

fn init_tracing() {
    static TEST_ENVIRONMENT: std::sync::Once = std::sync::Once::new();
    TEST_ENVIRONMENT.call_once(|| {
        std::env::set_var("FLOW_TEST_AUTO_GOVERN", "0");
        std::env::set_var("FLOW_PLUGINS_DIR", temp().join("plugins"));
        std::env::set_var("FLOW_SECRET_REMOTE_TOOL", "fixture-key");
    });
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
    let (proxies, session) = start_agent(&manager, handles.clone()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Legacy wall-clock script: recorded v2 Date.now intentionally does not advance.
    // This test exercises reconnection of the original execution contract.
    let workflow = install(
        &backend,
        json!({"semantics":1,"nodes":[
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
    disconnect(proxies, session).await;
    // 断线期间 run 不得终态（主进程挂起等对账，不判死重跑）。
    let snapshot = backend.state().await.unwrap().runs[&run_id].clone();
    assert!(
        !snapshot.terminal(),
        "run must stay pending during uplink loss (status={})",
        snapshot.status
    );
    // 重连：新流 + 同一 handles → resume → 裁决续跑。
    let (stream, _reconnected_proxies) = proxy_pair(&manager, "agent-r2").await;
    let config = agent_config();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let handles_reconnect = handles.clone();
    tokio::spawn(async move {
        let transport = FrameTransport::spawn(stream, 128);
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
    assert_eq!(done.status, RunStatus::Succeeded, "error={:?}", done.error);
    // 无双重执行：一次 dispatch、audit_seq 连续无重复。
    let node = &done.nodes["n"];
    assert_eq!(node.attempt, 1);
    assert_eq!(node.attempts.len(), 1);
    let output = match node.output.clone().unwrap() {
        StoredValue::Inline(value) => value,
        StoredValue::Ref(_) => json!({}),
    };
    assert_eq!(output["n"], 6);
    manager.shutdown().await;
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
    let (proxies, session) = start_agent(&manager, handles.clone()).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    // 挂起 HTTP：授权后断线再取消。
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hang_addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await;
    });
    let workflow = install(
        &backend,
        json!({"permissions":{"version":1,"mode":"bypass","allow":[]},"nodes":[
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
    disconnect(proxies, session).await;
    backend.run_cancel(&run_id, None).await.unwrap();
    // 重连 → resume → CancelAndDrain → 执行器回收、节点封口。
    let (stream, _reconnected_proxies) = proxy_pair(&manager, "agent-r2").await;
    let config = agent_config();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let handles_reconnect = handles.clone();
    tokio::spawn(async move {
        let transport = FrameTransport::spawn(stream, 128);
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
        r.status == RunStatus::Cancelled
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
    server.abort();
    manager.shutdown().await;
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
/// Apply a fault to the first tool reply after the master has queued it.
async fn tool_reply_proxy(
    manager: &Arc<flow_backend::execution::remote::AgentManager>,
    copies: usize,
) -> (
    tokio::io::DuplexStream,
    tokio::task::JoinHandle<()>,
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Receiver<Message>,
) {
    let (agent_stream, agent_proxy) = tokio::io::duplex(256 * 1024);
    let (master_stream, master_proxy) = tokio::io::duplex(256 * 1024);
    manager
        .attach_inprocess("agent-r2".into(), master_stream)
        .await;
    let (dropped, receipt) = tokio::sync::oneshot::channel();
    let (ready, registration) = tokio::sync::oneshot::channel();
    let proxy = tokio::spawn(async move {
        let mut agent = FrameTransport::spawn(agent_proxy, 128);
        let mut master = FrameTransport::spawn(master_proxy, 128);
        let mut ready = Some(ready);
        let mut observed = Some(dropped);
        loop {
            tokio::select! {
                frame = agent.recv() => {
                    let Ok(frame) = frame else { return };
                    if matches!(frame, Message::CapacityReport { .. }) {
                        if let Some(ready) = ready.take() {
                            let _ = ready.send(());
                        }
                    }
                    if master.send(frame).await.is_err() { return; }
                }
                frame = master.recv() => {
                    let Ok(frame) = frame else { return };
                    if let Message::Routed { inner, .. } = &frame {
                        if matches!(**inner, Message::ToolPermissionDecision { .. }) {
                            if let Some(observed) = observed.take() {
                                let _ = observed.send((**inner).clone());
                                for _ in 0..copies {
                                    if agent.send(frame.clone()).await.is_err() { return; }
                                }
                                if copies == 0 { return; }
                                continue;
                            }
                        }
                    }
                    if agent.send(frame).await.is_err() { return; }
                }
            }
        }
    });
    (agent_stream, proxy, registration, receipt)
}

#[cfg(unix)]
#[derive(Clone, Copy)]
enum ToolDisconnect {
    LoseAutoApproval,
    LoseManualApproval,
    LoseDenial,
    AwaitManualApproval,
    CancelAfterApproval,
}

#[cfg(unix)]
async fn tool_disconnect(scenario: ToolDisconnect) {
    use flow_backend::journal_permissions::ApprovalCommand;
    use flow_dto::permission::{ApprovalScope, Principal};
    use flow_engine::journal_state::ApprovalStatus;
    use flow_test_support::provider::{spawn_mock_openai, Turn};

    init_tracing();
    let _plugin = flow_test_support::io::agent_plugin_bin();
    let root = tempfile::tempdir().unwrap();
    let workdir = root.path().join("workspace");
    std::fs::create_dir(&workdir).unwrap();
    let counter = workdir.join("tool-count");
    let command = "printf x >> tool-count";
    let provider = spawn_mock_openai(vec![
        Turn::Tool {
            command: command.into(),
        },
        Turn::Final {
            text: "tool turn complete".into(),
        },
    ])
    .await;
    let backend = JournalBackend::open(&root.path().join("data"), JournalOptions::default())
        .await
        .unwrap();
    backend
        .install_harness_agent(Some(flow_dto::AgentRunConfig {
            base_url: provider.base_url(),
            model: "fixture-model".into(),
            ..Default::default()
        }))
        .unwrap();
    backend.ensure_bundled_grants().await;
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
    let (stream, proxy, ready, dropped) = tool_reply_proxy(&manager, 0).await;
    let session = start_agent_on(stream, handles.clone());
    tokio::time::timeout(Duration::from_secs(3), ready)
        .await
        .expect("agent reports capacity after handshake")
        .unwrap();

    let manual = matches!(
        scenario,
        ToolDisconnect::LoseManualApproval | ToolDisconnect::AwaitManualApproval
    );
    let mut allow = vec![
        json!({"id":"model","action":"llm_call","providers":[provider.base_url()],"models":["fixture-model"],"approval":"auto"}),
        json!({"id":"key","action":"secret","names":["REMOTE_TOOL"],"approval":"auto"}),
    ];
    if !matches!(scenario, ToolDisconnect::LoseDenial) {
        allow.push(json!({
            "id":"tool", "action":"shell",
            "script_digest":flow_dto::permission::sha256_hex(command.as_bytes()),
            "approval": if manual {"ask"} else {"auto"}
        }));
    }
    let workflow = install(
        &backend,
        json!({
            "permissions":{"version":1,"mode":"execute","allow":allow},
            "nodes":[
                {"id":"s","type":"start"},
                {"id":"a","type":"harness","params":{
                    "mode":"agent","prompt":"Run the fixture tool","api_key":"REMOTE_TOOL",
                    "workdir":workdir,"timeout_ms":60_000
                }},
                {"id":"e","type":"end"}
            ],
            "edges":[{"from":"s","to":"a"},{"from":"a","to":"e"}]
        }),
    )
    .await;
    let created = backend
        .run_start(&workflow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap();

    let approval = if manual {
        let run = until(&backend, run_id, |run| {
            run.terminal()
                || run
                    .nodes
                    .get("a")
                    .and_then(|node| node.approval.as_ref())
                    .is_some_and(|approval| {
                        approval.gate == "tool" && approval.status == ApprovalStatus::Pending
                    })
        })
        .await;
        assert!(
            !run.terminal(),
            "{:?}",
            run.nodes.get("a").and_then(|node| node.error.as_ref())
        );
        run.nodes["a"].approval.clone()
    } else {
        None
    };
    let approve = || async {
        let approval = approval.as_ref().unwrap();
        backend
            .run_decide(
                ApprovalCommand {
                    run_id,
                    node_id: "a",
                    operation_id: &approval.request.operation_id,
                    request_hash: &approval.request.request_hash,
                    allow: true,
                    scope: ApprovalScope::Once,
                    reason: None,
                    by: Principal::LocalAdmin,
                },
                "fixture-approve",
            )
            .await
            .unwrap();
    };

    if matches!(scenario, ToolDisconnect::AwaitManualApproval) {
        disconnect(vec![proxy], session).await;
        approve().await;
    } else {
        if manual {
            approve().await;
        }
        let reply = tokio::time::timeout(Duration::from_secs(12), dropped)
            .await
            .expect("tool decision must reach the proxy")
            .unwrap();
        assert!(
            matches!(reply, Message::ToolPermissionDecision { allowed, .. }
            if allowed != matches!(scenario, ToolDisconnect::LoseDenial))
        );
        proxy.await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), session)
            .await
            .expect("old link closes")
            .unwrap();
    }
    assert_eq!(
        provider.hits(),
        1,
        "tool response stayed on the disconnected link"
    );
    assert!(!counter.exists(), "tool ran before reconnect");
    if matches!(scenario, ToolDisconnect::CancelAfterApproval) {
        backend.run_cancel(run_id, None).await.unwrap();
    }

    let (stream, _proxies) = if matches!(scenario, ToolDisconnect::LoseAutoApproval) {
        let (stream, proxy, _, _) = tool_reply_proxy(&manager, 24).await;
        (stream, vec![proxy])
    } else {
        proxy_pair(&manager, "agent-r2").await
    };
    let shutdown = tokio_util::sync::CancellationToken::new();
    let reconnect_handles = handles.clone();
    let reconnect_shutdown = shutdown.clone();
    let reconnected = tokio::spawn(async move {
        let boot = flow_engine::execution_protocol::transport::fresh_boot_id();
        flow_agent::runtime::reconnect_session(
            &agent_config(),
            FrameTransport::spawn(stream, 128),
            &boot,
            4,
            reconnect_handles,
            reconnect_shutdown,
        )
        .await
    });
    let done = tokio::time::timeout(
        Duration::from_secs(6),
        until(&backend, run_id, |run| {
            run.terminal()
                && run
                    .nodes
                    .get("a")
                    .is_some_and(|node| node.attempts[&node.dispatch_id].sealed)
        }),
    )
    .await;
    shutdown.cancel();
    manager.shutdown().await;
    let _ = reconnected.await.unwrap();
    backend.close().await.unwrap();

    let done = done.expect("tool reply must be recovered on the new link");
    assert_eq!(done.nodes["a"].attempt, 1);
    if matches!(scenario, ToolDisconnect::CancelAfterApproval) {
        assert_eq!(done.status, RunStatus::Cancelled);
        assert_eq!(provider.hits(), 1);
        assert!(!counter.exists());
    } else {
        assert_eq!(done.status, RunStatus::Succeeded, "{:?}", done.error);
        assert_eq!(provider.hits(), 2);
        if matches!(scenario, ToolDisconnect::LoseDenial) {
            assert!(!counter.exists());
        } else {
            assert_eq!(std::fs::read(&counter).unwrap(), b"x");
        }
    }
}

#[tokio::test]
#[cfg(unix)]
async fn reconnect_replays_lost_auto_tool_approval() {
    tool_disconnect(ToolDisconnect::LoseAutoApproval).await;
}

#[tokio::test]
#[cfg(unix)]
async fn reconnect_replays_lost_manual_tool_approval() {
    tool_disconnect(ToolDisconnect::LoseManualApproval).await;
}

#[tokio::test]
#[cfg(unix)]
async fn reconnect_replays_lost_tool_denial() {
    tool_disconnect(ToolDisconnect::LoseDenial).await;
}

#[tokio::test]
#[cfg(unix)]
async fn reconnect_delivers_tool_approval_committed_while_disconnected() {
    tool_disconnect(ToolDisconnect::AwaitManualApproval).await;
}

#[tokio::test]
#[cfg(unix)]
async fn reconnect_cancellation_precedes_lost_tool_approval() {
    tool_disconnect(ToolDisconnect::CancelAfterApproval).await;
}
