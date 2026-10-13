//! 三期 R1-01：flow-agent 双 TLS（mTLS）与资源准入的网络集成回归。
//!
//! 真实 flow-agent 二进制进程 + 真实 TLS 监听 + 真实执行器子进程：
//! 端到端业务（script/HTTP）、过时 v2 帧拒绝、单任务取消不关上联。

use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::Run;
use flow_journal::{JournalOptions, StoredValue};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use flow_engine::journal_state::{RunStatus};

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-journal-r1-{}", uuid::Uuid::now_v7()))
}

fn agent_bin() -> std::path::PathBuf {
    flow_test_support::io::agent_bin()
}

/// 生成测试 PKI：CA + server(flow-server) + agent(CN=agent_id)。
fn generate_pki(
    dir: &std::path::Path,
    agent_cn: &str,
) -> (
    std::path::PathBuf,
    std::path::PathBuf,
    std::path::PathBuf,
    std::path::PathBuf,
    std::path::PathBuf,
) {
    use rcgen::{CertificateParams, KeyPair};
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let server_key = KeyPair::generate().unwrap();
    let mut server_params = CertificateParams::new(vec!["flow-server".to_string()]).unwrap();
    server_params.distinguished_name.push(
        rcgen::DnType::CommonName,
        rcgen::DnValue::Utf8String("flow-server".into()),
    );
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .unwrap();

    let agent_key = KeyPair::generate().unwrap();
    let mut agent_params = CertificateParams::new(vec![]).unwrap();
    agent_params.distinguished_name.push(
        rcgen::DnType::CommonName,
        rcgen::DnValue::Utf8String(agent_cn.into()),
    );
    let agent_cert = agent_params
        .signed_by(&agent_key, &ca_cert, &ca_key)
        .unwrap();

    let write = |name: &str, content: String| {
        let path = dir.join(name);
        std::fs::write(&path, content).unwrap();
        path
    };
    (
        write("ca.pem", ca_cert.pem()),
        write("server.pem", server_cert.pem()),
        write("server.key", server_key.serialize_pem()),
        write("agent.pem", agent_cert.pem()),
        write("agent.key", agent_key.serialize_pem()),
    )
}

fn remote_options(
    dir: &std::path::Path,
    control: u16,
) -> flow_backend::execution::remote::RemoteOptions {
    flow_backend::execution::remote::RemoteOptions {
        control_addr: format!("127.0.0.1:{control}"),
        ca_cert: dir.join("ca.pem"),
        server_cert: dir.join("server.pem"),
        server_key: dir.join("server.key"),
        attach_timeout_ms: 10_000,
        wss_addr: None,
        agent_token: None,
    }
}

fn free_ports(n: usize) -> Vec<u16> {
    (0..n)
        .map(|_| {
            std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        })
        .collect()
}

struct AgentProcess {
    child: std::process::Child,
    stderr: Option<std::process::ChildStderr>,
}

impl AgentProcess {
    /// 终止 agent 并读取积累的 stderr（诊断；需先 kill 才有 EOF）。
    fn dump_stderr(&mut self) -> String {
        use std::io::Read;
        let _ = self.child.kill();
        let _ = self.child.wait();
        let mut out = String::new();
        if let Some(stderr) = self.stderr.as_mut() {
            let _ = stderr.read_to_string(&mut out);
        }
        out
    }
}

#[allow(dead_code)]
fn unused_guard() {}

impl Drop for AgentProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_agent(
    dir: &std::path::Path,
    control: u16,
    agent_cn: &str,
    slots: u32,
) -> AgentProcess {
    let child = std::process::Command::new(agent_bin())
        .args([
            "--control-addr",
            &format!("127.0.0.1:{control}"),
            "--agent-id",
            agent_cn,
            "--ca-cert",
        ])
        .arg(dir.join("ca.pem"))
        .arg("--cert")
        .arg(dir.join("agent.pem"))
        .arg("--key")
        .arg(dir.join("agent.key"))
        .arg("--slots")
        .arg(slots.to_string())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn flow-agent");
    let mut child = child;
    let stderr = child.stderr.take();
    AgentProcess { child, stderr }
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
    .unwrap()
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "off".into()),
        )
        .try_init();
}

#[tokio::test]
async fn tls_agent_end_to_end_business() {
    init_tracing();
    let dir = temp();
    std::fs::create_dir_all(&dir).unwrap();
    let (ca, _sc, _sk, _ac, _ak) = generate_pki(&dir, "agent-tls-1");
    let _ = ca;
    let ports = free_ports(1);
    let backend = JournalBackend::open(&dir.join("data"), JournalOptions::default())
        .await
        .unwrap();
    backend
        .start_execution_remote(remote_options(&dir, ports[0]))
        .await
        .unwrap();
    // 确认监听可达。
    for port in &ports[..1] {
        let probe = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await;
        assert!(probe.is_ok(), "listener {port} must be reachable");
    }
    let mut _agent = spawn_agent(&dir, ports[0], "agent-tls-1", 4);
    // 等会话注册。
    tokio::time::sleep(Duration::from_millis(500)).await;
    let workflow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"n","type":"script","params":{"code":"console.log('remote'); return {n: input.n * 2};"}},
            {"id":"d","type":"delay","params":{"ms": 10}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"n"},{"from":"n","to":"d"},{"from":"d","to":"e"}]}),
    )
    .await;
    let created = backend
        .run_start(&workflow, None, json!({"n": 21}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    let done = until(&backend, &run_id, Run::terminal).await;
    if done.status != RunStatus::Succeeded {
        panic!(
            "tls run failed: {:?}; agent stderr:\n{}",
            done.error,
            _agent.dump_stderr()
        );
    }
    let output = match done.nodes["e"].output.clone().unwrap() {
        StoredValue::Inline(value) => value,
        StoredValue::Ref(reference) => flow_journal::value::materialize(
            backend.journal.root(),
            backend.journal.durable_lsn(),
            &StoredValue::Ref(reference),
            8 * 1024 * 1024,
        )
        .unwrap(),
    };
    assert_eq!(output["slept_ms"], 10);
    // script 输出经中继进入权威日志。
    match done.nodes["n"].output.clone().unwrap() {
        StoredValue::Inline(value) => assert_eq!(value["n"], 42),
        StoredValue::Ref(reference) => {
            let root = backend.journal.root().to_path_buf();
            let upper = backend.journal.durable_lsn();
            let value = tokio::task::spawn_blocking(move || {
                flow_journal::value::materialize(
                    &root,
                    upper,
                    &StoredValue::Ref(reference),
                    8 * 1024 * 1024,
                )
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(value["n"], 42);
        }
    }
    backend.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

/// 构造 v3 单流裸帧（control 标签）——测试注入用。
fn raw_frame(json: &Value) -> Vec<u8> {
    let bytes = serde_json::to_vec(json).unwrap();
    let mut frame = Vec::with_capacity(bytes.len() + 5);
    frame.extend_from_slice(&((bytes.len() + 1) as u32).to_be_bytes());
    frame.push(0u8); // control 标签
    frame.extend_from_slice(&bytes);
    frame
}

#[tokio::test]
async fn unexpected_session_frame_rejected_and_single_cancel_keeps_uplink() {
    // 会话级协议校验：v3 单流上联已无 DataBind（v2 遗留，目录外类型）；
    // 握手后注入过时的 DataBind 裸帧必须整会话拒绝（不能静默容忍）。
    // 走裸帧注入而不是 Message 构造：目录里已没有这个变体，拒绝路径
    // 恰恰是 from_envelope 的 UnknownMessage。
    use flow_engine::execution_protocol::contract::PROTOCOL_VERSION;
    use flow_engine::execution_protocol::frame::FrameDecoder;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = temp();
    std::fs::create_dir_all(&dir).unwrap();
    let backend = JournalBackend::open(&dir.join("data"), JournalOptions::default())
        .await
        .unwrap();
    let manager = flow_backend::execution::remote::AgentManager::new(
        backend.clone(),
        backend.journal.id().into(),
        1,
    );
    let (control_a, mut client) = tokio::io::duplex(64 * 1024);
    manager
        .attach_inprocess("agent-bad".to_string(), control_a)
        .await;
    // AgentHello 裸帧（与 flow-agent 上联同格式：v3 标签帧）。
    let hello = json!({
        "v": PROTOCOL_VERSION, "type": "AgentHello",
        "body": {
            "agent_boot_id": "boot-bad", "build": "t",
            "capabilities": ["relay", "resource_report", "reconnect"],
            "slots": "1"
        }
    });
    client.write_all(&raw_frame(&hello)).await.unwrap();
    // 读 AgentWelcome（裸帧解码）。
    let mut decoder = FrameDecoder::new_tagged();
    let welcome = loop {
        let mut chunk = [0u8; 16 * 1024];
        let n = client.read(&mut chunk).await.unwrap();
        assert!(n > 0, "stream closed before welcome");
        decoder.feed(&chunk[..n]).unwrap();
        if let Some((_, body)) = decoder.pop_tagged_frame().unwrap() {
            break serde_json::from_slice::<Value>(&body).unwrap();
        }
    };
    assert_eq!(welcome["type"], "AgentWelcome");
    // 过时的 DataBind（v2 双流遗留、v3 目录外）→ UnknownMessage → 会话关闭。
    let stale = json!({
        "v": PROTOCOL_VERSION, "type": "DataBind",
        "body": {
            "agent_boot_id": "boot-bad",
            "link_session_id": welcome["body"]["link_session_id"],
            "credential": "wrong-credential"
        }
    });
    client.write_all(&raw_frame(&stale)).await.unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut chunk = [0u8; 16 * 1024];
            if client.read(&mut chunk).await.unwrap_or(0) == 0 {
                return;
            }
        }
    })
    .await;
    assert!(closed.is_ok(), "unexpected session frame must close session");
    drop(client);
    backend.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn single_task_cancel_keeps_agent_uplink_alive() {
    let dir = temp();
    std::fs::create_dir_all(&dir).unwrap();
    generate_pki(&dir, "agent-cancel");
    let ports = free_ports(1);
    let backend = JournalBackend::open(&dir.join("data"), JournalOptions::default())
        .await
        .unwrap();
    backend
        .start_execution_remote(remote_options(&dir, ports[0]))
        .await
        .unwrap();
    let agent = spawn_agent(&dir, ports[0], "agent-cancel", 4);
    tokio::time::sleep(Duration::from_millis(500)).await;
    // 挂起 HTTP：取消第一个 run 的单个任务。
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
    let hang_flow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"h","type":"http_call","params":{"url": format!("http://{hang_addr}/hang"), "timeout_ms": 500}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]}),
    )
    .await;
    let created = backend
        .run_start(&hang_flow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let hang_run = created.result["run_id"].as_str().unwrap().to_string();
    // 等授权后取消（单任务取消）。
    let _ = until(&backend, &hang_run, |r| {
        r.nodes.get("h").is_some_and(|n| n.operation.is_some()) || r.terminal()
    })
    .await;
    backend.run_cancel(&hang_run, None).await.unwrap();
    let cancelled = until(&backend, &hang_run, |r| r.status == RunStatus::Cancelled).await;
    assert_eq!(cancelled.status, RunStatus::Cancelled);
    // 上联保持：agent 进程仍在，第二个 run 正常完成。
    let ok_flow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"n","type":"script","params":{"code":"return 7;"}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),
    )
    .await;
    let created = backend
        .run_start(&ok_flow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let ok_run = created.result["run_id"].as_str().unwrap().to_string();
    let done = until(&backend, &ok_run, Run::terminal).await;
    assert_eq!(
        done.status, RunStatus::Succeeded,
        "uplink must survive single cancel"
    );
    drop(agent);
    backend.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn coalesced_hello_and_trailing_frame_not_lost_by_master() {
    // T02 主进程侧：AgentHello 与尾随过时 DataBind 同一次 write。握手经
    // 统一 transport 收发后，尾随帧必须交付（表现为整会话拒绝关闭）；
    // 若握手用临时 decoder 取首帧后丢弃，尾帧丢失、会话悬挂不关闭。
    use flow_engine::execution_protocol::contract::PROTOCOL_VERSION;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = temp();
    std::fs::create_dir_all(&dir).unwrap();
    let backend = JournalBackend::open(&dir.join("data"), JournalOptions::default())
        .await
        .unwrap();
    let manager = flow_backend::execution::remote::AgentManager::new(
        backend.clone(),
        backend.journal.id().into(),
        1,
    );
    let (control_a, mut client) = tokio::io::duplex(64 * 1024);
    manager
        .attach_inprocess("agent-coal".to_string(), control_a)
        .await;
    let hello = json!({
        "v": PROTOCOL_VERSION, "type": "AgentHello",
        "body": {
            "agent_boot_id": "boot-coal", "build": "t",
            "capabilities": ["relay", "resource_report", "reconnect"],
            "slots": "1"
        }
    });
    let stale = json!({
        "v": PROTOCOL_VERSION, "type": "DataBind",
        "body": {"agent_boot_id": "boot-coal", "credential": "x"}
    });
    // 关键：两帧拼接后单次写入。
    let mut bytes = raw_frame(&hello);
    bytes.extend_from_slice(&raw_frame(&stale));
    client.write_all(&bytes).await.unwrap();
    // 尾随 DataBind 必须被交付：会话以协议错误拒绝并关闭（5 秒内 EOF）。
    // 若握手交接丢失尾帧，会话将永远悬挂等不到关闭。
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut chunk = [0u8; 16 * 1024];
            if client.read(&mut chunk).await.unwrap_or(0) == 0 {
                return;
            }
        }
    })
    .await;
    assert!(
        closed.is_ok(),
        "coalesced trailing frame must be delivered and close session"
    );
    drop(client);
    backend.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn coalesced_welcome_and_trailing_frame_not_lost_by_agent() {
    // T02 agent 侧：假 master 在同一次 write 里回 Welcome + 过时 DataBind。
    // agent 握手经统一 transport 后必须消费尾帧并断开会话；若握手临时
    // decoder 丢弃尾字节，agent 只能干等心跳超时（10s 级），测试在 5 秒
    // 内断言连接关闭即可区分两种实现。
    use flow_engine::execution_protocol::frame::{encode_tagged_frame, FrameDecoder};
    use flow_engine::execution_protocol::message::Message;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // 测试进程内安装 rustls provider（与 flow-agent/flow-backend 进程一致）。
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = temp();
    std::fs::create_dir_all(&dir).unwrap();
    let (ca, server_cert, server_key, _agent_cert, _agent_key) = generate_pki(&dir, "agent-wc");
    // mTLS server：镜像 AgentManager 的接入配置（客户端证书由 CA 签发）。
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut std::io::Cursor::new(
        std::fs::read(&ca).unwrap(),
    ))
    .collect::<std::result::Result<Vec<_>, _>>()
    .unwrap()
    {
        roots.add(cert).unwrap();
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder(std::sync::Arc::new(roots))
        .build()
        .unwrap();
    let server_certs: Vec<_> = rustls_pemfile::certs(&mut std::io::Cursor::new(
        std::fs::read(&server_cert).unwrap(),
    ))
    .collect::<std::result::Result<Vec<_>, _>>()
    .unwrap();
    let server_key = rustls_pemfile::private_key(&mut std::io::Cursor::new(
        std::fs::read(&server_key).unwrap(),
    ))
    .unwrap()
    .unwrap();
    let config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(server_certs, server_key)
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut tls = acceptor.accept(socket).await.unwrap();
        // 读 AgentHello（首帧，可能分多次到达）。
        let mut decoder = FrameDecoder::new_tagged();
        loop {
            let mut chunk = [0u8; 16 * 1024];
            let n = tls.read(&mut chunk).await.unwrap();
            assert!(n > 0, "closed before agent hello");
            decoder.feed(&chunk[..n]).unwrap();
            if let Some((_, _)) = decoder.pop_tagged_frame().unwrap() {
                break;
            }
        }
        // 关键：Welcome + 过时 DataBind（目录外类型）拼接后单次写回。
        let welcome = encode_tagged_frame(&Message::AgentWelcome {
            journal_id: "journal-wc".into(),
            master_epoch: 1,
            link_session_id: "link-wc".into(),
            agent_id: "agent-wc".into(),
            heartbeat_ms: flow_engine::execution_protocol::HEARTBEAT_INTERVAL_MS,
            agent_window_bytes: 8 * 1024 * 1024,
        })
        .unwrap();
        let stale = raw_frame(&serde_json::json!({
            "v": flow_engine::execution_protocol::PROTOCOL_VERSION,
            "type": "DataBind",
            "body": {"agent_boot_id": "boot-wc", "credential": "x"}
        }));
        let mut out = welcome;
        out.extend_from_slice(&stale);
        tls.write_all(&out).await.unwrap();
        tls.flush().await.unwrap();
        // agent 必须消费尾帧、拒绝会话并断开（EOF）。
        let closed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut chunk = [0u8; 16 * 1024];
                if tls.read(&mut chunk).await.unwrap_or(0) == 0 {
                    return;
                }
            }
        })
        .await;
        closed.is_ok()
    });
    let mut agent = spawn_agent(&dir, port, "agent-wc", 1);
    let delivered = tokio::time::timeout(Duration::from_secs(20), server_task)
        .await
        .expect("fake master session completes")
        .unwrap();
    assert!(
        delivered,
        "agent must consume coalesced trailing frame and close uplink; stderr:\n{}",
        agent.dump_stderr()
    );
    drop(agent);
    std::fs::remove_dir_all(&dir).unwrap();
}
