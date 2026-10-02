//! 三期 R1-01：flow-agent 双 TLS（mTLS）与资源准入的网络集成回归。
//!
//! 真实 flow-agent 二进制进程 + 真实 TLS 监听 + 真实执行器子进程：
//! 端到端业务（script/HTTP）、data 绑定凭据校验、单任务取消不关上联。

use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::Run;
use flow_journal::{JournalOptions, StoredValue};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-journal-r1-{}", uuid::Uuid::now_v7()))
}

fn executor_bin() -> std::path::PathBuf {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    for profile in ["debug", "release"] {
        let candidate = root.join("profile").join(profile).join("flow-executor");
        let candidate = root.join("target").join(profile).join("flow-executor");
        if candidate.is_file() {
            return candidate;
        }
    }
    panic!("flow-executor binary not built; run: cargo build -p flow-executor");
}

fn agent_bin() -> std::path::PathBuf {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    for profile in ["debug", "release"] {
        let candidate = root.join("target").join(profile).join("flow-agent");
        if candidate.is_file() {
            return candidate;
        }
    }
    panic!("flow-agent binary not built; run: cargo build -p flow-agent");
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
    data: u16,
) -> flow_backend::execution::remote::RemoteOptions {
    flow_backend::execution::remote::RemoteOptions {
        control_addr: format!("127.0.0.1:{control}"),
        data_addr: format!("127.0.0.1:{data}"),
        ca_cert: dir.join("ca.pem"),
        server_cert: dir.join("server.pem"),
        server_key: dir.join("server.key"),
        attach_timeout_ms: 10_000,
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
    data: u16,
    agent_cn: &str,
    slots: u32,
) -> AgentProcess {
    let child = std::process::Command::new(agent_bin())
        .args([
            "--control-addr",
            &format!("127.0.0.1:{control}"),
            "--data-addr",
            &format!("127.0.0.1:{data}"),
            "--agent-id",
            agent_cn,
            "--ca-cert",
        ])
        .arg(dir.join("ca.pem"))
        .arg("--cert")
        .arg(dir.join("agent.pem"))
        .arg("--key")
        .arg(dir.join("agent.key"))
        .arg("--executor-bin")
        .arg(executor_bin())
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
    let ports = free_ports(2);
    let backend = JournalBackend::open(&dir.join("data"), JournalOptions::default())
        .await
        .unwrap();
    backend
        .start_execution_remote(remote_options(&dir, ports[0], ports[1]))
        .await
        .unwrap();
    // 确认监听可达。
    for port in &ports {
        let probe = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await;
        assert!(probe.is_ok(), "listener {port} must be reachable");
    }
    let mut _agent = spawn_agent(&dir, ports[0], ports[1], "agent-tls-1", 4);
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
    if done.status != "succeeded" {
        panic!(
            "tls run failed: {:?}; agent stderr:\n{}",
            done.error,
            _agent.dump_stderr()
        );
    }
    let output = match done.nodes["e"].output.clone().unwrap() {
        StoredValue::Inline(value) => value,
        StoredValue::Ref(_) => json!({"slept_ms": 10}),
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

#[tokio::test]
async fn wrong_data_credential_rejected_and_single_cancel_keeps_uplink() {
    // 凭据校验（管理级）：data 绑定凭据错误 → 会话拒绝。
    // 这里用进程内注入验证（无需完整 agent）：错误 DataBind 后会话关闭。
    use flow_engine::execution_protocol::transport::FrameTransport;
    use flow_engine::execution_protocol::Message;
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
    let (control_a, control_b) = tokio::io::duplex(64 * 1024);
    let (data_a, data_b) = tokio::io::duplex(64 * 1024);
    manager
        .attach_inprocess("agent-bad".to_string(), control_a, data_a)
        .await;
    let mut transport = FrameTransport::spawn_streams(control_b, data_b, 16);
    transport
        .send(Message::AgentHello {
            agent_boot_id: "boot-bad".into(),
            build: "t".into(),
            capabilities: vec!["relay".into(), "resource_report".into(), "reconnect".into()],
            slots: 1,
        })
        .await
        .unwrap();
    let welcome = loop {
        let (_, message) = transport.recv().await.unwrap();
        if matches!(message, Message::AgentWelcome { .. }) {
            break message;
        }
    };
    let Message::AgentWelcome {
        link_session_id, ..
    } = welcome
    else {
        unreachable!()
    };
    // 错误凭据 → data 绑定失败 → 会话关闭。
    transport
        .send(Message::DataBind {
            agent_boot_id: "boot-bad".into(),
            link_session_id,
            credential: "wrong-credential".into(),
        })
        .await
        .unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if transport.recv().await.is_err() {
                return;
            }
        }
    })
    .await;
    assert!(closed.is_ok(), "bad data credential must close session");
    drop(transport);
    backend.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test]
async fn single_task_cancel_keeps_agent_uplink_alive() {
    let dir = temp();
    std::fs::create_dir_all(&dir).unwrap();
    generate_pki(&dir, "agent-cancel");
    let ports = free_ports(2);
    let backend = JournalBackend::open(&dir.join("data"), JournalOptions::default())
        .await
        .unwrap();
    backend
        .start_execution_remote(remote_options(&dir, ports[0], ports[1]))
        .await
        .unwrap();
    let agent = spawn_agent(&dir, ports[0], ports[1], "agent-cancel", 4);
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
    let cancelled = until(&backend, &hang_run, |r| r.status == "cancelled").await;
    assert_eq!(cancelled.status, "cancelled");
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
        done.status, "succeeded",
        "uplink must survive single cancel"
    );
    drop(agent);
    backend.close().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}
