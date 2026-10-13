//! T13：WSS Agent 上联集成回归。
//!
//! - 真实 flow-agent 进程经 WSS（代理承载模拟：明文 TCP + token 认证）
//!   上联并跑通业务（执行器 → 中继 → 权威日志）。
//! - 认证拒绝：缺/错 token、缺 agent 身份头。
//! - Text 消息拒绝：认证通过后发 Text，会话必须拒绝关闭。

use flow_backend::journal::JournalBackend;
use flow_engine::execution_protocol::transport::FrameTransport;
use flow_engine::journal_state::{Run, RunStatus};
use flow_journal::JournalOptions;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-wss-{}", uuid::Uuid::now_v7()))
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

fn plain_remote_options() -> flow_backend::execution::remote::RemoteOptions {
    // WSS 测试不经 mTLS 监听；证书路径仅为满足结构体字段。
    flow_backend::execution::remote::RemoteOptions {
        control_addr: "127.0.0.1:0".into(),
        ca_cert: "unused".into(),
        server_cert: "unused".into(),
        server_key: "unused".into(),
        attach_timeout_ms: 10_000,
        wss_addr: None,
        agent_token: None,
    }
}

struct AgentProcess {
    child: std::process::Child,
    stderr: Option<std::process::ChildStderr>,
}

impl AgentProcess {
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

impl Drop for AgentProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_agent_wss(port: u16, agent_id: &str, slots: u32, token: &str) -> AgentProcess {
    let mut child = std::process::Command::new(flow_test_support::io::agent_bin())
        .args([
            "--wss-url",
            &format!("ws://127.0.0.1:{port}/ws/agent"),
            "--agent-id",
            agent_id,
            "--token",
            token,
            "--slots",
            &slots.to_string(),
        ])
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn flow-agent (wss)");
    AgentProcess {
        stderr: child.stderr.take(),
        child,
    }
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

#[tokio::test]
async fn wss_agent_end_to_end_business() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "off".into()),
        )
        .try_init();
    let root = temp();
    std::fs::create_dir_all(&root).unwrap();
    let ports = free_ports(1);
    let backend = JournalBackend::open(&root.join("data"), JournalOptions::default())
        .await
        .unwrap();
    let manager = flow_backend::execution::remote::AgentManager::new(
        backend.clone(),
        backend.journal.id().into(),
        1,
    );
    backend
        .start_execution_remote_manager(manager.clone(), plain_remote_options())
        .await
        .unwrap();
    manager
        .listen_wss(format!("127.0.0.1:{}", ports[0]), "wss-secret".into())
        .await
        .unwrap();
    let mut agent = spawn_agent_wss(ports[0], "agent-wss-1", 2, "wss-secret");
    // 等会话注册（真实进程 TLS/WS 握手）。
    tokio::time::sleep(Duration::from_millis(800)).await;

    let workflow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"n","type":"script","params":{"code":"return {n: input.n * 2};"}},
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
            "wss run failed: {:?}; agent stderr:\n{}",
            done.error,
            agent.dump_stderr()
        );
    }
    assert_eq!(done.nodes["n"].output.clone().unwrap().as_inline()["n"], 42);
    drop(agent);
    manager.shutdown().await;
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn wss_rejects_bad_or_missing_credentials() {
    let root = temp();
    std::fs::create_dir_all(&root).unwrap();
    let ports = free_ports(1);
    let backend = JournalBackend::open(&root.join("data"), JournalOptions::default())
        .await
        .unwrap();
    let manager = flow_backend::execution::remote::AgentManager::new(
        backend.clone(),
        backend.journal.id().into(),
        1,
    );
    backend
        .start_execution_remote_manager(manager.clone(), plain_remote_options())
        .await
        .unwrap();
    manager
        .listen_wss(format!("127.0.0.1:{}", ports[0]), "right-token".into())
        .await
        .unwrap();
    let url = format!("ws://127.0.0.1:{}/ws/agent", ports[0]);

    let attempt = |token: Option<&str>, agent: Option<&str>| {
        let url = url.clone();
        let token = token.map(str::to_owned);
        let agent = agent.map(str::to_owned);
        async move {
            use tokio_tungstenite::tungstenite::client::IntoClientRequest;
            let mut request = url.into_client_request().unwrap();
            let headers = request.headers_mut();
            if let Some(token) = token {
                headers.insert("authorization", format!("Bearer {token}").parse().unwrap());
            }
            if let Some(agent) = agent {
                headers.insert("x-flow-agent-id", agent.parse().unwrap());
            }
            tokio_tungstenite::connect_async(request).await
        }
    };

    // 错 token → 拒绝。
    assert!(attempt(Some("wrong"), Some("agent-x")).await.is_err());
    // 缺 token → 拒绝。
    assert!(attempt(None, Some("agent-x")).await.is_err());
    // 缺身份头 → 拒绝。
    assert!(attempt(Some("right-token"), None).await.is_err());
    // 正确凭据 → 通过握手（随后会话等待 AgentHello，但握手本身成功）。
    assert!(attempt(Some("right-token"), Some("agent-ok")).await.is_ok());

    manager.shutdown().await;
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn wss_rejects_text_messages() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let root = temp();
    std::fs::create_dir_all(&root).unwrap();
    let ports = free_ports(1);
    let backend = JournalBackend::open(&root.join("data"), JournalOptions::default())
        .await
        .unwrap();
    let manager = flow_backend::execution::remote::AgentManager::new(
        backend.clone(),
        backend.journal.id().into(),
        1,
    );
    backend
        .start_execution_remote_manager(manager.clone(), plain_remote_options())
        .await
        .unwrap();
    manager
        .listen_wss(format!("127.0.0.1:{}", ports[0]), "tok".into())
        .await
        .unwrap();

    let mut request = format!("ws://127.0.0.1:{}/ws/agent", ports[0])
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer tok".parse().unwrap());
    request
        .headers_mut()
        .insert("x-flow-agent-id", "agent-text".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    use futures::SinkExt;
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        "not a protocol frame".into(),
    ))
    .await
    .unwrap();
    // 会话必须拒绝 Text 并关闭：客户端可见 Close/EOF。
    use futures::StreamExt;
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(message) = ws.next().await {
            match message {
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => return true,
                Err(_) => return true,
                _ => {}
            }
        }
        true
    })
    .await;
    assert!(closed.is_ok(), "text message must close the session");
    manager.shutdown().await;
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

/// 供断言用的小工具（Inline 值取值）。
trait AsInline {
    fn as_inline(&self) -> &Value;
}
impl AsInline for flow_journal::StoredValue {
    fn as_inline(&self) -> &Value {
        match self {
            flow_journal::StoredValue::Inline(value) => value,
            flow_journal::StoredValue::Ref(_) => panic!("expected inline value"),
        }
    }
}

/// 用不到的重导出占位，避免未使用告警。
#[allow(dead_code)]
fn _unused(_: &FrameTransport) {}
