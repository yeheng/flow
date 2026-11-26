//! T13：WSS Agent 上联集成回归。
//!
//! - 真实 flow-agent 进程经 WSS（代理承载模拟：明文 TCP + token 认证）
//!   上联并跑通业务（执行器 → 中继 → 权威日志）。
//! - 认证：身份由 token→agent_id 映射表决定，自报 header 只做一致性
//!   断言（错/缺 token、持 A 凭证声明 B 身份、伪造 header 冒充都拒绝）。
//! - Text 消息拒绝：认证通过后发 Text，会话必须拒绝关闭。

use flow_backend::journal::JournalBackend;
use flow_dispatcher::execution_protocol::transport::FrameTransport;
use flow_domain::journal_state::{Run, RunStatus};
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
        wss_agents: Vec::new(),
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
    spawn_agent_wss_with_executor(port, agent_id, slots, token, None)
}

fn spawn_agent_wss_with_executor(
    port: u16,
    agent_id: &str,
    slots: u32,
    token: &str,
    executor: Option<&std::path::Path>,
) -> AgentProcess {
    spawn_agent_wss_with_plugin(port, agent_id, slots, token, executor, None)
}

fn spawn_agent_wss_with_plugin(
    port: u16,
    agent_id: &str,
    slots: u32,
    token: &str,
    executor: Option<&std::path::Path>,
    model_plugin: Option<&std::path::Path>,
) -> AgentProcess {
    let mut command = std::process::Command::new(flow_test_support::io::agent_bin());
    command
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
        .stderr(std::process::Stdio::piped());
    if let Some(executor) = executor {
        command.arg("--executor-bin").arg(executor);
    }
    if let Some(plugin) = model_plugin {
        command.env("FLOW_LLM_PLUGIN_BIN", plugin).env(
            "FLOW_PLUGINS_DIR",
            plugin.parent().unwrap().join("empty-plugins"),
        );
    }
    let mut child = command.spawn().expect("spawn flow-agent (wss)");
    AgentProcess {
        stderr: child.stderr.take(),
        child,
    }
}

#[tokio::test]
async fn wss_routes_the_exact_frozen_plugin_content() {
    use flow_backend::execution::remote::{AgentCredential, AgentManager};
    use flow_test_support::fixtures::StubHttp;

    let root = tempfile::tempdir().unwrap();
    let backend = JournalBackend::open(root.path(), JournalOptions::default())
        .await
        .unwrap();
    let manager = AgentManager::new(backend.clone(), backend.journal.id().into(), 1);
    backend
        .start_execution_remote_manager(manager.clone(), plain_remote_options())
        .await
        .unwrap();
    let port = free_ports(1)[0];
    manager
        .listen_wss(
            format!("127.0.0.1:{port}"),
            vec![
                AgentCredential {
                    id: "a-wrong".into(),
                    token: "wrong-content".into(),
                },
                AgentCredential {
                    id: "z-exact".into(),
                    token: "exact-content".into(),
                },
            ],
        )
        .await
        .unwrap();
    let wrong_plugin = root.path().join("wrong-plugin");
    std::fs::create_dir(root.path().join("empty-plugins")).unwrap();
    std::fs::write(&wrong_plugin, b"different model plugin").unwrap();
    let _wrong = spawn_agent_wss_with_plugin(
        port,
        "a-wrong",
        1,
        "wrong-content",
        None,
        Some(&wrong_plugin),
    );
    tokio::time::sleep(Duration::from_millis(600)).await;

    let model = StubHttp::fixed(200, json!({"choices":[{"message":{"content":"exact"}}]})).await;
    let workflow = install(
        &backend,
        json!({
            "permissions":{"version":1,"mode":"bypass","allow":[]},
            "nodes":[{"id":"s","type":"start"},
                {"id":"model","type":"llm","params":{"url":model.url("/model"),
                    "body":{"model":"fixture","messages":[{"role":"user","content":"hello"}]}}},
                {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"model"},{"from":"model","to":"e"}]
        }),
    )
    .await;
    let run_model = || async {
        let receipt = backend
            .run_start(&workflow, None, Value::Null, "manual", None, None)
            .await
            .unwrap();
        until(
            &backend,
            receipt.result["run_id"].as_str().unwrap(),
            Run::terminal,
        )
        .await
    };
    let failed = run_model().await;
    let node = &failed.nodes["model"];
    assert!(
        node.error
            .as_deref()
            .is_some_and(|error| error.contains("no agent with plugin llm")),
        "status={:?}, node_error={:?}, run_error={:?}",
        failed.status,
        node.error,
        failed.error
    );
    assert!(node.prepared.is_none());
    assert!(model.requests().is_empty());

    let _exact = spawn_agent_wss(port, "z-exact", 1, "exact-content");
    tokio::time::sleep(Duration::from_millis(600)).await;
    let done = run_model().await;
    assert_eq!(
        done.status,
        RunStatus::Succeeded,
        "{:?}",
        done.nodes["model"].error
    );
    assert_eq!(model.requests().len(), 1);

    manager.shutdown().await;
    backend.close().await.unwrap();
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
        .listen_wss(
            format!("127.0.0.1:{}", ports[0]),
            vec![flow_backend::execution::remote::AgentCredential {
                id: "agent-wss-1".into(),
                token: "wss-secret".into(),
            }],
        )
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

/// W1：WSS 身份由 token→agent_id 映射表决定，客户端自报 header 只做
/// 一致性断言——任何单一凭证只能取得一个 agent 身份，冒充进不了门。
#[tokio::test]
async fn wss_identity_comes_from_token_not_client_header() {
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
        .listen_wss(
            format!("127.0.0.1:{}", ports[0]),
            vec![
                flow_backend::execution::remote::AgentCredential {
                    id: "agent-a".into(),
                    token: "token-a".into(),
                },
                flow_backend::execution::remote::AgentCredential {
                    id: "agent-b".into(),
                    token: "token-b".into(),
                },
            ],
        )
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

    // 错 token / 缺 token → 拒绝。
    assert!(attempt(Some("wrong"), Some("agent-a")).await.is_err());
    assert!(attempt(None, Some("agent-a")).await.is_err());
    // 持 A 凭证声明 B 身份 → 拒绝（自报 header 降级为一致性断言）。
    assert!(attempt(Some("token-a"), Some("agent-b")).await.is_err());
    // 伪造 header 冒充他 agent（token 对不上）→ 拒绝。
    assert!(attempt(Some("token-b"), Some("agent-a")).await.is_err());
    // 正确凭证 + 一致声明 → 通过。
    assert!(attempt(Some("token-a"), Some("agent-a")).await.is_ok());
    // 正确凭证 + 不声明 → 身份由凭证反查，照样通过。
    assert!(attempt(Some("token-b"), None).await.is_ok());

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
        .listen_wss(
            format!("127.0.0.1:{}", ports[0]),
            vec![flow_backend::execution::remote::AgentCredential {
                id: "agent-text".into(),
                token: "tok".into(),
            }],
        )
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

#[tokio::test]
#[ignore = "requires a separate no-default-features Worker in FLOW_TEST_BUILTIN_EXECUTOR_BIN"]
async fn mixed_worker_builds_route_plugins_without_consuming_builtin_capacity() {
    use flow_backend::execution::remote::{AgentCredential, AgentManager};
    use flow_test_support::fixtures::StubHttp;

    let builtin = std::path::PathBuf::from(
        std::env::var_os("FLOW_TEST_BUILTIN_EXECUTOR_BIN").expect("builtin Worker path"),
    );
    let plugin = flow_test_support::io::executor_bin();
    assert_ne!(
        builtin.canonicalize().unwrap(),
        plugin.canonicalize().unwrap()
    );
    let root = tempfile::tempdir().unwrap();
    let backend = JournalBackend::open(root.path(), JournalOptions::default())
        .await
        .unwrap();
    let manager = AgentManager::new(backend.clone(), backend.journal.id().into(), 1);
    backend
        .start_execution_remote_manager(manager.clone(), plain_remote_options())
        .await
        .unwrap();
    let port = free_ports(1)[0];
    manager
        .listen_wss(
            format!("127.0.0.1:{port}"),
            vec![
                AgentCredential {
                    id: "a-builtin".into(),
                    token: "builtin-fixture".into(),
                },
                AgentCredential {
                    id: "b-plugins".into(),
                    token: "plugins-fixture".into(),
                },
            ],
        )
        .await
        .unwrap();
    let installed_builtin = root.path().join("builtin-executor");
    std::fs::copy(&builtin, &installed_builtin).unwrap();
    let _builtin = spawn_agent_wss_with_executor(
        port,
        "a-builtin",
        2,
        "builtin-fixture",
        Some(&installed_builtin),
    );
    tokio::time::sleep(Duration::from_millis(600)).await;
    let model = StubHttp::fixed(200, json!({"choices":[{"message":{"content":"fixture"}}]})).await;
    let model_definition = |url| {
        json!({
            "permissions":{"version":1,"mode":"bypass","allow":[]},
            "nodes":[{"id":"s","type":"start"},
                {"id":"model","type":"llm","params":{"url":url,"body":{"model":"fixture","messages":[{"role":"user","content":"hello"}]}}},
                {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"model"},{"from":"model","to":"e"}]
        })
    };
    let workflow = install(&backend, model_definition(model.url("/model"))).await;
    let run_model = || async {
        backend
            .run_start(&workflow, None, Value::Null, "manual", None, None)
            .await
            .unwrap()
    };
    let missing = run_model().await;
    let done = until(
        &backend,
        missing.result["run_id"].as_str().unwrap(),
        Run::terminal,
    )
    .await;
    assert_eq!(done.status, RunStatus::Failed);
    let node = &done.nodes["model"];
    assert!(
        node.error
            .as_deref()
            .is_some_and(|error| error.contains("no plugin-capable agent")),
        "{:?}",
        node.error
    );
    assert!(
        node.prepared.is_none(),
        "the unsupported dispatch must not start a Worker"
    );
    assert!(model.requests().is_empty());

    let _plugins =
        spawn_agent_wss_with_executor(port, "b-plugins", 1, "plugins-fixture", Some(&plugin));
    tokio::time::sleep(Duration::from_millis(600)).await;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let held_url = format!("http://{}/model", listener.local_addr().unwrap());
    let (arrived, arrival) = tokio::sync::oneshot::channel();
    let (release, open) = tokio::sync::oneshot::channel();
    let held_http = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        assert!(socket.read(&mut request).await.unwrap() > 0);
        arrived.send(()).unwrap();
        open.await.unwrap();
        let body = r#"{"choices":[{"message":{"content":"held fixture"}}]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len(),
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    let held_flow = install(&backend, model_definition(held_url)).await;
    let held = backend
        .run_start(&held_flow, None, Value::Null, "manual", None, None)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), arrival)
        .await
        .expect("plugin Worker must reach the model")
        .unwrap();

    let queued = run_model().await;
    let queued_id = queued.result["run_id"].as_str().unwrap();
    until(&backend, queued_id, |run| {
        run.nodes.get("s").is_some_and(|node| node.output.is_some())
    })
    .await;
    let basic_flow = install(
        &backend,
        json!({
            "nodes":[{"id":"s","type":"start"},
                {"id":"compute","type":"script","params":{"code":"return 42;"}},
                {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"compute"},{"from":"compute","to":"e"}]
        }),
    )
    .await;
    let basic = backend
        .run_start(&basic_flow, None, Value::Null, "manual", None, None)
        .await
        .unwrap();
    let basic = tokio::time::timeout(
        Duration::from_secs(8),
        until(
            &backend,
            basic.result["run_id"].as_str().unwrap(),
            Run::terminal,
        ),
    )
    .await
    .expect("a busy plugin slot must leave the builtin Worker available");
    assert_eq!(basic.status, RunStatus::Succeeded, "{:?}", basic.error);
    assert_eq!(
        basic.nodes["compute"].output.as_ref().unwrap().as_inline(),
        &json!(42)
    );
    let waiting = backend.run_snapshot(queued_id).await.unwrap().unwrap();
    assert!(
        !waiting.nodes.contains_key("model"),
        "a queued plugin task must have no dispatch"
    );
    assert!(model.requests().is_empty());

    release.send(()).unwrap();
    held_http.await.unwrap();
    let held = until(
        &backend,
        held.result["run_id"].as_str().unwrap(),
        Run::terminal,
    )
    .await;
    assert_eq!(
        held.status,
        RunStatus::Succeeded,
        "{:?}",
        held.nodes.get("model")
    );
    let done = until(&backend, queued_id, Run::terminal).await;
    assert_eq!(
        done.status,
        RunStatus::Succeeded,
        "{:?}",
        done.nodes.get("model")
    );
    assert_eq!(model.requests().len(), 1);
    // The active Agent must corroborate its advertised profile on the new child.
    #[cfg(unix)]
    {
        let replacement = root.path().join("replacement-executor");
        std::fs::copy(&plugin, &replacement).unwrap();
        std::fs::rename(&replacement, &installed_builtin).unwrap();
        let changed = backend
            .run_start(&basic_flow, None, Value::Null, "manual", None, None)
            .await
            .unwrap();
        let changed = until(
            &backend,
            changed.result["run_id"].as_str().unwrap(),
            Run::terminal,
        )
        .await;
        assert_eq!(changed.status, RunStatus::Failed);
        assert!(
            changed.nodes["s"]
                .error
                .as_deref()
                .is_some_and(|err| err.contains("worker profile changed")),
            "{:?}",
            changed.nodes.get("s")
        );
        assert!(changed.nodes["s"].prepared.is_none());
    }
    manager.shutdown().await;
    backend.close().await.unwrap();
}
