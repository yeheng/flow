//! 三期 R0-01：模拟中继与远程复用契约回归。
//!
//! 同一组任务在直接 socketpair（二期 IPC）与模拟 relay（in-process agent +
//! 真实执行器二进制）路径下产生相同权威事实；错路由/伪造能力明确拒绝；
//! ACK/Permit 只能由主进程产生（中继结构上无法生成，relay 单测断言管理帧
//! 不可包装转发）。

use flow_agent::runtime::AgentConfig;
use flow_backend::journal::JournalBackend;
use flow_engine::execution_protocol::transport::FrameTransport;
use flow_engine::execution_protocol::Message;
use flow_engine::journal_state::Run;
use flow_journal::{JournalOptions, StoredValue};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-journal-r0-{}", uuid::Uuid::now_v7()))
}

fn executor_bin() -> std::path::PathBuf {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|p| p.parent())
        .expect("workspace root");
    for profile in ["debug", "release"] {
        let candidate = root.join("target").join(profile).join("flow-executor");
        if candidate.is_file() {
            return candidate;
        }
    }
    panic!("flow-executor binary not built; run: cargo build -p flow-executor");
}

fn remote_options() -> flow_backend::execution::remote::RemoteOptions {
    flow_backend::execution::remote::RemoteOptions {
        control_addr: "127.0.0.1:0".into(),
        data_addr: "127.0.0.1:0".into(),
        ca_cert: "unused".into(),
        server_cert: "unused".into(),
        server_key: "unused".into(),
        attach_timeout_ms: 30_000,
    }
}

/// 启动 in-process 模拟 agent（双 duplex 流，无 TLS）：等价于 flow-agent
/// 的上联会话，但身份由测试注入。
async fn spawn_simulated_agent(
    manager: &Arc<flow_backend::execution::remote::AgentManager>,
    agent_id: &str,
    slots: u32,
) {
    let (control_a, control_b) = tokio::io::duplex(256 * 1024);
    let (data_a, data_b) = tokio::io::duplex(256 * 1024);
    manager
        .attach_inprocess(agent_id.to_string(), control_a, data_a)
        .await;
    let config = AgentConfig {
        agent_id: agent_id.to_string(),
        control_addr: String::new(),
        data_addr: String::new(),
        ca_cert: "unused".into(),
        cert: "unused".into(),
        key: "unused".into(),
        executor_bin: executor_bin(),
        slots,
    };
    let shutdown = tokio_util::sync::CancellationToken::new();
    tokio::spawn(async move {
        // 模拟 agent：transport 建立后直接进入服务循环（跳过 TLS 与外层
        // AgentHello——manager 的 in-process 注入路径在握手时同样要求
        // AgentHello/DataBind；为满足握手，先用裸帧完成 Hello/Welcome/
        // DataBind 再交 serve。
        let mut transport = FrameTransport::spawn_streams(control_b, data_b, 128);
        let boot = flow_engine::execution_protocol::transport::fresh_boot_id();
        transport
            .send(flow_engine::execution_protocol::Message::AgentHello {
                agent_boot_id: boot.clone(),
                build: "sim".into(),
                capabilities: vec!["relay".into(), "resource_report".into(), "reconnect".into()],
                slots,
            })
            .await
            .expect("send agent hello");
        let welcome = loop {
            let (_, message) = transport.recv().await.expect("welcome");
            if let flow_engine::execution_protocol::Message::AgentWelcome { .. } = message {
                break message;
            }
        };
        let flow_engine::execution_protocol::Message::AgentWelcome {
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
            .send(flow_engine::execution_protocol::Message::DataBind {
                agent_boot_id: boot.clone(),
                link_session_id: link_session_id.clone(),
                credential: data_credential,
            })
            .await
            .expect("send data bind");
        // serve 内部依赖 relay/绑定/执行器循环；直接复用 run_agent 的 serve。
        let relay = Arc::new(parking_lot::Mutex::new(flow_agent::relay::Relay::new(
            agent_id_placeholder(&config),
            boot,
            link_session_id,
        )));
        let handles = Arc::new(tokio::sync::Mutex::new(std::collections::BTreeMap::new()));
        let reason = flow_agent::runtime::serve_for_tests(
            &config,
            &journal_id,
            master_epoch,
            transport,
            relay,
            handles,
            shutdown,
        )
        .await;
        let _ = reason;
    });
}

fn agent_id_placeholder(config: &AgentConfig) -> String {
    config.agent_id.clone()
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

async fn materialize(backend: &JournalBackend, run: &Run, node_id: &str) -> Value {
    match run.nodes[node_id].output.clone().unwrap() {
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

const WORKFLOW: &str = r#"{
    "nodes": [
        {"id":"s","type":"start"},
        {"id":"n","type":"script","params":{"code":"return {n: input.n + 1};"}},
        {"id":"d","type":"delay","params":{"ms": 20}},
        {"id":"e","type":"end"}
    ],
    "edges": [
        {"from":"s","to":"n"},
        {"from":"n","to":"d"},
        {"from":"d","to":"e"}
    ]
}"#;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "off".into()),
        )
        .try_init();
}

#[tokio::test]
async fn relay_path_produces_identical_authoritative_facts() {
    init_tracing();
    // 直接路径（二期 IPC）。
    let root_direct = temp();
    let direct = JournalBackend::open(&root_direct, JournalOptions::default())
        .await
        .unwrap();
    direct.start_execution().await.unwrap();
    let flow_direct = install(&direct, serde_json::from_str(WORKFLOW).unwrap()).await;
    let created = direct
        .run_start(&flow_direct, None, json!({"n": 41}), "manual", None, None)
        .await
        .unwrap();
    let run_direct = created.result["run_id"].as_str().unwrap().to_string();
    let done_direct = until(&direct, &run_direct, Run::terminal).await;
    assert_eq!(done_direct.status, "succeeded");
    direct.close().await.unwrap();

    // 中继路径（三期 in-process agent + 真实执行器）。
    let root_relay = temp();
    let relay_backend = JournalBackend::open(&root_relay, JournalOptions::default())
        .await
        .unwrap();
    let manager = flow_backend::execution::remote::AgentManager::new(
        relay_backend.clone(),
        relay_backend.journal.id().into(),
        1,
    );
    relay_backend
        .start_execution_remote_manager(manager.clone(), remote_options())
        .await
        .unwrap();
    spawn_simulated_agent(&manager, "agent-r0", 4).await;
    // 等会话就绪（容量注册）再启动 run。
    tokio::time::sleep(Duration::from_millis(200)).await;
    let flow_relay = install(&relay_backend, serde_json::from_str(WORKFLOW).unwrap()).await;
    let created = relay_backend
        .run_start(&flow_relay, None, json!({"n": 41}), "manual", None, None)
        .await
        .unwrap();
    let run_relay = created.result["run_id"].as_str().unwrap().to_string();
    let done_relay = until(&relay_backend, &run_relay, Run::terminal).await;
    if done_relay.status != "succeeded" {
        let mut report = String::new();
        for (id, n) in &done_relay.nodes {
            report.push_str(&format!(
                "  {id}: {} err={:?} wait={:?}\n",
                n.status,
                n.error,
                n.wait.as_ref().map(|w| w.kind.clone())
            ));
        }
        panic!("relay run failed: {:?}\n{}", done_relay.error, report);
    }

    // 同一组事实：逐节点状态/输出/attempt/audit_seq/prepared 一致。
    for node_id in ["s", "n", "d", "e"] {
        let a = &done_direct.nodes[node_id];
        let b = &done_relay.nodes[node_id];
        assert_eq!(a.status, b.status, "{node_id}");
        assert_eq!(a.output, b.output, "{node_id}");
        assert_eq!(a.attempt, b.attempt, "{node_id}");
        assert_eq!(
            a.attempts[a.dispatch_id.as_str()].audit_seq,
            b.attempts[b.dispatch_id.as_str()].audit_seq,
            "{node_id}"
        );
        assert_eq!(
            a.prepared.as_ref().map(|p| &p.params),
            b.prepared.as_ref().map(|p| &p.params),
            "{node_id}"
        );
    }
    // End 的直接前驱是 delay：单值直通（两种路径输出一致）。
    let out_direct = materialize(&direct, &done_direct, "e").await;
    let out_relay = materialize(&relay_backend, &done_relay, "e").await;
    assert_eq!(out_direct, out_relay);
    assert_eq!(out_relay["slept_ms"], 20);
    relay_backend.close().await.unwrap();
    std::fs::remove_dir_all(root_direct).unwrap();
    std::fs::remove_dir_all(root_relay).unwrap();
}

#[tokio::test]
async fn wrong_route_frames_never_reach_authority() {
    use flow_engine::execution_protocol::Message;
    // relay 层：错路由拒绝（详见 flow-agent relay 单测）；这里验证 master
    // 会话侧：未绑定 dispatch 的 Routed 帧被拒绝且 journal 无任何写入。
    let root = temp();
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    let manager = flow_backend::execution::remote::AgentManager::new(
        backend.clone(),
        backend.journal.id().into(),
        1,
    );
    // 直接注入恶意流：AgentHello 合法 → 收 Welcome → DataBind → 发送
    // 伪造路由帧（未绑定 dispatch）。
    let (control_a, control_b) = tokio::io::duplex(64 * 1024);
    let (data_a, data_b) = tokio::io::duplex(64 * 1024);
    manager
        .attach_inprocess("agent-evil".to_string(), control_a, data_a)
        .await;
    let mut transport = FrameTransport::spawn_streams(control_b, data_b, 16);
    transport
        .send(Message::AgentHello {
            agent_boot_id: "boot-evil".into(),
            build: "t".into(),
            capabilities: vec!["relay".into(), "resource_report".into(), "reconnect".into()],
            slots: 2,
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
        link_session_id,
        data_credential,
        ..
    } = welcome
    else {
        unreachable!()
    };
    transport
        .send(Message::DataBind {
            agent_boot_id: "boot-evil".into(),
            link_session_id,
            credential: data_credential,
        })
        .await
        .unwrap();
    // 伪造：为未绑定派发上报审计。
    transport
        .send(Message::Routed {
            envelope: flow_engine::execution_protocol::remote::RouteEnvelope {
                agent_id: "agent-evil".into(),
                agent_boot_id: "boot-evil".into(),
                link_session_id: "link-0".into(),
                executor_id: "e".into(),
                executor_boot_id: "eb".into(),
                dispatch_id: Some("dispatch-not-bound".into()),
            },
            inner: Box::new(Message::AuditBatch {
                dispatch_id: "dispatch-not-bound".into(),
                first_seq: 1,
                records: vec![flow_engine::execution_protocol::AuditRecord {
                    audit_seq: 1,
                    kind: "input_prepared".into(),
                    payload: json!({"prepared": {}}),
                }],
            }),
        })
        .await
        .unwrap();
    // 会话应对伪造路由明确拒绝（关闭/错误），journal 零事件。
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if transport.recv().await.is_err() {
                return;
            }
        }
    })
    .await;
    assert!(closed.is_ok(), "session must reject unbound routed frames");
    let state = backend.state().await;
    assert!(
        state.runs.is_empty(),
        "forged route must not enter authority"
    );
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn relay_cannot_wrap_authoritative_control_frames() {
    // ACK/Permit/ResultCommitted 只能由主进程产生：中继包装层结构上禁止
    // 生成/转发管理帧（flow-agent relay 单测亦覆盖）。
    let mut relay = flow_agent::relay::Relay::new("a".into(), "b".into(), "l".into());
    relay.bind("d", "exec", "eboot");
    for forbidden in [
        Message::AgentHello {
            agent_boot_id: "x".into(),
            build: "x".into(),
            capabilities: vec![],
            slots: 1,
        },
        Message::Drain { grace_ms: 1 },
    ] {
        assert!(relay.wrap_to_master("d", forbidden).is_err());
    }
    // AuditAck 这类业务帧本身只能来自主进程；执行器不产生它——relay 层
    // 无法区分来源，但其唯一写入口是 wrap_to_master(执行器消息)，管理帧
    // 已被 routable() 拒绝。
    use flow_engine::execution_protocol::remote::routable;
    assert!(!routable(&Message::AgentWelcome {
        journal_id: "j".into(),
        master_epoch: 1,
        link_session_id: "l".into(),
        agent_id: "a".into(),
        data_credential: "c".into(),
        heartbeat_ms: 1,
        agent_window_bytes: 1,
    }));
}

/// 三期回归（P0）：远程执行器产生的非 inline 值（http_call 的 Bytes 编码
/// body_raw、>64KiB 的 script 输出）必须携带**主进程 journal_id**——reducer
/// 会拒绝 journal_id 不符的 ValuePublished（value.rs 的硬校验）。历史上
/// agent 把自己的 agent_id 当 journal_id 传给执行器握手，导致所有 http/
/// llm/email 节点与所有大输出在 remote 模式下必然提交失败；当时的验收用例
/// 只用了小 inline 输出，恰好绕开了这条路径。
#[tokio::test]
async fn remote_http_and_large_output_commit_refs() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    init_tracing();
    // 本地 HTTP stub：响应体 >64KiB（超 INLINE_BYTES，必走 Ref）。
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let response = json!({"data": "x".repeat(70_000)}).to_string();
    let server = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut request = [0u8; 4096];
            let _ = socket.read(&mut request).await;
            socket
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        response.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });

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
    spawn_simulated_agent(&manager, "agent-ref", 4).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let definition = json!({
        "nodes": [
            {"id":"s","type":"start"},
            {"id":"h","type":"http_call","params":{"url": format!("http://{addr}/v1/data")}},
            {"id":"big","type":"script","params":{"code": "return { blob: 'y'.repeat(70000) };"}},
            {"id":"e","type":"end"}
        ],
        "edges": [
            {"from":"s","to":"h"},
            {"from":"h","to":"big"},
            {"from":"big","to":"e"}
        ]
    });
    let flow = install(&backend, definition).await;
    let created = backend
        .run_start(&flow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    let done = until(&backend, &run_id, Run::terminal).await;
    assert_eq!(
        done.status,
        "succeeded",
        "remote 非_inline 值必须能提交：{:?}; nodes: {:?}",
        done.error,
        done.nodes
            .iter()
            .map(|(id, n)| (id.clone(), n.status.clone(), n.error.clone()))
            .collect::<Vec<_>>()
    );

    // http 输出是 Bytes Ref：journal_id 必须是主进程 journal 的 id。
    let http_output = done.nodes["h"].output.clone().unwrap();
    let expected_journal = backend.journal.id().to_string();
    for (node_id, output) in [
        ("h", http_output.clone()),
        ("big", done.nodes["big"].output.clone().unwrap()),
    ] {
        let StoredValue::Ref(reference) = output else {
            panic!("{node_id} 输出应为 Ref（非 inline）");
        };
        assert_eq!(
            reference.journal_id, expected_journal,
            "{node_id} 的 ValueRef 必须盖主进程 journal_id"
        );
    }
    let http_value = materialize(&backend, &done, "h").await;
    assert_eq!(http_value["body"]["data"].as_str().unwrap().len(), 70_000);
    let big_value = materialize(&backend, &done, "big").await;
    assert_eq!(big_value["blob"].as_str().unwrap().len(), 70_000);
    backend.close().await.unwrap();
    server.abort();
}
