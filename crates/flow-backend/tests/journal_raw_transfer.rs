//! 大载荷 raw 传输（T12）：`TransferChunk` 在帧内作为 raw blob 承载（不 base64）。
//! 覆盖本地 IPC 与远程 agent 两条接入，并验证大输入逐字节往返。

use flow_agent::runtime::{serve_for_tests, AgentConfig};
use flow_backend::journal::JournalBackend;
use flow_engine::execution_protocol::contract::ExecutorInvocation;
use flow_engine::execution_protocol::transport::FrameTransport;
use flow_engine::journal_state::{Run, RunStatus};
use flow_journal::JournalOptions;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-raw-{}", uuid::Uuid::now_v7()))
}

fn ipc_options(tag: &str) -> flow_backend::execution::IpcOptions {
    flow_backend::execution::IpcOptions {
        executor: ExecutorInvocation::explicit(flow_test_support::io::executor_bin()),
        x_max: 1,
        tag: Some(tag.to_string()),
    }
}

fn remote_options() -> flow_backend::execution::remote::RemoteOptions {
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

/// 大输入（> INLINE_BYTES）走 TransferChunk raw 路径；脚本回显其长度。
fn big_input_flow() -> Value {
    json!({"nodes":[
        {"id":"s","type":"start"},
        {"id":"n","type":"script","params":{"code":"return {len: input.blob.length};"}},
        {"id":"e","type":"end"}],
        "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]})
}

async fn assert_len_output(b: &Arc<JournalBackend>, done: &Run) {
    let output = done.nodes["n"].output.clone().unwrap();
    let value = match output {
        flow_journal::StoredValue::Inline(value) => value,
        flow_journal::StoredValue::Ref(reference) => flow_journal::value::materialize(
            b.journal.root(),
            b.journal.durable_lsn(),
            &flow_journal::StoredValue::Ref(reference),
            8 * 1024 * 1024,
        )
        .unwrap(),
    };
    assert_eq!(
        value["len"],
        100 * 1024,
        "raw transfer must round-trip intact"
    );
}

#[tokio::test]
async fn ipc_raw_transfer_end_to_end() {
    let root = temp();
    let backend = JournalBackend::open(&root.join("data"), JournalOptions::default())
        .await
        .unwrap();
    let tag = format!("raw-{}", uuid::Uuid::now_v7().simple());
    backend
        .start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(ipc_options(
            &tag,
        )))
        .await
        .unwrap();
    let workflow = install(&backend, big_input_flow()).await;
    let blob = "x".repeat(100 * 1024);
    let created = backend
        .run_start(&workflow, None, json!({"blob": blob}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    let done = until(&backend, &run_id, Run::terminal).await;
    assert_eq!(done.status, RunStatus::Succeeded, "{:?}", done.error);
    assert_len_output(&backend, &done).await;
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

type Handles =
    Arc<tokio::sync::Mutex<std::collections::BTreeMap<String, flow_agent::pool::ExecutorHandle>>>;

async fn spawn_agent(
    manager: &Arc<flow_backend::execution::remote::AgentManager>,
    agent_id: &str,
    slots: u32,
    handles: Handles,
) {
    let (control_a, control_b) = tokio::io::duplex(1024 * 1024);
    manager
        .attach_inprocess(agent_id.to_string(), control_a)
        .await;
    let config = AgentConfig {
        agent_id: agent_id.into(),
        control_addr: "unused".into(),
        ca_cert: Default::default(),
        cert: Default::default(),
        key: Default::default(),
        executor: ExecutorInvocation::explicit(flow_test_support::io::executor_bin()),
        slots,
        wss_url: None,
        token: None,
    };
    let shutdown = tokio_util::sync::CancellationToken::new();
    let agent_id = agent_id.to_string();
    tokio::spawn(async move {
        use flow_engine::execution_protocol::Message;
        let mut transport = FrameTransport::spawn(control_b, 128);
        let boot = flow_engine::execution_protocol::transport::fresh_boot_id();
        if transport
            .send(Message::AgentHello {

                    version: flow_engine::execution_protocol::PROTOCOL_VERSION,agent_boot_id: boot.clone(),
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

                version: flow_engine::execution_protocol::PROTOCOL_VERSION,journal_id,
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
            shutdown,
        )
        .await;
    });
}

#[tokio::test]
async fn remote_raw_transfer_end_to_end() {
    let root = temp();
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
    let handles: Handles = Arc::new(tokio::sync::Mutex::new(Default::default()));
    spawn_agent(&manager, "agent-raw", 2, handles).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let workflow = install(&backend, big_input_flow()).await;
    let blob = "z".repeat(100 * 1024);
    let created = backend
        .run_start(&workflow, None, json!({"blob": blob}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    let done = until(&backend, &run_id, Run::terminal).await;
    assert_eq!(done.status, RunStatus::Succeeded, "{:?}", done.error);
    assert_len_output(&backend, &done).await;
    manager.shutdown().await;
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
