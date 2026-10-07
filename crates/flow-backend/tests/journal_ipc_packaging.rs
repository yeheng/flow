//! I09：IPC 开发打包与升级入口回归。
//!
//! - 执行器定位（显式路径/缺失报错，不静默回退进程内执行）
//! - 路径带空格可运行
//! - 模式环境变量解析与非法值拒绝
//! - 版本不兼容（假二进制）在握手处明确失败

use flow_backend::journal::JournalBackend;
use flow_journal::JournalOptions;
use serde_json::json;
use std::time::Duration;

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-journal-ipc9-{}", uuid::Uuid::now_v7()))
}

fn flow_bin() -> std::path::PathBuf {
    flow_test_support::io::flow_bin()
}

async fn install(b: &JournalBackend, definition: serde_json::Value) -> String {
    let created = b.workflow_create("test", None).await.unwrap();
    let id = created.result["workflow_id"].as_str().unwrap().to_string();
    b.workflow_update(&id, definition, None).await.unwrap();
    b.workflow_publish(&id, 1, None).await.unwrap();
    id
}

#[test]
fn mode_env_parsing_rejects_invalid_values() {
    use flow_backend::execution::{mode_from_env_with, ExecutionMode};
    for value in ["", "in_process"] {
        assert!(
            matches!(mode_from_env_with(value), Ok(ExecutionMode::InProcess)),
            "{value}"
        );
    }
    assert!(matches!(
        mode_from_env_with("ipc"),
        Ok(ExecutionMode::Ipc(_))
    ));
    // Remote startup uses remote_options_from_env, not the local/IPC parser.
    for value in ["remote", "bogus"] {
        assert!(mode_from_env_with(value).is_err(), "{value}");
    }
}

#[tokio::test]
async fn missing_executor_binary_fails_loudly_without_fallback() {
    let root = temp();
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    let workflow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"n","type":"script","params":{"code":"return 1;"}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),
    )
    .await;
    let created = backend
        .run_start(&workflow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    // 指向不存在的执行器：启动即失败，不静默回退。
    let result = backend
        .start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
            flow_backend::execution::IpcOptions {
                executor: flow_engine::execution_protocol::contract::ExecutorInvocation::explicit(
                    root.join("does-not-exist"),
                ),
                x_max: 1,
                tag: None,
            },
        ))
        .await;
    assert!(result.is_err(), "missing binary must fail startup");
    // 不回退：节点不执行（保持无派发），run 不被进程内路径悄悄完成。
    tokio::time::sleep(Duration::from_millis(200)).await;
    let run = backend.state().await.runs[&run_id].clone();
    assert!(run.nodes.is_empty(), "no silent in-process fallback");
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn executor_path_with_spaces_runs() {
    let root = temp();
    // 带空格目录中的副本（放在数据目录之外，避免污染空目录校验）。
    let spaced = std::env::temp_dir().join(format!("flow spaced {}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&spaced).unwrap();
    let copy = spaced.join("flow-executor");
    std::fs::copy(flow_bin(), &copy).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&copy, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    backend
        .start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
            flow_backend::execution::IpcOptions {
                executor: flow_engine::execution_protocol::contract::ExecutorInvocation::merged(
                    copy,
                ),
                x_max: 1,
                tag: Some("spaced".into()),
            },
        ))
        .await
        .unwrap();
    let workflow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"n","type":"script","params":{"code":"return {ok: true};"}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),
    )
    .await;
    let created = backend
        .run_start(&workflow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    let done = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let run = backend.state().await.runs[&run_id].clone();
            if run.terminal() {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("run terminates");
    assert_eq!(done.status, "succeeded");
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
    std::fs::remove_dir_all(spaced).unwrap();
}

#[tokio::test]
async fn incompatible_executor_binary_fails_handshake_and_node_errors() {
    let root = temp();
    // 假二进制：立即退出（版本不兼容/无法握手）；放在数据目录之外。
    let fake_dir = std::env::temp_dir().join(format!("flow-fake-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&fake_dir).unwrap();
    let fake = fake_dir.join("fake-executor");
    #[cfg(unix)]
    {
        std::fs::write(&fake, "#!/bin/sh\nexit 1\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    backend
        .start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
            flow_backend::execution::IpcOptions {
                executor: flow_engine::execution_protocol::contract::ExecutorInvocation::explicit(
                    fake,
                ),
                x_max: 1,
                tag: None,
            },
        ))
        .await
        .unwrap();
    let workflow = install(
        &backend,
        json!({"nodes":[
            {"id":"s","type":"start"},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"e"}]}),
    )
    .await;
    let created = backend
        .run_start(&workflow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    // 派发在握手失败处报错；节点失败，不静默回退。
    let done = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let run = backend.state().await.runs[&run_id].clone();
            if run.terminal() {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("run terminates");
    assert_eq!(
        done.status, "failed",
        "incompatible binary must fail loudly"
    );
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
    std::fs::remove_dir_all(fake_dir).unwrap();
}
