//! I09：IPC 开发打包与升级入口回归。
//!
//! - 执行器定位（显式路径/兄弟文件缺失报错，不静默回退进程内执行）
//! - 部署形态：flow-journal-dev 与 flow-executor 同目录（含带空格路径）
//!   且不设 FLOW_EXECUTOR_BIN 时，locate() 找到兄弟执行器并真实执行
//! - 模式环境变量解析与非法值拒绝
//! - 版本不兼容（假二进制）在握手处明确失败

use flow_backend::journal::JournalBackend;
use flow_domain::journal_state::RunStatus;
use flow_journal::JournalOptions;
use flow_test_support::io::LocalDeployment;
use serde_json::json;
use std::time::Duration;

fn temp() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("flow-journal-ipc9-{}", uuid::Uuid::now_v7()))
}

fn executor_bin() -> std::path::PathBuf {
    flow_test_support::io::executor_bin()
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
    // 进程内执行路径已全平台删除——"in_process" 必须明确报错，而不是
    // 静默改走 ipc；空值（未设置）默认 ipc。
    {
        let err = mode_from_env_with("in_process").expect_err("in_process removed");
        assert!(err.contains("in_process"), "{err}");
        std::env::set_var("FLOW_EXECUTOR_BIN", executor_bin());
        assert!(matches!(mode_from_env_with(""), Ok(ExecutionMode::Ipc(_))));
        std::env::remove_var("FLOW_EXECUTOR_BIN");
        // 未设置 + 定位不到执行器 → 明确报错，不静默回退。
        assert!(mode_from_env_with("").is_err());
    }
    // ipc 的解析包含执行器定位（I09：定位不到就报错，不静默回退）：
    // 给一个真实存在的二进制即可定位成功。本用例是本文件里唯一读写
    // FLOW_EXECUTOR_BIN 的地方，改环境变量不影响并行用例。
    std::env::set_var("FLOW_EXECUTOR_BIN", executor_bin());
    assert!(matches!(
        mode_from_env_with("ipc"),
        Ok(ExecutionMode::Ipc(_))
    ));
    std::env::remove_var("FLOW_EXECUTOR_BIN");
    // 既无显式路径、当前可执行文件旁也无兄弟 flow-executor（测试进程在
    // deps/ 里运行）→ 明确报错。
    let err = mode_from_env_with("ipc").expect_err("locate must fail");
    assert!(err.contains("cannot locate flow-executor"), "{err}");
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
                executor:
                    flow_dispatcher::execution_protocol::contract::ExecutorInvocation::explicit(
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
    let run = backend.state().await.unwrap().runs[&run_id].clone();
    assert!(run.nodes.is_empty(), "no silent in-process fallback");
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

/// Exercise the packaged entry point with no optional binaries or manifests.
#[tokio::test]
async fn deployed_sibling_executor_runs_in_spaced_directory() {
    let temp = tempfile::tempdir().unwrap();
    let deployment = LocalDeployment::copy_to(&temp.path().join("local bundle")).unwrap();
    let data = temp.path().join("data");
    let definition = temp.path().join("definition.json");
    let missing_plugin = temp.path().join("missing-plugin.json");
    let flow = json!({"nodes":[
        {"id":"s","type":"start"},
        {"id":"n","type":"script","params":{"code":"return {ok: true};"}},
        {"id":"e","type":"end"}],
        "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]});
    std::fs::write(&definition, serde_json::to_vec(&flow).unwrap()).unwrap();
    let mut missing = flow;
    missing["nodes"][1]["type"] = json!("absent-extension");
    std::fs::write(&missing_plugin, serde_json::to_vec(&missing).unwrap()).unwrap();

    let output = local_output(
        deployment
            .journal(&data)
            .arg("run")
            .arg("--definition")
            .arg(&missing_plugin),
    )
    .await;
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success() && error.contains("absent-extension"),
        "{error}"
    );

    let output = local_output(
        deployment
            .journal(&data)
            .arg("run")
            .arg("--definition")
            .arg(&definition),
    )
    .await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let run: serde_json::Value = serde_json::from_slice(
        output
            .stdout
            .rsplit(|&b| b == b'\n')
            .find(|line| !line.is_empty())
            .unwrap(),
    )
    .unwrap();
    assert_eq!(run["status"], "succeeded");
    assert_eq!(
        run["nodes"]["n"]["output"],
        json!({"type":"inline","value":{"ok":true}})
    );
    let report = local_output(deployment.journal(&data).arg("check-projection")).await;
    assert!(
        report.status.success(),
        "{}",
        String::from_utf8_lossy(&report.stderr)
    );

    std::fs::remove_file(deployment.executor()).unwrap();
    let without_worker = temp.path().join("without-worker");
    let output = local_output(
        deployment
            .journal(&without_worker)
            .arg("run")
            .arg("--definition")
            .arg(&definition),
    )
    .await;
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success() && error.contains("cannot locate flow-executor"),
        "{error}"
    );
    assert!(output.stdout.is_empty(), "startup failure issued a receipt");
    let status = local_output(deployment.journal(&without_worker).arg("status")).await;
    assert!(status.status.success(), "{:?}", status);
    let state: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(state["workflows"], json!({}));
    assert_eq!(state["runs"], json!({}));
}

async fn local_output(cmd: &mut tokio::process::Command) -> std::process::Output {
    tokio::time::timeout(Duration::from_secs(30), cmd.output())
        .await
        .expect("local deployment command must finish")
        .expect("spawn local deployment command")
}

#[tokio::test]
async fn invalid_run_submission_leaves_recovered_runs_dormant() {
    let root = tempfile::tempdir().unwrap();
    let deployment = LocalDeployment::copy_to(&root.path().join("local bundle")).unwrap();
    let data = root.path().join("data");
    let definition = json!({
        "nodes": [{"id":"s","type":"start"}, {"id":"e","type":"end"}],
        "edges": [{"from":"s","to":"e"}]
    });
    let backend = JournalBackend::open(&data, JournalOptions::default())
        .await
        .unwrap();
    let workflow = install(&backend, definition.clone()).await;
    let receipt = backend
        .run_start(
            &workflow,
            None,
            serde_json::Value::Null,
            "manual",
            None,
            None,
        )
        .await
        .unwrap();
    let pending = receipt.result["run_id"].as_str().unwrap();
    backend.close().await.unwrap();
    drop(backend);

    let mut invalid = definition;
    invalid["input_schema"] = json!({"type": "object", "required": ["value"]});
    let path = root.path().join("invalid-input.json");
    std::fs::write(&path, serde_json::to_vec(&invalid).unwrap()).unwrap();
    let rejected = local_output(
        deployment
            .journal(&data)
            .arg("run")
            .arg("--definition")
            .arg(&path),
    )
    .await;
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("input contract"));
    assert!(rejected.stdout.is_empty());

    let status = local_output(deployment.journal(&data).arg("status")).await;
    assert!(status.status.success(), "{status:?}");
    let state: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(
        state["master_epoch"], "0",
        "validation must not start the coordinator"
    );
    assert_eq!(state["runs"].as_object().unwrap().len(), 1);
    assert_eq!(state["runs"][pending]["nodes"], json!({}));

    let resumed = local_output(deployment.journal(&data).arg("resume")).await;
    assert!(resumed.status.success(), "{resumed:?}");
    let run: serde_json::Value = serde_json::from_slice(&resumed.stdout).unwrap();
    assert_eq!(run["run_id"], pending);
    assert_eq!(run["status"], "succeeded");
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
                executor:
                    flow_dispatcher::execution_protocol::contract::ExecutorInvocation::explicit(
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
            let run = backend.state().await.unwrap().runs[&run_id].clone();
            if run.terminal() {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("run terminates");
    assert_eq!(
        done.status,
        RunStatus::Failed,
        "incompatible binary must fail loudly"
    );
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
    std::fs::remove_dir_all(fake_dir).unwrap();
}

/// T10 验收：父端关闭（backend.close）后执行器被 kill+wait/reap，无孤儿
/// 进程、无僵尸。用唯一 --tag 做进程定位。
#[tokio::test]
async fn parent_close_reaps_executor_without_orphans() {
    let root = temp();
    let tag = format!("t10-reap-{}", uuid::Uuid::now_v7());
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    backend
        .start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
            flow_backend::execution::IpcOptions {
                executor:
                    flow_dispatcher::execution_protocol::contract::ExecutorInvocation::explicit(
                        executor_bin(),
                    ),
                x_max: 1,
                tag: Some(tag.clone()),
            },
        ))
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
    let done = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let run = backend.state().await.unwrap().runs[&run_id].clone();
            if run.terminal() {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("run terminates");
    assert_eq!(done.status, RunStatus::Succeeded);
    backend.close().await.unwrap();
    // 回收落定后不得残留该 tag 的执行器进程。
    let leaked = wait_pgrep(&tag, Duration::from_secs(5)).await;
    assert!(
        !leaked,
        "executor must be reaped after parent close (tag={tag})"
    );
    std::fs::remove_dir_all(root).unwrap();
}

/// T10 验收：连续取消与重建不泄漏进程/FD（每次回收后重新召唤）。
#[tokio::test]
async fn repeated_cancel_and_rebuild_leaves_no_orphans() {
    let root = temp();
    let tag = format!("t10-rebuild-{}", uuid::Uuid::now_v7());
    let backend = JournalBackend::open(&root, JournalOptions::default())
        .await
        .unwrap();
    backend
        .start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
            flow_backend::execution::IpcOptions {
                executor:
                    flow_dispatcher::execution_protocol::contract::ExecutorInvocation::explicit(
                        executor_bin(),
                    ),
                x_max: 1,
                tag: Some(tag.clone()),
            },
        ))
        .await
        .unwrap();
    // 挂起 HTTP：任务停在外部操作等待，取消后执行器回收并重建。
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hang = listener.local_addr().unwrap();
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
            {"id":"h","type":"http_call","params":{"url": format!("http://{hang}/hang"), "timeout_ms": 3000}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"h"},{"from":"h","to":"e"}]}),
    )
    .await;
    for _ in 0..3 {
        let created = backend
            .run_start(&workflow, None, json!({}), "manual", None, None)
            .await
            .unwrap();
        let run_id = created.result["run_id"].as_str().unwrap().to_string();
        // 等到任务进入外部操作等待再取消。
        let _ = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let run = backend.state().await.unwrap().runs[&run_id].clone();
                if run.nodes.get("h").is_some_and(|n| n.operation.is_some()) || run.terminal() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        backend.run_cancel(&run_id, None).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let run = backend.state().await.unwrap().runs[&run_id].clone();
                if run.terminal() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
    }
    backend.close().await.unwrap();
    let leaked = wait_pgrep(&tag, Duration::from_secs(5)).await;
    assert!(
        !leaked,
        "no executor may leak across cancel/rebuild (tag={tag})"
    );
    std::fs::remove_dir_all(root).unwrap();
}

/// T10 验收：env_clear 与 FD 隔离。包装脚本作为执行器入口，先落盘自身
/// 环境与打开的 FD，再 exec 真实执行器；运行成功后断言：
/// - 父进程设置的无关环境变量（含凭据样例）不出现（env_clear）；
/// - 父进程持有的非通道 FD 不出现（pre_exec 关闭继承 FD）。
#[cfg(unix)]
#[tokio::test]
async fn executor_child_gets_cleared_env_and_isolated_fds() {
    use std::os::unix::fs::PermissionsExt;
    let root = temp();
    std::fs::create_dir_all(&root).unwrap();
    let dump = root.join("child-env.txt");
    let fd_dump = root.join("child-fds.txt");
    // 父进程持有的“泄漏候选”文件：FD 隔离后不应出现在子进程里。
    let leaked_path = root.join(format!("leaked-{}", uuid::Uuid::now_v7()));
    std::fs::write(&leaked_path, b"secret").unwrap();
    let _leaked_fd = std::fs::File::open(&leaked_path).unwrap();

    // 包装脚本：解析 --channel-fd 作为阳性对照（通道 FD 必须在子进程可见），
    // 同时列出 /dev/fd 与自身环境，最后 exec 真实执行器。
    let wrapper = root.join("flow-executor");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nchan=\"\"\nprev=\"\"\nfor a in \"$@\"; do\n  if [ \"$prev\" = \"--channel-fd\" ]; then chan=\"$a\"; fi\n  prev=\"$a\"\ndone\n{{\n  echo \"channel_fd=$chan\"\n  if [ -n \"$chan\" ] && [ -e \"/dev/fd/$chan\" ]; then echo \"channel_present=yes\"; else echo \"channel_present=no\"; fi\n  ls -l /dev/fd 2>&1 || true\n}} > {fds}\nenv > {dump}\nexec {real} \"$@\"\n",
            dump = dump.display(),
            fds = fd_dump.display(),
            real = executor_bin().display(),
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();

    let backend = JournalBackend::open(&root.join("data"), JournalOptions::default())
        .await
        .unwrap();
    // 唯一 marker：父进程环境里存在，子进程必须看不到。
    std::env::set_var("FLOW_T10_LEAK", "credential-must-not-leak");
    backend
        .start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
            flow_backend::execution::IpcOptions {
                executor:
                    flow_dispatcher::execution_protocol::contract::ExecutorInvocation::explicit(
                        wrapper.clone(),
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
            {"id":"n","type":"script","params":{"code":"return 2;"}},
            {"id":"e","type":"end"}],
            "edges":[{"from":"s","to":"n"},{"from":"n","to":"e"}]}),
    )
    .await;
    let created = backend
        .run_start(&workflow, None, json!({}), "manual", None, None)
        .await
        .unwrap();
    let run_id = created.result["run_id"].as_str().unwrap().to_string();
    let done = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let run = backend.state().await.unwrap().runs[&run_id].clone();
            if run.terminal() {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("run terminates");
    std::env::remove_var("FLOW_T10_LEAK");
    assert_eq!(
        done.status,
        RunStatus::Succeeded,
        "wrapper passthrough must work"
    );

    let env_dump = std::fs::read_to_string(&dump).expect("env dump written");
    assert!(
        !env_dump.contains("FLOW_T10_LEAK"),
        "env_clear must drop parent-only variables:\n{env_dump}"
    );
    assert!(
        !env_dump.contains("credential-must-not-leak"),
        "credential value must not leak"
    );
    let fd_list = std::fs::read_to_string(&fd_dump).unwrap_or_default();
    // 阳性对照：通道 FD 必须存活（证明 FD 观测有效）。
    assert!(
        fd_list.contains("channel_present=yes"),
        "channel fd must survive exec:\n{fd_list}"
    );
    let leaked_name = leaked_path
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    assert!(
        !fd_list.contains(&leaked_name),
        "inherited non-channel fd must be closed:\n{fd_list}"
    );
    backend.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

/// 轮询 pgrep：等待孤儿进程消失（回收是异步的），返回是否仍存在。
async fn wait_pgrep(tag: &str, within: Duration) -> bool {
    let pattern = format!("flow-executor --tag {tag}");
    let deadline = tokio::time::Instant::now() + within;
    loop {
        let found = std::process::Command::new("pgrep")
            .args(["-f", &pattern])
            .output()
            .map(|out| out.status.success())
            .unwrap_or(false);
        if !found {
            return false;
        }
        if tokio::time::Instant::now() >= deadline {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
