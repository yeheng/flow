//! harness agent 模式（mode=agent）的契约测试：假 runner 脚本替身覆盖
//! 「密钥走 stdin、env 只有非敏感矩阵、输出派生、回归继承 baseline 配置」。
//! 真实 runner + mock provider 的联网链路在 agent_provider e2e 里覆盖。

use flow_backend::journal::JournalBackend;
use flow_engine::journal_state::{Run, RunStatus};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

/// 成功形状的假 runner：把 stdin 载荷与环境快照落盘（供泄漏断言），输出
/// 固定结果行。`capture` 是脚本生成时烧进去的绝对路径。
fn fake_runner(capture: &std::path::Path) -> std::path::PathBuf {
    let script = format!(
        "#!/bin/sh\n\
         input=$(cat)\n\
         printf '%s' \"$input\" > '{capture}'\n\
         env | sort > '{env}'\n\
         printf '{{\"text\":\"agent says hi\",\"stop_reason\":\"end_turn\",\"error\":null}}\\n'\n",
        capture = capture.display(),
        env = capture.with_extension("env").display(),
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fake-runner");
    std::fs::write(&path, &script).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    // tempdir 会在函数返回时删除：把脚本移到测试期目录（由调用方持有）
    let kept = std::env::temp_dir().join(format!("fake-runner-{}", std::process::id()));
    std::fs::write(&kept, &script).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&kept, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    kept
}

fn agent_config() -> flow_engine::AgentRunConfig {
    flow_engine::AgentRunConfig {
        base_url: "https://api.example.internal/v1".into(),
        model: "contract-model".into(),
        api: "openai".into(),
        proxy: None,
        max_turns: Some(7),
        max_tool_calls_per_turn: None,
        max_tokens: Some(1024),
        tool_timeout_secs: None,
    }
}

fn agent_definition(workdir: &str) -> Value {
    json!({
        "semantics": 2,
        "nodes": [
            {"id": "s", "type": "start"},
            {"id": "a", "type": "harness", "params": {
                "mode": "agent",
                "prompt": "summarize the repo",
                "workdir": workdir,
                "api_key": "E2EAGENT",
                "timeout_ms": 20_000,
            }},
            {"id": "e", "type": "end"}
        ],
        "edges": [
            {"from": "s", "to": "a"},
            {"from": "a", "to": "e"}
        ]
    })
}

async fn start_driver(b: &Arc<JournalBackend>) {
    b.start_execution_ipc(flow_backend::execution::ExecutionMode::Ipc(
        flow_backend::execution::IpcOptions {
            executor: flow_engine::execution_protocol::contract::ExecutorInvocation::explicit(
                flow_test_support::io::executor_bin(),
            ),
            x_max: 2,
            tag: None,
        },
    ))
    .await
    .unwrap();
}

async fn until(b: &JournalBackend, id: &str, done: impl Fn(&Run) -> bool) -> Run {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let run = b.run_snapshot(id).await.unwrap().unwrap();
            if done(&run) {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn agent_node_contract_stdin_key_env_matrix_and_regression_inheritance() {
    let dir = tempfile::tempdir().unwrap();
    let capture = dir.path().join("stdin.json");
    let runner = fake_runner(&capture);
    // 单测进程独占：runner 定位与密钥兜底都是进程环境（executor 由本进程
    // spawn，白名单会转发这两个值）。
    std::env::set_var("FLOW_HARNESS_AGENT_BIN", &runner);
    std::env::set_var("FLOW_SECRET_E2EAGENT", "sk-e2e-secret-value");

    let workdir = dir.path().join("workdir");
    std::fs::create_dir_all(&workdir).unwrap();

    // ---- backend 1：安装配置 A，跑通 agent 节点 ----
    let b = JournalBackend::open(&dir.path().join("journal"), Default::default())
        .await
        .unwrap();
    b.install_harness_agent(Some(agent_config())).unwrap();
    let b = Arc::new(b);
    start_driver(&b).await;

    let wf = {
        let id = b
            .workflow_create("agent-contract", None)
            .await
            .unwrap()
            .result["workflow_id"]
            .as_str()
            .unwrap()
            .to_owned();
        b.workflow_update(&id, agent_definition(&workdir.to_string_lossy()), None)
            .await
            .unwrap();
        b.workflow_publish(&id, 1, None).await.unwrap();
        id
    };
    let id = b
        .run_start(&wf, None, Value::Null, "manual", None, None)
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let baseline = until(&b, &id, Run::terminal).await;
    assert_eq!(
        baseline.status,
        RunStatus::Succeeded,
        "agent run 应成功（error: {:?}）",
        baseline.error
    );

    // stdin 契约：api_key 真值 + prompt + canonical workdir
    let stdin_payload: Value =
        serde_json::from_str(&std::fs::read_to_string(&capture).unwrap()).unwrap();
    assert_eq!(stdin_payload["api_key"], "sk-e2e-secret-value");
    assert_eq!(stdin_payload["prompt"], "summarize the repo");
    assert_eq!(
        stdin_payload["workdir"].as_str().unwrap(),
        workdir.canonicalize().unwrap().to_string_lossy()
    );

    // env 契约：非敏感 CONGA_* 矩阵在；密钥/协议/日志变量不在
    let env_snapshot = std::fs::read_to_string(capture.with_extension("env")).unwrap();
    for required in [
        "CONGA_LLM_BASE_URL=https://api.example.internal/v1",
        "CONGA_LLM_MODEL=contract-model",
        "CONGA_LLM_API=openai",
        "CONGA_MAX_TURNS=7",
        "CONGA_MAX_TOKENS=1024",
        "HOME=",
        "PATH=",
    ] {
        let (name, _) = required.split_once('=').unwrap();
        assert!(
            env_snapshot.lines().any(|line| line.starts_with(name)),
            "runner env 应包含 {name}：\n{env_snapshot}"
        );
    }
    assert!(
        env_snapshot
            .lines()
            .any(|line| line == "CONGA_LLM_BASE_URL=https://api.example.internal/v1"),
        "矩阵值不符：\n{env_snapshot}"
    );
    for banned in [
        "CONGA_LLM_KEY",
        "RUST_LOG",
        "FLOW_SECRET_E2EAGENT",
        "CONGA_CONFIG_DIR",
        "CONGA_GATEWAY_MODE",
        "CONGA_PROJECT_DIR",
    ] {
        assert!(
            !env_snapshot.lines().any(|line| line.starts_with(banned)),
            "runner env 不该包含 {banned}：\n{env_snapshot}"
        );
    }
    assert!(
        !env_snapshot.contains("sk-e2e-secret-value"),
        "密钥真值不得出现在 runner 环境的任何位置"
    );

    // 节点输出：stdout = runner 文本，agent 元数据随输出
    let node = baseline.nodes["a"].output.clone();
    let inline = match node {
        Some(flow_journal::StoredValue::Inline(value)) => value,
        other => panic!("agent 节点输出应为 inline：{other:?}"),
    };
    assert_eq!(inline["stdout"], "agent says hi");
    assert_eq!(inline["exit_code"], 0);
    assert_eq!(inline["agent"]["model"], "contract-model");
    assert_eq!(inline["agent"]["stop_reason"], "end_turn");

    // ---- 回归：同 backend，fixture 匹配（0 runner 调用）----
    let calls_before = std::fs::read_to_string(&capture).unwrap().len();
    let candidate = b
        .run_regression(&id, None, None, "same")
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    until(&b, &candidate, Run::terminal).await;
    assert_eq!(
        b.compare_runs(&id, &candidate).await.unwrap()["passed"],
        true,
        "回归候选应与 baseline 一致"
    );
    assert_eq!(
        std::fs::read_to_string(&capture).unwrap().len(),
        calls_before,
        "回归不得再次调用 runner"
    );

    // ---- backend 2：不安装配置重开同一 journal，回归仍匹配 ----
    // 这是对「回归候选继承 baseline 冻结配置」的判别测试：若候选按当前
    // 安装（None）构造，`_flow_execution.agent` 缺失会让 executor 直接
    // Business「配置未随派发下发」，指纹比较都到不了。
    b.close().await.unwrap();
    drop(b);
    let b2 = JournalBackend::open(&dir.path().join("journal"), Default::default())
        .await
        .unwrap();
    let b2 = Arc::new(b2);
    start_driver(&b2).await;
    let candidate2 = b2
        .run_regression(&id, None, None, "restart")
        .await
        .unwrap()
        .result["run_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let restarted = until(&b2, &candidate2, Run::terminal).await;
    assert_eq!(
        restarted.status,
        RunStatus::Succeeded,
        "重开且未安装配置的回归应成功"
    );
    assert_eq!(
        b2.compare_runs(&id, &candidate2).await.unwrap()["passed"],
        true
    );
    assert_eq!(
        std::fs::read_to_string(&capture).unwrap().len(),
        calls_before,
        "重启后的回归同样 0 runner 调用"
    );
    b2.close().await.unwrap();

    std::env::remove_var("FLOW_HARNESS_AGENT_BIN");
    std::env::remove_var("FLOW_SECRET_E2EAGENT");
    let _ = std::fs::remove_file(&runner);
}
