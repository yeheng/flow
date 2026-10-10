//! flow-cli 的端到端测试：**真起被测对象**——`flow-cli` 二进制作为子进程跑，
//! 服务端用 `flow_rpc::serve` + journal 后端（显式路径，不碰环境变量，
//! 测试可以并行）在进程内监听随机端口。
//!
//! 为什么不让测试当客户端直连 JSON-RPC：那测的是服务端。这里要钉住的是
//! CLI 自己的三件事——参数拼装、退出码契约、stdout/stderr 分工（文档走 stdout）。
//! 崩溃恢复、published 校验等语义由 backend-e2e / flow-rpc 的测试负责。
//!
//! 数据落盘遵守仓库约定：每个用例一个独占临时目录（journal 根），只清理该目录。

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::Duration;

use flow_backend::journal::JournalBackend;
const TOKEN: &str = "flow-cli-test-token-at-least-32-bytes";
use flow_test_support::io::TempDir;
use serde_json::{json, Value};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

const CLI_TIMEOUT: Duration = Duration::from_secs(30);

/// 一个独占临时目录，Drop 时整体删除。
/// 只删自己建的目录，不碰系统临时目录本身（DESIGN.md §13）。
///
/// 进程内的 flow-journal-server（随机端口）。持有 handle 即持有服务实例。
struct TestServer {
    addr: SocketAddr,
    _scratch: TempDir,
    handle: Option<jsonrpsee::server::ServerHandle>,
}

impl TestServer {
    async fn spawn() -> TestServer {
        let scratch = TempDir::new("flow-cli-test-server");
        let backend = JournalBackend::open(scratch.path(), Default::default())
            .await
            .expect("打开 journal 后端失败");
        flow_backend::start_execution(&flow_config::ExecutionConfig::default(), &backend)
            .await
            .expect("启动执行驱动失败");
        let (handle, addr) = flow_rpc::journal_v2::serve(
            backend,
            TOKEN.to_owned(),
            SocketAddr::from(([127, 0, 0, 1], 0)),
        )
        .await
        .expect("启动 RPC 服务失败");
        TestServer {
            addr,
            _scratch: scratch,
            handle: Some(handle),
        }
    }

    fn url(&self) -> String {
        format!("ws://{}", self.addr)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            // 停机是尽力而为：测试进程随即退出，卡住比泄漏更糟
            let _ = handle.stop();
        }
    }
}

/// 跑一次 flow-cli，返回 (退出码, stdout, stderr)。
async fn cli(server: &TestServer, args: &[&str]) -> (i32, String, String) {
    let output = run_cli(server, args).await;
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

async fn run_cli(server: &TestServer, args: &[&str]) -> Output {
    tokio::time::timeout(CLI_TIMEOUT, async {
        Command::new(env!("CARGO_BIN_EXE_flow-cli"))
            .env("FLOW_JOURNAL_TOKEN", TOKEN)
            .args(args)
            .env("FLOW_RPC", server.url())
            // 隔离父进程环境：--url / FLOW_RPC 的默认值行为由专门用例覆盖
            .env_remove("FLOW_DB")
            .env_remove("FLOW_DATA_DIR")
            .output()
            .await
            .expect("启动 flow-cli 失败")
    })
    .await
    .expect("flow-cli 超时")
}

fn stdout_json(stdout: &str) -> Value {
    serde_json::from_str(stdout)
        .unwrap_or_else(|err| panic!("stdout 不是合法 JSON：{err}\n{stdout}"))
}

/// 一个不依赖外部网络的最小工作流：start → script → end。
fn sample_workflow_json() -> Value {
    json!({
        "name": "cli-demo",
        "definition": {
            "nodes": [
                {"id": "start", "type": "start", "name": "开始"},
                {"id": "shape", "type": "script", "name": "加工",
                 "params": {"code": "return { greeting: 'hi ' + (input.who ?? 'world') };"}},
                {"id": "end", "type": "end", "name": "结束"}
            ],
            "edges": [
                {"from": "start", "to": "shape"},
                {"from": "shape", "to": "end"}
            ]
        }
    })
}

fn write_json(dir: &Path, name: &str, value: &Value) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, serde_json::to_string_pretty(value).unwrap() + "\n")
        .expect("写测试文件失败");
    path
}

#[tokio::test]
async fn import_then_run_then_export_roundtrip() {
    let server = TestServer::spawn().await;
    let scratch = TempDir::new("import-run");
    let file = write_json(
        scratch.path(),
        "demo.workflow.json",
        &sample_workflow_json(),
    );

    // import：新建 + 发布
    let (code, stdout, stderr) = cli(
        &server,
        &["workflow", "import", file.to_str().unwrap(), "--json"],
    )
    .await;
    assert_eq!(code, 0, "{stderr}");
    let imported = stdout_json(&stdout);
    assert_eq!(imported["created"], json!(true));
    assert_eq!(imported["status"], json!("published"));
    assert_eq!(imported["name"], json!("cli-demo"));
    let workflow_id = imported["workflow_id"].as_str().unwrap().to_string();

    // list：人类可读输出，含 name 与已发布版本
    let (code, stdout, stderr) = cli(&server, &["workflow", "list"]).await;
    assert_eq!(code, 0, "{stderr}");
    assert!(stdout.contains("cli-demo"), "{stdout}");
    assert!(stdout.contains(&workflow_id), "{stdout}");

    // 同名再次 import：追加版本而非新建（run 钉死旧版本不受影响）
    let (code, stdout, _) = cli(
        &server,
        &["workflow", "import", file.to_str().unwrap(), "--json"],
    )
    .await;
    assert_eq!(code, 0);
    let again = stdout_json(&stdout);
    assert_eq!(again["created"], json!(false));
    assert_eq!(again["workflow_id"], json!(workflow_id));

    // 手动触发：默认等待终态，stdout 就是 run 输出（纯 JSON）
    let (code, stdout, stderr) = cli(
        &server,
        &[
            "run",
            "start",
            "cli-demo",
            "--input",
            "{\"who\":\"flow-cli\"}",
            "--json",
        ],
    )
    .await;
    assert_eq!(code, 0, "{stderr}");
    let run = stdout_json(&stdout);
    assert_eq!(run["status"], json!("succeeded"));
    assert_eq!(run["output"]["greeting"], json!("hi flow-cli"));
    let run_id = run["id"].as_str().unwrap().to_string();

    // run list：人读表格 + id 截断说明
    let (code, stdout, stderr) = cli(&server, &["run", "list"]).await;
    assert_eq!(code, 0, "{stderr}");
    // 表格里 id 截断到 12 列（11 字符 + 省略号）
    assert!(stdout.contains(&run_id[..11]), "{stdout}");
    assert!(stderr.contains("id 已截断"), "{stderr}");

    // export → 文件；再 import 该文件（信封格式自洽）
    let exported = scratch.path().join("exported.json");
    let (code, _, stderr) = cli(
        &server,
        &[
            "workflow",
            "export",
            "cli-demo",
            "-o",
            exported.to_str().unwrap(),
        ],
    )
    .await;
    assert_eq!(code, 0, "{stderr}");
    let envelope = stdout_json(&std::fs::read_to_string(&exported).unwrap());
    assert_eq!(envelope["name"], json!("cli-demo"));
    assert_eq!(envelope["definition"]["nodes"][1]["id"], json!("shape"));

    let (code, _, stderr) = cli(
        &server,
        &["workflow", "import", exported.to_str().unwrap(), "--json"],
    )
    .await;
    assert_eq!(code, 0, "{stderr}");
    let reimported = stdout_json(&std::fs::read_to_string(&exported).unwrap());
    assert_eq!(reimported["workflow_id"], json!(workflow_id));
}

#[tokio::test]
async fn get_writes_only_json_to_stdout() {
    let server = TestServer::spawn().await;
    let scratch = TempDir::new("get");
    let file = write_json(
        scratch.path(),
        "demo.workflow.json",
        &sample_workflow_json(),
    );
    let (code, _, stderr) = cli(&server, &["workflow", "import", file.to_str().unwrap()]).await;
    assert_eq!(code, 0, "{stderr}");

    // 说明性元数据走 stderr，stdout 必须是能直接被 json 解析的 definition
    let (code, stdout, stderr) = cli(&server, &["workflow", "get", "cli-demo"]).await;
    assert_eq!(code, 0, "{stderr}");
    assert!(stderr.contains("最新已发布"), "{stderr}");
    let definition = stdout_json(&stdout);
    assert_eq!(definition["nodes"][1]["id"], json!("shape"));
    assert!(definition.get("nodes").is_some());
}

#[tokio::test]
async fn run_start_by_workflow_id_and_name_resolve_alike() {
    let server = TestServer::spawn().await;
    let scratch = TempDir::new("resolve");
    let file = write_json(
        scratch.path(),
        "demo.workflow.json",
        &sample_workflow_json(),
    );
    let (code, stdout, _) = cli(
        &server,
        &["workflow", "import", file.to_str().unwrap(), "--json"],
    )
    .await;
    assert_eq!(code, 0);
    let workflow_id = stdout_json(&stdout)["workflow_id"]
        .as_str()
        .unwrap()
        .to_string();

    for reference in [workflow_id.as_str(), "cli-demo"] {
        let (code, stdout, stderr) =
            cli(&server, &["run", "start", reference, "--input", "{}"]).await;
        assert_eq!(code, 0, "{stderr}");
        // 人类模式：stdout 只打印 run 输出
        let output = stdout_json(&stdout);
        assert_eq!(output["greeting"], json!("hi world"));
    }
}

#[tokio::test]
async fn detach_prints_run_id_only() {
    let server = TestServer::spawn().await;
    let scratch = TempDir::new("detach");
    let file = write_json(
        scratch.path(),
        "demo.workflow.json",
        &sample_workflow_json(),
    );
    let (code, _, stderr) = cli(&server, &["workflow", "import", file.to_str().unwrap()]).await;
    assert_eq!(code, 0, "{stderr}");

    let (code, stdout, stderr) = cli(&server, &["run", "start", "cli-demo", "--detach"]).await;
    assert_eq!(code, 0, "{stderr}");
    let run_id = stdout.trim().to_string();
    assert!(!run_id.is_empty());
    // 说明信息只在 stderr，stdout 可以安全地 `$(...)` 取用
    assert!(stderr.contains(run_id.as_str()), "{stderr}");

    let (code, stdout, stderr) = cli(&server, &["run", "get", &run_id]).await;
    assert_eq!(code, 0, "{stderr}");
    let detail = stdout_json(&stdout);
    assert_eq!(detail["run"]["id"], json!(run_id));
}

#[tokio::test]
async fn failed_run_exits_four_with_server_error() {
    let server = TestServer::spawn().await;
    let scratch = TempDir::new("failed");
    let file = write_json(
        scratch.path(),
        "boom.workflow.json",
        &json!({
            "name": "cli-boom",
            "definition": {
                "nodes": [
                    {"id": "start", "type": "start"},
                    {"id": "b", "type": "script", "params": {"code": "throw new Error('炸了')"}},
                    {"id": "end", "type": "end"}
                ],
                "edges": [
                    {"from": "start", "to": "b"},
                    {"from": "b", "to": "end"}
                ]
            }
        }),
    );
    let (code, _, stderr) = cli(&server, &["workflow", "import", file.to_str().unwrap()]).await;
    assert_eq!(code, 0, "{stderr}");

    let (code, _, stderr) = cli(&server, &["run", "start", "cli-boom"]).await;
    // 退出码 4 = run 非成功终态（不是命令用法错误）
    assert_eq!(code, 4, "{stderr}");
    assert!(stderr.contains("failed"), "{stderr}");
    assert!(stderr.contains("炸了"), "{stderr}");
}

#[tokio::test]
async fn unknown_workflow_exits_two_with_rpc_code() {
    let server = TestServer::spawn().await;
    let (code, _, stderr) = cli(&server, &["run", "start", "no-such-workflow"]).await;
    assert_eq!(code, 2, "{stderr}");
    assert!(stderr.contains("服务端错误 code -32011"), "{stderr}");
}

#[tokio::test]
async fn malformed_definition_is_rejected_locally() {
    let server = TestServer::spawn().await;
    let scratch = TempDir::new("bad-json");
    let file = scratch.path().join("bad.json");
    std::fs::write(&file, "{ 这不是 JSON").unwrap();

    let (code, _, stderr) = cli(
        &server,
        &[
            "workflow",
            "import",
            file.to_str().unwrap(),
            "--name",
            "bad",
        ],
    )
    .await;
    assert_eq!(code, 1, "{stderr}");
    assert!(stderr.contains("不是合法 JSON"), "{stderr}");
    // 本地错误不假报服务端错误码
    assert!(!stderr.contains("code"), "{stderr}");
}

#[tokio::test]
async fn invalid_definition_is_rejected_by_server_validation() {
    let server = TestServer::spawn().await;
    let scratch = TempDir::new("invalid-def");
    let file = write_json(
        scratch.path(),
        "invalid.workflow.json",
        &json!({
            "name": "cli-invalid",
            "definition": {
                // 两个 start：Definition::validate 在保存时就该拒
                "nodes": [
                    {"id": "a", "type": "start"},
                    {"id": "b", "type": "start"},
                    {"id": "end", "type": "end"}
                ],
                "edges": [{"from": "a", "to": "end"}, {"from": "b", "to": "end"}]
            }
        }),
    );
    let (code, _, stderr) = cli(&server, &["workflow", "import", file.to_str().unwrap()]).await;
    assert_eq!(code, 2, "{stderr}");
    assert!(stderr.contains("code -32602"), "{stderr}");
    // 失败不回滚：create 是已发生的副作用，留下一个没有版本的壳，
    // 且不会被当成可执行工作流（latest 0 / published null）
    let (code, stdout, _) = cli(&server, &["workflow", "list", "--json"]).await;
    assert_eq!(code, 0);
    let workflows = stdout_json(&stdout)["workflows"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(workflows.len(), 1);
    assert_eq!(workflows[0]["name"], json!("cli-invalid"));
    assert_eq!(workflows[0]["latest_version"], json!(0));
    assert!(workflows[0]["published_version"].is_null());
}

#[tokio::test]
async fn delete_requires_yes_outside_interactive_use() {
    let server = TestServer::spawn().await;
    let scratch = TempDir::new("delete");
    let file = write_json(
        scratch.path(),
        "demo.workflow.json",
        &sample_workflow_json(),
    );
    let (code, _, stderr) = cli(&server, &["workflow", "import", file.to_str().unwrap()]).await;
    assert_eq!(code, 0, "{stderr}");
    // 先造一条 run 记录：没有 run 时 delete 是允许的（见下）
    let (code, _, stderr) = cli(&server, &["run", "start", "cli-demo", "--detach"]).await;
    assert_eq!(code, 0, "{stderr}");

    // 子进程 stdin 不是终端：必须显式 -y，绝不挂起等输入
    let (code, _, stderr) = cli(&server, &["workflow", "delete", "cli-demo"]).await;
    assert_eq!(code, 1, "{stderr}");
    assert!(stderr.contains("-y"), "{stderr}");
    // 上面的拒绝没有删掉任何东西
    let (code, stdout, _) = cli(&server, &["workflow", "list", "--json"]).await;
    assert_eq!(code, 0);
    assert_eq!(
        stdout_json(&stdout)["workflows"].as_array().unwrap().len(),
        1
    );

    let (code, _, stderr) = cli(&server, &["workflow", "delete", "cli-demo", "-y"]).await;
    assert_eq!(code, 2, "{stderr}");
    // 已有 run 记录时服务端拒删（事件日志不能变孤儿）
    assert!(stderr.contains("code -32012"), "{stderr}");
}

#[tokio::test]
async fn url_flag_overrides_env() {
    let server = TestServer::spawn().await;
    // FLOW_RPC 给一个死地址，--url 指活地址：命令行必须赢
    let output = tokio::time::timeout(CLI_TIMEOUT, async {
        Command::new(env!("CARGO_BIN_EXE_flow-cli"))
            .env("FLOW_JOURNAL_TOKEN", TOKEN)
            .args(["workflow", "list", "--url", &server.url()])
            .env("FLOW_RPC", "ws://127.0.0.1:9")
            .output()
            .await
            .expect("启动 flow-cli 失败")
    })
    .await
    .expect("flow-cli 超时");
    assert!(output.status.success(), "{:?}", output.status);
}

#[tokio::test]
async fn unreachable_server_is_a_local_error_with_hint() {
    // 不起服务：错误必须是 exit 1 + 提示起服务，而不是难懂的传输层堆栈
    let output = tokio::time::timeout(CLI_TIMEOUT, async {
        Command::new(env!("CARGO_BIN_EXE_flow-cli"))
            .env("FLOW_JOURNAL_TOKEN", TOKEN)
            .args(["workflow", "list", "--url", "ws://127.0.0.1:9"])
            .output()
            .await
            .expect("启动 flow-cli 失败")
    })
    .await
    .expect("flow-cli 超时");
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("连不上 flow-journal-server"), "{stderr}");
    assert!(stderr.contains("flow-journal-server"), "{stderr}");
}

#[tokio::test]
async fn events_and_timeline_render_a_finished_run() {
    let server = TestServer::spawn().await;
    let scratch = TempDir::new("events");
    let file = write_json(
        scratch.path(),
        "demo.workflow.json",
        &sample_workflow_json(),
    );
    let (code, _, stderr) = cli(&server, &["workflow", "import", file.to_str().unwrap()]).await;
    assert_eq!(code, 0, "{stderr}");
    let (code, stdout, stderr) = cli(
        &server,
        &["run", "start", "cli-demo", "--input", "{}", "--detach"],
    )
    .await;
    assert_eq!(code, 0, "{stderr}");
    let run_id = stdout.trim().to_string();
    wait_terminal(&server, &run_id).await;

    // events：stdout 是 JSONL，每行一个事件
    let (code, stdout, stderr) = cli(&server, &["run", "events", &run_id]).await;
    assert_eq!(code, 0, "{stderr}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert!(lines.len() >= 8, "{stdout}");
    for line in &lines {
        serde_json::from_str::<Value>(line)
            .unwrap_or_else(|err| panic!("非 JSONL 行 {line}: {err}"));
    }
    let last: Value = serde_json::from_str(lines.last().unwrap()).unwrap();
    assert_eq!(last["event"]["kind"], json!("run_completed"));

    // timeline：节点行覆盖全部节点
    let (code, stdout, stderr) = cli(&server, &["run", "timeline", &run_id]).await;
    assert_eq!(code, 0, "{stderr}");
    for node in ["start", "shape", "end"] {
        assert!(stdout.contains(node), "{stdout}");
    }
}

#[tokio::test]
async fn input_forms_inline_file_and_stdin_agree() {
    let server = TestServer::spawn().await;
    let scratch = TempDir::new("input-forms");
    let file = write_json(
        scratch.path(),
        "demo.workflow.json",
        &sample_workflow_json(),
    );
    let (code, _, stderr) = cli(&server, &["workflow", "import", file.to_str().unwrap()]).await;
    assert_eq!(code, 0, "{stderr}");
    let input_file = write_json(scratch.path(), "input.json", &json!({"who": "from-file"}));

    // @文件
    let (code, stdout, stderr) = cli(
        &server,
        &[
            "run",
            "start",
            "cli-demo",
            "--input",
            &format!("@{}", input_file.display()),
        ],
    )
    .await;
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(stdout_json(&stdout)["greeting"], json!("hi from-file"));

    // '-' 读标准输入
    let mut child = tokio::time::timeout(CLI_TIMEOUT, async {
        Command::new(env!("CARGO_BIN_EXE_flow-cli"))
            .env("FLOW_JOURNAL_TOKEN", TOKEN)
            .args(["run", "start", "cli-demo", "--input", "-"])
            .env("FLOW_RPC", server.url())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("启动 flow-cli 失败")
    })
    .await
    .expect("flow-cli 超时");
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"{\"who\":\"from-stdin\"}")
        .await
        .unwrap();
    let piped = tokio::time::timeout(CLI_TIMEOUT, child.wait_with_output())
        .await
        .expect("flow-cli 超时")
        .expect("等待 flow-cli 失败");
    assert!(piped.status.success());
    let stdout = String::from_utf8_lossy(&piped.stdout);
    assert_eq!(stdout_json(&stdout)["greeting"], json!("hi from-stdin"));
}

/// 起 run 后轮询到终态（测试自己的等待，超时即失败）。
async fn wait_terminal(server: &TestServer, run_id: &str) -> Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let (code, stdout, stderr) = cli(server, &["run", "get", run_id, "--json"]).await;
        assert_eq!(code, 0, "{stderr}");
        let detail = stdout_json(&stdout);
        let status = detail["run"]["status"].as_str().unwrap_or_default();
        if matches!(status, "succeeded" | "failed" | "cancelled") {
            return detail;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "run {run_id} 未在 20s 内终结"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
