//! flow-playwright-plugin 端到端自测：真实 spawn 本二进制 + 真实 node
//! 子进程，覆盖握手 → invoke → 信封 → 各错误码。
//!
//! 这是插件进程的**协议级**验收：宿主侧（runtime plugin_node）由
//! backend-e2e 覆盖，两侧各自钉住自己的一半契约。
//!
//! 分层与跳过策略（照 python_e2e 惯例：缺环境自动跳过，不算失败）：
//! - 协议层（握手/畸形参数/未知方法）：无需 node；
//! - 执行层（成功/脚本失败）：需要 node；wrapper 用**契约一致的迷你
//!   假实现**（无浏览器，信封语义相同）；
//! - 冒烟层（真浏览器）：需要 node + runtime 里装好 playwright + 浏览器
//!   二进制；用仓库 `plugins/playwright/runtime` 真.wrapper。

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use flow_plugin_sdk::ipc::SpawnConfig;
use flow_plugin_sdk::protocol::envelope::{read_frame, write_frame, Frame, FrameKind};
use flow_plugin_sdk::protocol::handshake::{
    initialize_request, Capability, InitializeParams, PROTOCOL_VERSION,
};
use flow_plugin_sdk::protocol::jsonrpc::{ErrorCode, Id, Message, Request};
use serde_json::json;

fn plugin_bin() -> PathBuf {
    // 测试二进制与 bin 同 target：deps/../flow-playwright-plugin。
    let exe = std::env::current_exe().expect("test binary");
    let profile = exe
        .parent()
        .and_then(|deps| deps.parent())
        .expect("target profile dir");
    let bin = profile.join("flow-playwright-plugin");
    assert!(
        bin.is_file(),
        "flow-playwright-plugin 未构建：{}",
        bin.display()
    );
    bin
}

/// 插件按部署布局解析 runtime：exe → 上两级 → runtime。测试 spawn 的
/// 二进制在 target/debug 下，所以 fixture 落在 target/runtime。
fn fixture_runtime() -> PathBuf {
    let exe = plugin_bin();
    exe.parent()
        .and_then(|p| p.parent())
        .expect("target dir")
        .join("runtime")
}

/// 仓库源头的真 runtime（wrapper.js + package.json + node_modules）。
fn repo_runtime() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../runtime")
}

fn node_available() -> bool {
    std::process::Command::new("node")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// fixture 目录串行写：假 wrapper 与真 runtime 互换不能并发。
static FIXTURE: Mutex<()> = Mutex::new(());

/// 契约一致的迷你 wrapper（无浏览器）：跑 script 函数体，成功/失败
/// 信封语义与真 wrapper 相同。node 缺席时返回 false（测试跳过）。
fn install_fake_wrapper() -> bool {
    if !node_available() {
        return false;
    }
    let dir = fixture_runtime();
    std::fs::create_dir_all(&dir).expect("创建 fixture runtime");
    let fake = r#"'use strict';
const chunks = [];
process.stdin.on('data', (d) => chunks.push(d));
process.stdin.on('end', async () => {
  const req = JSON.parse(Buffer.concat(chunks).toString('utf8'));
  const envelope = {
    status: 'success', browser: req.browser, headless: req.headless,
    duration_ms: 1, logs: [], result: null, error: undefined,
  };
  try {
    const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
    envelope.result = await new AsyncFunction(req.script)();
    if (envelope.result === undefined) envelope.result = null;
  } catch (error) {
    envelope.status = 'failed';
    envelope.error = String(error);
  }
  process.stdout.write('__FLOW_PW__' + JSON.stringify(envelope) + '\n');
  process.exit(0);
});
"#;
    std::fs::write(dir.join("wrapper.js"), fake).expect("写假 wrapper");
    true
}

/// 真 runtime 冒烟前置：node + 仓库 runtime 的 playwright 装好。
/// 浏览器二进制缺失不在这里判（启动失败时按环境跳过）。
fn real_runtime_ready() -> bool {
    node_available() && repo_runtime().join("node_modules/playwright").is_dir()
}

/// 把 fixture runtime 指向仓库真 runtime（符号链接 node_modules + 拷贝
/// wrapper.js）。失败 = 环境问题，调用方跳过。
fn install_real_wrapper() -> bool {
    let dir = fixture_runtime();
    let repo = repo_runtime();
    std::fs::create_dir_all(&dir).expect("创建 fixture runtime");
    std::fs::copy(repo.join("wrapper.js"), dir.join("wrapper.js")).expect("拷真 wrapper");
    let link = dir.join("node_modules");
    let _ = std::fs::remove_dir_all(&link);
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(repo.join("node_modules"), &link).expect("链接 node_modules");
    }
    #[cfg(not(unix))]
    {
        std::fs::copy(repo.join("node_modules"), &link).ok();
    }
    true
}

async fn open_plugin() -> flow_plugin_sdk::ipc::Session {
    let mut config = SpawnConfig::new(plugin_bin());
    config.new_session = false;
    // 与生产同源：宿主 spawn 插件时环境是白名单注入（plugin_env：
    // PATH/HOME/TMPDIR…），插件再原样传给 node 子进程。测试注入同款
    // 最小集，否则插件里找不到 node。
    for name in ["PATH", "HOME", "TMPDIR"] {
        if let Some(value) = std::env::var_os(name) {
            config.env.push((name.into(), value));
        }
    }
    let mut session = flow_plugin_sdk::ipc::Session::spawn(config)
        .await
        .expect("spawn plugin");
    let params = InitializeParams {
        protocol_version: PROTOCOL_VERSION,
        capabilities: vec![Capability::BinaryBlocks],
        client_info: json!({"name": "self-test"}),
    };
    let initialize = Message::Request(initialize_request(Id::Number(1), &params));
    write(initialize, &mut session).await;
    let response = expect_rpc(&mut session).await;
    match response {
        Message::Response(response) => {
            assert!(response.error.is_none(), "握手被拒：{:?}", response.error);
            let result = response.result.expect("握手 result");
            assert_eq!(result["server_info"]["name"], "flow-playwright-plugin");
            // 受管插件必须协商 master_permit_v1（wire.md §3.3）。
            let capabilities = result["capabilities"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            assert!(
                capabilities.iter().any(|c| c == "master_permit_v1"),
                "capabilities 缺 master_permit_v1：{capabilities:?}"
            );
        }
        other => panic!("握手必须得到响应：{other:?}"),
    }
    session
}

async fn write(message: Message, session: &mut flow_plugin_sdk::ipc::Session) {
    let frame = Frame {
        kind: FrameKind::Rpc,
        payload: message.to_vec().expect("encode"),
    };
    write_frame(session.channel_mut(), &frame)
        .await
        .expect("write");
}

async fn expect_rpc(session: &mut flow_plugin_sdk::ipc::Session) -> Message {
    let frame = tokio::time::timeout(Duration::from_secs(10), read_frame(session.channel_mut()))
        .await
        .expect("frame within timeout")
        .expect("read");
    assert_eq!(frame.kind, FrameKind::Rpc, "只期望 rpc 帧");
    frame.as_rpc().expect("rpc frame")
}

/// 一次 invoke：`{params: <节点参数>}` → 响应。
async fn invoke(
    session: &mut flow_plugin_sdk::ipc::Session,
    node_params: serde_json::Value,
) -> Message {
    let request = Message::Request(Request {
        jsonrpc: "2.0".into(),
        id: Id::Number(2),
        method: "playwright/invoke".into(),
        params: Some(json!({"params": node_params})),
    });
    write(request, session).await;
    expect_rpc(session).await
}

fn error_code_and_message(message: &Message) -> (i64, String) {
    match message {
        Message::Response(response) => {
            let error = response.error.as_ref().expect("expected error response");
            let code = match error.code {
                ErrorCode::Application(code) => code,
                other => other.code(),
            };
            (code, error.message.clone())
        }
        other => panic!("expected response: {other:?}"),
    }
}

#[tokio::test]
async fn unknown_method_is_rejected_not_swallowed() {
    let mut session = open_plugin().await;
    let request = Message::Request(Request {
        jsonrpc: "2.0".into(),
        id: Id::Number(9),
        method: "no/such-method".into(),
        params: None,
    });
    write(request, &mut session).await;
    let response = expect_rpc(&mut session).await;
    let (code, _) = error_code_and_message(&response);
    assert_eq!(code, -32601);
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_params_map_to_32022() {
    // 协议层：无需 node / runtime fixture（校验在最前面）。
    let mut session = open_plugin().await;
    let response = invoke(&mut session, json!({"browser": "ie"})).await;
    let (code, message) = error_code_and_message(&response);
    assert_eq!(code, -32022, "message: {message}");
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn happy_path_returns_output_envelope() {
    let _guard = FIXTURE.lock().unwrap_or_else(|e| e.into_inner());
    if !install_fake_wrapper() {
        eprintln!("跳过：本机无 node");
        return;
    }
    let mut session = open_plugin().await;
    let response = invoke(
        &mut session,
        json!({"script": "return 41 + 1;", "browser": "firefox", "headless": false}),
    )
    .await;
    match response {
        Message::Response(response) => {
            let output = response.result.expect("成功响应")["output"].clone();
            assert_eq!(output["status"], "success");
            assert_eq!(output["browser"], "firefox");
            assert_eq!(output["headless"], false);
            assert_eq!(output["result"], 42);
            assert!(
                output["duration_ms"].as_u64().is_some(),
                "duration_ms 必须有"
            );
        }
        other => panic!("期望成功响应：{other:?}"),
    }
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn script_failure_maps_to_32020_with_detail() {
    let _guard = FIXTURE.lock().unwrap_or_else(|e| e.into_inner());
    if !install_fake_wrapper() {
        eprintln!("跳过：本机无 node");
        return;
    }
    let mut session = open_plugin().await;
    let response = invoke(
        &mut session,
        json!({"script": "throw new Error('boom-42');"}),
    )
    .await;
    let (code, message) = error_code_and_message(&response);
    assert_eq!(code, -32020);
    assert!(message.contains("boom-42"), "错误详情必须透传：{message}");
    session.shutdown().await.unwrap();
}

/// 缺 runtime → 环境错误（确定类），message 带修复指引。
#[tokio::test]
async fn missing_runtime_maps_to_32024() {
    let _guard = FIXTURE.lock().unwrap_or_else(|e| e.into_inner());
    // 挪走 wrapper（测试完恢复）。
    let dir = fixture_runtime();
    std::fs::create_dir_all(&dir).expect("fixture 目录");
    let wrapper = dir.join("wrapper.js");
    let backup = dir.join("wrapper.js.bak");
    std::fs::rename(&wrapper, &backup).ok();
    let mut session = open_plugin().await;
    let response = invoke(&mut session, json!({"script": "return 1;"})).await;
    std::fs::rename(&backup, &wrapper).ok();
    let (code, message) = error_code_and_message(&response);
    assert_eq!(code, -32024, "message: {message}");
    assert!(
        message.contains("runtime"),
        "message 必须带 runtime 修复指引：{message}"
    );
    session.shutdown().await.unwrap();
}

/// 真浏览器冒烟：chromium 打开 data: 页面，脚本 return title。浏览器
/// 二进制缺失（`Executable doesn't exist`）按环境跳过，其余失败照常红。
#[tokio::test]
async fn real_browser_smoke() {
    let _guard = FIXTURE.lock().unwrap_or_else(|e| e.into_inner());
    if !real_runtime_ready() || !install_real_wrapper() {
        eprintln!("跳过：playwright 未安装（npm install）或本机无 node");
        return;
    }
    let mut session = open_plugin().await;
    let script = r#"
        await page.goto('data:text/html,<title>flow-pw-smoke</title><h1>ok</h1>');
        return { title: await page.title(), heading: await page.locator('h1').textContent() };
    "#;
    let response = invoke(
        &mut session,
        json!({"script": script, "browser": "chromium", "headless": true, "timeout_ms": 60000}),
    )
    .await;
    match response {
        Message::Response(response) => match response.error {
            None => {
                let output = response.result.expect("成功响应")["output"].clone();
                assert_eq!(output["status"], "success");
                assert_eq!(output["result"]["title"], "flow-pw-smoke");
                assert_eq!(output["result"]["heading"], "ok");
                assert_eq!(output["browser"], "chromium");
                assert!(output["duration_ms"].as_u64().is_some());
            }
            Some(error) => {
                if error.message.contains("Executable doesn't exist") {
                    eprintln!("跳过：浏览器二进制未安装（playwright install）");
                } else {
                    panic!("冒烟失败：{}", error.message);
                }
            }
        },
        other => panic!("期望响应：{other:?}"),
    }
    session.shutdown().await.unwrap();
}
