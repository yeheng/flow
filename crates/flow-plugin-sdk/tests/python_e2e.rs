//! 跨语言端到端：Rust 宿主 ↔ Python 参考插件。
//!
//! 这是整个协议栈最有说服力的一个测试：宿主用 `proc-ipc` 拉起真实的
//! `python3` 进程（走 fd 收敛 + 固定号落位 + env 白名单），双方用
//! `plugin-proto` 的帧与 JSON-RPC 子集完成 initialize 握手、请求/响应
//! 往返、未知方法错误、通知忽略。Python 侧没有一行 Rust 代码——它只
//! 读 `docs/protocol/wire.md` 与向量。
//!
//! 机器上没有 `python3` 时跳过（打印原因，不算失败）；CI 的 ubuntu
//! runner 自带 python3。
//!
//! 顺带先跑一遍 Python 侧的向量校验脚本：同一个测试里同时钉住
//! 「向量是跨语言的」与「两个实现能对话」。

use std::path::{Path, PathBuf};
use std::time::Duration;

use flow_plugin_sdk::ipc::SpawnConfig;
use flow_plugin_sdk::protocol::envelope::{read_frame, write_frame, Frame, FrameKind};
use flow_plugin_sdk::protocol::handshake::{
    check_version, initialize_request, parse_initialize_params, InitializeParams, InitializeResult,
    PROTOCOL_VERSION,
};
use flow_plugin_sdk::protocol::jsonrpc::{ErrorCode, Id, Message, Request};
use serde_json::json;

/// 找 python3；找不到就跳过测试。
/// 找 python3，解析为**绝对路径**；找不到就跳过测试。
///
/// 必须绝对路径：SpawnConfig 的 env 白名单不继承 PATH，子进程无法按
/// 名字解析解释器（unix 靠 confstr 兑底路径碰运气，Windows 直接失败）。
fn python3() -> Option<PathBuf> {
    for name in ["python3", "python"] {
        let Ok(output) = std::process::Command::new(name)
            .args(["-c", "import sys; print(sys.executable)"])
            .output()
        else {
            continue;
        };
        if output.status.success() {
            let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !text.is_empty() {
                return Some(PathBuf::from(text));
            }
        }
    }
    None
}

fn plugin_py_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/plugin-py")
        .canonicalize()
        .expect("examples/plugin-py exists")
}

/// 发一条消息并读回一条（握手后的同步往返）。
async fn roundtrip(session: &mut flow_plugin_sdk::ipc::Session, message: &Message) -> Message {
    let frame = Frame {
        kind: FrameKind::Rpc,
        payload: message.to_vec().expect("encode"),
    };
    write_frame(session.channel_mut(), &frame)
        .await
        .expect("write frame");
    let reply = tokio::time::timeout(Duration::from_secs(5), read_frame(session.channel_mut()))
        .await
        .expect("reply within timeout")
        .expect("read frame");
    reply.as_rpc().expect("rpc reply")
}

#[tokio::test]
async fn python_reference_passes_vectors() {
    let Some(python) = python3() else {
        eprintln!("skip: python3 not found on this machine");
        return;
    };
    let output = std::process::Command::new(python)
        .arg(plugin_py_dir().join("test_vectors.py"))
        .output()
        .expect("run vector script");
    assert!(
        output.status.success(),
        "python vector check failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn rust_host_talks_to_python_plugin_end_to_end() {
    let Some(python) = python3() else {
        eprintln!("skip: python3 not found on this machine");
        return;
    };
    let mut config = SpawnConfig::new(&python);
    config.args = vec![plugin_py_dir().join("plugin_rpc.py").into_os_string()];
    let mut session = flow_plugin_sdk::ipc::Session::spawn(config)
        .await
        .expect("spawn python plugin");

    // --- initialize 握手：版本门必须过 ---
    let params = InitializeParams {
        protocol_version: PROTOCOL_VERSION,
        capabilities: vec![flow_plugin_sdk::protocol::handshake::Capability::BinaryBlocks],
        client_info: json!({"name": "rust-e2e", "version": "0"}),
    };
    let init = Message::Request(initialize_request(Id::Number(1), &params));
    let reply = roundtrip(&mut session, &init).await;
    let response = match reply {
        Message::Response(response) => response,
        other => panic!("initialize must get a response, got {other:?}"),
    };
    assert_eq!(response.id, Id::Number(1));
    let result = response.result.expect("initialize result");
    let result: InitializeResult = serde_json::from_value(result).expect("InitializeResult shape");
    check_version(result.protocol_version).expect("peer version must match");
    // unix fd 承载：双方都无令牌，直接通过（回连承载的核对在单测里钉）。
    flow_plugin_sdk::protocol::handshake::verify_session_token(None, &result).expect("token check");
    assert_eq!(
        result.server_info["name"], "python-reference",
        "对面必须是 Python 参考实现"
    );

    // --- 常规请求：ping → pong ---
    let ping = Message::Request(Request {
        jsonrpc: "2.0",
        id: Id::Number(2),
        method: "ping".into(),
        params: None,
    });
    let reply = roundtrip(&mut session, &ping).await;
    match reply {
        Message::Response(response) => {
            assert_eq!(response.id, Id::Number(2));
            assert_eq!(response.result, Some(json!("pong")));
        }
        other => panic!("ping must get a response, got {other:?}"),
    }

    // --- 未知方法：-32601，不静默 ---
    let nope = Message::Request(Request {
        jsonrpc: "2.0",
        id: Id::Number(3),
        method: "no_such_method".into(),
        params: None,
    });
    let reply = roundtrip(&mut session, &nope).await;
    match reply {
        Message::Response(response) => {
            let error = response.error.expect("must be an error response");
            assert_eq!(error.code, ErrorCode::MethodNotFound);
            assert!(error.message.contains("no_such_method"));
        }
        other => panic!("unknown method must get error response, got {other:?}"),
    }

    // --- 通知：不产生响应（发两条通知后跟一个请求，只有请求有回音） ---
    let note = Message::Notification(flow_plugin_sdk::protocol::jsonrpc::Notification {
        jsonrpc: "2.0",
        method: "$/cancelRequest".into(),
        params: Some(json!({"id": 9999})),
    });
    let frame = Frame {
        kind: FrameKind::Rpc,
        payload: note.to_vec().expect("encode"),
    };
    write_frame(session.channel_mut(), &frame)
        .await
        .expect("write notification");
    let reply = roundtrip(&mut session, &ping).await;
    assert!(
        matches!(reply, Message::Response(_)),
        "通知不得产生响应，丟能照常请求"
    );

    // --- 解析 initialize 参数（插件侧同款校验）在宿主侧也可用 ---
    let request = initialize_request(Id::Number(9), &params);
    let parsed = parse_initialize_params(&request).expect("parse own initialize");
    assert_eq!(parsed.protocol_version, PROTOCOL_VERSION);

    session.shutdown().await.expect("reap python plugin");
}
