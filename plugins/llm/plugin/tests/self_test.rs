//! llm-plugin 端到端自测：真实 spawn 本二进制 + 本地 mock HTTP server，
//! 覆盖握手 → invoke → 二进制帧 → 摘要校验 → 各错误码。
//!
//! 这是插件进程的**协议级**验收：宿主侧（runtime llm_node）由 backend-e2e
//! 的 llm 用例覆盖，两侧各自钉住自己的一半契约。

use std::time::Duration;

use plugin_proto::envelope::{read_frame, write_frame, Frame, FrameKind};
use plugin_proto::handshake::{
    initialize_request, parse_initialize_params, Capability, InitializeParams, PROTOCOL_VERSION,
};
use plugin_proto::jsonrpc::{ErrorCode, Id, Message, Request};
use proc_ipc::SpawnConfig;
use serde_json::json;

fn plugin_bin() -> std::path::PathBuf {
    // 测试二进制与 bin 同 target：deps/../flow-llm-plugin。
    let exe = std::env::current_exe().expect("test binary");
    let profile = exe
        .parent()
        .and_then(|deps| deps.parent())
        .expect("target profile dir");
    let bin = profile.join("flow-llm-plugin");
    assert!(bin.is_file(), "flow-llm-plugin 未构建：{}", bin.display());
    bin
}

/// 极简 mock HTTP server：接受一个连接，读请求行，回固定响应。
async fn mock_server(response: &'static str) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = vec![0u8; 8192];
        let _ = socket.read(&mut buf).await;
        socket.write_all(response.as_bytes()).await.unwrap();
        // 读完客户端 side 的半关闭，避免 RST 截断响应。
        let _ = socket.read(&mut buf).await;
    });
    (format!("http://{addr}/model"), handle)
}

async fn open_plugin() -> proc_ipc::Session {
    let mut config = SpawnConfig::new(plugin_bin());
    config.new_session = false;
    let mut session = proc_ipc::Session::spawn(config)
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
            let parsed = parse_initialize_params(&Request {
                jsonrpc: "2.0",
                id: Id::Number(1),
                method: "initialize".into(),
                params: Some(json!({"protocol_version": PROTOCOL_VERSION,
                    "capabilities": [], "client_info": {}})),
            })
            .ok();
            let _ = parsed;
            assert!(response.error.is_none(), "握手被拒：{:?}", response.error);
            let result = response.result.expect("握手 result");
            assert_eq!(result["server_info"]["name"], "flow-llm-plugin");
            assert_eq!(result["capabilities"][0], "binary_blocks");
        }
        other => panic!("握手必须得到响应：{other:?}"),
    }
    session
}

async fn write(message: Message, session: &mut proc_ipc::Session) {
    let frame = Frame {
        kind: FrameKind::Rpc,
        payload: message.to_vec().expect("encode"),
    };
    write_frame(session.channel_mut(), &frame)
        .await
        .expect("write");
}

async fn expect_rpc(session: &mut proc_ipc::Session) -> Message {
    loop {
        let frame =
            tokio::time::timeout(Duration::from_secs(10), read_frame(session.channel_mut()))
                .await
                .expect("frame within timeout")
                .expect("read");
        if frame.kind == FrameKind::Rpc {
            return frame.as_rpc().expect("rpc frame");
        }
        // 二进制帧由调用方在需要时先行收集；这里跳过直到 rpc。
    }
}

/// 收集「响应之前的全部二进制帧」。
async fn invoke(session: &mut proc_ipc::Session, params: serde_json::Value) -> (Message, Vec<u8>) {
    let request = Message::Request(Request {
        jsonrpc: "2.0",
        id: Id::Number(2),
        method: "llm/invoke".into(),
        params: Some(params),
    });
    write(request, session).await;
    let mut binary = Vec::new();
    loop {
        let frame =
            tokio::time::timeout(Duration::from_secs(10), read_frame(session.channel_mut()))
                .await
                .expect("frame within timeout")
                .expect("read");
        match frame.kind {
            FrameKind::Binary => binary.extend_from_slice(&frame.payload),
            FrameKind::Rpc => return (frame.as_rpc().expect("rpc"), binary),
        }
    }
}

fn error_code(message: &Message) -> i64 {
    match message {
        Message::Response(response) => response
            .error
            .as_ref()
            .map(|error| match error.code {
                ErrorCode::Application(code) => code,
                other => other.code(),
            })
            .expect("expected error response"),
        other => panic!("expected response: {other:?}"),
    }
}

#[tokio::test]
async fn happy_path_returns_binary_frames_then_digest() {
    let body = r#"{"choices":[{"message":{"content":"hi"}}]}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nSet-Cookie: a=1\r\nSet-Cookie: b=2\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let (url, server) = mock_server(Box::leak(response.into_boxed_str())).await;
    let mut session = open_plugin().await;
    let (message, binary) = invoke(
        &mut session,
        json!({"method": "POST", "url": url, "body": {"m": 1},
               "credential": "sk-test", "timeout_ms": 5000}),
    )
    .await;
    server.await.unwrap();

    match message {
        Message::Response(response) => {
            let result = response.result.expect("成功响应");
            assert_eq!(result["status"], 200);
            assert_eq!(result["body_bytes"], body.len() as u64);
            // 多值头合并：a=1, b=2 逗号分隔，不覆盖。
            assert_eq!(result["headers"]["set-cookie"], "a=1, b=2");
            // 摘要与重组字节互验。
            let digest = {
                use sha2::Digest;
                hex::encode(sha2::Sha256::digest(&binary))
            };
            assert_eq!(binary, body.as_bytes());
            assert_eq!(result["body_digest"], digest);
        }
        other => panic!("期望成功响应：{other:?}"),
    }
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn transport_failure_maps_to_egress_error() {
    // 端口无人监听：连接拒绝 = 出站失败（不确定类）。
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let mut session = open_plugin().await;
    let (message, _) = invoke(
        &mut session,
        json!({"method": "POST", "url": format!("http://{addr}/x"), "body": null}),
    )
    .await;
    assert_eq!(error_code(&message), -32020);
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_request_maps_to_malformed_error() {
    let mut session = open_plugin().await;
    let (message, _) = invoke(
        &mut session,
        json!({"method": "POST", "url": "not a url at all"}),
    )
    .await;
    assert_eq!(error_code(&message), -32022);
    // 未知方法：标准 -32601。
    let request = Message::Request(Request {
        jsonrpc: "2.0",
        id: Id::Number(3),
        method: "no/such".into(),
        params: Some(json!({})),
    });
    write(request, &mut session).await;
    let message = expect_rpc(&mut session).await;
    assert_eq!(error_code(&message), -32601);
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn no_redirect_following() {
    // 302 必须原样返回（不跟随——授权记一次副作用，重定向是第二次）。
    let response: &'static str =
        "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/elsewhere\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    let (url, server) = mock_server(response).await;
    let mut session = open_plugin().await;
    let (message, _) = invoke(&mut session, json!({"method": "GET", "url": url})).await;
    server.await.unwrap();
    match message {
        Message::Response(response) => {
            assert_eq!(response.result.expect("result")["status"], 302);
        }
        other => panic!("{other:?}"),
    }
    session.shutdown().await.unwrap();
}
