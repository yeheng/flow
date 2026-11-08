//! Golden vectors：跨语言实现的 ground truth。
//!
//! `tests/vectors/*.json` 是**独立于 Rust 实现生成的**契约样本（生成时
//! 只依赖 wire.md 的规范描述，不调用本 crate）。任何语言的实现都应当
//! 把同一份目录拷走，断言自己的编解码结果与 `bytes_hex` 完全一致。
//!
//! 本测试断言三件事（缺一不可）：
//!
//! 1. `bytes_hex` 能被解码成合法帧与合法 JSON-RPC 消息；
//! 2. 解码后的消息**再编码，字节完全一致**（规范形稳定性 —— 字段顺序、
//!    紧凑分隔符、UTF-8 都不许漂）；
//! 3. binary 帧的载荷与 `payload_hex` 一致。
//!
//! 向量**只增不改**：修改一条向量 = 修改契约 = 必须 bump 主版本并在
//! wire.md 的变更记录里留痕。

use std::path::Path;

use flow_plugin_sdk::protocol::envelope::{Frame, FrameKind};
use serde_json::Value;

/// 逐个加载向量文件并断言双向一致。
#[test]
fn golden_vectors_roundtrip_byte_exact() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/vectors");
    let mut checked = 0;
    let mut names: Vec<_> = std::fs::read_dir(&dir)
        .expect("vectors directory")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    names.sort();
    assert!(!names.is_empty(), "vectors directory must not be empty");

    for path in names {
        let vector: Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read vector"))
                .expect("vector json");
        let name = vector["name"].as_str().expect("name").to_string();
        let bytes = hex(vector["bytes_hex"].as_str().expect("bytes_hex"));
        let kind = vector["kind"].as_str().expect("kind");

        // 1) 解码：帧必须合法。
        let frame = Frame::decode(&bytes)
            .unwrap_or_else(|error| panic!("{name}: vector bytes must decode, got {error}"));

        match kind {
            "rpc" => {
                assert_eq!(frame.kind, FrameKind::Rpc, "{name}: kind byte");
                // 2) 规范形稳定：decode → re-encode 必须逐字节相等。
                let message = frame
                    .as_rpc()
                    .unwrap_or_else(|error| panic!("{name}: rpc payload must parse, got {error}"));
                let reencoded = message
                    .to_vec()
                    .unwrap_or_else(|error| panic!("{name}: re-encode must succeed, got {error}"));
                assert_eq!(
                    reencoded, frame.payload,
                    "{name}: canonical re-encode must be byte-identical"
                );
                // 向量里的 message 字段与 wire 载荷语义一致（宽松比较：
                // 允许键序不同，值必须相同）。
                let expected = &vector["message"];
                let actual: Value =
                    serde_json::from_slice(&frame.payload).expect("payload as json");
                assert_eq!(&actual, expected, "{name}: semantic mismatch");
            }
            "binary" => {
                assert_eq!(frame.kind, FrameKind::Binary, "{name}: kind byte");
                let expected = hex(vector["payload_hex"].as_str().expect("payload_hex"));
                assert_eq!(frame.payload, expected, "{name}: binary payload");
            }
            other => panic!("{name}: unknown vector kind {other}"),
        }
        checked += 1;
    }
    // 向量数量下限：防止「目录被清空、测试静默变绿」。
    assert!(
        checked >= 8,
        "expected at least 8 vectors, checked {checked}"
    );
}

/// 跨语言实现的用法示范：拼接帧流 + 逐帧切分。
#[tokio::test]
async fn concatenated_frames_split_correctly_on_stream() {
    use flow_plugin_sdk::protocol::envelope::read_frame;

    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/vectors");
    let mut stream_bytes = Vec::new();
    let mut expected = Vec::new();
    for name in ["initialize_request", "binary_block", "cancel_notification"] {
        let vector: Value = serde_json::from_str(
            &std::fs::read_to_string(dir.join(format!("{name}.json"))).unwrap(),
        )
        .unwrap();
        stream_bytes.extend_from_slice(&hex(vector["bytes_hex"].as_str().unwrap()));
        expected.push(vector["kind"].as_str().unwrap().to_string());
    }

    // 用内存双工流模拟「粘包 + 半包」的现实承载。
    let (mut writer, mut reader) = tokio::io::duplex(4096);
    use tokio::io::AsyncWriteExt;
    writer.write_all(&stream_bytes).await.unwrap();
    writer.shutdown().await.ok();
    drop(writer);

    let mut kinds = Vec::new();
    while let Ok(frame) = read_frame(&mut reader).await {
        kinds.push(match frame.kind {
            FrameKind::Rpc => "rpc",
            FrameKind::Binary => "binary",
        });
    }
    assert_eq!(kinds, expected);
}

/// 最小 hex 解码（避免给测试引入 hex 依赖）。
fn hex(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2), "hex length must be even");
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).expect("hex digit"))
        .collect()
}
