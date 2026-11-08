//! 插件线协议（wire 层）：显式信封 + JSON-RPC 2.0 子集 + initialize 握手。
//!
//! 原 plugin-proto crate 合并入 flow-plugin-sdk（T7）；协议不依赖 ipc——
//! 只认字节流。跨语言契约本体仍是 docs/protocol/wire.md 与 tests/vectors/。

/// 协议层错误：帧损坏、超限、非法 JSON-RPC 结构。
///
/// 传输层错误（EOF、管道破裂）不属于这里 —— 协议层只裁决「字节合不合法」。
#[derive(Debug, thiserror::Error)]
pub enum ProtoError {
    /// 帧长超过 [`MAX_FRAME_BYTES`]。超限一律拒绝，不截断、不协商：
    /// 一个坏插件不能靠巨帧撑爆宿主内存。
    #[error("frame of {0} bytes exceeds limit {MAX_FRAME_BYTES}")]
    FrameTooLarge(usize),
    /// 帧头声明的长度与实际载荷不符（流在帧中间断了）。
    #[error("stream ended mid-frame: declared {declared}, got {received}")]
    TruncatedFrame {
        /// 帧头声明的载荷长度。
        declared: usize,
        /// 实际读到的字节数。
        received: usize,
    },
    /// 未知 kind 字节。
    #[error("unknown frame kind 0x{0:02x}")]
    UnknownKind(u8),
    /// kind=rpc 的载荷不是合法 UTF-8 JSON。
    #[error("rpc payload is not valid JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    /// JSON-RPC 结构不合法（见 [`jsonrpc`] 的校验规则）。
    #[error("invalid json-rpc message: {0}")]
    InvalidMessage(String),
}

pub mod envelope;
pub mod handshake;
pub mod jsonrpc;
pub use envelope::{encode_frame, Frame, FrameKind, MAX_FRAME_BYTES};
pub use handshake::{
    Capability, InitializeParams, InitializeResult, PROTOCOL_VERSION, PROTOCOL_VERSION_MAJOR,
};
pub use jsonrpc::{
    Error, ErrorCode, Id, Message, Notification, Request, Response, APP_ERROR_RANGE_START,
};
