//! 对外插件协议：显式信封 + JSON-RPC 2.0 子集 + initialize 握手。
//!
//! 这个 crate 回答的问题是：**宿主与插件（或任何子进程/远端进程）在一条
//! 字节流上说什么。** 它不回答「字节流从哪来」—— 那是 `proc-ipc` 的事。
//!
//! 三层结构：
//!
//! 1. [`envelope`] —— 帧格式：`[u8 kind][u32 BE len][payload]`。
//!    kind 显式给出「这帧是什么」，不靠载荷结构猜。**这是与隐式类型编号
//!    （postcard 变体索引）的本质区别：加一个 kind 不改变既有 kind 的
//!    编码，跨版本配对不会静默错解。**
//! 2. [`jsonrpc`] —— JSON-RPC 2.0 子集：请求/响应/通知，明确禁用 batch。
//! 3. [`handshake`] —— `initialize` 握手：版本与能力协商，不匹配即拒绝。
//!
//! **规范文档是 [`wire.md`](../../docs/protocol/wire.md)，golden vectors 在
//! `tests/vectors/`。** 这两样才是对外契约本体；Rust API 只是契约的一种
//! 实现。跨语言实现者以文档 + 向量为准，不需要读这里的代码。
//!
//! 兼容性规则（改代码前先背下来）：
//! - 加新的 **kind**：安全（老实现会把未知 kind 当协议错误关闭连接，
//!   行为是定义过的）。
//! - 加 JSON-RPC **方法**：安全（老实现回 `-32601`）。
//! - 改 **帧头格式 / 现有 kind 的载荷含义 / initialize 字段语义**：
//!   破坏性 —— 必须 bump `handshake::PROTOCOL_VERSION` 的主版本。
//! - golden vectors **只增不改**：一条向量被修改 = 契约被修改。

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
