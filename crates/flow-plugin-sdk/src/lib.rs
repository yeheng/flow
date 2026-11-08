//! flow-plugin-sdk：插件双端共用的协议与传输 SDK。
//!
//! 两个单向模块（T7 合并原 proc-ipc 与 plugin-proto）：
//!
//! - [`protocol`]：帧、JSON-RPC 子集、握手——不依赖 ipc，接受任意字节流；
//! - [`ipc`]：通道、spawn、进程回收——不知道协议。
//!
//! SDK **不含**插件注册表、master 协议、审计窗口与调度策略，也不依赖
//! runtime/backend/engine。跨语言契约以 docs/protocol/wire.md 与
//! tests/vectors/ 为准（golden vectors 只增不改）。

pub mod ipc;
pub mod protocol;

pub use ipc::{Challenge, Channel, ChildGuard, Session, SpawnConfig};
pub use protocol::{
    encode_frame, Capability, Error, ErrorCode, Frame, FrameKind, Id, InitializeParams,
    InitializeResult, Message, Notification, ProtoError, Request, Response, APP_ERROR_RANGE_START,
    MAX_FRAME_BYTES, PROTOCOL_VERSION, PROTOCOL_VERSION_MAJOR,
};
