//! 执行 IPC 协议（v4 单流）。
//!
//! DTO、物理帧、传输与会话校验都在本模块；业务层只依赖 [`message`] 的
//! 消息目录与 [`transport`] 的小型 Transport 边界（按 control/data 发帧、
//! 收帧、关闭），不依赖 Unix FD、PID 或机器路径。远程中继（agent）复用
//! 同一消息目录与帧格式（`remote` 模块）。
//!
//! 子进程不得打开 store/journal；本模块不引入任何存储依赖。

pub mod contract;
pub mod fixtures;
pub mod frame;
pub mod loopback;
pub mod message;
pub mod remote;
#[cfg(unix)]
pub mod spawn;
pub mod transfer_key;
pub mod transport;
#[cfg(unix)]
pub mod unix;
pub mod ws;

pub use contract::*;
pub use loopback::{connect_loopback, spawn_executor_loopback, CONNECT_ADDR_ENV};
pub use message::{
    record_bytes, AuditRecord, Channel, ExecuteTask, Message, ObservationLine, PreparedMeta,
    ProtocolError, ResultOutcome, TaskValue, WaitRequest,
};
pub use transfer_key::TransferKey;
#[cfg(unix)]
pub use unix::{channel_from_raw, socketpair_channel};
