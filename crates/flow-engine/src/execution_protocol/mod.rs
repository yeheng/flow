//! 本地执行 IPC 协议（JSONL 重构二期 §3）。
//!
//! DTO、物理帧、传输与会话校验都在本模块；业务层只依赖 [`message`] 的
//! 消息目录与 [`transport`] 的小型 Transport 边界（按 control/data 发帧、
//! 收帧、关闭），不依赖 Unix FD、PID 或机器路径。远程适配在三期定义。
//!
//! 子进程不得打开 store/journal；本模块不引入任何存储依赖。

pub mod contract;
pub mod fixtures;
pub mod remote;
pub mod spawn;
pub mod frame;
pub mod message;
#[cfg(unix)]
pub mod transport;

pub use contract::*;
pub use message::{
    record_bytes, AuditRecord, Channel, ExecuteTask, Message, ObservationLine, ProtocolError,
    ResultOutcome, WaitRequest,
};
