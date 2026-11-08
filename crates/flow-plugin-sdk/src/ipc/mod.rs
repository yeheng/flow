//! 进程间字节通道（传输层）：spawn 子进程并与它说字节。
//!
//! 原 proc-ipc crate 合并入 flow-plugin-sdk（T7）；模块边界与不变量
//! 原样：通道凭据、FD 收敛、会话令牌、进程回收见各子模块文档。

pub mod challenge;
pub mod channel;
pub mod child;
pub mod env;
mod fds;
pub mod spawn;
#[cfg(unix)]
pub use challenge::Challenge;
pub use channel::Channel;
pub use child::ChildGuard;
pub use spawn::{Session, SpawnConfig};
