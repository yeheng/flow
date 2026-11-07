//! 进程间字节通道（传输层）。
//!
//! 这个 crate 回答的问题只有一个：**怎么把一个子进程拉起来，然后和它
//! 说字节。** 它不知道什么是 JSON-RPC，也不知道什么是 postcard —— 那是
//! 上层协议层的事。
//!
//! 之所以要有这一层：任何「宿主 + 插件/子进程」的系统都要先解决四件事，
//! 而这四件事跟业务语义无关，因此值得独立成可复用、可被非 Rust 实现
//! 照抄的构件：
//!
//! 1. **通道怎么建**：unix 用 socketpair（能力即凭据），其余平台用
//!    named pipe 回连（不占网络栈、无端口抢连窗口）。
//! 2. **通道怎么交到子进程手上**：unix 靠继承的 fd + 环境变量告知 fd 号
//!    （**不放 argv** —— argv 会出现在 `ps` 输出里）；其余平台靠环境变量
//!    告知回连地址。
//! 3. **子进程怎么被约束**：只继承一条通道，其余 fd 一律 CLOEXEC 收敛；
//!    环境不继承（`env_clear` + 显式白名单），凭据类变量不会漏出去。
//! 4. **进程怎么被回收**：会话结束时杀整棵进程树并 wait/reap，不留僵尸。
//!
//! **不变量**（改动时必须守住）：
//!
//! - 子进程只拿到一条字节通道；其余继承 fd 一律 CLOEXEC 收敛（unix）。
//! - 子进程不继承宿主环境：`env_clear` 后只回填显式给出的变量。
//! - unix 路径不设会话令牌（fd 即能力）；其余平台必须带令牌回验。
//! - 每个被 spawn 的子进程都必须被 reap：句柄 drop 即杀，shutdown 即
//!   杀整棵树并等待。
//!
//! **安全边界**（别把它当安全承诺卖）：令牌只防「意外抢连 / 脚本误连」，
//! 不防同 uid 恶意进程 —— 令牌经环境变量注入，同 uid 进程在 Linux 上可以
//! 直接读 `/proc/<pid>/environ`。unix 路径真正的底座是 FD 收敛，不是令牌。

pub mod challenge;
pub mod channel;
pub mod child;
pub mod env;
pub mod spawn;

#[cfg(unix)]
mod fds;

pub use challenge::Challenge;
pub use channel::Channel;
pub use child::ChildGuard;
pub use spawn::{Session, SpawnConfig};
