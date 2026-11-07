//! 通道相关环境变量的名字与读取。
//!
//! 约定（宿主与插件两侧都必须遵守）：
//!
//! | 变量 | 平台 | 含义 |
//! | --- | --- | --- |
//! | `PROC_IPC_FD` | unix | 通道 fd 号（宿主固定落位到 3，见 [`crate::spawn::CHANNEL_FD`]） |
//! | `PROC_IPC_ADDR` | 非 unix | 子进程要回连的本地 socket 地址 |
//! | `PROC_IPC_TOKEN` | 非 unix | 会话令牌（握手时回验） |
//!
//! 为什么不放 argv：argv 会出现在 `ps` 输出里，任何本机用户都能看到
//! fd 号与回连地址。环境变量同样对本机同 uid 进程可见（`/proc/<pid>/environ`），
//! 但它不会出现在普通的进程列表里 —— 这是「防手滑」与「防攻击」之间的
//! 实际分界，不要夸大。

/// unix：通道 fd 号。
pub const FD_ENV: &str = "PROC_IPC_FD";

/// 非 unix：回连地址（Windows 为 named pipe 名，unix 回退路径为 socket 路径）。
pub const ADDR_ENV: &str = "PROC_IPC_ADDR";

/// 非 unix：会话令牌。
pub const TOKEN_ENV: &str = "PROC_IPC_TOKEN";

/// 读取通道 fd 号（unix 子进程侧）。
pub fn fd_from_env() -> Option<i32> {
    std::env::var(FD_ENV).ok()?.trim().parse().ok()
}

/// 读取回连地址（非 unix 子进程侧）。
pub fn addr_from_env() -> Option<String> {
    std::env::var(ADDR_ENV).ok().filter(|v| !v.is_empty())
}

/// 读取会话令牌（非 unix 子进程侧）。
pub fn token_from_env() -> Option<String> {
    std::env::var(TOKEN_ENV).ok().filter(|v| !v.is_empty())
}
