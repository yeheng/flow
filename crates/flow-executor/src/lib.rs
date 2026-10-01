//! flow-executor：受管理的本地执行子进程运行时（JSONL 重构二期 §3）。
//!
//! 本进程不打开 store/journal、不持有 ChildRunLauncher；全部用户 JS
//! （模板、script、condition）与外部 HTTP 调用在本进程执行。一切持久
//! 事实经 IPC 发回主进程提交，socket 收到数据不等于持久回执。

pub mod audit;
pub mod observe;
pub mod runtime;
pub mod task;
pub mod transfer;

pub const EXECUTOR_BUILD: &str = env!("CARGO_PKG_VERSION");

/// 执行器声明的能力目录（与契约 REQUIRED_CAPABILITIES 对齐）。
pub const EXECUTOR_CAPABILITIES: &[&str] = &["execute", "js", "http", "transfer"];
