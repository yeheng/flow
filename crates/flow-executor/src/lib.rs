//! flow-executor：受管理的本地执行子进程运行时（JSONL 重构二期 §3）。
//!
//! 本进程不打开 store/journal、不持有 ChildRunLauncher；全部用户 JS
//! （模板、script、condition）与外部 HTTP 调用在本进程执行。一切持久
//! 事实经 IPC 发回主进程提交，socket 收到数据不等于持久回执。
//!
//! 故障隔离边界（设计抉择 C2）：节点语义只存在于本进程，主进程只做裁决
//! 与调度。工作流定义事实上是可执行代码（任意 JS + 任意出站 HTTP），
//! 跑在子进程里意味着失控脚本能拖垮的只是一个可丢弃的执行槽，SQLite
//! 权威写者不会被用户代码拖住。
//!
//! 值字节纪律（设计抉择 C6）：控制帧只携带身份与元数据，用户值一律走
//! data 语义帧分块传输。本进程既是这些分块的接收端，也是 journal 侧
//! 值分块的产出端，两侧共用同一组预算常量（见 `transfer.rs`）。
//!
//! 本进程没有恢复决策权，只保证「我做的事到底做了没有」可被主进程据实
//! 裁决：成功交 Result、业务失败交失败 Result、外部结果未知交 Uncertain，
//! 通道或进程级故障才走 Reject 回收。

mod agent_progress;
pub mod agent_summary;
pub mod audit;
pub mod observe;
pub mod runtime;
pub mod task;
pub mod transfer;

/// 执行器构建标识：握手时随 Hello 上报，主进程据此判断「这条派发是不是
/// 由匹配的执行器语义处理的」。它是溯源信息，不是恢复门禁（恢复门禁是
/// `flow_engine::RUNTIME_ID` 与审计里的 `engine_build`）。
pub const EXECUTOR_BUILD: &str = env!("CARGO_PKG_VERSION");

/// 执行器声明的能力目录（与契约 REQUIRED_CAPABILITIES 对齐）。
///
/// 目录在握手阶段交换：主进程据此判定本执行器能否承接某类节点语义，
/// 声明缺项时宁可拒绝派发，也不要让主进程按「本执行器会做」来编排。
pub const EXECUTOR_CAPABILITIES: &[&str] = &["execute", "js", "http", "transfer"];
