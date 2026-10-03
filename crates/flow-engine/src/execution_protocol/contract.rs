//! 冻结的本地 IPC 契约（JSONL 重构二期 §2/§3.5）。
//!
//! 这些常量在 I01 冻结：主进程与执行器实现都必须在同一组上限内工作，
//! 任何一方都不允许在握手之外重新协商硬上限。修改本文件即构成协议版本
//! 变更，必须 bump [`PROTOCOL_VERSION`] 并按二期 §2 声明兼容边界。
//!
//! 三类并发面符号（二期 §3.5，全文唯一，不得互相代用）：
//! - `R_MAX` 在飞 run 状态数（含持久等待中的 run），约束主进程状态内存；
//! - `A_MAX` 活跃派发上界（同时持有 dispatch 事实、占窗口预算），窗口全局
//!   预算 = `A_MAX × W_BYTES × K_FACTOR`；
//! - `X_MAX_DEFAULT` 执行槽位数（同时运行的执行子进程），约束进程数 × RSS。
//!
//! `R_MAX` 不得代入窗口预算公式，`A_MAX` 不得用来论证主进程状态内存，
//! `X_MAX` 不得用来论证审计吞吐。

/// IPC 协议版本。v1 = 二期本地 socketpair 版本。
pub const PROTOCOL_VERSION: u32 = 1;

/// control 通道单帧 JSON 上限（不含 4 字节长度头）。握手只可协商到本上限以内。
pub const CONTROL_MAX_FRAME: usize = 64 * 1024;
/// data 通道单帧 JSON 上限（不含 4 字节长度头）。
pub const DATA_MAX_FRAME: usize = 1024 * 1024;
/// 帧头长度：u32 大端长度，只计算 JSON 部分。
pub const FRAME_HEADER_BYTES: usize = 4;

/// 执行槽位数 X_max 默认值（可配置 1..=16）。每进程一个任务；常驻内存
/// ≈ X_max × 单进程 RSS（JS heap 32 MiB 上限 + 输入面预算 8 MiB）。
pub const X_MAX_DEFAULT: usize = 4;

/// 在飞 run 状态数上限（一期 F02/F17 基线，`R_max ≥ 1000`）。
/// 只约束主进程 run 状态内存、调度表与可卸载索引；与窗口预算无关。
pub const R_MAX: usize = 1000;

/// 活跃派发上界（继承一期执行并发 1..=16；主进程调度器同值限制）。
/// 全局窗口预算按它反推：`A_MAX × W_BYTES × K_FACTOR`。
pub const A_MAX: usize = 16;

/// 单派发持久确认窗口 W（审计在途字节，按编码后字节数计费）。
/// 依据：单条编码记录 ≤ [`MAX_AUDIT_RECORD_BYTES`]（512 KiB），W 取 4 条
/// 记录的余量，允许一个 256 KiB 原始 chunk 的 base64 编码记录在飞的
/// 同时仍有空间继续推进；由允许常驻内存上界（128 MiB）反推。
pub const W_BYTES: u64 = 2 * 1024 * 1024;

/// 每派发实际预算对 W 的倍数 k：生产队列 + 输入 transfer + 重传副本 +
/// 解码扩张 + 待落盘队列。全局预算 = `A_MAX × W × K` = 128 MiB（估算值，
/// I10 以真实负载复核）。
pub const K_FACTOR: u64 = 4;

/// 全局活跃派发预算（常驻内存上界，估算）。
pub const GLOBAL_DISPATCH_BUDGET_BYTES: u64 = A_MAX as u64 * W_BYTES * K_FACTOR;

/// 单条审计记录的最大协议编码字节数。必须 ≤ W，否则存在永远无法发送的
/// 记录（二期 §3.5 死锁条款）。base64(256 KiB chunk) ≈ 350 KiB < 512 KiB。
pub const MAX_AUDIT_RECORD_BYTES: u64 = 512 * 1024;

/// 输入/操作请求传输的原始块字节数（base64 前）。与一期 CHUNK_BYTES 对齐，
/// 避免主进程重切日志 chunk。
pub const TRANSFER_CHUNK_BYTES: usize = 256 * 1024;
/// 输入传输在途窗口（独立于审计窗口；二期 §3.5 末段）。
pub const INPUT_TRANSFER_WINDOW_BYTES: u64 = 2 * 1024 * 1024;

/// 观测（可丢弃可观察性记录）通道预算：有界队列深度，超限丢弃并计数。
/// 观测永不反压 golden source、不占 audit_seq、不占持久窗口（二期 §4.3）。
pub const OBSERVABILITY_QUEUE_DEPTH: usize = 256;
/// 单条 ObservabilityBatch 最多行数。
pub const OBSERVABILITY_BATCH_LINES: usize = 64;
/// 单条 ObservabilityBatch 的**转义后**字节预算（保守估计累计）：保证
/// 编码帧必然落在 DATA_MAX_FRAME 内——观测流量自身永远不能把帧打爆。
pub const OBSERVABILITY_BATCH_BYTES: usize = DATA_MAX_FRAME / 2;
/// 观测行消息上限（超出即截断为丢弃计数）。
pub const OBSERVABILITY_LINE_BYTES: usize = 16 * 1024;

/// 执行器心跳间隔（control，执行器→主进程，仅存活/进度提示）。
pub const HEARTBEAT_INTERVAL_MS: u64 = 2_000;
/// 心跳超时：超过后连接进入 Suspect（冻结新派发），二期 §3.7。
pub const HEARTBEAT_TIMEOUT_MS: u64 = 10_000;
/// 握手超时：Starting/Handshaking 状态停留上限，超时回收子进程。
pub const STARTUP_TIMEOUT_MS: u64 = 10_000;
/// Draining 宽限期：尽力封口窗口，到期 kill 并 wait/reap（二期 §3.9）。
pub const DRAIN_GRACE_MS: u64 = 5_000;
/// AuditAck 等待超时后重传同序号同内容（幂等；二期 §3.5）。
pub const ACK_RETRANSMIT_MS: u64 = 2_000;
/// 等待持久 ACK（AuditAck/Permit/ResultCommitted）的总超时上限，防无限占用
/// 槽位；超时按失联处理进入排空。0 表示无限制（由取消驱动）。
pub const DURABLE_WAIT_TIMEOUT_MS: u64 = 120_000;

/// 执行器二进制名称（打包/定位契约，I09）。
pub const EXECUTOR_BIN_NAME: &str = "flow-executor";
/// 主进程通过环境变量把固定 FD 槽位告知执行器（pre_exec 中 dup2 的目标）。
pub const EXECUTOR_CONTROL_FD_SLOT: i32 = 100;
pub const EXECUTOR_DATA_FD_SLOT: i32 = 101;
/// 显式指定执行器二进制路径的环境变量（优先级最高，I09）。
pub const EXECUTOR_BIN_ENV: &str = "FLOW_EXECUTOR_BIN";
/// 执行模式环境变量：`in_process`（默认，一期行为）或 `ipc`（二期）。
pub const EXECUTION_MODE_ENV: &str = "FLOW_EXECUTION_MODE";

/// 执行器在 Hello 中声明的能力目录。缺失必需能力的主进程必须明确拒绝，
/// 不降级执行（二期 §3.2）。
pub const REQUIRED_CAPABILITIES: &[&str] = &["execute", "js", "http", "transfer"];

/// 会话身份字段长度约束（二期 §3.2 表）。
pub const IDENTITY_MAX_BYTES: usize = 128;
