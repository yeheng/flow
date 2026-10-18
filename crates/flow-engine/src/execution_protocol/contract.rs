//! 冻结的 IPC 契约（单流 postcard 帧）。
//!
//! 这些常量是协议冻结面：主进程与执行器实现都必须在同一组上限内工作，
//! 任何一方都不允许在握手之外重新协商硬上限。修改本文件即构成协议版本
//! 变更，必须 bump [`PROTOCOL_VERSION`]。
//!
//! 两类协议并发面符号（不得互相代用）：
//! - `A_MAX` 活跃派发上界（同时持有 dispatch 事实、占窗口预算），窗口全局
//!   预算 = `A_MAX × W_BYTES`；
//! - `X_MAX_DEFAULT` 执行槽位数（同时运行的执行子进程），约束进程数 × RSS。
//!
//! `A_MAX` 不得用来论证主进程状态内存，`X_MAX` 不得用来论证审计吞吐。
//! 在飞 run 状态数上限（`R_MAX`）是主进程状态内存约束、不是协议量，其
//! 唯一定义与执行点在 `flow-backend`（`journal.rs::R_MAX`）。

/// IPC/上联协议版本。单流帧 `u32 body_len | postcard(Message)`：旧版 JSON
/// 信封、三种帧 kind、十进制字符串与 Channel 概念均已删除。版本门集中在
/// 握手的 version 字段——执行器侧 Hello/Welcome 与 agent 上联侧
/// AgentHello/AgentWelcome 都必须携带并校验它。postcard 的变体索引是位置
/// 编码，跨版本配对会静默错解成另一个消息类型，没有版本门的网络侧握手
/// 等于把「未定义但不报错」留给部署。v6 = 上联握手补上版本门（旧 agent
/// 无法安全共存，不提供兼容垫片）。
/// v7 adds recorded execution context, strict regression isolation and native model calls;
/// older executors must not silently ignore this task metadata.
pub const PROTOCOL_VERSION: u32 = 7;

/// control 通道单帧上限（不含 4 字节长度头）。握手
/// 只可协商到本上限以内。
///
/// 取 128 KiB 的约束：内联业务值上限 `flow_journal::INLINE_BYTES` 是 64 KiB
/// （按编码后字节数计），`Result`/`Wait` 控制帧 = 值编码 + 信封必须永远
/// 装得下——64 KiB 值 + 64 KiB 余量使「可 inline 但装不进控制帧」的窄带
/// 在算术上不可达，不再需要降级补丁。
pub const CONTROL_MAX_FRAME: usize = 128 * 1024;
/// 控制帧里自由文本（错误消息 / 拒绝原因）的上限。JS 抛出的消息长度用户可控，
/// 无界文本同样会撑爆控制帧并杀会话。
pub const CONTROL_TEXT_MAX: usize = 8 * 1024;

/// 把控制帧里的自由文本截到 [`CONTROL_TEXT_MAX`]（保持 UTF-8 边界）。
/// 超限时追加截断标记，便于排障时辨认。
pub fn bounded_control_text(mut text: String) -> String {
    if text.len() <= CONTROL_TEXT_MAX {
        return text;
    }
    let mut end = CONTROL_TEXT_MAX;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text.push_str(" …[truncated]");
    text
}

/// 单帧 body 上限（不含 4 字节长度头）：data 语义消息（TransferChunk /
/// AuditBatch / ObservabilityBatch / Routed）按此预算，control 语义消息
/// 收紧到 [`CONTROL_MAX_FRAME`]。
pub const DATA_MAX_FRAME: usize = 1024 * 1024;

/// 执行槽位数 X_max 默认值（可配置 1..=16）。每进程一个任务；常驻内存
/// ≈ X_max × 单进程 RSS（JS heap 32 MiB 上限 + 输入面预算 8 MiB）。
pub const X_MAX_DEFAULT: usize = 4;

/// 活跃派发上界（继承一期执行并发 1..=16；主进程调度器同值限制）。
/// 全局窗口预算按它反推：`A_MAX × W_BYTES × K_FACTOR`。
pub const A_MAX: usize = 16;

/// 单派发持久确认窗口 W（审计在途字节，按编码后字节数计费）。
/// 依据：单条编码记录 ≤ [`MAX_AUDIT_RECORD_BYTES`]（512 KiB），W 取 4 条
/// 记录的余量，允许一个 256 KiB 原始 chunk 记录在飞的同时仍有空间继续推进。
pub const W_BYTES: u64 = 2 * 1024 * 1024;

/// 单条审计记录的最大协议编码字节数。必须 ≤ W，否则存在永远无法发送的
/// 记录（二期 §3.5 死锁条款）。256 KiB chunk + 元数据 < DATA_MAX_FRAME。
pub const MAX_AUDIT_RECORD_BYTES: u64 = 512 * 1024;

/// 输入/操作请求传输的原始块字节数。与一期 CHUNK_BYTES 对齐，
/// 避免主进程重切日志 chunk。
///
/// 传输体积的真实约束不在此文件，由接收侧硬执行：输入流聚合 ≤ 执行器
/// `INPUT_BUDGET`（8 MiB）、单值 ≤ `flow_journal::MAX_VALUE_BYTES`
/// （见 flow-executor `transfer.rs` 的 feed/input_ready 拒绝路径）；
/// agent 侧队列按帧数有界（relay.rs）。本文件不重复声明这些数字。
pub const TRANSFER_CHUNK_BYTES: usize = 256 * 1024;

/// 观测（可丢弃可观察性记录）：帧级由 [`OBSERVABILITY_BATCH_LINES`] 与
/// [`OBSERVABILITY_BATCH_BYTES`] 约束（observe.rs 执行）；队列深度由
/// transport 出站队列按帧数有界；观测行截断由
/// `flow_engine::nodelog::MAX_LOG_LINE_BYTES` 执行（emit 入口统一截断）。
/// 观测永不反压 golden source、不占 audit_seq、不占持久窗口（二期 §4.3）。
pub const OBSERVABILITY_BATCH_LINES: usize = 64;
/// 单条 ObservabilityBatch 的**转义后**字节预算（保守估计累计）：保证
/// 编码帧必然落在 DATA_MAX_FRAME 内——观测流量自身永远不能把帧打爆。
pub const OBSERVABILITY_BATCH_BYTES: usize = DATA_MAX_FRAME / 2;

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

/// 执行器二进制名称（独立二进制的打包/定位契约，I09）。
/// 主进程（flow-journal-server / flow-agent）缺省在自身同目录定位该名字的兄弟文件。
pub const EXECUTOR_BIN_NAME: &str = "flow-executor";

/// 执行器进程召唤描述：程序路径。
///
/// 执行器是独立二进制，召唤只带程序路径；模式/诊断标记（`--remote-relay`、
/// `--tag`、`--channel-fd`）由 spawn 入口按调用点显式追加，不做「预置参数
/// 列表」的隐式传递。
#[derive(Debug, Clone)]
pub struct ExecutorInvocation {
    pub program: std::path::PathBuf,
}

impl ExecutorInvocation {
    /// 显式指定的执行器二进制。
    pub fn explicit(bin: impl Into<std::path::PathBuf>) -> Self {
        Self {
            program: bin.into(),
        }
    }

    /// 执行器定位（I09）：[`EXECUTOR_BIN_ENV`] 显式路径优先；否则取当前
    /// 可执行文件同目录下的兄弟 [`EXECUTOR_BIN_NAME`]（部署形态：全部
    /// 产品二进制同目录分发）。找不到即报错，不静默回退进程内执行。
    pub fn locate() -> Result<Self, String> {
        if let Some(path) = std::env::var_os(EXECUTOR_BIN_ENV) {
            let path = std::path::PathBuf::from(path);
            if path.is_file() {
                return Ok(Self::explicit(path));
            }
            return Err(format!(
                "{EXECUTOR_BIN_ENV}={path:?} does not point to an executable file"
            ));
        }
        let current = std::env::current_exe()
            .map_err(|_| "cannot locate executor: current_exe unavailable".to_string())?;
        let sibling = current
            .parent()
            .map(|dir| dir.join(EXECUTOR_BIN_NAME))
            .ok_or_else(|| "cannot locate executor: current_exe has no parent".to_string())?;
        if sibling.is_file() {
            return Ok(Self::explicit(sibling));
        }
        Err(format!(
            "cannot locate {EXECUTOR_BIN_NAME} next to {current:?}: set {EXECUTOR_BIN_ENV} \
             or deploy {EXECUTOR_BIN_NAME} beside the main binary"
        ))
    }
}
/// 显式指定执行器二进制路径的环境变量（优先级最高，I09）。
pub const EXECUTOR_BIN_ENV: &str = "FLOW_EXECUTOR_BIN";
/// 执行模式环境变量：默认 ipc（全平台）；显式值 `ipc` 覆盖默认，
/// `in_process` 已全平台删除、显式设置即报错。
pub const EXECUTION_MODE_ENV: &str = "FLOW_EXECUTION_MODE";

/// 回连承载的会话令牌环境变量（loopback 防本机抢连）：主进程 spawn 时
/// 随机生成并注入，执行器在 Hello 里回验；unix socketpair 走 FD 能力
/// （exec 前关闭其余继承 FD）不需要，不注入。
pub const SESSION_TOKEN_ENV: &str = "FLOW_EXECUTOR_SESSION_TOKEN";

/// 执行器在 Hello 中声明的能力目录。缺失必需能力的主进程必须明确拒绝，
/// 不降级执行（二期 §3.2）。
pub const REQUIRED_CAPABILITIES: &[&str] = &["execute", "js", "http", "transfer"];

/// 会话身份字段长度约束（二期 §3.2 表）。
pub const IDENTITY_MAX_BYTES: usize = 128;
