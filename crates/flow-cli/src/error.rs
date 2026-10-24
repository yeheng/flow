//! CLI 错误类型与退出码约定。
//!
//! 退出码是脚本分流的主要接口（约定即契约，测试钉住）：
//!
//! | 码 | 含义 |
//! |---|---|
//! | 0 | 成功 |
//! | 1 | 本地错误：文件读取失败、JSON 解析失败、连不上服务、等待超时 |
//! | 2 | 服务端 RPC 错误（-320xx：workflow 不存在、定义非法、状态冲突…） |
//! | 4 | 触发的 run 终态为 failed / cancelled（CLI 自身没有失败） |
//!
//! 3/4 之间留着给未来扩展；这里刻意不复用 1：`run` 失败不是命令用法错误，
//! 调用方的 CI 需要能区分「命令打错了」与「工作流跑失败了」。

use std::fmt;

/// 本地错误（exit 1）。
pub const EXIT_LOCAL: i32 = 1;
/// 服务端 RPC 错误（exit 2）。
pub const EXIT_RPC: i32 = 2;
/// 触发的 run 非成功终态（exit 4）。
pub const EXIT_RUN_FAILED: i32 = 4;

/// CLI 的错误类型。
///
/// 三种变体对应三种完全不同的处置方式：本地错误要人查环境，RPC 错误要人
/// 改请求，run 失败要人看业务。退出码把它们映射到可被脚本区分的三条路。
#[derive(Debug)]
pub enum CliError {
    /// 本地错误：文件、JSON、连接、超时、用法提示之外的一切环境问题。
    Local(String),
    /// 服务端返回的 JSON-RPC 错误对象（code 原样透出，供脚本分流）。
    Rpc { code: i32, message: String },
    /// run 终态为 failed / cancelled。
    RunFailed {
        /// 运行 id。
        run_id: String,
        /// 终态（failed / cancelled）。
        status: String,
        /// 服务端给出的失败原因；可能为空。
        error: Option<String>,
    },
}

impl CliError {
    /// 构造本地错误。
    pub fn local(message: impl Into<String>) -> CliError {
        CliError::Local(message.into())
    }

    /// 错误对应的进程退出码。
    pub fn exit_code(&self) -> i32 {
        match self {
            CliError::Local(_) => EXIT_LOCAL,
            CliError::Rpc { .. } => EXIT_RPC,
            CliError::RunFailed { .. } => EXIT_RUN_FAILED,
        }
    }
}

impl fmt::Display for CliError {
    /// 把错误渲染成一行人可读文本。
    ///
    /// RPC 错误必须带上 `code`：脚本可能只按 code 分流，人也要靠它判断
    /// 该重试还是该改请求。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::Local(message) => write!(f, "{message}"),
            CliError::Rpc { code, message } => {
                write!(f, "{message}（服务端错误 code {code}）")
            }
            CliError::RunFailed {
                run_id,
                status,
                error,
            } => {
                write!(f, "run {run_id} {status}")?;
                match error {
                    Some(error) if !error.is_empty() => write!(f, "：{error}"),
                    _ => Ok(()),
                }
            }
        }
    }
}

impl std::error::Error for CliError {}

/// 把文件/IO 错误收敛成本地错误，附带定位信息。
///
/// 带上上下文（哪个文件、哪一步）比只抛 IO 错误有用得多：CLI 的使用者
/// 往往只看到一行 stderr。
pub fn io_err(context: &str, err: std::io::Error) -> CliError {
    CliError::local(format!("{context}：{err}"))
}
