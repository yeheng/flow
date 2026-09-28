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

#[derive(Debug)]
pub enum CliError {
    /// 本地错误：文件、JSON、连接、超时、用法提示之外的一切环境问题。
    Local(String),
    /// 服务端返回的 JSON-RPC 错误对象（code 原样透出，供脚本分流）。
    Rpc { code: i32, message: String },
    /// run 终态为 failed / cancelled。
    RunFailed {
        run_id: String,
        status: String,
        error: Option<String>,
    },
}

impl CliError {
    pub fn local(message: impl Into<String>) -> CliError {
        CliError::Local(message.into())
    }

    pub fn exit_code(&self) -> i32 {
        match self {
            CliError::Local(_) => EXIT_LOCAL,
            CliError::Rpc { .. } => EXIT_RPC,
            CliError::RunFailed { .. } => EXIT_RUN_FAILED,
        }
    }
}

impl fmt::Display for CliError {
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
pub fn io_err(context: &str, err: std::io::Error) -> CliError {
    CliError::local(format!("{context}：{err}"))
}
