//! `flow-cli` 命令定义与分发。
//!
//! 本模块只负责三件事：**定义命令行表面**（clap）、**把参数分派到对应
//! 的业务模块**、**把错误映射成进程退出码**。它不包含任何业务判断——
//! 「这个工作流能不能跑」这类问题一律由服务端回答，CLI 只负责如实转述。
//!
//! 为什么坚持「CLI 不做业务判断」：一旦 CLI 自己也有一份校验，两份规则
//! 就会随版本漂移，用户会遇到「CLI 说可以、服务端说不行」这种最难排查
//! 的不一致。
//!
//! 全局参数（`--url` / `--json` / `--request_id`）是 `global = true`，
//! 因此可以出现在任意子命令之后——脚本里通常把通用参数写在末尾。

use clap::{Parser, Subcommand};

use crate::error::CliError;

/// flow-cli：flow 工作流引擎命令行客户端（JSON-RPC 2.0 over WebSocket）。
#[derive(Parser)]
#[command(
    name = "flow-cli",
    version,
    about = "flow 工作流引擎命令行客户端",
    long_about = "flow 工作流引擎命令行客户端。\n\n\
                  覆盖：workflow 增删改查与导入导出、run 手动触发与查询取消。\n\
                  服务端需另行运行（flow-server），地址用 --url 或 FLOW_RPC 指定。",
    arg_required_else_help = true
)]
pub struct Cli {
    /// flow-server 地址；缺 scheme（如 127.0.0.1:9802）按 ws:// 补全
    #[arg(
        long,
        global = true,
        env = "FLOW_RPC",
        default_value = "ws://127.0.0.1:9802"
    )]
    url: String,
    /// 打印服务端返回的原始 JSON（2 空格缩进），供 jq 等工具消费
    #[arg(long, global = true)]
    json: bool,
    /// Stable identity for writes; reuse it with identical arguments after a lost reply.
    #[arg(long, global = true, value_parser = parse_request_id)]
    request_id: Option<String>,
    /// 要执行的子命令。
    #[command(subcommand)]
    command: Command,
}

/// CLI 的顶层命令组。
#[derive(Subcommand)]
pub enum Command {
    /// JSONL v2 commands, bounded history pages and streaming downloads.
    Journal {
        /// journal 子命令。
        #[command(subcommand)]
        command: crate::journal::Command,
    },
    /// 工作流定义：增删改查 + 导入导出
    Workflow {
        /// workflow 子命令。
        #[command(subcommand)]
        command: WorkflowCommand,
    },
    /// 运行：手动触发 run + 运行记录查询/取消
    Run {
        /// run 子命令。
        #[command(subcommand)]
        command: RunCommand,
    },
}

/// 工作流相关子命令。
#[derive(Subcommand)]
pub enum WorkflowCommand {
    /// 列出全部工作流
    List,
    /// 查看定义（默认最新版本）
    Get {
        workflow_id: String,
        /// 指定版本（缺省取最新版本，可能是 draft）
        #[arg(long)]
        version: Option<i64>,
    },
    /// 版本历史（倒序：元数据列，定义走 workflow get）
    Versions { workflow_id: String },
    /// 创建空壳工作流（随后 update 补定义、publish 后才能执行）
    Create {
        #[arg(long)]
        name: String,
    },
    /// 保存定义为新版本（落库即校验，非法定义 -32010）
    Update {
        workflow_id: String,
        /// 定义文件（JSON）；'-' 读标准输入
        #[arg(long)]
        file: String,
    },
    /// 发布版本（只有 published 可执行）
    Publish {
        workflow_id: String,
        #[arg(long)]
        version: i64,
    },
    /// 删除工作流（已产生 run 记录时服务端拒绝）
    Delete {
        workflow_id: String,
        /// 跳过确认提示
        #[arg(short, long)]
        yes: bool,
    },
    /// 导入工作流：按 name upsert 并默认发布（文件含 {name, definition} 信封或裸 definition）
    Import {
        /// 定义文件；'-' 读标准输入
        file: String,
        /// 覆盖文件里的 name（缺省用信封 name，再缺省用文件名去扩展名）
        #[arg(long)]
        name: Option<String>,
        /// 导入后不发布（保持 draft）
        #[arg(long)]
        no_publish: bool,
    },
    /// 导出工作流定义为 {name, definition} 信封（可再 import 回来）
    Export {
        workflow_id: String,
        /// 指定版本（缺省取最新版本）
        #[arg(long)]
        version: Option<i64>,
        /// 输出文件（缺省 stdout）
        #[arg(short, long)]
        output: Option<String>,
    },
}

/// 运行相关子命令。
#[derive(Subcommand)]
pub enum RunCommand {
    /// 手动触发一次 run（默认等待终态并打印输出）
    Start {
        /// workflow_id 或 workflow name
        workflow: String,
        /// run 输入：内联 JSON、@文件 或 '-'（标准输入）；省缺为 null
        #[arg(long)]
        input: Option<String>,
        /// 指定版本（缺省取最新已发布版本）
        #[arg(long)]
        version: Option<i64>,
        /// 不等待终态，打印 run_id 即返回
        #[arg(long)]
        detach: bool,
        /// 等待终态的超时时间（秒）
        #[arg(long, default_value = "120")]
        timeout: u64,
    },
    /// 运行记录列表（新的在前）
    List {
        #[arg(long)]
        workflow_id: Option<String>,
        /// 状态过滤：initializing/running/awaiting_resume/succeeded/failed/cancelled
        #[arg(long)]
        status: Option<String>,
        /// 来源过滤：manual/schedule/webhook/sub_workflow
        #[arg(long)]
        source: Option<String>,
        #[arg(long, default_value = "20")]
        limit: i64,
    },
    /// 查看 run 记录（含 input/output/error）
    Get { run_id: String },
    /// 原始事件（JSONL，from_seq 增量）
    Events {
        run_id: String,
        #[arg(long)]
        from_seq: Option<u64>,
    },
    /// 只读时间线：节点状态 + 输出
    Timeline { run_id: String },
    /// 取消活着的 run（已终态返回 conflict）
    Cancel { run_id: String },
}

/// 进程入口：解析命令行 → 连服务（连不上是本地错误，exit 1）→ 分发到命令组。
///
/// 退出码契约：0 成功、1 本地/连接错误、2 参数错误、4 服务端业务错误。
/// 这套码是脚本的接口——CI 里靠它区分「命令写错了」与「业务失败了」。
pub async fn run_from_args(argv: &[String]) -> i32 {
    // parse_from 的首元素是程序名占位：`flow-cli` 是展示名，实际参数从 argv 起。
    let cli = Cli::parse_from(std::iter::once("flow-cli".to_string()).chain(argv.iter().cloned()));
    let request_id = cli.request_id.clone();
    // 整个执行包在一个幂等作用域里：回复丢失时用同一个 request_id 重发，
    // 服务端会认出「这是同一次写」而不是执行第二次。
    match crate::journal::REQUEST_ID
        .scope(request_id, dispatch(cli))
        .await
    {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("error: {err}");
            err.exit_code()
        }
    }
}

/// 建立连接并把子命令分派到对应业务模块。
///
/// 连接失败在这一层变成 `CliError`（本地错误，exit 1），与「服务端返回的
/// 业务错误」区分开：前者通常是服务没起，后者是请求本身有问题。
async fn dispatch(cli: Cli) -> Result<(), CliError> {
    let client = crate::client::connect(&cli.url).await?;
    match cli.command {
        Command::Journal { command } => crate::journal::dispatch(&client, command).await,
        Command::Workflow { command } => {
            crate::workflow::dispatch(&client, cli.json, command).await
        }
        Command::Run { command } => crate::run::dispatch(&client, cli.json, command).await,
    }
}

/// 校验 `--request_id`：非空且不超过 128 字节。
///
/// 长度上限是服务端存储的硬边界——超长 id 会在服务端被拒绝，
/// 与其让请求往返一趟才失败，不如在本地就挡住。
fn parse_request_id(value: &str) -> Result<String, String> {
    if value.is_empty() || value.len() > 128 {
        return Err("request_id must contain 1..=128 bytes".into());
    }
    Ok(value.into())
}
