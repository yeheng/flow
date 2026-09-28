//! flow-cli：flow 工作流引擎的命令行客户端。
//!
//! 形态：`flow-cli [全局选项] <命令组> <子命令> [参数]`。命令面与 flow-server 的
//! JSON-RPC 方法一一对应（DESIGN.md §9），CLI 是**纯客户端**——不直连 SQLite、
//! 不读事件日志，SQLite / Postgres 两个后端行为一致。
//!
//! 两条使用纪律：
//! - **文档走 stdout**：`workflow get` / `workflow export` / `run get` /
//!   `run start`（等待模式）把 JSON 写到 stdout，进度与提示写到 stderr，
//!   于是 `flow-cli workflow get X > def.json` 拿到的是纯 JSON；
//! - **退出码分流**：0 成功；1 本地错误；2 服务端 RPC 错误；4 触发的 run
//!   终态为 failed / cancelled（脚本据此区分「命令打错」与「工作流失败」）。

mod client;
mod error;
mod output;
mod run;
mod workflow;

use clap::{Parser, Subcommand};
use std::process::ExitCode;

use crate::error::CliError;

/// flow-cli：flow 工作流引擎命令行客户端（JSON-RPC 2.0 over WebSocket）。
#[derive(Parser)]
#[command(
    name = "flow-cli",
    version,
    about = "flow 工作流引擎命令行客户端",
    long_about = "flow 工作流引擎命令行客户端。\n\n\
                  覆盖：workflow 增删改查与导入导出、run 手动触发与查询取消。\n\
                  服务端需另行运行（cargo run --bin flow-server），地址用 --url 或 FLOW_RPC 指定。",
    arg_required_else_help = true
)]
struct Cli {
    /// flow-server 地址；缺 scheme（如 127.0.0.1:9800）按 ws:// 补全
    #[arg(
        long,
        global = true,
        env = "FLOW_RPC",
        default_value = "ws://127.0.0.1:9800"
    )]
    url: String,
    /// 打印服务端返回的原始 JSON（2 空格缩进），供 jq 等工具消费
    #[arg(long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// 工作流定义：增删改查 + 导入导出
    Workflow {
        #[command(subcommand)]
        command: WorkflowCommand,
    },
    /// 运行：手动触发 run + 运行记录查询/取消
    Run {
        #[command(subcommand)]
        command: RunCommand,
    },
}

#[derive(Subcommand)]
enum WorkflowCommand {
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

#[derive(Subcommand)]
enum RunCommand {
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

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match dispatch(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::from(err.exit_code() as u8)
        }
    }
}

/// 进程入口：先连服务（连不上是本地错误，exit 1），再分发到命令组。
async fn dispatch(cli: Cli) -> Result<(), CliError> {
    let client = client::connect(&cli.url).await?;
    match cli.command {
        Command::Workflow { command } => workflow::dispatch(&client, cli.json, command).await,
        Command::Run { command } => run::dispatch(&client, cli.json, command).await,
    }
}
