//! flow-journal-tool：journal 离线维护（bin: flow-journal-tool）。
//!
//! verify / rebuild-index / backup。全部动作先取数据目录排他锁。
//!
//! 运行：cargo run -p flow-journal --bin flow-journal-tool -- verify <data_dir>

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// flow-journal-tool：journal 离线维护。
#[derive(Parser)]
#[command(
    name = "flow-journal-tool",
    version,
    about = "journal 离线维护（verify / rebuild-index / backup）",
    arg_required_else_help = true
)]
struct JournalTool {
    #[command(subcommand)]
    command: JournalToolCommand,
}

#[derive(Subcommand)]
enum JournalToolCommand {
    /// 全量字节校验 + 已发布值闭环检查
    Verify { data_dir: PathBuf },
    /// 重建 SQLite 自有索引（不创建应用索引文件）
    RebuildIndex { data_dir: PathBuf },
    /// 将一致 SQLite 快照备份到新目录
    Backup {
        data_dir: PathBuf,
        destination: PathBuf,
    },
}

fn main() -> std::process::ExitCode {
    let tool = match JournalTool::try_parse() {
        Ok(args) => args,
        Err(error) => {
            let success = matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            );
            let _ = error.print();
            std::process::exit(if success { 0 } else { 1 });
        }
    };
    let code = journal_tool_main(tool.command);
    if code != 0 {
        return std::process::ExitCode::from(code.clamp(0, 255) as u8);
    }
    std::process::ExitCode::SUCCESS
}

/// 离线维护（全部动作先取数据目录排他锁）。
fn journal_tool_main(command: JournalToolCommand) -> i32 {
    let report = match command {
        JournalToolCommand::Verify { data_dir } => {
            let _lock = match flow_journal::maintenance::offline_lock(&data_dir) {
                Ok(lock) => lock,
                Err(err) => return fail(&err),
            };
            match flow_journal::maintenance::verify(&data_dir) {
                Ok(report) => report,
                Err(err) => return fail(&err),
            }
        }
        JournalToolCommand::RebuildIndex { data_dir } => {
            let _lock = match flow_journal::maintenance::offline_lock(&data_dir) {
                Ok(lock) => lock,
                Err(err) => return fail(&err),
            };
            match flow_journal::maintenance::rebuild_index(&data_dir, u64::MAX) {
                Ok(report) => report,
                Err(err) => return fail(&err),
            }
        }
        JournalToolCommand::Backup {
            data_dir,
            destination,
        } => {
            let _lock = match flow_journal::maintenance::offline_lock(&data_dir) {
                Ok(lock) => lock,
                Err(err) => return fail(&err),
            };
            match flow_journal::maintenance::backup(&data_dir, &destination, u64::MAX) {
                Ok(report) => report,
                Err(err) => return fail(&err),
            }
        }
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).unwrap_or_default()
    );
    if report.fault.is_some() {
        eprintln!("journal verification found corruption; source is unchanged");
        return 1;
    }
    0
}

fn fail(err: &dyn std::fmt::Display) -> i32 {
    eprintln!("error: {err}");
    1
}
