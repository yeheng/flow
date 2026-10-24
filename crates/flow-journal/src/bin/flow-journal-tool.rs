//! flow-journal-tool：journal 离线维护（bin: flow-journal-tool）。
//!
//! verify / rebuild-index / backup。全部动作先取数据目录排他锁。
//!
//! 运行：cargo run -p flow-journal --bin flow-journal-tool -- verify <data_dir>
//!
//! 本工具是运维面：只做「读权威库 / 复制权威库」这类动作，绝不修改线上
//! 权威本身。
//! 核心不变量：每个动作都先拿数据目录排他锁（写者必须已停机）；校验发现
//! 问题时源库原样不动，退出码非零表示「发现问题」而不是「执行失败」。
//! 需要它的原因：完整性核验与备份必须能在写者之外独立进行，否则无法区分
//! 「权威坏了」与「正在写入」。

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

/// 工具支持的离线动作。
#[derive(Subcommand)]
enum JournalToolCommand {
    /// Upgrade a stopped format-3 database into a NEW format-4 directory.
    Upgrade {
        source: PathBuf,
        destination: PathBuf,
    },
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

/// 命令行入口。
///
/// 帮助与版本信息视为成功（退出码 0），参数错误才是失败：脚本里 `--help`
/// 不应该被当成故障。
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
///
/// 每个分支都遵循同一顺序：先取锁，再执行动作，最后统一打印报告；报告里
/// 带故障时退出码为 1。
fn journal_tool_main(command: JournalToolCommand) -> i32 {
    let report = match command {
        JournalToolCommand::Upgrade {
            source,
            destination,
        } => {
            let _lock = match flow_journal::maintenance::offline_lock(&source) {
                Ok(v) => v,
                Err(e) => return fail(&e),
            };
            match flow_journal::maintenance::upgrade(&source, &destination) {
                Ok(v) => v,
                Err(e) => return fail(&e),
            }
        }
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
    // 报告以 JSON 输出，便于运维工具解析。
    println!(
        "{}",
        serde_json::to_string_pretty(&report).unwrap_or_default()
    );
    // 校验发现故障时退出码非零：CI 与巡检脚本据此判定权威不可信。
    if report.fault.is_some() {
        eprintln!("journal verification found corruption; source is unchanged");
        return 1;
    }
    0
}

/// 打印错误并返回失败退出码。
fn fail(err: &dyn std::fmt::Display) -> i32 {
    eprintln!("error: {err}");
    1
}
