//! flow-journal-dev：JSONL v2 开发运行器（bin: flow-journal-dev）。
//!
//! run / resume / status / import-legacy / rebuild-projection。
//! run 安装定义并真实执行；resume 只恢复「按已提交事实安全」的工作；
//! import-legacy 把停写 legacy（v1 SQLite）数据集保全为只读基线；
//! rebuild-projection 离线把 journal 重建进**新** SQLite 文件（不执行工作流）。
//!
//! 运行：cargo run -p flow-backend --bin flow-journal-dev -- --data-dir <dir> status

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "flow-journal-dev",
    version,
    about = "JSONL v2 开发运行器（run / resume / status / import-legacy）",
    arg_required_else_help = true
)]
struct JournalDev {
    /// 统一配置文件路径（缺省：FLOW_CONFIG → ./flow.toml → 平台配置目录）。
    #[arg(long, global = true, env = "FLOW_CONFIG")]
    config: Option<PathBuf>,
    /// journal 数据目录
    #[arg(long)]
    data_dir: PathBuf,
    #[command(subcommand)]
    command: JournalDevCommand,
}

#[derive(Subcommand)]
enum JournalDevCommand {
    /// Install/publish a definition and run it with a JSON input file.
    Run {
        #[arg(long)]
        definition: PathBuf,
        #[arg(long)]
        input: Option<PathBuf>,
    },
    /// Resume only work that is safe according to committed facts. Unknown operations wait.
    Resume,
    /// Print recovered state without starting any execution.
    Status,
    /// Preserve a stopped legacy dataset as a read-only baseline in a new JSONL directory.
    ImportLegacy {
        #[arg(long)]
        source: PathBuf,
        #[arg(long)]
        database: PathBuf,
    },
    /// Offline: rebuild into a NEW SQLite file, without executing any workflow.
    RebuildProjection {
        #[arg(long)]
        destination: PathBuf,
    },
}

fn main() -> std::process::ExitCode {
    let dev = match JournalDev::try_parse() {
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
    // 配置文件在构造 runtime 之前同步加载（[execution] 分区驱动执行模式）。
    let loaded = match flow_config::Config::load(dev.config.as_deref()) {
        Ok(loaded) => loaded,
        Err(err) => return exit_fail(&err),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("journal-dev tokio runtime");
    let code = match runtime.block_on(journal_dev(
        loaded.config.execution,
        dev.data_dir,
        dev.command,
    )) {
        Ok(()) => 0,
        Err(err) => {
            eprintln!("error: {err}");
            1
        }
    };
    if code != 0 {
        return std::process::ExitCode::from(code.clamp(0, 255) as u8);
    }
    std::process::ExitCode::SUCCESS
}

fn exit_fail(err: &dyn std::fmt::Display) -> std::process::ExitCode {
    eprintln!("error: {err}");
    std::process::ExitCode::FAILURE
}

async fn journal_dev(
    execution: flow_config::ExecutionConfig,
    data_dir: PathBuf,
    command: JournalDevCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::BTreeSet;
    use std::io::Read;

    use flow_backend::journal::JournalBackend;
    use flow_journal::JournalOptions;

    fn read_json(
        path: &std::path::Path,
        max: usize,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(max as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > max {
            return Err("input file exceeds development budget".into());
        }
        Ok(serde_json::from_slice(&bytes)?)
    }

    if let JournalDevCommand::ImportLegacy { source, database } = &command {
        let report = flow_backend::journal_import::import(source, database, &data_dir).await?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    if let JournalDevCommand::RebuildProjection { destination } = &command {
        let lsn = JournalBackend::rebuild_projection(&data_dir, destination).await?;
        println!(
            "{}",
            serde_json::json!({"applied_lsn":lsn.to_string(),"destination":destination})
        );
        return Ok(());
    }
    let backend = JournalBackend::open(&data_dir, JournalOptions::default()).await?;
    if let JournalDevCommand::Run { definition, input } = &command {
        let definition = read_json(definition, flow_journal::MAX_LINE_BYTES)?;
        let input = input
            .as_ref()
            .map(|p| read_json(p, 8 * 1024 * 1024))
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        let created = backend.workflow_create("development", None).await?;
        let workflow = created.result["workflow_id"].as_str().expect("workflow_id");
        backend.workflow_update(workflow, definition, None).await?;
        backend.workflow_publish(workflow, 1, None).await?;
        let receipt = backend
            .run_start(workflow, None, input, "manual", None, None)
            .await?;
        println!("{}", serde_json::to_string(&receipt)?);
    }
    if matches!(command, JournalDevCommand::Status) {
        serde_json::to_writer_pretty(std::io::stdout(), &backend.state().await)?;
        println!();
        backend.close().await?;
        return Ok(());
    }
    let mut pending = backend
        .state()
        .await
        .runs
        .values()
        .filter(|r| !r.terminal())
        .map(|r| r.run_id.clone())
        .collect::<BTreeSet<_>>();
    flow_backend::start_execution(&execution, &backend).await?;
    while !pending.is_empty() {
        tokio::select! {_=tokio::signal::ctrl_c()=>break,_=tokio::time::sleep(std::time::Duration::from_millis(100))=>{}}
        for run in backend.state().await.runs.values().filter(|r| r.terminal()) {
            if pending.remove(&run.run_id) {
                serde_json::to_writer(std::io::stdout(), run)?;
                println!();
            }
        }
    }
    backend.close().await?;
    Ok(())
}
