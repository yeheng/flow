//! Explicit development entry. It never opens the legacy SQLite/PG authority.
use clap::{Parser, Subcommand};
use flow_backend::journal::JournalBackend;
use flow_journal::JournalOptions;
use std::collections::BTreeSet;
use std::io::Read;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(about = "JSONL v2 development runner; phase-one acceptance is still in progress")]
struct Args {
    #[arg(long)]
    data_dir: PathBuf,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
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
    /// Offline: rebuild into a NEW SQLite file, without executing any workflow.
    RebuildProjection {
        #[arg(long)]
        destination: PathBuf,
    },
}
fn read_json(path: &Path, max: usize) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(max as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err("input file exceeds development budget".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    if let Command::RebuildProjection { destination } = &args.command {
        let lsn = JournalBackend::rebuild_projection(&args.data_dir, destination).await?;
        println!(
            "{}",
            serde_json::json!({"applied_lsn":lsn.to_string(),"destination":destination})
        );
        return Ok(());
    }
    let backend = JournalBackend::open(&args.data_dir, JournalOptions::default()).await?;
    if let Command::Run { definition, input } = &args.command {
        let definition = read_json(definition, flow_journal::MAX_LINE_BYTES)?;
        let input = input
            .as_ref()
            .map(|p| read_json(p, 8 * 1024 * 1024))
            .transpose()?
            .unwrap_or(serde_json::Value::Null);
        let created = backend.workflow_create("development", None).await?;
        let workflow = created.result["workflow_id"].as_str().unwrap();
        backend.workflow_update(workflow, definition, None).await?;
        backend.workflow_publish(workflow, 1, None).await?;
        let receipt = backend
            .run_start(workflow, None, input, "manual", None, None)
            .await?;
        println!("{}", serde_json::to_string(&receipt)?);
    }
    if matches!(args.command, Command::Status) {
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
    backend.start_execution().await?;
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
