use std::{io::Read, path::PathBuf, process::ExitCode};

use clap::Parser;
use flow_dispatcher::{execution_protocol::ExecutorInvocation, Dispatcher, IpcOptions};
use flow_domain::{journal_state::RunStatus, script::SCRIPT_V1_HANDLER, Definition};
use flow_facts::INLINE_BYTES;
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(about = "Run a local JSON workflow in memory with an IPC Script Worker")]
struct Args {
    #[arg(long)]
    worker: PathBuf,
    #[arg(long)]
    definition: PathBuf,
    #[arg(long)]
    input: Option<PathBuf>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match execute(Args::parse()).await {
        Ok(RunStatus::Succeeded) => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("flow-workflow: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn execute(args: Args) -> Result<RunStatus, Box<dyn std::error::Error>> {
    let mut definition = read_json(args.definition)?;
    if let Some(object) = definition.as_object_mut() {
        object.entry("semantics").or_insert(json!(2));
    }
    let definition: Definition = serde_json::from_value(definition)?;
    let input = args
        .input
        .map(read_json)
        .transpose()?
        .unwrap_or(Value::Null);
    let dispatcher = Dispatcher::new_streaming(
        IpcOptions {
            executor: ExecutorInvocation::explicit(args.worker),
            x_max: 1,
            tag: None,
        },
        SCRIPT_V1_HANDLER,
    );
    let cancel = CancellationToken::new();
    let signal = tokio::spawn({
        let cancel = cancel.clone();
        async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                cancel.cancel();
            }
        }
    });
    let result = flow_workflow::run(&definition, input, &dispatcher, &cancel).await;
    signal.abort();
    dispatcher.shutdown().await;
    let run = result?;
    serde_json::to_writer(std::io::stdout().lock(), &run)?;
    println!();
    Ok(run.status)
}

fn read_json(path: PathBuf) -> Result<Value, Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    std::fs::File::open(&path)?
        .take(INLINE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > INLINE_BYTES {
        return Err(format!(
            "{} exceeds the {INLINE_BYTES}-byte inline limit",
            path.display()
        )
        .into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
