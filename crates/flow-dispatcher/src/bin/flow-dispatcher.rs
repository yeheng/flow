//! Standalone entry point for the IPC task dispatcher.

use std::{io::Write, path::PathBuf, sync::Arc, time::Duration};

use flow_dispatcher::{
    execution_protocol::ExecutorInvocation, Dispatcher, IpcOptions, Job, JobCompletion,
};
use tokio::{task::JoinSet, time::Instant};
use tokio_util::sync::CancellationToken;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let usage = "usage: flow-dispatcher [--handler HANDLER] WORKER RECEIPT_DIR INPUT [INPUT ...]";
    let first = args.next().ok_or(usage)?;
    let (worker, handler, streaming) = if first == "--handler" {
        let handler = args.next().ok_or(usage)?;
        (args.next().ok_or(usage)?, handler, true)
    } else {
        (first, "echo.v1".to_owned(), false)
    };
    let receipts = PathBuf::from(args.next().ok_or(usage)?);
    let inputs: Vec<_> = args.collect();
    if inputs.is_empty() || !receipts.is_dir() {
        return Err(usage.into());
    }
    let options = IpcOptions {
        executor: ExecutorInvocation::explicit(worker),
        x_max: 2,
        tag: None,
    };
    let dispatcher = Arc::new(if streaming {
        Dispatcher::new_streaming(options, &handler)
    } else {
        Dispatcher::new(options, &handler)
    });
    let mut tasks = JoinSet::new();
    for input in inputs {
        let dispatcher = dispatcher.clone();
        let receipts = receipts.clone();
        let handler = handler.clone();
        tasks.spawn(async move {
            let job = Job {
                id: uuid::Uuid::now_v7().to_string(),
                handler,
                input: input.into_bytes(),
            };
            dispatcher
                .execute(
                    job,
                    Instant::now() + Duration::from_secs(30),
                    &CancellationToken::new(),
                    |result| async move {
                        tokio::task::spawn_blocking(move || persist(receipts, result))
                            .await
                            .map_err(std::io::Error::other)?
                    },
                )
                .await
        });
    }
    let mut failure = None;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(result)) => match result.outcome {
                Ok(bytes) => println!("{} {}", result.job_id, String::from_utf8_lossy(&bytes)),
                Err(error) => failure = Some(error),
            },
            Ok(Err(error)) => failure = Some(error.to_string()),
            Err(error) => failure = Some(error.to_string()),
        }
    }
    dispatcher.shutdown().await;
    failure.map_or(Ok(()), |error| Err(error.into()))
}

fn persist(dir: PathBuf, result: JobCompletion) -> std::io::Result<()> {
    let mut temp = tempfile::NamedTempFile::new_in(&dir)?;
    serde_json::to_writer(
        &mut temp,
        &serde_json::json!({
            "job_id": result.job_id, "dispatch_id": result.dispatch_id,
            "result_id": result.result_id, "outcome": result.outcome,
        }),
    )?;
    temp.flush()?;
    temp.as_file().sync_all()?;
    temp.persist_noclobber(dir.join(format!("{}.json", result.job_id)))?;
    #[cfg(unix)]
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}
