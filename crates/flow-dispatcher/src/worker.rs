//! IPC host for a single bounded, pure task handler.

use std::{sync::Arc, time::Duration};

use crate::execution_protocol::{
    connect_loopback,
    transport::{executor_handshake, FrameTransport},
    Message, ProtocolError, CONNECT_ADDR_ENV, SESSION_TOKEN_ENV, STARTUP_TIMEOUT_MS,
};
use crate::job_transfer::{
    frames, Transfer, JOB_INPUT, JOB_OUTPUT, MAX_STREAMED_JOB_BYTES, STREAM_CAPABILITY,
};
use crate::jobs::{capability, valid_job_identity, MAX_JOB_BYTES};

/// Connect to the inherited IPC endpoint and run a versioned pure handler.
/// Computation runs on a blocking worker while the IPC loop handles heartbeats
/// and cancellation. The dispatcher retires a cancelled process.
pub async fn run_worker(
    handler: &str,
    execute: impl Fn(Vec<u8>) -> Result<Vec<u8>, String> + Send + Sync + 'static,
) -> Result<(), ProtocolError> {
    let session_token = std::env::var(SESSION_TOKEN_ENV)
        .ok()
        .filter(|s| !s.is_empty());
    let mut transport = match std::env::var(CONNECT_ADDR_ENV)
        .ok()
        .filter(|s| !s.is_empty())
    {
        Some(addr) => {
            let stream = tokio::time::timeout(
                Duration::from_millis(STARTUP_TIMEOUT_MS),
                connect_loopback(&addr),
            )
            .await
            .map_err(|_| ProtocolError::Malformed("worker IPC connect timed out".into()))??;
            FrameTransport::spawn(stream, 32)
        }
        None => inherited_transport()?,
    };
    let result = serve(&mut transport, session_token, handler, execute).await;
    transport.close();
    transport.join().await;
    result
}

#[cfg(unix)]
fn inherited_transport() -> Result<FrameTransport, ProtocolError> {
    let args: Vec<_> = std::env::args().collect();
    let fd = args
        .windows(2)
        .find(|w| w[0] == "--channel-fd")
        .and_then(|w| w[1].parse().ok());
    match fd {
        Some(fd) => Ok(FrameTransport::spawn_local(
            crate::execution_protocol::channel_from_raw(fd)?,
            32,
        )),
        None => Err(ProtocolError::InvalidPayload(
            "missing worker IPC endpoint".into(),
        )),
    }
}

#[cfg(not(unix))]
fn inherited_transport() -> Result<FrameTransport, ProtocolError> {
    Err(ProtocolError::InvalidPayload(
        "missing worker IPC endpoint".into(),
    ))
}

type Outcome = Result<Vec<u8>, String>;

enum Active {
    Receiving(String, Transfer),
    Running(String, bool, tokio::task::JoinHandle<Outcome>),
    AwaitingCommit(String, String),
}

impl Active {
    fn dispatch(&self) -> &str {
        match self {
            Self::Receiving(id, _) | Self::Running(id, ..) | Self::AwaitingCommit(id, _) => id,
        }
    }

    fn phase(&self) -> &'static str {
        match self {
            Self::Receiving(..) => "receiving",
            Self::Running(..) => "running",
            Self::AwaitingCommit(..) => "awaiting_commit",
        }
    }
}

async fn serve(
    transport: &mut FrameTransport,
    session_token: Option<String>,
    handler: &str,
    execute: impl Fn(Vec<u8>) -> Outcome + Send + Sync + 'static,
) -> Result<(), ProtocolError> {
    let (_, limits) = executor_handshake(
        transport,
        "flow-worker",
        &[&capability(handler), STREAM_CAPABILITY],
        session_token,
    )
    .await?;
    let execute = Arc::new(execute);
    let mut heartbeat = tokio::time::interval(Duration::from_millis(limits.heartbeat_ms));
    let mut active: Option<Active> = None;
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                transport.send(Message::Heartbeat {
                    dispatch_id: active.as_ref().map(|job| job.dispatch().into()),
                    phase: active.as_ref().map_or("idle", Active::phase).into(),
                }).await?;
            }
            result = async {
                match &mut active {
                    Some(Active::Running(_, _, task)) => task.await,
                    _ => std::future::pending().await,
                }
            } => {
                let Some(Active::Running(dispatch_id, streaming, _)) = active.take() else { unreachable!() };
                let result_id = uuid::Uuid::now_v7().to_string();
                let limit = if streaming { MAX_STREAMED_JOB_BYTES } else { MAX_JOB_BYTES };
                let outcome = match result.map_err(|_| ProtocolError::Malformed("job handler stopped".into()))? {
                    Ok(output) if output.len() <= limit && streaming => {
                        for message in frames(&dispatch_id, JOB_OUTPUT, &output) {
                            transport.send(message).await?;
                        }
                        Ok(Vec::new())
                    }
                    Ok(output) if output.len() <= limit => Ok(output),
                    Err(error) if error.len() <= MAX_JOB_BYTES => Err(error),
                    _ => Err("worker result exceeds byte budget".into()),
                };
                transport.send(Message::JobFinished {
                    dispatch_id: dispatch_id.clone(), result_id: result_id.clone(), outcome,
                }).await?;
                active = Some(Active::AwaitingCommit(dispatch_id, result_id));
            }
            message = transport.recv() => match message {
                Ok(Message::ExecuteJob { command_id, dispatch_id, job }) if active.is_none() => {
                    if !job.valid(handler) {
                        return Err(ProtocolError::InvalidPayload("invalid inline job".into()));
                    }
                    transport.send(Message::Accepted { command_id, dispatch_id: dispatch_id.clone() }).await?;
                    let execute = execute.clone();
                    active = Some(Active::Running(dispatch_id, false, tokio::task::spawn_blocking(move || execute(job.input))));
                }
                Ok(Message::ExecuteJobStream { command_id, dispatch_id, job_id, handler: job_handler }) if active.is_none() => {
                    if !valid_job_identity(&job_id, &job_handler, handler) {
                        return Err(ProtocolError::InvalidPayload("invalid streamed job".into()));
                    }
                    transport.send(Message::Accepted { command_id, dispatch_id: dispatch_id.clone() }).await?;
                    active = Some(Active::Receiving(dispatch_id, Transfer::default()));
                }
                Ok(message @ (Message::TransferChunk { .. } | Message::InputReady { .. })) => {
                    let Some(Active::Receiving(id, transfer)) = &mut active else {
                        return Err(ProtocolError::InvalidPayload("no job awaiting its input".into()));
                    };
                    if message.dispatch_id() != Some(id.as_str()) {
                        return Err(ProtocolError::InvalidPayload("job transfer identity mismatch".into()));
                    }
                    transfer.receive(JOB_INPUT, message)?;
                    if transfer.is_ready() {
                        let input = std::mem::take(transfer).finish().expect("transfer is ready");
                        let execute = execute.clone();
                        active = Some(Active::Running(id.clone(), true, tokio::task::spawn_blocking(move || execute(input))));
                    }
                }
                Ok(Message::ResultCommitted { dispatch_id, result_id }) => {
                    if matches!(&active, Some(Active::AwaitingCommit(id, result)) if id == &dispatch_id && result == &result_id) {
                        active = None;
                        transport.send(Message::Ready).await?;
                    }
                }
                Ok(Message::Cancel { dispatch_id, .. }) => {
                    if active.as_ref().is_some_and(|job| job.dispatch() == dispatch_id) {
                        let sealed = !matches!(active, Some(Active::Running(..)));
                        transport.send(Message::Stopped {
                            dispatch_id, sealed, last_audit_seq: 0, pending_operation: None,
                        }).await?;
                        return Ok(());
                    }
                }
                Ok(Message::Heartbeat { .. }) => {},
                Err(ProtocolError::Closed) => return Ok(()),
                Err(error) => return Err(error),
                Ok(other) => return Err(ProtocolError::Malformed(format!("unexpected command {}", other.type_name()))),
            }
        }
    }
}
