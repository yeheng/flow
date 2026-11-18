use std::{future::Future, io, sync::Arc};

use serde::{Deserialize, Serialize};
use tokio::time::{timeout, timeout_at, Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    execution_protocol::Message, send_committed, send_execute, ExecutorPool, FromSession,
    IpcOptions,
};

pub use crate::job_transfer::MAX_STREAMED_JOB_BYTES;
use crate::job_transfer::{frames, Transfer, JOB_INPUT, JOB_OUTPUT, STREAM_CAPABILITY};

/// Inline limit for sessions that only advertise the original job protocol.
pub const MAX_JOB_BYTES: usize = 64 * 1024;
pub(crate) fn capability(handler: &str) -> String {
    format!("job:{handler}")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub handler: String,
    pub input: Vec<u8>,
}

impl Job {
    pub(crate) fn valid(&self, handler: &str) -> bool {
        valid_job_identity(&self.id, &self.handler, handler) && self.input.len() <= MAX_JOB_BYTES
    }
}

pub(crate) fn valid_job_identity(id: &str, actual: &str, expected: &str) -> bool {
    !id.is_empty()
        && id.len() <= 256
        && !actual.is_empty()
        && actual.len() <= 128
        && actual == expected
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobCompletion {
    pub job_id: String,
    pub dispatch_id: String,
    pub result_id: String,
    pub worker_pid: Option<u32>,
    pub outcome: Result<Vec<u8>, String>,
}

#[derive(Debug, thiserror::Error)]
pub enum JobError {
    #[error(transparent)]
    Pool(#[from] crate::pool::PoolError),
    #[error("{0}")]
    Protocol(String),
    #[error("{0}")]
    Lost(String),
    #[error("task deadline expired")]
    Deadline,
    #[error("task cancelled before completion")]
    Cancelled,
    #[error("result commit failed: {0}")]
    Commit(#[source] io::Error),
}

/// One dispatcher shares a bounded pool of worker processes among callers.
/// Submission and durable result storage are owned by the caller. No implicit
/// retry is made on a lost worker, failed commit, or cancellation.
pub struct Dispatcher {
    pool: Arc<ExecutorPool>,
    handler: String,
    streaming: bool,
}

impl Dispatcher {
    /// Open a pool whose workers advertise the required handler and version.
    pub fn new(options: IpcOptions, handler: impl Into<String>) -> Self {
        Self::with_streaming(options, handler.into(), false)
    }

    /// Use a worker that advertises chunked task inputs and outputs.
    pub fn new_streaming(options: IpcOptions, handler: impl Into<String>) -> Self {
        Self::with_streaming(options, handler.into(), true)
    }

    fn with_streaming(options: IpcOptions, handler: String, streaming: bool) -> Self {
        let mut caps = vec![capability(&handler)];
        if streaming {
            caps.push(STREAM_CAPABILITY.into());
        }
        Self {
            pool: ExecutorPool::with_capabilities(
                options,
                format!("dispatcher-{}", uuid::Uuid::now_v7()),
                1,
                caps,
            ),
            handler,
            streaming,
        }
    }

    pub async fn shutdown(&self) {
        self.pool.shutdown().await;
    }

    /// Run a job through acceptance and result. `commit` must only return Ok
    /// after durable persistence; failure retires the session without an ACK.
    /// After commit succeeds the result wins even if confirmation is lost.
    pub async fn execute<C, F>(
        &self,
        job: Job,
        deadline: Instant,
        cancel: &CancellationToken,
        commit: C,
    ) -> Result<JobCompletion, JobError>
    where
        C: FnOnce(JobCompletion) -> F,
        F: Future<Output = io::Result<()>>,
    {
        let size_limit = if self.streaming {
            MAX_STREAMED_JOB_BYTES
        } else {
            MAX_JOB_BYTES
        };
        if !valid_job_identity(&job.id, &job.handler, &self.handler) || job.input.len() > size_limit
        {
            return Err(JobError::Protocol(
                "invalid job identity, handler, or input size".into(),
            ));
        }
        let job_id = job.id.clone();
        let dispatch_id = uuid::Uuid::now_v7().to_string();
        let command_id = format!("execute-{dispatch_id}");
        let mut lease = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(JobError::Cancelled),
            result = timeout_at(deadline, self.pool.acquire()) => result.map_err(|_| JobError::Deadline)??,
        };
        let work = async {
            if self.streaming {
                send_execute(
                    &lease.to_session,
                    Message::ExecuteJobStream {
                        command_id: command_id.clone(),
                        dispatch_id: dispatch_id.clone(),
                        job_id: job.id,
                        handler: job.handler,
                    },
                )
                .await?;
                for message in frames(&dispatch_id, JOB_INPUT, &job.input) {
                    lease
                        .to_session
                        .send(crate::ToSession::Send(Box::new(message)))
                        .await
                        .map_err(|_| {
                            JobError::Lost("worker disconnected during input transfer".into())
                        })?;
                }
            } else {
                send_execute(
                    &lease.to_session,
                    Message::ExecuteJob {
                        command_id: command_id.clone(),
                        dispatch_id: dispatch_id.clone(),
                        job,
                    },
                )
                .await?;
            }
            receive_result(&mut lease.events, &dispatch_id, &command_id, self.streaming).await
        };
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(JobError::Cancelled),
            result = timeout_at(deadline, work) => result.unwrap_or(Err(JobError::Deadline)),
        };
        let (result_id, outcome) = match result {
            Ok(result) => result,
            Err(error) => {
                if matches!(error, JobError::Cancelled | JobError::Deadline) {
                    let _ = lease
                        .to_session
                        .try_send(crate::ToSession::CancelDispatch(dispatch_id));
                    // Bound drainage independently of the task deadline. The lease
                    // guard always closes and reaps even if Cancel cannot get through.
                    let _ = timeout(std::time::Duration::from_millis(100), async {
                        while let Some(event) = lease.events.recv().await {
                            match event {
                                FromSession::Incoming(message)
                                    if matches!(*message, Message::Stopped { .. }) =>
                                {
                                    break
                                }
                                FromSession::Dead(_) => break,
                                _ => {}
                            }
                        }
                    })
                    .await;
                }
                self.pool.release(lease, false).await;
                return Err(error);
            }
        };
        let completed = JobCompletion {
            job_id,
            dispatch_id,
            result_id,
            worker_pid: lease.child_pid,
            outcome,
        };
        commit(completed.clone()).await.map_err(JobError::Commit)?;
        // Wait for explicit readiness before reusing the slot. A disconnect at
        // this point affects the session, but cannot undo the committed result.
        let reusable = matches!(
            timeout(std::time::Duration::from_secs(1), async {
                send_committed(
                    &lease.to_session,
                    completed.dispatch_id.clone(),
                    completed.result_id.clone(),
                )
                .await?;
                loop {
                    match lease.events.recv().await {
                        Some(FromSession::Incoming(message))
                            if matches!(*message, Message::Ready) =>
                        {
                            return Ok(())
                        }
                        Some(FromSession::Incoming(message))
                            if matches!(*message, Message::Heartbeat { .. }) => {}
                        _ => {
                            return Err(JobError::Lost(
                                "worker did not return to ready state".into(),
                            ))
                        }
                    }
                }
            })
            .await,
            Ok(Ok(()))
        );
        self.pool.release(lease, reusable).await;
        Ok(completed)
    }
}

async fn receive_result(
    events: &mut tokio::sync::mpsc::Receiver<FromSession>,
    dispatch: &str,
    command: &str,
    streaming: bool,
) -> Result<(String, Result<Vec<u8>, String>), JobError> {
    let mut accepted = false;
    let mut output = Transfer::default();
    while let Some(event) = events.recv().await {
        let message = match event {
            FromSession::Incoming(message) => *message,
            FromSession::Dead(reason) => return Err(JobError::Lost(reason)),
        };
        if message.dispatch_id().is_some_and(|id| id != dispatch) {
            continue;
        }
        match message {
            Message::Accepted { command_id, .. } if command_id == command => accepted = true,
            message @ (Message::TransferChunk { .. } | Message::InputReady { .. })
                if streaming && accepted =>
            {
                output
                    .receive(JOB_OUTPUT, message)
                    .map_err(|error| JobError::Protocol(error.to_string()))?;
            }
            Message::JobFinished {
                result_id, outcome, ..
            } if accepted
                && !result_id.is_empty()
                && result_id.len() <= 256
                && outcome
                    .as_ref()
                    .map_or_else(|e| e.len() <= MAX_JOB_BYTES, |v| v.len() <= MAX_JOB_BYTES) =>
            {
                let outcome = match outcome {
                    Ok(value) if streaming && value.is_empty() => {
                        Ok(output.finish().ok_or_else(|| {
                            JobError::Protocol(
                                "job result missing a complete output transfer".into(),
                            )
                        })?)
                    }
                    Ok(_) if streaming => {
                        return Err(JobError::Protocol(
                            "unexpected inline streamed job result".into(),
                        ))
                    }
                    Err(_) if !output.is_empty() => {
                        return Err(JobError::Protocol(
                            "failed job sent an output transfer".into(),
                        ))
                    }
                    other => other,
                };
                return Ok((result_id, outcome));
            }
            Message::Heartbeat { .. } => {}
            other => {
                return Err(JobError::Protocol(format!(
                    "unexpected worker event {}",
                    other.type_name()
                )))
            }
        }
    }
    Err(JobError::Lost("worker event stream closed".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn stale_result_cannot_complete_a_new_dispatch() {
        let (tx, mut events) = mpsc::channel(4);
        for message in [
            Message::Accepted {
                command_id: "command".into(),
                dispatch_id: "current".into(),
            },
            Message::JobFinished {
                dispatch_id: "old".into(),
                result_id: "stale".into(),
                outcome: Ok(vec![0]),
            },
            Message::JobFinished {
                dispatch_id: "current".into(),
                result_id: "result".into(),
                outcome: Ok(vec![1]),
            },
        ] {
            tx.send(FromSession::Incoming(Box::new(message)))
                .await
                .unwrap();
        }
        let (id, output) = receive_result(&mut events, "current", "command", false)
            .await
            .unwrap();
        assert_eq!(id, "result");
        assert_eq!(output.unwrap(), vec![1]);
    }
}
