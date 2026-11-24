use std::{future, io, time::Duration};

use flow_dispatcher::{
    execution_protocol::STARTUP_TIMEOUT_MS, Dispatcher, Job, JobError, MAX_STREAMED_JOB_BYTES,
};
use flow_domain::script::{ScriptTask, SCRIPT_V1_HANDLER};
use flow_facts::{codec::bounded_json, Result};
use serde_json::Value;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{ExecutionError, NodeExecutor};

impl NodeExecutor for Dispatcher {
    async fn execute(
        &self,
        task: ScriptTask,
        cancel: &CancellationToken,
        accept: impl FnOnce(std::result::Result<Value, String>) -> Result<()> + Send,
    ) -> std::result::Result<(), ExecutionError> {
        let deadline =
            Instant::now() + Duration::from_millis(STARTUP_TIMEOUT_MS * 3) + task.timeout();
        let job = Job {
            id: uuid::Uuid::now_v7().to_string(),
            handler: SCRIPT_V1_HANDLER.into(),
            input: bounded_json(&task, MAX_STREAMED_JOB_BYTES)
                .map_err(|error| ExecutionError::Unavailable(error.to_string()))?,
        };
        Dispatcher::execute(self, job, deadline, cancel, |completion| {
            let accepted = (|| {
                let outcome = match completion.outcome {
                    Ok(bytes) => {
                        Ok(serde_json::from_slice(&bytes).map_err(flow_facts::Error::from)?)
                    }
                    Err(error) => Err(error),
                };
                accept(outcome)
            })();
            future::ready(accepted.map_err(io::Error::other))
        })
        .await
        .map(|_| ())
        .map_err(|error| match error {
            JobError::Cancelled => ExecutionError::Cancelled,
            error => ExecutionError::Unavailable(error.to_string()),
        })
    }
}
