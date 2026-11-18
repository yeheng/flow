use std::{io, time::Duration};

use flow_dispatcher::{
    execution_protocol::ExecutorInvocation, Dispatcher, IpcOptions, Job, JobError,
};
use tokio::{sync::Barrier, time::Instant};
use tokio_util::sync::CancellationToken;

fn dispatcher(slots: usize) -> Dispatcher {
    Dispatcher::new(
        IpcOptions {
            executor: ExecutorInvocation::explicit(env!("CARGO_BIN_EXE_flow-worker")),
            x_max: slots,
            tag: None,
        },
        "echo.v1",
    )
}

fn job(text: &str) -> Job {
    Job {
        id: text.into(),
        handler: "echo.v1".into(),
        input: text.as_bytes().into(),
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

#[tokio::test]
async fn independent_workers_commit_and_reuse_their_slots() {
    let dispatcher = dispatcher(2);
    let cancel = CancellationToken::new();
    let barrier = Barrier::new(2);
    // Neither completion may release its lease until both workers have replied.
    let (first, second) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(
            dispatcher.execute(job("first"), deadline(), &cancel, |_| async {
                barrier.wait().await;
                Ok(())
            }),
            dispatcher.execute(job("second"), deadline(), &cancel, |_| async {
                barrier.wait().await;
                Ok(())
            }),
        )
    })
    .await
    .unwrap();
    let first = first.unwrap();
    let second = second.unwrap();
    assert_eq!(first.outcome.unwrap(), b"first");
    assert_eq!(second.outcome.unwrap(), b"second");
    assert_ne!(first.worker_pid, second.worker_pid);
    let third = dispatcher
        .execute(job("third"), deadline(), &cancel, |_| async { Ok(()) })
        .await
        .unwrap();
    assert!([first.worker_pid, second.worker_pid].contains(&third.worker_pid));
    dispatcher.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn losing_a_worker_after_result_does_not_undo_commit_or_block_the_other_worker() {
    let dispatcher = dispatcher(2);
    let cancel = CancellationToken::new();
    let barrier = Barrier::new(2);
    let barrier = &barrier;
    let (lost, survived) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(
            dispatcher.execute(job("kill"), deadline(), &cancel, |result| async move {
                barrier.wait().await;
                let pid = result.worker_pid.unwrap() as libc::pid_t;
                assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
                // The caller's commit succeeds even if the worker disappears before ACK.
                Ok(())
            }),
            dispatcher.execute(job("survive"), deadline(), &cancel, |_| async {
                barrier.wait().await;
                Ok(())
            }),
        )
    })
    .await
    .unwrap();
    let lost = lost.unwrap();
    assert_eq!(lost.outcome.unwrap(), b"kill");
    assert_eq!(survived.unwrap().outcome.unwrap(), b"survive");
    let continued = dispatcher
        .execute(job("continue"), deadline(), &cancel, |_| async { Ok(()) })
        .await
        .unwrap();
    assert_ne!(continued.worker_pid, lost.worker_pid);
    dispatcher.shutdown().await;
}

#[tokio::test]
async fn a_failed_commit_retires_the_session_and_cancellation_does_not_commit() {
    let dispatcher = dispatcher(1);
    let cancel = CancellationToken::new();
    let mut failed_pid = None;
    let result = dispatcher
        .execute(job("recoverable"), deadline(), &cancel, |completion| {
            failed_pid = completion.worker_pid;
            async { Err(io::Error::other("injected commit failure")) }
        })
        .await;
    assert!(matches!(result, Err(JobError::Commit(_))));
    let result = dispatcher
        .execute(job("recoverable"), deadline(), &cancel, |_| async {
            Ok(())
        })
        .await
        .unwrap();
    assert_ne!(result.worker_pid, failed_pid);
    cancel.cancel();
    let result = dispatcher
        .execute(job("cancel"), deadline(), &cancel, |_| async {
            panic!("cancelled job committed")
        })
        .await;
    assert!(matches!(result, Err(JobError::Cancelled)));
    dispatcher.shutdown().await;
}
