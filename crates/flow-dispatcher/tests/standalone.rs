use std::{io, time::Duration};

use flow_dispatcher::{
    execution_protocol::ExecutorInvocation, Dispatcher, IpcOptions, Job, JobError,
};
use tokio::{sync::Barrier, time::Instant};
use tokio_util::sync::CancellationToken;

fn dispatcher(slots: usize) -> Dispatcher {
    Dispatcher::new(options(slots), "echo.v1")
}

fn options(slots: usize) -> IpcOptions {
    IpcOptions {
        executor: ExecutorInvocation::explicit(env!("CARGO_BIN_EXE_flow-worker")),
        x_max: slots,
        tag: None,
    }
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

#[tokio::test]
async fn streaming_worker_round_trips_empty_and_multichunk_values() {
    let dispatcher = Dispatcher::new_streaming(options(1), "echo.v1");
    let cancel = CancellationToken::new();
    let input: Vec<_> = (0..2 * flow_dispatcher::execution_protocol::TRANSFER_CHUNK_BYTES + 1)
        .map(|index| (index % 251) as u8)
        .collect();
    let mut worker = None;
    for bytes in [input.as_slice(), &[]] {
        let mut task = job("streamed");
        task.input = bytes.to_vec();
        let result = dispatcher
            .execute(task, deadline(), &cancel, |completion| async move {
                assert_eq!(completion.outcome.as_ref().unwrap().as_slice(), bytes);
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(result.outcome.unwrap().as_slice(), bytes);
        if let Some(first) = worker {
            assert_eq!(result.worker_pid, Some(first));
        }
        worker = result.worker_pid;
    }
    dispatcher.shutdown().await;
}

#[cfg(unix)]
#[tokio::test]
async fn aborting_the_caller_reaps_the_old_worker_before_the_next_commit() {
    use std::sync::Arc;

    let dispatcher = Arc::new(dispatcher(1));
    let (started, waiting) = tokio::sync::oneshot::channel();
    let dispatch = dispatcher.clone();
    let task = tokio::spawn(async move {
        dispatch
            .execute(
                job("abort"),
                deadline(),
                &CancellationToken::new(),
                |done| async move {
                    started.send(done.worker_pid.unwrap()).unwrap();
                    std::future::pending().await
                },
            )
            .await
    });
    let retired = tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));

    let result = dispatcher
        .execute(
            job("replacement"),
            deadline(),
            &CancellationToken::new(),
            |_| async {
                assert_eq!(unsafe { libc::kill(retired as libc::pid_t, 0) }, -1);
                Ok(())
            },
        )
        .await
        .unwrap();
    assert_eq!(result.outcome.unwrap(), b"replacement");
    dispatcher.shutdown().await;
}
