//! AuditStream 窗口与重传单元测试（I05 验收出口：满窗口可取消、ACK 只随
//! 连续持久前缀前进、窗口释放后继续发送）。

use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};

use flow_engine::execution_protocol::message::Message;

use flow_executor::audit::{AuditError, AuditStream};

fn stream(
    window: u64,
) -> (
    AuditStream,
    mpsc::Receiver<Message>,
    watch::Sender<(u64, u64)>,
) {
    let (outbound, rx) = mpsc::channel(64);
    let (acks, _) = watch::channel((0u64, 0u64));
    (
        AuditStream::new("dispatch-test".into(), outbound, window, acks.subscribe()),
        rx,
        acks,
    )
}

fn payload() -> serde_json::Value {
    serde_json::json!({"padding": "x".repeat(512)})
}

#[tokio::test]
async fn window_blocks_until_prefix_acked_then_resumes() {
    // 窗口只容一条记录：第二条必须等第一条的 ACK（B(S)-B(D) ≤ W）。
    let unit = flow_engine::execution_protocol::record_bytes(1, "business_audit", &payload());
    let (mut audit, mut rx, acks) = stream(unit + 16);
    assert_eq!(audit.push("business_audit", payload()).unwrap(), 1);
    assert!(audit.push("business_audit", payload()).is_err());
    audit.flush().await.unwrap();
    let first = rx.recv().await.unwrap();
    match &first {
        Message::AuditBatch {
            records, first_seq, ..
        } => {
            assert_eq!(*first_seq, 1);
            assert_eq!(records.len(), 1);
        }
        other => panic!("expected batch, got {}", other.type_name()),
    }
    // 满窗口：不发送 seq2。
    audit.flush().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), rx.recv())
            .await
            .is_err(),
        "window must block the second record"
    );
    // 把流移入等待任务：ACK 前缀 1 释放窗口 → seq2 发出 → ACK 2 完成。
    let cancel = tokio_util::sync::CancellationToken::new();
    let (done_tx, done_rx) = oneshot::channel();
    let waiter = tokio::spawn(async move {
        assert_eq!(
            audit.push_wait("business_audit", payload()).await.unwrap(),
            2
        );
        let _ = done_tx.send(audit.await_durable(2, &cancel).await);
    });
    acks.send((1, unit)).unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("second record flows after ack")
        .unwrap();
    match &second {
        Message::AuditBatch {
            records, first_seq, ..
        } => {
            assert_eq!(*first_seq, 2);
            assert_eq!(records.len(), 1);
        }
        other => panic!("expected batch, got {}", other.type_name()),
    }
    acks.send((2, unit * 2)).unwrap();
    waiter.await.unwrap();
    done_rx.await.unwrap().expect("durable through seq 2");
}

#[tokio::test]
async fn oversize_record_rejected_at_push() {
    // 单条记录必须能被窗口容纳（二期 §3.5 死锁条款）。
    let (mut audit, _rx, _acks) = stream(1024);
    let error = audit
        .push(
            "business_audit",
            serde_json::json!({"padding": "x".repeat(4096)}),
        )
        .unwrap_err();
    assert!(matches!(error, AuditError::Oversize(_, _)));
}

#[tokio::test]
async fn await_durable_is_cancellable() {
    // 满窗口/等待 ACK 必须可被取消唤醒（二期 §4.3）。
    let unit = flow_engine::execution_protocol::record_bytes(1, "business_audit", &payload());
    let (mut audit, _rx, _acks) = stream(unit + 16);
    audit.push("business_audit", payload()).unwrap();
    audit.flush().await.unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    let error = audit.await_durable(1, &cancel).await.unwrap_err();
    assert!(matches!(error, AuditError::Cancelled));
}

#[tokio::test]
async fn only_remote_uplink_replays_unacknowledged_batches() {
    let (mut local, mut local_rx, _local_acks) = stream(flow_engine::execution_protocol::W_BYTES);
    let (mut remote, mut remote_rx, _remote_acks) =
        stream(flow_engine::execution_protocol::W_BYTES);
    let local_cancel = tokio_util::sync::CancellationToken::new();
    let remote_cancel = tokio_util::sync::CancellationToken::new();
    local.configure(false, local_cancel.clone());
    remote.configure(true, remote_cancel.clone());
    local.push("business_audit", payload()).unwrap();
    remote.push("business_audit", payload()).unwrap();
    let local_task = tokio::spawn(async move { local.await_published(1).await });
    let remote_task = tokio::spawn(async move { remote.await_published(1).await });
    local_rx.recv().await.unwrap();
    remote_rx.recv().await.unwrap();
    let replay = tokio::time::timeout(Duration::from_secs(4), remote_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(replay, Message::AuditBatch { first_seq: 1, .. }));
    assert!(
        local_rx.try_recv().is_err(),
        "reliable local IPC must send once"
    );
    local_cancel.cancel();
    remote_cancel.cancel();
    assert!(matches!(
        local_task.await.unwrap(),
        Err(AuditError::Cancelled)
    ));
    assert!(matches!(
        remote_task.await.unwrap(),
        Err(AuditError::Cancelled)
    ));
}

#[tokio::test]
async fn large_output_production_stays_inside_window_and_frame_limit() {
    use flow_engine::execution_protocol::{
        contract::{DATA_MAX_FRAME, W_BYTES},
        frame::encode_tagged_frame,
    };
    let (mut audit, mut rx, acks) = stream(W_BYTES);
    let producer = tokio::spawn(async move {
        flow_executor::transfer::store_bytes_ref(
            &mut audit,
            "j",
            &vec![b'x'; 6 * 1024 * 1024],
            flow_journal::value::ValueCodec::Bytes,
        )
        .await
        .unwrap();
        assert!(audit.queued_bytes() <= W_BYTES);
    });
    let consumer = tokio::spawn(async move {
        let mut bytes = 0;
        let mut records_seen = 0;
        while let Some(message) = rx.recv().await {
            assert!(encode_tagged_frame(&message).unwrap().len() <= DATA_MAX_FRAME + 4);
            if let Message::AuditBatch { records, .. } = message {
                let last = records.last().unwrap().audit_seq;
                records_seen += records.len();
                bytes += records.iter().map(|r| r.encoded_bytes()).sum::<u64>();
                let _ = acks.send((last, bytes));
            }
        }
        assert_eq!(records_seen, 25);
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        producer.await.unwrap();
        consumer.await.unwrap();
    })
    .await
    .unwrap();
}
