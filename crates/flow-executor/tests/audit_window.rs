//! AuditStream 窗口与重传单元测试（I05 验收出口：满窗口可取消、ACK 只随
//! 连续持久前缀前进、窗口释放后继续发送）。

use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};

use flow_engine::execution_protocol::message::Message;

use flow_executor::audit::{AuditError, AuditStream};

fn stream(window: u64) -> (AuditStream, mpsc::Receiver<Message>, watch::Sender<(u64, u64)>) {
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
    assert_eq!(
        audit.push("business_audit", payload()).unwrap(),
        1
    );
    assert_eq!(
        audit.push("business_audit", payload()).unwrap(),
        2
    );
    audit.flush().await.unwrap();
    let first = rx.recv().await.unwrap();
    match &first {
        Message::AuditBatch { records, first_seq, .. } => {
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
        let _ = done_tx.send(audit.await_durable(2, &cancel).await);
    });
    acks.send((1, unit)).unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("second record flows after ack")
        .unwrap();
    match &second {
        Message::AuditBatch { records, first_seq, .. } => {
            assert_eq!(*first_seq, 2);
            assert_eq!(records.len(), 1);
        }
        other => panic!("expected batch, got {}", other.type_name()),
    }
    acks.send((2, unit * 2)).unwrap();
    waiter.await.unwrap();
    done_rx
        .await
        .unwrap()
        .expect("durable through seq 2");
}

#[tokio::test]
async fn oversize_record_rejected_at_push() {
    // 单条记录必须能被窗口容纳（二期 §3.5 死锁条款）。
    let (mut audit, _rx, _acks) = stream(1024);
    let error = audit
        .push("business_audit", serde_json::json!({"padding": "x".repeat(4096)}))
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
