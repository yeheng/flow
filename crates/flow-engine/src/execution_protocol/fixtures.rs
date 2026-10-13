//! 协议 fixtures（I01 验收：合法/损坏帧/异身份/重复消息样本）。
//!
//! 供 flow-engine 单元测试与 flow-executor / flow-backend 的跨 crate 回归
//! 复用；这些样本是冻结契约的一部分，修改等于变更协议。

use flow_journal::value::ValueCodec;
use flow_journal::{StoredValue, ValueRef};
use serde_json::json;

use super::contract::PROTOCOL_VERSION;
use super::frame::FRAME_LENGTH_BYTES;
use super::message::{
    AuditRecord, ExecuteTask, Message, ObservationLine, ResultOutcome, TaskValue, WaitRequest,
};

pub fn value_ref() -> ValueRef {
    ValueRef {
        journal_id: "journal-fixture".into(),
        output_id: "out-1".into(),
        codec: ValueCodec::Json,
        version: 1,
        chunk_count: 2,
        total_bytes: 512,
        digest: "aa".repeat(32),
    }
}

pub fn audit_record() -> AuditRecord {
    AuditRecord {
        audit_seq: 41,
        kind: "business_audit".into(),
        payload: json!({"message": "example"}),
    }
}

/// 全消息目录的合法样本（每类至少一条）。
pub fn valid_messages() -> Vec<Message> {
    vec![
        Message::Hello {
            version: PROTOCOL_VERSION,
            boot_id: "boot-1".into(),
            build: "0.1.0-test".into(),
            capabilities: vec![
                "execute".into(),
                "js".into(),
                "http".into(),
                "transfer".into(),
            ],
            session_token: None,
        },
        Message::Welcome {
            version: PROTOCOL_VERSION,
            journal_id: "journal-fixture".into(),
            master_epoch: 3,
            session_id: "session-1".into(),
            executor_id: "exec-1".into(),
            window_bytes: 2 * 1024 * 1024,
            max_record_bytes: 512 * 1024,
            heartbeat_ms: 2000,
            drain_grace_ms: 5000,
        },
        Message::Ready,
        Message::Execute {
            command_id: "cmd-1".into(),
            dispatch_id: "dispatch-09".into(),
            task: ExecuteTask {
                run_id: "run-1".into(),
                node_id: "n1".into(),
                node_execution_id: "ne-1".into(),
                attempt: 2,
                node: json!({"id":"n1","type":"script","params":{"code":"return 1;"}}),
                input: TaskValue::Local,
                predecessors: vec![("n0".into(), TaskValue::Ref(value_ref()))],
                prepared: None,
                previous_outcome: None,
                observability: true,
            },
        },
        Message::Accepted {
            command_id: "cmd-1".into(),
            dispatch_id: "dispatch-09".into(),
        },
        Message::AuditBatch {
            dispatch_id: "dispatch-09".into(),
            first_seq: 41,
            records: vec![audit_record()],
        },
        Message::AuditAck {
            dispatch_id: "dispatch-09".into(),
            durable_audit_seq: 41,
            durable_bytes: 128,
            commit_lsn: 9001,
        },
        Message::RequestOperation {
            dispatch_id: "dispatch-09".into(),
            command_id: "cmd-2".into(),
            operation_id: "op-1".into(),
            request_fingerprint: "ff".repeat(32),
            transfer_id: "op:op-1".into(),
        },
        Message::OperationPermit {
            dispatch_id: "dispatch-09".into(),
            operation_id: "op-1".into(),
            permit_id: "permit-1".into(),
            request_fingerprint: "ff".repeat(32),
            credential: Some("token".into()),
        },
        Message::TransferChunk {
            dispatch_id: "dispatch-09".into(),
            transfer_id: "in-1".into(),
            offset: 0,
            bytes: b"hello".to_vec(),
            digest: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into(),
        },
        Message::InputReady {
            dispatch_id: "dispatch-09".into(),
            transfer_id: "in-1".into(),
            total_bytes: 5,
            digest: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into(),
        },
        Message::Result {
            dispatch_id: "dispatch-09".into(),
            result_id: "res-1".into(),
            last_audit_seq: 43,
            outcome: ResultOutcome::Success {
                output: StoredValue::Inline(json!({"ok": true})),
                branch: None,
            },
        },
        Message::ResultCommitted {
            dispatch_id: "dispatch-09".into(),
            result_id: "res-1".into(),
        },
        Message::Cancel {
            command_id: "cmd-3".into(),
            dispatch_id: "dispatch-09".into(),
        },
        Message::Stopped {
            dispatch_id: "dispatch-09".into(),
            sealed: false,
            last_audit_seq: 43,
            pending_operation: Some("op-1".into()),
        },
        Message::Heartbeat {
            dispatch_id: Some("dispatch-09".into()),
            phase: "executing".into(),
        },
        Message::ObservabilityBatch {
            dispatch_id: Some("dispatch-09".into()),
            lines: vec![ObservationLine {
                level: "info".into(),
                stream: "stdout".into(),
                message: "hello".into(),
            }],
        },
        Message::Reject {
            reason: "fixture".into(),
        },
    ]
}

/// 等待类结果样本（delay / signal / 失败）。
pub fn wait_result() -> Message {
    Message::Result {
        dispatch_id: "dispatch-10".into(),
        result_id: "res-2".into(),
        last_audit_seq: 2,
        outcome: ResultOutcome::Wait {
            wait: WaitRequest {
                kind: "delay".into(),
                wake_at: Some(1790000000000),
                output: Some(StoredValue::Inline(json!({"slept_ms": 100}))),
                workflow_id: None,
                input_mapping: None,
            },
        },
    }
}

pub fn failure_result() -> Message {
    Message::Result {
        dispatch_id: "dispatch-11".into(),
        result_id: "res-3".into(),
        last_audit_seq: 1,
        outcome: ResultOutcome::Failure {
            error: "prepare failed".into(),
            uncertain_operation: false,
        },
    }
}

/// 必须被明确拒绝的损坏帧（v5 对抗样本，字节级）：
/// 变体索引越界、截断的 varint、巨型长度声明、帧体尾随垃圾。
/// 巨型长度声明必须报错而非分配（postcard 先做边界检查，见 message.rs
/// 对应单元测试）。
pub fn corrupt_frames() -> Vec<Vec<u8>> {
    let frame_of = |body: &[u8]| -> Vec<u8> {
        let mut frame = Vec::with_capacity(FRAME_LENGTH_BYTES + body.len());
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(body);
        frame
    };
    vec![
        // 变体索引越界（目录 28 个变体，索引 0..=27）。
        frame_of(&[28]),
        frame_of(&[200]),
        // 截断的 varint（0x80 起始的连续字节，帧体在此结束）。
        frame_of(&[0x80, 0x80, 0x80]),
        // 巨型长度声明：Hello 变体 + version + boot_id 长度声明 ≈ 2^62。
        {
            let mut body = vec![0u8, 1, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x3F];
            body.extend_from_slice(&[0x01]);
            frame_of(&body)
        },
        // 帧体尾随垃圾：合法 Ready 编码 + 2 字节走私。
        {
            let body = postcard::to_allocvec(&Message::Ready).unwrap();
            let mut body = body;
            body.extend_from_slice(&[0xDE, 0xAD]);
            frame_of(&body)
        },
    ]
}

/// 同 dispatch 同序号异内容样本（必须拒绝）。
pub fn conflicting_audit_records() -> (AuditRecord, AuditRecord) {
    let a = AuditRecord {
        audit_seq: 7,
        kind: "business_audit".into(),
        payload: json!({"value": "a"}),
    };
    let b = AuditRecord {
        audit_seq: 7,
        kind: "business_audit".into(),
        payload: json!({"value": "different"}),
    };
    (a, b)
}
