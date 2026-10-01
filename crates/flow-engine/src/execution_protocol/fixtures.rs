//! 协议 fixtures（I01 验收：合法/非法/异身份/重复消息样本）。
//!
//! 供 flow-engine 单元测试与 flow-executor / flow-backend 的跨 crate 回归
//! 复用；这些样本是冻结契约的一部分，修改等于变更协议。

use flow_journal::value::ValueCodec;
use flow_journal::{StoredValue, ValueRef};
use serde_json::{json, Value};

use super::message::{
    AuditRecord, ExecuteTask, Message, ObservationLine, ResultOutcome, WaitRequest,
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
            boot_id: "boot-1".into(),
            build: "0.1.0-test".into(),
            capabilities: vec![
                "execute".into(),
                "js".into(),
                "http".into(),
                "transfer".into(),
            ],
        },
        Message::Welcome {
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
                input: StoredValue::Inline(json!({"x": 1})),
                predecessors: vec![("n0".into(), StoredValue::Ref(value_ref()))],
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
            request: Some(StoredValue::Inline(
                json!({"method":"GET","url":"http://example.test"}),
            )),
            transfer_id: None,
        },
        Message::OperationPermit {
            dispatch_id: "dispatch-09".into(),
            operation_id: "op-1".into(),
            permit_id: "permit-1".into(),
            request_fingerprint: "ff".repeat(32),
            credential: Some("token".into()),
        },
        Message::TransferChunk {
            transfer_id: "in-1".into(),
            offset: 0,
            bytes: "aGVsbG8=".into(),
            digest: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".into(),
        },
        Message::InputReady {
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

/// 必须被明确拒绝的畸形信封。
pub fn malformed_envelopes() -> Vec<Value> {
    vec![
        json!("not an object"),
        json!({}),
        json!({"v": 2, "type": "Ready"}),  // 版本不兼容
        json!({"v": 1}),                   // 缺 type
        json!({"v": 1, "type": "Result"}), // 缺 dispatch/body
        json!({"v": 1, "type": "AuditAck", "dispatch_id": "d", "body": {"durable_audit_seq": "x", "durable_bytes": "1", "commit_lsn": "1"}}), // 非十进制
        json!({"v": 1, "type": "AuditAck", "dispatch_id": "d", "body": {"durable_audit_seq": 5, "durable_bytes": "1", "commit_lsn": "1"}}), // 数字而非字符串
        json!({"v": 1, "type": "Execute", "dispatch_id": "d", "body": {"command_id": "c", "task": {}}}), // 缺字段
        json!({"v": 1, "type": "AuditBatch", "dispatch_id": "d", "body": {"first_seq": "1", "records": [{"audit_seq": "1", "kind": "k"}]}}), // 记录缺 payload
    ]
}

pub fn unknown_type_envelope() -> Value {
    json!({"v": 1, "type": "Resume", "body": {}})
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
