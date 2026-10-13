//! T01 协议性能基线：可复现的编码/传输微基准（docs/IPC_REVIEW_OPTIMIZED_TASK_LIST.md）。
//!
//! 运行（release 计数，结果记录进基线报告）：
//! ```sh
//! cargo run -p flow-engine --release --example ipc_baseline --locked
//! ```
//!
//! 场景覆盖：小控制消息、审计批次、最大合法传输块的编解码（吞吐 +
//! 分配计数），以及真实 FrameTransport（duplex 承载）的端到端吞吐与
//! 「大帧洪泛中 Cancel 的排队延迟」。基线只测当前实现；收益结论必须
//! 用同一命令前后对比得出，不许外推。

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use sha2::Digest;

use flow_engine::execution_protocol::frame::encode_tagged_frame;
use flow_engine::execution_protocol::message::{AuditRecord, Message};
use flow_engine::execution_protocol::transport::FrameTransport;

// ---- 分配计数器（全局分配器包装）：每场景快照差值 ----
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        System.realloc(ptr, layout, new_size)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        System.alloc_zeroed(layout)
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

struct Counters {
    allocs: u64,
    bytes: u64,
}

fn snapshot() -> Counters {
    Counters {
        allocs: ALLOCS.load(Ordering::Relaxed),
        bytes: ALLOC_BYTES.load(Ordering::Relaxed),
    }
}

fn delta(before: Counters) -> Counters {
    Counters {
        allocs: ALLOCS.load(Ordering::Relaxed) - before.allocs,
        bytes: ALLOC_BYTES.load(Ordering::Relaxed) - before.bytes,
    }
}

fn report(name: &str, ops: u64, elapsed: std::time::Duration, payload_bytes: u64, allocs: Counters) {
    let us_per_op = elapsed.as_micros() as f64 / ops as f64;
    let mib_s = (payload_bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
    println!(
        "{name:<28} ops={ops:>7}  {us_per_op:>9.2} µs/op  {mib_s:>8.2} MiB/s  \
         allocs/op={:>>7.1}  alloc_bytes/op={:>>9.1}",
        allocs.allocs as f64 / ops as f64,
        allocs.bytes as f64 / ops as f64,
    );
}

fn heartbeat(dispatch: &str) -> Message {
    Message::Heartbeat {
        dispatch_id: Some(dispatch.into()),
        phase: "running".into(),
    }
}

fn audit_batch(dispatch: &str, records: usize, payload_len: usize) -> Message {
    let payload_text = "x".repeat(payload_len);
    let records = (1..=records)
        .map(|seq| AuditRecord {
            audit_seq: seq as u64,
            kind: "business_audit".into(),
            payload: serde_json::json!({"value": payload_text, "seq": seq}),
        })
        .collect();
    Message::AuditBatch {
        dispatch_id: dispatch.into(),
        first_seq: 1,
        records,
    }
}

fn max_transfer_chunk(dispatch: &str) -> Message {
    // 最大合法块：CHUNK_BYTES 原始字节（base64 后占 4/3）。
    let raw = vec![0xa5u8; flow_engine::execution_protocol::TRANSFER_CHUNK_BYTES];
    let digest = hex::encode(sha2::Sha256::digest(&raw));
    Message::TransferChunk {
        dispatch_id: dispatch.into(),
        transfer_id: "in:baseline".into(),
        offset: 0,
        bytes: b64(&raw),
        digest,
    }
}

fn b64(raw: &[u8]) -> String {
    // dev 依赖里手写最小 base64（标准字母表，含 padding）——避免为示例
    // 给 flow-engine 增加 base64 运行时依赖。
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(raw.len().div_ceil(3) * 4);
    for chunk in raw.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

fn codec_scenario(name: &str, ops: u64, message: &Message, wire_bytes: u64) {
    // 语义等价断言先于计时（基线必须先证明 roundtrip 正确）。
    let frame = encode_tagged_frame(message).expect("encode");
    let json = &frame[flow_engine::execution_protocol::FRAME_HEADER_BYTES + 1..];
    let decoded = Message::decode_envelope(json).expect("decode");
    assert_eq!(decoded, *message, "roundtrip must be semantically equal");

    let before = snapshot();
    let start = Instant::now();
    for _ in 0..ops {
        let frame = encode_tagged_frame(message).expect("encode");
        let json = &frame[flow_engine::execution_protocol::FRAME_HEADER_BYTES + 1..];
        let decoded = Message::decode_envelope(json).expect("decode");
        std::hint::black_box(decoded);
    }
    let elapsed = start.elapsed();
    report(name, ops, elapsed, wire_bytes * ops, delta(before));
}

fn main() {
    let ops_small = 20_000;
    let ops_batch = 2_000;
    let ops_max = 1_000;

    let small = heartbeat("dispatch-baseline");
    let batch = audit_batch("dispatch-baseline", 64, 1024);
    let max_chunk = max_transfer_chunk("dispatch-baseline");

    let wire_small = encode_tagged_frame(&small).unwrap().len() as u64;
    let wire_batch = encode_tagged_frame(&batch).unwrap().len() as u64;
    let wire_max = encode_tagged_frame(&max_chunk).unwrap().len() as u64;

    println!("flow-engine v3 协议基线（release，单线程编解码）");
    println!("wire bytes: control={} audit_batch(64×1KiB)={} max_chunk={}\n", wire_small, wire_batch, wire_max);

    codec_scenario("control_small (Heartbeat)", ops_small, &small, wire_small);
    codec_scenario("audit_batch (64×1KiB)", ops_batch, &batch, wire_batch);
    codec_scenario("transfer_max (256KiB chunk)", ops_max, &max_chunk, wire_max);

    // ---- 端到端 transport（duplex 承载）：吞吐 + 取消排队延迟 ----
    transport_throughput(500);
    cancel_latency_under_flood();
}

fn transport_throughput(batches: u64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(async move {
        let (a, b) = tokio::io::duplex(1024 * 1024);
        let sender = FrameTransport::spawn(a, 128);
        let mut receiver = FrameTransport::spawn(b, 128);
        let batch = audit_batch("dispatch-baseline", 64, 1024);
        let wire = encode_tagged_frame(&batch).unwrap().len() as u64;

        let producer = tokio::spawn(async move {
            for _ in 0..batches {
                sender.send(batch.clone()).await.expect("send");
            }
            sender
                .flush_and_close(std::time::Duration::from_secs(5))
                .await;
        });

        let before = snapshot();
        let start = Instant::now();
        let received = {
            let mut count = 0u64;
            while let Ok((_, message)) = receiver.recv().await {
                std::hint::black_box(&message);
                count += 1;
            }
            count
        };
        let elapsed = start.elapsed();
        producer.await.expect("producer");
        assert_eq!(received, batches, "must receive every batch exactly once");
        report(
            "transport_duplex throughput",
            batches,
            elapsed,
            wire * batches,
            delta(before),
        );
    });
    rt.shutdown_background();
}

fn cancel_latency_under_flood() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(async {
        let (a, b) = tokio::io::duplex(256 * 1024);
        let sender = FrameTransport::spawn(a, 128);
        let mut receiver = FrameTransport::spawn(b, 128);
        let big = max_transfer_chunk("dispatch-flood");

        // 预填发送队列（大帧洪泛），随后提交 Cancel——单流契约下 Cancel
        // 排在已入队帧之后，实测排队延迟即基线值（不是协议保证）。
        for _ in 0..32 {
            sender.try_send(big.clone()).expect("prefill queue");
        }
        let cancel = Message::Cancel {
            command_id: "cancel-baseline".into(),
            dispatch_id: "dispatch-flood".into(),
        };
        sender.try_send(cancel).expect("queue cancel");
        let start = Instant::now();
        let mut saw_cancel = false;
        let mut frames = 0u64;
        while let Ok((_, message)) = receiver.recv().await {
            frames += 1;
            if matches!(message, Message::Cancel { .. }) {
                saw_cancel = true;
                break;
            }
        }
        let latency = start.elapsed();
        assert!(saw_cancel, "cancel must arrive after queued frames");
        println!(
            "cancel_latency_under_flood     queued_frames_before_cancel={frames}  latency={:.2} ms",
            latency.as_secs_f64() * 1000.0
        );
    });
    rt.shutdown_background();
}
