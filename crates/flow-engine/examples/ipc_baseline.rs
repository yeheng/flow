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
    t12_breakdown();
    t12_v4_prototype();
}

/// T12 实测：同一逻辑内容（256 KiB 传输块）在 v3（base64 + JSON 信封）与
/// v4 原型（`u32 body_len + u8 kind + u32 meta_len + JSON meta + raw`）下的
/// 完整往返成本与线字节。v3 计入发送端 base64 编码（真实生产路径在构造
/// TransferChunk 时做）。
fn t12_v4_prototype() {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    let ops = 2_000u64;
    let raw = vec![0xa5u8; flow_engine::execution_protocol::TRANSFER_CHUNK_BYTES];
    let digest = hex::encode(sha2::Sha256::digest(&raw));
    let dispatch = "dispatch-t12";
    let transfer_id = "in:baseline";

    // ---------- v3：base64 编码 + JSON 信封往返 ----------
    let mut v3_wire = 0u64;
    let start = Instant::now();
    for _ in 0..ops {
        let encoded = STANDARD.encode(&raw);
        let message = Message::TransferChunk {
            dispatch_id: dispatch.into(),
            transfer_id: transfer_id.into(),
            offset: 0,
            bytes: encoded,
            digest: digest.clone(),
        };
        let frame = encode_tagged_frame(&message).expect("encode");
        v3_wire = frame.len() as u64;
        let json = &frame[flow_engine::execution_protocol::FRAME_HEADER_BYTES + 1..];
        let decoded = Message::decode_envelope(json).expect("decode");
        let Message::TransferChunk { bytes, .. } = &decoded else {
            unreachable!()
        };
        let roundtrip = STANDARD.decode(bytes).expect("b64 decode");
        std::hint::black_box(roundtrip);
    }
    let v3 = start.elapsed();

    // ---------- v4 原型：小 JSON meta + raw 字节 ----------
    let meta = serde_json::json!({
        "type": "TransferChunk",
        "dispatch_id": dispatch,
        "transfer_id": transfer_id,
        "offset": "0",
        "digest": digest,
    });
    let meta_bytes = serde_json::to_vec(&meta).unwrap();
    let mut v4_wire = 0u64;
    let start = Instant::now();
    for _ in 0..ops {
        // encode：预留头 + meta + raw
        let mut frame = Vec::with_capacity(12 + meta_bytes.len() + raw.len());
        frame.extend_from_slice(&0u32.to_be_bytes());
        frame.push(1u8); // kind: TransferChunk
        frame.extend_from_slice(&(meta_bytes.len() as u32).to_be_bytes());
        frame.extend_from_slice(&meta_bytes);
        frame.extend_from_slice(&raw);
        let body_len = frame.len() - 4;
        frame[..4].copy_from_slice(&(body_len as u32).to_be_bytes());
        v4_wire = frame.len() as u64;
        // decode：解析长度、借用 raw、解析 meta
        // 布局：0..4 body_len | 4 kind | 5..9 meta_len | 9.. meta | raw
        let meta_len = u32::from_be_bytes([frame[5], frame[6], frame[7], frame[8]]) as usize;
        let meta_slice = &frame[9..9 + meta_len];
        let payload = &frame[9 + meta_len..];
        let parsed: serde_json::Value = serde_json::from_slice(meta_slice).expect("meta");
        std::hint::black_box((parsed, payload));
    }
    let v4 = start.elapsed();

    let us = |d: std::time::Duration| d.as_micros() as f64 / ops as f64;
    let (v3_us, v4_us) = (us(v3), us(v4));
    println!(
        "\nT12 v3 vs v4 原型 (256KiB transfer, full roundtrip):\n  \
         v3(base64+json): {v3_us:>8.2} µs/op  wire={v3_wire:>7} B\n  \
         v4(meta+raw):    {v4_us:>8.2} µs/op  wire={v4_wire:>7} B\n  \
         speedup={:.2}x  wire_reduction={:.1}%",
        v3_us / v4_us,
        (v3_wire - v4_wire) as f64 / v3_wire as f64 * 100.0
    );

    // ---------- 现实口径：Journal 仍存 base64（Master 边界转回）----------
    // v3（审计方向）：executor: b64_encode + JSON ser（350KB 字符串）；
    //               master: JSON parse。
    // v4（审计方向）：executor: raw 拷贝入帧 + 小 meta；
    //               master: b64_encode（写 Journal）。
    // 两端 base64 工作量相抵，实际差异 = 大字符串的 JSON ser+parse。
    let start = Instant::now();
    for _ in 0..ops {
        let encoded = STANDARD.encode(&raw);
        let message = Message::AuditBatch {
            dispatch_id: dispatch.into(),
            first_seq: 1,
            records: vec![AuditRecord {
                audit_seq: 1,
                kind: "value_chunk".into(),
                payload: serde_json::json!({"data": encoded}),
            }],
        };
        let frame = encode_tagged_frame(&message).expect("encode");
        let json = &frame[flow_engine::execution_protocol::FRAME_HEADER_BYTES + 1..];
        let decoded = Message::decode_envelope(json).expect("decode");
        std::hint::black_box(decoded);
    }
    let v3_audit = start.elapsed();

    let start = Instant::now();
    for _ in 0..ops {
        let mut frame = Vec::with_capacity(12 + meta_bytes.len() + raw.len());
        frame.extend_from_slice(&0u32.to_be_bytes());
        frame.push(1u8);
        frame.extend_from_slice(&(meta_bytes.len() as u32).to_be_bytes());
        frame.extend_from_slice(&meta_bytes);
        frame.extend_from_slice(&raw);
        let meta_len = u32::from_be_bytes([frame[5], frame[6], frame[7], frame[8]]) as usize;
        let payload = &frame[9 + meta_len..];
        let _ = serde_json::from_slice::<serde_json::Value>(&frame[9..9 + meta_len]).expect("meta");
        // master 边界：raw → base64（写 Journal，schema 不变）。
        let journal_chunk = STANDARD.encode(payload);
        std::hint::black_box(journal_chunk);
    }
    let v4_audit = start.elapsed();
    println!(
        "T12 现实口径 (audit, Journal 仍存 base64):\n  \
         v3: {:.2} µs/op\n  \
         v4: {:.2} µs/op\n  \
         speedup={:.2}x",
        us(v3_audit),
        us(v4_audit),
        us(v3_audit) / us(v4_audit),
    );
}

/// T12 进入条件测量：把最大传输块的处理成本拆成「信封 JSON」「base64
/// 编解码」「整值 sha256」三块，判断二进制 wire 是否值得一次 v4 升级。
/// 输出各分量 µs/op 与 base64 占比（量化收益上界）。
fn t12_breakdown() {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    let ops = 2_000u64;
    let raw = vec![0xa5u8; flow_engine::execution_protocol::TRANSFER_CHUNK_BYTES];
    let encoded = b64(&raw);

    // 分量 1：base64 解码（执行器 transfer.feed 的主要开销）。
    let start = Instant::now();
    for _ in 0..ops {
        let decoded = STANDARD.decode(&encoded).expect("b64 decode");
        std::hint::black_box(decoded);
    }
    let b64_decode = start.elapsed();

    // 分量 2：整值 sha256（每块校验）。
    let start = Instant::now();
    for _ in 0..ops {
        std::hint::black_box(sha2::Sha256::digest(&raw));
    }
    let sha = start.elapsed();

    // 分量 3：信封 JSON 往返（生产编解码已去掉 body 深拷贝，见 T05）。
    let message = max_transfer_chunk("dispatch-t12");
    let start = Instant::now();
    for _ in 0..ops {
        let frame = encode_tagged_frame(&message).expect("encode");
        let json = &frame[flow_engine::execution_protocol::FRAME_HEADER_BYTES + 1..];
        let decoded = Message::decode_envelope(json).expect("decode");
        std::hint::black_box(decoded);
    }
    let envelope = start.elapsed();

    let us = |d: std::time::Duration| d.as_micros() as f64 / ops as f64;
    let (b, h, e) = (us(b64_decode), us(sha), us(envelope));
    let base64_share = b / (b + h + e) * 100.0;
    println!(
        "\nT12 breakdown (256KiB chunk): b64_decode={b:.2} µs  sha256={h:.2} µs  envelope_json={e:.2} µs  \
         base64_share={base64_share:.0}%  wire_inflation={:.0}%",
        (encoded.len() as f64 / raw.len() as f64 - 1.0) * 100.0
    );
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
