//! 一次性探针：对比「现值」与「单遍编/解码」的分配与耗时（用完删除）。
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
struct Counting;
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 { ALLOCS.fetch_add(1, Ordering::Relaxed); BYTES.fetch_add(l.size(), Ordering::Relaxed); System.alloc(l) }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) { System.dealloc(p, l) }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 { ALLOCS.fetch_add(1, Ordering::Relaxed); BYTES.fetch_add(n, Ordering::Relaxed); System.realloc(p, l, n) }
}
#[global_allocator]
static A: Counting = Counting;

use flow_engine::execution_protocol::frame::{encode_tagged_frame, FrameDecoder};
use flow_engine::execution_protocol::message::{record_bytes, AuditRecord, Message};
use serde::Serialize;
use serde_json::json;

fn audit_batch() -> Message {
    Message::AuditBatch {
        dispatch_id: "dispatch-09".into(),
        first_seq: 1,
        records: vec![AuditRecord { audit_seq: 1, kind: "business_audit".into(), payload: json!({"message": "x".repeat(120*1024)}) }],
    }
}

// ---- 单遍编码：直接写进帧缓冲，无中间 Value ----
#[derive(Serialize)]
struct AuditWire<'a> { first_seq: String, records: &'a [AuditRecord] }

fn encode_single_pass(message: &Message) -> Vec<u8> {
    let mut frame = Vec::with_capacity(256 * 1024);
    frame.extend_from_slice(&0u32.to_be_bytes()); // 长度占位
    frame.push(0u8); // control 标签占位
    frame.extend_from_slice(b"{\"v\":3,\"type\":\"AuditBatch\",\"dispatch_id\":\"dispatch-09\",\"body\":");
    let (records, first_seq) = match message { Message::AuditBatch { records, first_seq, .. } => (records, first_seq), _ => unreachable!() };
    serde_json::to_writer(&mut frame, &AuditWire { first_seq: first_seq.to_string(), records }).unwrap();
    frame.push(b'}');
    let payload_len = (frame.len() - 4) as u32;
    frame[0..4].copy_from_slice(&payload_len.to_be_bytes());
    frame
}

// ---- 单遍解码：信封只借 RawValue，body 直接 typed 解析 ----
struct Envelope {
    type_name: String,
    body: Box<serde_json::value::RawValue>,
}
fn parse_envelope(frame: &[u8]) -> Envelope {
    let bytes = &frame[5..];
    // 只借不拷贝：信封 3 个字段顺序扫描（v/type/body 由发送端固定）。
    let text = std::str::from_utf8(bytes).unwrap();
    let type_at = text.find("\"type\":\"").unwrap() + 8;
    let type_end = text[type_at..].find('"').unwrap() + type_at;
    let body_at = text.find("\"body\":").unwrap() + 7;
    // body 是最后一个字段：从 body_at 到 JSON 文本末尾的 } 之前。
    let json_end = text.len() - 1; // 信封收尾 }
    Envelope { type_name: text[type_at..type_end].to_string(), body: serde_json::value::RawValue::from_string(text[body_at..json_end].to_string()).unwrap() }
}
#[derive(serde::Deserialize)]
struct AuditBatchWire {
    #[serde(with = "flow_journal::decimal")]
    first_seq: u64,
    records: Vec<AuditRecord>,
}
fn decode_single_pass(frame: &[u8]) -> (String, u64, usize) {
    let env = parse_envelope(frame);
    let wire: AuditBatchWire = serde_json::from_str(env.body.get()).unwrap();
    (env.type_name, wire.first_seq, wire.records.len())
}

fn bench<T>(label: &str, iterations: usize, mut f: impl FnMut() -> T) {
    for _ in 0..10 { std::hint::black_box(f()); }
    let a0 = ALLOCS.load(Ordering::Relaxed);
    let b0 = BYTES.load(Ordering::Relaxed);
    let t0 = std::time::Instant::now();
    for _ in 0..iterations { std::hint::black_box(f()); }
    let d = t0.elapsed();
    println!("{label:40} allocs/op={:>4} bytes/op={:>9} {:?}/op",
        (ALLOCS.load(Ordering::Relaxed) - a0) / iterations,
        (BYTES.load(Ordering::Relaxed) - b0) / iterations,
        d / iterations as u32);
}

fn main() {
    let message = audit_batch();
    let frame_now = encode_tagged_frame(&message).unwrap();
    let frame_new = encode_single_pass(&message);
    println!("frame sizes: current={} single_pass={}\n", frame_now.len(), frame_new.len());

    println!("== ENCODE (AuditBatch ~123KiB) ==");
    bench("current: to_envelope + to_vec + frame", 2_000, || encode_tagged_frame(&message).unwrap());
    bench("single-pass: to_writer into frame", 2_000, || encode_single_pass(&message));

    println!("\n== DECODE (AuditBatch ~123KiB) ==");
    {
        let mut d = FrameDecoder::new_tagged();
        d.feed(&frame_now).unwrap();
        let (_, j) = d.pop_tagged_frame().unwrap().unwrap();
        let text = std::str::from_utf8(&j).unwrap();
        let body_at = text.find("\"body\":").unwrap() + 7;
        let env = parse_envelope(&frame_now);
        println!("debug body raw[..40]: {:?}", &env.body.get()[..40.min(env.body.get().len())]);
        let _ = body_at;
    }
    bench("current: from_slice<Value> + from_envelope", 2_000, || {
        let v: serde_json::Value = serde_json::from_slice(&frame_now).unwrap();
        Message::from_envelope(v).unwrap()
    });
    bench("decoder feed/pop only (no message)", 2_000, || {
        let mut d = FrameDecoder::new_tagged();
        d.feed(&frame_now).unwrap();
        let (_, j) = d.pop_tagged_frame().unwrap().unwrap();
        std::hint::black_box(j)
    });
    bench("single-pass envelope borrow + typed body", 2_000, || decode_single_pass(&frame_now));

    println!("\n== record_bytes 重复序列化（push/flush/ack 三次） ==");
    let record = AuditRecord { audit_seq: 1, kind: "business_audit".into(), payload: json!({"message": "x".repeat(120*1024)}) };
    bench("record_bytes() x3（现值三次）", 2_000, || {
        let a = record_bytes(record.audit_seq, &record.kind, &record.payload);
        let b = record_bytes(record.audit_seq, &record.kind, &record.payload);
        let c = record_bytes(record.audit_seq, &record.kind, &record.payload);
        (a, b, c)
    });
}
