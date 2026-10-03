//! ValueCatalog 快照语义：live 引用 ∪ 最近 PUBLISHED_TAIL 条发布。
//!
//! 回归背景：published 只增不减时，8 MiB 快照硬限会在约 2.8 万条历史值后
//! **永久打死 checkpoint**（每次启动退回全量重放）；而只按 live 过滤又会在
//! 「audit 发布值 ↔ 引用它的结果事务」之间开 checkpoint 窗口，重放后缀的
//! values.check 拒绝重放。live ∪ 尾部缓冲两个都要。

use base64::Engine;
use flow_journal::value::{ValueCatalog, ValueCodec, PUBLISHED_TAIL};
use flow_journal::{Event, EventKind, StoredValue, ValueRef};
use serde_json::json;
use sha2::Digest;
use std::collections::HashSet;

fn publish(catalog: &mut ValueCatalog, output_id: &str, bytes: &[u8]) -> ValueRef {
    let reference = ValueRef {
        journal_id: "j".into(),
        output_id: output_id.into(),
        codec: ValueCodec::Json,
        version: 1,
        chunk_count: 1,
        total_bytes: bytes.len() as u64,
        digest: hex::encode(sha2::Sha256::digest(bytes)),
    };
    catalog
        .apply(
            &Event::new(
                EventKind::ValueChunk,
                json!({
                    "output_id": output_id,
                    "chunk_index": "0",
                    "data": base64::engine::general_purpose::STANDARD.encode(bytes),
                }),
            ),
            "j",
        )
        .unwrap();
    catalog
        .apply(
            &Event::new(
                EventKind::ValuePublished,
                serde_json::to_value(&reference).unwrap(),
            ),
            "j",
        )
        .unwrap();
    reference
}

#[test]
fn snapshot_keeps_live_and_recent_tail_and_evicts_old_unreferenced() {
    let mut catalog = ValueCatalog::default();
    // 老值（无引用，越过尾部缓冲后必须被逐出）。
    let old = publish(&mut catalog, "old", b"old-bytes");
    // 尾部新值（无引用，仍在缓冲内必须保留——覆盖在飞窗口）。
    let newest_tail = format!("tail-{}", PUBLISHED_TAIL - 1);
    for index in 0..PUBLISHED_TAIL {
        publish(&mut catalog, &format!("tail-{index}"), b"t");
    }
    // live 引用值（最老也必须保留）。
    let live = publish(&mut catalog, "live", b"live-bytes");
    let mut live_ids = HashSet::new();
    live_ids.insert("live".to_string());

    let snapshot = catalog.snapshot(&live_ids).unwrap();
    let published = snapshot["published"].as_object().unwrap();
    assert!(
        !published.contains_key(&old.output_id),
        "越过尾部缓冲且无引用的旧值必须被逐出"
    );
    assert!(published.contains_key("live"), "live 引用必须保留");
    assert!(
        published.contains_key(&newest_tail),
        "尾部缓冲内的最新发布必须保留"
    );
    assert!(
        published.len() >= PUBLISHED_TAIL,
        "尾部 + live 至少保留 PUBLISHED_TAIL 条：{}",
        published.len()
    );

    // 快照可回载，且逐出后的 catalog 对旧引用的 check 如实失败。
    let restored = ValueCatalog::from_snapshot(snapshot).unwrap();
    assert!(restored.check(&StoredValue::Ref(old)).is_err());
    assert!(restored.check(&StoredValue::Ref(live)).is_ok());
}
