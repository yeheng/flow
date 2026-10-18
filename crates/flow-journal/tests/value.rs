//! Publication cache semantics: the reducer validates business references against
//! verified descriptors only. Chunk bytes and midstate hashing never enter this cache.

use flow_journal::value::{Publications, ValueCodec, PUBLISHED_TAIL};
use flow_journal::{Event, EventKind, StoredValue, ValueRef};
use serde_json::json;

fn descriptor(output_id: &str, digest: &str) -> ValueRef {
    ValueRef {
        journal_id: "j".into(),
        output_id: output_id.into(),
        codec: ValueCodec::Json,
        version: 1,
        chunk_count: 1,
        total_bytes: 1,
        digest: digest.into(),
    }
}

#[test]
fn publication_descriptors_are_the_only_reference_authority() {
    let mut publications = Publications::default();
    let value = descriptor("v1", &"a".repeat(64));
    publications
        .apply(
            &Event::new(
                EventKind::ValuePublished,
                serde_json::to_value(&value).unwrap(),
            ),
            "j",
        )
        .unwrap();
    assert!(publications.check(&StoredValue::Ref(value.clone())).is_ok());
    let mut wrong = value.clone();
    wrong.digest = "b".repeat(64);
    assert!(publications.check(&StoredValue::Ref(wrong)).is_err());
    assert!(publications
        .check(&StoredValue::Inline(json!({"inline": true})))
        .is_ok());
}

#[test]
fn chunk_events_do_not_require_reducer_state() {
    let mut publications = Publications::default();
    publications
        .apply(
            &Event::new(
                EventKind::ValueChunk,
                json!({"output_id": "v1", "chunk_index": "0", "data": "eA=="}),
            ),
            "j",
        )
        .unwrap();
    assert_eq!(publications.cached_publications(), 0);
}

#[test]
fn duplicate_or_foreign_publication_is_rejected() {
    let mut publications = Publications::default();
    let value = descriptor("v1", &"a".repeat(64));
    let event = Event::new(
        EventKind::ValuePublished,
        serde_json::to_value(&value).unwrap(),
    );
    publications.apply(&event, "j").unwrap();
    assert!(publications.apply(&event, "j").is_err());
    let mut foreign = descriptor("v2", &"b".repeat(64));
    foreign.journal_id = "other".into();
    assert!(publications
        .apply(
            &Event::new(
                EventKind::ValuePublished,
                serde_json::to_value(foreign).unwrap(),
            ),
            "j",
        )
        .is_err());
}

#[test]
fn trim_evicts_old_descriptors_and_remember_reseeds() {
    let mut publications = Publications::default();
    for index in 0..(PUBLISHED_TAIL + 10) {
        publications
            .remember(descriptor(&format!("v{index}"), &"a".repeat(64)))
            .unwrap();
    }
    publications.trim_cache();
    assert_eq!(publications.cached_publications(), PUBLISHED_TAIL);
    let evicted = descriptor("v0", &"a".repeat(64));
    assert!(publications
        .check(&StoredValue::Ref(evicted.clone()))
        .is_err());
    publications.remember(evicted.clone()).unwrap();
    assert!(publications.check(&StoredValue::Ref(evicted)).is_ok());
}
