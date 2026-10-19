use flow_engine::execution_protocol::message::Message;
use flow_executor::transfer::IncomingTransfers;
use sha2::{Digest, Sha256};

fn chunk(id: &str, offset: u64, bytes: &[u8]) -> Message {
    Message::TransferChunk {
        dispatch_id: "d".into(),
        transfer_id: id.into(),
        offset,
        bytes: bytes.to_vec(),
        digest: hex::encode(Sha256::digest(bytes)),
    }
}
fn ready(id: &str, bytes: &[u8]) -> Message {
    Message::InputReady {
        dispatch_id: "d".into(),
        transfer_id: id.into(),
        total_bytes: bytes.len() as u64,
        digest: hex::encode(Sha256::digest(bytes)),
    }
}

#[test]
fn offset_overflow_and_overlapping_chunks_are_rejected() {
    let mut transfers = IncomingTransfers::default();
    assert!(transfers
        .feed(&chunk("in:run-input", u64::MAX, b"x"))
        .is_err());
    transfers.feed(&chunk("in:run-input", 0, b"abc")).unwrap();
    assert!(transfers.feed(&chunk("in:run-input", 2, b"cd")).is_err());
    transfers.feed(&chunk("in:run-input", 0, b"abc")).unwrap();
    transfers
        .input_ready(&ready("in:run-input", b"abc"))
        .unwrap();
    assert_eq!(
        transfers.take("in:run-input").unwrap().as_slice(),
        b"abc".as_slice()
    );
    // Completed buffers still accept exact remote replay without retaining chunks twice.
    transfers.feed(&chunk("in:run-input", 0, b"abc")).unwrap();
    transfers
        .input_ready(&ready("in:run-input", b"abc"))
        .unwrap();
    assert!(transfers.feed(&chunk("in:run-input", 0, b"xyz")).is_err());
}

#[test]
fn aggregate_input_budget_is_enforced_before_assembly() {
    let mut transfers = IncomingTransfers::default();
    let bytes = vec![b'a'; flow_journal::CHUNK_BYTES];
    for index in 0..32 {
        transfers
            .feed(&chunk("in:run-input", index * bytes.len() as u64, &bytes))
            .unwrap();
    }
    assert!(transfers.feed(&chunk("in:other", 0, b"x")).is_err());
    let mut transfers = IncomingTransfers::default();
    assert!(transfers
        .input_ready(&Message::InputReady {
            dispatch_id: "d".into(),
            transfer_id: "in:run-input".into(),
            total_bytes: 8 * 1024 * 1024 + 1,
            digest: "0".repeat(64)
        })
        .is_err());
}
