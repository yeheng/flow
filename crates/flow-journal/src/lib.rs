//! Versioned, bounded, single-writer JSONL authority. No execution or projection side effects.
pub mod codec;
pub mod maintenance;
pub mod page;
pub mod storage;
pub mod tail;
pub mod value;
pub mod writer;

pub use codec::{Event, EventKind, Transaction};
pub use storage::{scan, Location, Recovery, ScanFault};
pub use value::{StoredValue, ValueRef};
pub use writer::{Commit, Journal, JournalOptions, QueueClass};

pub const MAX_LINE_BYTES: usize = 1024 * 1024;
pub const INLINE_BYTES: usize = 64 * 1024;
pub const CHUNK_BYTES: usize = 256 * 1024;
pub const MAX_VALUE_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("journal I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("journal JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid journal: {0}")]
    Invalid(String),
    #[error("journal limit exceeded: {0}")]
    Limit(String),
    #[error("journal unavailable: {0}")]
    Unavailable(String),
    #[error("journal conflict: {0}")]
    Conflict(String),
}
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

/// Canonical decimal u64 on disk and in v2 DTOs, never a JSON number.
pub mod decimal {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(v: &u64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        let s = String::deserialize(d)?;
        let value: u64 = s.parse().map_err(serde::de::Error::custom)?;
        if value.to_string() != s {
            return Err(serde::de::Error::custom("noncanonical decimal"));
        }
        Ok(value)
    }
}
