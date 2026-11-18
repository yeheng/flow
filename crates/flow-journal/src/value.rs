//! Storage adapters for shared value contracts.

pub use flow_facts::value::*;

#[cfg(feature = "storage")]
mod storage_impl;
#[cfg(feature = "storage")]
pub use storage_impl::*;
