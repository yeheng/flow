//! Extension registration surface for in-process Rust crates.
//!
//! An extension is a normal Rust crate (path / workspace dependency) that
//! exports `pub fn register(api: &mut dyn ExtensionApi)`. The host binary
//! calls those `register` functions at startup (often behind Cargo features).
//! There is no dynamic loading, no ABI version, no `.so` marketplace.

pub mod api;

pub use api::{AfterToolCallHandler, BeforeToolCallHandler, ExtensionApi, ExtensionApiImpl};
