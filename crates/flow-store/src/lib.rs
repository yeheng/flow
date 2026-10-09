//! flow-store：journal v2 的 SQLite 投影。
//!
//! v1 的 `Store`（flow.db 元数据权威）已随 v1 后端删除（2026-10-09，
//! 历史数据不迁移——见 docs/SQLITE_V1_TO_V2_MIGRATION.md）。本 crate 只剩
//! [`projection`]：journal 权威的一次性查询投影，可随时由
//! `flow-journal-dev rebuild-projection` 重建。定义 checksum 的实现移至
//! 各使用方（flow-pg 自带同算法副本）。

pub mod projection;
