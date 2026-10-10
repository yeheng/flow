//! Observation log vocabulary.
use serde::{Deserialize, Serialize};
/// 节点日志级别（NodeLog.level）。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// 节点日志来源：engine=引擎叙事，stdout/stderr=脚本 console 输出。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Engine,
    Stdout,
    Stderr,
}
