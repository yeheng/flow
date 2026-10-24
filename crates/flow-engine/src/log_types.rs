//! Observation log vocabulary.
//!
//! 本模块定义观测日志的两个词汇：**级别**（`LogLevel`）与**来源**
//! （`LogStream`）。二者都是 wire 格式（snake_case 序列化），会写进
//! 观测记录并被前端按字符串匹配，所以取值集合是协议的一部分。
//!
//! 它守护的不变量是「级别管重要性、来源管去向」：级别决定展示与过滤，
//! 来源决定落到 stdout 还是 stderr，两者互相独立。
//!
//! 不这样做的代价：前端要按级别过滤时还得先猜这条日志是不是脚本输出，
//! 分类规则散落在各调用点后必然出现「同一条日志在不同页面归类不同」。
use serde::{Deserialize, Serialize};
/// 节点日志级别（NodeLog.level）。
///
/// 级别只表达重要性，不表达来源：引擎叙事与脚本输出共用同一套级别，
/// 这样前端可以只按级别过滤而不必区分是谁写的。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    /// 调试细节：排查用，默认不展示。
    Debug,
    /// 常规叙事：节点执行的主要步骤。
    Info,
    /// 警告：未致命但值得注意（如降级、重试前的提示）。
    Warn,
    /// 错误：与本次节点执行失败相关。
    Error,
}

/// 节点日志来源：engine=引擎叙事，stdout/stderr=脚本 console 输出。
///
/// 来源同时决定落盘去向：脚本的 warn/error 走 stderr，其余走 stdout，
/// 与 Node 的常规约定一致（process.rs 侧的 harness 采集同此口径）。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    /// 引擎自身写出的叙事日志。
    Engine,
    /// 脚本的标准输出。
    Stdout,
    /// 脚本的标准错误。
    Stderr,
}
