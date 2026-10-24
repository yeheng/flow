//! 引擎侧的错误类型：只承载「表达式求值」这一类失败。
//!
//! 调度、存储、协议等其它失败各自归属其拥有者 crate（flow-journal 等），
//! 不在这里统一分类——一个跨域的错误枚举会把「业务失败」与「平台故障」
//! 混进同一分支，正好违反 DESIGN.md §13.3 的铁律。
//!
//! 它守护的不变量是**失败面单一**：这个 crate 只可能因为求值失败而失败，
//! 出现别的错误就说明有代码走错了层次。
//!
//! 不这样做的代价：调用方无法用变体来区分「用户脚本写错了」（该重试或
//! 让用户改）与「基础设施坏了」（该挂起等恢复），只能靠字符串匹配。
use thiserror::Error;

/// Expression evaluation errors. Scheduling and storage failures belong to their
/// owning crates; they are not classified by the expression evaluator.
///
/// 本枚举刻意只有一个变体：它是「求值面」的错误边界，调用方拿到它就
/// 知道失败发生在沙箱内（脚本异常、超时、预算超限、序列化失败）。
#[derive(Debug, Error)]
pub enum EngineError {
    /// 表达式/脚本求值失败，携带可直接展示给用户的原因文本。
    #[error("表达式求值失败：{0}")]
    Expr(String),
}
