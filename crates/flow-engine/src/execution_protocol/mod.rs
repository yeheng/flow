//! 执行 IPC 协议（postcard 单流帧）。
//!
//! DTO、物理帧、传输与会话校验都在本模块；业务层只依赖 [`message`] 的
//! 消息目录与 [`transport`] 的小型 Transport 边界（发帧、收帧、关闭），
//! 不依赖 Unix FD、PID 或机器路径。远程中继（agent）复用同一消息目录
//! 与帧格式（`remote` 模块）。
//!
//! 子进程不得打开 store/journal；本模块不引入任何存储依赖。
//!
//! 本模块守护的三条硬不变量（其余文件的价值都在维护它们）：
//! - 帧格式唯一：`u32 body_len | postcard(Message)`（见 [`frame`]）；
//!   control 语义消息 ≤ [`contract::CONTROL_MAX_FRAME`]，data 语义消息
//!   ≤ [`contract::DATA_MAX_FRAME`]。超限一律拒绝，不截断、不协商。
//! - 版本门集中在握手：[`contract::PROTOCOL_VERSION`] 必须双方相等，
//!   逐帧不带版本号——postcard 的变体索引是位置编码，跨版本配对会
//!   静默错解成另一种消息类型，是最难查的协议 bug 形态。
//! - 握手顺序固定：执行器侧 Hello → Welcome → Ready，agent 上联侧
//!   AgentHello → AgentWelcome；必需能力缺失即明确拒绝，不降级执行。
//!
//! 之所以把上界收在协议层而不是各业务模块：控制帧一旦允许携带用户值
//! 字节，一个大输出就能撑爆帧并杀掉整条会话；协议层是唯一能保证
//! 「任何一帧都有确定性上界」的位置。

pub mod bootstrap;
pub mod contract;
pub mod fixtures;
pub mod frame;
pub mod loopback;
pub mod message;
pub mod remote;
#[cfg(unix)]
pub mod spawn;
pub mod transfer_key;
pub mod transport;
#[cfg(unix)]
pub mod unix;
pub mod ws;

// 契约面（版本 / 帧上限 / 时限 / 能力目录）整体重导出：调用方按协议
// 语义取用，不必知道常量住在哪个子模块——协议冻结面因此只有一个入口。
pub use contract::*;
// 回连承载入口与地址 env 常量同属一组：生产 unix 本地执行走 socketpair，
// 这里主要服务 Windows 与测试路径。
pub use loopback::{connect_loopback, spawn_executor_loopback, CONNECT_ADDR_ENV};
// 消息目录的公开面：只导出业务层真正会构造/匹配的类型，wire 承载辅助
// （`wire_json` / `wire_stored`）留在 message 内部，不外泄编码细节。
pub use message::{
    record_bytes, AuditRecord, ExecuteTask, Message, ObservationLine, PreparedMeta, ProtocolError,
    ResultOutcome, TaskValue, WaitKind, WaitRequest,
};
// transfer id 的 wire 格式收在单一类型里：散落的字符串前缀拼写错误
// 只会静默丢块（执行器死等 InputReady 直到 deadline）。
pub use transfer_key::TransferKey;
// unix 专属的 FD 通道构造：仅 unix 编译，其他平台走回连承载。
#[cfg(unix)]
pub use unix::{channel_from_raw, socketpair_channel};
