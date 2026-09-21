//! 部署配置（DISTRIBUTED.md §10）。TTL/续期/轮询间隔的默认值是起点，
//! 正式部署必须依据数据库延迟实测调整。

use std::time::Duration;

/// executor 角色。gateway 与 executor 可以同进程（小部署），也可拆分。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// gateway + executor（默认）
    All,
    /// 只提供元数据与持久输入入口
    Gateway,
    /// 只执行租约扫描与驱动
    Executor,
}

#[derive(Debug, Clone)]
pub struct PgConfig {
    /// 租约 TTL。续期周期为 TTL/3。
    pub lease_ttl: Duration,
    /// executor 扫描间隔。
    pub scan_interval: Duration,
    /// Driver 对持久 inbox 的轮询间隔。
    pub inbox_poll: Duration,
    /// 本进程最大驱动 run 数（容量许可）。
    pub max_runs: usize,
    /// gateway 等待信号落账的超时；超时后返回 pending，客户端用 run.signal_status 查询。
    pub signal_wait: Duration,
    /// gateway 等待落账的轮询间隔。
    pub signal_poll: Duration,
    /// 订阅兜底轮询间隔（§8：LISTEN/NOTIFY 唤醒是正常路径，此项只是通知丢失
    /// 时的安全网；也是 child.rs 等待子 run 终态的兜底间隔）。
    pub subscribe_poll: Duration,
    /// 语句超时（§5.1：锁等待、语句执行和 idle transaction 必须有超时）。
    pub statement_timeout: Duration,
    pub lock_timeout: Duration,
    pub idle_tx_timeout: Duration,
    pub role: Role,
}

impl Default for PgConfig {
    fn default() -> Self {
        PgConfig {
            lease_ttl: Duration::from_secs(30),
            scan_interval: Duration::from_millis(1000),
            inbox_poll: Duration::from_millis(200),
            max_runs: 8,
            signal_wait: Duration::from_secs(10),
            signal_poll: Duration::from_millis(200),
            subscribe_poll: Duration::from_secs(10),
            statement_timeout: Duration::from_secs(10),
            lock_timeout: Duration::from_secs(10),
            idle_tx_timeout: Duration::from_secs(10),
            role: Role::All,
        }
    }
}

fn env_ms(key: &str, default_ms: u64) -> Duration {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(default_ms))
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

impl PgConfig {
    pub fn from_env() -> PgConfig {
        let role = match std::env::var("FLOW_ROLE").as_deref() {
            Ok("gateway") => Role::Gateway,
            Ok("executor") => Role::Executor,
            _ => Role::All,
        };
        PgConfig {
            lease_ttl: env_ms("FLOW_LEASE_TTL_MS", 30_000),
            scan_interval: env_ms("FLOW_SCAN_INTERVAL_MS", 1_000),
            inbox_poll: env_ms("FLOW_INBOX_POLL_MS", 200),
            max_runs: env_usize("FLOW_MAX_RUNS", 8),
            signal_wait: env_ms("FLOW_SIGNAL_WAIT_MS", 10_000),
            signal_poll: env_ms("FLOW_SIGNAL_POLL_MS", 200),
            subscribe_poll: env_ms("FLOW_SUBSCRIBE_POLL_MS", 10_000),
            statement_timeout: env_ms("FLOW_STATEMENT_TIMEOUT_MS", 10_000),
            lock_timeout: env_ms("FLOW_LOCK_TIMEOUT_MS", 10_000),
            idle_tx_timeout: env_ms("FLOW_IDLE_TX_TIMEOUT_MS", 10_000),
            role,
        }
    }
}
