//! 部署配置（`flow-pg/src/config.rs`）。TTL/续期/轮询间隔的默认值是起点，
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
    /// 连接池上限。
    pub max_connections: u32,
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
            max_connections: 16,
        }
    }
}

fn env_number<T: std::str::FromStr>(key: &str, default: T) -> Result<T, String> {
    match std::env::var(key) {
        Ok(value) => value
            .parse()
            .map_err(|_| format!("{key} must be a non-negative integer")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(_) => Err(format!("{key} must be valid Unicode")),
    }
}
fn env_ms(key: &str, default_ms: u64) -> Result<Duration, String> {
    env_number(key, default_ms).map(Duration::from_millis)
}
impl PgConfig {
    pub fn from_env() -> Result<PgConfig, String> {
        let role = match std::env::var("FLOW_ROLE").as_deref() {
            Ok("gateway") => Role::Gateway,
            Ok("executor") => Role::Executor,
            Ok("all") | Err(std::env::VarError::NotPresent) => Role::All,
            _ => return Err("FLOW_ROLE must be all, gateway, or executor".into()),
        };
        Ok(PgConfig {
            lease_ttl: env_ms("FLOW_LEASE_TTL_MS", 30_000)?,
            scan_interval: env_ms("FLOW_SCAN_INTERVAL_MS", 1_000)?,
            inbox_poll: env_ms("FLOW_INBOX_POLL_MS", 200)?,
            max_runs: env_number("FLOW_MAX_RUNS", 8)?,
            signal_wait: env_ms("FLOW_SIGNAL_WAIT_MS", 10_000)?,
            signal_poll: env_ms("FLOW_SIGNAL_POLL_MS", 200)?,
            subscribe_poll: env_ms("FLOW_SUBSCRIBE_POLL_MS", 10_000)?,
            statement_timeout: env_ms("FLOW_STATEMENT_TIMEOUT_MS", 10_000)?,
            lock_timeout: env_ms("FLOW_LOCK_TIMEOUT_MS", 10_000)?,
            idle_tx_timeout: env_ms("FLOW_IDLE_TX_TIMEOUT_MS", 10_000)?,
            role,
            max_connections: env_number("FLOW_PG_MAX_CONNECTIONS", 16)?,
        })
    }
}

impl PgConfig {
    /// 统一配置入口：从 flow-config 的 `[pg]` 分区构造。
    /// role 字符串在这里解析（配置文件反序列化层只保证是三者之一以外的
    /// 字符串会被 validate 拦下，这里是最后防线）。
    pub fn from_tuning(tuning: &flow_config::PgTuning) -> Result<PgConfig, String> {
        let role = match tuning.role.as_str() {
            "gateway" => Role::Gateway,
            "executor" => Role::Executor,
            "all" => Role::All,
            other => {
                return Err(format!(
                    "pg.role 必须是 all, gateway, or executor：{other:?}"
                ))
            }
        };
        Ok(PgConfig {
            lease_ttl: Duration::from_millis(tuning.lease_ttl_ms),
            scan_interval: Duration::from_millis(tuning.scan_interval_ms),
            inbox_poll: Duration::from_millis(tuning.inbox_poll_ms),
            max_runs: tuning.max_runs as usize,
            signal_wait: Duration::from_millis(tuning.signal_wait_ms),
            signal_poll: Duration::from_millis(tuning.signal_poll_ms),
            subscribe_poll: Duration::from_millis(tuning.subscribe_poll_ms),
            statement_timeout: Duration::from_millis(tuning.statement_timeout_ms),
            lock_timeout: Duration::from_millis(tuning.lock_timeout_ms),
            idle_tx_timeout: Duration::from_millis(tuning.idle_tx_timeout_ms),
            role,
            max_connections: tuning.max_connections,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_explicit_environment_is_rejected() {
        for (key, value) in [
            ("FLOW_MAX_RUNS", "many"),
            ("FLOW_SCAN_INTERVAL_MS", "-1"),
            ("FLOW_ROLE", "executro"),
        ] {
            let before = std::env::var_os(key);
            std::env::set_var(key, value);
            let result = PgConfig::from_env();
            match before {
                Some(old) => std::env::set_var(key, old),
                None => std::env::remove_var(key),
            }
            assert!(result.unwrap_err().contains(key));
        }
    }
}
