//! SQLite inspection and filesystem initialization. Locations identify logical facts.
//!
//! 本模块负责「读事实」与「准备文件系统」：扫描已提交事实并报告恢复情况，
//! 以及创建权限受限、拒绝符号链接的数据目录与文件。
//! 核心不变量：扫描只读已提交水位以内的事实；数据目录 0700、数据文件 0600，
//! 且拒绝符号链接目标。
//! 需要它的原因：恢复期要能回答「权威到哪一条为止」，同时不能让别的主体
//! 通过软链或宽松权限把权威库换掉或读走。
use crate::codec::Transaction;
use crate::{invalid, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::path::Path;

/// 逻辑事实的位置：提交序号与编码字节数。
///
/// 字节数用于订阅与 tail 的按字节限流；它必须与落盘时记录的口径一致。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Location {
    #[serde(with = "crate::decimal")]
    pub bytes: u64,
    #[serde(with = "crate::decimal")]
    pub lsn: u64,
}

/// 扫描中断点：出错的 LSN 与原因。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanFault {
    pub lsn: u64,
    pub reason: String,
}

/// 扫描或校验报告。
///
/// `last_lsn` 是连续校验通过的最后一条事实；`fault` 存在表示扫描在此中断，
/// 之后的事实未被验证。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Recovery {
    pub journal_id: String,
    #[serde(with = "crate::decimal")]
    pub last_lsn: u64,
    pub fault: Option<ScanFault>,
}

/// 扫描全部已提交事实，逐条交给回调处理。
///
/// # Errors
/// 打开或读取权威库失败时返回错误；回调自身的错误会被记为扫描故障并
/// 提前结束扫描，而不是让整次扫描失败。
pub fn scan(
    root: &Path,
    apply: impl FnMut(&Transaction, &Location) -> Result<()>,
) -> Result<Recovery> {
    scan_until(root, u64::MAX, apply)
}

/// 扫描到指定提交序号为止的事实。
///
/// 回调出错即视为扫描故障：已验证的前缀仍然可用，出错点之后的事实未被
/// 验证，报告里必须如实体现。
///
/// # Errors
/// 游标越过数据库水位、打开或读取权威库失败时返回错误。
pub fn scan_until(
    root: &Path,
    upper_lsn: u64,
    mut apply: impl FnMut(&Transaction, &Location) -> Result<()>,
) -> Result<Recovery> {
    let conn = crate::sqlite::read_connection(root)?;
    conn.execute_batch("BEGIN").map_err(crate::sqlite::db)?;
    let (identity, durable) = crate::sqlite::metadata(&conn)?;
    if upper_lsn != u64::MAX && upper_lsn > durable {
        return Err(invalid("cursor ahead of database"));
    }
    let upper = upper_lsn.min(durable);
    let mut report = Recovery {
        journal_id: identity.clone(),
        ..Default::default()
    };
    for lsn in 1..=upper {
        let result = (|| {
            let (tx, bytes) = crate::sqlite::transaction_with_size(&conn, &identity, lsn)?;
            apply(&tx, &Location { bytes, lsn })
        })();
        if let Err(error) = result {
            report.fault = Some(ScanFault {
                lsn,
                reason: error.to_string(),
            });
            return Ok(report);
        }
        // 只有连续通过校验的事实才推进 last_lsn：它代表「已验证的前缀」。
        report.last_lsn = lsn;
    }
    Ok(report)
}

/// 同步目录本身。
///
/// 目录项（新建文件、重命名）也要落盘：只 fsync 文件不 fsync 目录，崩溃后
/// 可能看不到已经「创建成功」的文件。
pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

/// 以私有权限打开或新建文件（0600），并拒绝符号链接目标。
///
/// 拒绝软链是为了防止「把权威文件指向别处」：符号链接的检查必须在打开
/// 之前做，否则跟随链接就晚了。
pub(crate) fn private_file(path: &Path, create_new: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).append(true).create_new(create_new);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    // Do not accept symlink destinations (including a replaced active segment).
    if fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(invalid("symlink journal file"));
    }
    Ok(options.open(path)?)
}

/// 创建（或校正）数据目录的权限，并持久化新建目录项。
pub(crate) fn private_dir(path: &Path) -> Result<()> {
    private_dir_recorded(path, &mut SyncLatency::default())
}
/// 递归创建数据目录，并把每个新建祖先的目录项同步落盘。
///
/// 逐级创建并逐级 fsync 父目录：`create_dir_all` 之后只同步直接父目录是
/// 不够的——嵌套的中间目录项同样可能丢失。
fn private_dir_recorded(path: &Path, latency: &mut SyncLatency) -> Result<()> {
    if fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(invalid("symlink data directory"));
    }
    // Persist each new ancestor's directory entry, including nested fresh roots.
    // create_dir_all followed by syncing only the immediate parent is insufficient.
    if !path.exists() {
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if !parent.exists() {
            private_dir_recorded(parent, latency)?;
        }
        #[cfg_attr(not(unix), allow(unused_mut))]
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        match builder.create(path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // 并发下目录可能已被别人建好：只有确实是目录（且不是软链）
                // 才能接受，否则宁可失败。
                if fs::symlink_metadata(path)?.file_type().is_symlink() || !path.is_dir() {
                    return Err(invalid("invalid data directory"));
                }
            }
            Err(e) => return Err(e.into()),
        }
        let started = std::time::Instant::now();
        sync_dir(parent)?;
        latency.record(started.elapsed());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Fault hooks are opt-in and deterministic; all paths use the same production writer.
///
/// 故障注入开关：只在测试与演练中打开，所有注入点都走生产代码路径，不存在
/// 「只在注入时才生效」的旁路逻辑。
#[derive(Debug, Clone, Copy, Default)]
pub struct FaultInjection {
    pub fail_after_bytes: Option<u64>,
    pub fail_data_sync: Option<u64>,
    pub fail_dir_sync: Option<u64>,
    pub sync_delay_ms: u64,
    /// Deterministic crash barrier after all appends and before COMMIT.
    pub before_data_sync: Option<fn(u64)>,
}

/// Fixed logarithmic microsecond buckets: bucket i contains durations <= 2^i us,
/// with the last bucket also collecting longer calls. Includes directory initialization.
///
/// 固定的对数微秒直方图：第 i 个桶收集 <= 2^i 微秒的样本，最后一个桶兜底
/// 更长的调用。分桶固定，便于跨机器比较 fsync 延迟分布。
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct SyncLatency {
    pub samples: u64,
    pub total_us: u64,
    pub max_us: u64,
    pub buckets: [u64; 24],
}
impl SyncLatency {
    /// 记录一次同步耗时。
    pub(crate) fn record(&mut self, elapsed: std::time::Duration) {
        let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let bucket = (64 - us.max(1).saturating_sub(1).leading_zeros() as usize).min(23);
        self.samples += 1;
        self.total_us = self.total_us.saturating_add(us);
        self.max_us = self.max_us.max(us);
        self.buckets[bucket] += 1;
    }
}
