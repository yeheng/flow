//! 子进程句柄与进程树回收。
//!
//! 不变量：**每个被 spawn 的子进程都必须被 reap**。tokio 的
//! `kill_on_drop` 只保证「句柄被丢弃时子进程被杀」，它不保证进程被
//! 回收（wait），也不保证子进程的**子进程**被杀。
//!
//! 平台差异（诚实声明，别当已解决的）：
//!
//! - unix：子进程 `setsid` 自成会话（会话首进程即进程组组长），整棵
//!   进程树用一次 `kill(-pgid, SIGKILL)` 收掉。
//! - Windows：目前只保证「直接子进程被杀并回收」。**孙进程不在保证
//!   范围内**——Job Object 托管是已知缺口（见 crate 文档），在没有
//!   Windows CI 测试信号之前不写假装安全的代码。

use std::io;
use std::process::ExitStatus;

use tokio::process::Child;

/// 被托管的子进程句柄。
pub struct ChildGuard {
    child: Child,
    /// unix：进程组 id（等于子进程 pid：它是 `setsid` 出来的会话首
    /// 进程）。Windows / 未开 `new_session` 时为 `None`，只杀直接子进程。
    #[cfg(unix)]
    pgid: Option<i32>,
}

impl ChildGuard {
    pub(crate) fn new(child: Child, #[cfg(unix)] pgid: Option<i32>) -> Self {
        Self {
            child,
            #[cfg(unix)]
            pgid,
        }
    }

    /// 子进程 pid（已退出则为 `None`）。
    pub fn id(&self) -> Option<u32> {
        self.child.id()
    }

    /// 杀掉整棵进程树（unix：杀进程组；其余：杀直接子进程）。
    ///
    /// 幂等：进程已经死了不算错误。
    pub fn kill_tree(&mut self) -> io::Result<()> {
        #[cfg(unix)]
        if let Some(pgid) = self.pgid {
            // SAFETY: kill 是异步信号安全的系统调用；负 pid 表示进程组。
            // 进程组可能已经消失（ESRCH），此时忽略——下面的 start_kill
            // 会给出权威结论。
            unsafe {
                libc::kill(-pgid, libc::SIGKILL);
            }
        }
        match self.child.start_kill() {
            Ok(()) => Ok(()),
            // 进程已经退出：不是错误。
            Err(error) if error.kind() == io::ErrorKind::InvalidInput => Ok(()),
            Err(error) => Err(error),
        }
    }

    /// 等待子进程结束并回收（reap）。
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        self.child.wait().await
    }

    /// 非阻塞查看子进程状态。
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        // 兜底：句柄被丢弃时至少把进程组收掉（tokio 的 kill_on_drop
        // 负责直接子进程）。这里不能 await，因此只做尽力而为。
        #[cfg(unix)]
        if let Some(pgid) = self.pgid {
            // SAFETY: 见 kill_tree。
            unsafe {
                libc::kill(-pgid, libc::SIGKILL);
            }
        }
    }
}
