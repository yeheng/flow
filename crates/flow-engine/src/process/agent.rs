//! Last-resort guard for an inline agent in a disposable executor. This thread
//! does not depend on Tokio, model progress, or heartbeats. Session sampling is
//! an operational budget, not an OS security sandbox or an allocation hard cap.
//!
//! 本模块是**内联 agent 的最后一道看门狗**：独立于 Tokio 的线程盯着执行器会话，
//! 超时或超预算即结束执行器进程。
//! 不变量：不依赖运行时进展（宿主堵死 runtime 时协作式超时与心跳都失效）；
//! 触发时直接 exit(75)（此时进程多半已不响应清理）；会话采样是运维预算而非
//! 安全沙箱（真正的隔离要靠 job/container）。
//! 不这样做的代价：模型或工具死循环时宿主整体卡住，执行器永不回收，端口、
//! 内存与沙箱目录一起泄漏。
use std::{
    io,
    sync::mpsc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

/// 取消后允许的收尾宽限：超过它就认为 agent 不肯停。
const GRACE: Duration = Duration::from_millis(crate::execution_protocol::contract::DRAIN_GRACE_MS);
/// 采样间隔：太密会烧 CPU，太疏则错过突发。
const SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
/// 会话常驻内存预算（RSS 合计）。
const MEMORY_BYTES: u64 = 1024 * 1024 * 1024;
/// 会话进程数预算。
const PROCESSES: usize = 64;
/// 单文件写入上限（RLIMIT_FSIZE）。
#[cfg(unix)]
const FILE_BYTES: u64 = 256 * 1024 * 1024;

/// 看门狗句柄：Drop 即停线程。
pub struct AgentWatchdog {
    stop: Option<mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl AgentWatchdog {
    /// Call only in a managed, one-shot executor, after authorization. File-size
    /// limits are inherited by tools and intentionally never restored: this
    /// process must not be reused. Aggregate RSS/PID budgets include all groups
    /// in the executor session on Linux/macOS; detached sessions need an OS sandbox.
    pub fn start(timeout: Duration, cancel: CancellationToken) -> io::Result<Self> {
        #[cfg(unix)]
        unsafe {
            let mut limit: libc::rlimit = std::mem::zeroed();
            if libc::getrlimit(libc::RLIMIT_FSIZE, &mut limit) != 0 {
                return Err(io::Error::last_os_error());
            }
            // 取 min：不能抬高系统给的硬上限，只允许收紧。
            limit.rlim_cur = limit.rlim_cur.min(FILE_BYTES as libc::rlim_t);
            limit.rlim_max = limit.rlim_max.min(FILE_BYTES as libc::rlim_t);
            // 收紧后不再恢复：子进程（工具）继承这个限制，而执行器本身
            // 是一次性的，恢复对已派生的工具有害无益。
            if libc::setrlimit(libc::RLIMIT_FSIZE, &limit) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        let (stop, receiver) = mpsc::channel();
        // 硬截止 = 业务超时 + 宽限：取消后仍留出收尾时间。
        let deadline = Instant::now() + timeout + GRACE;
        let thread = std::thread::Builder::new()
            .name("agent-watchdog".into())
            .spawn(move || {
                // 记录首次观察到取消的时刻：取消可能发生在两次采样之间。
                let mut cancelled_at = None;
                loop {
                    match receiver.recv_timeout(SAMPLE_INTERVAL) {
                        // 停止信号或通道断开：正常退出路径。
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    if cancel.is_cancelled() {
                        cancelled_at.get_or_insert_with(Instant::now);
                    }
                    let reason = if Instant::now() >= deadline {
                        // 硬截止优先：它与业务超时无关，是最后的安全网。
                        Some("agent lifecycle hard deadline exceeded")
                    } else if cancelled_at.is_some_and(|at: Instant| at.elapsed() >= GRACE) {
                        // 取消后过了宽限仍未退出：agent 不肯停。
                        Some("agent did not stop after cancellation")
                    } else {
                        match session_usage() {
                            Ok((memory, processes))
                                if memory > MEMORY_BYTES || processes > PROCESSES =>
                            {
                                Some("agent session exceeded RSS or process budget")
                            }
                            Ok(_) => None,
                            Err(error) => {
                                // 采样失败按「无法验证」处理：宁可误杀，也
                                // 不让一个失控的会话继续占资源。
                                eprintln!("flow-executor: agent budget inspection failed: {error}");
                                Some("agent resource budget could not be verified")
                            }
                        }
                    };
                    if let Some(reason) = reason {
                        eprintln!("flow-executor: {reason}; retiring executor session");
                        // Do not wait for host destructors or a stuck Tokio runtime.
                        // The owning ManagedChild reaps the complete session/job.
                        // 直接退出会跳过所有 Drop：此刻进程可能已经不响应
                        // 任何清理逻辑，整棵进程树由宿主侧的 ManagedChild 收。
                        std::process::exit(75);
                    }
                }
            })?;
        Ok(Self {
            stop: Some(stop),
            thread: Some(thread),
        })
    }
}

impl Drop for AgentWatchdog {
    /// 停看门狗线程并等待它退出：丢弃 stop 发送端即让接收端在下一个
    /// 采样周期内感知断开并正常返回。
    fn drop(&mut self) {
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// 采样执行器会话的常驻内存合计与进程数。
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn session_usage() -> io::Result<(u64, usize)> {
    // 自己所在的 session 即执行器会话：执行器被 setsid 领起，是该 session 的组长。
    let session = unsafe { libc::getsid(0) };
    if session < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut memory = 0u64;
    let mut processes = 0;
    for pid in super::session_pids()?.into_iter().filter(|pid| *pid > 0) {
        // 只统计同 session 的进程：别的 session 不归这次执行负责。
        if unsafe { libc::getsid(pid) } != session {
            continue;
        }
        match resident_bytes(pid) {
            Ok(bytes) => {
                // saturating_add：采样期间的累计不能因溢出回绕成小值。
                memory = memory.saturating_add(bytes);
                processes += 1;
            }
            // Processes can exit between enumeration and inspection.
            // 枚举与采样之间进程退出了：重新确认一次归属，确属本会话
            // 才算采样失败（否则只是它恰好退出了）。
            Err(error) if unsafe { libc::getsid(pid) } != session => {
                let _ = error;
            }
            Err(error) => return Err(error),
        }
    }
    Ok((memory, processes))
}

/// 读某个进程的常驻内存（linux：/proc/<pid>/statm 的 resident 页数）。
#[cfg(target_os = "linux")]
fn resident_bytes(pid: i32) -> io::Result<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/statm"))?;
    let pages = stat
        .split_whitespace()
        .nth(1)
        .and_then(|n| n.parse::<u64>().ok())
        .ok_or_else(|| io::Error::other("invalid process RSS"))?;
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(pages.saturating_mul(page_size as u64))
}

/// 读某个进程的常驻内存（macOS：proc_pidinfo 的 pti_resident_size）。
#[cfg(target_os = "macos")]
fn resident_bytes(pid: i32) -> io::Result<u64> {
    let mut info: libc::proc_taskinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of_val(&info) as i32;
    let read = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTASKINFO,
            0,
            (&mut info as *mut libc::proc_taskinfo).cast(),
            size,
        )
    };
    // 返回长度不符说明这次采样没拿到完整数据：当成失败而不是半份数据。
    if read != size {
        return Err(io::Error::last_os_error());
    }
    Ok(info.pti_resident_size)
}

// Other platforms retain the independent lifecycle watchdog. Production
// resource containment there must be supplied by the worker job/container.
/// 其他平台：只保留生命周期看门狗，资源用量按 0 计（永不会因预算触发）。
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn session_usage() -> io::Result<(u64, usize)> {
    Ok((0, 0))
}

/// 看门狗行为的测试。
#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use crate::process::ManagedChild;

    // Run only inside a new managed child session: the watchdog deliberately
    // exits the process and applies non-restorable file limits.
    /// 夹具子进程：扮演被看门狗盯住的宿主（会阻塞在 sleep 上）。
    #[test]
    fn watchdog_child() {
        let Ok(mode) = std::env::var("FLOW_WATCHDOG_TEST_MODE") else {
            return;
        };
        let timeout = if mode == "deadline" {
            Duration::from_millis(1)
        } else {
            Duration::from_secs(30)
        };
        let _guard = AgentWatchdog::start(timeout, CancellationToken::new()).unwrap();
        let _children: Vec<_> = if mode == "processes" {
            (0..PROCESSES)
                .map(|_| {
                    std::process::Command::new("sleep")
                        .arg("30")
                        .spawn()
                        .unwrap()
                })
                .collect()
        } else {
            Vec::new()
        };
        // No runtime, no await, no heartbeat: cooperative timeout cannot help.
        std::thread::sleep(Duration::from_secs(30));
        panic!("watchdog failed to stop blocked host");
    }

    /// 在一个受管子进程里跑夹具，返回它从启动到退出的耗时。
    async fn run_child(mode: &str) -> Duration {
        let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "process::agent::tests::watchdog_child",
                "--nocapture",
            ])
            .env("FLOW_WATCHDOG_TEST_MODE", mode);
        let mut child = ManagedChild::spawn_executor(&mut command).unwrap();
        let started = Instant::now();
        let status = tokio::time::timeout(Duration::from_secs(9), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status.code(), Some(75));
        started.elapsed()
    }

    /// 宿主被堵死时，靠独立看门狗的硬截止把它停掉（退出码 75）。
    #[tokio::test]
    async fn blocked_host_is_stopped_by_independent_deadline() {
        let elapsed = run_child("deadline").await;
        assert!(
            elapsed >= GRACE,
            "must stop on the deadline, not an unrelated inspection error"
        );
        assert!(elapsed < Duration::from_secs(8));
    }

    /// 工具进程数要按整个执行器 session 统计，而不是只看直接子进程。
    #[tokio::test]
    async fn tool_processes_are_counted_across_the_executor_session() {
        assert!(run_child("processes").await < GRACE);
    }
}
