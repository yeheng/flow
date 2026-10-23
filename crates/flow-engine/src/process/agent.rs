//! Last-resort guard for an inline agent in a disposable executor. This thread
//! does not depend on Tokio, model progress, or heartbeats. Session sampling is
//! an operational budget, not an OS security sandbox or an allocation hard cap.
use std::{
    io,
    sync::mpsc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

const GRACE: Duration = Duration::from_millis(crate::execution_protocol::contract::DRAIN_GRACE_MS);
const SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
const MEMORY_BYTES: u64 = 1024 * 1024 * 1024;
const PROCESSES: usize = 64;
#[cfg(unix)]
const FILE_BYTES: u64 = 256 * 1024 * 1024;

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
            limit.rlim_cur = limit.rlim_cur.min(FILE_BYTES as libc::rlim_t);
            limit.rlim_max = limit.rlim_max.min(FILE_BYTES as libc::rlim_t);
            if libc::setrlimit(libc::RLIMIT_FSIZE, &limit) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        let (stop, receiver) = mpsc::channel();
        let deadline = Instant::now() + timeout + GRACE;
        let thread = std::thread::Builder::new()
            .name("agent-watchdog".into())
            .spawn(move || {
                let mut cancelled_at = None;
                loop {
                    match receiver.recv_timeout(SAMPLE_INTERVAL) {
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    if cancel.is_cancelled() {
                        cancelled_at.get_or_insert_with(Instant::now);
                    }
                    let reason = if Instant::now() >= deadline {
                        Some("agent lifecycle hard deadline exceeded")
                    } else if cancelled_at.is_some_and(|at: Instant| at.elapsed() >= GRACE) {
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
                                eprintln!("flow-executor: agent budget inspection failed: {error}");
                                Some("agent resource budget could not be verified")
                            }
                        }
                    };
                    if let Some(reason) = reason {
                        eprintln!("flow-executor: {reason}; retiring executor session");
                        // Do not wait for host destructors or a stuck Tokio runtime.
                        // The owning ManagedChild reaps the complete session/job.
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
    fn drop(&mut self) {
        self.stop.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn session_usage() -> io::Result<(u64, usize)> {
    let session = unsafe { libc::getsid(0) };
    if session < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut memory = 0u64;
    let mut processes = 0;
    for pid in super::session_pids()?.into_iter().filter(|pid| *pid > 0) {
        if unsafe { libc::getsid(pid) } != session {
            continue;
        }
        match resident_bytes(pid) {
            Ok(bytes) => {
                memory = memory.saturating_add(bytes);
                processes += 1;
            }
            // Processes can exit between enumeration and inspection.
            Err(error) if unsafe { libc::getsid(pid) } != session => {
                let _ = error;
            }
            Err(error) => return Err(error),
        }
    }
    Ok((memory, processes))
}

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
    if read != size {
        return Err(io::Error::last_os_error());
    }
    Ok(info.pti_resident_size)
}

// Other platforms retain the independent lifecycle watchdog. Production
// resource containment there must be supplied by the worker job/container.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn session_usage() -> io::Result<(u64, usize)> {
    Ok((0, 0))
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use crate::process::ManagedChild;

    // Run only inside a new managed child session: the watchdog deliberately
    // exits the process and applies non-restorable file limits.
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

    #[tokio::test]
    async fn blocked_host_is_stopped_by_independent_deadline() {
        let elapsed = run_child("deadline").await;
        assert!(
            elapsed >= GRACE,
            "must stop on the deadline, not an unrelated inspection error"
        );
        assert!(elapsed < Duration::from_secs(8));
    }

    #[tokio::test]
    async fn tool_processes_are_counted_across_the_executor_session() {
        assert!(run_child("processes").await < GRACE);
    }
}
