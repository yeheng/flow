//! Owned process trees. Executors own sessions; each harness owns a process group.
//! Windows jobs are assigned while the child is suspended, before user code runs.
//!
//! 本模块负责**子进程树托管**：spawn 时建立归属，结束时收干净整棵树。
//! 不变量：executor 独占 session、harness 独占进程组；清理要覆盖执行器
//! 被 SIGKILL 的情形（那时由父进程按 session 扫描收尸）；继承的 fd 必须在
//! exec 前收敛，只留显式声明的那一个。
//! 不这样做的代价：harness 的孙进程会活过「取消」，继续占端口、沙箱目录
//! 与凭据环境。
use std::{
    io,
    ops::{Deref, DerefMut},
    process::ExitStatus,
};
use tokio::process::{Child, Command};

#[cfg(unix)]
mod fds;
#[cfg(unix)]
pub(crate) use fds::close_inherited_fds;
mod agent;
pub use agent::AgentWatchdog;

/// 被托管的子进程句柄。
///
/// 持有 `ProcessTree` 守卫：句柄被丢弃时整棵进程树随之清理。
pub struct ManagedChild {
    child: Child,
    tree: Option<ProcessTree>,
}

impl ManagedChild {
    /// 以「执行器」身份 spawn：子进程独占一个 session。
    ///
    /// session 让执行器成为一整棵树的根，后续可以按 session 扫描收尸。
    pub fn spawn_executor(command: &mut Command) -> io::Result<Self> {
        Self::spawn(command, true)
    }

    /// 以「harness」身份 spawn：子进程在执行器 session 内独占一个进程组。
    ///
    /// harness 与 executor 的差别在清理粒度：harness 只需收自己的进程组，
    /// executor 才需要兜底整个 session。
    pub fn spawn_harness(command: &mut Command) -> io::Result<Self> {
        Self::spawn(command, false)
    }

    /// 公共 spawn 路径：按身份决定 session/进程组归属与 fd 收敛策略。
    fn spawn(command: &mut Command, session: bool) -> io::Result<Self> {
        // 子进程退出即杀：清理不能依赖调用方记得收尾。
        command.kill_on_drop(true);
        #[cfg(unix)]
        if session {
            // Only an async-signal-safe syscall runs after fork.
            unsafe {
                command.pre_exec(|| {
                    // setsid：新 session 的首进程即进程组组长，之后能按组收尸。
                    if libc::setsid() < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        } else {
            // harness：自成一个进程组，但不另起 session——它必须留在
            // executor 的 session 内，才能被 session 级兜底扫描看见。
            command.process_group(0);
            // 收敛继承的 fd：harness 只需要 argv 与标准流。
            close_inherited_fds(command, None);
        }
        #[cfg(windows)]
        // 挂起创建：必须在用户代码跑起来之前把它放进 Job，否则它能先逃出去。
        command.creation_flags(windows_sys::Win32::System::Threading::CREATE_SUSPENDED);
        let mut child = command.spawn()?;
        let tree = match ProcessTree::attach(&child, session) {
            Ok(tree) => tree,
            Err(error) => {
                // 归属建立失败就不能放行：否则这个子进程永远收不到清理。
                let _ = child.start_kill();
                return Err(error);
            }
        };
        Ok(Self {
            child,
            tree: Some(tree),
        })
    }

    /// 立刻杀掉子进程，并放弃进程树守卫。
    ///
    /// 守卫被取走：终止动作之后，整棵树的收尾由平台的 job/组语义负责。
    pub fn start_kill(&mut self) -> io::Result<()> {
        self.tree.take();
        self.child.start_kill()
    }

    /// 等待子进程结束；结束后释放进程树守卫（此时再清理已无意义）。
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        let status = self.child.wait().await?;
        self.tree.take();
        Ok(status)
    }

    /// 非阻塞查看子进程状态；已退出则释放进程树守卫。
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        let status = self.child.try_wait()?;
        if status.is_some() {
            self.tree.take();
        }
        Ok(status)
    }
}
impl Deref for ManagedChild {
    type Target = Child;
    /// 透传到内层 `Child`，调用方可以继续用标准库 API。
    fn deref(&self) -> &Child {
        &self.child
    }
}
impl DerefMut for ManagedChild {
    /// 可变透传到内层 `Child`（如取走 stdout 管道）。
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}
impl Drop for ManagedChild {
    /// 释放进程树守卫：整棵树的清理由 `ProcessTree::drop` 承担。
    fn drop(&mut self) {
        self.tree.take();
    }
}

/// unix 上的进程树守卫：记住 pid 与归属层级（session / 进程组）。
#[cfg(unix)]
struct ProcessTree {
    pid: i32,
    session: bool,
}
#[cfg(unix)]
impl ProcessTree {
    /// 记录子进程 pid，供清理时按组/按 session 发信号。
    fn attach(child: &Child, session: bool) -> io::Result<Self> {
        Ok(Self {
            // 子进程已经退出：说明它在 spawn 与此刻之间就结束了，
            // 无法再建立归属，只能报失败。
            pid: child
                .id()
                .ok_or_else(|| io::Error::other("child already exited"))? as i32,
            session,
        })
    }
}

#[cfg(unix)]
impl Drop for ProcessTree {
    /// 收掉整棵进程树。
    ///
    /// session 情形（executor）：先把发现的进程组 SIGSTOP 冻住再 SIGKILL，
    /// 否则进程在被杀的窗口里可能 fork 出新后代，扫描就漏了。
    fn drop(&mut self) {
        if self.session {
            // Harnesses have their own groups within this executor session.
            // Stop discovered groups before killing them, including a harness
            // whose executor was SIGKILLed and cannot run its local Drop guard.
            // 先冻住 executor 自己的组：它在被扫描期间不得继续 fork。
            let mut groups = std::collections::BTreeSet::from([self.pid]);
            unsafe {
                libc::kill(-self.pid, libc::SIGSTOP);
            }
            // 最多再发现 4 轮：组是逐层嵌套的，冻住一层才敢看下一层，
            // 轮数上限防止无限扫描拖住清理路径。
            for _ in 0..4 {
                let pids = match session_pids() {
                    Ok(pids) => pids,
                    Err(error) => {
                        // 枚举失败：只能退回到已知的那几组，不能假装已经扫全。
                        tracing::error!(%error, "cannot enumerate executor session for cleanup");
                        break;
                    }
                };
                let mut added = false;
                for pid in pids {
                    unsafe {
                        // 只收同 session 的进程：别的 session 不归我们管。
                        if libc::getsid(pid) == self.pid {
                            let group = libc::getpgid(pid);
                            if group > 0 && groups.insert(group) {
                                // 先冻住新发现的组，避免它在被 kill 前分裂。
                                libc::kill(-group, libc::SIGSTOP);
                                added = true;
                            }
                        }
                    }
                }
                if !added {
                    // 没有新组了：再扫也是同一集合，停止。
                    break;
                }
            }
            // 冻结之后统一 SIGKILL：冻住的进程不会在被杀前 fork 出新后代。
            for group in groups {
                unsafe {
                    libc::kill(-group, libc::SIGKILL);
                }
            }
        } else {
            // harness：只杀自己的进程组（pid 取负 = 整组）。
            unsafe {
                libc::kill(-self.pid, libc::SIGKILL);
            }
        }
    }
}

/// 列出系统当前所有 pid（linux：读 /proc 目录项）。
#[cfg(target_os = "linux")]
fn session_pids() -> io::Result<Vec<i32>> {
    Ok(std::fs::read_dir("/proc")?
        .filter_map(|e| e.ok()?.file_name().to_str()?.parse().ok())
        .collect())
}
/// 列出系统当前所有 pid（macOS：proc_listallpids）。
#[cfg(target_os = "macos")]
fn session_pids() -> io::Result<Vec<i32>> {
    unsafe {
        // 先用 null 查数量：两次调用之间进程数会变，多留 1024 的余量。
        let count = libc::proc_listallpids(std::ptr::null_mut(), 0);
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut pids = vec![0i32; count as usize + 1024];
        let n = libc::proc_listallpids(
            pids.as_mut_ptr().cast(),
            std::mem::size_of_val(pids.as_slice()) as i32,
        );
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        // 按实际返回数截断：缓冲区尾部是未填充的 0。
        pids.truncate(n as usize);
        Ok(pids)
    }
}
/// 其他 unix 平台不支持枚举进程：session 级清理退化为只杀已知组。
#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn session_pids() -> io::Result<Vec<i32>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "executor session cleanup requires Linux or macOS",
    ))
}

/// Windows 上的进程树守卫：Job 对象，句柄关闭即杀掉全部成员。
#[cfg(windows)]
struct ProcessTree {
    _job: std::os::windows::io::OwnedHandle,
}
#[cfg(windows)]
impl ProcessTree {
    /// 创建 Job 对象并把子进程放进去。
    ///
    /// 必须在子进程仍处于挂起状态时完成归属：用户代码一旦跑起来，
    /// 它可能先 spawn 出逃出 Job 的后代，那时再入 Job 已经来不及。
    fn attach(child: &Child, _session: bool) -> io::Result<Self> {
        use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
        use windows_sys::Win32::{
            Foundation::INVALID_HANDLE_VALUE,
            System::{Diagnostics::ToolHelp::*, JobObjects::*, Threading::*},
        };
        unsafe {
            let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if job.is_null() {
                return Err(io::Error::last_os_error());
            }
            let job = OwnedHandle::from_raw_handle(job);
            let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            // KILL_ON_JOB_CLOSE：句柄一掉，全部成员随进程树一起消失。
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if SetInformationJobObject(
                job.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of_val(&limits) as u32,
            ) == 0
                || AssignProcessToJobObject(
                    job.as_raw_handle(),
                    child
                        .raw_handle()
                        .ok_or_else(|| io::Error::other("child handle unavailable"))?,
                ) == 0
            {
                return Err(io::Error::last_os_error());
            }
            let tree = Self { _job: job };
            // CREATE_SUSPENDED guarantees that there is only the initial thread.
            let snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
            if snapshot == INVALID_HANDLE_VALUE {
                return Err(io::Error::last_os_error());
            }
            let snapshot = OwnedHandle::from_raw_handle(snapshot);
            let mut thread: THREADENTRY32 = std::mem::zeroed();
            thread.dwSize = std::mem::size_of_val(&thread) as u32;
            let mut found = Thread32First(snapshot.as_raw_handle(), &mut thread);
            while found != 0 {
                if Some(thread.th32OwnerProcessID) == child.id() {
                    let handle = OpenThread(THREAD_SUSPEND_RESUME, 0, thread.th32ThreadID);
                    if handle.is_null() {
                        return Err(io::Error::last_os_error());
                    }
                    let handle = OwnedHandle::from_raw_handle(handle);
                    // 恢复初始线程：用户代码此刻才第一次获得运行机会。
                    if ResumeThread(handle.as_raw_handle()) == u32::MAX {
                        return Err(io::Error::last_os_error());
                    }
                    return Ok(tree);
                }
                found = Thread32Next(snapshot.as_raw_handle(), &mut thread);
            }
            Err(io::Error::other("suspended child thread unavailable"))
        }
    }
}

/// 其他平台的占位守卫：不提供进程树清理。
#[cfg(not(any(unix, windows)))]
struct ProcessTree;
#[cfg(not(any(unix, windows)))]
impl ProcessTree {
    /// 直接报错：不能托管进程树的平台不能假装支持。
    fn attach(_: &Child, _: bool) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "managed process trees require Unix or Windows",
        ))
    }
}

/// 进程树清理的不变量测试。
#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use flow_test_support::io::TempDir;
    use std::{process::Stdio, time::Duration};
    use tokio::io::{AsyncBufReadExt, BufReader};

    /// 等待子进程打出就绪标记：早于它启动后台子进程，测试才有意义。
    async fn ready(child: &mut ManagedChild) {
        let mut out = BufReader::new(child.stdout.take().unwrap()).lines();
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(line) = out.next_line().await.unwrap() {
                if line == "tree-ready" {
                    return;
                }
            }
            panic!("child exited before ready");
        })
        .await
        .unwrap();
    }

    /// 杀 harness 或直接丢弃句柄，其后代都必须被清掉。
    #[tokio::test]
    async fn killing_or_dropping_harness_stops_its_descendants() {
        for drop_only in [false, true] {
            let dir = TempDir::new("harness-tree");
            let marker = dir.join("escaped");
            let script = format!(
                "(sleep 0.5; touch '{}') & printf 'tree-ready\\n'; wait",
                marker.display()
            );
            let mut child = ManagedChild::spawn_harness(
                Command::new("/bin/sh")
                    .args(["-c", &script])
                    .stdout(Stdio::piped()),
            )
            .unwrap();
            ready(&mut child).await;
            if !drop_only {
                child.start_kill().unwrap();
                child.wait().await.unwrap();
            }
            drop(child);
            tokio::time::sleep(Duration::from_millis(700)).await;
            assert!(!marker.exists(), "descendant survived harness cleanup");
        }
    }

    // A separate test process models an executor killed before it can run Drop.
    /// 夹具子进程：扮演「被 SIGKILL 的执行器」，它的后代会晚于它存在。
    #[test]
    fn session_fixture() {
        let Some(marker) = std::env::var_os("FLOW_TREE_FIXTURE") else {
            return;
        };
        use std::os::unix::process::CommandExt;
        let script = format!(
            "(sleep 0.5; touch '{}') & printf 'tree-ready\\n'; wait",
            std::path::Path::new(&marker).display()
        );
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", &script])
            .process_group(0)
            .spawn()
            .unwrap();
        let _ = child.wait();
    }

    /// 执行器被 SIGKILL（Drop 不跑）时，父进程仍要靠 session 扫描收掉后代。
    #[tokio::test]
    async fn reaping_a_dead_executor_stops_other_groups_in_its_session() {
        let dir = TempDir::new("executor-session");
        let marker = dir.join("escaped");
        let mut child = ManagedChild::spawn_executor(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "process::tests::session_fixture", "--nocapture"])
                .env("FLOW_TREE_FIXTURE", &marker)
                .stdout(Stdio::piped()),
        )
        .unwrap();
        ready(&mut child).await;
        unsafe {
            assert_eq!(libc::kill(child.id().unwrap() as i32, libc::SIGKILL), 0);
        }
        child.wait().await.unwrap();
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(!marker.exists(), "descendant survived executor death");
    }
}
