//! Owned process trees. Executors own sessions; each harness owns a process group.
//! Windows jobs are assigned while the child is suspended, before user code runs.
use std::{
    io,
    ops::{Deref, DerefMut},
    process::ExitStatus,
};
use tokio::process::{Child, Command};

pub struct ManagedChild {
    child: Child,
    tree: Option<ProcessTree>,
}

impl ManagedChild {
    pub fn spawn_executor(command: &mut Command) -> io::Result<Self> {
        Self::spawn(command, true)
    }

    pub fn spawn_harness(command: &mut Command) -> io::Result<Self> {
        Self::spawn(command, false)
    }

    fn spawn(command: &mut Command, session: bool) -> io::Result<Self> {
        command.kill_on_drop(true);
        #[cfg(unix)]
        if session {
            // Only an async-signal-safe syscall runs after fork.
            unsafe {
                command.pre_exec(|| {
                    if libc::setsid() < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        } else {
            command.process_group(0);
        }
        #[cfg(windows)]
        command.creation_flags(windows_sys::Win32::System::Threading::CREATE_SUSPENDED);
        let mut child = command.spawn()?;
        let tree = match ProcessTree::attach(&child, session) {
            Ok(tree) => tree,
            Err(error) => {
                let _ = child.start_kill();
                return Err(error);
            }
        };
        Ok(Self {
            child,
            tree: Some(tree),
        })
    }

    pub fn start_kill(&mut self) -> io::Result<()> {
        self.tree.take();
        self.child.start_kill()
    }

    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        let status = self.child.wait().await?;
        self.tree.take();
        Ok(status)
    }

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
    fn deref(&self) -> &Child {
        &self.child
    }
}
impl DerefMut for ManagedChild {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}
impl Drop for ManagedChild {
    fn drop(&mut self) {
        self.tree.take();
    }
}

#[cfg(unix)]
struct ProcessTree {
    pid: i32,
    session: bool,
}
#[cfg(unix)]
impl ProcessTree {
    fn attach(child: &Child, session: bool) -> io::Result<Self> {
        Ok(Self {
            pid: child
                .id()
                .ok_or_else(|| io::Error::other("child already exited"))? as i32,
            session,
        })
    }
}

#[cfg(unix)]
impl Drop for ProcessTree {
    fn drop(&mut self) {
        if self.session {
            // Harnesses have their own groups within this executor session.
            // Stop discovered groups before killing them, including a harness
            // whose executor was SIGKILLed and cannot run its local Drop guard.
            let mut groups = std::collections::BTreeSet::from([self.pid]);
            unsafe {
                libc::kill(-self.pid, libc::SIGSTOP);
            }
            for _ in 0..4 {
                let pids = match session_pids() {
                    Ok(pids) => pids,
                    Err(error) => {
                        tracing::error!(%error, "cannot enumerate executor session for cleanup");
                        break;
                    }
                };
                let mut added = false;
                for pid in pids {
                    unsafe {
                        if libc::getsid(pid) == self.pid {
                            let group = libc::getpgid(pid);
                            if group > 0 && groups.insert(group) {
                                libc::kill(-group, libc::SIGSTOP);
                                added = true;
                            }
                        }
                    }
                }
                if !added {
                    break;
                }
            }
            for group in groups {
                unsafe {
                    libc::kill(-group, libc::SIGKILL);
                }
            }
        } else {
            unsafe {
                libc::kill(-self.pid, libc::SIGKILL);
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn session_pids() -> io::Result<Vec<i32>> {
    Ok(std::fs::read_dir("/proc")?
        .filter_map(|e| e.ok()?.file_name().to_str()?.parse().ok())
        .collect())
}
#[cfg(target_os = "macos")]
fn session_pids() -> io::Result<Vec<i32>> {
    unsafe {
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
        pids.truncate(n as usize);
        Ok(pids)
    }
}
#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn session_pids() -> io::Result<Vec<i32>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "executor session cleanup requires Linux or macOS",
    ))
}

#[cfg(windows)]
struct ProcessTree {
    _job: std::os::windows::io::OwnedHandle,
}
#[cfg(windows)]
impl ProcessTree {
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

#[cfg(not(any(unix, windows)))]
struct ProcessTree;
#[cfg(not(any(unix, windows)))]
impl ProcessTree {
    fn attach(_: &Child, _: bool) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "managed process trees require Unix or Windows",
        ))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use flow_test_support::io::TempDir;
    use std::{process::Stdio, time::Duration};
    use tokio::io::{AsyncBufReadExt, BufReader};

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
