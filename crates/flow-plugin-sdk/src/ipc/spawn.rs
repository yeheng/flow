//! spawn 子进程并建立字节通道。
//!
//! 两种承载：
//!
//! - **unix**：socketpair 一次性造出两端；子端 fd 号经环境变量
//!   `PROC_IPC_FD` 交给子进程；其余继承 fd 一律 CLOEXEC 收敛。通道凭据
//!   就是这个 fd（能力即凭据），**不需要令牌**。
//! - **其余平台**：先建本地 socket 监听（named pipe），把地址经
//!   `PROC_IPC_ADDR` 交给子进程，子进程回连出唯一一条连接；名字本机
//!   可见，因此**必须**带 `PROC_IPC_TOKEN` 令牌在握手时回验。
//!
//! 两条路径对上层完全等价：都返回 [`Session`]，都有通道、有句柄、
//! 都能一站式回收。

use std::ffi::OsString;
use std::io;
use std::path::PathBuf;

use crate::ipc::challenge::Challenge;
use crate::ipc::child::ChildGuard;
use crate::ipc::env;
use crate::Channel;

/// 回连等待上限（毫秒）。子进程起不来（缺二进制、秒退）时靠它收口，
/// 绝不允许宿主无限等待一个不会来的连接。
pub const CONNECT_TIMEOUT_MS: u64 = 10_000;

/// unix 承载的通道固定 fd 号：pre_exec 里把通道 dup2 到这个号。
///
/// 固定号的价值：子进程看到的号不随宿主 fd 表布局变化（宿主关 stdin、
/// 多线程 fork、并发 spawn 都不会再改变这个号），跨语言实现也只需记住
/// 一个常量。stdio 占 0..=2，3 是最低的安全号。
pub const CHANNEL_FD: i32 = 3;

/// spawn 参数。
#[derive(Debug, Clone, Default)]
pub struct SpawnConfig {
    /// 可执行程序（路径或名字）。
    pub program: PathBuf,
    /// 命令行参数（不含程序名）。
    pub args: Vec<OsString>,
    /// 显式环境变量白名单。**宿主环境不继承**：凭据类变量不会漏给子进程。
    pub env: Vec<(OsString, OsString)>,
    /// 会话令牌。非 unix 承载必须给（名字可见）；unix 承载不给
    /// （fd 即能力）。
    pub challenge: Option<Challenge>,
    /// unix：是否让子进程 `setsid` 自成会话（进程组组长），以便整棵
    /// 进程树一次性回收。默认 `true`。
    pub new_session: bool,
}

impl SpawnConfig {
    /// 新建 spawn 参数（默认自成会话）。
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            new_session: true,
            ..Self::default()
        }
    }

    /// 追加一个环境变量。
    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }
}

/// 一次子进程会话：一条字节通道 + 一个被托管的子进程。
pub struct Session {
    channel: Channel,
    child: ChildGuard,
}

impl Session {
    /// 拉起子进程并建立通道。
    ///
    /// # Errors
    /// 进程创建失败、通道建立失败、回连超时或失败时返回 I/O 错误；失败
    /// 路径上子进程一定被杀掉并回收（不留孤儿）。
    pub async fn spawn(config: SpawnConfig) -> io::Result<Self> {
        #[cfg(unix)]
        {
            spawn_unix(config).await
        }
        #[cfg(not(unix))]
        {
            spawn_loopback(config).await
        }
    }

    /// 通道（只读引用）。
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// 通道（可变引用）。
    pub fn channel_mut(&mut self) -> &mut Channel {
        &mut self.channel
    }

    /// 子进程 pid。
    pub fn child_id(&self) -> Option<u32> {
        self.child.id()
    }

    /// 关闭会话：杀掉整棵进程树并回收子进程。
    ///
    /// 这是**唯一**能保证「不留僵尸」的显式收尾路径；直接丢弃 `Session`
    /// 只能做到「尽力杀掉」（drop 里不能 await）。
    pub async fn shutdown(self) -> io::Result<std::process::ExitStatus> {
        let Session { mut child, .. } = self;
        child.kill_tree()?;
        child.wait().await
    }
}

/// unix 承载：socketpair + fd 号经环境变量交接。
#[cfg(unix)]
async fn spawn_unix(config: SpawnConfig) -> io::Result<Session> {
    use std::os::fd::AsRawFd;

    let (parent, child_end) = crate::ipc::channel::pair()?;
    let child_fd = child_end.as_raw_fd();

    let mut command = tokio::process::Command::new(&config.program);
    command.args(&config.args);
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit());
    command.kill_on_drop(true);
    // 环境隔离：子进程只应看到显式白名单里的变量。
    command.env_clear();
    for (key, value) in &config.env {
        command.env(key, value);
    }
    command.env(env::FD_ENV, CHANNEL_FD.to_string());
    if let Some(token) = &config.challenge {
        command.env(env::TOKEN_ENV, token.as_str());
    }
    // 子进程自成会话：整棵进程树可以被一次 kill(-pgid) 收掉。
    // setsid 是 async-signal-safe 的，可以在 pre_exec 里做。
    if config.new_session {
        // SAFETY: pre_exec 中只调用 async-signal-safe 的系统调用。
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() < 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    // fd 收敛 + 落位：通道 dup2 到固定号 3，其余继承 fd 一律 CLOEXEC。
    crate::ipc::fds::close_inherited_fds_to(&mut command, child_fd, CHANNEL_FD);

    let child = command.spawn()?;
    // 父进程侧必须丢掉子端副本：留着它会让父子同时持有同一端点，
    // EOF 语义会失真（父进程以为子进程死了，其实是自己没关）。
    drop(child_end);
    let pgid = if config.new_session {
        child.id().map(|pid| pid as i32)
    } else {
        None
    };
    Ok(Session {
        channel: parent,
        child: ChildGuard::new(child, pgid),
    })
}

/// 非 unix 承载：本地 socket 监听 + 子进程回连。
#[cfg(not(unix))]
async fn spawn_loopback(config: SpawnConfig) -> io::Result<Session> {
    use std::time::Duration;

    use interprocess::local_socket::traits::tokio::Listener as _;
    use interprocess::local_socket::{GenericNamespaced, ListenerOptions, ToNsName};

    let token = config
        .challenge
        .clone()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "回连承载必须提供会话令牌"))?;
    let unique = random_hex(16);
    let addr = format!(r"\\.\pipe\proc-ipc-{unique}");
    let name = addr.as_str().to_ns_name::<GenericNamespaced>()?;

    // listener 必须先于 spawn 建立：子进程可能秒级内就绪，晚了就会
    // 撞上「连接被拒绝」。
    let listener = ListenerOptions::new().name(name).create_tokio()?;

    let mut command = tokio::process::Command::new(&config.program);
    command.args(&config.args);
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit());
    command.kill_on_drop(true);
    command.env_clear();
    for (key, value) in &config.env {
        command.env(key, value);
    }
    command.env(env::ADDR_ENV, &addr);
    command.env(env::TOKEN_ENV, token.as_str());
    let mut child = command.spawn()?;
    let accept = async { listener.accept().await };
    match tokio::time::timeout(Duration::from_millis(CONNECT_TIMEOUT_MS), accept).await {
        Ok(Ok(stream)) => Ok(Session {
            channel: stream,
            child: ChildGuard::new(child),
        }),
        Ok(Err(error)) => {
            // 监听失败：杀掉子进程并收割，避免留下孤儿进程。
            let _ = child.start_kill();
            let _ = child.wait().await;
            Err(error)
        }
        Err(_) => {
            // 超时：子进程可能还活着并会永远等待一个不会到来的连接。
            let _ = child.start_kill();
            let _ = child.wait().await;
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "channel connect timeout",
            ))
        }
    }
}

/// 随机 hex 串（回连地址命名用，防止撞名/抢注）。
#[cfg(not(unix))]
fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    // 取不到随机源时退化为时间戳：地址唯一性退化为「大概率唯一」，
    // 抢连防御仍然由会话令牌承担。
    if getrandom::fill(&mut buf).is_err() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        buf[..8].copy_from_slice(&nanos.to_le_bytes());
    }
    hex::encode(buf)
}

#[cfg(all(test, unix))]
mod tests {
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    /// 子进程侧 helper：接管通道，读 4 字节，按约定回 4 字节。
    ///
    /// 回 `CLEAN` 表示没继承到宿主的环境变量、也没继承到宿主的 fd；
    /// 回 `LEAK` 表示泄漏了。被父进程以 `--exact` 拉起时才干活；作为
    /// 普通测试跑时什么都不做。
    #[tokio::test]
    async fn child_echo_helper() {
        let Some(fd) = crate::ipc::env::fd_from_env() else {
            return;
        };
        // env 告知的号上必须是 socket（dup2 落位正确的直接证据：如果通道
        // 还在原号而 env 说了 3，这里会拿到脏 fd 或非 socket）。
        if !fd_is_socket(fd) {
            eprintln!("fd {fd} is not a socket");
            std::process::exit(3);
        }
        let mut channel = crate::ipc::channel::adopt_raw_fd(fd).expect("adopt channel fd");
        let mut buf = [0u8; 5];
        channel.read_exact(&mut buf).await.expect("read request");
        let reply: &[u8; 5] = if leaked() { b"LEAK!" } else { b"CLEAN" };
        channel.write_all(reply).await.expect("write reply");
    }

    /// 子进程侧 helper：检查宿主给的探针 fd 号不在自己的 fd 表里。
    #[tokio::test]
    async fn child_fd_probe_helper() {
        let Some(fd) = crate::ipc::env::fd_from_env() else {
            return;
        };
        let leaked = probe_leaked();
        let mut channel = crate::ipc::channel::adopt_raw_fd(fd).expect("adopt channel fd");
        let mut buf = [0u8; 5];
        channel.read_exact(&mut buf).await.expect("read request");
        let reply: &[u8; 5] = if leaked { b"LEAK!" } else { b"CLEAN" };
        channel.write_all(reply).await.expect("write reply");
    }

    /// 父进程侧：拉起子进程，走通通道，然后回收。
    #[tokio::test]
    async fn spawn_unix_gives_a_working_channel_and_reaps_the_child() {
        let mut session = spawn_helper().await.expect("spawn");
        assert!(session.child_id().is_some(), "child pid must be visible");

        session
            .channel_mut()
            .write_all(b"hello")
            .await
            .expect("write request");
        let mut buf = [0u8; 5];
        tokio::time::timeout(
            Duration::from_secs(5),
            session.channel_mut().read_exact(&mut buf),
        )
        .await
        .expect("reply within timeout")
        .expect("read reply");
        assert_eq!(&buf, b"CLEAN", "子进程不得继承宿主环境变量");

        let status = session.shutdown().await.expect("reap child");
        assert!(!status.success(), "helper exits after the channel closes");
    }

    /// 父进程侧：宿主环境不泄漏给子进程。
    #[tokio::test]
    async fn spawn_does_not_leak_host_environment() {
        std::env::set_var("PROC_IPC_LEAK", "must-not-reach-child");
        let mut session = spawn_helper().await.expect("spawn");
        session
            .channel_mut()
            .write_all(b"hello")
            .await
            .expect("write request");
        let mut buf = [0u8; 5];
        tokio::time::timeout(
            Duration::from_secs(5),
            session.channel_mut().read_exact(&mut buf),
        )
        .await
        .expect("reply within timeout")
        .expect("read reply");
        assert_eq!(&buf, b"CLEAN", "宿主环境变量不得出现在子进程里");
        let _ = session.shutdown().await;
        std::env::remove_var("PROC_IPC_LEAK");
    }

    /// 父进程侧：除通道外，其余继承 fd 一律收敛（子进程看不到宿主的 fd）。
    #[tokio::test]
    async fn spawn_converges_inherited_descriptors() {
        use std::os::fd::AsRawFd;

        // 宿主创建一个唯一探针文件并保持打开：一个「不该被继承的 fd」。
        // 不用 /dev/null：stdin 就是 /dev/null，inode 相同会误判。
        let dir = std::env::temp_dir().join(format!("proc-ipc-probe-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let probe_path = dir.join("probe.txt");
        std::fs::write(&probe_path, b"probe").unwrap();
        let probe = std::fs::File::open(&probe_path).expect("open probe");
        let probe_fd = probe.as_raw_fd();

        let exe = std::env::current_exe().expect("test binary");
        let mut config = SpawnConfig::new(exe);
        config.args = helper_args("ipc::spawn::tests::child_fd_probe_helper");
        config.env.push((
            OsString::from("PROC_IPC_PROBE_FD"),
            OsString::from(probe_fd.to_string()),
        ));
        config.env.push((
            OsString::from("PROC_IPC_PROBE_PATH"),
            OsString::from(probe_path.to_string_lossy().to_string()),
        ));
        let mut session = Session::spawn(config).await.expect("spawn");
        session
            .channel_mut()
            .write_all(b"hello")
            .await
            .expect("write request");
        let mut buf = [0u8; 5];
        tokio::time::timeout(
            Duration::from_secs(5),
            session.channel_mut().read_exact(&mut buf),
        )
        .await
        .expect("reply within timeout")
        .expect("read reply");
        assert_eq!(&buf, b"CLEAN", "宿主 fd 不得出现在子进程里");
        let _ = session.shutdown().await;
        drop(probe);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 父进程侧：宿主 fd 表很脏（低号被占用）时，通道仍必须落在固定号 3。
    ///
    /// 这钉住 dup2 落位的价值：没有它，通道号取决于「fork 那一刻的 fd 表
    /// 布局」——宿主关 stdin、并发 spawn、守护进程化都会端着这个契约漂。
    #[tokio::test]
    async fn channel_lands_on_fixed_fd_even_with_dirty_parent_table() {
        // 故意占住 3..=7：迫使 socketpair 拿到更高的号，dup2 必须把它搬回 3。
        let mut junk = Vec::new();
        for _ in 0..5 {
            junk.push(std::fs::File::open("/dev/null").expect("open junk fd"));
        }
        let mut session = spawn_helper().await.expect("spawn");
        session
            .channel_mut()
            .write_all(b"hello")
            .await
            .expect("write request");
        let mut buf = [0u8; 5];
        tokio::time::timeout(
            Duration::from_secs(5),
            session.channel_mut().read_exact(&mut buf),
        )
        .await
        .expect("reply within timeout")
        .expect("read reply");
        assert_eq!(&buf, b"CLEAN");
        let _ = session.shutdown().await;
        drop(junk);
    }

    /// 拉起 helper 子进程（当前测试二进制扮演子进程）。
    async fn spawn_helper() -> io::Result<Session> {
        let exe = std::env::current_exe().expect("test binary");
        let mut config = SpawnConfig::new(exe);
        config.args = helper_args("ipc::spawn::tests::child_echo_helper");
        Session::spawn(config).await
    }

    /// fd 是否是 socket（fstat 判型）。
    fn fd_is_socket(fd: i32) -> bool {
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: fstat 只填充调用方提供的缓冲。
        if unsafe { libc::fstat(fd, &mut stat) } != 0 {
            return false;
        }
        (stat.st_mode & libc::S_IFMT) == libc::S_IFSOCK
    }

    /// 拉起 helper 子进程的启动参数：只跑一个测试，避免子进程里再套一层。
    fn helper_args(name: &str) -> Vec<OsString> {
        vec!["--exact".into(), name.into(), "--test-threads=1".into()]
    }

    /// 子进程侧的泄漏判定：环境变量直查；fd 走身份比对（probe_leaked）。
    fn leaked() -> bool {
        std::env::var("PROC_IPC_LEAK").is_ok() || probe_leaked()
    }

    /// 探针判定：探针 fd 号仍打开，且 fstat 的 (st_dev, st_ino) 与宿主
    /// 告知的唯一探针文件相同，才算泄漏。exec 后 fd 编号被子进程自己的
    /// 运行时（dylib、epoll）合法复用是常态——只查编号会假阳性。
    fn probe_leaked() -> bool {
        let (Some(fd), Some(path)) = (
            std::env::var("PROC_IPC_PROBE_FD")
                .ok()
                .and_then(|v| v.parse::<i32>().ok()),
            std::env::var("PROC_IPC_PROBE_PATH").ok(),
        ) else {
            return false;
        };
        let Some(expected) = stat_identity(&path) else {
            return false;
        };
        fd_identity(fd) == Some(expected)
    }

    /// fd 的 (st_dev, st_ino)。
    fn fd_identity(fd: i32) -> Option<(u64, u64)> {
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: fstat 只填充调用方提供的缓冲。
        if unsafe { libc::fstat(fd, &mut stat) } != 0 {
            return None;
        }
        Some((stat.st_dev as u64, stat.st_ino as u64))
    }

    /// 路径的 (st_dev, st_ino)。
    fn stat_identity(path: &str) -> Option<(u64, u64)> {
        let c_path = std::ffi::CString::new(path).ok()?;
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: stat 只填充调用方提供的缓冲。
        if unsafe { libc::stat(c_path.as_ptr(), &mut stat) } != 0 {
            return None;
        }
        Some((stat.st_dev as u64, stat.st_ino as u64))
    }
}
