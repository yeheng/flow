//! 产品无关的测试基建：独占临时目录守卫、空闲端口、TCP 就绪等待。
//!
//! 这些东西每个测试都要写一遍；写进 `tests/` 就得在每个集成测试二进制里
//! 各编译一份、各自漂移一遍。集中到一个 publish = false 的 lib 里，
//! 单点维护，所有测试共指一份（DESIGN §13：跨 crate 共享的测试代码只放这里）。
//!
//! 硬约束（DESIGN.md §13）：临时目录必须是**单独创建的子目录**，Drop 只
//! 删这一个子目录——绝不能删 `temp_dir()` 本身，那会连带别的进程正在用的
//! 东西（以及别人的数据）。

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// TCP 就绪等待的上限：本地端口上拉起一个服务进程远快于这个数；真超时了
/// 往往是端口没绑上或二进制起不来，早炸比晚炸省时间。
pub const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// 独占临时目录。Drop 时整体删除。
///
/// 断言失败 / panic 都会先经过 Drop 再 unwind，所以用例失败不会漏垃圾；
/// 这也正是「手写 `std::fs::remove_dir_all(...).unwrap()`」做不到的——
/// 那个 `unwrap()` 挂在清理路径上，一旦用例已失败它会用「删目录」的 panic
/// 覆盖掉原始断言信息。
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn new(tag: &str) -> TempDir {
        Self::new_in(&std::env::temp_dir(), tag)
    }

    /// 在 `base` 下建一个独占子目录。
    pub fn new_in(base: &Path, tag: &str) -> TempDir {
        let path = base.join(format!("{tag}-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&path).expect("创建测试临时目录失败");
        TempDir { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 目录下的一个子路径（如 `flow.db`）。
    pub fn join(&self, rel: impl AsRef<Path>) -> PathBuf {
        self.path.join(rel)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        // 只删自己建的子目录；失败意味着它本来就没建成，不值得为它 panic
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// 绑 `127.0.0.1:0` 拿到随机端口后立刻放掉。
///
/// **这个端口是「建议」不是「预留」**：从放掉到被测进程真正 bind 之间存在
/// TOCTOU 窗口，并行测试（尤其 CI 上多核 arm64）足够让另一个用例抢走它。
/// 因此凡是「拿到端口 → spawn 进程 bind 该端口」的地方都必须配合
/// [`wait_ready_child`] 检视子进程存活并重试，见 `PORT_RETRY_ATTEMPTS`。
pub fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定临时端口失败");
    listener.local_addr().expect("读取本地地址失败").port()
}

/// 端口被抢（TOCTOU）时的重试次数。
///
/// 重试而不是「换端口就完事」是因为失败表现不唯一：被测进程可能因
/// `AddrInUse` 立即退出，也可能仍然活着但我们连上的是**别人的**服务
/// （`wait_ready` 只做 TCP connect，别的用例的进程正监听在那个端口上）。
/// 两种都必须靠「子进程还活着吗」这一条信号区分。
pub const PORT_RETRY_ATTEMPTS: usize = 5;

/// 就绪等待的结果。
#[derive(Debug, PartialEq, Eq)]
pub enum Ready {
    /// `addr` 上能连上，且子进程仍然活着。
    Ready,
    /// 子进程已退出——通常是端口被抢（`AddrInUse`）或二进制起不来。
    ChildExited(std::process::ExitStatus),
    /// 超时仍未就绪。
    TimedOut,
}

/// 轮询到 `addr` 上能接受 TCP 连接**且 `child` 仍活着**为止。
///
/// 「子进程还活着」是必要条件而非锦上添花：只判 TCP 可连时，若该端口已被
/// 另一个用例的进程占着，我们会连上**那个**进程并把它的响应当成自己服务端的
/// 响应——用例于是对着别人的数据库操作，报错信息完全指错方向。
///
/// 返回值让调用方决定是换端口重试（[`Ready::ChildExited`]）还是直接失败
/// （[`Ready::TimedOut`]），而不是在这里 panic 掉重试机会——失败文案由调用方
/// 拼（它知道服务名、端口与重试次数）。
pub fn wait_ready_child(addr: SocketAddr, child: &mut std::process::Child) -> Ready {
    let deadline = Instant::now() + READY_TIMEOUT;
    while Instant::now() < deadline {
        // 先看子进程：它若已退出（端口被抢）就别再对着别人的服务空等
        if let Ok(Some(status)) = child.try_wait() {
            return Ready::ChildExited(status);
        }
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            // 连上后再确认一次：端口可能在我们 connect 的瞬间才被别人占住，
            // 而我们的子进程随后才因 AddrInUse 退出。
            match child.try_wait() {
                Ok(Some(status)) => return Ready::ChildExited(status),
                _ => return Ready::Ready,
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ready::TimedOut
}

/// [`wait_ready_child`] 的 panic 版，供没有子进程句柄可查的调用点使用
/// （例如只起了个线程内的监听）。有子进程可查的一律用
/// [`wait_ready_child`]：它能区分「端口被抢」与「真没起来」。
pub fn wait_ready(what: &str, addr: SocketAddr) {
    // 无子进程可查：退回纯 TCP 判定
    let deadline = Instant::now() + READY_TIMEOUT;
    while Instant::now() < deadline {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("{what} {addr} 未在 {READY_TIMEOUT:?} 内就绪");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};

    /// 端口被抢时 `wait_ready_child` 必须报 `ChildExited`，而不是因「连得上
    /// 别人的服务」而误判 Ready。这是 CI 上 parallel 用例的真实失败模式。
    #[test]
    fn child_exited_is_reported_even_when_port_is_connectable() {
        // 占住一个端口并开始监听（模拟另一个用例的进程）
        let squatter = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = squatter.local_addr().unwrap();

        // 我们的「子进程」立刻退出（模拟 AddrInUse 导致的启动失败）
        let mut child = Command::new("sh")
            .args(["-c", "exit 42"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        std::thread::sleep(Duration::from_millis(200));

        // 端口可连（squatter 在听），但子进程已死 → 必须报 ChildExited
        assert!(matches!(
            wait_ready_child(addr, &mut child),
            Ready::ChildExited(_)
        ));
    }

    /// 子进程活着且端口能连上 → Ready。
    #[test]
    fn alive_child_on_listening_port_is_ready() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut child = Command::new("sh")
            .args(["-c", "sleep 30"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        assert!(matches!(wait_ready_child(addr, &mut child), Ready::Ready));
        let _ = child.kill();
        let _ = child.wait();
    }
}

/// 被测进程自报端口后的结果。
#[derive(Debug, Clone, Copy)]
pub struct ReportedPorts {
    /// JSON-RPC / WebSocket 端口。
    pub rpc: SocketAddr,
    /// 附带的第二个 HTTP 端口（webhook 入口）。
    pub http: SocketAddr,
}

/// 让子进程自己绑 `127.0.0.1:0`，再从它的启动日志里读回**实际**端口。
///
/// **为什么不用 [`free_port`]**：那是「父进程拿到端口 → 放掉 → 子进程 bind」的
/// TOCTOU，两步之间端口可被并行用例抢走。抢走时子进程因 `AddrInUse` 立即退出，
/// 而父进程此刻可能正把「连得上」误判成就绪——连上的是**别人的**服务，随后
/// 连接断开，报错指向完全无关的地方（`background task closed`、`AddrInUse`）。
/// 靠「检测子进程早退再重试」只能缩小窗口：子进程要 exec 完才到 bind，
/// 检查那一刻它往往还活着。
///
/// 端口 0 由内核在 bind 那一刻原子分配，**没有窗口**。
///
/// 代价：依赖子进程把实际地址打进日志。标记（`local_addr=` / `http_addr=`）
/// 是 flow-rpc 的 `tracing` 输出，键名与字段名对应，改动会让这里解析不到而
/// 明确失败（见返回值的 Err），不会静默连错服务。
pub fn spawn_reporting_ports(
    cmd: &mut std::process::Command,
    rpc_addr_env: &str,
    http_addr_env: &str,
    log_path: Option<PathBuf>,
) -> Result<(std::process::Child, ReportedPorts), String> {
    const RPC_MARKER: &str = "local_addr=";
    const HTTP_MARKER: &str = "http_addr=";

    cmd.env(rpc_addr_env, "127.0.0.1:0")
        .env(http_addr_env, "127.0.0.1:0")
        .stdout(std::process::Stdio::piped());
    let mut child = cmd.spawn().map_err(|e| format!("启动被测进程失败：{e}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "拿不到子进程 stdout".to_string())?;

    let (tx, rx) = std::sync::mpsc::channel::<ReportedPorts>();
    std::thread::spawn(move || {
        use std::io::{BufRead, Write};
        let mut log = log_path.and_then(|p| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
                .ok()
        });
        let mut rpc = None;
        let mut http = None;
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if let Some(w) = log.as_mut() {
                let _ = writeln!(w, "{line}");
            }
            // tracing 默认给字段名/值上 ANSI 颜色（即使写到管道），转义序列插在
            // `local_addr` 与 `=` 之间，直接按 "local_addr=" 匹配永远落空——
            // 先剥掉 CSI 序列再解析。
            let line = strip_ansi(&line);
            if rpc.is_none() {
                if let Some(addr) = parse_addr_after(&line, RPC_MARKER) {
                    rpc = Some(addr);
                }
            }
            if http.is_none() {
                if let Some(addr) = parse_addr_after(&line, HTTP_MARKER) {
                    http = Some(addr);
                }
            }
            if let (Some(rpc), Some(http)) = (rpc, http) {
                let _ = tx.send(ReportedPorts { rpc, http });
            }
        }
        // 端口没报出来时给一次明确失败，而不是让调用方对着 127.0.0.1:0 空等
        if rpc.is_none() || http.is_none() {
            let _ = tx.send(ReportedPorts {
                rpc: SocketAddr::from(([127, 0, 0, 1], 0)),
                http: SocketAddr::from(([127, 0, 0, 1], 0)),
            });
        }
    });

    match rx.recv_timeout(READY_TIMEOUT) {
        Ok(ports) if ports.rpc.port() != 0 && ports.http.port() != 0 => Ok((child, ports)),
        Ok(_) | Err(_) => Err(format!(
            "被测进程未在 {READY_TIMEOUT:?} 内报出实际端口\
             （期望日志含 {RPC_MARKER} 与 {HTTP_MARKER}）"
        )),
    }
}

/// 剥掉一行日志里的 ANSI CSI 转义序列（ESC `[` … 终止字母）。
/// tracing-subscriber 的 fmt 层默认开 ANSI 颜色且不探 tty，管道/文件里同样
/// 带颜色码；颜色只出现在渲染层，剥掉不影响 `key=value` 的语义。
fn strip_ansi(line: &str) -> std::borrow::Cow<'_, str> {
    if !line.contains('\u{1b}') {
        return std::borrow::Cow::Borrowed(line);
    }
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            // CSI：参数/中间字节之后以 0x40–0x7E 的终止字母收尾
            for c in chars.by_ref() {
                if ('\u{40}'..='\u{7e}').contains(&c) {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    std::borrow::Cow::Owned(out)
}

/// 从一行日志里取 `marker` 之后的 `host:port`。
fn parse_addr_after(line: &str, marker: &str) -> Option<SocketAddr> {
    let rest = line.split(marker).nth(1)?.trim_start();
    let token = rest
        .split(|c: char| c.is_whitespace() || c == '"' || c == ',')
        .next()?;
    token.parse().ok()
}

#[cfg(test)]
mod port_parse_tests {
    use super::*;

    /// tracing fmt 的真实输出形态：字段名与 `=` 之间隔着 ANSI 颜色序列，
    /// 剥掉后必须能解析出地址（这是 spawn_reporting_ports 曾经 30s 超时
    /// 的根因）。
    #[test]
    fn ansi_colored_tracing_line_parses() {
        let line = "\u{1b}[2m2026-09-30T06:57:58Z\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m \
                    \u{1b}[2mflow_rpc\u{1b}[0m\u{1b}[2m:\u{1b}[0m flow-server 已启动 \
                    \u{1b}[3mlocal_addr\u{1b}[0m\u{1b}[2m=\u{1b}[0m127.0.0.1:54915 \
                    \u{1b}[3mbackend\u{1b}[0m\u{1b}[2m=\u{1b}[0m\"sqlite\"";
        let addr = parse_addr_after(&strip_ansi(line), "local_addr=").unwrap();
        assert_eq!(addr, SocketAddr::from(([127, 0, 0, 1], 54915)));
    }

    #[test]
    fn strip_ansi_leaves_plain_text_untouched() {
        let plain = "http_addr=127.0.0.1:9801 plain";
        assert!(matches!(strip_ansi(plain), std::borrow::Cow::Borrowed(_)));
        assert_eq!(strip_ansi(plain), plain);
    }
}
