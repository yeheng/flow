//! 产品无关的测试基建：独占临时目录守卫、空闲端口、TCP 就绪等待。
//!
//! 这些东西每个测试都要写一遍；写进 `tests/` 就得在每个集成测试二进制里
//! 各编译一份、各自漂移一遍（这个仓库历史上就因为散着写而复制出三份
//! `temp_dir() + remove_dir_all`）。集中到一个 publish = false 的 lib 里，
//! 单点维护，所有测试共指一份。
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
/// 并发起多个进程时存在极小竞态（拿到端口后、对方 bind 前被第三方抢走），
/// 由启动失败时的 panic 暴露，不会静默错配。
pub fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("绑定临时端口失败");
    listener.local_addr().expect("读取本地地址失败").port()
}

/// 轮询到 `addr` 上真的能接受 TCP 连接为止；超时 panic。
///
/// 就绪判定只用「能否连上」：被测服务（flow-server）的 RPC 端口是启动早期
/// 就 bind 的，连上即代表它可以被客户端打了。
pub fn wait_ready(what: &str, addr: SocketAddr) {
    let deadline = Instant::now() + READY_TIMEOUT;
    while Instant::now() < deadline {
        if std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("{what} {addr} 未在 {READY_TIMEOUT:?} 内就绪");
}
