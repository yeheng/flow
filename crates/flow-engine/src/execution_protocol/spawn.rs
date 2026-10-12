//! 执行器子进程 spawn（二期 §3.1 的 FD 映射，供本地池与远程 agent 复用）。
//!
//! 仅映射目标 socketpair FD（dup2 到固定槽位后关闭其余继承 FD），其余 FD
//! 默认 close-on-exec；父进程 spawn 后需自行关闭子端副本（本函数返回的
//! 父端持有前，子端 `ChannelPair` 由调用方 drop）。

use std::os::fd::AsRawFd;

use tokio::process::{Child, Command};

use super::contract::ExecutorInvocation;
use super::unix::{socketpair_channels, ChannelPair};

/// spawn 一个执行器进程并返回 (父端双通道, 子进程句柄)。
/// `tag` 为可选诊断参数（执行器忽略未知参数）。
pub fn spawn_executor_process(
    executor: &ExecutorInvocation,
    tag: Option<&str>,
) -> std::io::Result<(ChannelPair, Child, ChannelPair)> {
    let (parent, child_pair) = socketpair_channels()?;
    let child_control = child_pair.control.as_raw_fd();
    let child_data = child_pair.data.as_raw_fd();
    let mut command = Command::new(&executor.program);
    command.args(&executor.prefix);
    if let Some(tag) = tag {
        command.arg("--tag").arg(tag);
    }
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::inherit())
        .kill_on_drop(true);
    // 环境隔离（T6）：执行器只需要 argv 与 pre_exec 落位的 FD 槽位，
    // 不继承主进程环境（凭据/路径/代理等一概不泄漏）；仅回填诊断日志
    // 级别，保持可观测性。
    command.env_clear();
    if let Some(log) = std::env::var_os("RUST_LOG") {
        command.env("RUST_LOG", log);
    }
    // pre_exec 只做允许的低层 FD 操作：fcntl/dup2/close/getrlimit。禁止
    // 分配内存、加锁或调用普通异步代码（二期 §3.1）。
    unsafe {
        command.pre_exec(move || {
            place_fds(child_control, child_data)?;
            // 关闭其余继承 FD（父端 + 兄弟 socketpair），防泄漏/串配。
            let limit = fd_limit();
            let mut fd: i32 = 3;
            while fd < limit {
                if fd != super::contract::EXECUTOR_CONTROL_FD_SLOT
                    && fd != super::contract::EXECUTOR_DATA_FD_SLOT
                {
                    libc::close(fd);
                }
                fd += 1;
            }
            Ok(())
        });
    }
    let child = command.spawn()?;
    // 注意：子端副本由调用方 drop（本函数返回它以明确生命周期）。
    Ok((parent, child, child_pair))
}

/// 把两个子端通道 FD 落位到固定槽位（100/101）。
///
/// F_DUPFD 由内核分配「>= base 的最低空闲 FD」，天然无冲突：两次分配必然
/// 不同、必然 > 101，后续 dup2 不可能覆盖临时位——因此不需要 spare 扫描、
/// 不需要槽位冲突矩阵、也不需要自关闭守卫（原 FD 恰在 100/101/临时区都
/// 安全）。原 FD 不在此关闭，由 pre_exec 的 close-all 统一回收。
fn place_fds(child_control: i32, child_data: i32) -> std::io::Result<()> {
    use super::contract::{EXECUTOR_CONTROL_FD_SLOT, EXECUTOR_DATA_FD_SLOT};
    let limit = fd_limit();
    let base = 1024.min(limit - 8);
    if base <= EXECUTOR_DATA_FD_SLOT {
        return Err(std::io::Error::from_raw_os_error(libc::EMFILE));
    }
    // SAFETY: fcntl/dup2/close 均为异步信号安全调用，FD 由调用方保证有效。
    unsafe {
        let temp_control = libc::fcntl(child_control, libc::F_DUPFD, base);
        if temp_control < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let temp_data = libc::fcntl(child_data, libc::F_DUPFD, base);
        if temp_data < 0 {
            return Err(std::io::Error::last_os_error());
        }
        if libc::dup2(temp_control, EXECUTOR_CONTROL_FD_SLOT) < 0
            || libc::dup2(temp_data, EXECUTOR_DATA_FD_SLOT) < 0
        {
            return Err(std::io::Error::last_os_error());
        }
        libc::close(temp_control);
        libc::close(temp_data);
    }
    Ok(())
}

/// 子进程 FD 上限（getrlimit 是 pre_exec 安全的低层调用）。
fn fd_limit() -> i32 {
    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 4096,
            rlim_max: 4096,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
            limit.rlim_cur.min(65_536) as i32
        } else {
            4096
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::FromRawFd;

    /// 子进程侧要跑的占用排列：交叉互占（control=101/data=100）、单侧占用、
    /// 以及无占用的常态。
    const CASES: &[(Option<i32>, Option<i32>)] = &[
        (None, None),
        (Some(100), None),
        (None, Some(100)),
        (Some(101), None),
        (None, Some(101)),
        (Some(100), Some(101)),
        (Some(101), Some(100)),
    ];
    /// 子进程用哪个排列（父进程按 case 号 fork 一次）。
    const CASE_ENV: &str = "FLOW_PLACE_FDS_CASE";

    fn pipe() -> (std::os::unix::io::RawFd, std::os::unix::io::RawFd) {
        let mut fds = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        (fds[0], fds[1])
    }

    fn read_one_byte(fd: i32) -> u8 {
        // SAFETY: FD 由测试构造并只移交一次。
        let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
        let mut buf = [0u8; 1];
        std::io::Read::read_exact(&mut file, &mut buf).unwrap();
        buf[0]
    }

    fn write_byte(fd: i32, byte: u8) {
        // SAFETY: FD 由测试构造并只移交一次。
        let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
        std::io::Write::write_all(&mut file, &[byte]).unwrap();
    }

    /// 原始 FD 落在 100/101 雷区的所有排列下，落位后槽位必须指向原连接。
    ///
    /// **每个排列都在独立子进程里跑**：`place_fds` 会 `dup2` 到固定的
    /// 100/101，覆盖同进程其它线程此刻持有的同号 FD——在测试进程内并行跑时，
    /// 被覆盖的 `OwnedFd` 析构会撞上 `IO Safety violation: owned file
    /// descriptor already closed`（SIGABRT，整个测试二进制挂掉）。生产里
    /// `place_fds` 只在 `pre_exec`（fork 之后、子进程单线程）执行，子进程
    /// 宿主才是与真实语义一致的隔离。
    #[test]
    fn place_fds_survives_slot_occupation() {
        let exe = std::env::current_exe().expect("test binary path");
        for (case, (control_at, data_at)) in CASES.iter().enumerate() {
            let status = std::process::Command::new(&exe)
                .args([
                    "--exact",
                    "execution_protocol::spawn::tests::place_fds_case_child",
                    "--nocapture",
                ])
                .env(CASE_ENV, case.to_string())
                .status()
                .expect("spawn fd-case child");
            assert!(
                status.success(),
                "case {case} (control={control_at:?} data={data_at:?}) failed in child: {status}"
            );
        }
    }

    /// 子进程侧用例：按 [`CASE_ENV`] 选一个排列，落位后校验槽位指向原连接。
    /// 无环境变量时直接返回（作为普通用例被其它入口跑到时是 no-op）。
    #[test]
    fn place_fds_case_child() {
        let Ok(case) = std::env::var(CASE_ENV) else {
            return;
        };
        let case: usize = case.parse().expect("case index");
        let (control_at, data_at) = CASES[case];
        let (control_read, control_write) = pipe();
        let (data_read, data_write) = pipe();
        let move_to = |fd: i32, slot: Option<i32>| match slot {
            // SAFETY: dup2 到测试自选槽位；原 FD 由测试关闭。
            Some(slot) => unsafe {
                let moved = libc::dup(fd);
                libc::dup2(moved, slot);
                libc::close(moved);
                libc::close(fd);
                slot
            },
            None => fd,
        };
        let control_fd = move_to(control_read, control_at);
        let data_fd = move_to(data_read, data_at);
        place_fds(control_fd, data_fd).unwrap();
        write_byte(control_write, 0xA1);
        write_byte(data_write, 0xD2);
        assert_eq!(
            read_one_byte(100),
            0xA1,
            "control broken at occupation {control_at:?}/{data_at:?}"
        );
        assert_eq!(
            read_one_byte(101),
            0xD2,
            "data broken at occupation {control_at:?}/{data_at:?}"
        );
        // 收尾：写端与原始残留 FD。
        // SAFETY: 测试自管的 FD。
        unsafe {
            libc::close(control_write);
            libc::close(data_write);
            if control_fd < 100 {
                libc::close(control_fd);
            }
            if data_fd < 100 {
                libc::close(data_fd);
            }
        }
    }
}
