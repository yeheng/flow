//! docker CLI 管理 Postgres 容器：启动、就绪等待、确定性清理、残留清扫。
//!
//! 为什么直接调 docker CLI 而不是 testcontainers：本仓库的部署面已经有
//! docker-compose（见根目录与开发文档），e2e 只需要「一个一次性
//! postgres:16-alpine + 随机端口」，CLI 方案零协议依赖、行为可预测，
//! 编译期不引入重依赖。
//!
//! 生命周期（三层保险，对应「每次运行后清理残余」）：
//! 1. 用例内：每个用例一个独占数据库，用例结束 DROP（`common::db`）；
//! 2. 进程退出：`atexit` 里 `docker rm -f` 掉本进程起的容器（正常退出与
//!    用例失败都会走到；SIGKILL 见第 3 层）；
//! 3. 下次启动：按 label 清扫上一次运行遗留的容器 + 清扫 `e2e_%` 测试库。

use std::process::{Command, Output, Stdio};
use std::sync::OnceLock;
use std::time::Duration;

use uuid::Uuid;

/// 容器 label：残留下一次启动时按它清扫（只动自己起的，不碰 flow-pgtest 等开发容器）。
/// 注意必须带 `label=` 前缀——docker 的 filter 简写形式会直接报 invalid filter，
/// 而清理路径是「尽力而为」的，错误会被静默吞掉。
pub const E2E_LABEL: &str = "label=com.flow.e2e=1";
/// 测试数据库名前缀：残留下一次启动时按它 DROP。
pub const E2E_DB_PREFIX: &str = "e2e_";

fn image() -> String {
    std::env::var("FLOW_E2E_PG_IMAGE").unwrap_or_else(|_| "postgres:16-alpine".into())
}

/// 本进程容器的 id（atexit 清理用）。静态的目的是让 `extern "C"` 处理函数能拿到。
static CONTAINER_ID: OnceLock<String> = OnceLock::new();

fn docker(args: &[&str]) -> Output {
    let output = Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .unwrap_or_else(|e| {
            panic!(
                "执行 docker {:?} 失败：{e}（需要 docker CLI 在 PATH 中）",
                args
            )
        });
    if !output.status.success() {
        panic!(
            "docker {:?} 退出码 {:?}：{}",
            args,
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    output
}

/// 尽力而为的 docker 调用（清理路径用，失败只记录不炸测试）。
fn docker_try(args: &[&str]) -> Option<Output> {
    let output = Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .ok()?;
    Some(output)
}

extern "C" fn remove_container_on_exit() {
    if let Some(id) = CONTAINER_ID.get() {
        eprintln!("backend-e2e: 清理 Postgres 容器 {id}");
        let _ = docker_try(&["rm", "-f", id]);
    }
}

/// 清扫上一次运行遗留的容器（进程被 SIGKILL 时 atexit 没跑到，靠这一层）。
fn remove_stale_containers() {
    let output = docker(&["ps", "-aq", "--filter", E2E_LABEL]);
    let ids: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .map(str::to_string)
        .collect();
    if ids.is_empty() {
        return;
    }
    eprintln!(
        "backend-e2e: 清扫上次运行遗留的 Postgres 容器 {} 个：{:?}",
        ids.len(),
        ids
    );
    let mut args = vec!["rm", "-f"];
    args.extend(ids.iter().map(String::as_str));
    // 清理失败必须吼出来：静默失败的残留清扫等于没有。例外是目标已达成的两种
    // 回执：另一个进程正在删同一个容器（already in progress）、或已经删掉
    // （No such container）——都算清扫成功，不值得炸掉本次运行。
    let removed = docker_try(&args).expect("执行 docker 失败（需要 docker CLI 在 PATH 中）");
    if !removed.status.success() {
        let stderr = String::from_utf8_lossy(&removed.stderr);
        let already_gone = stderr.contains("No such container")
            || stderr.contains("removal of container");
        assert!(
            already_gone,
            "docker {:?} 退出码 {:?}：{}",
            args,
            removed.status.code(),
            stderr.trim()
        );
    }
}

/// 挑一个空闲端口（bind 后即放，与现有 ws_* 测试同款；并发起容器时的极小竞态
/// 由 docker 启动失败时的 panic 暴露，不会静默错配）。
pub fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("绑定临时端口失败");
    listener.local_addr().expect("读取端口失败").port()
}

pub struct PgContainer {
    id: String,
    port: u16,
}

impl PgContainer {
    /// 连接地址（库名 flow，与容器初始化参数一致）。
    pub fn url(&self) -> String {
        format!("postgres://flow:flow@127.0.0.1:{}/flow", self.port)
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// 起容器并等待到可以执行查询。docker CLI 缺失或镜像拉取失败会直接 panic——
    /// e2e 的语义就是「没有可用 Postgres 就是失败」，不像 ws_pg 那样跳过。
    pub async fn start() -> PgContainer {
        docker_try(&["version"]).unwrap_or_else(|| {
            panic!("docker CLI 不可用：backend-e2e 需要 docker 来运行 Postgres 容器")
        });
        remove_stale_containers();

        let port = free_port();
        let name = format!("flow-e2e-{}", Uuid::now_v7().simple());
        let port_mapping = format!("127.0.0.1:{port}:5432");
        let output = docker(&[
            "run",
            "-d",
            "--rm",
            "--label",
            "com.flow.e2e=1",
            "--name",
            &name,
            "-e",
            "POSTGRES_USER=flow",
            "-e",
            "POSTGRES_PASSWORD=flow",
            "-e",
            "POSTGRES_DB=flow",
            "-p",
            &port_mapping,
            &image(),
        ]);
        let id = String::from_utf8_lossy(&output.stdout).trim().to_string();

        // 注册进程退出清理（幂等：OnceLock 已初始化时重复注册无害，
        // atexit 同一函数指针只会保留一次回调）。
        if CONTAINER_ID.set(id.clone()).is_ok() {
            // SAFETY: extern "C" fn 无捕获，atexit 契约允许在进程正常退出时回调。
            unsafe {
                libc::atexit(remove_container_on_exit);
            }
        }

        let container = PgContainer { id, port };
        container.wait_ready().await;
        container
    }

    /// 轮询到容器里的 Postgres 真的能接受连接并执行查询为止。
    async fn wait_ready(&self) {
        let url = self.url();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        let mut last_error = String::from("尚未尝试");
        loop {
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "Postgres 容器 {} 在 90s 内未就绪（镜像：{}），最后一次错误：{last_error}",
                    self.id,
                    image()
                );
            }
            let attempt = tokio::time::timeout(Duration::from_secs(3), async {
                let pool = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&url)
                    .await?;
                // SELECT 1 在 PG 里是 INT4：只验证「能连上且能执行查询」
                sqlx::query("SELECT 1").execute(&pool).await?;
                pool.close().await;
                Ok::<(), sqlx::Error>(())
            })
            .await;
            match attempt {
                Ok(Ok(())) => return,
                Ok(Err(err)) => last_error = err.to_string(),
                Err(err) => last_error = format!("连接超时：{err}"),
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

/// 进程内共享容器（每个测试二进制一个；cargo 顺序跑各测试文件，容器不重叠）。
/// 泄漏出 `&'static`：容器活到进程退出，退出清理走 atexit（见上）。
pub async fn shared() -> &'static PgContainer {
    static CONTAINER: tokio::sync::OnceCell<PgContainer> = tokio::sync::OnceCell::const_new();
    CONTAINER
        .get_or_init(|| async { PgContainer::start().await })
        .await
}
