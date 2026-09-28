//! Postgres 测试基建：docker 一次性容器 + 每用例独占数据库 + 孤儿 volume 回收。
//!
//! 为什么要管 docker 的 volume：PG 的镜像声明了 `VOLUME /var/lib/postgresql/data`，
//! 不带挂载启动就会落到一个**匿名** volume 上。Docker 只在容器**自己退出**时
//! 回收匿名 volume（`--rm` 的语义）；测试清理走的是 `docker rm -f`（atexit 与
//! 残留清扫都是这条路），它只删容器，把 volume 留在 `/var/lib/docker/volumes`
//! 里变成孤儿。一个 e2e 运行漏一个，几十次下来就是几百 MB 白占。
//!
//! 所以这里三层都堵住：
//! 1. **不创建**：数据目录挂 `--tmpfs`，匿名 volume 根本不出现（临时库的
//!    数据本来就没有跨容器的意义，tmpfs 还更快）；
//! 2. **不留**：删除容器一律 [`remove_container`] / `docker rm -f -v`，
//!    `-v` 把匿名 volume 一并收走；
//! 3. **回收历史遗留**：启动时清扫上一次运行（以及本文件修复前的老版本）
//!    漏下的孤儿 volume。
//!
//! 清扫只认**匿名** volume（docker 给它们起的名字是 64 位十六进制）——
//! 命名 volume 是开发者显式建的，不管名字像不像都绝不动。

use std::process::{Command, Output, Stdio};
use std::sync::OnceLock;
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

/// 本 crate 起的容器的 label：残留下一次启动时按它清扫（只动自己的，不碰
/// 开发者的 postgres 容器）。注意必须带 `label=` 前缀——docker 的 filter 简写
/// 形式会直接报 invalid filter。
pub const LABEL: &str = "label=com.flow.e2e=1";

/// 测试数据库名里的时间戳格式（`{前缀}{时间戳}_{uuid}`）：清扫时据此判断残留库
/// 是否已经老到「不可能还有活着的用例」。
const TS_FORMAT: &str = "%Y%m%d%H%M%S";
const TS_LEN: usize = "20240101123045".len();

/// 残留测试库的最小年龄。超过这个年纪的同前缀库，它的用例进程一定已经死了
/// （一次完整 e2e 远短于 15 分钟），删掉不会打到并发运行的另一个实例。
const STALE_DB_AFTER: Duration = Duration::from_secs(15 * 60);

/// 匿名 volume 名：docker 用 64 位十六进制命名。
const ANON_VOLUME_LEN: usize = 64;

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
    Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .ok()
}

/// docker 给匿名 volume 起的名字是 64 位十六进制——只按这一条认身份，
/// 命名 volume（哪怕名字长得像）绝不能进去。
fn is_anonymous_volume(name: &str) -> bool {
    name.len() == ANON_VOLUME_LEN && name.bytes().all(|b| b.is_ascii_hexdigit())
}

/// 挑一个空闲端口（bind 后即放，与 ws_* 测试同款；并发起容器时的极小竞态
/// 由 docker 启动失败时的 panic 暴露，不会静默错配）。
pub fn free_port() -> u16 {
    crate::io::free_port()
}

fn image() -> String {
    std::env::var("FLOW_E2E_PG_IMAGE").unwrap_or_else(|_| "postgres:16-alpine".into())
}

/// 当前 dangling 的匿名 volume 名列表。只读不写，供「不留垃圾」的回归测试观察。
pub fn anonymous_dangling_volumes() -> Vec<String> {
    let output = docker(&["volume", "ls", "-q", "--filter", "dangling=true"]);
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .filter(|name| is_anonymous_volume(name))
        .map(str::to_string)
        .collect()
}

/// 删掉一个容器，并把它拖着的匿名 volume 一并收走。
///
/// `-v` 是省不得的：匿名 volume 只在容器**自己退出**时才跟着 `--rm` 走，
/// 强制删除路径必须显式要 volume，否则就是漏一个孤儿（本仓库踩过，见模块文档）。
pub fn remove_container(id: &str) {
    docker(&["rm", "-f", "-v", id]);
}

/// 容器上挂着的匿名 volume 名。
///
/// 容器还活着时它的 volume 不是 dangling，「有没有匿名挂载」只能直接问容器——
/// 这也正是断言「不创建 volume」唯一可观测的时机（等进程退出就晚了）。
pub fn container_anonymous_volumes(id: &str) -> Vec<String> {
    let template = "{{range .Mounts}}{{if eq .Type \"volume\"}}{{.Name}} {{end}}{{end}}";
    let output = docker(&["inspect", "-f", template, id]);
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .filter(|name| is_anonymous_volume(name))
        .map(str::to_string)
        .collect()
}

/// 回收孤儿匿名 volume（正在被容器使用的那个不会出现在列表里，天然不会被误删）。
pub fn reclaim_orphan_volumes() -> usize {
    let orphans = anonymous_dangling_volumes();
    if orphans.is_empty() {
        return 0;
    }
    eprintln!(
        "backend-e2e: 回收 {} 个孤儿匿名 volume（老版本运行的 `docker rm -f` 漏下的）",
        orphans.len()
    );
    let mut args = vec!["volume", "rm"];
    args.extend(orphans.iter().map(String::as_str));
    // 失败不炸：volume 只是在本次运行里少回收一点，下一层 atexit / 下次启动
    // 还有机会。但它已经吼过一声了（eprintln），不会静默。
    let _ = docker_try(&args);
    orphans.len()
}

/// 起一个带着匿名 volume 的空闲探针容器，返回容器 id。
///
/// 只服务于 docker 卫生回归测试：测「当前进程把容器删掉」这个动作本身干不干净。
/// `sleep 600` 是关键——容器必须活着，否则会被 `--rm` 连 volume 一起收走，
/// 就伪造不出孤儿了。
///
/// 故意留在 `#[cfg(test)]` 之外：用它的代码在别的 crate 的集成测试里。
#[doc(hidden)]
pub fn start_probe_container() -> String {
    let name = format!("flow-probe-{}", Uuid::now_v7().simple());
    let run = docker_try(&[
        "run",
        "-d",
        "--rm",
        "--name",
        &name,
        "-v",
        "/data",
        &image(),
        "sleep",
        "600",
    ])
    .expect("docker run 失败（需要 docker CLI）");
    assert!(
        run.status.success(),
        "探针容器启动失败：{}",
        String::from_utf8_lossy(&run.stderr).trim()
    );
    String::from_utf8_lossy(&run.stdout).trim().to_string()
}

/// 本进程起的容器 id（atexit 清理用）。静态的目的是让 `extern "C"` 处理函数
/// 能拿到——atexit 回调拿不到任何参数。
static CONTAINER_ID: OnceLock<String> = OnceLock::new();

extern "C" fn remove_container_on_exit() {
    if let Some(id) = CONTAINER_ID.get() {
        eprintln!("backend-e2e: 清理 Postgres 容器 {id}");
        remove_container(id);
    }
}

/// 清扫上一次运行遗留的容器（进程被 SIGKILL 时 atexit 没跑到，靠这一层）。
///
/// 必须在 [`reclaim_orphan_volumes`] **之前**跑：容器还在，它的 volume 就不是
/// dangling，扫不掉；先把容器（连同 `-v`）删掉，孤儿才会暴露给 volume 清扫。
fn remove_stale_containers() {
    let output = docker(&["ps", "-aq", "--filter", LABEL]);
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
    let mut args = vec!["rm", "-f", "-v"];
    args.extend(ids.iter().map(String::as_str));
    // 清理失败必须吼出来：静默失败的残留清扫等于没有。例外是目标已达成的两种
    // 回执：另一个进程正在删同一个容器（already in progress）、或已经删掉
    // （No such container）——都算清扫成功，不值得炸掉本次运行。
    let removed = docker_try(&args).expect("执行 docker 失败（需要 docker CLI 在 PATH 中）");
    if !removed.status.success() {
        let stderr = String::from_utf8_lossy(&removed.stderr);
        let already_gone =
            stderr.contains("No such container") || stderr.contains("removal of container");
        assert!(
            already_gone,
            "docker {:?} 退出码 {:?}：{}",
            args,
            removed.status.code(),
            stderr.trim()
        );
    }
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

        // 顺序有讲究：先删残留容器（它们的 volume 才算 dangling），再回收孤儿 volume。
        remove_stale_containers();
        reclaim_orphan_volumes();

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
            // 防线 1：数据目录挂 tmpfs，镜像声明的匿名 volume 根本不会被创建
            "--tmpfs",
            "/var/lib/postgresql/data",
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

/// 一个 Postgres 测试库。`url` 给被测服务进程用，`pool` 给测试自己断言投影用。
pub struct TestDb {
    /// 指到本用例数据库的连接串（传给被测服务进程的 FLOW_DATABASE_URL）。
    pub url: String,
    /// 测试库名（`{prefix}{时间戳}_{uuid}`）。
    pub name: String,
    pool: PgPool,
    server_url: String,
}

impl TestDb {
    /// 在 `base_url` 指向的服务器上开一个全新数据库。
    ///
    /// schema 的初始化交给调用方（flow-pg 的 `schema::init`）：本 crate 不依赖
    /// 任何被测 crate——那会把依赖方向倒过来。
    pub async fn create(base_url: &str, db_prefix: &str) -> TestDb {
        // 建新库之前先扫一遍陈旧的残留库（进程内只跑一次）：上次运行被 SIGKILL
        // 时用例内的 DROP 没走到，全靠这一下收尾。
        sweep_stale_databases(base_url, db_prefix).await;
        let server_url = replace_db(base_url, "postgres");
        let name = format!(
            "{}{}_{}",
            db_prefix,
            chrono::Utc::now().format(TS_FORMAT),
            Uuid::now_v7().simple()
        );
        let admin = PgPool::connect(&server_url)
            .await
            .unwrap_or_else(|e| panic!("连接 Postgres 失败：{e}"));
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {name}")))
            .execute(&admin)
            .await
            .unwrap_or_else(|e| panic!("创建测试数据库 {name} 失败：{e}"));
        admin.close().await;

        let url = replace_db(base_url, &name);
        let pool = PgPool::connect(&url)
            .await
            .unwrap_or_else(|e| panic!("连接测试数据库 {name} 失败：{e}"));
        TestDb {
            url,
            name,
            pool,
            server_url,
        }
    }

    /// 测试自己用的连接池。
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// 关掉连接池并 **DROP 数据库**（幂等：已消失也算干净）。
    ///
    /// 用例结尾调它；失败留给「下次启动的残留清扫」兜底。调用方过去写的是
    /// `db.close()`，那时 `TestDb` 有个同名方法；现在统一叫 `cleanup()`——
    /// **故意不再提供 `close()`**：早期 `Deref<Target=PgPool>` 会让
    /// `db.close()` 静默解析成 `PgPool::close()`，只关池不删库，几十个测试库
    /// 就那样留在服务器上了。
    pub async fn cleanup(&self) {
        // 注意顺序：**先 DROP，后关池**。
        //
        // 早先是反的——先 `self.pool.close()` 再删。而用例手里常有第二个池
        // （PgEngine / PgBackend / 自建的 EventHub），`PgPool::close()` 要等服务
        // 端把连接收干净；自建 hub 不先 stop 时这一步能拖好几秒，调用方外面套的
        // `timeout(.., db.cleanup())` 会把整个 future 掐掉，库就漏了。
        // 把删库提到关池前面，删库就再也不會被关池的耗时挡住。
        let Ok(admin) = PgPool::connect(&self.server_url).await else {
            return;
        };
        // `WITH (FORCE)`（PG 13+）原子地把库里所有会话踢掉再删库。
        //
        // 不用「pg_terminate_backend + 轮询 pg_stat_activity + DROP」那套手工
        // 舞步：测试手里常有**第二个**池（flow-pg 的 PgEngine、flow-backend 的
        // PgBackend、EventHub 的 LISTEN 连接），它们在作用域末尾才释放，cleanup
        // 时还活着；terminate 又只发 SIGTERM，等连接真的断会撞上 sqlx 的重连，
        // 轮询永远等不到 0——每次运行稳定漏一个库。
        //
        // 老服务器（PG 12）不认这个语法，退回 terminate + 轮询 + DROP。
        let forced = sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            self.name
        )))
        .execute(&admin)
        .await;
        if forced.is_err() {
            let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'",
                self.name
            )))
            .execute(&admin)
            .await;
            let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
                "DROP DATABASE IF EXISTS {}",
                self.name
            )))
            .execute(&admin)
            .await;
        }
        admin.close().await;
        // 关池是收尾，不是前提：给它一个上限，绝不把用例卡在这儿
        let _ = tokio::time::timeout(Duration::from_secs(2), self.pool.close()).await;
    }

    /// 这个测试库此刻是否还存在（给「cleanup 真的把库删了」的回归测试用）。
    ///
    /// 曾经踩过：`TestDb` 实现了 `Deref<Target = PgPool>`，调用点的
    /// `db.close()` 静默解析成 `PgPool::close()`——只关池、不删库，几十个
    /// 测试库就那样留在服务器上，而且没有任何测试会响。
    pub async fn database_exists(&self) -> bool {
        let Ok(admin) = PgPool::connect(&self.server_url).await else {
            return false;
        };
        let found: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT datname FROM pg_database WHERE datname = '{}'",
            self.name
        )))
        .fetch_optional(&admin)
        .await
        .unwrap_or(None);
        admin.close().await;
        found.is_some()
    }
}

/// 把 `postgres://.../<db>` 的库名换掉。
pub fn replace_db(url: &str, db: &str) -> String {
    let cut = url.rfind('/').expect("连接串必须带库名");
    format!("{}/{}", &url[..cut], db)
}

/// 进程内一次性清扫陈旧的残留测试库（上次运行被 SIGKILL 时用例内 DROP 没跑到）。
///
/// 只删**超过 [`STALE_DB_AFTER`]** 的同前缀库：一次完整测试远短于这个年纪，
/// 所以还年轻的名字一定是**另一个并发实例**正在用的，绝不能动。
pub async fn sweep_stale_databases(base_url: &str, db_prefix: &str) {
    static SWEPT: OnceLock<()> = OnceLock::new();
    if SWEPT.set(()).is_err() {
        return;
    }
    let server_url = replace_db(base_url, "postgres");
    let Ok(admin) = PgPool::connect(&server_url).await else {
        return;
    };
    let rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT datname FROM pg_database WHERE datname LIKE '{}%'",
        db_prefix
    )))
    .fetch_all(&admin)
    .await
    .unwrap_or_default();
    for (name,) in rows {
        if !is_stale(&name, db_prefix) {
            continue;
        }
        let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{name}'"
        )))
        .execute(&admin)
        .await;
        let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP DATABASE IF EXISTS {name}"
        )))
        .execute(&admin)
        .await;
    }
    admin.close().await;
}

/// `{prefix}{时间戳}_...` 里的时间戳是否已经老到可以确定「用例进程已死」。
fn is_stale(name: &str, db_prefix: &str) -> bool {
    // 注意：时间戳是定长 14 字符。早先这个函数取的是 `.get(..16)`（多了 2 位
    // uuid 前缀），`parse_from_str` 对多出的字符直接报错 → `?` 一路 continue
    // 下去，残留清扫**从来没删掉过任何库**。定长取 + 显式 else 才让这条路径
    // 真的生效。
    let Some(stamp) = name.get(db_prefix.len()..db_prefix.len() + TS_LEN) else {
        return false;
    };
    let Some(created) = chrono::NaiveDateTime::parse_from_str(stamp, TS_FORMAT).ok() else {
        // 时间戳解析不出来的库不是我们起的：别碰
        return false;
    };
    let age = chrono::Utc::now().naive_utc() - created;
    age.to_std()
        .map(|age| age >= STALE_DB_AFTER)
        .unwrap_or(false)
}

/// 服务器上还挂着哪些给定前缀的测试库（给「没漏库」的回归测试用）。
pub async fn test_databases(base_url: &str, db_prefix: &str) -> Vec<String> {
    let server_url = replace_db(base_url, "postgres");
    let Ok(admin) = PgPool::connect(&server_url).await else {
        return Vec::new();
    };
    let rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT datname FROM pg_database WHERE datname LIKE '{}%'",
        db_prefix
    )))
    .fetch_all(&admin)
    .await
    .unwrap_or_default();
    admin.close().await;
    rows.into_iter().map(|(name,)| name).collect()
}
