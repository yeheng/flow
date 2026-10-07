//! docker 卫生回归：e2e 跑完，磁盘上不能留下容器 / 测试库 / **孤儿匿名 volume**。
//!
//! 背景（真实漏过）：PG 镜像声明了 `VOLUME /var/lib/postgresql/data`，不带挂载
//! 启动就落到一个**匿名** volume 上。Docker 只在容器**自己退出**时（`--rm`）
//! 回收匿名 volume；测试清理走的是 `docker rm -f`（atexit 与残留清扫都走这条），
//! 它只删容器，volume 留在 `/var/lib/docker/volumes` 里变成孤儿。一个 e2e 运行
//! 漏一个（PG 数据目录数十 MB），几十次之后就是几百 MB 白占。
//!
//! 三条防线在这一个用例里各有一条断言（必须串行：它们都在数同一批 docker
//! volume，并行会互相把对方的孤儿/清扫算进来）：
//! 1. **不创建**：数据目录挂 `--tmpfs` → 运行中的容器身上没有任何匿名 volume；
//! 2. **不留**：删除一律 `docker rm -f -v` → 删掉带 volume 的容器不剩孤儿；
//! 3. **回收历史遗留**：启动时清扫 dangling 匿名 volume → 早于本文件的老运行漏下
//!    的孤儿，下一次 e2e 启动就该被回收。
//!
//! 「不创建」必须在容器**活着的**时候查（`container_anonymous_volumes`）：
//! 等到进程退出就晚了——那时 volume 已经是孤儿，它是否被 atexit 收走，
//! 测试已经看不到了。
//!
//! 这些用例属于「harness 契约」而非「后端行为契约」，所以不套 `e2e_test!`
//! （那个宏按双后端展开），而是普通 `#[tokio::test]`。

use backend_e2e::common::E2E_DB_PREFIX;
use flow_test_support::pg::{
    anonymous_dangling_volumes, container_anonymous_volumes, reclaim_orphan_volumes,
    remove_container, shared, start_probe_container, test_databases, TestDb,
};

static HYGIENE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// 三条防线各一条断言。探针容器用完就删，主容器留给 atexit。
#[tokio::test]
async fn pg_run_creates_no_volume_and_reclaims_every_orphan() {
    let _guard = HYGIENE_LOCK.lock().await;
    // ---- 防线 1：容器身上不该有匿名 volume（数据目录挂 tmpfs）----
    let pg = shared().await;
    assert!(
        container_anonymous_volumes(pg.id()).is_empty(),
        "测试容器不得挂匿名 volume（数据目录必须挂 --tmpfs）：{:?}",
        container_anonymous_volumes(pg.id())
    );

    // ---- 一轮完整用例：建库 → 落 schema → 写数据 → DROP ----
    let db = TestDb::create(&pg.url(), E2E_DB_PREFIX).await;
    flow_pg::schema::init(db.pool())
        .await
        .expect("初始化测试库 schema 失败");
    sqlx::query("CREATE TABLE IF NOT EXISTS _hygiene_probe (v int)")
        .execute(db.pool())
        .await
        .expect("执行 DDL 失败");
    assert!(db.database_exists().await, "建库后应查得到这个库");

    // 真实场景：用例手里通常还有**第二个**池——flow-pg 的 PgEngine（自带一个
    // EventHub 在 LISTEN）。它到作用域末尾才释放，cleanup 时还活着。cleanup 若
    // 把「关自己的池」排在删库前面，就可能被这第二个池拖住，删库被跳过。
    let engine = flow_pg::PgEngine::connect(&db.url, flow_pg::PgConfig::default())
        .await
        .expect("连接引擎失败");

    // cleanup 的语义是「关池 + DROP 数据库」，不是「关池」。若 TestDb 实现
    // Deref<Target=PgPool>，调用点的 db.close() 会静默解析成 PgPool::close()——
    // 只关池不删库，一圈测试下来服务器上堆了几十个残留库，且没有任何测试会响。
    db.cleanup().await;
    assert!(
        !db.database_exists().await,
        "cleanup 必须真的 DROP 数据库，只关连接池不算清理"
    );
    assert!(
        anonymous_dangling_volumes().is_empty(),
        "跑完用例仍不该有孤儿 volume：{:?}",
        anonymous_dangling_volumes()
    );
    drop(engine);

    // 服务端一个测试库都不该剩下。删库必须排在「关自己的池」之前——顺序反了
    // 会漏：PgPool::close() 要等服务端把连接收干净，上面的第二个池到作用域
    // 末尾才释放，调用方外面再套个 timeout(.., db.cleanup()) 就把 future 掐了。
    assert_no_test_databases(&pg.url(), E2E_DB_PREFIX).await;
    assert_no_test_databases(&pg.url(), "flow_test_").await;

    // ---- 防线 2：删掉一个「真带匿名 volume」的容器，不能留孤儿 ----
    let probe = start_probe_container();
    let mounted = container_anonymous_volumes(&probe);
    assert!(
        !mounted.is_empty(),
        "探针容器应该带着匿名 volume（否则本断言什么都没验）"
    );
    remove_container(&probe);
    assert!(
        anonymous_dangling_volumes().is_empty(),
        "删容器必须收走它的匿名 volume（rm -f 得带 -v）：{:?}",
        anonymous_dangling_volumes()
    );

    // ---- 防线 3：上一轮漏下的孤儿要被启动清扫回收 ----
    // 复刻那条错误路径：起带 volume 的容器，然后**不带 -v** 地强删
    let stale = start_probe_container();
    docker_bare_rm(&stale);
    let orphaned = anonymous_dangling_volumes();
    assert!(
        orphaned.len() >= mounted.len(),
        "没造出孤儿 volume，本断言就什么都没验——检查 docker CLI / 镜像是否可用"
    );

    // 下一轮启动（shared() 已缓存，所以直接调清扫函数：同一段代码路径）
    let reclaimed = reclaim_orphan_volumes();
    assert_eq!(
        reclaimed,
        orphaned.len(),
        "启动清扫应把 {0} 个孤儿全数回收",
        orphaned.len()
    );
    assert!(
        anonymous_dangling_volumes().is_empty(),
        "孤儿 volume 未被收干净：{:?}",
        anonymous_dangling_volumes()
    );
}

/// 命名 volume 是开发者显式建的东西：清扫只该动匿名 volume，绝不能顺手删掉
/// 别人的命名 volume（名字再像也不行）。
#[tokio::test]
async fn janitor_never_touches_named_volumes() {
    let _guard = HYGIENE_LOCK.lock().await;
    docker_ok(&[
        "volume",
        "create",
        "--name",
        "flow-test-support-named-probe",
    ]);
    assert!(
        named_volume_exists(),
        "命名 volume 应该建出来了（docker CLI 可用？）"
    );

    reclaim_orphan_volumes();

    assert!(
        named_volume_exists(),
        "命名 volume 不该被匿名 volume 清扫碰掉——哪怕是 dangling 的"
    );
    docker_ok(&["volume", "rm", "-f", "flow-test-support-named-probe"]);
}

/// 复刻那条错误删除路径：强删但不带 `-v`（会漏下匿名 volume）。
fn docker_bare_rm(id: &str) {
    std::process::Command::new("docker")
        .args(["rm", "-f", id])
        .stdin(std::process::Stdio::null())
        .output()
        .expect("执行 docker rm 失败");
}

fn named_volume_exists() -> bool {
    docker_ok(&[
        "volume",
        "inspect",
        "--format",
        "{{.Name}}",
        "flow-test-support-named-probe",
    ])
}

fn docker_ok(args: &[&str]) -> bool {
    std::process::Command::new("docker")
        .args(args)
        .stdin(std::process::Stdio::null())
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

/// 服务器上不该留着任何给定前缀的测试库（cleanup 删库必须生效）。
async fn assert_no_test_databases(base_url: &str, prefix: &str) {
    let left = test_databases(base_url, prefix).await;
    assert!(
        left.is_empty(),
        "{prefix}* 测试库应被全部 DROP，残留：{left:?}"
    );
}
