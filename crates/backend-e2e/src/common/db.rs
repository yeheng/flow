//! 每个 Postgres 用例一个独占数据库：创建、schema 初始化、用完即 DROP。
//!
//! 库名带时间戳 + uuid：并行/连续运行互不撞名；DROP 前先
//! `pg_terminate_backend` 踢掉残留连接（服务进程刚 kill 掉时连接还在）。

use std::sync::OnceLock;

use sqlx::PgPool;
use uuid::Uuid;

use crate::common::container::E2E_DB_PREFIX;

pub struct TestDb {
    /// 指到本用例数据库的连接串（传给被测服务进程的 FLOW_DATABASE_URL）。
    pub url: String,
    name: String,
    /// maintenance 库（postgres）连接串：建库/删库用。
    server_url: String,
}

impl TestDb {
    /// 在共享容器上开一个全新数据库并初始化 schema。
    pub async fn create(base_url: &str) -> TestDb {
        sweep_stale_databases(base_url).await;
        let server_url = replace_db(base_url, "postgres");
        let name = format!(
            "{}{}_{}",
            E2E_DB_PREFIX,
            chrono::Utc::now().format("%Y%m%d%H%M%S"),
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
        flow_pg::schema::init(&pool)
            .await
            .unwrap_or_else(|e| panic!("初始化 schema 失败：{e}"));
        pool.close().await;
        TestDb {
            url,
            name,
            server_url,
        }
    }

    /// 踢掉残余连接并 DROP 数据库（幂等：已消失也算干净）。
    pub async fn cleanup(&self) {
        let Ok(admin) = PgPool::connect(&self.server_url).await else {
            return;
        };
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
        admin.close().await;
    }
}

/// 把 `postgres://.../<db>` 的库名换掉。
fn replace_db(url: &str, db: &str) -> String {
    let cut = url.rfind('/').expect("连接串必须带库名");
    format!("{}/{}", &url[..cut], db)
}

/// 进程内一次性清扫 `e2e_%` 残留库（上次运行被 SIGKILL 时用例内 DROP 没跑到）。
/// 容器是本进程独占的，删自己前缀的库没有并行冲突。
async fn sweep_stale_databases(base_url: &str) {
    static SWEEPED: OnceLock<()> = OnceLock::new();
    if SWEEPED.set(()).is_err() {
        return;
    }
    let server_url = replace_db(base_url, "postgres");
    let Ok(admin) = PgPool::connect(&server_url).await else {
        return;
    };
    let rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT datname FROM pg_database WHERE datname LIKE '{E2E_DB_PREFIX}%'"
    )))
    .fetch_all(&admin)
    .await
    .unwrap_or_default();
    for (name,) in rows {
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
