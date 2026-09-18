use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use flow_engine::Engine;
use flow_rpc::{recover_unfinished, serve, AppState, StoreObserver};
use flow_store::Store;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,flow_engine=debug,flow_rpc=debug,flow_pg=debug".into()),
        )
        .init();

    let addr: SocketAddr = std::env::var("FLOW_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9800".into())
        .parse()?;
    let backend = std::env::var("FLOW_BACKEND").unwrap_or_else(|_| "sqlite".into());

    match backend.as_str() {
        "postgres" | "postgresql" => start_postgres(addr).await,
        "sqlite" => start_sqlite(addr).await,
        other => Err(format!("未知 FLOW_BACKEND：{other}（支持 sqlite | postgres）").into()),
    }
}

/// 单机模式（DESIGN.md）：SQLite 元数据 + event.jsonl。
async fn start_sqlite(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = PathBuf::from(std::env::var("FLOW_DATA_DIR").unwrap_or_else(|_| "data".into()));
    let db_path = PathBuf::from(
        std::env::var("FLOW_DB").unwrap_or_else(|_| data_dir.join("flow.db").display().to_string()),
    );

    let store = Arc::new(Store::open(&db_path).await?);
    let engine = Arc::new(Engine::new(
        &data_dir,
        Arc::new(StoreObserver::new(store.clone())),
    ));
    let state = Arc::new(AppState { store, engine });
    // 两阶段注入：launcher 依赖 Engine，Engine 的 Driver 需要 launcher
    state
        .engine
        .set_child_launcher(Arc::new(flow_rpc::child::LocalChildLauncher::new(
            state.store.clone(),
            state.engine.clone(),
        )));

    // 崩溃恢复：未结束的 run 从 event.jsonl 折叠回来继续跑
    for (run_id, error) in recover_unfinished(&state).await? {
        tracing::error!(run_id = %run_id, error = %error, "恢复 run 失败");
    }

    let (handle, local_addr) = serve(state, addr).await?;
    tracing::info!(%local_addr, backend = "sqlite", db = %db_path.display(), "flow-server 已启动 (JSON-RPC 2.0 over WebSocket)");

    tokio::signal::ctrl_c().await?;
    tracing::info!("收到中断信号，正在停止");
    handle.stop()?;
    handle.stopped().await;
    Ok(())
}

/// Postgres 模式（DISTRIBUTED.md）：共享日志 + epoch 租约 + 持久 inbox。
/// 小部署在同进程运行 gateway 和 executor（FLOW_ROLE 可拆分）。
async fn start_postgres(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("FLOW_DATABASE_URL")
        .map_err(|_| "Postgres 模式必须设置 FLOW_DATABASE_URL".to_string())?;
    let cfg = flow_pg::PgConfig::from_env();
    let engine = Arc::new(flow_pg::PgEngine::connect(&url, cfg).await?);
    tracing::info!(
        instance = engine.instance_id().unwrap_or("gateway"),
        role = ?engine.config().role,
        max_runs = engine.config().max_runs,
        ttl_ms = engine.config().lease_ttl.as_millis() as u64,
        "Postgres 后端已连接"
    );

    // executor 扫描循环（peer 模式唯一的租约获取入口）；gateway 角色不起
    let executor_task = if engine.config().role != flow_pg::Role::Gateway {
        let executor_engine = engine.clone();
        Some(tokio::spawn(async move {
            if let Err(err) = executor_engine.run_executor().await {
                tracing::error!(error = %err, "executor 扫描循环退出");
            }
        }))
    } else {
        None
    };

    let state = Arc::new(flow_rpc::pg::PgState {
        engine: engine.clone(),
    });
    let (handle, local_addr) = flow_rpc::pg::serve(state, addr).await?;
    tracing::info!(%local_addr, backend = "postgres", "flow-server 已启动 (JSON-RPC 2.0 over WebSocket)");

    tokio::signal::ctrl_c().await?;
    tracing::info!("收到中断信号，正在停止");
    // 先停 executor（abort inflight + 安全时释放租约），再关 RPC
    engine.shutdown().await;
    if let Some(executor_task) = executor_task {
        let _ = executor_task.await;
    }
    handle.stop()?;
    handle.stopped().await;
    Ok(())
}
