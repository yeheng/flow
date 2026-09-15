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
                .unwrap_or_else(|_| "info,flow_engine=debug,flow_rpc=debug".into()),
        )
        .init();

    let data_dir = PathBuf::from(std::env::var("FLOW_DATA_DIR").unwrap_or_else(|_| "data".into()));
    let db_path = PathBuf::from(
        std::env::var("FLOW_DB").unwrap_or_else(|_| data_dir.join("flow.db").display().to_string()),
    );
    let addr: SocketAddr = std::env::var("FLOW_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9800".into())
        .parse()?;

    let store = Arc::new(Store::open(&db_path).await?);
    let engine = Arc::new(Engine::new(&data_dir, Arc::new(StoreObserver::new(store.clone()))));
    let state = Arc::new(AppState {
        store,
        engine,
    });

    // 崩溃恢复：未结束的 run 从 event.jsonl 折叠回来继续跑
    for (run_id, error) in recover_unfinished(&state).await? {
        tracing::error!(run_id = %run_id, error = %error, "恢复 run 失败");
    }

    let (handle, local_addr) = serve(state, addr).await?;
    tracing::info!(%local_addr, db = %db_path.display(), data = %data_dir.display(), "flow-server 已启动 (JSON-RPC 2.0 over WebSocket)");

    tokio::signal::ctrl_c().await?;
    tracing::info!("收到中断信号，正在停止");
    handle.stop()?;
    handle.stopped().await;
    Ok(())
}
