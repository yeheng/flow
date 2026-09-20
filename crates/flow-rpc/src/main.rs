use std::net::SocketAddr;
use std::sync::Arc;

use flow_backend::open_from_env;
use flow_rpc::{serve, AppState};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                "info,flow_engine=debug,flow_rpc=debug,flow_backend=debug".into()
            }),
        )
        .init();

    let addr: SocketAddr = std::env::var("FLOW_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9800".into())
        .parse()?;

    // 后端选择只发生在这里一次：FLOW_BACKEND=sqlite（缺省，canonical：
    // SQLite + event.jsonl）| postgres（可替代：共享日志 + 租约 + inbox）。
    // 之后整条 RPC 链路只看 AnyBackend 枚举。
    let backend = open_from_env().await?;
    backend.start().await?;

    let state = Arc::new(AppState {
        backend: backend.clone(),
    });
    let (handle, local_addr) = serve(state, addr).await?;
    tracing::info!(
        %local_addr,
        backend = backend.name(),
        detail = backend.describe(),
        "flow-server 已启动 (JSON-RPC 2.0 over WebSocket)"
    );

    tokio::signal::ctrl_c().await?;
    tracing::info!("收到中断信号，正在停止");
    // 停机错误只能记日志：进程即将退出，没有重试的意义
    if let Err(err) = backend.shutdown().await {
        tracing::error!(error = %err, "后端停机失败");
    }
    handle.stop()?;
    handle.stopped().await;
    Ok(())
}
