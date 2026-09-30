//! Opt-in v2 development service. Never opens the legacy SQLite/PG backend.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::PathBuf::from(std::env::var("FLOW_JOURNAL_DATA_DIR")?);
    let token = std::env::var("FLOW_JOURNAL_TOKEN")?;
    let addr = std::env::var("FLOW_JOURNAL_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9802".into())
        .parse()?;
    let backend = flow_backend::journal::JournalBackend::open(&root, Default::default()).await?;
    let download_addr: std::net::SocketAddr = std::env::var("FLOW_JOURNAL_HTTP_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:9803".into())
        .parse()?;
    if !download_addr.ip().is_loopback() {
        return Err("development downloads require loopback".into());
    }
    let listener = tokio::net::TcpListener::bind(download_addr).await?;
    let router = flow_rpc::journal_download::router(backend.clone(), token.clone())?;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut downloads = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let (server, addr) = flow_rpc::journal_v2::serve(backend.clone(), token, addr).await?;
    backend.start_execution().await?;
    eprintln!("JSONL development RPC listening on {addr}");
    tokio::signal::ctrl_c().await?;
    server.stop()?;
    server.stopped().await;
    let _ = stop.send(());
    if tokio::time::timeout(std::time::Duration::from_secs(5), &mut downloads)
        .await
        .is_err()
    {
        downloads.abort();
        let _ = downloads.await;
    }
    backend.close().await?;
    Ok(())
}
