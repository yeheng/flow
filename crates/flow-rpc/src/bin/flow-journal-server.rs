//! flow-journal-server：JSONL v2 开发服务（bin: flow-journal-server）。
//!
//! WS RPC（journal v2 方法集）+ 有界下载 HTTP（仅 loopback）+ journal 触发器。
//! token 经 FLOW_JOURNAL_TOKEN 环境变量提供（≥32 字节；凭据不走配置文件）。
//! 数据目录：FLOW_JOURNAL_DATA_DIR / [journal].data_dir。
//!
//! 运行：FLOW_JOURNAL_TOKEN=... cargo run -p flow-rpc --bin flow-journal-server

use std::path::PathBuf;

use clap::Parser;

/// flow-journal-server：JSONL v2 开发服务（WS RPC + 有界下载；仅 loopback）。
#[derive(Parser)]
#[command(
    name = "flow-journal-server",
    version,
    about = "JSONL v2 开发服务（WS RPC + 下载）",
    long_about = "JSONL v2 开发服务。\n\n\
                  WS RPC（journal v2 方法集）+ 有界下载 HTTP（仅 loopback）+ journal 触发器。\n\
                  token：FLOW_JOURNAL_TOKEN（≥32 字节）；数据目录：FLOW_JOURNAL_DATA_DIR \
                  或配置 [journal].data_dir。"
)]
struct JournalServer {
    /// 统一配置文件路径（缺省：FLOW_CONFIG → ./flow.toml → 平台配置目录）。
    #[arg(long, global = true, env = "FLOW_CONFIG")]
    config: Option<PathBuf>,
}

fn main() -> std::process::ExitCode {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args = match JournalServer::try_parse() {
        Ok(args) => args,
        Err(error) => {
            let success = matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            );
            let _ = error.print();
            std::process::exit(if success { 0 } else { 1 });
        }
    };
    let loaded = match flow_config::Config::load(args.config.as_deref()) {
        Ok(loaded) => loaded,
        Err(err) => return exit_fail(&err),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("journal-server tokio runtime");
    match runtime.block_on(journal_server(loaded.config)) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// 配置加载失败的统一出口。
fn exit_fail(err: &dyn std::fmt::Display) -> std::process::ExitCode {
    eprintln!("error: {err}");
    std::process::ExitCode::FAILURE
}

async fn journal_server(config: flow_config::Config) -> Result<(), Box<dyn std::error::Error>> {
    use flow_backend::journal::JournalBackend;

    let root = std::env::var("FLOW_JOURNAL_DATA_DIR")
        .ok()
        .or_else(|| config.journal.data_dir.clone())
        .map(PathBuf::from)
        .ok_or("journal 数据目录缺失：配置 [journal].data_dir 或环境变量 FLOW_JOURNAL_DATA_DIR")?;
    let token = std::env::var("FLOW_JOURNAL_TOKEN")?;
    let addr: std::net::SocketAddr = config.journal.addr.parse()?;
    let backend = JournalBackend::open(&root, Default::default()).await?;
    let download_addr: std::net::SocketAddr = config.journal.http_addr.parse()?;
    if !download_addr.ip().is_loopback() {
        return Err("development downloads require loopback".into());
    }
    let listener = tokio::net::TcpListener::bind(download_addr).await?;
    let router = flow_rpc::journal_download::router(backend.clone(), token.clone())?.merge(
        flow_rpc::journal_triggers::router(backend.clone(), token.clone()),
    );
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut downloads = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    let (server, addr) = flow_rpc::journal_v2::serve(backend.clone(), token, addr).await?;
    flow_backend::start_execution(&config.execution, &backend).await?;
    let scheduler =
        flow_rpc::journal_triggers::start(backend.clone(), config.journal_trigger_tick());
    eprintln!("JSONL development RPC listening on {addr}");
    tokio::signal::ctrl_c().await?;
    scheduler.abort();
    let _ = scheduler.await;
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
