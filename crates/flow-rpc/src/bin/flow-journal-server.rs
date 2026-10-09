//! flow-journal-server：JSONL v2 产品服务（bin: flow-journal-server）。
//!
//! WS RPC（v2 全量产品面：workflow/run/触发器/模板/统一配置/密钥，v2 协议
//! + 物化数据形状）+ 下载/webhook HTTP（`[journal].http_addr`）+ journal 触发器。
//! token 经 FLOW_JOURNAL_TOKEN 环境变量提供（≥32 字节；凭据不走配置文件）。
//! 数据目录：FLOW_JOURNAL_DATA_DIR / [journal].data_dir。
//!
//! 运行：FLOW_JOURNAL_TOKEN=... cargo run -p flow-rpc --bin flow-journal-server

use std::path::PathBuf;

use clap::Parser;

/// flow-journal-server：JSONL v2 产品服务（WS RPC + 下载/触发 HTTP）。
#[derive(Parser)]
#[command(
    name = "flow-journal-server",
    version,
    about = "JSONL v2 产品服务（WS RPC + 下载/触发器 HTTP）",
    long_about = "JSONL v2 产品服务。\n\n\
                  WS RPC（v2 全量产品面：workflow/run/触发器/模板/统一配置/密钥，v2 协议\
                  + 物化数据形状）+ 下载与 webhook HTTP（[journal].http_addr）。\
                  token：FLOW_JOURNAL_TOKEN（≥32 字节）；数据目录：FLOW_JOURNAL_DATA_DIR \
                  或配置 [journal].data_dir。浏览器前端的 <webroot>/config.json 指到这里。"
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
    match runtime.block_on(journal_server(loaded.config, loaded.path)) {
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

async fn journal_server(
    config: flow_config::Config,
    config_path: Option<PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    use flow_backend::journal::JournalBackend;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,flow_engine=debug,flow_rpc=debug,flow_backend=debug".into()),
        )
        .init();

    let root = std::env::var("FLOW_JOURNAL_DATA_DIR")
        .ok()
        .or_else(|| config.journal.data_dir.clone())
        .map(PathBuf::from)
        .ok_or("journal 数据目录缺失：配置 [journal].data_dir 或环境变量 FLOW_JOURNAL_DATA_DIR")?;
    let token = std::env::var("FLOW_JOURNAL_TOKEN")?;
    let addr: std::net::SocketAddr = config.journal.addr.parse()?;
    let backend = JournalBackend::open(&root, Default::default()).await?;
    let download_addr: std::net::SocketAddr = config.journal.http_addr.parse()?;
    let listener = tokio::net::TcpListener::bind(download_addr).await?;
    // 记实际绑到的地址（端口 0 时日志才有意义）；字段名是测试 harness 的
    // 就绪标记（flow-test-support::io::spawn_reporting_ports 的 HTTP_MARKER），
    // 不能改。
    let bound_http = listener.local_addr()?;
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
    // 产品装配：统一配置面 + 持久化密钥（密钥在 data_dir 下，与桌面同规则）
    let data_dir = PathBuf::from(&config.storage.data_dir);
    let file_config = match &config_path {
        Some(path) => {
            let text = std::fs::read_to_string(path)?;
            flow_rpc::toml_config_from_str(&text)?
        }
        None => flow_config::Config::default(),
    };
    let config_state = flow_rpc::ConfigState {
        config: std::sync::RwLock::new(file_config),
        path: config_path,
        env_overrides: Vec::new(),
    };
    let (state, _secrets) =
        flow_rpc::AppState::for_production(flow_backend::AnyBackend::Journal(backend.clone()), config_state, &data_dir).await;
    let module = flow_rpc::journal_v2::module_product(backend.clone(), token, Some(state))?;
    let (server, addr) = flow_rpc::journal_v2::serve_product(module, addr).await?;
    tracing::info!(
        local_addr = %addr,
        http_addr = %bound_http,
        data_dir = %root.display(),
        "flow-journal-server 已启动 (JSONL v2 WS RPC + 下载/触发器 HTTP)"
    );
    flow_backend::start_execution(&config.execution, &backend).await?;
    // cron 触发器：[server].scheduler_enabled=false 时不启动（与桌面同一开关）
    let scheduler = if config.server.scheduler_enabled {
        flow_rpc::journal_triggers::start(backend.clone(), config.journal_trigger_tick())
    } else {
        tracing::info!("scheduler_enabled=false，cron 触发器未启动");
        tokio::spawn(async {})
    };
    tokio::signal::ctrl_c().await?;
    tracing::info!("收到中断信号，正在停止");
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
