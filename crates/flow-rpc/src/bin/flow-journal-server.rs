//! flow-journal-server：JSONL v2 产品服务（bin: flow-journal-server）。
//!
//! WS RPC（v2 全量产品面：workflow/run/触发器/模板/统一配置/密钥，v2 协议
//! + 物化数据形状）+ 下载/webhook HTTP（`[journal].http_addr`）+ journal 触发器。
//!
//! token 经 FLOW_JOURNAL_TOKEN 环境变量提供（≥32 字节；凭据不走配置文件）。
//! 数据目录：FLOW_JOURNAL_DATA_DIR / [journal].data_dir。
//!
//! 服务主体在 [`flow_rpc::serve_journal_product`]——backend-e2e 的被测进程、
//! backend-perf 的自举服务模式共用同一份实现，「被测的就是生产进程」。
//! 本入口只做三件事：rustls crypto provider 安装（进程级一次）、统一配置
//! 加载（`--config` / FLOW_CONFIG / ./flow.toml / 平台目录）、多线程 runtime。
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
    match runtime.block_on(flow_rpc::serve_journal_product(loaded.config, loaded.path)) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("flow-journal-server 异常退出：{err}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// 配置加载失败的统一出口。
fn exit_fail(err: &dyn std::fmt::Display) -> std::process::ExitCode {
    eprintln!("error: {err}");
    std::process::ExitCode::FAILURE
}
