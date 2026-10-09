//! flow-server：JSON-RPC 2.0 over WebSocket 服务（bin: flow-server）。
//!
//! 服务主体（RPC 模块 + cron 调度 + webhook HTTP）在 [`flow_rpc::run`]——
//! backend-e2e 的被测进程、backend-perf 的自举服务模式共用同一份实现，
//! 「被测的就是生产进程」。本入口只做三件事：
//! - rustls crypto provider 安装（进程级一次）；
//! - 统一配置加载（`--config` / FLOW_CONFIG / ./flow.toml / 平台目录）；
//! - runtime 构造（多线程；v1 sqlite 的 current_thread 形态已随其后端删除）。
//!
//! 运行：cargo run -p flow-rpc --bin flow-server

use std::path::PathBuf;

use clap::Parser;

/// flow-server：JSON-RPC 2.0 over WebSocket 服务（+ cron 调度 + webhook HTTP）。
#[derive(Parser)]
#[command(
    name = "flow-server",
    version,
    about = "flow 工作流引擎服务",
    long_about = "flow 工作流引擎服务。\n\n\
                  配置分层：CLI 参数 > 环境变量 > 配置文件（--config / FLOW_CONFIG / \
                  ./flow.toml / 平台配置目录）> 内置默认值。\n\
                  后端在启动时用 storage.backend 选择（sqlite 缺省 | postgres），\
                  不在构建期区分。",
    arg_required_else_help = false
)]
struct Server {
    /// 统一配置文件路径（缺省：FLOW_CONFIG → ./flow.toml → 平台配置目录）。
    #[arg(long, global = true, env = "FLOW_CONFIG")]
    config: Option<PathBuf>,
}

fn main() -> std::process::ExitCode {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let server = match Server::try_parse() {
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
    // 配置文件在构造 runtime 之前同步加载：runtime 形态（current_thread /
    // multi_thread）取决于 storage.backend。
    let loaded = match flow_config::Config::load(server.config.as_deref()) {
        Ok(loaded) => loaded,
        Err(err) => return exit_fail(&err),
    };
    // v1 sqlite 的 current_thread 单写者形态已随其后端删除；统一多线程
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("server tokio runtime");
    match runtime.block_on(flow_rpc::run(loaded)) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("flow-server 异常退出：{err}");
            std::process::ExitCode::FAILURE
        }
    }
}

/// 配置加载失败的统一出口。
fn exit_fail(err: &dyn std::fmt::Display) -> std::process::ExitCode {
    eprintln!("error: {err}");
    std::process::ExitCode::FAILURE
}
