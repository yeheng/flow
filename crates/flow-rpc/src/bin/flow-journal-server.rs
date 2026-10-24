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
//!
//! 本入口守住的边界：**只做进程级准备，不含任何产品逻辑**。rustls provider
//! 安装、配置加载、runtime 构建之后就把控制权交给
//! [`flow_rpc::serve_journal_product`]，因此「桌面内嵌服务」「测试 harness
//! 被测进程」「本 bin」跑的是同一份服务实现。
//!
//! 为什么必须共用一份实现：只要产品 bin 有一行独有的装配逻辑，测试里
//! 验证过的就不是线上跑的那份进程，回归结论会静默失效。

use std::path::PathBuf;

use clap::Parser;

/// 命令行参数：统一配置文件路径。
///
/// 只有一个参数是刻意的：产品面的一切都走统一配置文件或环境变量，bin
/// 不再携带第二套「启动开关」，避免「配置文件说一套、命令行说另一套」。
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

/// 进程入口：准备运行环境后启动产品服务。
///
/// 退出码语义：`--help` / `--version` 走 0（这是正常请求，不是失败），
/// 参数错误走 1；服务自身异常退出也走 1。
fn main() -> std::process::ExitCode {
    // rustls 的 crypto provider 是进程级单例：安装失败只说明别的 provider
    // 已装好，不影响本服务（本服务当前只走明文 WS/HTTP）。
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args = match JournalServer::try_parse() {
        Ok(args) => args,
        Err(error) => {
            // clap 把 --help/--version 也走 Err 返回：这两种是正常请求，
            // 不该按失败退出（脚本里 `--help` 返回非 0 会误判）。
            let success = matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            );
            let _ = error.print();
            std::process::exit(if success { 0 } else { 1 });
        }
    };
    // 配置加载失败直接退出：配置没读对就没法确定数据目录与端口，
    // 带着默认值继续跑只会连到错误的数据集。
    let loaded = match flow_config::Config::load(args.config.as_deref()) {
        Ok(loaded) => loaded,
        Err(err) => return exit_fail(&err),
    };
    // 多线程 runtime：journal 写入、投影与执行器会并发占用 CPU，
    // 单线程 runtime 会让它们互相拖慢。
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
///
/// 走 stderr + 退出码 1：不写日志框架（此时还没初始化），让 supervisor
/// 能直接读到原因。
fn exit_fail(err: &dyn std::fmt::Display) -> std::process::ExitCode {
    eprintln!("error: {err}");
    std::process::ExitCode::FAILURE
}
