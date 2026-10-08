//! flow-cli：flow 工作流引擎命令行客户端（bin: flow-cli）。
//!
//! 纯 RPC 客户端：只连 flow-server 的 JSON-RPC 2.0 over WebSocket，
//! 命令定义与分发在 `flow_cli::cli`（[`flow_cli::run_from_args`]），
//! 退出码契约（0/1/2/4）在库侧统一维护。
//!
//! 运行：cargo run -p flow-cli -- workflow list

fn main() -> std::process::ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let code = flow_cli::run_from_args(&argv);
    if code != 0 {
        return std::process::ExitCode::from(code.clamp(0, 255) as u8);
    }
    std::process::ExitCode::SUCCESS
}
