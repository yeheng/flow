//! runner 入口。生命周期：
//!
//! 1. 同步读 stdin `{api_key, prompt, workdir}`（密钥只经这里进结构体）；
//! 2. 固定 warn 日志 filter（不继承 `RUST_LOG`——conga 的 debug 层打完整
//!    请求体，不能进被持久化的 stderr 尾部）；
//! 3. `headless_prepare(roots=cwd, project_dir=workdir)`：钉 conga 状态根、
//!    拒绝 env 携带的 `CONGA_LLM_KEY`（全部在主线程、创建 runtime 之前）；
//! 4. current_thread runtime：`build_headless`（provider/tunables 从 env
//!    解析，key 来自参数）→ `run_turn` → 汇总单行输出 → 清理会话资源。
//!
//! 退出码：0 完成（末条 EndTurn）/ 1 回合失败或 max_turns 耗尽 / 130 中止
//! （防御性）/ 2 输入或装配错误。

use flow_harness_agent::{summarize, RunnerInput, RunnerOutcome};
use std::io::Write;
use std::process::ExitCode;

fn emit(outcome: &RunnerOutcome) {
    let mut line = serde_json::to_string(outcome).unwrap_or_else(|_| {
        r#"{"text":"","stop_reason":"error","error":"outcome serialization failed"}"#.into()
    });
    line.push('\n');
    let mut lock = std::io::stdout().lock();
    let _ = lock.write_all(line.as_bytes());
    let _ = lock.flush();
}

fn assembly_error(message: String) -> ExitCode {
    emit(&RunnerOutcome {
        text: String::new(),
        stop_reason: "error".into(),
        error: Some(message),
        exit_code: 2,
    });
    ExitCode::from(2)
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::filter::EnvFilter::new(
            "conga=warn,conga_host=warn,flow_harness_agent=warn",
        ))
        .with_writer(std::io::stderr)
        .init();

    let input: RunnerInput = match serde_json::from_reader(std::io::stdin()) {
        Ok(input) => input,
        Err(error) => return assembly_error(format!("runner stdin payload invalid: {error}")),
    };
    // executor 把 cwd 设为本次 run 的临时根（HOME/TMPDIR 同根）；状态与
    // 会话全部落在里面，退出后由 executor 递归删除。
    let roots = match std::env::current_dir() {
        Ok(dir) => dir,
        Err(error) => return assembly_error(format!("runner cwd unavailable: {error}")),
    };
    if let Err(error) = conga_host::headless::headless_prepare(&roots, &input.workdir) {
        return assembly_error(error.to_string());
    }

    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => return assembly_error(format!("runner runtime build failed: {error}")),
    };
    let outcome = runtime.block_on(async {
        let host = match conga_host::headless::build_headless(
            &input.api_key,
            &roots,
            conga_host::permission::Mode::FullAuto,
        )
        .await
        {
            Ok(host) => host,
            Err(error) => {
                return RunnerOutcome {
                    text: String::new(),
                    stop_reason: "error".into(),
                    error: Some(error.to_string()),
                    exit_code: 2,
                }
            }
        };
        let turn = host.run_turn(&input.prompt, |_| {}).await;
        let outcome = match turn {
            Ok(summary) => summarize(&summary),
            Err(error) => RunnerOutcome {
                text: String::new(),
                stop_reason: "error".into(),
                error: Some(error.to_string()),
                exit_code: 1,
            },
        };
        conga_host::session_cleanup::cleanup_session_resources(&host.session().current_id()).await;
        outcome
    });
    let code = outcome.exit_code;
    emit(&outcome);
    ExitCode::from(code as u8)
}
