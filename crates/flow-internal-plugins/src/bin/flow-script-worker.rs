use flow_internal_plugins::script::{ScriptTask, SCRIPT_V1};

fn main() -> std::process::ExitCode {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("worker runtime");
    let result = runtime.block_on(flow_dispatcher::worker::run_worker(
        SCRIPT_V1,
        ScriptTask::execute_job,
    ));
    runtime.shutdown_timeout(std::time::Duration::from_millis(100));
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("flow-script-worker: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
