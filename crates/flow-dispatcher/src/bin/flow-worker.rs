#[tokio::main(flavor = "current_thread")]
async fn main() -> std::process::ExitCode {
    match flow_dispatcher::worker::run_worker("echo.v1", Ok).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("flow-worker: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
