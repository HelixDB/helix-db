use std::process::ExitCode;

/// The server allocates through mimalloc rather than the platform allocator.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Runs the server, printing a failure as its message and every cause
/// beneath it rather than its debug form.
#[tokio::main]
async fn main() -> ExitCode {
    #[cfg(feature = "async-index-benchmark")]
    let result = server::benchmark::run_from_env().await;
    #[cfg(not(feature = "async-index-benchmark"))]
    let result = server::run_from_env().await;
    let Err(error) = result else {
        return ExitCode::SUCCESS;
    };
    eprintln!("{}", server::error_report(&*error));
    ExitCode::FAILURE
}
