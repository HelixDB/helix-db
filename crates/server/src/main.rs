use std::process::ExitCode;

/// The server's allocator. glibc malloc/free was 30-40% of server CPU under
/// search load; on 2 CPUs mimalloc gave 36-44% more throughput and 26-30% less
/// CPU per request, levelled memory off under sustained load and returned some
/// when idle. It reads the page size at runtime, so one build runs on 4K, 16K
/// and 64K page arm64 kernels (jemalloc fixes the page size at build time).
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
