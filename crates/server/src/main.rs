use std::process::ExitCode;

/// Runs the server, printing a failure as its message and every cause
/// beneath it rather than its debug form.
#[tokio::main]
async fn main() -> ExitCode {
    let Err(error) = server::run_from_env().await else {
        return ExitCode::SUCCESS;
    };
    eprintln!("{}", server::error_report(&*error));
    ExitCode::FAILURE
}
