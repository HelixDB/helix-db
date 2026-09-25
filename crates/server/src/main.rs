use std::process::ExitCode;

/// Runs the server, printing a startup or runtime failure as its message
/// (which names the variable to fix) rather than its debug form.
#[tokio::main]
async fn main() -> ExitCode {
    let Err(error) = server::run_from_env().await else {
        return ExitCode::SUCCESS;
    };
    eprintln!("Error: {error}");
    ExitCode::FAILURE
}
