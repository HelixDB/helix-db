#[tokio::main]
async fn main() -> server::ServerResult<()> {
    server::run_from_env().await
}
