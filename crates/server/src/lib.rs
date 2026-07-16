//! Reusable server runtime for HelixDB transports.

#![recursion_limit = "256"]

mod config;
mod grpc;
mod http;
mod state;

use std::error::Error;
use std::sync::Arc;

use db::{DbConfig, HelixDB, HelixRuntimeDependencies};
use state::ServerState;
use tokio::sync::watch;

pub use config::{ServerConfig, ServerConfigError, StorageConfig};

/// Boxed error returned by the server runtime.
pub type ServerResult<T> = Result<T, Box<dyn Error + Send + Sync + 'static>>;

/// Initialize tracing from `RUST_LOG`, falling back to server defaults.
pub fn init_tracing_from_env() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "helix_db_server=info,tower_http=info".into());
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

/// Load configuration from the environment and run until Ctrl-C.
pub async fn run_from_env() -> ServerResult<()> {
    init_tracing_from_env();
    let config = ServerConfig::from_env()?;
    run_until_ctrl_c(config).await
}

/// Open the configured database and run all transports until Ctrl-C.
pub async fn run_until_ctrl_c(config: ServerConfig) -> ServerResult<()> {
    let db_source = config.db_source();
    let db = Arc::new(HelixDB::open(db_source).await?);
    run_open_database_until_ctrl_c(config, db).await
}

/// Open shared storage with trusted coordinators and run until Ctrl-C.
///
/// The supplied dependencies are runtime authority rather than user
/// configuration. Memory-backed standalone servers should continue through
/// [`run_until_ctrl_c`], whose source creates one process-local token.
pub async fn run_until_ctrl_c_with_runtime_dependencies(
    config: ServerConfig,
    runtime_dependencies: HelixRuntimeDependencies,
) -> ServerResult<()> {
    let db_source = config.db_source();
    let db = Arc::new(
        HelixDB::open_with_runtime_dependencies(db_source, DbConfig::new(), runtime_dependencies)
            .await?,
    );
    run_open_database_until_ctrl_c(config, db).await
}

/// Runs transports for one already-open exact database identity.
async fn run_open_database_until_ctrl_c(
    config: ServerConfig,
    db: Arc<HelixDB>,
) -> ServerResult<()> {
    let state = ServerState::new(Arc::clone(&db));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let http_config = config.clone();
    let http_state = state.clone();
    let http_shutdown = shutdown_rx.clone();
    let mut http_task =
        tokio::spawn(
            async move { http::serve(http_config.http_addr, http_state, http_shutdown).await },
        );

    let grpc_config = config;
    let grpc_state = state;
    let grpc_shutdown = shutdown_rx;
    let mut grpc_task =
        tokio::spawn(
            async move { grpc::serve(grpc_config.grpc_addr, grpc_state, grpc_shutdown).await },
        );

    let transport_result: ServerResult<()> = tokio::select! {
        signal = tokio::signal::ctrl_c() => {
            async {
                let _ = shutdown_tx.send(true);
                signal?;
                http_task.await??;
                grpc_task.await??;
                Ok(())
            }
            .await
        }
        result = &mut http_task => {
            async {
                let _ = shutdown_tx.send(true);
                let completed = result?;
                grpc_task.await??;
                completed?;
                Ok(())
            }
            .await
        }
        result = &mut grpc_task => {
            async {
                let _ = shutdown_tx.send(true);
                let completed = result?;
                http_task.await??;
                completed?;
                Ok(())
            }
            .await
        }
    };

    let close_result = db.close().await;
    match (transport_result, close_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(Box::new(error)),
        (Err(error), Err(close_error)) => {
            tracing::error!(%close_error, "database close also failed after transport shutdown");
            Err(error)
        }
    }
}
