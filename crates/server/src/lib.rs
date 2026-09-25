//! Reusable server runtime for HelixDB transports.

#![recursion_limit = "256"]

mod config;
mod grpc;
mod http;
mod state;
#[cfg(test)]
mod transport_contracts;

use std::error::Error;
use std::future::Future;
use std::sync::Arc;

use db::HelixDB;
use helix_metrics::{query, telemetry};
use state::ServerState;
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub use config::{CacheConfig, HybridCache, ServerConfig, ServerConfigError, StorageConfig};

/// Boxed error returned by the server runtime.
pub type ServerResult<T> = Result<T, Box<dyn Error + Send + Sync + 'static>>;

/// Maximum encoded query body accepted by either public transport.
pub const MAX_QUERY_BODY_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const TENANT_ID_HEADER_NAME: &str = "x-helix-tenant-id";

pub(crate) fn query_metrics_tenant_id(value: Option<&str>) -> Option<query::TenantId> {
    value.and_then(|value| query::TenantId::new(value).ok())
}

/// Initialize tracing from `RUST_LOG`, falling back to server defaults.
pub fn init_tracing_from_env() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "helix_db_server=info,tower_http=info".into());
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

/// Load configuration from the environment and run until process shutdown.
///
/// Unix processes shut down gracefully on either `SIGINT` or `SIGTERM`, so
/// container runtimes can stop the server without bypassing database cleanup.
/// Other platforms use their standard Ctrl-C notification.
pub async fn run_from_env() -> ServerResult<()> {
    init_tracing_from_env();
    let config = ServerConfig::from_env()?;
    run_with_shutdown(config, shutdown_signal()).await
}

/// Opens the configured database, first allowing the open files a hybrid
/// disk cache needs. Every runner opens storage through here.
async fn open_database(config: &ServerConfig) -> ServerResult<Arc<HelixDB>> {
    #[cfg(unix)]
    config
        .required_open_files()
        .map(ensure_open_file_limit)
        .transpose()?;
    Ok(Arc::new(
        HelixDB::open_for_server(config.db_source(), config.db_config()).await?,
    ))
}

/// Raises the soft open-file limit to the hard limit for a hybrid disk cache.
///
/// `required` is a floor, not an estimate of peak use: the full-text tier
/// also holds one descriptor per disk-opened split, bounded by bytes rather
/// than count. So the soft limit goes to the hard limit, as container
/// runtimes commonly default it to 1024. A hard limit below `required`
/// fails startup with an error naming `HELIX_DISK_CACHE_BYTES` instead of a
/// later `EMFILE` inside the cache. macOS also caps descriptors at
/// `kern.maxfilesperproc`, which counts as part of the hard limit.
#[cfg(unix)]
fn ensure_open_file_limit(required: u64) -> Result<(), ServerConfigError> {
    let limit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    // `None` is unlimited.
    let hard = limit
        .maximum
        .into_iter()
        .chain(max_files_per_process())
        .min();
    hard.filter(|&hard| hard < required)
        .map_or(Ok(()), |hard| {
            Err(ServerConfigError::OpenFileLimit {
                required,
                limit: hard,
            })
        })?;
    if limit.current == hard {
        return Ok(());
    }
    rustix::process::setrlimit(
        rustix::process::Resource::Nofile,
        rustix::process::Rlimit {
            current: hard,
            maximum: limit.maximum,
        },
    )
    .map_err(|source| ServerConfigError::RaiseOpenFileLimit {
        required,
        source: source.into(),
    })?;
    tracing::info!(
        from = ?limit.current,
        to = ?hard,
        required,
        "raised the soft open-file limit for the disk cache"
    );
    Ok(())
}

/// The per-process descriptor cap macOS enforces on top of `RLIMIT_NOFILE`,
/// and above which it rejects a soft limit.
#[cfg(target_os = "macos")]
fn max_files_per_process() -> Option<u64> {
    let mut value: libc::c_int = 0;
    let mut size = std::mem::size_of::<libc::c_int>();
    // SAFETY: the name is NUL-terminated, and `value` and `size` describe one
    // writable `c_int`, the type of `kern.maxfilesperproc`.
    let status = unsafe {
        libc::sysctlbyname(
            c"kern.maxfilesperproc".as_ptr(),
            std::ptr::from_mut(&mut value).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    (status == 0)
        .then_some(value)
        .and_then(|value| u64::try_from(value).ok())
}

/// Other Unix kernels enforce `RLIMIT_NOFILE` alone.
#[cfg(all(unix, not(target_os = "macos")))]
fn max_files_per_process() -> Option<u64> {
    None
}

/// Formats an error and every `source()` beneath it, one per line, so a
/// wrapper whose message is generic still shows its cause.
///
/// # Examples
///
/// ```
/// let error = std::io::Error::other("disk on fire");
/// assert_eq!(server::error_report(&error), "Error: disk on fire");
/// ```
pub fn error_report(error: &(dyn Error + 'static)) -> String {
    std::iter::successors(error.source(), |&cause| cause.source())
        .fold(format!("Error: {error}"), |report, cause| {
            format!("{report}\ncaused by: {cause}")
        })
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        let Ok(mut terminate) = terminate else {
            let _ = tokio::signal::ctrl_c().await;
            return;
        };

        first_shutdown(tokio::signal::ctrl_c(), terminate.recv()).await;
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(unix)]
async fn first_shutdown<Interrupt, Terminate>(interrupt: Interrupt, terminate: Terminate)
where
    Interrupt: Future,
    Terminate: Future,
{
    tokio::select! {
        _ = interrupt => {}
        _ = terminate => {}
    }
}

/// Open the configured database and run all transports until Ctrl-C.
pub async fn run_until_ctrl_c(config: ServerConfig) -> ServerResult<()> {
    let db = open_database(&config).await?;
    run_open_database_until_shutdown(config, db, async {
        tokio::signal::ctrl_c().await?;
        Ok(())
    })
    .await
}

/// Open the configured database and run until an embedding requests shutdown.
///
/// This is the deterministic lifecycle boundary used by service managers and
/// tests that already own their shutdown signal.
///
/// # Examples
///
/// ```
/// # tokio_test::block_on(async {
/// use server::{ServerConfig, StorageConfig};
///
/// let config = ServerConfig {
///     http_addr: "127.0.0.1:0".parse().unwrap(),
///     grpc_addr: "127.0.0.1:0".parse().unwrap(),
///     db_path: "server-shutdown-example".to_string(),
///     storage: StorageConfig::Memory,
/// };
/// server::run_with_shutdown(config, async {}).await.unwrap();
/// # });
/// ```
pub async fn run_with_shutdown(
    config: ServerConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> ServerResult<()> {
    let db = open_database(&config).await?;
    run_open_database_until_shutdown(config, db, async move {
        shutdown.await;
        Ok(())
    })
    .await
}

/// Runs transports for one already-open exact database identity.
async fn run_open_database_until_shutdown(
    config: ServerConfig,
    db: Arc<HelixDB>,
    shutdown: impl Future<Output = ServerResult<()>> + Send + 'static,
) -> ServerResult<()> {
    let (query_metrics, query_metrics_runtime) = server_query_metrics();
    let state = ServerState::new(Arc::clone(&db), query_metrics);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let http_config = config.clone();
    let http_state = state.clone();
    let http_shutdown = shutdown_rx.clone();
    let http_task: JoinHandle<ServerResult<()>> = tokio::spawn(async move {
        http::serve(http_config.http_addr, http_state, http_shutdown)
            .await
            .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync + 'static>)
    });

    let grpc_config = config;
    let grpc_state = state;
    let grpc_shutdown = shutdown_rx;
    let grpc_task: JoinHandle<ServerResult<()>> = tokio::spawn(async move {
        grpc::serve(grpc_config.grpc_addr, grpc_state, grpc_shutdown)
            .await
            .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync + 'static>)
    });

    supervise_transports_and_close(shutdown_tx, http_task, grpc_task, shutdown, async move {
        if let Some(runtime) = query_metrics_runtime {
            runtime.shutdown().await;
        }
        db.close()
            .await
            .map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync + 'static>)
    })
    .await
}

/// Joins both transports before closing the exact database they share.
///
/// A signal or the first completed transport broadcasts shutdown. The peer is
/// always joined before the database-close future is polled, including when a
/// transport fails, so no request task can outlive storage or the index worker.
async fn supervise_transports_and_close<Signal, Close>(
    shutdown_tx: watch::Sender<bool>,
    mut http_task: JoinHandle<ServerResult<()>>,
    mut grpc_task: JoinHandle<ServerResult<()>>,
    signal: Signal,
    close: Close,
) -> ServerResult<()>
where
    Signal: Future<Output = ServerResult<()>>,
    Close: Future<Output = ServerResult<()>>,
{
    tokio::pin!(signal);

    let transport_result: ServerResult<()> = tokio::select! {
        signal = &mut signal => {
            async {
                let _ = shutdown_tx.send(true);
                let http_result: ServerResult<()> = match http_task.await {
                    Ok(result) => result,
                    Err(error) => Err(Box::new(error)),
                };
                let grpc_result: ServerResult<()> = match grpc_task.await {
                    Ok(result) => result,
                    Err(error) => Err(Box::new(error)),
                };
                signal?;
                http_result?;
                grpc_result?;
                Ok(())
            }
            .await
        }
        result = &mut http_task => {
            async {
                let _ = shutdown_tx.send(true);
                let completed: ServerResult<()> = match result {
                    Ok(result) => result,
                    Err(error) => Err(Box::new(error)),
                };
                let grpc_result: ServerResult<()> = match grpc_task.await {
                    Ok(result) => result,
                    Err(error) => Err(Box::new(error)),
                };
                completed?;
                grpc_result?;
                Ok(())
            }
            .await
        }
        result = &mut grpc_task => {
            async {
                let _ = shutdown_tx.send(true);
                let completed: ServerResult<()> = match result {
                    Ok(result) => result,
                    Err(error) => Err(Box::new(error)),
                };
                let http_result: ServerResult<()> = match http_task.await {
                    Ok(result) => result,
                    Err(error) => Err(Box::new(error)),
                };
                completed?;
                http_result?;
                Ok(())
            }
            .await
        }
    };

    let close_result = close.await;
    match (transport_result, close_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(close_error)) => {
            tracing::error!(%close_error, "database close also failed after transport shutdown");
            Err(error)
        }
    }
}

fn server_query_metrics() -> (
    Option<helix_metrics::query::transport::OssQueryMetrics>,
    Option<telemetry::Runtime>,
) {
    match helix_metrics::query::transport::start_oss_from_env(telemetry::Source::Server) {
        Ok(Some(started)) => (Some(started.recorder), Some(started.runtime)),
        Ok(None) => (None, None),
        Err(error) => {
            tracing::warn!(%error, "server query metrics are disabled");
            (None, None)
        }
    }
}

#[cfg(test)]
mod tests;
