use std::sync::LazyLock;
use std::sync::atomic::{self, AtomicUsize};
use std::thread::available_parallelism;
use std::time::{Duration, Instant};
use std::{collections::HashMap, sync::Arc};

use axum::body::Body;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use core_affinity::{CoreId, set_for_current};
use helix_metrics::events::{EventType, QueryErrorEvent, QuerySuccessEvent};
use sonic_rs::json;
use tower_http::compression::CompressionLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tracing::{info, trace, warn};

use super::router::router::{HandlerFn, HelixRouter};
#[cfg(feature = "dev-instance")]
use crate::helix_gateway::builtin::all_nodes_and_edges::nodes_edges_handler;
#[cfg(feature = "dev-instance")]
use crate::helix_gateway::builtin::node_by_id::node_details_handler;
#[cfg(feature = "dev-instance")]
use crate::helix_gateway::builtin::node_connections::node_connections_handler;
#[cfg(feature = "dev-instance")]
use crate::helix_gateway::builtin::nodes_by_label::nodes_by_label_handler;
use crate::helix_gateway::introspect_schema::introspect_schema_handler;
use crate::helix_gateway::worker_pool::WorkerPool;
use crate::protocol;
use crate::{
    helix_engine::traversal_core::{HelixGraphEngine, HelixGraphEngineOpts},
    helix_gateway::mcp::mcp::MCPHandlerFn,
};

pub struct GatewayOpts {}

impl GatewayOpts {
    pub const DEFAULT_POOL_SIZE: usize = 8;
    pub const DEFAULT_MAX_REQUEST_SIZE: usize = 100 * 1024 * 1024; // 100MB
    pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60); // 60s
}

pub static HELIX_METRICS_CLIENT: LazyLock<helix_metrics::HelixMetricsClient> =
    LazyLock::new(helix_metrics::HelixMetricsClient::new);

pub struct HelixGateway {
    address: String,
    worker_size: usize,
    io_size: usize,
    graph_access: Arc<HelixGraphEngine>,
    router: Arc<HelixRouter>,
    opts: Option<HelixGraphEngineOpts>,
    cluster_id: Option<String>,
}

impl HelixGateway {
    pub fn new(
        address: &str,
        graph_access: Arc<HelixGraphEngine>,
        worker_size: usize,
        io_size: usize,
        routes: Option<HashMap<String, HandlerFn>>,
        mcp_routes: Option<HashMap<String, MCPHandlerFn>>,
        opts: Option<HelixGraphEngineOpts>,
    ) -> HelixGateway {
        let router = Arc::new(HelixRouter::new(routes, mcp_routes));
        let cluster_id = std::env::var("CLUSTER_ID").ok();

        // Apply environment variable overrides
        let worker_size = std::env::var("HELIX_WORKER_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(worker_size);

        let io_size = std::env::var("HELIX_IO_THREADS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(io_size);

        HelixGateway {
            address: address.to_string(),
            graph_access,
            router,
            worker_size,
            io_size,
            opts,
            cluster_id,
        }
    }

    pub fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        trace!("Starting Helix Gateway");
        let (io_setter, worker_setter) = match core_affinity::get_core_ids() {
            Some(all_cores) => {
                let io_cores = CoreSetter::new(&all_cores[0..self.io_size]);
                let worker_cores = CoreSetter::new(&all_cores[self.io_size..]);
                (Some(io_cores), Some(worker_cores))
            }
            None => {
                warn!("Failed to get core ids");
                (None, None)
            }
        };

        if let Ok(total_cores) = available_parallelism()
            && total_cores.get() < self.worker_size + self.io_size
        {
            warn!(
                "using more threads ({} io + {} worker = {}) than available cores ({}).",
                self.io_size,
                self.worker_size,
                self.io_size + self.worker_size,
                total_cores.get()
            );
        }

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(self.io_size)
            .on_thread_start({
                let local_setter = io_setter.clone();
                move || {
                    if let Some(s) = &local_setter {
                        s.set_current();
                    }
                }
            })
            .enable_all()
            .build()?;

        let rt = Arc::new(rt);

        let worker_pool = WorkerPool::new(
            self.worker_size,
            worker_setter,
            self.graph_access.clone(),
            self.router.clone(),
            rt.clone(),
        );

        // Get configurable values from environment
        let max_request_size = std::env::var("HELIX_MAX_REQUEST_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(GatewayOpts::DEFAULT_MAX_REQUEST_SIZE);

        let request_timeout = std::env::var("HELIX_REQUEST_TIMEOUT")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or(GatewayOpts::DEFAULT_REQUEST_TIMEOUT);

        info!(
            "Starting gateway with worker_threads={}, io_threads={}, max_request_size={}, timeout={}s",
            self.worker_size,
            self.io_size,
            max_request_size,
            request_timeout.as_secs()
        );

        let mut axum_app = axum::Router::new();

        axum_app = axum_app
            .route("/{*path}", post(post_handler))
            .route("/introspect", get(introspect_schema_handler));

        #[cfg(feature = "dev-instance")]
        {
            axum_app = axum_app
                .route("/nodes-edges", get(nodes_edges_handler))
                .route("/nodes-by-label", get(nodes_by_label_handler))
                .route("/node-connections", get(node_connections_handler))
                .route("/node-details", get(node_details_handler));
        }

        // Add middleware layers
        let axum_app = axum_app
            .layer(RequestBodyLimitLayer::new(max_request_size))
            .layer(TimeoutLayer::new(request_timeout))
            .layer(CompressionLayer::new())
            .with_state(Arc::new(AppState {
                worker_pool,
                schema_json: self.opts.and_then(|o| o.config.schema),
                cluster_id: self.cluster_id,
            }));

        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind(&self.address).await.unwrap();

            // Note: TCP socket optimizations could be added here in the future
            // by using socket2 crate to set TCP_NODELAY, buffer sizes, etc.
            info!("Listener bound at {}", self.address);
            axum::serve(listener, axum_app)
                .with_graceful_shutdown(shutdown_signal())
                .await
                .unwrap()
        });

        Ok(())
    }
}

async fn shutdown_signal() {
    // Respond to either Ctrl-C (SIGINT) or SIGTERM (e.g. `kill` or systemd stop)
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl-C, starting graceful shutdown…");
        }
        // #[cfg(unix)]
        _ = sigterm() => {
            info!("Received SIGTERM, starting graceful shutdown…");
        }
    }
}

async fn sigterm() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        term.recv().await;
    }
    #[cfg(not(unix))]
    {
        use tokio::signal::ctrl_c;
        ctrl_c().await;
    }
}

async fn post_handler(
    State(state): State<Arc<AppState>>,
    req: protocol::request::Request,
) -> axum::http::Response<Body> {
    // #[cfg(feature = "metrics")]
    let start_time = Instant::now();
    let body = req.body.to_vec();
    let query_name = req.name.clone();
    let res = state.worker_pool.process(req).await;

    match res {
        Ok(r) => {
            // #[cfg(feature = "metrics")]
            {
                HELIX_METRICS_CLIENT.send_event(
                    EventType::QuerySuccess,
                    QuerySuccessEvent {
                        cluster_id: state.cluster_id.clone(),
                        query_name,
                        time_taken_usec: start_time.elapsed().as_micros() as u32,
                    },
                );
            }
            r.into_response()
        }
        Err(e) => {
            info!(?e, "Got error");
            HELIX_METRICS_CLIENT.send_event(
                EventType::QueryError,
                QueryErrorEvent {
                    cluster_id: state.cluster_id.clone(),
                    query_name,
                    input_json: sonic_rs::to_string(&body).ok(),
                    output_json: sonic_rs::to_string(&json!({ "error": e.to_string() })).ok(),
                    time_taken_usec: start_time.elapsed().as_micros() as u32,
                },
            );
            e.into_response()
        }
    }
}

pub struct AppState {
    pub worker_pool: WorkerPool,
    pub schema_json: Option<String>,
    pub cluster_id: Option<String>,
}

#[derive(Clone)]
pub struct CoreSetter(Arc<CoreSetterInner>);

pub struct CoreSetterInner {
    cores: Vec<CoreId>,
    index: AtomicUsize,
}

impl CoreSetter {
    pub fn new(cores: &[CoreId]) -> Self {
        Self(Arc::new(CoreSetterInner {
            cores: cores.to_vec(),
            index: AtomicUsize::new(0),
        }))
    }

    pub fn set_current(&self) {
        let inner = &self.0;
        let idx = inner.index.fetch_add(1, atomic::Ordering::SeqCst);
        match inner.cores.get(idx) {
            Some(c) => {
                set_for_current(*c);
                trace!("Set core affinity to: {c:?}");
            }
            None => warn!("Tried to set core affinity, but all cores already used"),
        };
    }
}
