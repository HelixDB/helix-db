use std::sync::LazyLock;
use std::sync::atomic::{self, AtomicUsize};
use std::time::Instant;
use std::{collections::HashMap, sync::Arc};

use axum::body::Body;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use core_affinity::CoreId;
use helix_metrics::events::{EventType, QueryErrorEvent, QuerySuccessEvent};
use sonic_rs::json;
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
    pub const DEFAULT_WORKERS_PER_CORE: usize = 5;
}

pub static HELIX_METRICS_CLIENT: LazyLock<helix_metrics::HelixMetricsClient> =
    LazyLock::new(helix_metrics::HelixMetricsClient::new);

pub struct HelixGateway {
    address: String,
    workers_per_core: usize,
    graph_access: Arc<HelixGraphEngine>,
    router: Arc<HelixRouter>,
    opts: Option<HelixGraphEngineOpts>,
    cluster_id: Option<String>,
}

impl HelixGateway {
    pub fn new(
        address: &str,
        graph_access: Arc<HelixGraphEngine>,
        workers_per_core: usize,
        routes: Option<HashMap<String, HandlerFn>>,
        mcp_routes: Option<HashMap<String, MCPHandlerFn>>,
        opts: Option<HelixGraphEngineOpts>,
    ) -> HelixGateway {
        let router = Arc::new(HelixRouter::new(routes, mcp_routes));
        let cluster_id = std::env::var("CLUSTER_ID").ok();
        HelixGateway {
            address: address.to_string(),
            graph_access,
            router,
            workers_per_core,
            opts,
            cluster_id,
        }
    }

    pub fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        trace!("Starting Helix Gateway");

        let all_core_ids = core_affinity::get_core_ids().expect("unable to get core IDs");

        let tokio_core_ids = all_core_ids.clone();
        let tokio_core_setter = Arc::new(CoreSetter::new(tokio_core_ids, 1));

        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(tokio_core_setter.num_threads())
                .on_thread_start(move || Arc::clone(&tokio_core_setter).set_current())
                .enable_all()
                .build()?,
        );

        let worker_core_ids = all_core_ids.clone();
        let worker_core_setter = Arc::new(CoreSetter::new(worker_core_ids, self.workers_per_core));

        let worker_pool = WorkerPool::new(
            worker_core_setter,
            Arc::clone(&self.graph_access),
            Arc::clone(&self.router),
            Arc::clone(&rt),
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

        let axum_app = axum_app.with_state(Arc::new(AppState {
            worker_pool,
            schema_json: self.opts.and_then(|o| o.config.schema),
            cluster_id: self.cluster_id,
        }));

        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind(self.address)
                .await
                .expect("Failed to bind listener");
            info!("Listener has been bound, starting server");
            axum::serve(listener, axum_app)
                .with_graceful_shutdown(shutdown_signal())
                .await
                .expect("Failed to serve")
        });

        Ok(())
    }
}

async fn shutdown_signal() {
    // Respond to either Ctrl-C (SIGINT) or SIGTERM (e.g. `kill` or systemd stop)
    #[cfg(unix)]
    {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received Ctrl-C, starting graceful shutdown…");
            }
            _ = sigterm() => {
                info!("Received SIGTERM, starting graceful shutdown…");
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        info!("Received Ctrl-C, starting graceful shutdown…");
    }
}

#[cfg(unix)]
async fn sigterm() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    term.recv().await;
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

pub struct CoreSetter {
    cores: Vec<CoreId>,
    threads_per_core: usize,
    incrementing_index: AtomicUsize,
}

impl CoreSetter {
    pub fn new(cores: Vec<CoreId>, threads_per_core: usize) -> Self {
        Self {
            cores,
            threads_per_core,
            incrementing_index: AtomicUsize::new(0),
        }
    }

    pub fn num_threads(&self) -> usize {
        self.cores.len() * self.threads_per_core
    }

    pub fn set_current(self: Arc<Self>) {
        let curr_idx = self
            .incrementing_index
            .fetch_add(1, atomic::Ordering::SeqCst);

        let core_index = curr_idx / self.threads_per_core;
        match self.cores.get(core_index) {
            Some(c) => {
                core_affinity::set_for_current(*c);
                trace!("Set core affinity to: {c:?}");
            }
            None => warn!(
                "CoreSetter::set_current called more times than cores.len() * threads_per_core. Core affinity not set"
            ),
        };
    }
}
