use std::sync::atomic::{self, AtomicUsize};
use std::thread::available_parallelism;
use std::{collections::HashMap, sync::Arc};

use axum::body::Body;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use core_affinity::{CoreId, set_for_current};
use tracing::{info, trace, warn};

use super::router::router::{HandlerFn, HelixRouter};
use crate::helix_engine::graph_core::graph_core::HelixGraphEngineOpts;
#[cfg(feature = "dev-instance")]
use crate::helix_gateway::builtin::all_nodes_and_edges::nodes_edges_handler;
#[cfg(feature = "dev-instance")]
use crate::helix_gateway::builtin::node_by_id::node_details_handler;
#[cfg(feature = "dev-instance")]
use crate::helix_gateway::builtin::node_connections::node_connections_handler;
#[cfg(feature = "dev-instance")]
use crate::helix_gateway::builtin::nodes_by_label::nodes_by_label_handler;
use crate::helix_gateway::graphvis;
use crate::helix_gateway::introspect_schema::introspect_schema_handler;
use crate::helix_gateway::worker_pool::WorkerPool;
use crate::protocol;
use crate::{
    helix_engine::graph_core::graph_core::HelixGraphEngine, helix_gateway::mcp::mcp::MCPHandlerFn,
};

pub struct GatewayOpts {}

impl GatewayOpts {
    pub const DEFAULT_POOL_SIZE: usize = 8;
}

pub struct HelixGateway {
    address: String,
    worker_size: usize,
    io_size: usize,
    graph_access: Arc<HelixGraphEngine>,
    router: Arc<HelixRouter>,
    opts: Option<HelixGraphEngineOpts>,
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
        HelixGateway {
            address: address.to_string(),
            graph_access,
            router,
            worker_size,
            io_size,
            opts,
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

        let worker_pool = WorkerPool::new(
            self.worker_size,
            worker_setter,
            self.graph_access.clone(),
            self.router.clone(),
        );

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

        let mut axum_app = axum::Router::new()
            .route("/{*path}", post(post_handler))
            .route("/graphvis", get(graphvis::graphvis_handler))
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
        }));

        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind(self.address).await.unwrap();
            info!("Listener has been bound, starting server");
            axum::serve(listener, axum_app).await.unwrap()
        });

        Ok(())
    }
}

async fn post_handler(
    State(state): State<Arc<AppState>>,
    req: protocol::request::Request,
) -> axum::http::Response<Body> {
    let res = state.worker_pool.process(req).await;

    match res {
        Ok(r) => r.into_response(),
        Err(e) => {
            info!(?e, "Got error");
            e.into_response()
        }
    }
}

pub struct AppState {
    pub worker_pool: WorkerPool,
    pub schema_json: Option<String>,
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
