use std::{collections::HashMap, sync::Arc};

use super::connection::connection::ConnectionHandler;
use crate::helix_runtime::AsyncRuntime;
use super::router::router::{HandlerFn, HelixRouter};
use crate::{
    helix_engine::graph_core::graph_core::HelixGraphEngine, helix_gateway::mcp::mcp::MCPHandlerFn,
};
use crate::helix_transport::Transport;

pub struct GatewayOpts {}

impl GatewayOpts {
    pub const DEFAULT_POOL_SIZE: usize = 8;
}

pub struct HelixGateway<R, T>
where
    R: AsyncRuntime + Clone + Send + Sync + 'static,
    T: Transport,
{
    pub connection_handler: ConnectionHandler<R, T>,
    pub runtime: R,
}

impl<R, T> HelixGateway<R, T>
where
    R: AsyncRuntime + Clone + Send + Sync + 'static,
    T: Transport,
    T::Stream: 'static,
{
    pub async fn new(
        address: &str,
        graph: Arc<HelixGraphEngine>,
        size: usize,
        routes: Option<HashMap<(String, String), HandlerFn>>,
        mcp_routes: Option<HashMap<(String, String), MCPHandlerFn>>,
        runtime: R,
        transport: T,
    ) -> HelixGateway<R, T> {
        let router = HelixRouter::new(routes, mcp_routes);
        let connection_handler =
            ConnectionHandler::new(address, graph, size, router, runtime.clone(), transport)
                .unwrap();
        println!("Gateway created");
        HelixGateway {
            connection_handler,
            runtime,
        }
    }
}
