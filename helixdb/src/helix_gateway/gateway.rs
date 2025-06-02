use super::{
    connection::connection::ConnectionHandler,
    router::router::{HandlerFn, HelixRouter},
};
use crate::helix_engine::storage_core::storage_core::HelixGraphStorage;
use std::{collections::HashMap, sync::Arc};

pub struct GatewayOpts {}

impl GatewayOpts {
    pub const DEFAULT_POOL_SIZE: usize = 1024;
}

pub struct HelixGateway {
    pub connection_handler: ConnectionHandler,
}

impl HelixGateway {
    pub async fn new(
        address: &str,
        graph: Arc<HelixGraphStorage>,
        size: usize,
        routes: Option<HashMap<(String, String), HandlerFn>>,
    ) -> HelixGateway {
        let router = HelixRouter::new(routes);
        let connection_handler = ConnectionHandler::new(address, graph, size, router).unwrap();
        println!("Gateway created");
        HelixGateway { connection_handler }
    }
}

