use crate::helix_engine::graph_core::config::Config;
use crate::helix_engine::storage_core::storage_core::HelixGraphStorage;
use crate::helix_engine::types::GraphError;
use crate::helix_gateway::mcp::mcp::{MCPBackend, McpConnections};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub enum QueryInput {
    StringValue { value: String },
    IntegerValue { value: i32 },
    FloatValue { value: f64 },
    BooleanValue { value: bool },
}

pub struct HelixGraphEngine {
    pub storage: Arc<HelixGraphStorage>,
    pub mcp_backend: Option<Arc<MCPBackend>>,
    pub mcp_connections: Option<Arc<Mutex<McpConnections>>>,
}

#[derive(Default, Clone)]
pub struct HelixGraphEngineOpts {
    pub path: String,
    pub config: Config,
}

impl HelixGraphEngine {
    pub fn new(opts: HelixGraphEngineOpts) -> Result<HelixGraphEngine, GraphError> {
        let should_use_mcp = opts.config.mcp;
        let storage = match HelixGraphStorage::new(opts.path.as_str(), opts.config) {
            Ok(db) => Arc::new(db),
            Err(err) => return Err(err),
        };

        let (mcp_backend, mcp_connections) = if should_use_mcp.unwrap_or(false) {
            let mcp_backend = Arc::new(MCPBackend::new(storage.clone()));
            let mcp_connections = Arc::new(Mutex::new(McpConnections::new()));
            (Some(mcp_backend), Some(mcp_connections))
        } else {
            (None, None)
        };

        Ok(Self {
            storage,
            mcp_backend,
            mcp_connections,
        })
    }
}

