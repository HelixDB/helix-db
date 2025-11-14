pub mod config;
pub mod ops;
pub mod traversal_iter;
pub mod traversal_value;

use crate::helix_engine::storage_core::{HelixGraphStorage, version_info::VersionInfo};
use crate::helix_engine::traversal_core::config::Config;
use crate::helix_engine::types::GraphError;
use crate::helix_gateway::mcp::mcp::{McpBackend, McpConnections};
use std::sync::{Arc, Mutex};

pub const LMDB_STRING_HEADER_LENGTH: usize = 8;

#[derive(Debug)]
pub enum QueryInput {
    StringValue { value: String },
    IntegerValue { value: i32 },
    FloatValue { value: f64 },
    BooleanValue { value: bool },
}

pub struct HelixGraphEngine {
    pub storage: Arc<HelixGraphStorage>,
    pub mcp_backend: Option<Arc<McpBackend>>,
    pub mcp_connections: Option<Arc<Mutex<McpConnections>>>,
}

#[derive(Default, Clone)]
pub struct HelixGraphEngineOpts {
    pub path: String,
    pub config: Config,
    pub version_info: VersionInfo,
}

impl HelixGraphEngine {
    pub fn new(opts: HelixGraphEngineOpts) -> Result<Self, GraphError> {
        let should_use_mcp = opts.config.mcp;
        let storage =
            match HelixGraphStorage::new(opts.path.as_str(), opts.config, opts.version_info) {
                Ok(db) => Arc::new(db),
                Err(err) => return Err(err),
            };

        let (mcp_backend, mcp_connections) = if should_use_mcp.unwrap_or(false) {
            let mcp_backend = Arc::new(McpBackend::new(storage.clone()));
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
