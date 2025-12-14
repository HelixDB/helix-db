use std::collections::HashMap;
use std::sync::Arc;
use helix_db::helix_engine::traversal_core::{
    HelixGraphEngine, HelixGraphEngineOpts, config::Config,
};
use helix_db::helix_engine::storage_core::version_info::VersionInfo;
use helix_db::helix_gateway::router::router::{HandlerFn, HandlerInput, HandlerSubmission};
use crate::error::{HelixError, HelixResult};

pub struct HelixDB {
    pub(crate) engine: Arc<HelixGraphEngine>,
    pub(crate) handlers: HashMap<String, HandlerFn>,
}

impl HelixDB {
    pub fn new(path: &str) -> HelixResult<Self> {
        let opts = HelixGraphEngineOpts {
            path: path.to_string(),
            config: Config::default(),
            version_info: VersionInfo::default(),
        };
        
        let engine = Arc::new(
            HelixGraphEngine::new(opts)
                .map_err(HelixError::from)?,
        );
        
        // Collect handlers at runtime
        let mut handlers_map: HashMap<String, HandlerFn> = HashMap::new();
        for submission in inventory::iter::<HandlerSubmission> {
            let handler = &submission.0;
            let func = handler.func;
            let handler_name = handler.name.to_string();
            let handler_fn: HandlerFn = Arc::new(move |input: HandlerInput| func(input));
            handlers_map.insert(handler_name, handler_fn);
        }
        let handlers = handlers_map;
        
        Ok(Self { engine, handlers })
    }
    
    pub fn with_config(path: &str, config: Config) -> HelixResult<Self> {
        let opts = HelixGraphEngineOpts {
            path: path.to_string(),
            config,
            version_info: VersionInfo::default(),
        };
        
        let engine = Arc::new(
            HelixGraphEngine::new(opts)
                .map_err(HelixError::from)?,
        );
        
        // Collect handlers at runtime
        let mut handlers_map: HashMap<String, HandlerFn> = HashMap::new();
        for submission in inventory::iter::<HandlerSubmission> {
            let handler = &submission.0;
            let func = handler.func;
            let handler_name = handler.name.to_string();
            let handler_fn: HandlerFn = Arc::new(move |input: HandlerInput| func(input));
            handlers_map.insert(handler_name, handler_fn);
        }
        let handlers = handlers_map;
        
        Ok(Self { engine, handlers })
    }
    
    pub fn engine(&self) -> &Arc<HelixGraphEngine> {
        &self.engine
    }
    
    /// Execute a function with a read transaction
    /// The transaction is automatically committed when the function returns successfully
    pub fn with_read_transaction<F, T>(&self, f: F) -> HelixResult<T>
    where
        F: FnOnce(&heed3::RoTxn) -> Result<T, helix_db::helix_engine::types::GraphError>,
    {
        let txn = self.engine.storage.graph_env.read_txn()
            .map_err(HelixError::from)?;
        f(&txn).map_err(HelixError::from)
    }
    
    /// Execute a function with a write transaction
    /// The transaction is automatically committed when the function returns successfully
    /// If the function returns an error, the transaction is automatically rolled back
    pub fn with_write_transaction<F, T>(&self, f: F) -> HelixResult<T>
    where
        F: FnOnce(&mut heed3::RwTxn) -> Result<T, helix_db::helix_engine::types::GraphError>,
    {
        let mut txn = self.engine.storage.graph_env.write_txn()
            .map_err(HelixError::from)?;
        let result = f(&mut txn)?;
        txn.commit().map_err(HelixError::from)?;
        Ok(result)
    }
}

// Include generated handler methods - these extend the HelixDB impl
include!(concat!(env!("OUT_DIR"), "/handler_methods.rs"));
