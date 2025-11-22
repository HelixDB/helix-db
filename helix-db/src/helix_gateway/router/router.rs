// router

// takes in raw [u8] data
// parses to request type

// then locks graph and passes parsed data and graph to handler to execute query

// returns response

use crate::{
    helix_engine::{traversal_core::HelixGraphEngine, types::GraphError},
    helix_gateway::mcp::mcp::MCPHandlerFn,
    protocol::request::RetChan,
};
use core::fmt;
use std::{collections::HashMap, fmt::Debug, future::Future, pin::Pin, sync::Arc};

use crate::protocol::{Request, Response};

pub struct HandlerInput {
    pub request: Request,
    pub graph: Arc<HelixGraphEngine>,
}

pub type ContMsg = (
    RetChan,
    Box<dyn FnOnce() -> Result<Response, GraphError> + Send + Sync>,
);
pub type ContChan = flume::Sender<ContMsg>;

pub type ContFut = Pin<Box<dyn Future<Output = ()> + Send + Sync>>;

pub struct IoContFn(pub Box<dyn FnOnce(ContChan, RetChan) -> ContFut + Send + Sync>);

impl IoContFn {
    pub fn create_err<F>(func: F) -> GraphError
    where
        F: FnOnce(ContChan, RetChan) -> ContFut + Send + Sync + 'static,
    {
        GraphError::IoNeeded(Self(Box::new(func)))
    }
}

impl Debug for IoContFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Asyncronous IO is needed to complete the DB operation")
    }
}

// basic type for function pointer
pub type BasicHandlerFn = fn(HandlerInput) -> Result<Response, GraphError>;

// thread safe type for multi threaded use
pub type HandlerFn = Arc<dyn Fn(HandlerInput) -> Result<Response, GraphError> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct HandlerSubmission(pub Handler);

#[derive(Clone, Debug)]
pub struct Handler {
    pub name: &'static str,
    pub func: BasicHandlerFn,
    pub is_write: bool,
}

impl Handler {
    pub const fn new(name: &'static str, func: BasicHandlerFn, is_write: bool) -> Self {
        Self { name, func, is_write }
    }
}

inventory::collect!(HandlerSubmission);

/// Router for handling requests and MCP requests
///
/// Standard Routes and MCP Routes are stored in a HashMap with the method and path as the key
pub struct HelixRouter {
    /// Name => Function
    pub routes: HashMap<String, HandlerFn>,
    pub mcp_routes: HashMap<String, MCPHandlerFn>,
    /// Set of route names that perform write operations
    pub write_routes: std::collections::HashSet<String>,
}

impl HelixRouter {
    /// Create a new router with a set of routes
    pub fn new(
        routes: Option<HashMap<String, HandlerFn>>,
        mcp_routes: Option<HashMap<String, MCPHandlerFn>>,
        write_routes: Option<std::collections::HashSet<String>>,
    ) -> Self {
        let rts = routes.unwrap_or_default();
        let mcp_rts = mcp_routes.unwrap_or_default();
        let write_rts = write_routes.unwrap_or_default();
        Self {
            routes: rts,
            mcp_routes: mcp_rts,
            write_routes: write_rts,
        }
    }

    /// Check if a route is a write operation
    pub fn is_write_route(&self, name: &str) -> bool {
        self.write_routes.contains(name)
    }

    /// Add a route to the router
    pub fn add_route(&mut self, name: &str, handler: BasicHandlerFn, is_write: bool) {
        self.routes.insert(name.to_string(), Arc::new(handler));
        if is_write {
            self.write_routes.insert(name.to_string());
        }
    }
}

#[derive(Debug)]
pub enum RouterError {
    Io(std::io::Error),
    New(String),
}

impl fmt::Display for RouterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RouterError::Io(e) => write!(f, "IO error: {e}"),
            RouterError::New(msg) => write!(f, "Graph error: {msg}"),
        }
    }
}

impl From<String> for RouterError {
    fn from(error: String) -> Self {
        RouterError::New(error)
    }
}

impl From<std::io::Error> for RouterError {
    fn from(error: std::io::Error) -> Self {
        RouterError::Io(error)
    }
}

impl From<GraphError> for RouterError {
    fn from(error: GraphError) -> Self {
        RouterError::New(error.to_string())
    }
}

impl From<RouterError> for GraphError {
    fn from(error: RouterError) -> Self {
        GraphError::New(error.to_string())
    }
}
