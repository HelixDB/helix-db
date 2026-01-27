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
        Self {
            name,
            func,
            is_write,
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Format, Response};
    use std::collections::{HashMap, HashSet};

    // Helper function for tests
    fn dummy_handler(_input: HandlerInput) -> Result<Response, GraphError> {
        Ok(Response {
            body: b"ok".to_vec(),
            fmt: Format::Json,
        })
    }

    fn another_handler(_input: HandlerInput) -> Result<Response, GraphError> {
        Ok(Response {
            body: b"another".to_vec(),
            fmt: Format::Json,
        })
    }

    // ============================================================================
    // HelixRouter Tests
    // ============================================================================

    #[test]
    fn test_router_new_empty() {
        let router = HelixRouter::new(None, None, None);

        assert!(router.routes.is_empty());
        assert!(router.mcp_routes.is_empty());
        assert!(router.write_routes.is_empty());
    }

    #[test]
    fn test_router_new_with_routes() {
        let mut routes: HashMap<String, HandlerFn> = HashMap::new();
        routes.insert("test".to_string(), Arc::new(dummy_handler));

        let mut write_routes = HashSet::new();
        write_routes.insert("test".to_string());

        let router = HelixRouter::new(Some(routes), None, Some(write_routes));

        assert_eq!(router.routes.len(), 1);
        assert!(router.routes.contains_key("test"));
        assert!(router.write_routes.contains("test"));
    }

    #[test]
    fn test_router_is_write_route_true() {
        let mut write_routes = HashSet::new();
        write_routes.insert("write_op".to_string());

        let router = HelixRouter::new(None, None, Some(write_routes));

        assert!(router.is_write_route("write_op"));
    }

    #[test]
    fn test_router_is_write_route_false() {
        let mut write_routes = HashSet::new();
        write_routes.insert("write_op".to_string());

        let router = HelixRouter::new(None, None, Some(write_routes));

        assert!(!router.is_write_route("read_op"));
        assert!(!router.is_write_route("nonexistent"));
    }

    #[test]
    fn test_router_add_route_basic() {
        let mut router = HelixRouter::new(None, None, None);

        router.add_route("new_route", dummy_handler, false);

        assert!(router.routes.contains_key("new_route"));
        assert!(!router.write_routes.contains("new_route"));
    }

    #[test]
    fn test_router_add_route_as_write() {
        let mut router = HelixRouter::new(None, None, None);

        router.add_route("write_route", dummy_handler, true);

        assert!(router.routes.contains_key("write_route"));
        assert!(router.write_routes.contains("write_route"));
        assert!(router.is_write_route("write_route"));
    }

    #[test]
    fn test_router_add_route_overwrites() {
        let mut router = HelixRouter::new(None, None, None);

        // Add initial route
        router.add_route("test", dummy_handler, false);
        assert!(router.routes.contains_key("test"));

        // Overwrite with new handler
        router.add_route("test", another_handler, true);

        // Route should still exist (was overwritten)
        assert!(router.routes.contains_key("test"));
        // And should now be a write route
        assert!(router.is_write_route("test"));
    }

    // ============================================================================
    // RouterError Tests
    // ============================================================================

    #[test]
    fn test_router_error_display_new() {
        let error = RouterError::New("test error".to_string());
        let display = format!("{}", error);
        assert!(display.contains("test error"));
    }

    #[test]
    fn test_router_error_from_string() {
        let error: RouterError = "custom error".to_string().into();
        match error {
            RouterError::New(msg) => assert_eq!(msg, "custom error"),
            _ => panic!("Expected RouterError::New"),
        }
    }
}
