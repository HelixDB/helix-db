// router

// takes in raw [u8] data
// parses to request type

// then locks graph and passes parsed data and graph to handler to execute query

// returns response

use crate::{
    helix_engine::types::GraphError,
    helix_gateway::{
        graphvis,
        mcp::mcp::{MCPHandlerFn, MCPToolInput},
        worker_pool::TaskContext,
    },
    protocol::{
        HelixError,
        request::{RequestType, RetChan},
    },
};
use core::fmt;
use std::{collections::HashMap, sync::Arc};

use crate::protocol::Request;

pub struct HandlerInput {
    pub request: Request,
    pub context: TaskContext,
}

/// basic type for function pointer
pub type BasicHandlerFn = fn(&HandlerInput, RetChan);

/// thread safe type for multi threaded use
pub type HandlerFn = Arc<dyn Fn(&HandlerInput, RetChan) + Send + Sync>;

/// A Continuation of a handler execution
pub type ContFn = Box<dyn Fn() + Send + Sync>;

#[derive(Clone, Debug)]
pub struct HandlerSubmission(pub Handler);

#[derive(Clone, Debug)]
pub struct Handler {
    pub name: &'static str,
    pub func: BasicHandlerFn,
}

impl Handler {
    pub const fn new(name: &'static str, func: BasicHandlerFn) -> Self {
        Self { name, func }
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
}

impl HelixRouter {
    /// Create a new router with a set of routes
    pub fn new(
        routes: Option<HashMap<String, HandlerFn>>,
        mcp_routes: Option<HashMap<String, MCPHandlerFn>>,
    ) -> Self {
        let rts = routes.unwrap_or_default();
        let mcp_rts = mcp_routes.unwrap_or_default();
        Self {
            routes: rts,
            mcp_routes: mcp_rts,
        }
    }

    /// Add a route to the router
    pub fn add_route(&mut self, name: &str, handler: BasicHandlerFn) {
        self.routes.insert(name.to_string(), Arc::new(handler));
    }

    /// Handle a request by finding the appropriate handler and executing it
    ///
    /// ## Arguments
    ///
    /// * `graph_access` - A reference to the graph engine
    /// * `request` - The request to handle
    pub fn handle(&self, context: TaskContext, request: Request, ret_chan: RetChan) {
        let out = match request.req_type {
            RequestType::Query => {
                if let Some(handler) = self.routes.get(&request.name) {
                    let input = HandlerInput { request, context };
                    return handler(&input, ret_chan);
                } else {
                    Err(request)
                }
            }
            RequestType::MCP => {
                if let Some(mcp_handler) = self.mcp_routes.get(&request.name) {
                    let graph_access = context.graph_access;
                    let mut mcp_input = MCPToolInput {
                        request,
                        mcp_backend: Arc::clone(graph_access.mcp_backend.as_ref().unwrap()),
                        mcp_connections: Arc::clone(graph_access.mcp_connections.as_ref().unwrap()),
                        schema: Some(graph_access.storage.storage_config.schema.clone()),
                    };
                    Ok(mcp_handler(&mut mcp_input).map_err(Into::into))
                } else {
                    Err(request)
                }
            }
            RequestType::GraphVis => {
                let input = HandlerInput { request, context };
                Ok(graphvis::graphvis_inner(&input))
            }
        };

        match out {
            Ok(v) => ret_chan.send(v).expect("Return channel should suceed"),
            Err(request) => ret_chan
                .send(Err(HelixError::NotFound {
                    ty: request.req_type,
                    name: request.name,
                }))
                .expect("Return channel should suceed"),
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
