use crate::{
    helix_engine::{
        graph_core::ops::tr_val::TraversalVal, storage_core::storage_core::HelixGraphStorage,
        types::GraphError,
    },
    helix_gateway::mcp::tools::ToolArgs,
    protocol::{Format, Request, Response, return_values::ReturnValue},
    utils::id::v6_uuid,
};
use helix_macros::mcp_handler;
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    vec::IntoIter,
};

pub struct McpConnections {
    pub connections: HashMap<String, MCPConnection>,
}

impl Default for McpConnections {
    fn default() -> Self {
        Self::new()
    }
}

impl McpConnections {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
        }
    }
    pub fn new_with_max_connections(max_connections: usize) -> Self {
        Self {
            connections: HashMap::with_capacity(max_connections),
        }
    }
    pub fn add_connection(&mut self, connection: MCPConnection) {
        self.connections
            .insert(connection.connection_id.clone(), connection);
    }

    pub fn remove_connection(&mut self, connection_id: &str) -> Option<MCPConnection> {
        self.connections.remove(connection_id)
    }

    pub fn get_connection(&self, connection_id: &str) -> Option<&MCPConnection> {
        self.connections.get(connection_id)
    }

    pub fn get_connection_mut(&mut self, connection_id: &str) -> Option<&mut MCPConnection> {
        self.connections.get_mut(connection_id)
    }

    pub fn get_connection_owned(&mut self, connection_id: &str) -> Option<MCPConnection> {
        self.connections.remove(connection_id)
    }
}
pub struct MCPBackend {
    pub db: Arc<HelixGraphStorage>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ToolCallRequest {
    pub connection_id: String,
    pub tool: ToolArgs,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ResourceCallRequest {
    pub connection_id: String,
}

impl MCPBackend {
    pub fn new(db: Arc<HelixGraphStorage>) -> Self {
        Self { db }
    }
}

pub struct MCPConnection {
    pub connection_id: String,
    pub iter: IntoIter<TraversalVal>,
}

impl MCPConnection {
    pub fn new(connection_id: String, iter: IntoIter<TraversalVal>) -> Self {
        Self {
            connection_id,
            iter,
        }
    }
}

pub struct MCPToolInput {
    pub request: Request,
    pub mcp_backend: Arc<MCPBackend>,
    pub mcp_connections: Arc<Mutex<McpConnections>>,
    pub schema: Option<String>,
}

// basic type for function pointer
pub type BasicMCPHandlerFn = for<'a> fn(&'a mut MCPToolInput) -> Result<Response, GraphError>;

// thread safe type for multi threaded use
pub type MCPHandlerFn =
    Arc<dyn for<'a> Fn(&'a mut MCPToolInput) -> Result<Response, GraphError> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct MCPHandlerSubmission(pub MCPHandler);

#[derive(Clone, Debug)]
pub struct MCPHandler {
    pub name: &'static str,
    pub func: BasicMCPHandlerFn,
}

impl MCPHandler {
    pub const fn new(name: &'static str, func: BasicMCPHandlerFn) -> Self {
        Self { name, func }
    }
}

inventory::collect!(MCPHandlerSubmission);

#[derive(Deserialize)]
pub struct InitRequest {
    pub connection_addr: String,
    pub connection_port: u16,
}

#[mcp_handler]
pub fn init(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let connection_id = uuid::Uuid::from_u128(v6_uuid()).to_string();
    let mut connections = input.mcp_connections.lock().unwrap();
    connections.add_connection(MCPConnection::new(
        connection_id.clone(),
        vec![].into_iter(),
    ));
    drop(connections);
    Ok(Format::Json.create_response(&ReturnValue::from(connection_id)))
}

#[derive(Deserialize)]
pub struct NextRequest {
    pub connection_id: String,
}

#[mcp_handler]
pub fn next(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let data: NextRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(e) => return Err(GraphError::from(e)),
    };

    let mut connections = input.mcp_connections.lock().unwrap();
    let connection = match connections.get_connection_mut(&data.connection_id) {
        Some(conn) => conn,
        None => return Err(GraphError::StorageError("Connection not found".to_string())),
    };

    let next = connection
        .iter
        .next()
        .unwrap_or(TraversalVal::Empty)
        .clone();
    drop(connections);

    Ok(Format::Json.create_response(&ReturnValue::from(next)))
}

#[derive(Deserialize)]
pub struct Range {
    pub start: usize,
    pub end: usize,
}

#[derive(Deserialize)]
pub struct CollectRequest {
    pub connection_id: String,
    pub range: Option<Range>,
}

#[mcp_handler]
pub fn collect(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let data: CollectRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(e) => return Err(GraphError::from(e)),
    };

    let mut connections = input.mcp_connections.lock().unwrap();
    let connection = match connections.get_connection_owned(&data.connection_id) {
        Some(conn) => conn,
        None => return Err(GraphError::StorageError("Connection not found".to_string())),
    };
    drop(connections);

    let (start, end) = match data.range {
        Some(range) => (range.start, range.end),
        None => (0, 10),
    };

    let values = connection
        .iter
        .skip(start)
        .take(end - start)
        .collect::<Vec<TraversalVal>>();

    let mut connections = input.mcp_connections.lock().unwrap();
    connections.add_connection(MCPConnection::new(
        connection.connection_id.clone(),
        vec![].into_iter(),
    ));
    drop(connections);

    Ok(Format::Json.create_response(&ReturnValue::from(values)))
}

#[mcp_handler]
pub fn schema_resource(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let data: ResourceCallRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(e) => return Err(GraphError::from(e)),
    };

    let _ = match input
        .mcp_connections
        .lock()
        .unwrap()
        .get_connection(&data.connection_id)
    {
        Some(conn) => conn,
        None => return Err(GraphError::StorageError("Connection not found".to_string())),
    };

    if input.schema.is_some() {
        Ok(Format::Json.create_response(&ReturnValue::from(
            input.schema.as_ref().unwrap().to_string(),
        )))
    } else {
        Ok(Format::Json.create_response(&ReturnValue::from("no schema".to_string())))
    }
}
