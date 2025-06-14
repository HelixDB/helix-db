// provides tool endpoints for mcp
// init endpoint to get a user id and establish a connection to helix server

// wraps iter in new tools

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    vec::IntoIter,
};

use get_routes::{local_handler, mcp_handler};
use crate::helix_storage::heed3::{AnyTls, RoTxn};
use serde::Deserialize;

use crate::{
    helix_engine::{
        graph_core::{
            graph_core::HelixGraphEngine,
            ops::{
                in_::{in_::InNodesIterator, in_e::InEdgesIterator},
                out::{out::OutNodesIterator, out_e::OutEdgesIterator},
                source::{add_e::EdgeType, n_from_type::NFromType},
                tr_val::{Traversable, TraversalVal},
            },
            traversal_iter::RoTraversalIterator,
        },
        storage_core::storage_core::HelixGraphStorage,
        types::GraphError,
    },
    helix_gateway::{
        mcp::tools::{ToolArgs, ToolCalls},
        router::router::HandlerInput,
    },
    protocol::{
        items::v6_uuid, label_hash::hash_label, request::Request, response::Response,
        return_values::ReturnValue,
    },
};

pub struct McpConnections {
    pub connections: HashMap<String, MCPConnection>,
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
}
pub struct McpBackend {
    pub db: Arc<HelixGraphStorage>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ToolCallRequest {
    pub connection_id: String,
    pub tool: ToolArgs,
}

impl McpBackend {
    pub fn new(db: Arc<HelixGraphStorage>) -> Self {
        Self { db }
    }
}

pub struct MCPConnection {
    pub connection_id: String,
    pub connection_addr: String,
    pub connection_port: u16,
    pub iter: IntoIter<TraversalVal>,
}

// pub struct McpIter<I> {
//     pub iter: I,
// }

impl MCPConnection {
    pub fn new(
        connection_id: String,
        connection_addr: String,
        connection_port: u16,
        iter: IntoIter<TraversalVal>,
    ) -> Self {
        Self {
            connection_id,
            connection_addr,
            connection_port,
            iter,
        }
    }
}

pub struct MCPToolInput {
    pub request: Request,
    // pub graph: Arc<HelixGraphEngine>,
    pub mcp_backend: Arc<McpBackend>,
    pub mcp_connections: Arc<Mutex<McpConnections>>,
}

// basic type for function pointer
pub type BasicMCPHandlerFn =
    for<'a> fn(&'a mut MCPToolInput, &mut Response) -> Result<(), GraphError>;

// thread safe type for multi threaded use
pub type MCPHandlerFn = Arc<
    dyn for<'a> Fn(&'a mut MCPToolInput, &mut Response) -> Result<(), GraphError> + Send + Sync,
>;

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

#[mcp_handler]
pub fn call_tool<'a>(
    input: &'a mut MCPToolInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    let data: ToolCallRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut connections = input.mcp_connections.lock().unwrap();
    let mut connection = match connections.remove_connection(&data.connection_id) {
        Some(conn) => conn,
        None => return Err(GraphError::Default),
    };
    drop(connections);
    let txn = input.mcp_backend.db.graph_env.read_txn()?;

    let result = input.mcp_backend.call(&txn, &connection, data.tool)?;

    let first = result.first().unwrap_or(&TraversalVal::Empty).clone();

    connection.iter = result.into_iter();
    let mut connections = input.mcp_connections.lock().unwrap();
    connections.add_connection(connection);
    drop(connections);

    response.body = sonic_rs::to_vec(&ReturnValue::from(first)).unwrap();

    Ok(())
}

#[derive(Deserialize)]
pub struct InitRequest {
    pub connection_addr: String,
    pub connection_port: u16,
}

#[mcp_handler]
pub fn init<'a>(input: &'a mut MCPToolInput, response: &mut Response) -> Result<(), GraphError> {
    let data: InitRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let connection_id = uuid::Uuid::from_u128(v6_uuid()).to_string();
    let mut connections = input.mcp_connections.lock().unwrap();
    connections.add_connection(MCPConnection::new(
        connection_id.clone(),
        data.connection_addr,
        data.connection_port,
        vec![].into_iter(),
    ));
    drop(connections);
    response.body = sonic_rs::to_vec(&ReturnValue::from(connection_id)).unwrap();

    Ok(())
}

#[derive(Deserialize)]
pub struct NextRequest {
    pub connection_id: String,
}

#[mcp_handler]
pub fn next<'a>(input: &'a mut MCPToolInput, response: &mut Response) -> Result<(), GraphError> {
    let data: NextRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut connections = input.mcp_connections.lock().unwrap();
    let connection = connections.get_connection_mut(&data.connection_id).unwrap();
    let next = connection
        .iter
        .next()
        .unwrap_or(TraversalVal::Empty)
        .clone();
    drop(connections);
    response.body = sonic_rs::to_vec(&ReturnValue::from(next)).unwrap();
    Ok(())
}
