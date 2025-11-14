use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::util::{aggregate::AggregateAdapter, group_by::GroupByAdapter},
            traversal_value::TraversalValue,
        },
        types::GraphError,
    },
    helix_gateway::mcp::tools::{EdgeType, FilterTraversal, Order, ToolArgs, execute_query_chain},
    protocol::{Format, Request, Response},
    utils::id::v6_uuid,
};
use bumpalo::Bump;
use helix_macros::mcp_handler;
use serde::Deserialize;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub type QueryStep = ToolArgs;

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

impl Default for McpConnections {
    fn default() -> Self {
        Self::new()
    }
}

pub struct McpBackend {
    pub db: Arc<HelixGraphStorage>,
}

impl McpBackend {
    pub fn new(db: Arc<HelixGraphStorage>) -> Self {
        Self { db }
    }
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

pub struct MCPConnection {
    pub connection_id: String,
    pub query_chain: Vec<QueryStep>,
    pub current_position: usize,
}

impl MCPConnection {
    pub fn new(connection_id: String) -> Self {
        Self {
            connection_id,
            query_chain: Vec::new(),
            current_position: 0,
        }
    }

    pub fn add_query_step(&mut self, step: QueryStep) {
        self.query_chain.push(step);
        self.current_position = 0;
    }

    pub fn reset_position(&mut self) {
        self.current_position = 0;
    }

    pub fn clear_chain(&mut self) {
        self.query_chain.clear();
        self.reset_position();
    }

    pub fn next_item<'db, 'arena>(
        &mut self,
        db: &'db HelixGraphStorage,
        arena: &'arena Bump,
    ) -> Result<TraversalValue<'arena>, GraphError>
    where
        'db: 'arena,
    {
        let txn = db.graph_env.read_txn()?;
        let stream = execute_query_chain(&self.query_chain, db, &txn, arena)?;
        match stream.nth(self.current_position)? {
            Some(value) => {
                self.current_position += 1;
                Ok(value)
            }
            None => Ok(TraversalValue::Empty),
        }
    }
}

pub struct MCPToolInput {
    pub request: Request,
    pub mcp_backend: Arc<McpBackend>,
    pub mcp_connections: Arc<Mutex<McpConnections>>,
    pub schema: Option<String>,
}

pub type BasicMCPHandlerFn = for<'a> fn(&'a mut MCPToolInput) -> Result<Response, GraphError>;

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

/// Helper function to execute a tool step on a connection
fn execute_tool_step(
    input: &mut MCPToolInput,
    connection_id: &str,
    tool: ToolArgs,
) -> Result<Response, GraphError> {
    tracing::debug!(
        "[EXECUTE_TOOL_STEP] Starting with connection_id: {}",
        connection_id
    );

    // Clone necessary data while holding the lock
    let query_chain = {
        tracing::debug!("[EXECUTE_TOOL_STEP] Acquiring connection lock");
        let mut connections = input.mcp_connections.lock().unwrap();

        tracing::debug!(
            "[EXECUTE_TOOL_STEP] Available connections: {:?}",
            connections.connections.keys().collect::<Vec<_>>()
        );

        let connection = connections
            .get_connection_mut(connection_id)
            .ok_or_else(|| {
                tracing::error!(
                    "[EXECUTE_TOOL_STEP] Connection not found: {}",
                    connection_id
                );
                GraphError::StorageError(format!("Connection not found: {}", connection_id))
            })?;

        tracing::debug!(
            "[EXECUTE_TOOL_STEP] Adding query step, current chain length: {}",
            connection.query_chain.len()
        );
        connection.add_query_step(tool);
        connection.query_chain.clone()
    };

    tracing::debug!(
        "[EXECUTE_TOOL_STEP] Executing query chain with {} steps",
        query_chain.len()
    );

    // Execute long-running operation without holding the lock
    let arena = Bump::new();
    let storage = input.mcp_backend.db.as_ref();
    let txn = storage.graph_env.read_txn().map_err(|e| {
        tracing::error!(
            "[EXECUTE_TOOL_STEP] Failed to create read transaction: {:?}",
            e
        );
        e
    })?;

    let stream = execute_query_chain(&query_chain, storage, &txn, &arena).map_err(|e| {
        tracing::error!("[EXECUTE_TOOL_STEP] Failed to execute query chain: {:?}", e);
        e
    })?;

    let mut iter = stream.into_inner_iter();

    let (first, consumed_one) = match iter.next() {
        Some(value) => {
            let val = value.map_err(|e| {
                tracing::error!("[EXECUTE_TOOL_STEP] Error getting first value: {:?}", e);
                e
            })?;
            (val, true)
        }
        None => (TraversalValue::Empty, false),
    };

    tracing::debug!(
        "[EXECUTE_TOOL_STEP] Got first result, consumed: {}",
        consumed_one
    );

    // Update connection state
    {
        let mut connections = input.mcp_connections.lock().unwrap();
        let connection = connections
            .get_connection_mut(connection_id)
            .ok_or_else(|| {
                tracing::error!(
                    "[EXECUTE_TOOL_STEP] Connection not found when updating state: {}",
                    connection_id
                );
                GraphError::StorageError(format!("Connection not found: {}", connection_id))
            })?;
        connection.current_position = if consumed_one { 1 } else { 0 };
    }

    tracing::debug!("[EXECUTE_TOOL_STEP] Successfully completed");
    Ok(Format::Json.create_response(&first))
}

#[derive(Deserialize)]
pub struct InitRequest {
    pub connection_addr: String,
    pub connection_port: u16,
}

#[mcp_handler]
pub fn init(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let connection_id = uuid::Uuid::from_u128(v6_uuid()).to_string();
    let mut connections = input.mcp_connections.lock().unwrap();
    connections.add_connection(MCPConnection::new(connection_id.clone()));
    drop(connections);
    Ok(Format::Json.create_response(&connection_id))
}

#[mcp_handler]
pub fn tool_call(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let data: ToolCallRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    execute_tool_step(input, &data.connection_id, data.tool)
}

#[derive(Deserialize)]
pub struct NextRequest {
    pub connection_id: String,
}

#[mcp_handler]
pub fn next(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let data: NextRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => {
            tracing::error!("[NEXT] Failed to parse request: {:?}", err);
            return Err(GraphError::from(err));
        }
    };

    tracing::debug!("[NEXT] Processing for connection: {}", data.connection_id);

    // Clone necessary data while holding the lock
    let (query_chain, current_position) = {
        let connections = input.mcp_connections.lock().unwrap();
        tracing::debug!(
            "[NEXT] Available connections: {:?}",
            connections.connections.keys().collect::<Vec<_>>()
        );

        let connection = connections
            .get_connection(&data.connection_id)
            .ok_or_else(|| {
                tracing::error!("[NEXT] Connection not found: {}", data.connection_id);
                GraphError::StorageError(format!("Connection not found: {}", data.connection_id))
            })?;
        (connection.query_chain.clone(), connection.current_position)
    };

    tracing::debug!(
        "[NEXT] Current position: {}, chain length: {}",
        current_position,
        query_chain.len()
    );

    // Execute long-running operation without holding the lock
    let arena = Bump::new();
    let storage = input.mcp_backend.db.as_ref();
    let txn = storage.graph_env.read_txn().map_err(|e| {
        tracing::error!("[NEXT] Failed to create read transaction: {:?}", e);
        e
    })?;

    let stream = execute_query_chain(&query_chain, storage, &txn, &arena).map_err(|e| {
        tracing::error!("[NEXT] Failed to execute query chain: {:?}", e);
        e
    })?;

    let next_value = match stream.nth(current_position).map_err(|e| {
        tracing::error!(
            "[NEXT] Error iterating to position {}: {:?}",
            current_position,
            e
        );
        e
    })? {
        Some(value) => {
            // Update current_position
            let mut connections = input.mcp_connections.lock().unwrap();
            let connection = connections
                .get_connection_mut(&data.connection_id)
                .ok_or_else(|| {
                    tracing::error!(
                        "[NEXT] Connection not found when updating position: {}",
                        data.connection_id
                    );
                    GraphError::StorageError(format!(
                        "Connection not found: {}",
                        data.connection_id
                    ))
                })?;
            connection.current_position += 1;
            tracing::debug!(
                "[NEXT] Updated position to: {}",
                connection.current_position
            );
            value
        }
        None => {
            tracing::debug!("[NEXT] No more values, returning Empty");
            TraversalValue::Empty
        }
    };

    Ok(Format::Json.create_response(&next_value))
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
    pub drop: Option<bool>,
}

#[mcp_handler]
pub fn collect(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let data: CollectRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    // Clone necessary data while holding the lock
    let query_chain = {
        let connections = input.mcp_connections.lock().unwrap();
        let connection = connections
            .get_connection(&data.connection_id)
            .ok_or_else(|| {
                GraphError::StorageError(format!("Connection not found: {}", data.connection_id))
            })?;
        connection.query_chain.clone()
    };

    // Execute long-running operation without holding the lock
    let arena = Bump::new();
    let storage = input.mcp_backend.db.as_ref();
    let txn = storage.graph_env.read_txn()?;
    let stream = execute_query_chain(&query_chain, storage, &txn, &arena)?;
    let iter = stream.into_inner_iter();

    let range = data.range;
    let start = range.as_ref().map(|r| r.start).unwrap_or(0);
    let end = range.as_ref().map(|r| r.end);

    let mut values = Vec::new();
    for (index, item) in iter.enumerate() {
        let item = item?;
        if index >= start {
            if let Some(end) = end
                && index >= end
            {
                break;
            }
            values.push(item);
        }
    }

    // Update connection state
    {
        let mut connections = input.mcp_connections.lock().unwrap();
        let connection = connections
            .get_connection_mut(&data.connection_id)
            .ok_or_else(|| {
                GraphError::StorageError(format!("Connection not found: {}", data.connection_id))
            })?;

        if data.drop.unwrap_or(true) {
            connection.clear_chain();
        }
    }

    Ok(Format::Json.create_response(&values))
}

#[derive(Deserialize)]
pub struct AggregateRequest {
    pub connection_id: String,
    properties: Vec<String>,
    pub drop: Option<bool>,
}

#[mcp_handler]
pub fn aggregate_by(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let data: AggregateRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    // Clone necessary data while holding the lock
    let query_chain = {
        let connections = input.mcp_connections.lock().unwrap();
        let connection = connections
            .get_connection(&data.connection_id)
            .ok_or_else(|| {
                GraphError::StorageError(format!("Connection not found: {}", data.connection_id))
            })?;
        connection.query_chain.clone()
    };

    // Execute long-running operation without holding the lock
    let arena = Bump::new();
    let storage = input.mcp_backend.db.as_ref();
    let txn = storage.graph_env.read_txn()?;
    let stream = execute_query_chain(&query_chain, storage, &txn, &arena)?;

    let aggregation = stream
        .into_ro()
        .aggregate_by(&data.properties, true)?
        .into_count();

    // Update connection state
    {
        let mut connections = input.mcp_connections.lock().unwrap();
        let connection = connections
            .get_connection_mut(&data.connection_id)
            .ok_or_else(|| {
                GraphError::StorageError(format!("Connection not found: {}", data.connection_id))
            })?;

        if data.drop.unwrap_or(true) {
            connection.clear_chain();
        }
    }

    Ok(Format::Json.create_response(&aggregation))
}

#[mcp_handler]
pub fn group_by(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let data: AggregateRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    // Clone necessary data while holding the lock
    let query_chain = {
        let connections = input.mcp_connections.lock().unwrap();
        let connection = connections
            .get_connection(&data.connection_id)
            .ok_or_else(|| {
                GraphError::StorageError(format!("Connection not found: {}", data.connection_id))
            })?;
        connection.query_chain.clone()
    };

    // Execute long-running operation without holding the lock
    let arena = Bump::new();
    let storage = input.mcp_backend.db.as_ref();
    let txn = storage.graph_env.read_txn()?;
    let stream = execute_query_chain(&query_chain, storage, &txn, &arena)?;

    let aggregation = stream
        .into_ro()
        .group_by(&data.properties, true)?
        .into_count();

    // Update connection state
    {
        let mut connections = input.mcp_connections.lock().unwrap();
        let connection = connections
            .get_connection_mut(&data.connection_id)
            .ok_or_else(|| {
                GraphError::StorageError(format!("Connection not found: {}", data.connection_id))
            })?;

        if data.drop.unwrap_or(true) {
            connection.clear_chain();
        }
    }

    Ok(Format::Json.create_response(&aggregation))
}

#[derive(Deserialize)]
pub struct ResetRequest {
    pub connection_id: String,
}

#[mcp_handler]
pub fn reset(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let data: ResetRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let mut connections = input.mcp_connections.lock().unwrap();
    let connection = connections
        .get_connection_mut(&data.connection_id)
        .ok_or_else(|| {
            GraphError::StorageError(format!("Connection not found: {}", data.connection_id))
        })?;

    connection.clear_chain();
    let connection_id = connection.connection_id.clone();
    drop(connections);

    Ok(Format::Json.create_response(&connection_id))
}

#[mcp_handler]
pub fn schema_resource(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let data: ResourceCallRequest = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let connections = input.mcp_connections.lock().unwrap();
    if !connections.connections.contains_key(&data.connection_id) {
        return Err(GraphError::StorageError("Connection not found".to_string()));
    }
    drop(connections);

    if let Some(schema) = &input.schema {
        Ok(Format::Json.create_response(&schema.clone()))
    } else {
        Ok(Format::Json.create_response(&"no schema".to_string()))
    }
}

// Individual tool endpoint handlers

#[derive(Debug, Deserialize)]
pub struct OutStepData {
    pub edge_label: String,
    pub edge_type: EdgeType,
    pub filter: Option<FilterTraversal>,
}

#[derive(Debug, Deserialize)]
pub struct OutStepInput {
    pub connection_id: String,
    pub data: OutStepData,
}

#[mcp_handler]
pub fn out_step(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let req: OutStepInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let tool = ToolArgs::OutStep {
        edge_label: req.data.edge_label,
        edge_type: req.data.edge_type,
        filter: req.data.filter,
    };

    execute_tool_step(input, &req.connection_id, tool)
}

#[derive(Debug, Deserialize)]
pub struct InStepData {
    pub edge_label: String,
    pub edge_type: EdgeType,
    pub filter: Option<FilterTraversal>,
}

#[derive(Debug, Deserialize)]
pub struct InStepInput {
    pub connection_id: String,
    pub data: InStepData,
}

#[mcp_handler]
pub fn in_step(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let req: InStepInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let tool = ToolArgs::InStep {
        edge_label: req.data.edge_label,
        edge_type: req.data.edge_type,
        filter: req.data.filter,
    };

    execute_tool_step(input, &req.connection_id, tool)
}

#[derive(Debug, Deserialize)]
pub struct OutEStepData {
    pub edge_label: String,
    pub filter: Option<FilterTraversal>,
}

#[derive(Debug, Deserialize)]
pub struct OutEStepInput {
    pub connection_id: String,
    pub data: OutEStepData,
}

#[mcp_handler]
pub fn out_e_step(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let req: OutEStepInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let tool = ToolArgs::OutEStep {
        edge_label: req.data.edge_label,
        filter: req.data.filter,
    };

    execute_tool_step(input, &req.connection_id, tool)
}

#[derive(Debug, Deserialize)]
pub struct InEStepData {
    pub edge_label: String,
    pub filter: Option<FilterTraversal>,
}

#[derive(Debug, Deserialize)]
pub struct InEStepInput {
    pub connection_id: String,
    pub data: InEStepData,
}

#[mcp_handler]
pub fn in_e_step(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let req: InEStepInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let tool = ToolArgs::InEStep {
        edge_label: req.data.edge_label,
        filter: req.data.filter,
    };

    execute_tool_step(input, &req.connection_id, tool)
}

#[derive(Debug, Deserialize)]
pub struct NFromTypeData {
    pub node_type: String,
}

#[derive(Debug, Deserialize)]
pub struct NFromTypeInput {
    pub connection_id: String,
    pub data: NFromTypeData,
}

#[mcp_handler]
pub fn n_from_type(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let req: NFromTypeInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let tool = ToolArgs::NFromType {
        node_type: req.data.node_type,
    };

    execute_tool_step(input, &req.connection_id, tool)
}

#[derive(Debug, Deserialize)]
pub struct EFromTypeData {
    pub edge_type: String,
}

#[derive(Debug, Deserialize)]
pub struct EFromTypeInput {
    pub connection_id: String,
    pub data: EFromTypeData,
}

#[mcp_handler]
pub fn e_from_type(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let req: EFromTypeInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let tool = ToolArgs::EFromType {
        edge_type: req.data.edge_type,
    };

    execute_tool_step(input, &req.connection_id, tool)
}

#[derive(Debug, Deserialize)]
pub struct FilterItemsData {
    #[serde(default)]
    pub filter: FilterTraversal,
}

#[derive(Debug, Deserialize)]
pub struct FilterItemsInput {
    pub connection_id: String,
    pub data: FilterItemsData,
}

#[mcp_handler]
pub fn filter_items(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let req: FilterItemsInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let tool = ToolArgs::FilterItems {
        filter: req.data.filter,
    };

    execute_tool_step(input, &req.connection_id, tool)
}

#[derive(Debug, Deserialize)]
pub struct OrderByData {
    pub properties: String,
    pub order: Order,
}

#[derive(Debug, Deserialize)]
pub struct OrderByInput {
    pub connection_id: String,
    pub data: OrderByData,
}

#[mcp_handler]
pub fn order_by(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let req: OrderByInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let tool = ToolArgs::OrderBy {
        properties: req.data.properties,
        order: req.data.order,
    };

    execute_tool_step(input, &req.connection_id, tool)
}

#[derive(Debug, Deserialize)]
pub struct SearchKeywordData {
    pub query: String,
    pub limit: usize,
    pub label: String,
}

#[derive(Debug, Deserialize)]
pub struct SearchKeywordInput {
    pub connection_id: String,
    pub data: SearchKeywordData,
}

#[mcp_handler]
pub fn search_keyword(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    use crate::helix_engine::traversal_core::ops::{bm25::search_bm25::SearchBM25Adapter, g::G};

    let req: SearchKeywordInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    // Verify connection exists
    {
        let connections = input.mcp_connections.lock().unwrap();
        connections
            .get_connection(&req.connection_id)
            .ok_or_else(|| {
                GraphError::StorageError(format!("Connection not found: {}", req.connection_id))
            })?;
    }

    // Execute long-running operation without holding the lock
    let arena = Bump::new();
    let storage = input.mcp_backend.db.as_ref();
    let txn = storage.graph_env.read_txn()?;

    // Perform BM25 search using the existing index
    let results = G::new(storage, &txn, &arena)
        .search_bm25(&req.data.label, &req.data.query, req.data.limit)?
        .collect::<Result<Vec<_>, _>>()?;

    let (first, consumed_one) = match results.first() {
        Some(value) => (value.clone(), true),
        None => (TraversalValue::Empty, false),
    };

    // Update connection state
    {
        let mut connections = input.mcp_connections.lock().unwrap();
        let connection = connections
            .get_connection_mut(&req.connection_id)
            .ok_or_else(|| {
                GraphError::StorageError(format!("Connection not found: {}", req.connection_id))
            })?;

        // Store remaining results for pagination
        connection.current_position = if consumed_one { 1 } else { 0 };
        // Note: For search_keyword, we don't update the query_chain since it's a starting operation
    }

    Ok(Format::Json.create_response(&first))
}

#[derive(Debug, Deserialize)]
pub struct SearchVectorTextData {
    pub query: String,
    pub label: String,
    pub k: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct SearchVectorTextInput {
    pub connection_id: String,
    pub data: SearchVectorTextData,
}

#[mcp_handler]
pub fn search_vector_text(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    use crate::helix_engine::traversal_core::ops::{g::G, vectors::search::SearchVAdapter};
    use crate::helix_gateway::embedding_providers::{EmbeddingModel, get_embedding_model};

    let req: SearchVectorTextInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => {
            tracing::error!("[VECTOR_SEARCH] Failed to parse request: {:?}", err);
            return Err(GraphError::from(err));
        }
    };

    tracing::debug!(
        "[VECTOR_SEARCH] Starting search for connection: {}, query: {}, label: {}, k: {:?}",
        req.connection_id,
        req.data.query,
        req.data.label,
        req.data.k
    );

    // Verify connection exists
    {
        tracing::debug!("[VECTOR_SEARCH] Verifying connection exists");
        let connections = input.mcp_connections.lock().unwrap();
        tracing::debug!(
            "[VECTOR_SEARCH] Available connections: {:?}",
            connections.connections.keys().collect::<Vec<_>>()
        );

        connections
            .get_connection(&req.connection_id)
            .ok_or_else(|| {
                tracing::error!(
                    "[VECTOR_SEARCH] Connection not found: {}",
                    req.connection_id
                );
                GraphError::StorageError(format!("Connection not found: {}", req.connection_id))
            })?;
    }

    tracing::debug!("[VECTOR_SEARCH] Connection verified, starting long-running operations");

    // Execute long-running operations without holding the lock
    let arena = Bump::new();
    let storage = input.mcp_backend.db.as_ref();
    let txn = storage.graph_env.read_txn().map_err(|e| {
        tracing::error!("[VECTOR_SEARCH] Failed to create read transaction: {:?}", e);
        e
    })?;

    // Get embedding model and convert query text to vector
    tracing::debug!("[VECTOR_SEARCH] Getting embedding model");
    let embedding_model = get_embedding_model(None, None, None).map_err(|e| {
        tracing::error!("[VECTOR_SEARCH] Failed to get embedding model: {:?}", e);
        e
    })?;

    tracing::debug!("[VECTOR_SEARCH] Fetching embedding for query text");
    let query_embedding = embedding_model
        .fetch_embedding(&req.data.query)
        .map_err(|e| {
            tracing::error!("[VECTOR_SEARCH] Failed to fetch embedding: {:?}", e);
            e
        })?;
    let query_vec_arena = arena.alloc_slice_copy(&query_embedding);

    // Perform vector search
    let k_value = req.data.k.unwrap_or(10);
    let label_arena = arena.alloc_str(&req.data.label);

    tracing::debug!(
        "[VECTOR_SEARCH] Performing vector search with k={}",
        k_value
    );
    let results = G::new(storage, &txn, &arena)
        .search_v::<fn(&crate::helix_engine::vector_core::vector::HVector, &heed3::RoTxn) -> bool, _>(
            query_vec_arena,
            k_value,
            label_arena,
            None
        )
        .collect::<Result<Vec<_>,_>>()?;

    tracing::debug!("[VECTOR_SEARCH] Search returned {} results", results.len());

    let (first, consumed_one) = match results.first() {
        Some(value) => {
            tracing::debug!("[VECTOR_SEARCH] Returning first result");
            (value.clone(), true)
        }
        None => {
            tracing::debug!("[VECTOR_SEARCH] No results found, returning Empty");
            (TraversalValue::Empty, false)
        }
    };

    // Update connection state
    {
        tracing::debug!("[VECTOR_SEARCH] Updating connection state");
        let mut connections = input.mcp_connections.lock().unwrap();
        let connection = connections
            .get_connection_mut(&req.connection_id)
            .ok_or_else(|| {
                tracing::error!(
                    "[VECTOR_SEARCH] Connection not found when updating state: {}",
                    req.connection_id
                );
                GraphError::StorageError(format!("Connection not found: {}", req.connection_id))
            })?;

        connection.current_position = if consumed_one { 1 } else { 0 };
        tracing::debug!(
            "[VECTOR_SEARCH] Updated position to: {}",
            connection.current_position
        );
    }

    tracing::debug!("[VECTOR_SEARCH] Successfully completed");
    Ok(Format::Json.create_response(&first))
}

#[derive(Debug, Deserialize)]
pub struct SearchVectorData {
    pub vector: Vec<f64>,
    pub k: usize,
    pub min_score: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct SearchVectorInput {
    pub connection_id: String,
    pub data: SearchVectorData,
}

#[mcp_handler]
pub fn search_vector(input: &mut MCPToolInput) -> Result<Response, GraphError> {
    let req: SearchVectorInput = match sonic_rs::from_slice(&input.request.body) {
        Ok(data) => data,
        Err(err) => return Err(GraphError::from(err)),
    };

    let tool = ToolArgs::SearchVec {
        vector: req.data.vector,
        k: req.data.k,
        min_score: req.data.min_score,
    };

    execute_tool_step(input, &req.connection_id, tool)
}
