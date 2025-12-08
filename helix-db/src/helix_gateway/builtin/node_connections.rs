use std::collections::HashSet;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;
use sonic_rs::{JsonValueTrait, json};
use tracing::info;

use crate::helix_engine::storage_core::HelixGraphStorage;
use crate::helix_engine::storage_core::storage_methods::StorageMethods;
use crate::helix_engine::traversal_core::traversal_value::TraversalValue;
use crate::helix_engine::types::GraphError;
use crate::helix_gateway::gateway::AppState;
use crate::helix_gateway::router::router::{Handler, HandlerInput, HandlerSubmission};
use crate::protocol::{self, request::RequestType};
use crate::utils::id::ID;

// get all nodes connected to a specific node
// curl "http://localhost:PORT/node-connections?node_id=YOUR_NODE_ID"

#[derive(Deserialize)]
pub struct NodeConnectionsQuery {
    node_id: String,
}

pub async fn node_connections_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<NodeConnectionsQuery>,
) -> axum::http::Response<Body> {
    let mut req = protocol::request::Request {
        name: "node_connections".to_string(),
        req_type: RequestType::Query,
        api_key: None,
        body: axum::body::Bytes::new(),
        in_fmt: protocol::Format::default(),
        out_fmt: protocol::Format::default(),
    };

    if let Ok(params_json) = sonic_rs::to_vec(&json!({
        "node_id": params.node_id
    })) {
        req.body = axum::body::Bytes::from(params_json);
    }

    let res = state.worker_pool.process(req).await;

    match res {
        Ok(r) => r.into_response(),
        Err(e) => {
            info!(?e, "Got error");
            e.into_response()
        }
    }
}

pub fn node_connections_inner(input: HandlerInput) -> Result<protocol::Response, GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().map_err(GraphError::from)?;
    let arena = bumpalo::Bump::new();

    let node_id_str = if !input.request.body.is_empty() {
        match sonic_rs::from_slice::<sonic_rs::Value>(&input.request.body) {
            Ok(params) => params
                .get("node_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            Err(_) => None,
        }
    } else {
        None
    };

    let node_id_str =
        node_id_str.ok_or_else(|| GraphError::New("node_id is required".to_string()))?;

    let node_id = if let Ok(uuid) = uuid::Uuid::parse_str(&node_id_str) {
        uuid.as_u128()
    } else if let Ok(num) = node_id_str.parse::<u128>() {
        num
    } else {
        return Err(GraphError::New(
            "Invalid node_id format - must be UUID or u128".to_string(),
        ));
    };

    let mut connected_node_ids = HashSet::with_capacity(50);
    let mut connected_nodes = Vec::with_capacity(50);

    let incoming_edges = db
        .in_edges_db
        .prefix_iter(&txn, &node_id.to_be_bytes())?
        .filter_map(|result| match result {
            Ok((_, value)) => match HelixGraphStorage::unpack_adj_edge_data(value) {
                Ok((edge_id, from_node)) => {
                    if connected_node_ids.insert(from_node)
                        && let Ok(node) = db.get_node(&txn, &from_node, &arena)
                    {
                        connected_nodes.push(TraversalValue::Node(node));
                    }

                    match db.get_edge(&txn, &edge_id, &arena) {
                        Ok(edge) => Some(TraversalValue::Edge(edge)),
                        Err(_) => None,
                    }
                }
                Err(_) => None,
            },
            Err(_) => None,
        })
        .collect::<Vec<_>>();

    let outgoing_edges = db
        .out_edges_db
        .prefix_iter(&txn, &node_id.to_be_bytes())?
        .filter_map(|result| match result {
            Ok((_, value)) => match HelixGraphStorage::unpack_adj_edge_data(value) {
                Ok((edge_id, to_node)) => {
                    if connected_node_ids.insert(to_node)
                        && let Ok(node) = db.get_node(&txn, &to_node, &arena)
                    {
                        connected_nodes.push(TraversalValue::Node(node));
                    }

                    match db.get_edge(&txn, &edge_id, &arena) {
                        Ok(edge) => Some(TraversalValue::Edge(edge)),
                        Err(_) => None,
                    }
                }
                Err(_) => None,
            },
            Err(_) => None,
        })
        .collect::<Vec<_>>();

    let connected_nodes_json: Vec<sonic_rs::Value> = connected_nodes
        .into_iter()
        .filter_map(|tv| {
            if let TraversalValue::Node(node) = tv {
                let id_str = ID::from(node.id).stringify();
                let mut node_json = json!({
                    "id": id_str.clone(),
                    "label": node.label,
                    "title": id_str
                });
                if let Some(properties) = &node.properties {
                    for (key, value) in properties.iter() {
                        node_json[key] = sonic_rs::to_value(&value.inner_stringify())
                            .unwrap_or_else(|_| sonic_rs::Value::from(""));
                    }
                }
                Some(node_json)
            } else {
                None
            }
        })
        .collect();

    let incoming_edges_json: Vec<sonic_rs::Value> = incoming_edges
        .into_iter()
        .filter_map(|tv| {
            if let TraversalValue::Edge(edge) = tv {
                Some(json!({
                    "id": ID::from(edge.id).stringify(),
                    "from_node": ID::from(edge.from_node).stringify(),
                    "to_node": ID::from(edge.to_node).stringify(),
                    "label": edge.label
                }))
            } else {
                None
            }
        })
        .collect();

    let outgoing_edges_json: Vec<sonic_rs::Value> = outgoing_edges
        .into_iter()
        .filter_map(|tv| {
            if let TraversalValue::Edge(edge) = tv {
                Some(json!({
                    "id": ID::from(edge.id).stringify(),
                    "from_node": ID::from(edge.from_node).stringify(),
                    "to_node": ID::from(edge.to_node).stringify(),
                    "label": edge.label
                }))
            } else {
                None
            }
        })
        .collect();

    let result = json!({
        "connected_nodes": connected_nodes_json,
        "incoming_edges": incoming_edges_json,
        "outgoing_edges": outgoing_edges_json
    });

    Ok(protocol::Response {
        body: sonic_rs::to_vec(&result).map_err(|e| GraphError::New(e.to_string()))?,
        fmt: Default::default(),
    })
}

inventory::submit! {
    HandlerSubmission(
        Handler::new("node_connections", node_connections_inner, false)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        helix_engine::{
            storage_core::version_info::VersionInfo,
            traversal_core::{
                HelixGraphEngine, HelixGraphEngineOpts,
                config::Config,
                ops::{
                    g::G,
                    source::{add_e::AddEAdapter, add_n::AddNAdapter},
                },
            },
        },
        helix_gateway::router::router::HandlerInput,
        protocol::{Format, request::Request, request::RequestType},
        utils::id::ID,
    };
    use axum::body::Bytes;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn setup_test_engine() -> (HelixGraphEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_str().unwrap();
        let opts = HelixGraphEngineOpts {
            path: db_path.to_string(),
            config: Config::default(),
            version_info: VersionInfo::default(),
        };
        let engine = HelixGraphEngine::new(opts).unwrap();
        (engine, temp_dir)
    }

    #[test]
    fn test_node_connections_with_outgoing() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, _temp_dir) = setup_test_engine();
        let mut txn = engine.storage.graph_env.write_txn().unwrap();
        let arena = bumpalo::Bump::new();

        let node1 = G::new_mut(&engine.storage, &arena, &mut txn)
            .add_n(arena.alloc_str("person"), None, None)
            .collect_to_obj()?;

        let node2 = G::new_mut(&engine.storage, &arena, &mut txn)
            .add_n(arena.alloc_str("person"), None, None)
            .collect_to_obj()?;

        let _edge = G::new_mut(&engine.storage, &arena, &mut txn)
            .add_edge(
                arena.alloc_str("knows"),
                None,
                node1.id(),
                node2.id(),
                false,
            )
            .collect_to_obj()?;

        txn.commit().unwrap();

        let node_id_str = ID::from(node1.id()).stringify();
        let params_json = sonic_rs::to_vec(&json!({"node_id": node_id_str})).unwrap();

        let request = Request {
            name: "node_connections".to_string(),
            req_type: RequestType::Query,
            api_key: None,
            body: Bytes::from(params_json),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
        };

        let result = node_connections_inner(input);
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_str = String::from_utf8(response.body).unwrap();
        assert!(body_str.contains("outgoing_edges"));
        assert!(body_str.contains("connected_nodes"));
        Ok(())
    }

    #[test]
    fn test_node_connections_with_incoming() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, _temp_dir) = setup_test_engine();
        let mut txn = engine.storage.graph_env.write_txn().unwrap();
        let arena = bumpalo::Bump::new();

        let node1 = G::new_mut(&engine.storage, &arena, &mut txn)
            .add_n(arena.alloc_str("person"), None, None)
            .collect_to_obj()?;

        let node2 = G::new_mut(&engine.storage, &arena, &mut txn)
            .add_n(arena.alloc_str("person"), None, None)
            .collect_to_obj()?;

        let _edge = G::new_mut(&engine.storage, &arena, &mut txn)
            .add_edge(
                arena.alloc_str("knows"),
                None,
                node1.id(),
                node2.id(),
                false,
            )
            .collect_to_obj()?;

        txn.commit().unwrap();

        let node_id_str = ID::from(node2.id()).stringify();
        let params_json = sonic_rs::to_vec(&json!({"node_id": node_id_str})).unwrap();

        let request = Request {
            name: "node_connections".to_string(),
            req_type: RequestType::Query,
            api_key: None,
            body: Bytes::from(params_json),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
        };

        let result = node_connections_inner(input);
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_str = String::from_utf8(response.body).unwrap();
        assert!(body_str.contains("incoming_edges"));
        Ok(())
    }

    #[test]
    fn test_node_connections_no_connections() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, _temp_dir) = setup_test_engine();
        let mut txn = engine.storage.graph_env.write_txn().unwrap();
        let arena = bumpalo::Bump::new();

        let node = G::new_mut(&engine.storage, &arena, &mut txn)
            .add_n(arena.alloc_str("person"), None, None)
            .collect_to_obj()?;

        txn.commit().unwrap();

        let node_id_str = ID::from(node.id()).stringify();
        let params_json = sonic_rs::to_vec(&json!({"node_id": node_id_str})).unwrap();

        let request = Request {
            name: "node_connections".to_string(),
            req_type: RequestType::Query,
            api_key: None,
            body: Bytes::from(params_json),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
        };

        let result = node_connections_inner(input);
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_str = String::from_utf8(response.body).unwrap();
        assert!(body_str.contains("connected_nodes"));
        assert!(body_str.contains("incoming_edges"));
        assert!(body_str.contains("outgoing_edges"));
        Ok(())
    }

    #[test]
    fn test_node_connections_invalid_id() {
        let (engine, _temp_dir) = setup_test_engine();

        let params_json = sonic_rs::to_vec(&json!({"node_id": "invalid"})).unwrap();

        let request = Request {
            name: "node_connections".to_string(),
            req_type: RequestType::Query,
            api_key: None,
            body: Bytes::from(params_json),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
        };

        let result = node_connections_inner(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_node_connections_missing_node_id() {
        let (engine, _temp_dir) = setup_test_engine();

        let request = Request {
            name: "node_connections".to_string(),
            req_type: RequestType::Query,
            api_key: None,
            body: Bytes::new(),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
        };

        let result = node_connections_inner(input);
        assert!(result.is_err());
    }
}
