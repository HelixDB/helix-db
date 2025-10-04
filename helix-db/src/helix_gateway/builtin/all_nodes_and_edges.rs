use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;
use sonic_rs::{JsonValueTrait, json};
use tracing::info;

use crate::helix_engine::storage_core::graph_visualization::GraphVisualization;
use crate::helix_engine::types::GraphError;
use crate::helix_engine::vector_core::hnsw::HNSW;
use crate::helix_gateway::gateway::AppState;
use crate::helix_gateway::router::router::{Handler, HandlerInput, HandlerSubmission};
use crate::protocol::{self, request::RequestType};
use crate::utils::id::ID;
use crate::utils::items::{Edge, Node};
use heed3::RoTxn;

// get top nodes by cardinality (with limit, max 300):
// curl "http://localhost:PORT/nodes-edges?limit=50"

// get top 100 nodes with most connections and include a specific node property as label
// curl "http://localhost:PORT/nodes-edges?limit=100&node_label=name"

// get everything (no limit)
// curl "http://localhost:PORT/nodes-edges"

// get everything with a specific node property as label
// curl "http://localhost:PORT/nodes-edges?node_label=name"

#[derive(Deserialize)]
pub struct NodesEdgesQuery {
    limit: Option<usize>,
    node_label: Option<String>,
}

pub async fn nodes_edges_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<NodesEdgesQuery>,
) -> axum::http::Response<Body> {
    let mut req = protocol::request::Request {
        name: "nodes_edges".to_string(),
        req_type: RequestType::Query,
        body: axum::body::Bytes::new(),
        in_fmt: protocol::Format::default(),
        out_fmt: protocol::Format::default(),
    };

    if let Ok(params_json) = sonic_rs::to_vec(&json!({
        "limit": params.limit,
        "node_label": params.node_label
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

pub fn nodes_edges_inner(input: HandlerInput) -> Result<protocol::Response, GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().map_err(GraphError::from)?;

    let (limit, node_label) = if !input.request.body.is_empty() {
        match sonic_rs::from_slice::<sonic_rs::Value>(&input.request.body) {
            Ok(params) => (
                params
                    .get("limit")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as usize),
                params
                    .get("node_label")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string()),
            ),
            Err(_) => (None, None),
        }
    } else {
        (None, None)
    };

    let json_result = if limit.is_some() {
        db.nodes_edges_to_json(&txn, limit, node_label)?
    } else {
        get_all_nodes_edges_json(&db, &txn, node_label)?
    };

    let db_stats = db.get_db_stats_json(&txn)?;

    let vectors_result = db
        .vectors
        .get_all_vectors(&txn, None)
        .map(|vecs| {
            let vectors_json: Vec<sonic_rs::Value> = vecs
                .iter()
                .map(|v| {
                    json!({
                        "id": v.id.to_string(),
                        "level": v.level,
                        "distance": v.distance,
                        "data": v.data,
                        "dimension": v.data.len()
                    })
                })
                .collect();
            sonic_rs::to_string(&vectors_json).unwrap_or_else(|_| "[]".to_string())
        })
        .unwrap_or_else(|_| "[]".to_string());

    let combined =
        format!(r#"{{"data": {json_result}, "vectors": {vectors_result}, "stats": {db_stats}}}"#);

    Ok(protocol::Response {
        body: combined.into_bytes(),
        fmt: Default::default(),
    })
}

fn get_all_nodes_edges_json(
    db: &Arc<crate::helix_engine::storage_core::HelixGraphStorage>,
    txn: &RoTxn,
    node_label: Option<String>,
) -> Result<String, GraphError> {
    use crate::utils::filterable::Filterable;
    use sonic_rs::json;

    let nodes_length = db.nodes_db.len(txn)?;
    let mut nodes = Vec::with_capacity(nodes_length as usize);
    let node_iter = db.nodes_db.iter(txn)?;
    for result in node_iter {
        let (id, value) = result?;
        let id_str = ID::from(id).stringify();

        let mut json_node = json!({
            "id": id_str.clone(),
            "title": id_str.clone()
        });

        if let Some(prop) = &node_label {
            let node = Node::decode_node(value, id)?;
            json_node["label"] = json!(node.label());
            if let Some(props) = node.properties {
                if let Some(prop_value) = props.get(prop) {
                    json_node["label"] = sonic_rs::to_value(&prop_value.inner_stringify())
                        .unwrap_or_else(|_| sonic_rs::Value::from(""));
                }
            }
        }
        nodes.push(json_node);
    }

    let edges_length = db.edges_db.len(txn)?;
    let mut edges = Vec::with_capacity(edges_length as usize);
    let edge_iter = db.edges_db.iter(txn)?;
    for result in edge_iter {
        let (id, value) = result?;
        let edge = Edge::decode_edge(value, id)?;
        let id_str = ID::from(id).stringify();

        edges.push(json!({
            "from": ID::from(edge.from_node).stringify(),
            "to": ID::from(edge.to_node).stringify(),
            "title": id_str.clone(),
            "id": id_str
        }));
    }

    let result = json!({
        "nodes": nodes,
        "edges": edges
    });

    sonic_rs::to_string(&result).map_err(|e| GraphError::New(e.to_string()))
}

inventory::submit! {
    HandlerSubmission(
        Handler::new("nodes_edges", nodes_edges_inner)
    )
}
