use crate::{
    helix_engine::{storage_core::graph_visualization::GraphVisualization, types::GraphError},
    helix_gateway::{gateway::AppState, router::router::HandlerInput},
    protocol::{self, HelixError, request::RequestType},
};
use axum::{body::Body, extract::State, http::HeaderValue, response::IntoResponse};
use sonic_rs::{JsonContainerTrait, JsonValueMutTrait, Value};
use std::sync::Arc;
use tracing::info;

pub async fn graphvis_handler(
    State(state): State<Arc<AppState>>,
    mut req: protocol::request::Request,
) -> axum::http::Response<Body> {
    req.req_type = RequestType::GraphVis;
    let res = state.worker_pool.process(req).await;

    match res {
        Ok(r) => {
            let mut out = r.into_response();
            out.headers_mut()
                .insert("Content-Type", HeaderValue::from_static("text/html"));
            out
        }
        Err(e) => {
            info!(?e, "Got error");
            e.into_response()
        }
    }
}

pub fn graphvis_inner(input: &HandlerInput) -> Result<protocol::Response, HelixError> {
    let db = Arc::clone(&input.context.graph_access.storage);
    let txn = db.graph_env.read_txn().map_err(GraphError::from)?;

    let json_ne: String =
        db.nodes_edges_to_json(&txn, None, db.storage_config.graphvis_node_label.clone())?;

    let json_ne_m = modify_graph_json(&json_ne).map_err(|e| GraphError::New(e.to_string()))?;

    let db_counts: String = db.get_db_stats_json(&txn)?;
    let db_counts_m: Value =
        sonic_rs::from_str(&db_counts).map_err(|e| GraphError::New(e.to_string()))?;
    let num_nodes = json_ne_m["nodes"]
        .as_array()
        .map(|arr| arr.len())
        .unwrap_or(0);

    let html_template = include_str!("graphvis.html");
    let html_content = html_template
        .replace(
            "{NODES_JSON_DATA}",
            &sonic_rs::to_string(&json_ne_m["nodes"]).unwrap(),
        )
        .replace(
            "{EDGES_JSON_DATA}",
            &sonic_rs::to_string(&json_ne_m["edges"]).unwrap(),
        )
        .replace(
            "{NUM_NODES}",
            &sonic_rs::to_string(&db_counts_m["num_nodes"]).unwrap(),
        )
        .replace(
            "{NUM_EDGES}",
            &sonic_rs::to_string(&db_counts_m["num_edges"]).unwrap(),
        )
        .replace(
            "{NUM_VECTORS}",
            &sonic_rs::to_string(&db_counts_m["num_vectors"]).unwrap(),
        )
        .replace("{NUM_NODES_SHOWING}", &num_nodes.to_string());

    // This is a hack for the moment
    Ok(protocol::Response {
        body: html_content.into_bytes(),
        fmt: Default::default(),
    })
}

fn modify_graph_json(input: &str) -> Result<Value, sonic_rs::Error> {
    let mut json: Value = sonic_rs::from_str(input)?;

    if let Some(nodes) = json.get_mut("nodes").and_then(|n| n.as_array_mut()) {
        for node in nodes {
            if let Some(obj) = node.as_object_mut() {
                obj.insert("color", Value::from_static_str("#97c2fc"));
                obj.insert("shape", Value::from_static_str("dot"));
            }
        }
    }

    if let Some(edges) = json.get_mut("edges").and_then(|e| e.as_array_mut()) {
        for edge in edges {
            if let Some(obj) = edge.as_object_mut() {
                obj.insert("arrows", Value::from_static_str("to"));
            }
        }
    }

    Ok(json)
}
