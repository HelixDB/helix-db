use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;
use sonic_rs::{JsonValueTrait, json};
use tracing::info;

use crate::helix_engine::types::GraphError;
use crate::helix_gateway::gateway::AppState;
use crate::helix_gateway::router::router::{Handler, HandlerInput, HandlerSubmission};
use crate::protocol::{self, request::RequestType};
use crate::utils::filterable::Filterable;
use crate::utils::id::ID;
use crate::utils::items::Node;

// get all nodes with a specific label
// curl "http://localhost:PORT/nodes-by-label?label=YOUR_LABEL&limit=100"

#[derive(Deserialize)]
pub struct NodesByLabelQuery {
    label: String,
    limit: Option<usize>,
}

pub async fn nodes_by_label_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<NodesByLabelQuery>,
) -> axum::http::Response<Body> {
    let mut req = protocol::request::Request {
        name: "nodes_by_label".to_string(),
        req_type: RequestType::Query,
        body: axum::body::Bytes::new(),
        in_fmt: protocol::Format::default(),
        out_fmt: protocol::Format::default(),
    };

    if let Ok(params_json) = sonic_rs::to_vec(&json!({
        "label": params.label,
        "limit": params.limit
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

pub fn nodes_by_label_inner(input: HandlerInput) -> Result<protocol::Response, GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().map_err(GraphError::from)?;

    let (label, limit) = if !input.request.body.is_empty() {
        match sonic_rs::from_slice::<sonic_rs::Value>(&input.request.body) {
            Ok(params) => {
                let label = params
                    .get("label")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                let limit = params
                    .get("limit")
                    .and_then(|v| v.as_u64())
                    .map(|n| n as usize);
                (label, limit)
            }
            Err(_) => (None, None),
        }
    } else {
        (None, None)
    };

    let label = label.ok_or_else(|| GraphError::New("label is required".to_string()))?;

    let mut nodes_json = Vec::new();
    let mut count = 0;

    for result in db.nodes_db.iter(&txn)? {
        let (id, node_data) = result?;
        match Node::decode_node(node_data, id) {
            Ok(node) => {
                if node.label() == label {
                    let id_str = ID::from(id).stringify();

                    let mut node_json = json!({
                        "id": id_str.clone(),
                        "label": node.label(),
                        "title": id_str
                    });

                    // Add node properties
                    if let Some(properties) = &node.properties {
                        for (key, value) in properties {
                            node_json[key] = sonic_rs::to_value(&value.inner_stringify())
                                .unwrap_or_else(|_| sonic_rs::Value::from(""));
                        }
                    }

                    nodes_json.push(node_json);
                    count += 1;

                    if let Some(limit_count) = limit {
                        if count >= limit_count {
                            break;
                        }
                    }
                }
            }
            Err(_) => continue,
        }
    }

    let result = json!({
        "nodes": nodes_json,
        "count": count
    });

    Ok(protocol::Response {
        body: sonic_rs::to_vec(&result).map_err(|e| GraphError::New(e.to_string()))?,
        fmt: Default::default(),
    })
}

inventory::submit! {
    HandlerSubmission(
        Handler::new("nodes_by_label", nodes_by_label_inner)
    )
}
