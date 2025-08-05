use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};
use sonic_rs::{JsonValueTrait, json};
use tracing::info;

use crate::helix_engine::graph_core::ops::tr_val::TraversalVal;
use crate::helix_engine::types::GraphError;
use crate::helix_gateway::gateway::AppState;
use crate::helix_gateway::router::router::{Handler, HandlerInput, HandlerSubmission};
use crate::protocol::remapping::RemappingMap;
use crate::protocol::return_values::ReturnValue;
use crate::protocol::{self, request::RequestType};
use crate::utils::filterable::Filterable;
use crate::utils::items::Node;

// get all nodes with a specific label
// curl "http://localhost:PORT/nodes-by-label?label=YOUR_LABEL"

#[derive(Deserialize)]
pub struct NodesByLabelQuery {
    label: String,
}

#[derive(Serialize)]
pub struct NodesByLabelResponse {
    pub nodes: Vec<ReturnValue>,
    pub count: usize,
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
        "label": params.label
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

pub fn nodes_by_label_inner(input: &HandlerInput) -> Result<protocol::Response, GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().map_err(GraphError::from)?;

    let label = if !input.request.body.is_empty() {
        match sonic_rs::from_slice::<sonic_rs::Value>(&input.request.body) {
            Ok(params) => params
                .get("label")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            Err(_) => None,
        }
    } else {
        None
    };

    let label = label.ok_or_else(|| GraphError::New("label is required".to_string()))?;

    let remapping_vals = RemappingMap::new();

    let nodes: Vec<TraversalVal> = db
        .nodes_db
        .iter(&txn)?
        .filter_map(|result| match result {
            Ok((id, node_data)) => match Node::decode_node(node_data, id) {
                Ok(node) => {
                    if node.label() == label {
                        Some(Ok(TraversalVal::Node(node)))
                    } else {
                        None
                    }
                }
                Err(e) => Some(Err(e)),
            },
            Err(e) => Some(Err(GraphError::from(e))),
        })
        .collect::<Result<Vec<_>, GraphError>>()?;

    let count = nodes.len();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();

    return_vals.insert(
        "nodes".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(nodes, remapping_vals.borrow_mut()),
    );

    return_vals.insert("count".to_string(), ReturnValue::from(count as i32));

    Ok(protocol::Response {
        body: sonic_rs::to_vec(&return_vals).map_err(|e| GraphError::New(e.to_string()))?,
        fmt: Default::default(),
    })
}

inventory::submit! {
    HandlerSubmission(
        Handler::new("nodes_by_label", nodes_by_label_inner)
    )
}
