use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;
use sonic_rs::{JsonValueTrait, json};
use tracing::info;

use crate::helix_engine::storage_core::storage_methods::StorageMethods;
use crate::helix_engine::types::GraphError;
use crate::helix_gateway::gateway::AppState;
use crate::helix_gateway::router::router::{Handler, HandlerInput, HandlerSubmission};
use crate::protocol::{self, request::RequestType};
use crate::utils::filterable::Filterable;
use crate::utils::id::ID;

// get node details by ID
// curl "http://localhost:PORT/node-details?id=YOUR_NODE_ID"

#[derive(Deserialize)]
pub struct NodeDetailsQuery {
    id: String,
}

pub async fn node_details_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<NodeDetailsQuery>,
) -> axum::http::Response<Body> {
    let mut req = protocol::request::Request {
        name: "node_details".to_string(),
        req_type: RequestType::Query,
        body: axum::body::Bytes::new(),
        in_fmt: protocol::Format::default(),
        out_fmt: protocol::Format::default(),
    };

    if let Ok(params_json) = sonic_rs::to_vec(&json!({
        "id": params.id
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

pub fn node_details_inner(input: HandlerInput) -> Result<protocol::Response, GraphError> {
    let db = Arc::clone(&input.graph.storage);
    let txn = db.graph_env.read_txn().map_err(GraphError::from)?;

    let node_id_str = if !input.request.body.is_empty() {
        match sonic_rs::from_slice::<sonic_rs::Value>(&input.request.body) {
            Ok(params) => params
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            Err(_) => None,
        }
    } else {
        None
    };

    let node_id_str = node_id_str.ok_or_else(|| GraphError::New("id is required".to_string()))?;

    let node_id = match uuid::Uuid::parse_str(&node_id_str) {
        Ok(uuid) => uuid.as_u128(),
        Err(_) => match node_id_str.parse::<u128>() {
            Ok(id) => id,
            Err(_) => {
                return Err(GraphError::New(
                    "invalid ID format: must be UUID or u128".to_string(),
                ));
            }
        },
    };

    let result = match db.get_node(&txn, &node_id) {
        Ok(node) => {
            let id_str = ID::from(node_id).stringify();

            let mut node_json = json!({
                "id": id_str.clone(),
                "label": node.label(),
                "title": id_str
            });

            if let Some(properties) = &node.properties {
                for (key, value) in properties {
                    node_json[key] = sonic_rs::to_value(&value.inner_stringify())
                        .unwrap_or_else(|_| sonic_rs::Value::from(""));
                }
            }

            json!({
                "node": node_json,
                "found": true
            })
        }
        Err(_) => {
            json!({
                "node": null,
                "found": false
            })
        }
    };

    Ok(protocol::Response {
        body: sonic_rs::to_vec(&result).map_err(|e| GraphError::New(e.to_string()))?,
        fmt: Default::default(),
    })
}

inventory::submit! {
    HandlerSubmission(
        Handler::new("node_details", node_details_inner)
    )
}
