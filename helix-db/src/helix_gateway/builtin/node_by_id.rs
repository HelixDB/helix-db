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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;
    use axum::body::Bytes;
    use crate::helix_engine::traversal_core::traversal_value::Traversable;
    use crate::{
        helix_engine::{
            storage_core::version_info::VersionInfo,
            traversal_core::{
                HelixGraphEngine, HelixGraphEngineOpts,
                config::Config,
                ops::{
                    g::G,
                    source::add_n::AddNAdapter,
                },
            },
        },
        protocol::{request::Request, request::RequestType, Format, value::Value},
        helix_gateway::router::router::HandlerInput,
        utils::id::ID,
    };

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
    fn test_node_details_found() {
        let (engine, _temp_dir) = setup_test_engine();
        let mut txn = engine.storage.graph_env.write_txn().unwrap();

        let node = G::new_mut(Arc::clone(&engine.storage), &mut txn)
            .add_n("person", Some(vec![("name".to_string(), Value::String("Alice".to_string()))]), None)
            .collect_to_obj()?;

        txn.commit().unwrap();

        let node_id_str = ID::from(node.id()).stringify();
        let params_json = sonic_rs::to_vec(&json!({"id": node_id_str})).unwrap();

        let request = Request {
            name: "node_details".to_string(),
            req_type: RequestType::Query,
            body: Bytes::from(params_json),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
            
        };

        let result = node_details_inner(input);
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_str = String::from_utf8(response.body).unwrap();
        assert!(body_str.contains("\"found\":true"));
    }

    #[test]
    fn test_node_details_not_found() {
        let (engine, _temp_dir) = setup_test_engine();

        let fake_id = uuid::Uuid::new_v4().to_string();
        let params_json = sonic_rs::to_vec(&json!({"id": fake_id})).unwrap();

        let request = Request {
            name: "node_details".to_string(),
            req_type: RequestType::Query,
            body: Bytes::from(params_json),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
            
        };

        let result = node_details_inner(input);
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_str = String::from_utf8(response.body).unwrap();
        assert!(body_str.contains("\"found\":false"));
    }

    #[test]
    fn test_node_details_invalid_id_format() {
        let (engine, _temp_dir) = setup_test_engine();

        let params_json = sonic_rs::to_vec(&json!({"id": "not-a-valid-id"})).unwrap();

        let request = Request {
            name: "node_details".to_string(),
            req_type: RequestType::Query,
            body: Bytes::from(params_json),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
            
        };

        let result = node_details_inner(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_node_details_missing_id() {
        let (engine, _temp_dir) = setup_test_engine();

        let request = Request {
            name: "node_details".to_string(),
            req_type: RequestType::Query,
            body: Bytes::new(),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
            
        };

        let result = node_details_inner(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_node_details_with_properties() {
        let (engine, _temp_dir) = setup_test_engine();
        let mut txn = engine.storage.graph_env.write_txn().unwrap();

        let node = G::new_mut(Arc::clone(&engine.storage), &mut txn)
            .add_n("person", Some(vec![
                ("name".to_string(), Value::String("Alice".to_string())),
                ("age".to_string(), Value::I64(30)),
            ]), None)
            .collect_to_obj()?;

        txn.commit().unwrap();

        let node_id_str = ID::from(node.id()).stringify();
        let params_json = sonic_rs::to_vec(&json!({"id": node_id_str})).unwrap();

        let request = Request {
            name: "node_details".to_string(),
            req_type: RequestType::Query,
            body: Bytes::from(params_json),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
            
        };

        let result = node_details_inner(input);
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_str = String::from_utf8(response.body).unwrap();
        assert!(body_str.contains("Alice"));
        assert!(body_str.contains("30"));
    }
}
