use std::sync::Arc;

#[cfg(feature = "rocks")]
use crate::helix_engine::storage_core::txn::ReadTransaction;
use crate::helix_engine::types::GraphError;
use crate::helix_gateway::gateway::AppState;
use crate::helix_gateway::router::router::{Handler, HandlerInput, HandlerSubmission};
use crate::protocol::{self, request::RequestType};
use crate::utils::id::ID;
use crate::utils::items::Node;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::response::IntoResponse;
use serde::Deserialize;
use sonic_rs::{JsonValueTrait, json};
use tracing::info;

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
        api_key_hash: None,
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
    let txn = db.graph_env.read_txn()?;
    let arena = bumpalo::Bump::new();

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
    const MAX_PREALLOCATE_CAPACITY: usize = 100_000;

    let initial_capacity = match limit {
        Some(n) if n <= MAX_PREALLOCATE_CAPACITY => n,
        Some(_) => MAX_PREALLOCATE_CAPACITY,
        None => 100,
    };

    let mut nodes_json = Vec::with_capacity(initial_capacity);
    let mut count = 0;

    #[cfg(feature = "lmdb")]
    {
        // LMDB implementation
        for result in db.nodes_db.iter(&txn)? {
            let (id, node_data) = result?;
            match Node::from_bincode_bytes(id, node_data, &arena) {
                Ok(node) => {
                    if node.label == label {
                        let id_str = ID::from(id).stringify();

                        let mut node_json = json!({
                            "id": id_str.clone(),
                            "label": node.label,
                            "title": id_str
                        });

                        // Add node properties
                        if let Some(properties) = &node.properties {
                            for (key, value) in properties.iter() {
                                node_json[key] = sonic_rs::to_value(&value.inner_stringify())
                                    .unwrap_or_else(|_| sonic_rs::Value::from(""));
                            }
                        }

                        nodes_json.push(node_json);
                        count += 1;

                        if let Some(limit_count) = limit
                            && count >= limit_count
                        {
                            break;
                        }
                    }
                }
                Err(_) => continue,
            }
        }
    }

    #[cfg(feature = "rocks")]
    {
        // RocksDB implementation
        let cf_nodes = db.cf_nodes();
        let iter = txn.iterator_cf(&cf_nodes, rocksdb::IteratorMode::Start);

        for (key, value) in iter.flatten() {
            assert!(key.len() == 16);
            let id = u128::from_be_bytes(key[..].try_into().unwrap());
            if let Ok(node) = Node::from_bincode_bytes(id, &value, &arena)
                && node.label == label
            {
                let id_str = ID::from(id).stringify();

                let mut node_json = json!({
                    "id": id_str.clone(),
                    "label": node.label,
                    "title": id_str
                });

                // Add node properties
                if let Some(properties) = &node.properties {
                    for (key, value) in properties.iter() {
                        node_json[key] = sonic_rs::to_value(&value.inner_stringify())
                            .unwrap_or_else(|_| sonic_rs::Value::from(""));
                    }
                }

                nodes_json.push(node_json);
                count += 1;

                if let Some(limit_count) = limit
                    && count >= limit_count
                {
                    break;
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "rocks")]
    use crate::helix_engine::storage_core::txn::WriteTransaction;
    use crate::{
        helix_engine::{
            storage_core::version_info::VersionInfo,
            traversal_core::{
                HelixGraphEngine, HelixGraphEngineOpts,
                config::Config,
                ops::{g::G, source::add_n::AddNAdapter},
            },
        },
        helix_gateway::router::router::HandlerInput,
        protocol::{Format, request::Request, request::RequestType, value::Value},
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
    fn test_nodes_by_label_found() -> Result<(), Box<dyn std::error::Error>> {
        use crate::utils::properties::ImmutablePropertiesMap;

        let (engine, _temp_dir) = setup_test_engine();
        let mut txn = engine.storage.graph_env.write_txn().unwrap();
        let arena = bumpalo::Bump::new();

        let props1 = vec![("name", Value::String("Alice".to_string()))];
        let props_map1 = ImmutablePropertiesMap::new(
            props1.len(),
            props1
                .iter()
                .map(|(k, v)| (arena.alloc_str(k) as &str, v.clone())),
            &arena,
        );

        let _node1 = G::new_mut(&engine.storage, &arena, &mut txn)
            .add_n(arena.alloc_str("person"), Some(props_map1), None)
            .collect_to_obj()?;

        let props2 = vec![("name", Value::String("Bob".to_string()))];
        let props_map2 = ImmutablePropertiesMap::new(
            props2.len(),
            props2
                .iter()
                .map(|(k, v)| (arena.alloc_str(k) as &str, v.clone())),
            &arena,
        );

        let _node2 = G::new_mut(&engine.storage, &arena, &mut txn)
            .add_n(arena.alloc_str("person"), Some(props_map2), None)
            .collect_to_obj()?;

        txn.commit().unwrap();

        let params_json = sonic_rs::to_vec(&json!({"label": "person"})).unwrap();

        let request = Request {
            name: "nodes_by_label".to_string(),
            req_type: RequestType::Query,
            api_key_hash: None,
            body: Bytes::from(params_json),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
        };

        let result = nodes_by_label_inner(input);
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_str = String::from_utf8(response.body).unwrap();
        assert!(body_str.contains("\"count\":2"));
        Ok(())
    }

    #[test]
    fn test_nodes_by_label_with_limit() -> Result<(), Box<dyn std::error::Error>> {
        use crate::utils::properties::ImmutablePropertiesMap;

        let (engine, _temp_dir) = setup_test_engine();
        let mut txn = engine.storage.graph_env.write_txn().unwrap();
        let arena = bumpalo::Bump::new();

        for i in 0..10 {
            let props = vec![("index", Value::I64(i))];
            let props_map = ImmutablePropertiesMap::new(
                props.len(),
                props
                    .iter()
                    .map(|(k, v)| (arena.alloc_str(k) as &str, v.clone())),
                &arena,
            );

            let _node = G::new_mut(&engine.storage, &arena, &mut txn)
                .add_n(arena.alloc_str("person"), Some(props_map), None)
                .collect_to_obj()?;
        }

        txn.commit().unwrap();

        let params_json = sonic_rs::to_vec(&json!({"label": "person", "limit": 5})).unwrap();

        let request = Request {
            name: "nodes_by_label".to_string(),
            req_type: RequestType::Query,
            api_key_hash: None,
            body: Bytes::from(params_json),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
        };

        let result = nodes_by_label_inner(input);
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_str = String::from_utf8(response.body).unwrap();
        assert!(body_str.contains("\"count\":5"));
        Ok(())
    }

    #[test]
    fn test_nodes_by_label_not_found() {
        let (engine, _temp_dir) = setup_test_engine();

        let params_json = sonic_rs::to_vec(&json!({"label": "nonexistent"})).unwrap();

        let request = Request {
            name: "nodes_by_label".to_string(),
            req_type: RequestType::Query,
            api_key_hash: None,
            body: Bytes::from(params_json),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
        };

        let result = nodes_by_label_inner(input);
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_str = String::from_utf8(response.body).unwrap();
        assert!(body_str.contains("\"count\":0"));
    }

    #[test]
    fn test_nodes_by_label_missing_label() {
        let (engine, _temp_dir) = setup_test_engine();

        let request = Request {
            name: "nodes_by_label".to_string(),
            req_type: RequestType::Query,
            api_key_hash: None,
            body: Bytes::new(),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
        };

        let result = nodes_by_label_inner(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_nodes_by_label_multiple_labels() -> Result<(), Box<dyn std::error::Error>> {
        let (engine, _temp_dir) = setup_test_engine();
        let mut txn = engine.storage.graph_env.write_txn().unwrap();
        let arena = bumpalo::Bump::new();

        let _person = G::new_mut(&engine.storage, &arena, &mut txn)
            .add_n(arena.alloc_str("person"), None, None)
            .collect_to_obj()?;

        let _company = G::new_mut(&engine.storage, &arena, &mut txn)
            .add_n(arena.alloc_str("company"), None, None)
            .collect_to_obj()?;

        txn.commit().unwrap();

        let params_json = sonic_rs::to_vec(&json!({"label": "person"})).unwrap();

        let request = Request {
            name: "nodes_by_label".to_string(),
            req_type: RequestType::Query,
            api_key_hash: None,
            body: Bytes::from(params_json),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        };

        let input = HandlerInput {
            graph: Arc::new(engine),
            request,
        };

        let result = nodes_by_label_inner(input);
        assert!(result.is_ok());

        let response = result.unwrap();
        let body_str = String::from_utf8(response.body).unwrap();
        assert!(body_str.contains("\"count\":1"));
        Ok(())
    }
}
