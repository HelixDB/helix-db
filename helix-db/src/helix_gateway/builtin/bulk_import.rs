use std::collections::HashMap;
use std::sync::Arc;

use sonic_rs::{JsonContainerTrait, JsonValueTrait};

use crate::helix_engine::bm25::bm25::HBM25Config;
use crate::helix_engine::storage_core::HelixGraphStorage;
use crate::helix_engine::types::GraphError;
use crate::helix_gateway::router::router::{Handler, HandlerInput, HandlerSubmission};
use crate::protocol;
use crate::protocol::value::Value;
use crate::utils::id::{v6_uuid, ID};
use crate::utils::items::{Edge, Node};
use crate::utils::label_hash::hash_label;
use crate::utils::properties::ImmutablePropertiesMap;

/// Converts a `sonic_rs::Value` into the internal `Value` type used by HelixDB.
fn sonic_value_to_value(v: &sonic_rs::Value) -> Value {
    if let Some(s) = v.as_str() {
        Value::String(s.to_string())
    } else if let Some(b) = v.as_bool() {
        Value::Boolean(b)
    } else if let Some(n) = v.as_i64() {
        Value::I64(n)
    } else if let Some(n) = v.as_u64() {
        Value::U64(n)
    } else if let Some(n) = v.as_f64() {
        Value::F64(n)
    } else if let Some(arr) = v.as_array() {
        Value::Array(arr.iter().map(sonic_value_to_value).collect())
    } else if let Some(obj) = v.as_object() {
        let mut map = HashMap::with_capacity(obj.len());
        for (k, val) in obj.iter() {
            map.insert(k.to_string(), sonic_value_to_value(val));
        }
        Value::Object(map)
    } else {
        Value::Empty
    }
}

fn insert_bm25_node_doc(
    bm25: &HBM25Config,
    txn: &mut heed3::RwTxn<'_>,
    node_id: u128,
    properties: &ImmutablePropertiesMap<'_>,
    label: &str,
) -> Result<(), GraphError> {
    bm25.insert_doc_for_node(txn, node_id, properties, label)
}

/// Bulk imports nodes and edges into the graph in a single atomic LMDB transaction.
///
/// # JSON Request Format
///
/// ```json
/// {
///   "nodes": [
///     { "label": "Person", "temp_id": "n0", "properties": { "name": "Alice", "age": 30 } },
///     { "label": "Person", "temp_id": "n1", "properties": { "name": "Bob" } }
///   ],
///   "edges": [
///     { "label": "Knows", "from": "n0", "to": "n1", "properties": { "since": "2024" } }
///   ]
/// }
/// ```
///
/// - `nodes`: Required array of node objects. Each node must have `label` (string) and `temp_id`
///   (string). `properties` is optional.
/// - `edges`: Optional array of edge objects. Each edge must have `label`, `from`, and `to`.
///   `from` and `to` can reference either a `temp_id` from this request or an existing node UUID.
///   `properties` is optional.
///
/// # Response Format
///
/// ```json
/// {
///   "node_ids": { "n0": "<uuid>", "n1": "<uuid>" },
///   "nodes_created": 2,
///   "edges_created": 1
/// }
/// ```
///
/// `temp_id` values are scoped to a single request and are used only to wire up edges within the
/// same batch. The entire batch is atomic -- if any part fails, nothing is committed.
pub fn bulk_import_inner(input: HandlerInput) -> Result<protocol::Response, GraphError> {
    // 1. Deserialize request body into typed structs
    let req: BulkImportRequest = sonic_rs::from_slice(&input.request.body)
        .map_err(|e| GraphError::New(format!("Invalid JSON: {e}")))?;

    // 2. Get storage and extract version_info reference before consuming input
    let db = Arc::clone(&input.graph.storage);
    let version_info = &db.version_info;

    // 3. Open write txn
    let mut txn = db.graph_env.write_txn().map_err(GraphError::from)?;

    // 4. Create arena with pre-sized capacity
    let arena = bumpalo::Bump::with_capacity(req.nodes.len() * 80);

    // 5. Phase 1: Create all nodes
    let mut temp_id_map: HashMap<String, u128> = HashMap::with_capacity(req.nodes.len());
    let mut nodes_created: usize = 0;
    let mut buf = Vec::with_capacity(512);

    for (i, node_val) in req.nodes.iter().enumerate() {
        let label: &str = arena.alloc_str(&node_val.label);
        let temp_id = &node_val.temp_id;

        // Check for duplicate temp_id
        if temp_id_map.contains_key(temp_id.as_str()) {
            return Err(GraphError::New(format!(
                "Duplicate temp_id '{}' at node index {i}",
                temp_id
            )));
        }

        // Parse properties
        let properties = if let Some(ref props_map) = node_val.properties {
            if !props_map.is_empty() {
                let items: Vec<(&str, Value)> = props_map
                    .iter()
                    .map(|(k, v)| {
                        let key: &str = arena.alloc_str(k);
                        (key, sonic_value_to_value(v))
                    })
                    .collect();
                Some(ImmutablePropertiesMap::new(
                    items.len(),
                    items.into_iter(),
                    &arena,
                ))
            } else {
                None
            }
        } else {
            None
        };

        let node_id = v6_uuid();
        let version = version_info.get_latest(label);
        let node = Node {
            id: node_id,
            label,
            version,
            properties,
        };

        // Serialize and write to nodes_db
        buf.clear();
        bincode::serialize_into(&mut buf, &node)
            .map_err(|e| GraphError::New(format!("Serialize: {e}")))?;
        db.nodes_db.put(&mut txn, &node_id, &buf)?;

        // Handle secondary indexes
        for (index_name, (index_db, secondary_index)) in &db.secondary_indices {
            if let Some(value) = node.get_property(index_name) {
                let serialized = bincode::serialize(value)
                    .map_err(|e| GraphError::New(format!("Index serialize: {e}")))?;
                match secondary_index {
                    crate::helix_engine::types::SecondaryIndex::Unique(_) => {
                        index_db
                            .put_with_flags(
                                &mut txn,
                                heed3::PutFlags::NO_OVERWRITE,
                                &serialized,
                                &node_id,
                            )
                            .map_err(|_| {
                                GraphError::DuplicateKey(format!(
                                    "Unique index '{index_name}' violation at node index {i}"
                                ))
                            })?;
                    }
                    crate::helix_engine::types::SecondaryIndex::Index(_) => {
                        index_db.put(&mut txn, &serialized, &node_id)?;
                    }
                    crate::helix_engine::types::SecondaryIndex::None => {}
                }
            }
        }

        // Insert into BM25 index if configured
        if let Some(bm25) = &db.bm25
            && let Some(props) = node.properties.as_ref()
        {
            insert_bm25_node_doc(bm25, &mut txn, node_id, props, node.label)?;
        }

        temp_id_map.insert(temp_id.clone(), node_id);
        nodes_created += 1;
    }

    // 6. Phase 2: Create all edges
    let mut edges_created: usize = 0;

    for (i, edge_val) in req.edges.iter().enumerate() {
        let label: &str = arena.alloc_str(&edge_val.label);

        let from_str = &edge_val.from;
        let to_str = &edge_val.to;

        // Resolve from: try UUID parse first, then temp_id lookup
        let from_node = resolve_node_ref(from_str, &temp_id_map).map_err(|_| {
            GraphError::New(format!(
                "Edge at index {i}: unknown 'from' reference '{from_str}'"
            ))
        })?;
        let to_node = resolve_node_ref(to_str, &temp_id_map).map_err(|_| {
            GraphError::New(format!(
                "Edge at index {i}: unknown 'to' reference '{to_str}'"
            ))
        })?;

        // Parse edge properties
        let properties = if let Some(ref props_map) = edge_val.properties {
            if !props_map.is_empty() {
                let items: Vec<(&str, Value)> = props_map
                    .iter()
                    .map(|(k, v)| {
                        let key: &str = arena.alloc_str(k);
                        (key, sonic_value_to_value(v))
                    })
                    .collect();
                Some(ImmutablePropertiesMap::new(
                    items.len(),
                    items.into_iter(),
                    &arena,
                ))
            } else {
                None
            }
        } else {
            None
        };

        let edge_id = v6_uuid();
        let version = version_info.get_latest(label);
        let edge = Edge {
            id: edge_id,
            label,
            version,
            properties,
            from_node,
            to_node,
        };

        // Write edge to edges_db
        buf.clear();
        bincode::serialize_into(&mut buf, &edge)
            .map_err(|e| GraphError::New(format!("Edge serialize: {e}")))?;
        db.edges_db.put(&mut txn, &edge_id, &buf)?;

        // Write to out_edges_db and in_edges_db
        let label_hash = hash_label(label, None);
        let out_key = HelixGraphStorage::out_edge_key(&from_node, &label_hash);
        db.out_edges_db.put(
            &mut txn,
            &out_key,
            &HelixGraphStorage::pack_edge_data(&edge_id, &to_node),
        )?;

        let in_key = HelixGraphStorage::in_edge_key(&to_node, &label_hash);
        db.in_edges_db.put(
            &mut txn,
            &in_key,
            &HelixGraphStorage::pack_edge_data(&edge_id, &from_node),
        )?;

        edges_created += 1;
    }

    // 7. Commit transaction
    txn.commit().map_err(GraphError::from)?;

    // 8. Build response
    let mut node_ids_map = sonic_rs::Object::with_capacity(temp_id_map.len());
    for (temp_id, real_id) in &temp_id_map {
        node_ids_map.insert(
            temp_id.as_str(),
            sonic_rs::Value::from(ID::from(*real_id).stringify().as_str()),
        );
    }

    let response_json = sonic_rs::json!({
        "node_ids": node_ids_map,
        "nodes_created": nodes_created,
        "edges_created": edges_created
    });

    let body = sonic_rs::to_vec(&response_json)
        .map_err(|e| GraphError::New(format!("Response serialize: {e}")))?;

    Ok(protocol::Response {
        body,
        fmt: Default::default(),
    })
}

#[derive(sonic_rs::Deserialize)]
struct BulkImportRequest {
    nodes: Vec<BulkNode>,
    #[serde(default)]
    edges: Vec<BulkEdge>,
}

#[derive(sonic_rs::Deserialize)]
struct BulkNode {
    label: String,
    temp_id: String,
    #[serde(default)]
    properties: Option<HashMap<String, sonic_rs::Value>>,
}

#[derive(sonic_rs::Deserialize)]
struct BulkEdge {
    label: String,
    from: String,
    to: String,
    #[serde(default)]
    properties: Option<HashMap<String, sonic_rs::Value>>,
}

/// Resolves a node reference string: tries UUID parse first, then temp_id lookup.
fn resolve_node_ref(
    reference: &str,
    temp_id_map: &HashMap<String, u128>,
) -> Result<u128, GraphError> {
    // Try UUID parse first
    if let Ok(uuid) = uuid::Uuid::parse_str(reference) {
        return Ok(uuid.as_u128());
    }
    // Then try temp_id lookup
    temp_id_map
        .get(reference)
        .copied()
        .ok_or_else(|| GraphError::New(format!("Unknown node reference: '{reference}'")))
}

inventory::submit! {
    HandlerSubmission(
        Handler::new("bulk_import", bulk_import_inner, true)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        helix_engine::{
            storage_core::{storage_methods::StorageMethods, version_info::VersionInfo},
            traversal_core::{
                HelixGraphEngine, HelixGraphEngineOpts,
                config::Config,
                ops::{
                    g::G,
                    source::add_n::AddNAdapter,
                },
            },
        },
        protocol::{Format, request::Request, request::RequestType},
    };
    use axum::body::Bytes;
    use sonic_rs::JsonContainerTrait;
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

    fn make_request(body: &[u8]) -> Request {
        Request {
            name: "bulk_import".to_string(),
            req_type: RequestType::Query,
            api_key: None,
            body: Bytes::copy_from_slice(body),
            in_fmt: Format::Json,
            out_fmt: Format::Json,
        }
    }

    #[test]
    fn test_bulk_import_nodes_only() {
        let (engine, _temp_dir) = setup_test_engine();

        let body = sonic_rs::json!({
            "nodes": [
                { "label": "Person", "temp_id": "n0", "properties": { "name": "Alice", "age": 30 } },
                { "label": "Person", "temp_id": "n1", "properties": { "name": "Bob", "age": 25 } },
                { "label": "Company", "temp_id": "n2", "properties": { "name": "Acme" } }
            ]
        });
        let body_bytes = sonic_rs::to_vec(&body).unwrap();

        let input = HandlerInput {
            graph: Arc::new(engine),
            request: make_request(&body_bytes),
        };

        let result = bulk_import_inner(input);
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());

        let response = result.unwrap();
        let resp: sonic_rs::Value = sonic_rs::from_slice(&response.body).unwrap();

        assert_eq!(resp.get("nodes_created").unwrap().as_u64().unwrap(), 3);
        assert_eq!(resp.get("edges_created").unwrap().as_u64().unwrap(), 0);

        let node_ids = resp.get("node_ids").unwrap().as_object().unwrap();
        assert!(node_ids.get(&"n0".to_string()).is_some());
        assert!(node_ids.get(&"n1".to_string()).is_some());
        assert!(node_ids.get(&"n2".to_string()).is_some());

        // Verify each returned ID is a valid UUID
        for (_k, v) in node_ids.iter() {
            let uuid_str = v.as_str().unwrap();
            assert!(uuid::Uuid::parse_str(uuid_str).is_ok());
        }
    }

    #[test]
    fn test_bulk_import_nodes_and_edges() {
        let (engine, _temp_dir) = setup_test_engine();

        let body = sonic_rs::json!({
            "nodes": [
                { "label": "Person", "temp_id": "n0", "properties": { "name": "Alice" } },
                { "label": "Person", "temp_id": "n1", "properties": { "name": "Bob" } }
            ],
            "edges": [
                { "label": "Knows", "from": "n0", "to": "n1", "properties": { "since": "2024" } }
            ]
        });
        let body_bytes = sonic_rs::to_vec(&body).unwrap();

        let input = HandlerInput {
            graph: Arc::new(engine),
            request: make_request(&body_bytes),
        };

        let result = bulk_import_inner(input);
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());

        let response = result.unwrap();
        let resp: sonic_rs::Value = sonic_rs::from_slice(&response.body).unwrap();

        assert_eq!(resp.get("nodes_created").unwrap().as_u64().unwrap(), 2);
        assert_eq!(resp.get("edges_created").unwrap().as_u64().unwrap(), 1);
    }

    #[test]
    fn test_bulk_import_edge_to_existing_node() {
        let (engine, _temp_dir) = setup_test_engine();

        // Pre-insert a node via the traversal API
        let arena = bumpalo::Bump::new();
        let mut txn = engine.storage.graph_env.write_txn().unwrap();

        let props = [("name", Value::String("Existing".to_string()))];
        let props_map = ImmutablePropertiesMap::new(
            props.len(),
            props
                .iter()
                .map(|(k, v)| (arena.alloc_str(k) as &str, v.clone())),
            &arena,
        );

        let existing_node = G::new_mut(&engine.storage, &arena, &mut txn)
            .add_n(arena.alloc_str("Person"), Some(props_map), None)
            .collect_to_obj()
            .unwrap();

        let existing_uuid_str = ID::from(existing_node.id()).stringify();
        txn.commit().unwrap();

        // Now bulk import a new node and an edge pointing to the existing node
        let body = sonic_rs::json!({
            "nodes": [
                { "label": "Person", "temp_id": "n0", "properties": { "name": "NewPerson" } }
            ],
            "edges": [
                { "label": "Knows", "from": "n0", "to": existing_uuid_str }
            ]
        });
        let body_bytes = sonic_rs::to_vec(&body).unwrap();

        let input = HandlerInput {
            graph: Arc::new(engine),
            request: make_request(&body_bytes),
        };

        let result = bulk_import_inner(input);
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());

        let response = result.unwrap();
        let resp: sonic_rs::Value = sonic_rs::from_slice(&response.body).unwrap();

        assert_eq!(resp.get("nodes_created").unwrap().as_u64().unwrap(), 1);
        assert_eq!(resp.get("edges_created").unwrap().as_u64().unwrap(), 1);
    }

    #[test]
    fn test_bulk_import_empty() {
        let (engine, _temp_dir) = setup_test_engine();

        let body = sonic_rs::json!({
            "nodes": [],
            "edges": []
        });
        let body_bytes = sonic_rs::to_vec(&body).unwrap();

        let input = HandlerInput {
            graph: Arc::new(engine),
            request: make_request(&body_bytes),
        };

        let result = bulk_import_inner(input);
        assert!(result.is_ok());

        let response = result.unwrap();
        let resp: sonic_rs::Value = sonic_rs::from_slice(&response.body).unwrap();

        assert_eq!(resp.get("nodes_created").unwrap().as_u64().unwrap(), 0);
        assert_eq!(resp.get("edges_created").unwrap().as_u64().unwrap(), 0);
    }

    #[test]
    fn test_bulk_import_invalid_json() {
        let (engine, _temp_dir) = setup_test_engine();

        let input = HandlerInput {
            graph: Arc::new(engine),
            request: make_request(b"not valid json{{{"),
        };

        let result = bulk_import_inner(input);
        assert!(result.is_err());
    }

    #[test]
    fn test_bulk_import_unknown_temp_id() {
        let (engine, _temp_dir) = setup_test_engine();

        let body = sonic_rs::json!({
            "nodes": [
                { "label": "Person", "temp_id": "n0", "properties": { "name": "Alice" } }
            ],
            "edges": [
                { "label": "Knows", "from": "n0", "to": "nonexistent" }
            ]
        });
        let body_bytes = sonic_rs::to_vec(&body).unwrap();

        let input = HandlerInput {
            graph: Arc::new(engine),
            request: make_request(&body_bytes),
        };

        let result = bulk_import_inner(input);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.err().unwrap());
        assert!(
            err_msg.contains("nonexistent"),
            "Error should mention the unknown temp_id"
        );
    }

    #[test]
    fn test_bulk_import_read_back_nodes() {
        let (engine, _temp_dir) = setup_test_engine();

        let body = sonic_rs::json!({
            "nodes": [
                { "label": "Person", "temp_id": "n0", "properties": { "name": "Alice", "age": 30 } },
                { "label": "Person", "temp_id": "n1", "properties": { "name": "Bob" } }
            ]
        });
        let body_bytes = sonic_rs::to_vec(&body).unwrap();

        let input = HandlerInput {
            graph: Arc::clone(&Arc::new(engine)),
            request: make_request(&body_bytes),
        };

        let db = Arc::clone(&input.graph.storage);
        let result = bulk_import_inner(input).unwrap();
        let resp: sonic_rs::Value = sonic_rs::from_slice(&result.body).unwrap();
        let node_ids = resp.get("node_ids").unwrap().as_object().unwrap();

        // Read each node back from the database and verify properties
        let arena = bumpalo::Bump::new();
        let txn = db.graph_env.read_txn().unwrap();

        for (temp_id, uuid_val) in node_ids.iter() {
            let uuid_str = uuid_val.as_str().unwrap();
            let uuid = uuid::Uuid::parse_str(uuid_str).unwrap();
            let node = db.get_node(&txn, &uuid.as_u128(), &arena).unwrap();

            assert_eq!(node.label, "Person");

            let props = node.properties.as_ref().expect("Node should have properties");
            if temp_id == "n0" {
                assert_eq!(
                    props.get("name"),
                    Some(&Value::String("Alice".to_string()))
                );
                assert_eq!(props.get("age"), Some(&Value::I64(30)));
            } else if temp_id == "n1" {
                assert_eq!(
                    props.get("name"),
                    Some(&Value::String("Bob".to_string()))
                );
            }
        }
    }

    #[test]
    fn test_bulk_import_duplicate_temp_id() {
        let (engine, _temp_dir) = setup_test_engine();

        let body = sonic_rs::json!({
            "nodes": [
                { "label": "Person", "temp_id": "n0", "properties": { "name": "Alice" } },
                { "label": "Person", "temp_id": "n0", "properties": { "name": "Bob" } }
            ]
        });
        let body_bytes = sonic_rs::to_vec(&body).unwrap();

        let input = HandlerInput {
            graph: Arc::new(engine),
            request: make_request(&body_bytes),
        };

        let result = bulk_import_inner(input);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.err().unwrap());
        assert!(
            err_msg.contains("Duplicate temp_id"),
            "Error should mention duplicate temp_id, got: {err_msg}"
        );
    }

    #[test]
    fn test_bulk_import_missing_nodes_key() {
        let (engine, _temp_dir) = setup_test_engine();

        let body = sonic_rs::json!({
            "edges": [
                { "label": "Knows", "from": "n0", "to": "n1" }
            ]
        });
        let body_bytes = sonic_rs::to_vec(&body).unwrap();

        let input = HandlerInput {
            graph: Arc::new(engine),
            request: make_request(&body_bytes),
        };

        let result = bulk_import_inner(input);
        assert!(result.is_err(), "Should fail when 'nodes' key is missing");
    }

    #[test]
    fn test_bulk_import_missing_label() {
        let (engine, _temp_dir) = setup_test_engine();

        let body = sonic_rs::json!({
            "nodes": [
                { "temp_id": "n0", "properties": { "name": "Alice" } }
            ]
        });
        let body_bytes = sonic_rs::to_vec(&body).unwrap();

        let input = HandlerInput {
            graph: Arc::new(engine),
            request: make_request(&body_bytes),
        };

        let result = bulk_import_inner(input);
        assert!(result.is_err(), "Should fail when node is missing 'label' field");
    }
}
