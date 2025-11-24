use std::sync::Arc;

use bumpalo::Bump;
use tempfile::TempDir;

use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        tests::traversal_tests::test_utils::props_option,
        traversal_core::{
            ops::{
                g::G,
                in_::in_e::InEdgesAdapter,
                out::{out::OutAdapter, out_e::OutEdgesAdapter},
                source::{
                    add_e::AddEAdapter, add_n::AddNAdapter, e_from_id::EFromIdAdapter,
                    e_from_type::EFromTypeAdapter, n_from_id::NFromIdAdapter,
                },
                util::drop::Drop,
                vectors::insert::InsertVAdapter,
            },
            traversal_value::TraversalValue,
        },
        types::GraphError,
    },
    props,
    protocol::value::Value,
};

fn setup_test_db() -> (TempDir, Arc<HelixGraphStorage>) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let storage = HelixGraphStorage::new(
        db_path,
        crate::helix_engine::traversal_core::config::Config::default(),
        Default::default(),
    )
    .unwrap();
    (temp_dir, Arc::new(storage))
}

fn edge_id(value: &TraversalValue) -> u128 {
    match value {
        TraversalValue::Edge(edge) => edge.id,
        _ => panic!("expected edge"),
    }
}

#[test]
fn test_add_edge_creates_relationship() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let source_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let target_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let edge = G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, source_id, target_id, false)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let fetched = G::new(&storage, &txn, &arena)
        .e_from_id(&edge.id())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(fetched.len(), 1);
    assert_eq!(edge_id(&fetched[0]), edge.id());
}

#[test]
fn test_out_e_returns_edge() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let source_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let target_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, source_id, target_id, false)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(&storage, &txn, &arena)
        .n_from_id(&source_id)
        .out_e("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id(), edge_id(&edges[0]));
}

#[test]
fn test_in_e_returns_edge() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let source_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let target_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, source_id, target_id, false)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(&storage, &txn, &arena)
        .n_from_id(&target_id)
        .in_e("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edge_id(&edges[0]), edges[0].id());
}

#[test]
fn test_out_node_returns_neighbor() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let source_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let neighbor_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, source_id, neighbor_id, false)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let neighbors = G::new(&storage, &txn, &arena)
        .n_from_id(&source_id)
        .out_node("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0].id(), neighbor_id);
}

#[test]
fn test_edge_properties_can_be_read() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let source_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let target_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "knows",
            props_option(&arena, props! { "since" => 2024 }),
            source_id,
            target_id,
            false,
        )
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let edge = G::new(&storage, &txn, &arena)
        .e_from_type("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(edge.len(), 1);
    if let TraversalValue::Edge(edge) = &edge[0] {
        match edge.properties.as_ref().unwrap().get("since").unwrap() {
            Value::I64(year) => assert_eq!(*year, 2024),
            Value::I32(year) => assert_eq!(*year, 2024),
            other => panic!("unexpected value {other:?}"),
        }
    } else {
        panic!("expected edge");
    }
}

#[test]
fn test_vector_edges_roundtrip() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("doc", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let vector_id = match G::new_mut(&storage, &arena, &mut txn)
        .insert_v(&[1.0, 0.0, 0.0], "embedding", None)
        .collect_to_obj()
        .unwrap()
    {
        TraversalValue::Vector(vector) => vector.id,
        other => panic!("unexpected traversal value: {other:?}"),
    };
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("has_vector", None, node_id, vector_id, false)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let vectors = G::new(&storage, &txn, &arena)
        .n_from_id(&node_id)
        .out_vec("has_vector", true)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(vectors.len(), 1);
    match &vectors[0] {
        TraversalValue::Vector(vec) => assert_eq!(vec.id, vector_id),
        other => panic!("unexpected traversal value: {other:?}"),
    }
}

// ============================================================================
// Error Tests for e_from_id
// ============================================================================

#[test]
fn test_e_from_id_with_nonexistent_id() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // Generate a random ID that doesn't exist
    let fake_id = uuid::Uuid::new_v4().as_u128();

    // Attempt to query
    let result = G::new(&storage, &txn, &arena)
        .e_from_id(&fake_id)
        .collect_to_obj();

    // Assert it returns EdgeNotFound error
    assert!(matches!(result, Err(GraphError::EdgeNotFound)));
}

#[test]
fn test_e_from_id_with_deleted_edge() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create two nodes and an edge between them
    let source_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let target_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let edge = G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, source_id, target_id, false)
        .collect_to_obj()
        .unwrap();
    let edge_id = edge.id();

    txn.commit().unwrap();

    // Delete the edge
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let edge_to_delete = G::new(&storage, &txn, &arena)
        .e_from_id(&edge_id)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::drop_traversal(
        edge_to_delete.into_iter().map(Ok::<_, GraphError>),
        storage.as_ref(),
        &mut txn,
    )
    .unwrap();
    txn.commit().unwrap();

    // Try to query the deleted edge
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let result = G::new(&storage, &txn, &arena)
        .e_from_id(&edge_id)
        .collect_to_obj();

    // Assert it returns EdgeNotFound error
    assert!(matches!(result, Err(GraphError::EdgeNotFound)));
}

#[test]
fn test_e_from_id_with_zero_id() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // Query with ID = 0
    let result = G::new(&storage, &txn, &arena)
        .e_from_id(&0)
        .collect_to_obj();

    // Assert it returns EdgeNotFound error
    assert!(matches!(result, Err(GraphError::EdgeNotFound)));
}

#[test]
fn test_e_from_id_with_max_id() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // Query with ID = u128::MAX
    let result = G::new(&storage, &txn, &arena)
        .e_from_id(&u128::MAX)
        .collect_to_obj();

    // Assert it returns EdgeNotFound error
    assert!(matches!(result, Err(GraphError::EdgeNotFound)));
}
