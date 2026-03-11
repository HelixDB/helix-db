use std::sync::Arc;

use bumpalo::Bump;
use tempfile::TempDir;

use super::test_utils::props_option;
use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::{
                g::G,
                source::{
                    add_n::AddNAdapter, n_from_id::NFromIdAdapter, n_from_index::NFromIndexAdapter,
                },
                util::{drop::Drop, update::UpdateAdapter, upsert::UpsertAdapter},
            },
            traversal_value::TraversalValue,
        },
        types::{GraphError, SecondaryIndex},
    },
    props,
    protocol::value::Value,
};

fn setup_indexed_db() -> (TempDir, Arc<HelixGraphStorage>) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let mut config = crate::helix_engine::traversal_core::config::Config::default();
    config.graph_config.as_mut().unwrap().secondary_indices =
        Some(vec![SecondaryIndex::Index("name".to_string())]);
    let storage = HelixGraphStorage::new(db_path, config, Default::default()).unwrap();
    (temp_dir, Arc::new(storage))
}

fn setup_unique_indexed_db() -> (TempDir, Arc<HelixGraphStorage>) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let mut config = crate::helix_engine::traversal_core::config::Config::default();
    config.graph_config.as_mut().unwrap().secondary_indices =
        Some(vec![SecondaryIndex::Unique("name".to_string())]);
    let storage = HelixGraphStorage::new(db_path, config, Default::default()).unwrap();
    (temp_dir, Arc::new(storage))
}

fn to_result_iter(
    values: Vec<TraversalValue>,
) -> impl Iterator<Item = Result<TraversalValue, GraphError>> {
    values.into_iter().map(Ok)
}

#[test]
fn test_delete_node_with_secondary_index() {
    let (_temp_dir, storage) = setup_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();
    let node_id = node.id();

    G::new_mut_from_iter(&storage, &mut txn, std::iter::once(node), &arena)
        .update(&[("name", Value::from("Jane"))])
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let jane_nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"Jane".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(jane_nodes.len(), 1);
    assert_eq!(jane_nodes[0].id(), node_id);

    let john_nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"John".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(john_nodes.is_empty());
    drop(txn);

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_id(&node_id)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::drop_traversal(to_result_iter(traversal), storage.as_ref(), &mut txn).unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let node = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"Jane".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(node.is_empty());
}

#[test]
fn test_update_of_secondary_indices() {
    let (_temp_dir, storage) = setup_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();
    G::new_mut_from_iter(&storage, &mut txn, std::iter::once(node), &arena)
        .update(&[("name", Value::from("Jane"))])
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"Jane".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(nodes.len(), 1);
    if let TraversalValue::Node(node) = &nodes[0] {
        match node.properties.as_ref().unwrap().get("name").unwrap() {
            Value::String(name) => assert_eq!(name, "Jane"),
            other => panic!("unexpected value: {other:?}"),
        }
    } else {
        panic!("expected node");
    }

    let john_nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"John".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(john_nodes.is_empty());
}

#[test]
fn test_unique_index_rejects_duplicate() {
    let (_temp_dir, storage) = setup_unique_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // First insert should succeed
    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();
    assert!(matches!(node, TraversalValue::Node(_)));

    // Second insert with same value should fail with DuplicateKey
    let result = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj();
    assert!(
        matches!(result, Err(GraphError::DuplicateKey(_))),
        "Expected DuplicateKey error, got: {result:?}"
    );
    txn.commit().unwrap();

    // Verify only one node exists in the index
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"John".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        nodes.len(),
        1,
        "Expected exactly one node, but found {}",
        nodes.len()
    );
}

#[test]
fn test_unique_index_allows_different_values() {
    let (_temp_dir, storage) = setup_unique_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();

    // Different value should succeed
    G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "Jane" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let john = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"John".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(john.len(), 1);

    let jane = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"Jane".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(jane.len(), 1);
}

// ============================================================================
// Unique Index: Update Tests
// ============================================================================

#[test]
fn test_unique_index_update_rejects_duplicate() {
    let (_temp_dir, storage) = setup_unique_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create "John"
    G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();

    // Create "Jane"
    let jane = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "Jane" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();
    let jane_id = jane.id();

    // Update "Jane" to "John" → should fail with DuplicateKey
    let result = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(jane), &arena)
        .update(&[("name", Value::from("John"))])
        .collect_to_obj();
    assert!(
        matches!(result, Err(GraphError::DuplicateKey(_))),
        "Expected DuplicateKey error, got: {result:?}"
    );
    txn.commit().unwrap();

    // Verify both nodes unchanged
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let john_nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"John".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(john_nodes.len(), 1);
    assert_ne!(john_nodes[0].id(), jane_id);

    let jane_nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"Jane".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(jane_nodes.len(), 1);
    assert_eq!(jane_nodes[0].id(), jane_id);
}

#[test]
fn test_unique_index_update_allows_same_value() {
    let (_temp_dir, storage) = setup_unique_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();
    let node_id = node.id();

    // Update same node's name to "John" (same value) → should succeed
    G::new_mut_from_iter(&storage, &mut txn, std::iter::once(node), &arena)
        .update(&[("name", Value::from("John"))])
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    // Verify node still exists in index
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"John".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), node_id);
}

#[test]
fn test_unique_index_update_allows_different_value() {
    let (_temp_dir, storage) = setup_unique_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();
    let node_id = node.id();

    // Update "John" to "Jane" → should succeed
    G::new_mut_from_iter(&storage, &mut txn, std::iter::once(node), &arena)
        .update(&[("name", Value::from("Jane"))])
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    // Verify index updated correctly
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let jane_nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"Jane".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(jane_nodes.len(), 1);
    assert_eq!(jane_nodes[0].id(), node_id);

    let john_nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"John".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(john_nodes.is_empty());
}

// ============================================================================
// Unique Index: Upsert Create Path Tests
// ============================================================================

#[test]
fn test_unique_index_upsert_create_rejects_duplicate() {
    let (_temp_dir, storage) = setup_unique_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create node "John" via add_n
    G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();

    // Upsert_n with empty iter (create path) with name "John" → should fail
    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n("person", &[("name", Value::from("John"))])
    .collect::<Result<Vec<_>, _>>();
    assert!(
        matches!(result, Err(GraphError::DuplicateKey(_))),
        "Expected DuplicateKey error, got: {result:?}"
    );
    txn.commit().unwrap();

    // Verify only one "John" exists
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"John".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(nodes.len(), 1);
}

#[test]
fn test_unique_index_upsert_create_allows_different_value() {
    let (_temp_dir, storage) = setup_unique_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create node "John" via add_n
    G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();

    // Upsert_n create path with name "Jane" → should succeed
    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n("person", &[("name", Value::from("Jane"))])
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    assert_eq!(result.len(), 1);
    txn.commit().unwrap();

    // Verify both exist in index
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let john = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"John".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(john.len(), 1);

    let jane = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"Jane".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(jane.len(), 1);
}

// ============================================================================
// Unique Index: Upsert Update Path Tests
// ============================================================================

#[test]
fn test_unique_index_upsert_update_rejects_duplicate() {
    let (_temp_dir, storage) = setup_unique_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create "John"
    G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();

    // Create "Jane"
    let jane = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "Jane" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();
    let jane_id = jane.id();

    // Upsert_n with Jane node, setting name to "John" → should fail
    let result = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(jane), &arena)
        .upsert_n("person", &[("name", Value::from("John"))])
        .collect::<Result<Vec<_>, _>>();
    assert!(
        matches!(result, Err(GraphError::DuplicateKey(_))),
        "Expected DuplicateKey error, got: {result:?}"
    );
    txn.commit().unwrap();

    // Verify both nodes unchanged
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let john_nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"John".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(john_nodes.len(), 1);
    assert_ne!(john_nodes[0].id(), jane_id);

    let jane_nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"Jane".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(jane_nodes.len(), 1);
    assert_eq!(jane_nodes[0].id(), jane_id);
}

#[test]
fn test_unique_index_upsert_update_allows_same_value() {
    let (_temp_dir, storage) = setup_unique_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create node "John"
    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();
    let node_id = node.id();

    // Upsert_n with same node, keeping name "John" → should succeed
    G::new_mut_from_iter(&storage, &mut txn, std::iter::once(node), &arena)
        .upsert_n("person", &[("name", Value::from("John"))])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    txn.commit().unwrap();

    // Verify node still in index
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"John".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), node_id);
}

// ============================================================================
// Unique Index: Consistency / No Partial State Tests
// ============================================================================

#[test]
fn test_unique_index_add_n_no_partial_state_on_failure() {
    let (_temp_dir, storage) = setup_unique_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create node "John"
    G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    // Try to add another "John" → fails
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let result = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj();
    assert!(matches!(result, Err(GraphError::DuplicateKey(_))));
    txn.abort();

    // Verify index is clean: still exactly one "John", no extra nodes
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"John".to_string())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        nodes.len(),
        1,
        "Expected exactly one John node after failed add"
    );
}
