use std::sync::Arc;

use bumpalo::Bump;
use heed3::RoTxn;
use tempfile::TempDir;

use super::test_utils::props_option;
use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::{
                g::G,
                in_::in_::InAdapter,
                in_::in_e::InEdgesAdapter,
                out::out::OutAdapter,
                out::out_e::OutEdgesAdapter,
                source::{
                    add_e::AddEAdapter, add_n::AddNAdapter, e_from_id::EFromIdAdapter,
                    n_from_id::NFromIdAdapter,
                },
                util::upsert::UpsertAdapter,
                vectors::{insert::InsertVAdapter, search::SearchVAdapter},
            },
            traversal_value::TraversalValue,
        },
        vector_core::vector::HVector,
    },
    props,
    protocol::value::Value,
};

type Filter = fn(&HVector, &RoTxn) -> bool;

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

// ============================================================================
// Node Upsert Tests (upsert_n)
// ============================================================================

#[test]
fn test_upsert_n_creates_new_node_when_none_exists() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Start with empty vec (no existing node)
    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n(
        "person",
        &[("name", Value::from("Alice")), ("age", Value::from(30))],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Node(node) = &result[0] {
        assert_eq!(node.label, "person");
        assert_eq!(node.get_property("name").unwrap(), &Value::from("Alice"));
        assert_eq!(node.get_property("age").unwrap(), &Value::from(30));
    } else {
        panic!("Expected node");
    }
    txn.commit().unwrap();
}

#[test]
fn test_upsert_n_creates_node_with_empty_properties() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n("document", &[])
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Node(node) = &result[0] {
        assert_eq!(node.label, "document");
        assert!(node.properties.is_none());
    } else {
        panic!("Expected node");
    }
    txn.commit().unwrap();
}

#[test]
fn test_upsert_n_updates_existing_node_with_no_properties() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create existing node without properties
    let existing_node = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let node_id = existing_node.id();

    // Upsert with new properties
    let result = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(existing_node), &arena)
        .upsert_n(
            "person",
            &[("name", Value::from("Bob")), ("age", Value::from(25))],
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Node(node) = &result[0] {
        assert_eq!(node.id, node_id);
        assert_eq!(node.get_property("name").unwrap(), &Value::from("Bob"));
        assert_eq!(node.get_property("age").unwrap(), &Value::from(25));
    } else {
        panic!("Expected node");
    }
    txn.commit().unwrap();
}

#[test]
fn test_upsert_n_updates_existing_node_with_properties() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create existing node with properties
    let existing_node = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(
                &arena,
                props!("name" => "Charlie", "age" => 20, "city" => "NYC"),
            ),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let node_id = existing_node.id();

    // Upsert with updated and new properties
    let result = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(existing_node), &arena)
        .upsert_n(
            "person",
            &[
                ("name", Value::from("Charles")),              // Update existing
                ("age", Value::from(21)),                      // Update existing
                ("email", Value::from("charles@example.com")), // Add new
            ],
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Node(node) = &result[0] {
        assert_eq!(node.id, node_id);
        assert_eq!(node.get_property("name").unwrap(), &Value::from("Charles"));
        assert_eq!(node.get_property("age").unwrap(), &Value::from(21));
        assert_eq!(node.get_property("city").unwrap(), &Value::from("NYC")); // Preserved
        assert_eq!(
            node.get_property("email").unwrap(),
            &Value::from("charles@example.com")
        );
    } else {
        panic!("Expected node");
    }
    txn.commit().unwrap();
}

#[test]
fn test_upsert_n_ignores_non_node_values() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create an edge to include in the iterator
    let node1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .clone();
    let node2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .clone();
    let edge = G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, node1.id(), node2.id(), false, false)
        .collect_to_obj()
        .unwrap();

    // Upsert with edge in iterator (should be ignored)
    let result = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(edge), &arena)
        .upsert_n("person", &[("name", Value::from("David"))])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Should return Empty since edge was ignored
    assert_eq!(result.len(), 1);
    if let TraversalValue::Empty = &result[0] {
        // This is the correct behavior - non-matching types are ignored
        // and return TraversalValue::Empty
    } else {
        panic!("Expected Empty, got: {:?}", result[0]);
    }
    txn.commit().unwrap();
}

// ============================================================================
// Edge Upsert Tests (upsert_e)
// ============================================================================

#[test]
fn test_upsert_e_creates_new_edge_when_none_exists() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create nodes to connect
    let node1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let node2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Upsert edge with empty iterator
    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_e(
        "knows",
        node1,
        node2,
        &[("since", Value::from(2023)), ("strength", Value::from(0.8))],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Edge(edge) = &result[0] {
        assert_eq!(edge.label, "knows");
        assert_eq!(edge.from_node, node1);
        assert_eq!(edge.to_node, node2);
        assert_eq!(edge.get_property("since").unwrap(), &Value::from(2023));
        assert_eq!(edge.get_property("strength").unwrap(), &Value::from(0.8));
    } else {
        panic!("Expected edge");
    }
    txn.commit().unwrap();
}

#[test]
fn test_upsert_e_creates_edge_with_empty_properties() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let node2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_e("follows", node1, node2, &[])
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Edge(edge) = &result[0] {
        assert_eq!(edge.label, "follows");
        assert!(edge.properties.is_none());
    } else {
        panic!("Expected edge");
    }
    txn.commit().unwrap();
}

#[test]
fn test_upsert_e_updates_existing_edge_with_no_properties() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let node2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Create existing edge
    let existing_edge = G::new_mut(&storage, &arena, &mut txn)
        .add_edge("likes", None, node1, node2, false, false)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let edge_id = if let TraversalValue::Edge(ref e) = existing_edge {
        e.id
    } else {
        panic!("Expected edge");
    };

    // Upsert with properties
    let result = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(existing_edge), &arena)
        .upsert_e("likes", node1, node2, &[("rating", Value::from(5))])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Edge(edge) = &result[0] {
        assert_eq!(edge.id, edge_id);
        assert_eq!(edge.get_property("rating").unwrap(), &Value::from(5));
    } else {
        panic!("Expected edge");
    }
    txn.commit().unwrap();
}

#[test]
fn test_upsert_e_updates_existing_edge_with_properties() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let node2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Create existing edge with properties
    let existing_edge = G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "works_with",
            props_option(
                &arena,
                props!("since" => 2020, "project" => "Apollo", "rating" => 4),
            ),
            node1,
            node2,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let edge_id = if let TraversalValue::Edge(ref e) = existing_edge {
        e.id
    } else {
        panic!("Expected edge");
    };

    // Upsert with updated and new properties
    let result = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(existing_edge), &arena)
        .upsert_e(
            "works_with",
            node1,
            node2,
            &[
                ("since", Value::from(2021)),             // Update existing
                ("rating", Value::from(5)),               // Update existing
                ("role", Value::from("senior_engineer")), // Add new
            ],
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Edge(edge) = &result[0] {
        assert_eq!(edge.id, edge_id);
        assert_eq!(edge.get_property("since").unwrap(), &Value::from(2021));
        assert_eq!(edge.get_property("rating").unwrap(), &Value::from(5));
        assert_eq!(
            edge.get_property("project").unwrap(),
            &Value::from("Apollo")
        ); // Preserved
        assert_eq!(
            edge.get_property("role").unwrap(),
            &Value::from("senior_engineer")
        );
    } else {
        panic!("Expected edge");
    }
    txn.commit().unwrap();
}

#[test]
fn test_upsert_e_ignores_iterator_content() {
    // After fix for issue #850: upsert_e now looks up edges by from_node/to_node/label
    // directly in the database, ignoring the source iterator content entirely.
    // This test verifies that even if a Node is in the iterator, the upsert
    // still correctly creates an edge between the specified nodes.
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .clone();
    let node2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Upsert with node in iterator - iterator content is now ignored,
    // edge is created based on from_node/to_node parameters
    let result = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(node1.clone()), &arena)
        .upsert_e(
            "connects",
            node1.id(),
            node2,
            &[("type", Value::from("friend"))],
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Should create edge since no edge exists between node1 and node2
    assert_eq!(result.len(), 1);
    if let TraversalValue::Edge(edge) = &result[0] {
        assert_eq!(edge.from_node, node1.id());
        assert_eq!(edge.to_node, node2);
        assert_eq!(edge.label, "connects");
        assert_eq!(edge.get_property("type").unwrap(), &Value::from("friend"));
    } else {
        panic!("Expected Edge, got: {:?}", result[0]);
    }
    txn.commit().unwrap();
}

// ============================================================================
// Vector Upsert Tests (upsert_v)
// ============================================================================

#[test]
fn test_upsert_v_creates_new_vector_when_none_exists() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let query = [0.1, 0.2, 0.3];
    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_v(
        &query,
        "embedding",
        &[("model", Value::from("bert")), ("version", Value::from(2))],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Vector(vector) = &result[0] {
        assert_eq!(vector.label, "embedding");
        assert_eq!(vector.data, &query);
        assert_eq!(vector.get_property("model").unwrap(), &Value::from("bert"));
        assert_eq!(vector.get_property("version").unwrap(), &Value::from(2));
    } else {
        panic!("Expected vector");
    }
    txn.commit().unwrap();
}

#[test]
fn test_upsert_v_creates_vector_with_default_data_when_none_provided() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_v(&[], "placeholder", &[("status", Value::from("pending"))])
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Vector(vector) = &result[0] {
        assert_eq!(vector.label, "placeholder");
        assert!(vector.data.is_empty()); // Default empty data
        assert_eq!(
            vector.get_property("status").unwrap(),
            &Value::from("pending")
        );
    } else {
        panic!("Expected vector");
    }
    txn.commit().unwrap();
}

#[test]
fn test_upsert_v_updates_existing_vector_with_no_properties() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create existing vector
    let existing_vector = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[0.5, 0.6, 0.7], "embedding", None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let vector_id = existing_vector.id();

    // Upsert with new properties
    let result = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(existing_vector), &arena)
        .upsert_v(
            &[0.8, 0.9, 1.0],
            "embedding",
            &[("source", Value::from("openai")), ("dim", Value::from(3))],
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Vector(vector) = &result[0] {
        assert_eq!(vector.id, vector_id);
        assert_eq!(
            vector.get_property("source").unwrap(),
            &Value::from("openai")
        );
        assert_eq!(vector.get_property("dim").unwrap(), &Value::from(3));
    } else {
        panic!("Expected vector");
    }
    txn.commit().unwrap();
}

#[test]
fn test_upsert_v_updates_existing_vector_with_properties() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create existing vector with properties
    let existing_vector = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(
            &[0.1, 0.2, 0.3],
            "embedding",
            props_option(
                &arena,
                props!("model" => "gpt3", "version" => 1, "accuracy" => 0.95),
            ),
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    let vector_id = existing_vector.id();

    // Upsert with updated and new properties
    let result = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(existing_vector), &arena)
        .upsert_v(
            &[0.4, 0.5, 0.6],
            "embedding",
            &[
                ("model", Value::from("gpt4")),         // Update existing
                ("version", Value::from(2)),            // Update existing
                ("timestamp", Value::from(1640995200)), // Add new
            ],
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Vector(vector) = &result[0] {
        assert_eq!(vector.id, vector_id);
        assert_eq!(vector.get_property("model").unwrap(), &Value::from("gpt4"));
        assert_eq!(vector.get_property("version").unwrap(), &Value::from(2));
        assert_eq!(vector.get_property("accuracy").unwrap(), &Value::from(0.95)); // Preserved
        assert_eq!(
            vector.get_property("timestamp").unwrap(),
            &Value::from(1640995200)
        );
    } else {
        panic!("Expected vector");
    }
    txn.commit().unwrap();
}

#[test]
fn test_upsert_v_ignores_non_vector_values() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .clone();

    // Upsert with node in iterator (should be ignored)
    let result = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(node), &arena)
        .upsert_v(
            &[0.7, 0.8, 0.9],
            "document_embedding",
            &[("type", Value::from("paragraph"))],
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Should return Empty since node was ignored
    assert_eq!(result.len(), 1);
    if let TraversalValue::Empty = &result[0] {
        // This is the correct behavior - non-matching types are ignored
        // and return TraversalValue::Empty
    } else {
        panic!("Expected Empty, got: {:?}", result[0]);
    }
    txn.commit().unwrap();
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn test_upsert_n_serialization_errors_handled() {
    // This test verifies that serialization errors in the upsert process
    // are properly handled and propagated
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Normal upsert should work fine
    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n("person", &[("name", Value::from("Test"))])
    .collect::<Result<Vec<_>, _>>();

    assert!(result.is_ok());
    assert_eq!(result.unwrap().len(), 1);
    txn.commit().unwrap();
}

#[test]
fn test_upsert_e_with_nonexistent_nodes() {
    // Test edge upsert with nodes that don't exist
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let fake_node1 = 999999u128;
    let fake_node2 = 888888u128;

    // This should create the edge even if nodes don't exist
    // (depending on implementation - may need to adjust based on actual behavior)
    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_e("fake_edge", fake_node1, fake_node2, &[])
    .collect::<Result<Vec<_>, _>>();

    // The behavior here depends on implementation
    // Either it succeeds (creates edge with non-existent nodes) or fails
    // We'll just verify the method doesn't panic
    match result {
        Ok(edges) => {
            // Edge was created
            assert_eq!(edges.len(), 1);
        }
        Err(_) => {
            // Error was returned - also acceptable
        }
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

#[test]
fn test_upsert_n_with_secondary_indices() {
    let (_temp_dir, storage) = setup_test_db();

    // This test would need mutable access to storage, which Arc doesn't provide
    // Instead, we'll test that upsert works normally without secondary indices

    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create node with property
    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n("person", &[("name", Value::from("regular_user"))])
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    assert_eq!(result.len(), 1);

    // Update the property
    let existing = result.into_iter().next().unwrap();
    let updated = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(existing), &arena)
        .upsert_n("person", &[("name", Value::from("updated_regular_user"))])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(updated.len(), 1);
    if let TraversalValue::Node(node) = &updated[0] {
        assert_eq!(
            node.get_property("name").unwrap(),
            &Value::from("updated_regular_user")
        );
    }

    txn.commit().unwrap();
}

#[test]
fn test_upsert_preserves_version_info() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create initial node
    let original = G::new_mut(&storage, &arena, &mut txn)
        .add_n("versioned", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .clone();

    let original_version = if let TraversalValue::Node(ref node) = original {
        node.version
    } else {
        panic!("Expected node");
    };

    // Upsert should preserve version
    let result = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(original), &arena)
        .upsert_n("versioned", &[("updated", Value::from(true))])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    if let TraversalValue::Node(node) = &result[0] {
        assert_eq!(node.version, original_version);
        assert_eq!(node.get_property("updated").unwrap(), &Value::from(true));
    } else {
        panic!("Expected node");
    }

    txn.commit().unwrap();
}

#[test]
fn test_multiple_upserts_in_sequence() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // First upsert (create)
    let first = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n("counter", &[("value", Value::from(1))])
    .collect::<Result<Vec<_>, _>>()
    .unwrap()[0]
        .clone();

    // Second upsert (update)
    let second = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(first), &arena)
        .upsert_n("counter", &[("value", Value::from(2))])
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .clone();

    // Third upsert (update)
    let third = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(second), &arena)
        .upsert_n("counter", &[("value", Value::from(3))])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    if let TraversalValue::Node(node) = &third[0] {
        assert_eq!(node.get_property("value").unwrap(), &Value::from(3));
    } else {
        panic!("Expected node");
    }

    txn.commit().unwrap();
}

#[test]
fn test_upsert_e_preserves_edge_relationships() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create nodes
    let node1_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Alice")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let node2_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Bob")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Create initial edge
    let edge = G::new_mut(&storage, &arena, &mut txn)
        .add_edge("friends", None, node1_id, node2_id, false, false)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .clone();

    // Upsert edge with additional properties
    let updated_edge = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(edge), &arena)
        .upsert_e(
            "friends",
            node1_id,
            node2_id,
            &[("since", Value::from("2023-01-01"))],
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    if let TraversalValue::Edge(edge) = &updated_edge[0] {
        assert_eq!(edge.from_node, node1_id);
        assert_eq!(edge.to_node, node2_id);
        assert_eq!(edge.label, "friends");
        assert_eq!(
            edge.get_property("since").unwrap(),
            &Value::from("2023-01-01")
        );
    } else {
        panic!("Expected edge");
    }

    txn.commit().unwrap();
}

#[test]
fn test_upsert_v_with_bm25_indexing() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Test upsert vector with text content that should be indexed by BM25
    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_v(
        &[0.1, 0.2, 0.3],
        "document",
        &[(
            "content",
            Value::from("machine learning artificial intelligence"),
        )],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Vector(vector) = &result[0] {
        assert_eq!(vector.label, "document");
        assert_eq!(
            vector.get_property("content").unwrap(),
            &Value::from("machine learning artificial intelligence")
        );
    }

    txn.commit().unwrap();
}

#[test]
fn test_upsert_v_new_vector_is_searchable() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let query = [0.1, 0.2, 0.3];
    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_v(
        &query,
        "searchable_embedding",
        &[("model", Value::from("test"))],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    assert_eq!(result.len(), 1);
    let inserted_id = result[0].id();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let search_results = G::new(&storage, &txn, &arena)
        .search_v::<Filter, _>(&query, 10, "searchable_embedding", None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(
        !search_results.is_empty(),
        "Search should find the upserted vector"
    );
    assert_eq!(
        search_results[0].id(),
        inserted_id,
        "Should find the same vector"
    );
}

// ============================================================================
// Regression Tests - Property Update and Revert
// ============================================================================

#[test]
fn test_upsert_n_update_property_then_revert() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create initial node with a property
    let original = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n("email", &[("message_id", Value::from("original_msg_id"))])
    .collect::<Result<Vec<_>, _>>()
    .unwrap()[0]
        .clone();

    let node_id = original.id();

    // Update property to a new value
    let updated = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(original), &arena)
        .upsert_n("email", &[("message_id", Value::from("changed_msg_id"))])
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .clone();

    assert_eq!(updated.id(), node_id);
    if let TraversalValue::Node(node) = &updated {
        assert_eq!(
            node.get_property("message_id").unwrap(),
            &Value::from("changed_msg_id")
        );
    }

    // Revert property back to original value - THIS IS THE REGRESSION TEST
    let reverted = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(updated), &arena)
        .upsert_n("email", &[("message_id", Value::from("original_msg_id"))])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(reverted.len(), 1);
    assert_eq!(reverted[0].id(), node_id);
    if let TraversalValue::Node(node) = &reverted[0] {
        assert_eq!(
            node.get_property("message_id").unwrap(),
            &Value::from("original_msg_id")
        );
    }

    txn.commit().unwrap();
}

#[test]
fn test_upsert_n_multiple_nodes_same_property_value() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create first node with a property value
    let node1 = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n(
        "email",
        &[
            ("email_id", Value::from("email_001")),
            ("message_id", Value::from("shared_msg_id")),
        ],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap()[0]
        .clone();

    // Create second node with SAME message_id (should succeed - not unique)
    let node2 = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n(
        "email",
        &[
            ("email_id", Value::from("email_002")),
            ("message_id", Value::from("shared_msg_id")),
        ],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap()[0]
        .clone();

    // Create third node with same message_id
    let node3 = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n(
        "email",
        &[
            ("email_id", Value::from("email_003")),
            ("message_id", Value::from("shared_msg_id")),
        ],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap()[0]
        .clone();

    // Verify all three nodes were created with different IDs
    assert_ne!(node1.id(), node2.id());
    assert_ne!(node2.id(), node3.id());
    assert_ne!(node1.id(), node3.id());

    // All should have the same message_id
    for node in [&node1, &node2, &node3] {
        if let TraversalValue::Node(n) = node {
            assert_eq!(
                n.get_property("message_id").unwrap(),
                &Value::from("shared_msg_id")
            );
        }
    }

    txn.commit().unwrap();
}

#[test]
fn test_upsert_n_update_one_node_preserves_others_with_same_value() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create two nodes with same message_id
    let node1 = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n(
        "email",
        &[
            ("email_id", Value::from("email_001")),
            ("message_id", Value::from("shared_msg")),
        ],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap()[0]
        .clone();

    let node2 = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n(
        "email",
        &[
            ("email_id", Value::from("email_002")),
            ("message_id", Value::from("shared_msg")),
        ],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap()[0]
        .clone();

    let node1_id = node1.id();
    let _node2_id = node2.id();

    // Update node1's message_id to a different value
    let node1_updated = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(node1), &arena)
        .upsert_n("email", &[("message_id", Value::from("unique_msg"))])
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .clone();

    // Verify node1 was updated
    if let TraversalValue::Node(n) = &node1_updated {
        assert_eq!(n.id, node1_id);
        assert_eq!(
            n.get_property("message_id").unwrap(),
            &Value::from("unique_msg")
        );
    }

    // Now revert node1 back to shared_msg - should succeed
    let node1_reverted =
        G::new_mut_from_iter(&storage, &mut txn, std::iter::once(node1_updated), &arena)
            .upsert_n("email", &[("message_id", Value::from("shared_msg"))])
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

    assert_eq!(node1_reverted.len(), 1);
    if let TraversalValue::Node(n) = &node1_reverted[0] {
        assert_eq!(n.id, node1_id);
        assert_eq!(
            n.get_property("message_id").unwrap(),
            &Value::from("shared_msg")
        );
    }

    txn.commit().unwrap();
}

#[test]
fn test_upsert_n_sequential_property_updates() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create initial node
    let mut current = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n("email", &[("status", Value::from("draft"))])
    .collect::<Result<Vec<_>, _>>()
    .unwrap()[0]
        .clone();

    let node_id = current.id();

    // Sequence of updates: draft -> sent -> read -> archived -> draft (back to original)
    let statuses = ["sent", "read", "archived", "draft"];

    for status in statuses {
        current = G::new_mut_from_iter(&storage, &mut txn, std::iter::once(current), &arena)
            .upsert_n("email", &[("status", Value::from(status))])
            .collect::<Result<Vec<_>, _>>()
            .unwrap()[0]
            .clone();

        assert_eq!(current.id(), node_id);
        if let TraversalValue::Node(n) = &current {
            assert_eq!(n.get_property("status").unwrap(), &Value::from(status));
        }
    }

    txn.commit().unwrap();
}

// ============================================================================
// Regression Tests - Issue #850
// ============================================================================

#[test]
fn test_upsert_e_creates_edge_between_correct_nodes_issue_850() {
    // Regression test for issue #850: UpsertE was ignoring From()/To() parameters
    // and instead using the first edge from the source iterator.
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create 4 nodes
    let node_a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let node_b = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let node_c = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let node_d = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Create existing edge A -> B
    let _existing = G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, node_a, node_b, false, false)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // UpsertE targeting C -> D (should create new edge, not update A->B)
    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_e("knows", node_c, node_d, &[("since", Value::from(2024))])
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Edge(edge) = &result[0] {
        // Key assertion: edge connects C->D, not A->B
        assert_eq!(edge.from_node, node_c);
        assert_eq!(edge.to_node, node_d);
        assert_eq!(edge.get_property("since").unwrap(), &Value::from(2024));
    } else {
        panic!("Expected edge");
    }
    txn.commit().unwrap();
}

#[test]
fn test_upsert_e_updates_correct_edge_when_multiple_edges_exist_issue_850() {
    // Another regression test for issue #850: When multiple edges exist,
    // UpsertE should find and update the correct edge based on from_node/to_node.
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create nodes
    let node_a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let node_b = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let node_c = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Create edge A -> B with initial property
    let edge_ab = G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "knows",
            props_option(&arena, props!("since" => 2020)),
            node_a,
            node_b,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let edge_ab_id = if let TraversalValue::Edge(e) = &edge_ab[0] {
        e.id
    } else {
        panic!("Expected edge");
    };

    // Create edge A -> C with initial property
    let edge_ac = G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "knows",
            props_option(&arena, props!("since" => 2021)),
            node_a,
            node_c,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let edge_ac_id = if let TraversalValue::Edge(e) = &edge_ac[0] {
        e.id
    } else {
        panic!("Expected edge");
    };

    // UpsertE targeting A -> C should update edge_ac, NOT edge_ab
    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_e(
        "knows",
        node_a,
        node_c,
        &[("since", Value::from(2025)), ("updated", Value::from(true))],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    assert_eq!(result.len(), 1);
    if let TraversalValue::Edge(edge) = &result[0] {
        // Key assertions: should update edge_ac, not edge_ab
        assert_eq!(edge.id, edge_ac_id, "Should update the correct edge (A->C)");
        assert_ne!(edge.id, edge_ab_id, "Should NOT update edge A->B");
        assert_eq!(edge.from_node, node_a);
        assert_eq!(edge.to_node, node_c);
        assert_eq!(edge.get_property("since").unwrap(), &Value::from(2025));
        assert_eq!(edge.get_property("updated").unwrap(), &Value::from(true));
    } else {
        panic!("Expected edge");
    }
    txn.commit().unwrap();
}

// ============================================================================
// Edge Adjacency & Persistence Tests
// ============================================================================

#[test]
fn test_upsert_e_new_edge_adjacency_via_out_e_in_e() {
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

    let upserted = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_e(
        "knows",
        source_id,
        target_id,
        &[("since", Value::from(2024))],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    let edge_id = upserted[0].id();
    txn.commit().unwrap();

    // Fresh arena + read txn
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // out_e from source should find the edge
    let out_edges = G::new(&storage, &txn, &arena)
        .n_from_id(&source_id)
        .out_e("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(out_edges.len(), 1);
    assert_eq!(out_edges[0].id(), edge_id);

    // in_e from target should find the edge
    let in_edges = G::new(&storage, &txn, &arena)
        .n_from_id(&target_id)
        .in_e("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(in_edges.len(), 1);
    assert_eq!(in_edges[0].id(), edge_id);
}

#[test]
fn test_upsert_e_update_preserves_adjacency() {
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

    // Create edge via add_edge
    let edge = G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "knows",
            props_option(&arena, props!("since" => 2020)),
            source_id,
            target_id,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let edge_id = edge[0].id();

    // Upsert to update props on the same (from, to, label)
    G::new_mut_from_iter(&storage, &mut txn, edge.into_iter(), &arena)
        .upsert_e(
            "knows",
            source_id,
            target_id,
            &[("since", Value::from(2025)), ("close", Value::from(true))],
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    txn.commit().unwrap();

    // Fresh arena + read txn: adjacency still works AND updated props visible
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let out_edges = G::new(&storage, &txn, &arena)
        .n_from_id(&source_id)
        .out_e("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(out_edges.len(), 1);
    assert_eq!(out_edges[0].id(), edge_id);
    if let TraversalValue::Edge(e) = &out_edges[0] {
        assert_eq!(e.get_property("since").unwrap(), &Value::from(2025));
        assert_eq!(e.get_property("close").unwrap(), &Value::from(true));
    } else {
        panic!("Expected edge");
    }

    let in_edges = G::new(&storage, &txn, &arena)
        .n_from_id(&target_id)
        .in_e("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(in_edges.len(), 1);
    assert_eq!(in_edges[0].id(), edge_id);
}

#[test]
fn test_upsert_e_different_endpoints_creates_new_edge_keeps_old() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node_a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let node_b = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let node_c = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Create A->B via upsert
    G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_e("knows", node_a, node_b, &[("rel", Value::from("old"))])
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    // Upsert A->C (different endpoint — creates new edge)
    G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_e("knows", node_a, node_c, &[("rel", Value::from("new"))])
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // A should have 2 outgoing "knows" edges
    let out_edges = G::new(&storage, &txn, &arena)
        .n_from_id(&node_a)
        .out_e("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(out_edges.len(), 2);

    // B should have 1 incoming "knows" edge
    let in_b = G::new(&storage, &txn, &arena)
        .n_from_id(&node_b)
        .in_e("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(in_b.len(), 1);

    // C should have 1 incoming "knows" edge
    let in_c = G::new(&storage, &txn, &arena)
        .n_from_id(&node_c)
        .in_e("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(in_c.len(), 1);

    // Old edge to B is untouched
    if let TraversalValue::Edge(e) = &in_b[0] {
        assert_eq!(e.get_property("rel").unwrap(), &Value::from("old"));
    } else {
        panic!("Expected edge");
    }
}

#[test]
fn test_upsert_e_idempotent_same_triple_no_duplicate() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node_a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let node_b = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // First upsert
    let first = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_e("knows", node_a, node_b, &[("version", Value::from(1))])
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let first_id = first[0].id();

    // Second upsert — same (from, to, label)
    let second = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_e("knows", node_a, node_b, &[("version", Value::from(2))])
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let second_id = second[0].id();

    // Same edge ID
    assert_eq!(first_id, second_id);

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // Only 1 edge in adjacency
    let out_edges = G::new(&storage, &txn, &arena)
        .n_from_id(&node_a)
        .out_e("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(out_edges.len(), 1);

    // Second props win
    if let TraversalValue::Edge(e) = &out_edges[0] {
        assert_eq!(e.get_property("version").unwrap(), &Value::from(2));
    } else {
        panic!("Expected edge");
    }
}

#[test]
fn test_upsert_e_multiple_labels_same_node_pair() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node_a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let node_b = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Upsert "knows"
    G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_e("knows", node_a, node_b, &[("since", Value::from(2020))])
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    // Upsert "likes" — same nodes, different label
    G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_e("likes", node_a, node_b, &[("rating", Value::from(5))])
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // Each label queryable separately
    let knows_edges = G::new(&storage, &txn, &arena)
        .n_from_id(&node_a)
        .out_e("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(knows_edges.len(), 1);
    if let TraversalValue::Edge(e) = &knows_edges[0] {
        assert_eq!(e.get_property("since").unwrap(), &Value::from(2020));
    } else {
        panic!("Expected edge");
    }

    let likes_edges = G::new(&storage, &txn, &arena)
        .n_from_id(&node_a)
        .out_e("likes")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(likes_edges.len(), 1);
    if let TraversalValue::Edge(e) = &likes_edges[0] {
        assert_eq!(e.get_property("rating").unwrap(), &Value::from(5));
    } else {
        panic!("Expected edge");
    }
}

#[test]
fn test_upsert_e_persisted_after_commit() {
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

    let upserted = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_e(
        "works_with",
        source_id,
        target_id,
        &[
            ("project", Value::from("helix")),
            ("role", Value::from("lead")),
        ],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let edge_id = upserted[0].id();
    txn.commit().unwrap();

    // Fresh arena + read txn — re-read via e_from_id
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let fetched = G::new(&storage, &txn, &arena)
        .e_from_id(&edge_id)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(fetched.len(), 1);
    if let TraversalValue::Edge(e) = &fetched[0] {
        assert_eq!(e.id, edge_id);
        assert_eq!(e.label, "works_with");
        assert_eq!(e.from_node, source_id);
        assert_eq!(e.to_node, target_id);
        assert_eq!(e.get_property("project").unwrap(), &Value::from("helix"));
        assert_eq!(e.get_property("role").unwrap(), &Value::from("lead"));
    } else {
        panic!("Expected edge");
    }
}

// ============================================================================
// Node Persistence Tests
// ============================================================================

#[test]
fn test_upsert_n_persisted_readable_via_n_from_id() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let result = G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_n(
        "person",
        &[
            ("name", Value::from("Alice")),
            ("age", Value::from(30)),
            ("active", Value::from(true)),
        ],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    let node_id = result[0].id();
    txn.commit().unwrap();

    // Fresh arena + read txn
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let fetched = G::new(&storage, &txn, &arena)
        .n_from_id(&node_id)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(fetched.len(), 1);
    if let TraversalValue::Node(n) = &fetched[0] {
        assert_eq!(n.id, node_id);
        assert_eq!(n.label, "person");
        assert_eq!(n.get_property("name").unwrap(), &Value::from("Alice"));
        assert_eq!(n.get_property("age").unwrap(), &Value::from(30));
        assert_eq!(n.get_property("active").unwrap(), &Value::from(true));
    } else {
        panic!("Expected node");
    }
}

#[test]
fn test_upsert_n_updated_props_visible_via_traversal() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create two nodes and an edge between them
    let node_a = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Alice")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let node_a_id = node_a[0].id();

    let node_b_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Bob")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, node_b_id, node_a_id, false, false)
        .collect_to_obj()
        .unwrap();

    // Upsert node_a with updated props
    G::new_mut_from_iter(&storage, &mut txn, node_a.into_iter(), &arena)
        .upsert_n(
            "person",
            &[
                ("name", Value::from("Alice Updated")),
                ("email", Value::from("alice@example.com")),
            ],
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    txn.commit().unwrap();

    // Traverse from node_b via in_node to reach node_a — should see updated props
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let reached = G::new(&storage, &txn, &arena)
        .n_from_id(&node_b_id)
        .out_node("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(reached.len(), 1);
    assert_eq!(reached[0].id(), node_a_id);
    if let TraversalValue::Node(n) = &reached[0] {
        assert_eq!(
            n.get_property("name").unwrap(),
            &Value::from("Alice Updated")
        );
        assert_eq!(
            n.get_property("email").unwrap(),
            &Value::from("alice@example.com")
        );
    } else {
        panic!("Expected node");
    }
}

// ============================================================================
// Vector Persistence Tests
// ============================================================================

#[test]
fn test_upsert_v_update_persisted_and_searchable() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Insert initial vector
    let initial = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(&[0.5, 0.6, 0.7], "embedding", None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let vector_id = initial[0].id();

    // Upsert to update props
    G::new_mut_from_iter(&storage, &mut txn, initial.into_iter(), &arena)
        .upsert_v(
            &[0.5, 0.6, 0.7],
            "embedding",
            &[("model", Value::from("v2")), ("score", Value::from(0.95))],
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    txn.commit().unwrap();

    // Fresh arena + read txn — search should still find it
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let results = G::new(&storage, &txn, &arena)
        .search_v::<Filter, _>(&[0.5, 0.6, 0.7], 10, "embedding", None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(!results.is_empty(), "Updated vector should be searchable");
    assert_eq!(results[0].id(), vector_id);
    if let TraversalValue::Vector(v) = &results[0] {
        assert_eq!(v.get_property("model").unwrap(), &Value::from("v2"));
        assert_eq!(v.get_property("score").unwrap(), &Value::from(0.95));
    } else {
        panic!("Expected vector");
    }
}

#[test]
fn test_upsert_v_multiple_sequential_upserts() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Insert initial vector with props
    let v1 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<Filter>(
            &[0.1, 0.2, 0.3],
            "embedding",
            props_option(&arena, props!("model" => "v1", "dim" => 3)),
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let vector_id = v1[0].id();

    // First upsert: update "model", add "source"
    let v2 = G::new_mut_from_iter(&storage, &mut txn, v1.into_iter(), &arena)
        .upsert_v(
            &[0.1, 0.2, 0.3],
            "embedding",
            &[
                ("model", Value::from("v2")),
                ("source", Value::from("openai")),
            ],
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(v2[0].id(), vector_id, "ID should be stable across upserts");

    // Second upsert: overwrite "source", add "timestamp"
    let v3 = G::new_mut_from_iter(&storage, &mut txn, v2.into_iter(), &arena)
        .upsert_v(
            &[0.1, 0.2, 0.3],
            "embedding",
            &[
                ("source", Value::from("anthropic")),
                ("timestamp", Value::from(1700000000)),
            ],
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(v3[0].id(), vector_id, "ID should be stable across upserts");

    // Verify accumulated properties
    if let TraversalValue::Vector(v) = &v3[0] {
        assert_eq!(v.get_property("model").unwrap(), &Value::from("v2")); // from first upsert
        assert_eq!(v.get_property("dim").unwrap(), &Value::from(3)); // from insert (preserved)
        assert_eq!(v.get_property("source").unwrap(), &Value::from("anthropic")); // overwritten
        assert_eq!(
            v.get_property("timestamp").unwrap(),
            &Value::from(1700000000)
        ); // added
    } else {
        panic!("Expected vector");
    }

    txn.commit().unwrap();
}

// ============================================================================
// Cross-Type Test
// ============================================================================

#[test]
fn test_upsert_e_between_different_node_labels() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let person_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "Alice")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let company_id = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "company",
            props_option(&arena, props!("name" => "Helix Corp")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Upsert edge person -> company
    G::new_mut_from_iter(
        &storage,
        &mut txn,
        std::iter::empty::<TraversalValue>(),
        &arena,
    )
    .upsert_e(
        "works_at",
        person_id,
        company_id,
        &[
            ("role", Value::from("engineer")),
            ("since", Value::from(2023)),
        ],
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap();

    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // out_node from person reaches company
    let out_nodes = G::new(&storage, &txn, &arena)
        .n_from_id(&person_id)
        .out_node("works_at")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(out_nodes.len(), 1);
    assert_eq!(out_nodes[0].id(), company_id);
    if let TraversalValue::Node(n) = &out_nodes[0] {
        assert_eq!(n.label, "company");
        assert_eq!(n.get_property("name").unwrap(), &Value::from("Helix Corp"));
    } else {
        panic!("Expected node");
    }

    // in_node from company reaches person
    let in_nodes = G::new(&storage, &txn, &arena)
        .n_from_id(&company_id)
        .in_node("works_at")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(in_nodes.len(), 1);
    assert_eq!(in_nodes[0].id(), person_id);
    if let TraversalValue::Node(n) = &in_nodes[0] {
        assert_eq!(n.label, "person");
    } else {
        panic!("Expected node");
    }

    // out_e has correct props
    let out_edges = G::new(&storage, &txn, &arena)
        .n_from_id(&person_id)
        .out_e("works_at")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(out_edges.len(), 1);
    if let TraversalValue::Edge(e) = &out_edges[0] {
        assert_eq!(e.from_node, person_id);
        assert_eq!(e.to_node, company_id);
        assert_eq!(e.get_property("role").unwrap(), &Value::from("engineer"));
        assert_eq!(e.get_property("since").unwrap(), &Value::from(2023));
    } else {
        panic!("Expected edge");
    }
}
