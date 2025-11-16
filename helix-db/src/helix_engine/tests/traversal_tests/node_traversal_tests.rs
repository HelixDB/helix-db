use super::test_utils::props_option;
use std::sync::Arc;

use crate::{
    helix_engine::{
        storage_core::{HelixGraphStorage, txn::{ReadTransaction, WriteTransaction}},
        traversal_core::{
            ops::{
                g::G,
                in_::in_::InAdapter,
                out::out::OutAdapter,
                source::{
                    add_e::AddEAdapter, add_n::AddNAdapter, e_from_type::EFromTypeAdapter,
                    n_from_id::NFromIdAdapter, n_from_type::NFromTypeAdapter,
                },
                util::{filter_ref::FilterRefAdapter, drop::Drop},
            },
            traversal_value::TraversalValue,
        },
        types::GraphError,
    },
    props,
    protocol::value::Value,
    utils::id::ID,
};

use bumpalo::Bump;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
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

#[test]
fn test_add_n() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();

    let mut txn = storage.graph_env.write_txn().unwrap();

    let nodes = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John"}),
            None,
        )
        .filter_map(|node| node.ok())
        .collect::<Vec<_>>();
    println!("nodes {:?}", nodes);
    let node = &nodes.first().unwrap();
    println!("node {:?}", node);
    let node_results: Vec<_> = G::new(&storage, &txn, &arena)
        .n_from_id(&node.id())
        .collect();

    let node = node_results
        .into_iter()
        .filter_map(|res| res.ok())
        .collect::<Vec<_>>();
    assert_eq!(node.first().unwrap().label(), "person");
    println!("node: {:?}", node.first().unwrap());

    assert_eq!(node.first().unwrap().id(), nodes.first().unwrap().id());
    assert_eq!(
        *node.first().unwrap().get_property("name").unwrap(),
        Value::String("John".to_string())
    );
    println!("node: {:?}", node.first().unwrap());

    // If we haven't dropped txn, ensure no borrows exist before commit
    txn.commit().unwrap();
}

#[test]
fn test_out() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create graph: (person1)-[knows]->(person2)-[knows]->(person3)
    let person1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>,_>>().unwrap();
    let person1 = person1.first().unwrap();
    let person2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>,_>>().unwrap();
    let person2 = person2.first().unwrap();
    let person3 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>,_>>().unwrap();
    let person3 = person3.first().unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, person1.id(), person2.id(), false)
        .collect::<Result<Vec<_>,_>>().unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, person2.id(), person3.id(), false)
        .collect::<Result<Vec<_>,_>>().unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    let nodes = G::new(&storage, &txn, &arena)
        .n_from_id(&person1.id())
        .out_node("knows")
        .filter_map(|node| node.ok())
        .collect::<Vec<_>>();

    txn.commit().unwrap();
    // Check that current step is at person2
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person2.id());
}

#[test]
fn test_in() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create graph: (person1)-[knows]->(person2)
    let person1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>,_>>().unwrap();
    let person1 = person1.first().unwrap();
    let person2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>,_>>().unwrap();
    let person2 = person2.first().unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, person1.id(), person2.id(), false)
        .collect::<Result<Vec<_>,_>>().unwrap();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(&storage, &txn, &arena)
        .n_from_id(&person2.id())
        .in_node("knows")
        .collect::<Result<Vec<_>,_>>().unwrap();

    txn.commit().unwrap();
    // Check that current step is at person1
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person1.id());
}

#[test]
fn test_complex_traversal() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Graph structure:
    // (person1)-[knows]->(person2)-[likes]->(person3)
    //     ^                                     |
    //     |                                     |
    //     +-------<------[follows]------<-------+

    let person1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>,_>>().unwrap();
    let person1 = person1.first().unwrap();
    let person2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>,_>>().unwrap();
    let person2 = person2.first().unwrap();
    let person3 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>,_>>().unwrap();
    let person3 = person3.first().unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, person1.id(), person2.id(), false)
        .collect::<Result<Vec<_>,_>>().unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("likes", None, person2.id(), person3.id(), false)
        .collect::<Result<Vec<_>,_>>().unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("follows", None, person3.id(), person1.id(), false)
        .collect::<Result<Vec<_>,_>>().unwrap();
    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();

    let nodes = G::new(&storage, &txn, &arena)
        .n_from_id(&person1.id())
        .out_node("knows")
        .collect::<Result<Vec<_>,_>>().unwrap();

    // Check that current step is at person2
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person2.id());

    // Traverse from person2 to person3
    let node_id = nodes[0].id();
    let nodes = G::new(&storage, &txn, &arena)
        .n_from_id(&node_id)
        .out_node("likes")
        .collect::<Result<Vec<_>,_>>().unwrap();

    // Check that current step is at person3
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person3.id());

    // Traverse from person3 to person1
    let node_id = nodes[0].id();
    let nodes = G::new(&storage, &txn, &arena)
        .n_from_id(&node_id)
        .out_node("follows")
        .collect::<Result<Vec<_>,_>>().unwrap();

    // Check that current step is at person1
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person1.id());
}

#[test]
fn test_n_from_id() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create a test node
    let person = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect_to_obj().unwrap();
    let node_id = person.id();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(&storage, &txn, &arena)
        .n_from_id(&node_id)
        .collect::<Result<Vec<_>,_>>().unwrap();

    assert_eq!(count.len(), 1);
}

#[test]
fn test_n_from_id_with_traversal() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph: (person1)-[knows]->(person2)
    let person1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect_to_obj().unwrap();
    let person2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect_to_obj().unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, person1.id(), person2.id(), true)
        .collect::<Result<Vec<_>,_>>().unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(&storage, &txn, &arena)
        .n_from_id(&person1.id())
        .out_node("knows")
        .collect::<Result<Vec<_>,_>>().unwrap();

    // Check that traversal reaches person2
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].id(), person2.id());
}

#[test]
#[should_panic]
fn test_n_from_id_nonexistent() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    G::new(&storage, &txn, &arena)
        .n_from_id(&100)
        .collect::<Result<Vec<_>,_>>().unwrap();
}

#[test]
fn test_n_from_id_chain_operations() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph: (person1)-[knows]->(person2)-[likes]->(person3)
    let person1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect_to_obj().unwrap();
    let person2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect_to_obj().unwrap();
    let _ = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect_to_obj().unwrap();
    let person3 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect_to_obj().unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, person1.id(), person2.id(), false)
        .collect::<Result<Vec<_>,_>>().unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("likes", None, person2.id(), person3.id(), false)
        .collect::<Result<Vec<_>,_>>().unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(&storage, &txn, &arena)
        .n_from_id(&person1.id())
        .out_node("knows")
        .out_node("likes")
        .collect::<Result<Vec<_>,_>>().unwrap();

    // Check that the chain of traversals reaches person3
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person3.id());
}

#[test]
fn test_with_id_type() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "test" }),
            None,
        )
        .collect_to_obj().unwrap();
    txn.commit().unwrap();
    #[derive(Serialize, Deserialize, Debug)]
    struct Input {
        id: ID,
        name: String,
    }

    let input = sonic_rs::from_slice::<Input>(
        format!(
            "{{\"id\":\"{}\",\"name\":\"test\"}}",
            uuid::Uuid::from_u128(node.id())
        )
        .as_bytes(),
    )
    .unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_id(&input.id)
        .collect::<Result<Vec<_>,_>>().unwrap();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), input.id.inner());
}

#[test]
fn test_double_add_and_double_fetch() {
    let (_temp_dir, storage) = setup_test_db();
    let db = &*storage;
    let arena = Bump::new();
    let mut txn = db.graph_env.write_txn().unwrap();

    let original_node1 = G::new_mut(&db, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "entity_name" => "person1" }),
            None,
        )
        .collect_to_obj().unwrap();

    let original_node2 = G::new_mut(&db, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "entity_name" => "person2" }),
            None,
        )
        .collect_to_obj().unwrap();

    txn.commit().unwrap();

    let mut txn = db.graph_env.write_txn().unwrap();
    let node1 = G::new(&db, &txn, &arena)
        .n_from_type("person")
        .filter_ref(|val, _| {
            if let Ok(val) = val {
                Ok(val
                    .get_property("entity_name")
                    .map_or(false, |v| *v == "person1"))
            } else {
                Ok(false)
            }
        })
        .collect::<Result<Vec<_>,_>>().unwrap();

    let node2 = G::new(&db, &txn, &arena)
        .n_from_type("person")
        .filter_ref(|val, _| {
            if let Ok(val) = val {
                Ok(val
                    .get_property("entity_name")
                    .map_or(false, |v| *v == "person2"))
            } else {
                Ok(false)
            }
        })
        .collect::<Result<Vec<_>,_>>().unwrap();

    assert_eq!(node1.len(), 1);
    assert_eq!(node1[0].id(), original_node1.id());
    assert_eq!(node2.len(), 1);
    assert_eq!(node2[0].id(), original_node2.id());

    let _e = G::new_mut(&db, &arena, &mut txn)
        .add_edge(
            "knows",
            None,
            node1.first().unwrap().id(),
            node2.first().unwrap().id(),
            false,
        )
        .collect_to_obj().unwrap();

    txn.commit().unwrap();

    let txn = db.graph_env.read_txn().unwrap();
    let e = G::new(&db, &txn, &arena)
        .e_from_type("knows")
        .collect::<Result<Vec<_>,_>>().unwrap();
    assert_eq!(e.len(), 1);
    assert_eq!(e[0].id(), e.first().unwrap().id());
    if let TraversalValue::Edge(e) = &e[0] {
        assert_eq!(e.from_node, node1.first().unwrap().id());
        assert_eq!(e.to_node, node2.first().unwrap().id());
    } else {
        panic!("e[0] is not an edge");
    }
}

// ============================================================================
// Error Tests for n_from_id
// ============================================================================

#[test]
fn test_n_from_id_with_nonexistent_id() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // Generate a random ID that doesn't exist
    let fake_id = uuid::Uuid::new_v4().as_u128();

    // Attempt to query
    let result = G::new(&storage, &txn, &arena)
        .n_from_id(&fake_id)
        .collect_to_obj();

    // Assert it returns NodeNotFound error
    assert!(matches!(result, Err(GraphError::NodeNotFound)));
}

#[test]
fn test_n_from_id_with_deleted_node() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create a node
    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect_to_obj()
        .unwrap();
    let node_id = node.id();

    txn.commit().unwrap();

    // Delete the node
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let node_to_delete = G::new(&storage, &txn, &arena)
        .n_from_id(&node_id)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::drop_traversal(
        node_to_delete.into_iter().map(Ok::<_, GraphError>),
        storage.as_ref(),
        &mut txn,
    )
    .unwrap();
    txn.commit().unwrap();

    // Try to query the deleted node
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let result = G::new(&storage, &txn, &arena)
        .n_from_id(&node_id)
        .collect_to_obj();

    // Assert it returns NodeNotFound error
    assert!(matches!(result, Err(GraphError::NodeNotFound)));
}

#[test]
fn test_n_from_id_with_zero_id() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // Query with ID = 0
    let result = G::new(&storage, &txn, &arena)
        .n_from_id(&0)
        .collect_to_obj();

    // Assert it returns NodeNotFound error
    assert!(matches!(result, Err(GraphError::NodeNotFound)));
}

#[test]
fn test_n_from_id_with_max_id() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    // Query with ID = u128::MAX
    let result = G::new(&storage, &txn, &arena)
        .n_from_id(&u128::MAX)
        .collect_to_obj();

    // Assert it returns NodeNotFound error
    assert!(matches!(result, Err(GraphError::NodeNotFound)));
}
