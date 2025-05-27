use std::{sync::Arc, time::Instant};

use crate::{
    helix_engine::graph_core::ops::source::n_from_type::NFromTypeAdapter,
    protocol::{
        filterable::Filterable,
        id::ID,
        items::{Edge, Node},
        traversal_value::TraversalValue,
        value::Value,
    },
};
use crate::{
    helix_engine::graph_core::ops::source::{
        bulk_add_e::BulkAddEAdapter, e_from_type::EFromTypeAdapter,
    },
    props,
};
use crate::{
    helix_engine::{
        graph_core::ops::{
            g::G,
            in_::{in_e::InEdgesAdapter, to_n::ToNAdapter},
            out::{from_n::FromNAdapter, out::OutAdapter},
            source::{
                add_n::AddNAdapter, bulk_add_n::BulkAddNAdapter, e_from_id::EFromIdAdapter,
                n_from_id::NFromIdAdapter,
            },
            tr_val::{Traversable, TraversalVal},
            util::{dedup::DedupAdapter, range::RangeAdapter},
        },
        storage_core::{storage_core::HelixGraphStorage, storage_methods::StorageMethods},
        types::GraphError,
    },
    protocol::items::v6_uuid,
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use super::ops::{
    in_::in_::InAdapter,
    out::out_e::OutEdgesAdapter,
    source::add_e::{AddEAdapter, EdgeType},
    util::filter_ref::FilterRefAdapter,
};

fn setup_test_db() -> (Arc<HelixGraphStorage>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let storage = HelixGraphStorage::new(db_path, super::config::Config::default()).unwrap();
    (Arc::new(storage), temp_dir)
}

#[test]
fn test_add_n() {
    let (storage, _temp_dir) = setup_test_db();

    let mut txn = storage.graph_env.write_txn().unwrap();

    let nodes = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "name" => "John"}), None)
        .filter_map(|node| node.ok())
        .collect::<Vec<_>>();

    let node = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&nodes.first().unwrap().id())
        .collect_to::<Vec<_>>();
    assert_eq!(node.first().unwrap().label(), "person");
    println!("node: {:?}", node.first().unwrap());

    assert_eq!(node.first().unwrap().id(), nodes.first().unwrap().id());
    assert_eq!(
        *node.first().unwrap().check_property("name").unwrap(),
        Value::String("John".to_string())
    );
    println!("node: {:?}", node.first().unwrap());

    // If we haven't dropped txn, ensure no borrows exist before commit
    txn.commit().unwrap();
}

#[test]
fn test_add_e() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let node1 = node1.first().unwrap();
    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let node2 = node2.first().unwrap();

    txn.commit().unwrap();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let edges = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            node1.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .filter_map(|edge| edge.ok())
        .collect::<Vec<_>>();
    txn.commit().unwrap();
    // Check that the current step contains a single edge
    match edges.first() {
        Some(edge) => {
            assert_eq!(edge.label(), "knows");
            match edge {
                TraversalVal::Edge(edge) => {
                    assert_eq!(edge.from_node(), node1.id());
                    assert_eq!(edge.to_node(), node2.id());
                }
                _ => panic!("Expected Edge value"),
            }
        }
        None => panic!("Expected SingleEdge value"),
    }
}

#[test]
fn test_out() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create graph: (person1)-[knows]->(person2)-[knows]->(person3)
    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person1 = person1.first().unwrap();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person2 = person2.first().unwrap();
    let person3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person3 = person3.first().unwrap();

    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person2.id(),
            person3.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();

    txn.commit().unwrap();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // let nodes = VFromId::new(&storage, &txn, person1.id.as_str())
    //     .out("knows")
    //     .filter_map(|node| node.ok())
    //     .collect::<Vec<_>>();
    let nodes = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&person1.id())
        .out("knows", &EdgeType::Node)
        .filter_map(|node| node.ok())
        .collect::<Vec<_>>();

    // txn.commit().unwrap();
    // Check that current step is at person2
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person2.id());
}

#[test]
fn test_out_e() {
    let (storage, _temp_dir) = setup_test_db();

    // Create graph: (person1)-[knows]->(person2)

    let mut txn = storage.graph_env.write_txn().unwrap();
    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .filter_map(|node| node.ok())
        .collect::<Vec<_>>();
    let person1 = person1.first().unwrap();
    txn.commit().unwrap();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .filter_map(|node| node.ok())
        .collect::<Vec<_>>();
    let person2 = person2.first().unwrap();
    txn.commit().unwrap();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id().clone(),
            person2.id().clone(),
            false,
            EdgeType::Node,
        )
        .filter_map(|edge| edge.ok())
        .collect::<Vec<_>>();
    let edge = edge.first().unwrap();
    // println!("traversal edge: {:?}", edge);

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    println!("processing");
    let edges = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&person1.id())
        .out_e("knows")
        .collect_to::<Vec<_>>();
    println!("edges: {}", edges.len());

    // Check that current step is at the edge between person1 and person2
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id(), edge.id());
    assert_eq!(edges[0].label(), "knows");
}

#[test]
fn test_in() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create graph: (person1)-[knows]->(person2)
    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person1 = person1.first().unwrap();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person2 = person2.first().unwrap();

    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&person2.id())
        .in_("knows", &EdgeType::Node)
        .collect_to::<Vec<_>>();

    // Check that current step is at person1
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person1.id());
}

#[test]
fn test_in_e() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph: (person1)-[knows]->(person2)
    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person1 = person1.first().unwrap();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person2 = person2.first().unwrap();
    println!("person1: {:?}", person1);
    println!("person2: {:?}", person2);

    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            true,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    let edge = edge.first().unwrap();
    println!("edge: {:?}", edge);

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    let edges = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&person2.id())
        .in_e("knows")
        .collect_to::<Vec<_>>();

    // Check that current step is at the edge between person1 and person2
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id(), edge.id());
    assert_eq!(edges[0].label(), "knows");
}

#[test]
fn test_complex_traversal() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Graph structure:
    // (person1)-[knows]->(person2)-[likes]->(person3)
    //     ^                                     |
    //     |                                     |
    //     +-------<------[follows]------<-------+

    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person1 = person1.first().unwrap();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person2 = person2.first().unwrap();
    let person3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person3 = person3.first().unwrap();

    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "likes",
            Some(props!()),
            person2.id(),
            person3.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "follows",
            Some(props!()),
            person3.id(),
            person1.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();

    let nodes = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&person1.id())
        .out("knows", &EdgeType::Node)
        .collect_to::<Vec<_>>();

    // Check that current step is at person2
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person2.id());

    // Traverse from person2 to person3
    let nodes = G::new_from(Arc::clone(&storage), &txn, vec![nodes[0].clone()])
        .out("likes", &EdgeType::Node)
        .collect_to::<Vec<_>>();

    // Check that current step is at person3
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person3.id());

    // Traverse from person3 to person1
    let nodes = G::new_from(Arc::clone(&storage), &txn, vec![nodes[0].clone()])
        .out("follows", &EdgeType::Node)
        .collect_to::<Vec<_>>();

    // Check that current step is at person1
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person1.id());
}

#[test]
fn test_count_single_node() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let person = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person = person.first().unwrap();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&person.id())
        .count();

    assert_eq!(count, 1);
}

#[test]
fn test_count_node_array() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person") // Get all nodes
        .count();
    assert_eq!(count, 3);
}

#[test]
fn test_count_mixed_steps() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create a graph with multiple paths
    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person1 = person1.first().unwrap();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person2 = person2.first().unwrap();
    let person3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person3 = person3.first().unwrap();

    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person3.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    txn.commit().unwrap();
    println!(
        "person1: {:?},\nperson2: {:?},\nperson3: {:?}",
        person1, person2, person3
    );

    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&person1.id())
        .out("knows", &EdgeType::Node)
        .count();

    assert_eq!(count, 2);
}

#[test]
fn test_range_subset() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create multiple nodes
    let _: Vec<_> = (0..5)
        .map(|_| {
            G::new_mut(Arc::clone(&storage), &mut txn)
                .add_n("person", Some(props!()), None)
                .collect_to::<Vec<_>>()
                .first()
                .unwrap();
        })
        .collect();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person") // Get all nodes
        .range(1, 3) // Take nodes at index 1 and 2
        .count();

    assert_eq!(count, 2);
}

#[test]
fn test_range_chaining() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create graph: (p1)-[knows]->(p2)-[knows]->(p3)-[knows]->(p4)-[knows]->(p5)
    let nodes: Vec<_> = (0..5)
        .map(|i| {
            G::new_mut(Arc::clone(&storage), &mut txn)
                .add_n("person", Some(props! { "name" => i }), None)
                .collect_to::<Vec<_>>()
                .first()
                .unwrap()
                .clone()
        })
        .collect();

    // Create edges connecting nodes sequentially
    for i in 0..4 {
        G::new_mut(Arc::clone(&storage), &mut txn)
            .add_e(
                "knows",
                Some(props!()),
                nodes[i].id(),
                nodes[i + 1].id(),
                false,
                EdgeType::Node,
            )
            .collect_to::<Vec<_>>();
    }

    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            nodes[4].id(),
            nodes[0].id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person") // Get all nodes
        .range(0, 3) // Take first 3 nodes
        .out("knows", &EdgeType::Node) // Get their outgoing nodes
        .collect_to::<Vec<_>>();

    assert_eq!(count.len(), 3);
}

#[test]
fn test_range_empty() {
    let (storage, _temp_dir) = setup_test_db();

    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person") // Get all nodes
        .range(0, 0) // Take first 3 nodes
        .collect_to::<Vec<_>>();

    assert_eq!(count.len(), 0);
}

#[test]
fn test_count_empty() {
    let (storage, _temp_dir) = setup_test_db();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person") // Get all nodes
        .range(0, 0) // Take first 3 nodes
        .count();

    assert_eq!(count, 0);
}

#[test]
fn test_n_from_id() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create a test node
    let person = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let node_id = person.id().clone();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node_id)
        .collect_to::<Vec<_>>();

    assert_eq!(count.len(), 1);
}

#[test]
fn test_n_from_id_with_traversal() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph: (person1)-[knows]->(person2)
    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            true,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&person1.id())
        .out("knows", &EdgeType::Node)
        .collect_to::<Vec<_>>();

    // Check that traversal reaches person2
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].id(), person2.id());
}

#[test]
fn test_e_from_id() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph and edge
    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    let edge_id = edge.first().unwrap().id();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&edge_id)
        .collect_to::<Vec<_>>();

    // Check that the current step contains the correct single edge
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id(), edge_id);
    assert_eq!(edges[0].label(), "knows");
    if let Some(TraversalVal::Edge(edge)) = edges.first() {
        assert_eq!(edge.from_node(), person1.id());
        assert_eq!(edge.to_node(), person2.id());
    } else {
        assert!(false, "Expected Edge value");
    }
}

#[test]
fn test_n_from_id_nonexistent() {
    let (storage, _temp_dir) = setup_test_db();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&100)
        .collect_to::<Vec<_>>();
    assert!(nodes.is_empty());
}

#[test]
fn test_e_from_id_nonexistent() {
    let (storage, _temp_dir) = setup_test_db();
    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&100)
        .collect_to::<Vec<_>>();
    assert!(edges.is_empty());
}

#[test]
fn test_n_from_id_chain_operations() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph: (person1)-[knows]->(person2)-[likes]->(person3)
    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();

    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "likes",
            Some(props!()),
            person2.id(),
            person3.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&person1.id())
        .out("knows", &EdgeType::Node)
        .out("likes", &EdgeType::Node)
        .collect_to::<Vec<_>>();

    // Check that the chain of traversals reaches person3
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person3.id());
}

#[test]
fn test_e_from_id_chain_operations() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph and edges
    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();

    let edge1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person2.id(),
            person1.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "likes",
            Some(props!()),
            person2.id(),
            person3.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&edge1.id())
        .from_n()
        .collect_to::<Vec<_>>();

    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person2.id());
    assert_eq!(nodes[0].label(), "person");
}

/*
#[test]
fn test_filter_nodes() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create nodes with different properties
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 25 }), None)
        .collect_to::<Vec<_>>();
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 30 }), None)
        .collect_to::<Vec<_>>();
    let person3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 35 }), None)
        .collect_to::<Vec<_>>();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .filter_ref(|val, _| {
            if let Ok(TraversalVal::Node(node)) = val {
                if let Ok(value) = node.check_property("age") {
                    match value {
                        Value::F64(age) => Ok(*age > 30.0),
                        Value::I32(age) => Ok(*age > 30),
                        _ => Ok(false),
                    }
                } else {
                    Ok(false)
                }
            } else {
                Ok(false)
            }
        })
        .collect_to::<Vec<_>>();
    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person3.id());
}

#[test]
fn test_filter_macro_single_argument() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "name" => "Alice" }), None)
        .collect_to::<Vec<_>>();
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "name" => "Bob" }), None)
        .collect_to::<Vec<_>>();

    fn has_name(val: &Result<TraversalVal, GraphError>) -> Result<bool, GraphError> {
        if let Ok(TraversalVal::Node(node)) = val {
            return node.check_property("name").map_or(Ok(false), |_| Ok(true));
        } else {
            return Ok(false);
        }
    }

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .filter_ref(|val, _| has_name(val))
        .collect_to::<Vec<_>>();
    assert_eq!(traversal.len(), 2);
    assert!(traversal
        .iter()
        .any(|val| if let TraversalVal::Node(node) = val {
            let name = node.check_property("name").unwrap();
            name == &Value::String("Alice".to_string()) || name == &Value::String("Bob".to_string())
        } else {
            false
        }));
}

#[test]
fn test_filter_macro_multiple_arguments() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 25 }), None)
        .collect_to::<Vec<_>>();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 30 }), None)
        .collect_to::<Vec<_>>();
    txn.commit().unwrap();

    fn age_greater_than(
        val: &Result<TraversalVal, GraphError>,
        min_age: i32,
    ) -> Result<bool, GraphError> {
        if let Ok(TraversalVal::Node(node)) = val {
            if let Ok(value) = node.check_property("age") {
                match value {
                    Value::F64(age) => Ok(*age > min_age as f64),
                    Value::I32(age) => Ok(*age > min_age),
                    _ => Ok(false),
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .filter_ref(|val, _| age_greater_than(val, 27))
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person2.id());
}

#[test]
fn test_filter_edges() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();

    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props! { "since" => 2020 }),
            person1.id(),
            person2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    let edge2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props! { "since" => 2022 }),
            person2.id(),
            person1.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    fn recent_edge(val: &Result<TraversalVal, GraphError>, year: i32) -> Result<bool, GraphError> {
        if let Ok(TraversalVal::Edge(edge)) = val {
            if let Ok(value) = edge.check_property("since") {
                match value {
                    Value::I32(since) => return Ok(*since > year),
                    Value::F64(since) => return Ok(*since > year as f64),
                    _ => return Ok(false),
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    let traversal = G::new(Arc::clone(&storage), &txn)
        .e_from_type("knows")
        .filter_ref(|val, _| recent_edge(val, 2021))
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), edge2.id());
}

#[test]
fn test_filter_empty_result() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 25 }), None)
        .collect_to::<Vec<_>>();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .filter_ref(|val, _| {
            if let Ok(TraversalVal::Node(node)) = val {
                if let Ok(value) = node.check_property("age") {
                    match value {
                        Value::I32(age) => return Ok(*age > 100),
                        Value::F64(age) => return Ok(*age > 100.0),
                        _ => return Ok(false),
                    }
                } else {
                    Ok(false)
                }
            } else {
                Ok(false)
            }
        })
        .collect_to::<Vec<_>>();
    assert!(traversal.is_empty());
}

#[test]
fn test_filter_chain() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n(
            "person",
            Some(props! { "age" => 25, "name" => "Alice" }),
            None,
        )
        .collect_to_val();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n(
            "person",
            Some(props! { "age" => 30, "name" => "Bob" }),
            None,
        )
        .collect_to_val();
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 35 }), None)
        .collect_to_val();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    fn has_name(val: &Result<TraversalVal, GraphError>) -> Result<bool, GraphError> {
        if let Ok(TraversalVal::Node(node)) = val {
            return node.check_property("name").map_or(Ok(false), |_| Ok(true));
        } else {
            return Ok(false);
        }
    }

    fn age_greater_than(
        val: &Result<TraversalVal, GraphError>,
        min_age: i32,
    ) -> Result<bool, GraphError> {
        if let Ok(TraversalVal::Node(node)) = val {
            if let Ok(value) = node.check_property("age") {
                match value {
                    Value::F64(age) => return Ok(*age > min_age as f64),
                    Value::I32(age) => return Ok(*age > min_age),
                    _ => return Ok(false),
                }
            } else {
                return Ok(false);
            }
        } else {
            return Ok(false);
        }
    }

    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .filter_ref(|val, _| has_name(val))
        .filter_ref(|val, _| age_greater_than(val, 27))
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person2.id());
}

#[test]
fn test_in_n() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("per son", Some(props!()), None)
        .collect_to_val();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to_val();

    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_val();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&edge.id())
        .to_n()
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person2.id());
}

#[test]
fn test_out_n() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to_val();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to_val();

    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_val();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&edge.id())
        .from_n()
        .collect_to::<Vec<_>>();
    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person1.id());
}

#[test]
fn test_edge_properties() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let node1 = node1.first().unwrap().clone();
    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to_val();
    let props = Some(props! { "since" => 2020, "date" => 1744965900, "name" => "hello"});
    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            props.clone(),
            node1.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let edge = G::new_from(Arc::clone(&storage), &txn, vec![node1])
        .out_e("knows")
        .filter_ref(|val, _| {
            if let Ok(val) = val {
                println!("val: {:?}", val.check_property("date"));
                val.check_property("date").map_or(Ok(false), |v| {
                    println!("v: {:?}", v);
                    println!("v: {:?}", *v == 1743290007);
                    Ok(*v >= 1743290007)
                })
            } else {
                Ok(false)
            }
        })
        .collect_to::<Vec<_>>();
    let edge = edge.first().unwrap();
    match edge {
        TraversalVal::Edge(edge) => {
            assert_eq!(
                edge.properties.clone().unwrap(),
                props.unwrap().into_iter().collect()
            );
        }
        _ => {
            panic!("Expected Edge value");
        }
    }
}

// #[test]
// fn test_shortest_mutual_path() {
//     let (storage, _temp_dir) = setup_test_db();
//     let mut txn = storage.graph_env.write_txn().unwrap();

//     // Create a complex network of mutual and one-way connections
//     // Mutual: Alice <-> Bob <-> Charlie <-> David
//     // One-way: Alice -> Eve -> David
//     let users: Vec<Node> = vec!["alice", "bob", "charlie", "dave", "eve"]
//         .iter()
//         .map(|name| {
//             storage
//                 .create_node(&mut txn, "person", Some(props! ){ "name" => *name }, None, None)
//                 .unwrap()
//         })
//         .collect();

//     for (i, j) in [(0, 1), (1, 2), (2, 3)].iter() {
//         storage
//             .create_edge(&mut txn, "knows", &users[*i].id, &users[*j].id, Some(props!()))
//             .unwrap();
//         storage
//             .create_edge(&mut txn, "knows", &users[*j].id, &users[*i].id, Some(props!()))
//             .unwrap();
//     }

//     storage
//         .create_edge(&mut txn, "knows", &users[0].id, &users[4].id, Some(props!()))
//         .unwrap();
//     storage
//         .create_edge(&mut txn, "knows", &users[4].id, &users[3].id, Some(props!()))
//         .unwrap();

//     txn.commit().unwrap();

//     let txn = storage.graph_env.read_txn().unwrap();
//     let mut tr =
//         TraversalBuilder::new(Arc::clone(&storage), TraversalValue::from(users[0].clone()));
//     tr.shortest_mutual_path_to(&txn, &users[3].id);

//     let result = tr.result(txn);
//     let paths = match result.unwrap() {
//         TraversalValue::Paths(paths) => paths,
//         _ => {
//             panic!("Expected PathArray value")
//         }
//     };

//     assert_eq!(paths.len(), 1);
//     let (nodes, edges) = &paths[0];

//     assert_eq!(nodes.len(), 4);
//     assert_eq!(edges.len(), 3);
//     assert_eq!(nodes[0].id, users[3].id); // David
//     assert_eq!(nodes[1].id, users[2].id); // Charlie
//     assert_eq!(nodes[2].id, users[1].id); // Bob
//     assert_eq!(nodes[3].id, users[0].id); // Alice
// }

#[test]
fn huge_traversal() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let mut nodes = Vec::with_capacity(65_000_000);
    let mut start = Instant::now();

    for i in 0..100_000 {
        // nodes.push(Node::new("person", Some(props! ){ "name" => i}));
        nodes.push(v6_uuid());
    }
    println!("time taken to initialise nodes: {:?}", start.elapsed());
    start = Instant::now();

    println!("time taken to sort nodes: {:?}", start.elapsed());
    start = Instant::now();
    let now = Instant::now();
    let res = G::new_mut(Arc::clone(&storage), &mut txn)
        .bulk_add_n(&mut nodes, None, 1000000)
        .map(|res| res.unwrap())
        .collect::<Vec<_>>();
    txn.commit().unwrap();
    println!("time taken to add nodes: {:?}", now.elapsed());
    let start = Instant::now();
    let mut edges = Vec::with_capacity(6000 * 2000);
    for i in 0..10_000 {
        let random_node1 = &nodes[rand::rng().random_range(0..nodes.len())];
        let random_node2 = &nodes[rand::rng().random_range(0..nodes.len())];
        // edges.push(Edge {
        //     id: v6_uuid(),
        //     label: "knows".to_string(),
        //     properties: HashMap::new(),
        //     from_node: random_node1.id,
        //     to_node: random_node2.id,
        // });
        edges.push((*random_node1, *random_node2, v6_uuid()));
    }
    println!(
        "time taken to create {} edges: {:?}",
        edges.len(),
        start.elapsed()
    );
    let mut start = Instant::now();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let res = G::new_mut(Arc::clone(&storage), &mut txn)
        .bulk_add_e(edges, false, 1000000)
        .map(|res| res.unwrap())
        .collect::<Vec<_>>();
    txn.commit().unwrap();
    println!("time taken to add edges: {:?}", start.elapsed());

    let txn = storage.graph_env.read_txn().unwrap();
    let now = Instant::now();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("user")
        .out_e("knows")
        .to_n()
        .out("knows", &EdgeType::Node)
        // .filter_ref(|val, _| {
        //     if let Ok(TraversalVal::Node(node)) = val {
        //         if let Some(value) = node.check_property("name") {
        //             match value {
        //                 Value::I32(name) => return *name < 700000,
        //                 _ => return false,
        //             }
        //         } else {
        //             return false;
        //         }
        //     } else {
        //         return false;
        //     }
        // })
        .out("knows", &EdgeType::Node)
        .out("knows", &EdgeType::Node)
        .out("knows", &EdgeType::Node)
        .out("knows", &EdgeType::Node)
        .dedup()
        .range(0, 10000)
        .count();
    println!("optimized version time: {:?}", now.elapsed());
    println!("traversal: {:?}", traversal);
    println!(
        "size of mdb file on disk: {:?}",
        storage.graph_env.real_disk_size()
    );
    txn.commit().unwrap();

    // let txn = storage.graph_env.read_txn().unwrap();
    // let now = Instant::now();
    // let mut tr = TraversalBuilder::new(Arc::clone(&storage), TraversalValue::Empty);
    // tr.v(&txn)
    //     .out_e(&txn, "knows")
    //     .in_v(&txn)
    //     .out(&txn, "knows")
    //     .filter_nodes(&txn, |val| {
    //         if let Some(value) = val.check_property("name") {
    //             match value {
    //                 Value::I32(name) => return Ok(*name < 1000),
    //                 _ => return Err(GraphError::Default),
    //             }
    //         } else {
    //             return Err(GraphError::Default);
    //         }
    //     })
    //     .out(&txn, "knows")
    //     .out(&txn, "knows")
    //     .out(&txn, "knows")
    //     .out(&txn, "knows")
    //     .range(0, 100);

    // let result = tr.finish();
    // println!("original version time: {:?}", now.elapsed());
    // println!(
    //     "traversal: {:?}",
    //     match result {
    //         Ok(TraversalValue::NodeArray(nodes)) => nodes.len(),
    //         Err(e) => {
    //             println!("error: {:?}", e);
    //             0
    //         }
    //         _ => {
    //             println!("error: {:?}", result);
    //             0
    //         }
    //     }
    // );
    // // print size of mdb file on disk
    // println!(
    //     "size of mdb file on disk: {:?}",
    //     storage.graph_env.real_disk_size()
    // );
    assert!(false);
}

#[test]
fn test_with_id_type() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "name" => "test" }), None)
        .collect_to_val();
    txn.commit().unwrap();
    #[derive(Serialize, Deserialize, Debug)]
    struct Input {
        id: ID,
        name: String,
    }

    let input = sonic_rs::from_slice::<Input>(
        format!(
            "{{\"id\":\"{}\",\"name\":\"test\"}}",
            uuid::Uuid::from_u128(node.id()).to_string()
        )
        .as_bytes(),
    )
    .unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&input.id)
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), input.id.inner());
}
