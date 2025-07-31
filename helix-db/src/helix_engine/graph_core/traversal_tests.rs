use std::{collections::HashMap, sync::Arc, time::Instant};

use crate::{
    exclude_field,
    helix_engine::{
        graph_core::ops::{
            g::G,
            in_::{in_e::InEdgesAdapter, to_n::ToNAdapter, to_v::ToVAdapter},
            out::{from_n::FromNAdapter, from_v::FromVAdapter, out::OutAdapter},
            source::{add_n::AddNAdapter, e_from_id::EFromIdAdapter, n_from_id::NFromIdAdapter},
            tr_val::{Traversable, TraversalVal},
            util::{
                dedup::DedupAdapter, map::MapAdapter, order::OrderByAdapter, props::PropsAdapter, range::RangeAdapter
            },
            vectors::brute_force_search::BruteForceSearchVAdapter,
        },
        storage_core::storage_core::HelixGraphStorage,
        types::GraphError,
    },
    protocol::{
        remapping::{Remapping, RemappingMap, ResponseRemapping},
        return_values::ReturnValue,
    },
};
use crate::{
    helix_engine::graph_core::ops::{
        source::n_from_type::NFromTypeAdapter, util::paths::ShortestPathAdapter,
    },
    protocol::value::Value,
    utils::{filterable::Filterable, id::ID},
};
use crate::{
    helix_engine::{
        graph_core::ops::{
            source::e_from_type::EFromTypeAdapter,
            util::drop::Drop,
            vectors::{insert::InsertVAdapter, search::SearchVAdapter},
        },
        vector_core::vector::HVector,
    },
    props,
};
use heed3::RoTxn;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

use super::ops::{
    in_::in_::InAdapter,
    out::out_e::OutEdgesAdapter,
    source::add_e::{AddEAdapter, EdgeType},
    util::{filter_ref::FilterRefAdapter, update::UpdateAdapter},
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
    let txn = storage.graph_env.write_txn().unwrap();

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
            person1.id(),
            person2.id(),
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
    println!("person1: {person1:?}");
    println!("person2: {person2:?}");

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
    println!("edge: {edge:?}");

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
    println!("person1: {person1:?},\nperson2: {person2:?},\nperson3: {person3:?}");

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
    let node_id = person.id();

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
        panic!("Expected Edge value");
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
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
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
                    match value.as_ref() {
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
            node.check_property("name").map_or(Ok(false), |_| Ok(true))
        } else {
            Ok(false)
        }
    }

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .filter_ref(|val, _| has_name(val))
        .collect_to::<Vec<_>>();
    assert_eq!(traversal.len(), 2);
    assert!(
        traversal
            .iter()
            .any(|val| if let TraversalVal::Node(node) = val {
                let name = node.check_property("name").unwrap();
                name.as_ref() == &Value::String("Alice".to_string())
                    || name.as_ref() == &Value::String("Bob".to_string())
            } else {
                false
            })
    );
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
                match value.as_ref() {
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
                match value.as_ref() {
                    Value::I32(since) => Ok(*since > year),
                    Value::F64(since) => Ok(*since > year as f64),
                    _ => Ok(false),
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
                    match value.as_ref() {
                        Value::I32(age) => Ok(*age > 100),
                        Value::F64(age) => Ok(*age > 100.0),
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
            node.check_property("name").map_or(Ok(false), |_| Ok(true))
        } else {
            Ok(false)
        }
    }

    fn age_greater_than(
        val: &Result<TraversalVal, GraphError>,
        min_age: i32,
    ) -> Result<bool, GraphError> {
        if let Ok(TraversalVal::Node(node)) = val {
            if let Ok(value) = node.check_property("age") {
                match value.as_ref() {
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
    let props = props! { "since" => 2020, "date" => 1744965900, "name" => "hello"};
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props.clone()),
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
                    println!("v: {v:?}");
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
                props.into_iter().collect()
            );
        }
        _ => {
            panic!("Expected Edge value");
        }
    }
}

#[test]
fn test_drop_node() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "test")), None)
        .collect_to_val();
    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "test2")), None)
        .collect_to_val();
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            node.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_val();
    txn.commit().unwrap();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node.id())
        .collect_to::<Vec<_>>();

    Drop::<Vec<_>>::drop_traversal(traversal, Arc::clone(&storage), &mut txn).unwrap();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node.id())
        .collect_to_obj();

    let edges = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node2.id())
        .in_e("knows")
        .collect_to::<Vec<_>>();

    assert_eq!(traversal, TraversalVal::Empty);
    assert_eq!(edges.len(), 0);
}

#[test]
fn test_drop_edge() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to_val();
    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to_val();
    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            node1.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_val();

    txn.commit().unwrap();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&edge.id())
        .collect_to::<Vec<_>>();
    Drop::<Vec<_>>::drop_traversal(traversal, Arc::clone(&storage), &mut txn).unwrap();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&edge.id())
        .collect_to_obj();
    assert_eq!(traversal, TraversalVal::Empty);

    let edges = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node1.id())
        .out_e("knows")
        .collect_to::<Vec<_>>();

    assert_eq!(edges.len(), 0);

    let edges = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node2.id())
        .in_e("knows")
        .collect_to::<Vec<_>>();

    assert_eq!(edges.len(), 0);
}

#[test]
fn test_update_node() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "test")), None)
        .collect_to_val();
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "test2")), None)
        .collect_to_val();

    txn.commit().unwrap();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let _ = {
        let update_tr = G::new(Arc::clone(&storage), &txn)
            .n_from_id(&node.id())
            .collect_to::<Vec<_>>();
        G::new_mut_from(Arc::clone(&storage), &mut txn, update_tr)
            .update(Some(props! { "name" => "john"}))
            .collect_to::<Vec<_>>()
    };
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let updated_users = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node.id())
        .collect_to::<Vec<_>>();
    assert_eq!(updated_users.len(), 1);
    assert_eq!(
        updated_users[0]
            .check_property("name")
            .unwrap()
            .into_owned()
            .to_string(),
        "john"
    );
}

#[test]
fn test_shortest_path() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "node1")), None)
        .collect_to_val();
    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "node2")), None)
        .collect_to_val();
    let node3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "node3")), None)
        .collect_to_val();
    let node4 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "node4")), None)
        .collect_to_val();

    let edge1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!("name" => "edge1")),
            node1.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_val();
    let edge2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!("name" => "edge2")),
            node2.id(),
            node3.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_val();
    let edge3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!("name" => "edge3")),
            node3.id(),
            node4.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_val();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let path = G::new_from(Arc::clone(&storage), &txn, vec![node1.clone()])
        .shortest_path(Some("knows"), None, Some(&node4.id()))
        .collect_to::<Vec<_>>();
    assert_eq!(path.len(), 1);

    match path.first() {
        Some(TraversalVal::Path((nodes, edges))) => {
            assert_eq!(nodes.len(), 4);
            assert_eq!(edges.len(), 3);
            assert_eq!(*nodes[0].check_property("name").unwrap(), "node1");
            assert_eq!(*nodes[1].check_property("name").unwrap(), "node2");
            assert_eq!(*nodes[2].check_property("name").unwrap(), "node3");
            assert_eq!(*nodes[3].check_property("name").unwrap(), "node4");
            assert_eq!(*edges[0].check_property("name").unwrap(), "edge1");
            assert_eq!(*edges[1].check_property("name").unwrap(), "edge2");
            assert_eq!(*edges[2].check_property("name").unwrap(), "edge3");
            assert_eq!(*nodes[0].id(), node1.id());
            assert_eq!(*nodes[1].id(), node2.id());
            assert_eq!(*nodes[2].id(), node3.id());
            assert_eq!(*nodes[3].id(), node4.id());
            assert_eq!(*edges[0].id(), edge1.id());
            assert_eq!(*edges[1].id(), edge2.id());
            assert_eq!(*edges[2].id(), edge3.id());
        }
        _ => {
            panic!("Expected Path value");
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

#[ignore]
#[test]
#[should_panic]
fn huge_traversal() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let _start = Instant::now();
    let mut nodes = Vec::with_capacity(1_000_000);
    for _ in 0..1_000_000 {
        let id = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_n("user", None, None)
            .collect_to_val();
        nodes.push(id.id());
    }
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    println!(
        "size of mdb file on disk: {:?}",
        storage.graph_env.real_disk_size()
    );
    txn.commit().unwrap();
    let start = Instant::now();
    let mut txn = storage.graph_env.write_txn().unwrap();
    for _ in 0..1_000_000 {
        let random_node1 = nodes[rand::rng().random_range(0..nodes.len())];
        let random_node2 = nodes[rand::rng().random_range(0..nodes.len())];
        G::new_mut(Arc::clone(&storage), &mut txn)
            .add_e(
                "knows",
                None,
                random_node1,
                random_node2,
                false,
                EdgeType::Node,
            )
            .count();
    }
    println!("time taken to create edges: {:?}", start.elapsed());

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
        // .out("knows", &EdgeType::Node)
        // .out("knows", &EdgeType::Node)
        // .out("knows", &EdgeType::Node)
        .dedup()
        .range(0, 10000)
        .count();
    println!("optimized version time: {:?}", now.elapsed());
    println!("traversal: {traversal:?}");
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
    panic!();
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
            uuid::Uuid::from_u128(node.id())
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

#[ignore]
#[test]
#[should_panic]
fn test_add_n_parallel() {
    let (storage, _temp_dir) = setup_test_db();
    let n = 100_000_000;
    let chunks = n / 10000000;
    let _ = n / chunks;
    let start = Instant::now();

    let mut txn = storage.graph_env.write_txn().unwrap();
    for _ in 0..n {
        let _ = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_n("person", None, None)
            .count();
    }
    txn.commit().unwrap();

    println!("time taken to add {} nodes: {:?}", n, start.elapsed());
    println!(
        "size of mdb file on disk: {:?}",
        storage.graph_env.real_disk_size()
    );

    let start = Instant::now();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .collect_to::<Vec<_>>();

    println!("time taken to collect nodes: {:?}", start.elapsed());
    panic!("count: {:?}", count.len());
}

// 3 614 375 936
// 3 411 509 248

#[test]
fn test_add_e_between_node_and_vector() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_val();

    let vector = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(&[1.0, 2.0, 3.0], "vector", None)
        .collect_to_val();

    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e("knows", None, node.id(), vector.id(), false, EdgeType::Vec)
        .collect_to_val();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node.id())
        .out("knows", &EdgeType::Vec)
        .collect_to::<Vec<_>>();

    println!("traversal: {traversal:?}");

    println!(
        "edges: {:?}",
        G::new(Arc::clone(&storage), &txn)
            .e_from_type("knows")
            .collect_to::<Vec<_>>()
    );

    println!(
        "vectors: {:?}",
        G::new(Arc::clone(&storage), &txn)
            .search_v::<fn(&HVector, &RoTxn) -> bool, _>(&[1.0, 2.0, 3.0], 10, "vector", None)
            .collect_to::<Vec<_>>()
    );

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), vector.id());
}

#[test]
fn test_from_v() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_val();

    let vector = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(&[1.0, 2.0, 3.0], "vector", None)
        .collect_to_val();

    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e("knows", None, vector.id(), node.id(), false, EdgeType::Vec)
        .collect_to_val();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node.id())
        .in_e("knows")
        .from_v()
        .collect_to::<Vec<_>>();

    println!("traversal: {traversal:?}");

    assert_eq!(traversal.len(), 1);
}

#[test]
fn test_to_v() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_val();

    let vector = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(&[1.0, 2.0, 3.0], "vector", None)
        .collect_to_val();

    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e("knows", None, node.id(), vector.id(), false, EdgeType::Vec)
        .collect_to_val();

    txn.commit().unwrap();
    println!("node: {node:?}");

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node.id())
        .out_e("knows")
        .to_v()
        .collect_to::<Vec<_>>();

    println!("traversal: {traversal:?}");

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), vector.id());
}

#[test]
fn test_brute_force_vector_search() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_val();

    let vectors = vec![
        vec![1.0, 2.0, 3.0],
        vec![4.0, 5.0, 6.0],
        vec![7.0, 8.0, 9.0],
    ];

    let mut vector_ids = Vec::new();
    for vector in vectors {
        let vector_id = G::new_mut(Arc::clone(&storage), &mut txn)
            .insert_v::<fn(&HVector, &RoTxn) -> bool>(&vector, "vector", None)
            .collect_to_val()
            .id();
        let _ = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_e(
                "embedding",
                None,
                node.id(),
                vector_id,
                false,
                EdgeType::Vec,
            )
            .collect_to_val()
            .id();
        vector_ids.push(vector_id);
    }

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node.id())
        .out_e("embedding")
        .to_v()
        .brute_force_search_v(&[1.0, 2.0, 3.0], 10)
        .collect_to::<Vec<_>>();

    println!("traversal: {traversal:?}");

    assert_eq!(traversal.len(), 3);
    assert_eq!(traversal[0].id(), vector_ids[0]);
    assert_eq!(traversal[1].id(), vector_ids[1]);
    assert_eq!(traversal[2].id(), vector_ids[2]);
}

#[test]
fn test_order_by_desc() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 10 }), None)
        .collect_to_val();

    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 20 }), None)
        .collect_to_val();

    let node3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 30 }), None)
        .collect_to_val();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .order_by_desc("age")
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 3);
    assert_eq!(traversal[0].id(), node3.id());
    assert_eq!(traversal[1].id(), node2.id());
    assert_eq!(traversal[2].id(), node.id());
}

#[test]
fn test_order_by_asc() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 30 }), None)
        .collect_to_val();

    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 20 }), None)
        .collect_to_val();

    let node3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 10 }), None)
        .collect_to_val();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .order_by_asc("age")
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 3);
    assert_eq!(traversal[0].id(), node3.id());
    assert_eq!(traversal[1].id(), node2.id());
    assert_eq!(traversal[2].id(), node.id());
}

#[test]
fn test_vector_search() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let mut i = 0;
    let mut inserted_vectors = Vec::with_capacity(10000);

    let mut rng = rand::rng();
    for _ in 10..2000 {
        // between 0 and 1
        let random_vector = vec![
            rng.random::<f64>(),
            rng.random::<f64>(),
            rng.random::<f64>(),
            rng.random::<f64>(),
            rng.random::<f64>(),
            rng.random::<f64>(),
        ];
        let _ = G::new_mut(Arc::clone(&storage), &mut txn)
            .insert_v::<fn(&HVector, &RoTxn) -> bool>(&random_vector, "vector", None)
            .collect_to_val();
        println!("inserted vector: {i:?}");
        i += 1;
    }

    let vectors = vec![
        vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
    ];

    for vector in vectors {
        let node = G::new_mut(Arc::clone(&storage), &mut txn)
            .insert_v::<fn(&HVector, &RoTxn) -> bool>(&vector, "vector", None)
            .collect_to_val();
        inserted_vectors.push(node.id());
        println!("inserted vector: {i:?}");
        i += 1;
    }

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .search_v::<fn(&HVector, &RoTxn) -> bool, _>(&[1.0, 1.0, 1.0, 1.0, 1.0, 1.0], 2000, "vector", None)
        .collect_to::<Vec<_>>();
    // traversal.reverse();

    for vec in &traversal[0..10] {
        if let TraversalVal::Vector(vec) = vec {
            println!("vec {:?} {}", vec.get_data(), vec.get_distance());
            assert!(vec.get_distance() < 0.1);
        }
    }
}

#[test]
fn test_double_add_and_double_fetch() {
    let (db, _temp_dir) = setup_test_db();
    let mut txn = db.graph_env.write_txn().unwrap();

    let original_node1 = G::new_mut(Arc::clone(&db), &mut txn)
        .add_n("person", Some(props! { "entity_name" => "person1" }), None)
        .collect_to_val();

    let original_node2 = G::new_mut(Arc::clone(&db), &mut txn)
        .add_n("person", Some(props! { "entity_name" => "person2" }), None)
        .collect_to_val();

    txn.commit().unwrap();

    let mut txn = db.graph_env.write_txn().unwrap();
    let node1 = G::new(Arc::clone(&db), &txn)
        .n_from_type("person")
        .filter_ref(|val, txn| {
            if let Ok(val) = val {
                Ok(G::new_from(Arc::clone(&db), txn, val.clone())
                    .check_property("entity_name")
                    .map_value_or(false, |v| *v == "person1")?)
            } else {
                Ok(false)
            }
        })
        .collect_to::<Vec<_>>();

    let node2 = G::new(Arc::clone(&db), &txn)
        .n_from_type("person")
        .filter_ref(|val, txn| {
            if let Ok(val) = val {
                Ok(G::new_from(Arc::clone(&db), txn, val.clone())
                    .check_property("entity_name")
                    .map_value_or(false, |v| *v == "person2")?)
            } else {
                Ok(false)
            }
        })
        .collect_to::<Vec<_>>();

    assert_eq!(node1.len(), 1);
    assert_eq!(node1[0].id(), original_node1.id());
    assert_eq!(node2.len(), 1);
    assert_eq!(node2[0].id(), original_node2.id());

    let _e = G::new_mut(Arc::clone(&db), &mut txn)
        .add_e("knows", None, node1.id(), node2.id(), false, EdgeType::Node)
        .collect_to_val();

    txn.commit().unwrap();

    let txn = db.graph_env.read_txn().unwrap();
    let e = G::new(Arc::clone(&db), &txn)
        .e_from_type("knows")
        .collect_to::<Vec<_>>();
    assert_eq!(e.len(), 1);
    assert_eq!(e[0].id(), e.id());
    if let TraversalVal::Edge(e) = &e[0] {
        assert_eq!(e.from_node(), node1.id());
        assert_eq!(e.to_node(), node2.id());
    } else {
        panic!("e[0] is not an edge");
    }
}

#[test]
fn test_drop_traversal() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_val();

    for _ in 0..10 {
        let new_node = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_n("person", None, None)
            .collect_to_val();
        let _ = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_e(
                "knows",
                None,
                node.id(),
                new_node.id(),
                false,
                EdgeType::Node,
            )
            .collect_to_val();
    }

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .collect_to::<Vec<_>>();
    txn.commit().unwrap();
    println!("traversal: {traversal:?}");

    assert_eq!(traversal.len(), 11);

    let mut txn = storage.graph_env.write_txn().unwrap();

    Drop::drop_traversal(
        G::new(Arc::clone(&storage), &txn)
            .n_from_id(&node.id())
            .out("knows", &EdgeType::Node)
            .collect_to::<Vec<_>>(),
        Arc::clone(&storage),
        &mut txn,
    )
    .unwrap();

    Drop::drop_traversal(
        G::new(Arc::clone(&storage), &txn)
            .n_from_id(&node.id())
            .collect_to::<Vec<_>>(),
        Arc::clone(&storage),
        &mut txn,
    )
    .unwrap();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .collect_to::<Vec<_>>();
    txn.commit().unwrap();
    println!("traversal: {traversal:?}");

    assert_eq!(traversal.len(), 0);
}

#[test]
fn test_exclude_field_remapping() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let _node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n(
            "person",
            Some(props! { "text" => "test", "other" => "other" }),
            None,
        )
        .collect_to_val();

    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .collect_to::<Vec<_>>();

    let remapping_vals = RemappingMap::new();

    let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
    return_vals.insert(
        "files".to_string(),
        ReturnValue::from_traversal_value_array_with_mixin(
            G::new_from(Arc::clone(&storage), &txn, traversal.clone())
                .map_traversal(|item, _txn| {
                    println!("item: {item:?}");
                    exclude_field!(remapping_vals, item.clone(), "text")?;
                    Ok(item)
                })
                .collect_to::<Vec<_>>()
                .clone(),
            remapping_vals.borrow_mut(),
        ),
    );

    assert_eq!(return_vals.len(), 1);

    #[derive(Serialize, Deserialize)]
    struct Test {
        text: Option<String>,
        other: Option<String>,
    }
    #[derive(Serialize, Deserialize)]
    struct Response {
        files: Vec<Test>,
    }
    let value = sonic_rs::to_vec(&return_vals).unwrap();
    let value: Response = sonic_rs::from_slice(&value).unwrap();
    let value = value.files.first().unwrap();

    let expected = Test {
        text: None,
        other: Some("other".to_string()),
    };

    assert_eq!(value.text, expected.text);
    assert_eq!(value.other, expected.other);
}

#[test]
fn test_delete_vector() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let vector = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(&[1.0, 1.0, 1.0, 1.0, 1.0, 1.0], "vector", None)
        .collect_to_val();
    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_val();
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e("knows", None, node.id(), vector.id(), false, EdgeType::Vec)
        .collect_to_val();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .search_v::<fn(&HVector, &RoTxn) -> bool, usize>(
            &[1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            2000, "vector",
            None,
        )
        .collect_to::<Vec<_>>();

    txn.commit().unwrap();
    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), vector.id());

    let mut txn = storage.graph_env.write_txn().unwrap();

    Drop::drop_traversal(
        G::new(Arc::clone(&storage), &txn)
            .search_v::<fn(&HVector, &RoTxn) -> bool, _>(
                &[1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                2000, "vector",
                None,
            )
            .collect_to::<Vec<_>>(),
        Arc::clone(&storage),
        &mut txn,
    )
    .unwrap();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .search_v::<fn(&HVector, &RoTxn) -> bool, usize>(
            &[1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            2000, "vector",         
            None,
        )
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 0);

    let traversal = G::new(Arc::clone(&storage), &txn)
        .e_from_type("knows")
        .collect_to::<Vec<_>>();
    assert_eq!(traversal.len(), 0);
}

/*
QUERY updateEntity (entity_id: ID, name: String, name_embedding: [F64], group_id: String, summary: String, created_at: Date, labels: [String], attributes: String) =>
    entity <- N<Entity>(entity_id)::UPDATE({name: name, group_id: group_id, summary: summary, created_at: created_at, labels: labels, attributes: attributes})
    DROP N<Entity>(entity_id)::Out<Entity_to_Embedding>
    DROP N<Entity>(entity_id)::OutE<Entity_to_Embedding>
    embedding <- AddV<Entity_Embedding>(name_embedding, {name_embedding: name_embedding})
    edge <- AddE<Entity_to_Embedding>({group_id: group_id})::From(entity)::To(embedding)
    RETURN entity
*/
#[test]
fn test_drop_vectors_then_add_them_back() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let entity = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("Entity", Some(props! { "name" => "entity1" }), None)
        .collect_to_val();

    let embedding = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(&[1.0, 1.0, 1.0, 1.0, 1.0, 1.0], "vector", None)
        .collect_to_val();

    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "Entity_to_Embedding",
            Some(props! { "group_id" => "group1" }),
            entity.id(),
            embedding.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_val();

    txn.commit().unwrap();

    let mut txn = storage.graph_env.write_txn().unwrap();
    let entity = {
        let update_tr = G::new(Arc::clone(&storage), &txn)
            .n_from_id(&entity.id())
            .collect_to::<Vec<_>>();
        G::new_mut_from(Arc::clone(&storage), &mut txn, update_tr)
            .update(Some(props! { "name" => "entity2" }))
            .collect_to_obj()
    };
    Drop::<Vec<_>>::drop_traversal(
        G::new(Arc::clone(&storage), &txn)
            .n_from_id(&entity.id())
            .out("Entity_to_Embedding", &EdgeType::Vec)
            .collect_to::<Vec<_>>(),
        Arc::clone(&storage),
        &mut txn,
    )
    .unwrap();

    // check no vectors are left
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&entity.id())
        .out("Entity_to_Embedding", &EdgeType::Vec)
        .collect_to::<Vec<_>>();

    let out_edges = storage
        .out_edges_db
        .prefix_iter(&txn, &entity.id().to_be_bytes())
        .unwrap()
        .count();
    let in_edges = storage
        .in_edges_db
        .prefix_iter(&txn, &entity.id().to_be_bytes())
        .unwrap()
        .count();
    assert_eq!(out_edges, 0);
    assert_eq!(in_edges, 0);
    assert_eq!(traversal.len(), 0);

    let embedding = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(
            &[1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            "Entity_Embedding",
            Some(props! { "name_embedding" => [1.0, 1.0, 1.0, 1.0, 1.0, 1.0].to_vec() }),
        )
        .collect_to_obj();
    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "Entity_to_Embedding",
            Some(props! { "group_id" => "group2" }),
            entity.id(),
            embedding.id(),
            true,
            EdgeType::Node,
        )
        .collect_to_obj();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .search_v::<fn(&HVector, &RoTxn) -> bool, usize>(
            &[1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            2000,
            "vector",
            None,
        )
        .collect_to::<Vec<_>>();
    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), embedding.id());

    let traversal = G::new(Arc::clone(&storage), &txn)
        .e_from_type("Entity_to_Embedding")
        .collect_to::<Vec<_>>();
    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), edge.id());

    txn.commit().unwrap();

    let mut txn = storage.graph_env.write_txn().unwrap();

    let embedding = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(&[1.0, 1.0, 1.0, 1.0, 1.0, 1.0], "vector", None)
        .collect_to_val();

    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "Entity_to_Embedding",
            Some(props! { "group_id" => "group1" }),
            entity.id(),
            embedding.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_val();

    txn.commit().unwrap();

    let mut txn = storage.graph_env.write_txn().unwrap();
    let entity = {
        let update_tr = G::new(Arc::clone(&storage), &txn)
            .n_from_id(&entity.id())
            .collect_to::<Vec<_>>();
        G::new_mut_from(Arc::clone(&storage), &mut txn, update_tr)
            .update(Some(props! { "name" => "entity2" }))
            .collect_to_obj()
    };
    Drop::<Vec<_>>::drop_traversal(
        G::new(Arc::clone(&storage), &txn)
            .n_from_id(&entity.id())
            .out("Entity_to_Embedding", &EdgeType::Vec)
            .collect_to::<Vec<_>>(),
        Arc::clone(&storage),
        &mut txn,
    )
    .unwrap();

    let embedding = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(
            &[1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            "Entity_Embedding",
            Some(props! { "name_embedding" => [1.0, 1.0, 1.0, 1.0, 1.0, 1.0].to_vec() }),
        )
        .collect_to_obj();
    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "Entity_to_Embedding",
            Some(props! { "group_id" => "group2" }),
            entity.id(),
            embedding.id(),
            true,
            EdgeType::Node,
        )
        .collect_to_obj();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .search_v::<fn(&HVector, &RoTxn) -> bool, usize>(
            &[1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            2000,
            "vector",
            None,
        )
        .collect_to::<Vec<_>>();
    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), embedding.id());

    let traversal = G::new(Arc::clone(&storage), &txn)
        .e_from_type("Entity_to_Embedding")
        .collect_to::<Vec<_>>();
    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), edge.id());

    txn.commit().unwrap();
}

#[test]
fn test_node_deletion_in_existing_graph() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let source_node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_val();

    let mut other_nodes = Vec::new();

    for _ in 0..10 {
        let other_node = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_n("person", None, None)
            .collect_to_val();
        other_nodes.push(other_node);
    }

    for other_node in &other_nodes {
        let node_id = other_nodes[rand::rng().random_range(0..other_nodes.len())].id();
        let _ = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_e(
                "knows",
                None,
                node_id,
                other_node.id(),
                false,
                EdgeType::Node,
            )
            .collect_to_val();

        // 20 edges from source to other nodes
        let _ = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_e(
                "knows",
                None,
                source_node.id(),
                other_node.id(),
                false,
                EdgeType::Node,
            )
            .collect_to_val();
        let _ = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_e(
                "knows",
                None,
                other_node.id(),
                source_node.id(),
                false,
                EdgeType::Node,
            )
            .collect_to_val();
    }

    let edges = G::new(Arc::clone(&storage), &txn)
        .e_from_type("knows")
        .collect_to::<Vec<_>>();
    assert_eq!(edges.len(), 30);
    txn.commit().unwrap();

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::<Vec<_>>::drop_traversal(
        G::new(Arc::clone(&storage), &txn)
            .n_from_id(&source_node.id())
            .collect_to::<Vec<_>>(),
        Arc::clone(&storage),
        &mut txn,
    )
    .unwrap();

    let source_out_edges = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&source_node.id())
        .out("knows", &EdgeType::Node)
        .collect_to::<Vec<_>>();
    let source_in_edges = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&source_node.id())
        .in_("knows", &EdgeType::Node)
        .collect_to::<Vec<_>>();
    assert_eq!(source_out_edges.len(), 0);
    assert_eq!(source_in_edges.len(), 0);

    let other_edges = G::new(Arc::clone(&storage), &txn)
        .e_from_type("knows")
        .collect_to::<Vec<_>>();
    assert_eq!(other_edges.len(), 10);
    assert!(other_edges.iter().all(|edge| {
        if let TraversalVal::Edge(edge) = edge {
            edge.from_node != source_node.id() && edge.to_node != source_node.id()
        } else {
            false
        }
    }));

    txn.commit().unwrap();
}

#[test]
fn test_edge_deletion_in_existing_graph() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_val();

    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_val();

    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e("knows", None, node1.id(), node2.id(), false, EdgeType::Node)
        .collect_to_val();

    let edge2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e("knows", None, node2.id(), node1.id(), false, EdgeType::Node)
        .collect_to_val();

    txn.commit().unwrap();

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::<Vec<_>>::drop_traversal(
        G::new(Arc::clone(&storage), &txn)
            .e_from_id(&edge.id())
            .collect_to::<Vec<_>>(),
        Arc::clone(&storage),
        &mut txn,
    )
    .unwrap();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(Arc::clone(&storage), &txn)
        .e_from_type("knows")
        .collect_to::<Vec<_>>();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id(), edge2.id());

    txn.commit().unwrap();
}

#[test]
fn test_vector_deletion_in_existing_graph() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node: TraversalVal = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_val();

    let mut other_vectors = Vec::new();

    for _ in 0..10 {
        let other_vector = G::new_mut(Arc::clone(&storage), &mut txn)
            .insert_v::<fn(&HVector, &RoTxn) -> bool>(
                &[1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                "vector",
                None,
            )
            .collect_to_val();
        other_vectors.push(other_vector);
    }

    let vector = G::new_mut(Arc::clone(&storage), &mut txn)
            .insert_v::<fn(&HVector, &RoTxn) -> bool>(
                &[1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                "vector",
                None,
            )
            .collect_to_val();

    for other_vector in &other_vectors {
        let random_vector = other_vectors[rand::rng().random_range(0..other_vectors.len())].id();
        let _ = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_e(
                "knows",
                None,
                other_vector.id(),
                random_vector,
                false,
                EdgeType::Node,
            )
            .collect_to_val();
        let _ = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_e(
                "knows",
                None,
                node.id(),
                vector.id(),
                false,
                EdgeType::Vec,
            )
            .collect_to_val();
        let _ = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_e(
                "knows",
                None,
                vector.id(),
                node.id(),
                false,
                EdgeType::Node,
            )
            .collect_to_val();
    }

    let edges = G::new(Arc::clone(&storage), &txn)
        .e_from_type("knows")
        .collect_to::<Vec<_>>();
    assert_eq!(edges.len(), 30);
    txn.commit().unwrap();

    let mut txn = storage.graph_env.write_txn().unwrap();
    let vector_id = vector.id();

    Drop::<Vec<_>>::drop_traversal(
        G::new(Arc::clone(&storage), &txn)
            .n_from_id(&node.id())
            .out("knows", &EdgeType::Vec)
            .filter_ref(|val, _| {
                if let Ok(TraversalVal::Vector(vector)) = val {
                    Ok(*vector.id() == vector_id)
                } else {
                    Ok(false)
                }
            })
            .dedup()
            .collect_to::<Vec<_>>(),
        Arc::clone(&storage),
        &mut txn,
    )
    .unwrap();

    let source_out_edges = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node.id())
        .out("knows", &EdgeType::Vec)
        .collect_to::<Vec<_>>();
    let source_in_edges = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node.id())
        .in_("knows", &EdgeType::Vec)
        .collect_to::<Vec<_>>();
    assert_eq!(source_out_edges.len(), 0);
    assert_eq!(source_in_edges.len(), 0);

    let other_edges = G::new(Arc::clone(&storage), &txn)
        .e_from_type("knows")
        .collect_to::<Vec<_>>();
    assert_eq!(other_edges.len(), 10);
    assert!(other_edges.iter().all(|edge| {
        if let TraversalVal::Edge(edge) = edge {
            edge.from_node != node.id() && edge.to_node != node.id()
        } else {
            false
        }
    }));

    txn.commit().unwrap();
}
