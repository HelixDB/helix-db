use std::{sync::Arc};

use crate::{helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::{g::G, source::{add_e::{AddEAdapter, EdgeType}, add_n::AddNAdapter}, util::paths::{ShortestPathAdapter, PathAlgorithm}},
            traversal_value::{Traversable, TraversalValue},
        },
    }, props, utils::filterable::Filterable};

use tempfile::TempDir;

fn setup_test_db() -> (Arc<HelixGraphStorage>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let storage = HelixGraphStorage::new(
        db_path,
        crate::helix_engine::traversal_core::config::Config::default(),
        Default::default(),
    )
    .unwrap();
    (Arc::new(storage), temp_dir)
}

#[test]
fn test_shortest_path() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "node1")), None)
        .collect_to_obj();
    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "node2")), None)
        .collect_to_obj();
    let node3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "node3")), None)
        .collect_to_obj();
    let node4 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "node4")), None)
        .collect_to_obj();

    let edge1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!("name" => "edge1")),
            node1.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();
    let edge2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!("name" => "edge2")),
            node2.id(),
            node3.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();
    let edge3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!("name" => "edge3")),
            node3.id(),
            node4.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let path = G::new_from(Arc::clone(&storage), &txn, vec![node1.clone()])
        .shortest_path(Some("knows"), None, Some(&node4.id()))
        .collect_to::<Vec<_>>();
    assert_eq!(path.len(), 1);

    match path.first() {
        Some(TraversalValue::Path((nodes, edges))) => {
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

#[test]
fn test_dijkstra_shortest_path_weighted() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create a graph with weighted edges
    // Node1 -> Node2 (weight: 10)
    // Node1 -> Node3 (weight: 5)
    // Node3 -> Node2 (weight: 3)
    // Node2 -> Node4 (weight: 1)
    // Node3 -> Node4 (weight: 9)
    let node1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("city", Some(props!("name" => "city1")), None)
        .collect_to_obj();
    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("city", Some(props!("name" => "city2")), None)
        .collect_to_obj();
    let node3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("city", Some(props!("name" => "city3")), None)
        .collect_to_obj();
    let node4 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("city", Some(props!("name" => "city4")), None)
        .collect_to_obj();

    // Direct path: node1 -> node2 -> node4 (total weight: 11)
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "road",
            Some(props!("name" => "road1", "weight" => 10.0)),
            node1.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();
    
    // Alternative path: node1 -> node3 -> node2 -> node4 (total weight: 9)
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "road",
            Some(props!("name" => "road2", "weight" => 5.0)),
            node1.id(),
            node3.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();
    
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "road",
            Some(props!("name" => "road3", "weight" => 3.0)),
            node3.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();
    
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "road",
            Some(props!("name" => "road4", "weight" => 1.0)),
            node2.id(),
            node4.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();
    
    // Alternative direct path: node3 -> node4 (weight: 9)
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "road",
            Some(props!("name" => "road5", "weight" => 9.0)),
            node3.id(),
            node4.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    
    // Test Dijkstra's algorithm - it should find the path with minimum weight
    let node4_id = node4.id();
    let path = G::new_from(Arc::clone(&storage), &txn, vec![node1.clone()])
        .shortest_path_with_algorithm(Some("road"), None, Some(&node4_id), PathAlgorithm::Dijkstra)
        .collect_to::<Vec<_>>();
    
    assert_eq!(path.len(), 1);

    match path.first() {
        Some(TraversalValue::Path((nodes, edges))) => {
            // The shortest path by weight should be: node1 -> node3 -> node2 -> node4
            assert_eq!(nodes.len(), 4);
            assert_eq!(edges.len(), 3);
            assert_eq!(*nodes[0].check_property("name").unwrap(), "city1");
            assert_eq!(*nodes[1].check_property("name").unwrap(), "city3");
            assert_eq!(*nodes[2].check_property("name").unwrap(), "city2");
            assert_eq!(*nodes[3].check_property("name").unwrap(), "city4");
            assert_eq!(*edges[0].check_property("name").unwrap(), "road2");
            assert_eq!(*edges[1].check_property("name").unwrap(), "road3");
            assert_eq!(*edges[2].check_property("name").unwrap(), "road4");
        }
        _ => {
            panic!("Expected Path value");
        }
    }
}

#[test]
fn test_dijkstra_vs_bfs_comparison() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create a graph where BFS and Dijkstra give different results
    let node1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("city", Some(props!("name" => "start")), None)
        .collect_to_obj();
    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("city", Some(props!("name" => "middle1")), None)
        .collect_to_obj();
    let node3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("city", Some(props!("name" => "middle2")), None)
        .collect_to_obj();
    let node4 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("city", Some(props!("name" => "end")), None)
        .collect_to_obj();

    // Direct path: node1 -> node4 (weight: 100, hop count: 1)
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "road",
            Some(props!("weight" => 100.0)),
            node1.id(),
            node4.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();
    
    // Indirect path: node1 -> node2 -> node3 -> node4 (weight: 10 total, hop count: 3)
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "road",
            Some(props!("weight" => 3.0)),
            node1.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();
    
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "road",
            Some(props!("weight" => 3.0)),
            node2.id(),
            node3.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();
    
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "road",
            Some(props!("weight" => 4.0)),
            node3.id(),
            node4.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    
    // BFS should find the path with minimum hops (1 hop)
    let node4_id = node4.id();
    let bfs_path = G::new_from(Arc::clone(&storage), &txn, vec![node1.clone()])
        .shortest_path_with_algorithm(Some("road"), None, Some(&node4_id), PathAlgorithm::BFS)
        .collect_to::<Vec<_>>();
    
    match bfs_path.first() {
        Some(TraversalValue::Path((nodes, _))) => {
            assert_eq!(nodes.len(), 2); // Direct path
        }
        _ => panic!("Expected Path value"),
    }
    
    // Dijkstra should find the path with minimum weight (3 hops)
    let dijkstra_path = G::new_from(Arc::clone(&storage), &txn, vec![node1.clone()])
        .shortest_path_with_algorithm(Some("road"), None, Some(&node4_id), PathAlgorithm::Dijkstra)
        .collect_to::<Vec<_>>();
    
    match dijkstra_path.first() {
        Some(TraversalValue::Path((nodes, _))) => {
            assert_eq!(nodes.len(), 4); // Indirect path through 3 edges
        }
        _ => panic!("Expected Path value"),
    }
}

#[test] 
fn test_dijkstra_negative_weight_handling() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("city", Some(props!("name" => "city1")), None)
        .collect_to_obj();
    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("city", Some(props!("name" => "city2")), None)
        .collect_to_obj();

    // Create edge with negative weight
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "road",
            Some(props!("weight" => -5.0)),
            node1.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    
    // Dijkstra should return error for negative weights
    let node2_id = node2.id();
    let mut iter = G::new_from(Arc::clone(&storage), &txn, vec![node1.clone()])
        .shortest_path_with_algorithm(Some("road"), None, Some(&node2_id), PathAlgorithm::Dijkstra);
    
    match iter.next() {
        Some(Err(e)) => {
            assert!(e.to_string().contains("Negative edge weights are not supported"));
        }
        _ => panic!("Expected error for negative edge weights"),
    }
}
