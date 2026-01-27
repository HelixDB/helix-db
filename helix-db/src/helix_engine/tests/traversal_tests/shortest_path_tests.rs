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
                source::{add_e::AddEAdapter, add_n::AddNAdapter, n_from_id::NFromIdAdapter},
                util::paths::{PathAlgorithm, ShortestPathAdapter, default_weight_fn},
            },
            traversal_value::TraversalValue,
        },
    },
    props,
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

#[test]
fn test_shortest_path_simple_chain() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node_ids: Vec<_> = ["A", "B", "C", "D"]
        .into_iter()
        .map(|name| {
            G::new_mut(&storage, &arena, &mut txn)
                .add_n("person", props_option(&arena, props!("name" => name)), None)
                .collect::<Result<Vec<_>, _>>()
                .unwrap()[0]
                .id()
        })
        .collect();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, node_ids[0], node_ids[1], false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, node_ids[1], node_ids[2], false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, node_ids[2], node_ids[3], false, false)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let path = G::new(&storage, &txn, &arena)
        .n_from_id(&node_ids[0])
        .shortest_path(Some("knows"), None, Some(&node_ids[3]))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(path.len(), 1);
    if let TraversalValue::Path((nodes, edges)) = &path[0] {
        assert_eq!(nodes.len(), 4);
        assert_eq!(edges.len(), 3);
    } else {
        panic!("expected path");
    }
}

#[test]
fn test_dijkstra_shortest_path_weighted_graph() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let start = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "start")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let mid1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("city", props_option(&arena, props!("name" => "mid1")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let mid2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("city", props_option(&arena, props!("name" => "mid2")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let end = G::new_mut(&storage, &arena, &mut txn)
        .add_n("city", props_option(&arena, props!("name" => "end")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("weight" => 100.0)),
            start,
            end,
            false,
            false,
        )
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("weight" => 3.0)),
            start,
            mid1,
            false,
            false,
        )
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("weight" => 3.0)),
            mid1,
            mid2,
            false,
            false,
        )
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("weight" => 4.0)),
            mid2,
            end,
            false,
            false,
        )
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let bfs = G::new(&storage, &txn, &arena)
        .n_from_id(&start)
        .shortest_path_with_algorithm(
            Some("road"),
            None,
            Some(&end),
            PathAlgorithm::BFS,
            default_weight_fn,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    if let TraversalValue::Path((nodes, _)) = &bfs[0] {
        assert_eq!(nodes.len(), 2);
    } else {
        panic!("expected path");
    }

    let dijkstra = G::new(&storage, &txn, &arena)
        .n_from_id(&start)
        .shortest_path_with_algorithm(
            Some("road"),
            None,
            Some(&end),
            PathAlgorithm::Dijkstra,
            default_weight_fn,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    if let TraversalValue::Path((nodes, _)) = &dijkstra[0] {
        assert_eq!(nodes.len(), 4);
    } else {
        panic!("expected path");
    }
}

#[test]
fn test_dijkstra_custom_weight_function() {
    use crate::protocol::value::Value;

    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let start = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "start")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let mid = G::new_mut(&storage, &arena, &mut txn)
        .add_n("city", props_option(&arena, props!("name" => "mid")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let end = G::new_mut(&storage, &arena, &mut txn)
        .add_n("city", props_option(&arena, props!("name" => "end")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Direct route with distance 10
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("distance" => 10.0)),
            start,
            end,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Route through mid with distance 3 + 3 = 6
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("distance" => 3.0)),
            start,
            mid,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("distance" => 3.0)),
            mid,
            end,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    txn.commit().unwrap();

    // Test with custom weight function using "distance" property
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let custom_weight = |edge: &crate::utils::items::Edge,
                         _src: &crate::utils::items::Node,
                         _dst: &crate::utils::items::Node| {
        edge.properties
            .as_ref()
            .and_then(|props| props.get("distance"))
            .and_then(|v| match v {
                Value::F64(f) => Some(*f),
                Value::F32(f) => Some(*f as f64),
                _ => None,
            })
            .ok_or_else(|| {
                crate::helix_engine::types::GraphError::TraversalError(
                    "Missing distance property".to_string(),
                )
            })
    };

    let path = G::new(&storage, &txn, &arena)
        .n_from_id(&start)
        .shortest_path_with_algorithm(
            Some("road"),
            None,
            Some(&end),
            PathAlgorithm::Dijkstra,
            custom_weight,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    if let TraversalValue::Path((nodes, edges)) = &path[0] {
        // Should take the route through mid (3 nodes, 2 edges) because 3+3 < 10
        assert_eq!(nodes.len(), 3, "Expected path through mid node");
        assert_eq!(edges.len(), 2, "Expected 2 edges in path");
    } else {
        panic!("expected path");
    }
}

#[test]
fn test_dijkstra_multi_context_weight() {
    use crate::protocol::value::Value;

    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create nodes with traffic_factor property
    let start = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "start", "traffic_factor" => 1.0)),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let mid1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "mid1", "traffic_factor" => 2.0)),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let mid2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "mid2", "traffic_factor" => 1.1)),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let end = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "end", "traffic_factor" => 1.0)),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Route through mid1: distance 5, source traffic 1.0 -> weight = 5 * 1.0 = 5
    // Then mid1 to end: distance 5, source traffic 2.0 -> weight = 5 * 2.0 = 10
    // Total: 15
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("distance" => 5.0)),
            start,
            mid1,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("distance" => 5.0)),
            mid1,
            end,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Route through mid2: distance 6, source traffic 1.0 -> weight = 6 * 1.0 = 6
    // Then mid2 to end: distance 6, source traffic 1.1 -> weight = 6 * 1.1 = 6.6
    // Total: 12.6 (should be chosen)
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("distance" => 6.0)),
            start,
            mid2,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("distance" => 6.0)),
            mid2,
            end,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    txn.commit().unwrap();

    // Test with multi-context weight: distance * source_traffic_factor
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let multi_context_weight = |edge: &crate::utils::items::Edge,
                                src: &crate::utils::items::Node,
                                _dst: &crate::utils::items::Node| {
        let distance = edge
            .properties
            .as_ref()
            .and_then(|props| props.get("distance"))
            .and_then(|v| match v {
                Value::F64(f) => Some(*f),
                Value::F32(f) => Some(*f as f64),
                _ => None,
            })
            .ok_or_else(|| {
                crate::helix_engine::types::GraphError::TraversalError(
                    "Missing distance".to_string(),
                )
            })?;

        let traffic = src
            .properties
            .as_ref()
            .and_then(|props| props.get("traffic_factor"))
            .and_then(|v| match v {
                Value::F64(f) => Some(*f),
                Value::F32(f) => Some(*f as f64),
                _ => None,
            })
            .ok_or_else(|| {
                crate::helix_engine::types::GraphError::TraversalError(
                    "Missing traffic_factor".to_string(),
                )
            })?;

        Ok(distance * traffic)
    };

    let path = G::new(&storage, &txn, &arena)
        .n_from_id(&start)
        .shortest_path_with_algorithm(
            Some("road"),
            None,
            Some(&end),
            PathAlgorithm::Dijkstra,
            multi_context_weight,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    if let TraversalValue::Path((nodes, _edges)) = &path[0] {
        // Should take route through mid2 (lower total weight: 12.6 < 15)
        assert_eq!(nodes.len(), 3);
        // Verify it's mid2 by checking the middle node
        if let Some(mid_node) = nodes.get(1) {
            let mid_name = mid_node
                .properties
                .as_ref()
                .and_then(|p| p.get("name"))
                .and_then(|v| match v {
                    Value::String(s) => Some(s.as_str()),
                    _ => None,
                });
            assert_eq!(mid_name, Some("mid2"), "Expected path through mid2 node");
        }
    } else {
        panic!("expected path");
    }
}

#[test]
fn test_default_weight_fn_unit() {
    use crate::{
        protocol::value::Value,
        utils::items::{Edge, Node},
        utils::properties::ImmutablePropertiesMap,
    };
    use bumpalo::Bump;

    let arena = Bump::new();

    // Create edge with weight property
    let props_data = [("weight", Value::F64(5.5))];
    let props_map = ImmutablePropertiesMap::new(props_data.len(), props_data.into_iter(), &arena);

    let edge = Edge {
        id: 1,
        label: "test",
        version: 0,
        from_node: 1,
        to_node: 2,
        properties: Some(props_map),
    };

    // Create dummy nodes
    let node1 = Node {
        id: 1,
        label: "test",
        version: 0,
        properties: None,
    };
    let node2 = Node {
        id: 2,
        label: "test",
        version: 0,
        properties: None,
    };

    // Test default_weight_fn returns the weight property
    let weight = default_weight_fn(&edge, &node1, &node2).unwrap();
    assert_eq!(weight, 5.5);

    // Test default weight when property is missing
    let edge_no_weight = Edge {
        id: 2,
        label: "test",
        version: 0,
        from_node: 1,
        to_node: 2,
        properties: None,
    };
    let default = default_weight_fn(&edge_no_weight, &node1, &node2).unwrap();
    assert_eq!(default, 1.0);
}

#[test]
fn test_shortest_path_with_constant_weight() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let start = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "node",
            props_option(&arena, props!("name" => "start")),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let mid = G::new_mut(&storage, &arena, &mut txn)
        .add_n("node", props_option(&arena, props!("name" => "mid")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();
    let end = G::new_mut(&storage, &arena, &mut txn)
        .add_n("node", props_option(&arena, props!("name" => "end")), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Direct route
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("link", None, start, end, false, false)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Route through mid (2 hops)
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("link", None, start, mid, false, false)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("link", None, mid, end, false, false)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    txn.commit().unwrap();

    // Test with constant weight (equivalent to counting hops)
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let constant_weight = |_edge: &crate::utils::items::Edge,
                           _src: &crate::utils::items::Node,
                           _dst: &crate::utils::items::Node| { Ok(1.0) };

    let path = G::new(&storage, &txn, &arena)
        .n_from_id(&start)
        .shortest_path_with_algorithm(
            Some("link"),
            None,
            Some(&end),
            PathAlgorithm::Dijkstra,
            constant_weight,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    if let TraversalValue::Path((nodes, edges)) = &path[0] {
        // Should take direct route (2 nodes, 1 edge)
        assert_eq!(nodes.len(), 2);
        assert_eq!(edges.len(), 1);
    } else {
        panic!("expected path");
    }
}

#[test]
fn test_astar_with_property_heuristic() {
    use crate::helix_engine::traversal_core::ops::util::paths::property_heuristic;

    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create a graph where A* should find the optimal path
    // Graph: start -> mid1 -> goal
    //        start -> mid2 -> goal
    // Weights: start->mid1: 5.0, mid1->goal: 5.0 (total: 10.0)
    //          start->mid2: 1.0, mid2->goal: 15.0 (total: 16.0)
    // Heuristics: start: h=10, mid1: h=5, mid2: h=5, goal: h=0

    let start = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "start", "h" => 10.0)),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let mid1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "mid1", "h" => 5.0)),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let mid2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "mid2", "h" => 5.0)),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let goal = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "goal", "h" => 0.0)),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    // Add edges with weights
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("weight" => 5.0)),
            start,
            mid1,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("weight" => 5.0)),
            mid1,
            goal,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("weight" => 1.0)),
            start,
            mid2,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("weight" => 15.0)),
            mid2,
            goal,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    txn.commit().unwrap();

    // Run A* with property-based heuristic
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let heuristic = |node: &crate::utils::items::Node| property_heuristic(node, "h");

    let path = G::new(&storage, &txn, &arena)
        .n_from_id(&start)
        .shortest_path_astar(
            Some("road"),
            None,
            Some(&goal),
            default_weight_fn,
            heuristic,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(path.len(), 1);
    if let TraversalValue::Path((nodes, edges)) = &path[0] {
        // A* should find the path: start -> mid1 -> goal (total cost 10.0)
        assert_eq!(nodes.len(), 3);
        assert_eq!(edges.len(), 2);

        // Verify the nodes in the path
        assert_eq!(nodes[0].id, start);
        assert_eq!(nodes[1].id, mid1);
        assert_eq!(nodes[2].id, goal);
    } else {
        panic!("expected path");
    }
}

#[test]
fn test_astar_matches_dijkstra_with_zero_heuristic() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let start = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "start", "h" => 0.0)),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let mid = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "mid", "h" => 0.0)),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let end = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "end", "h" => 0.0)),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("weight" => 5.0)),
            start,
            mid,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("weight" => 3.0)),
            mid,
            end,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    txn.commit().unwrap();

    // Test that A* with zero heuristic behaves like Dijkstra
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let zero_heuristic = |_node: &crate::utils::items::Node| Ok(0.0);

    let astar_path = G::new(&storage, &txn, &arena)
        .n_from_id(&start)
        .shortest_path_astar(
            Some("road"),
            None,
            Some(&end),
            default_weight_fn,
            zero_heuristic,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let dijkstra_path = G::new(&storage, &txn, &arena)
        .n_from_id(&start)
        .shortest_path_with_algorithm(
            Some("road"),
            None,
            Some(&end),
            PathAlgorithm::Dijkstra,
            default_weight_fn,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Both should find the same path
    assert_eq!(astar_path.len(), 1);
    assert_eq!(dijkstra_path.len(), 1);

    if let (
        TraversalValue::Path((astar_nodes, astar_edges)),
        TraversalValue::Path((dijkstra_nodes, dijkstra_edges)),
    ) = (&astar_path[0], &dijkstra_path[0])
    {
        assert_eq!(astar_nodes.len(), dijkstra_nodes.len());
        assert_eq!(astar_edges.len(), dijkstra_edges.len());
    } else {
        panic!("expected paths");
    }
}

#[test]
fn test_astar_custom_weight_and_heuristic() {
    use crate::helix_engine::traversal_core::ops::util::paths::property_heuristic;

    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let start = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "start", "h" => 10.0)),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    let end = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "city",
            props_option(&arena, props!("name" => "end", "h" => 0.0)),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("distance" => 100.0, "traffic" => 0.5)),
            start,
            end,
            false,
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    txn.commit().unwrap();

    // Test A* with custom weight function (distance * traffic) and property heuristic
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();

    let custom_weight = |edge: &crate::utils::items::Edge,
                         _src: &crate::utils::items::Node,
                         _dst: &crate::utils::items::Node| {
        let distance = edge
            .get_property("distance")
            .ok_or(crate::helix_engine::types::GraphError::New(
                "distance property not found".to_string(),
            ))?
            .as_f64();
        let traffic = edge
            .get_property("traffic")
            .ok_or(crate::helix_engine::types::GraphError::New(
                "traffic property not found".to_string(),
            ))?
            .as_f64();
        Ok(distance * traffic)
    };

    let heuristic = |node: &crate::utils::items::Node| property_heuristic(node, "h");

    let path = G::new(&storage, &txn, &arena)
        .n_from_id(&start)
        .shortest_path_astar(Some("road"), None, Some(&end), custom_weight, heuristic)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(path.len(), 1);
    if let TraversalValue::Path((nodes, edges)) = &path[0] {
        assert_eq!(nodes.len(), 2);
        assert_eq!(edges.len(), 1);
    } else {
        panic!("expected path");
    }
}
