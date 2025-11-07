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
                util::paths::{PathAlgorithm, ShortestPathAdapter},
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
                .collect::<Result<Vec<_>,_>>().unwrap()[0]
                .id()
        })
        .collect();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, node_ids[0], node_ids[1], false)
        .collect_to_obj().unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, node_ids[1], node_ids[2], false)
        .collect_to_obj().unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, node_ids[2], node_ids[3], false)
        .collect_to_obj().unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let path = G::new(&storage, &txn, &arena)
        .n_from_id(&node_ids[0])
        .shortest_path(Some("knows"), None, Some(&node_ids[3]))
        .collect::<Result<Vec<_>,_>>().unwrap();
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
        .collect::<Result<Vec<_>,_>>().unwrap()[0]
        .id();
    let mid1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("city", props_option(&arena, props!("name" => "mid1")), None)
        .collect::<Result<Vec<_>,_>>().unwrap()[0]
        .id();
    let mid2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("city", props_option(&arena, props!("name" => "mid2")), None)
        .collect::<Result<Vec<_>,_>>().unwrap()[0]
        .id();
    let end = G::new_mut(&storage, &arena, &mut txn)
        .add_n("city", props_option(&arena, props!("name" => "end")), None)
        .collect::<Result<Vec<_>,_>>().unwrap()[0]
        .id();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("weight" => 100.0)),
            start,
            end,
            false,
        )
        .collect_to_obj().unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("weight" => 3.0)),
            start,
            mid1,
            false,
        )
        .collect_to_obj().unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("weight" => 3.0)),
            mid1,
            mid2,
            false,
        )
        .collect_to_obj().unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "road",
            props_option(&arena, props!("weight" => 4.0)),
            mid2,
            end,
            false,
        )
        .collect_to_obj().unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let bfs = G::new(&storage, &txn, &arena)
        .n_from_id(&start)
        .shortest_path_with_algorithm(Some("road"), None, Some(&end), PathAlgorithm::BFS)
        .collect::<Result<Vec<_>,_>>().unwrap();
    if let TraversalValue::Path((nodes, _)) = &bfs[0] {
        assert_eq!(nodes.len(), 2);
    } else {
        panic!("expected path");
    }

    let dijkstra = G::new(&storage, &txn, &arena)
        .n_from_id(&start)
        .shortest_path_with_algorithm(Some("road"), None, Some(&end), PathAlgorithm::Dijkstra)
        .collect::<Result<Vec<_>,_>>().unwrap();
    if let TraversalValue::Path((nodes, _)) = &dijkstra[0] {
        assert_eq!(nodes.len(), 4);
    } else {
        panic!("expected path");
    }
}
