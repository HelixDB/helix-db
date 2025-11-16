use std::sync::Arc;
use super::test_utils::props_option;

use tempfile::TempDir;
use bumpalo::Bump;
use crate::{
    helix_engine::{
        storage_core::{HelixGraphStorage, txn::{ReadTransaction, WriteTransaction}},
        traversal_core::{
            ops::{
                g::G,
                out::out::OutAdapter,
                source::{
                    add_e::AddEAdapter,
                    add_n::AddNAdapter,
                    n_from_type::NFromTypeAdapter,
                },
                util::range::RangeAdapter,
            },
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
fn test_range_subset() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create multiple nodes
    let _: Vec<_> = (0..5)
        .map(|_| {
            G::new_mut(&storage, &arena, &mut txn)
                .add_n("person", None, None)
                .collect::<Result<Vec<_>,_>>().unwrap()
                .first()
                .unwrap();
        })
        .collect();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(&storage, &txn, &arena)
        .n_from_type("person") // Get all nodes
        .range(1, 3) // Take nodes at index 1 and 2
        .count();

    assert_eq!(count, 2);
}

#[test]
fn test_range_chaining() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create graph: (p1)-[knows]->(p2)-[knows]->(p3)-[knows]->(p4)-[knows]->(p5)
    let nodes: Vec<_> = (0..5)
        .map(|i| {
            G::new_mut(&storage, &arena, &mut txn)
                .add_n("person", props_option(&arena, props! { "name" => i }), None)
                .collect::<Result<Vec<_>,_>>().unwrap()
                .first()
                .unwrap()
                .clone()
        })
        .collect();

    // Create edges connecting nodes sequentially
    for i in 0..4 {
        G::new_mut(&storage, &arena, &mut txn)
            .add_edge(
                "knows",
                None,
                nodes[i].id(),
                nodes[i + 1].id(),
                false,
            )
            .collect::<Result<Vec<_>,_>>().unwrap();
    }

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "knows",
            None,
            nodes[4].id(),
            nodes[0].id(),
            false,
        )
        .collect::<Result<Vec<_>,_>>().unwrap();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(&storage, &txn, &arena)
        .n_from_type("person") // Get all nodes
        .range(0, 3) // Take first 3 nodes
        .out_node("knows") // Get their outgoing nodes
        .collect::<Result<Vec<_>,_>>().unwrap();

    assert_eq!(count.len(), 3);
}

#[test]
fn test_range_empty() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();

    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(&storage, &txn, &arena)
        .n_from_type("person") // Get all nodes
        .range(0, 0) // Take first 3 nodes
        .collect::<Result<Vec<_>,_>>().unwrap();

    assert_eq!(count.len(), 0);
}
