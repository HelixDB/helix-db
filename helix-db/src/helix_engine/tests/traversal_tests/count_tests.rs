use std::{sync::Arc, time::Duration};

use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::{
                g::G,
                out::out::OutAdapter,
                source::{
                    add_e::AddEAdapter,
                    add_n::AddNAdapter,
                    n_from_id::NFromIdAdapter,
                    n_from_type::NFromTypeAdapter,
                },
                util::{count::CountAdapter, filter_ref::FilterRefAdapter, range::RangeAdapter},
            },
        },
    },
};

use rand::Rng;
use tempfile::TempDir;
use bumpalo::Bump;
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
fn test_count_single_node() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let person = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>,_>>().unwrap();
    let person = person.first().unwrap();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(&storage, &txn, &arena)
        .n_from_id(&person.id())
        .count();

    assert_eq!(count, 1);
}

#[test]
fn test_count_node_array() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let _ = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>,_>>().unwrap();
    let _ = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>,_>>().unwrap();
    let _ = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect::<Result<Vec<_>,_>>().unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(&storage, &txn, &arena)
        .n_from_type("person") // Get all nodes
        .count();
    assert_eq!(count, 3);
}

#[test]
fn test_count_mixed_steps() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create a graph with multiple paths
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
        .add_edge(
            "knows",
            None,
            person1.id(),
            person2.id(),
            false,
        )
        .collect::<Result<Vec<_>,_>>().unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "knows",
            None,
            person1.id(),
            person3.id(),
            false,
        )
        .collect::<Result<Vec<_>,_>>().unwrap();
    txn.commit().unwrap();
    println!("person1: {person1:?},\nperson2: {person2:?},\nperson3: {person3:?}");

    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(&storage, &txn, &arena)
        .n_from_id(&person1.id())
        .out_node("knows")
        .count();

    assert_eq!(count, 2);
}

#[test]
fn test_count_empty() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let count = G::new(&storage, &txn, &arena)
        .n_from_type("person") // Get all nodes
        .range(0, 0) // Take first 3 nodes
        .count();

    assert_eq!(count, 0);
}

#[test]
fn test_count_filter_ref() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let mut nodes = Vec::new();
    for _ in 0..100 {
        let node = G::new_mut(&storage, &arena, &mut txn)
            .add_n("Country", None, None)
            .collect_to_obj().unwrap();
        nodes.push(node);
    }
    let mut num_countries = 0;
    for node in nodes {
        let rand_num = rand::rng().random_range(0..100);
        for _ in 0..rand_num {
            let city = G::new_mut(&storage, &arena, &mut txn)
                .add_n("City", None, None)
                .collect_to_obj().unwrap();
            G::new_mut(&storage, &arena, &mut txn)
                .add_edge(
                    "Country_to_City",
                    None,
                    node.id(),
                    city.id(),
                    false,
                )
                .collect::<Result<Vec<_>,_>>().unwrap();
            // sleep for one microsecond
            std::thread::sleep(Duration::from_micros(1));
        }
        if rand_num > 10 {
            num_countries += 1;
        }
    }

    let count = G::new(&storage, &txn, &arena)
        .n_from_type("Country")
        .filter_ref(|val, txn| {
            if let Ok(val) = val {
                let val_id = val.id();
                Ok(G::new(&storage, &txn, &arena)
                    .n_from_id(&val_id)
                    .out_node("Country_to_City")
                    .count_to_val()
                    .map_value_or(false, |v| {
                        println!(
                            "v: {v:?}, res: {:?}",
                            *v > 10.clone()
                        );
                        *v > 10.clone()
                    })?)
            } else {
                Ok(false)
            }
        })
        .collect::<Result<Vec<_>,_>>().unwrap();

    println!("count: {count:?}, num_countries: {num_countries}");

    assert_eq!(count.len(), num_countries);
}
