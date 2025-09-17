use std::sync::Arc;

use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::{
                g::G,
                out::out::OutAdapter,
                source::{
                    add_e::{AddEAdapter, EdgeType},
                    add_n::AddNAdapter,
                    n_from_id::NFromIdAdapter,
                    n_from_type::NFromTypeAdapter,
                },
                util::{count::CountAdapter, filter_ref::FilterRefAdapter, range::RangeAdapter},
            },
            traversal_value::Traversable,
        },
    },
    props,
};

use rand::Rng;
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
fn test_count_filter_ref() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let mut nodes = Vec::new();
    for _ in 0..100 {
        let node = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_n("Country", Some(props!()), None)
            .collect_to_obj();
        nodes.push(node);
    }
    let mut num_countries = 0;
    for node in nodes {
        let rand_num = rand::rng().random_range(0..100);
        for _ in 0..rand_num {
            let city = G::new_mut(Arc::clone(&storage), &mut txn)
                .add_n("City", Some(props!()), None)
                .collect_to_obj();
            G::new_mut(Arc::clone(&storage), &mut txn)
                .add_e(
                    "Country_to_City",
                    Some(props!()),
                    node.id(),
                    city.id(),
                    false,
                    EdgeType::Node,
                )
                .collect_to::<Vec<_>>();
        }
        if rand_num > 10 {
            num_countries += 1;
        }
    }

    let count = G::new(Arc::clone(&storage), &txn)
        .n_from_type("Country")
        .filter_ref(|val, txn| {
            if let Ok(val) = val {
                Ok(G::new_from(Arc::clone(&storage), &txn, val.clone())
                    .out("Country_to_City", &EdgeType::Node)
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
        .collect_to::<Vec<_>>();

    println!("count: {count:?}, num_countries: {num_countries}");

    assert_eq!(count.len(), num_countries);
}
