use std::sync::Arc;

use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::{
                g::G,
                out::{out::OutAdapter, out_e::OutEdgesAdapter},
                source::{
                    add_e::{AddEAdapter, EdgeType},
                    add_n::AddNAdapter,
                    n_from_type::NFromTypeAdapter,
                },
                util::{dedup::DedupAdapter, order::OrderByAdapter},
                vectors::{insert::InsertVAdapter, search::SearchVAdapter},
            },
            traversal_value::Traversable,
        },
        vector_core::{vector::HVector, vector_distance::SimilarityMethod},
    },
    props,
};

use heed3::RoTxn;
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
fn test_order_node_by_asc() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 30 }), None)
        .collect_to_obj();

    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 20 }), None)
        .collect_to_obj();

    let node3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 10 }), None)
        .collect_to_obj();

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
fn test_order_node_by_desc() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 30 }), None)
        .collect_to_obj();

    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 20 }), None)
        .collect_to_obj();

    let node3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 10 }), None)
        .collect_to_obj();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .order_by_desc("age")
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 3);
    assert_eq!(traversal[0].id(), node.id());
    assert_eq!(traversal[1].id(), node2.id());
    assert_eq!(traversal[2].id(), node3.id());
}

#[test]
fn test_order_edge_by_asc() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 30 }), None)
        .collect_to_obj();

    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 20 }), None)
        .collect_to_obj();

    let node3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 10 }), None)
        .collect_to_obj();

    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props! { "since" => 2010 }),
            node.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();

    let edge2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props! { "since" => 2014 }),
            node3.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .out_e("knows")
        .order_by_asc("since")
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 2);
    assert_eq!(traversal[0].id(), edge.id());
    assert_eq!(traversal[1].id(), edge2.id());
}

#[test]
fn test_order_edge_by_desc() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 30 }), None)
        .collect_to_obj();

    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 20 }), None)
        .collect_to_obj();

    let node3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 10 }), None)
        .collect_to_obj();

    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props! { "since" => 2010 }),
            node.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();

    let edge2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props! { "since" => 2014 }),
            node3.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .out_e("knows")
        .order_by_desc("since")
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 2);
    assert_eq!(traversal[0].id(), edge2.id());
    assert_eq!(traversal[1].id(), edge.id());
}

#[test]
fn test_order_vector_by_asc() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();
    type FnTy = fn(&HVector, &RoTxn) -> bool;

    let vector = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<FnTy>(
            &[1.0, 2.0, 3.0],
            "vector",
            Some(props! { "age" => 30 }),
            &SimilarityMethod::default(),
        )
        .collect_to_obj();

    let vector2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<FnTy>(
            &[1.0, 2.0, 3.0],
            "vector",
            Some(props! { "age" => 20 }),
            &SimilarityMethod::default(),
        )
        .collect_to_obj();

    let vector3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<FnTy>(
            &[1.0, 2.0, 3.0],
            "vector",
            Some(props! { "age" => 10 }),
            &SimilarityMethod::default(),
        )
        .collect_to_obj();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .search_v::<FnTy, _>(
            &[1.0, 2.0, 3.0],
            10,
            "vector",
            None,
            &SimilarityMethod::default(),
        )
        .order_by_asc("age")
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 3);
    assert_eq!(traversal[0].id(), vector3.id());
    assert_eq!(traversal[1].id(), vector2.id());
    assert_eq!(traversal[2].id(), vector.id());
}

#[test]
fn test_order_vector_by_desc() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();
    type FnTy = fn(&HVector, &RoTxn) -> bool;

    let vector = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<FnTy>(
            &[1.0, 2.0, 3.0],
            "vector",
            Some(props! { "age" => 30 }),
            &SimilarityMethod::default(),
        )
        .collect_to_obj();

    let vector2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<FnTy>(
            &[1.0, 2.0, 3.0],
            "vector",
            Some(props! { "age" => 20 }),
            &SimilarityMethod::default(),
        )
        .collect_to_obj();

    let vector3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<FnTy>(
            &[1.0, 2.0, 3.0],
            "vector",
            Some(props! { "age" => 10 }),
            &SimilarityMethod::default(),
        )
        .collect_to_obj();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .search_v::<FnTy, _>(
            &[1.0, 2.0, 3.0],
            10,
            "vector",
            None,
            &SimilarityMethod::default(),
        )
        .order_by_desc("age")
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 3);
    assert_eq!(traversal[0].id(), vector.id());
    assert_eq!(traversal[1].id(), vector2.id());
    assert_eq!(traversal[2].id(), vector3.id());
}

#[test]
fn test_dedup() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 30 }), None)
        .collect_to_obj();

    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 20 }), None)
        .collect_to_obj();

    let node3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 10 }), None)
        .collect_to_obj();

    let _edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props! { "since" => 2010 }),
            node.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();

    let _edge2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props! { "since" => 2010 }),
            node3.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .out("knows", &EdgeType::Node)
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 2);

    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_type("person")
        .out("knows", &EdgeType::Node)
        .dedup()
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), node2.id());
}
