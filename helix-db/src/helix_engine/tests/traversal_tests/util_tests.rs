use super::test_utils::props_option;
use std::sync::Arc;

use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::ops::{
            g::G,
            out::{out::OutAdapter, out_e::OutEdgesAdapter},
            source::{add_e::AddEAdapter, add_n::AddNAdapter, n_from_type::NFromTypeAdapter},
            util::{dedup::DedupAdapter, order::OrderByAdapter},
            vectors::{insert::InsertVAdapter, search::SearchVAdapter},
        },
        vector_core::vector::HVector,
    },
    props,
    protocol::value::Value,
};

use bumpalo::Bump;
use heed3::RoTxn;
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
fn test_order_node_by_asc() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 30 }), None)
        .collect_to_obj()
        .unwrap();

    let node2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 20 }), None)
        .collect_to_obj()
        .unwrap();

    let node3 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 10 }), None)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_type("person")
        .order_by_asc(|tv| tv.get_property("age").cloned().unwrap_or(Value::Empty))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(traversal.len(), 3);
    assert_eq!(traversal[0].id(), node3.id());
    assert_eq!(traversal[1].id(), node2.id());
    assert_eq!(traversal[2].id(), node.id());
}

#[test]
fn test_order_node_by_desc() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 30 }), None)
        .collect_to_obj()
        .unwrap();

    let node2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 20 }), None)
        .collect_to_obj()
        .unwrap();

    let node3 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 10 }), None)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_type("person")
        .order_by_desc(|tv| tv.get_property("age").cloned().unwrap_or(Value::Empty))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(traversal.len(), 3);
    assert_eq!(traversal[0].id(), node.id());
    assert_eq!(traversal[1].id(), node2.id());
    assert_eq!(traversal[2].id(), node3.id());
}

#[test]
fn test_order_edge_by_asc() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 30 }), None)
        .collect_to_obj()
        .unwrap();

    let node2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 20 }), None)
        .collect_to_obj()
        .unwrap();

    let node3 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 10 }), None)
        .collect_to_obj()
        .unwrap();

    let edge = G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "knows",
            props_option(&arena, props! { "since" => 2010 }),
            node.id(),
            node2.id(),
            false,
            false,
        )
        .collect_to_obj()
        .unwrap();

    let edge2 = G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "knows",
            props_option(&arena, props! { "since" => 2014 }),
            node3.id(),
            node2.id(),
            false,
            false,
        )
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_type("person")
        .out_e("knows")
        .order_by_asc(|tv| tv.get_property("since").cloned().unwrap_or(Value::Empty))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(traversal.len(), 2);
    assert_eq!(traversal[0].id(), edge.id());
    assert_eq!(traversal[1].id(), edge2.id());
}

#[test]
fn test_order_edge_by_desc() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 30 }), None)
        .collect_to_obj()
        .unwrap();

    let node2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 20 }), None)
        .collect_to_obj()
        .unwrap();

    let node3 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 10 }), None)
        .collect_to_obj()
        .unwrap();

    let edge = G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "knows",
            props_option(&arena, props! { "since" => 2010 }),
            node.id(),
            node2.id(),
            false,
            false,
        )
        .collect_to_obj()
        .unwrap();

    let edge2 = G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "knows",
            props_option(&arena, props! { "since" => 2014 }),
            node3.id(),
            node2.id(),
            false,
            false,
        )
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_type("person")
        .out_e("knows")
        .order_by_desc(|tv| tv.get_property("since").cloned().unwrap_or(Value::Empty))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(traversal.len(), 2);
    assert_eq!(traversal[0].id(), edge2.id());
    assert_eq!(traversal[1].id(), edge.id());
}

#[test]
fn test_order_vector_by_asc() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();
    type FnTy = fn(&HVector, &RoTxn) -> bool;

    let vector = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<FnTy>(
            &[1.0, 2.0, 3.0],
            "vector",
            props_option(&arena, props! { "age" => 30 }),
        )
        .collect_to_obj()
        .unwrap();

    let vector2 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<FnTy>(
            &[1.0, 2.0, 3.0],
            "vector",
            props_option(&arena, props! { "age" => 20 }),
        )
        .collect_to_obj()
        .unwrap();

    let vector3 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<FnTy>(
            &[1.0, 2.0, 3.0],
            "vector",
            props_option(&arena, props! { "age" => 10 }),
        )
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .search_v::<FnTy, _>(&[1.0, 2.0, 3.0], 10, "vector", None)
        .order_by_asc(|tv| tv.get_property("age").cloned().unwrap_or(Value::Empty))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(traversal.len(), 3);
    assert_eq!(traversal[0].id(), vector3.id());
    assert_eq!(traversal[1].id(), vector2.id());
    assert_eq!(traversal[2].id(), vector.id());
}

#[test]
fn test_order_vector_by_desc() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();
    type FnTy = fn(&HVector, &RoTxn) -> bool;

    let vector = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<FnTy>(
            &[1.0, 2.0, 3.0],
            "vector",
            props_option(&arena, props! { "age" => 30 }),
        )
        .collect_to_obj()
        .unwrap();

    let vector2 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<FnTy>(
            &[1.0, 2.0, 3.0],
            "vector",
            props_option(&arena, props! { "age" => 20 }),
        )
        .collect_to_obj()
        .unwrap();

    let vector3 = G::new_mut(&storage, &arena, &mut txn)
        .insert_v::<FnTy>(
            &[1.0, 2.0, 3.0],
            "vector",
            props_option(&arena, props! { "age" => 10 }),
        )
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .search_v::<FnTy, _>(&[1.0, 2.0, 3.0], 10, "vector", None)
        .order_by_desc(|tv| tv.get_property("age").cloned().unwrap_or(Value::Empty))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(traversal.len(), 3);
    assert_eq!(traversal[0].id(), vector.id());
    assert_eq!(traversal[1].id(), vector2.id());
    assert_eq!(traversal[2].id(), vector3.id());
}

#[test]
fn test_dedup() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 30 }), None)
        .collect_to_obj()
        .unwrap();

    let node2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 20 }), None)
        .collect_to_obj()
        .unwrap();

    let node3 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 10 }), None)
        .collect_to_obj()
        .unwrap();

    let _edge = G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "knows",
            props_option(&arena, props! { "since" => 2010 }),
            node.id(),
            node2.id(),
            false,
            false,
        )
        .collect_to_obj()
        .unwrap();

    let _edge2 = G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "knows",
            props_option(&arena, props! { "since" => 2010 }),
            node3.id(),
            node2.id(),
            false,
            false,
        )
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_type("person")
        .out_node("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(traversal.len(), 2);

    let traversal = G::new(&storage, &txn, &arena)
        .n_from_type("person")
        .out_node("knows")
        .dedup()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), node2.id());
}
