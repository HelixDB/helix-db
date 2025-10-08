use std::sync::Arc;

use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::{
                g::G,
                in_::{in_e::InEdgesAdapter, to_v::ToVAdapter},
                out::{from_v::FromVAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
                source::{
                    add_e::{AddEAdapter, EdgeType},
                    add_n::AddNAdapter,
                    e_from_type::EFromTypeAdapter,
                    n_from_id::NFromIdAdapter,
                    n_from_type::NFromTypeAdapter,
                },
                util::{drop::Drop, order::OrderByAdapter, update::UpdateAdapter},
                vectors::{
                    brute_force_search::BruteForceSearchVAdapter, insert::InsertVAdapter,
                    search::SearchVAdapter,
                },
            },
            traversal_value::{Traversable, TraversalValue},
        },
        vector_core::{VectorData, vector::HVector},
    },
    props,
};

use heed3::RoTxn;
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
fn test_from_v() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_obj();

    let vector = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(
            VectorData::F64(vec![1.0, 2.0, 3.0]),
            "vector",
            None,
        )
        .collect_to_obj();

    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e("knows", None, vector.id(), node.id(), false, EdgeType::Vec)
        .collect_to_obj();

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
        .collect_to_obj();

    let vector = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(
            VectorData::F64(vec![1.0, 2.0, 3.0]),
            "vector",
            None,
        )
        .collect_to_obj();

    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e("knows", None, node.id(), vector.id(), false, EdgeType::Vec)
        .collect_to_obj();

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
        .collect_to_obj();

    let vectors = vec![
        vec![1.0, 2.0, 3.0],
        vec![4.0, 5.0, 6.0],
        vec![7.0, 8.0, 9.0],
    ];

    let mut vector_ids = Vec::new();
    for vector in vectors {
        let vector_id = G::new_mut(Arc::clone(&storage), &mut txn)
            .insert_v::<fn(&HVector, &RoTxn) -> bool>(VectorData::F64(vector), "vector", None)
            .collect_to_obj()
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
            .collect_to_obj()
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
        .collect_to_obj();

    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 20 }), None)
        .collect_to_obj();

    let node3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props! { "age" => 30 }), None)
        .collect_to_obj();

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
            .insert_v::<fn(&HVector, &RoTxn) -> bool>(
                VectorData::F64(random_vector),
                "vector",
                None,
            )
            .collect_to_obj();
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
            .insert_v::<fn(&HVector, &RoTxn) -> bool>(VectorData::F64(vector), "vector", None)
            .collect_to_obj();
        inserted_vectors.push(node.id());
        println!("inserted vector: {i:?}");
        i += 1;
    }

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .search_v::<fn(&HVector, &RoTxn) -> bool, _>(
            VectorData::F64(vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
            2000,
            "vector",
            None,
        )
        .collect_to::<Vec<_>>();
    // traversal.reverse();

    for vec in &traversal[0..10] {
        if let TraversalValue::Vector(vec) = vec {
            println!("vec {:?} {}", vec.get_data(), vec.get_distance());
            assert!(vec.get_distance() < 0.1);
        }
    }
}

#[test]
fn test_delete_vector() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let vector = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(
            VectorData::F64(vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
            "vector",
            None,
        )
        .collect_to_obj();
    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_obj();
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e("knows", None, node.id(), vector.id(), false, EdgeType::Vec)
        .collect_to_obj();

    txn.commit().unwrap();

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .search_v::<fn(&HVector, &RoTxn) -> bool, usize>(
            VectorData::F64(vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
            2000,
            "vector",
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
                VectorData::F64(vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
                2000,
                "vector",
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
            VectorData::F64(vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
            2000,
            "vector",
            None,
        )
        .collect_to::<Vec<_>>();

    println!();
    println!("traversal: {:?}", traversal);
    println!();

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
        .collect_to_obj();

    let embedding = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(
            VectorData::F64(vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
            "vector",
            None,
        )
        .collect_to_obj();

    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "Entity_to_Embedding",
            Some(props! { "group_id" => "group1" }),
            entity.id(),
            embedding.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();

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
            VectorData::F64(vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
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
            VectorData::F64(vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
            2000,
            "Entity_Embedding",
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
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(
            VectorData::F64(vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
            "vector",
            None,
        )
        .collect_to_obj();

    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "Entity_to_Embedding",
            Some(props! { "group_id" => "group1" }),
            entity.id(),
            embedding.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();

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
            VectorData::F64(vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
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
            VectorData::F64(vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
            2000,
            "Entity_Embedding",
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
