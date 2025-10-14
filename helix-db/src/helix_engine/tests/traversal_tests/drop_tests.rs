use std::sync::Arc;

use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::{
                g::G,
                in_::{in_::InAdapter, in_e::InEdgesAdapter},
                out::{out::OutAdapter, out_e::OutEdgesAdapter},
                source::{
                    add_e::{AddEAdapter, EdgeType},
                    add_n::AddNAdapter,
                    e_from_id::EFromIdAdapter,
                    e_from_type::EFromTypeAdapter,
                    n_from_id::NFromIdAdapter,
                    n_from_type::NFromTypeAdapter,
                },
                util::{dedup::DedupAdapter, drop::Drop, filter_ref::FilterRefAdapter},
                vectors::insert::InsertVAdapter,
            },
            traversal_value::{Traversable, TraversalValue},
        },
        vector_core::{vector::HVector, VectorData},
    },
    props,
    utils::filterable::Filterable,
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
fn test_drop_edge() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to_obj();
    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to_obj();
    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            node1.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();

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
    assert_eq!(traversal, TraversalValue::Empty);

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
fn test_drop_node() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "test")), None)
        .collect_to_obj();
    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "test2")), None)
        .collect_to_obj();
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            node.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();
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

    assert_eq!(traversal, TraversalValue::Empty);
    assert_eq!(edges.len(), 0);
}

#[test]
fn test_drop_traversal() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_obj();

    for _ in 0..10 {
        let new_node = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_n("person", None, None)
            .collect_to_obj();
        let _ = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_e(
                "knows",
                None,
                node.id(),
                new_node.id(),
                false,
                EdgeType::Node,
            )
            .collect_to_obj();
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
fn test_node_deletion_in_existing_graph() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let source_node = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_obj();

    let mut other_nodes = Vec::new();

    for _ in 0..10 {
        let other_node = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_n("person", None, None)
            .collect_to_obj();
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
            .collect_to_obj();

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
            .collect_to_obj();
        let _ = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_e(
                "knows",
                None,
                other_node.id(),
                source_node.id(),
                false,
                EdgeType::Node,
            )
            .collect_to_obj();
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
        if let TraversalValue::Edge(edge) = edge {
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
        .collect_to_obj();

    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_obj();

    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e("knows", None, node1.id(), node2.id(), false, EdgeType::Node)
        .collect_to_obj();

    let edge2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e("knows", None, node2.id(), node1.id(), false, EdgeType::Node)
        .collect_to_obj();

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

    let node: TraversalValue = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", None, None)
        .collect_to_obj();

    let mut other_vectors = Vec::new();

    for _ in 0..10 {
        let other_vector = G::new_mut(Arc::clone(&storage), &mut txn)
            .insert_v::<fn(&HVector, &RoTxn) -> bool>(
                VectorData::F64(vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
                "vector",
                None,
            )
            .collect_to_obj();
        other_vectors.push(other_vector);
    }

    let vector = G::new_mut(Arc::clone(&storage), &mut txn)
        .insert_v::<fn(&HVector, &RoTxn) -> bool>(
            VectorData::F64(vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
            "vector",
            None,
        )
        .collect_to_obj();

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
            .collect_to_obj();
        let _ = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_e("knows", None, node.id(), vector.id(), false, EdgeType::Vec)
            .collect_to_obj();
        let _ = G::new_mut(Arc::clone(&storage), &mut txn)
            .add_e("knows", None, vector.id(), node.id(), false, EdgeType::Node)
            .collect_to_obj();
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
                if let Ok(TraversalValue::Vector(vector)) = val {
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
        if let TraversalValue::Edge(edge) = edge {
            edge.from_node != node.id() && edge.to_node != node.id()
        } else {
            false
        }
    }));

    txn.commit().unwrap();
}
