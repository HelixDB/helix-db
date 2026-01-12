use std::sync::Arc;

use bumpalo::Bump;
use rand::Rng;
use tempfile::TempDir;

use super::test_utils::props_option;
use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::{
                g::G,
                in_::{in_::InAdapter, in_e::InEdgesAdapter},
                out::{out::OutAdapter, out_e::OutEdgesAdapter},
                source::{
                    add_e::AddEAdapter, add_n::AddNAdapter, e_from_id::EFromIdAdapter,
                    e_from_type::EFromTypeAdapter, n_from_id::NFromIdAdapter,
                    n_from_type::NFromTypeAdapter,
                },
                util::{dedup::DedupAdapter, drop::Drop, filter_ref::FilterRefAdapter},
                vectors::insert::InsertVAdapter,
            },
            traversal_value::TraversalValue,
        },
        types::GraphError,
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

fn to_result_iter(
    values: Vec<TraversalValue>,
) -> impl Iterator<Item = Result<TraversalValue, GraphError>> {
    values.into_iter().map(Ok)
}

fn node_id(value: TraversalValue) -> u128 {
    match value {
        TraversalValue::Node(node) => node.id,
        _ => panic!("expected node"),
    }
}

fn edge_id(value: TraversalValue) -> u128 {
    match value {
        TraversalValue::Edge(edge) => edge.id,
        _ => panic!("expected edge"),
    }
}

#[test]
fn test_drop_edge() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1_id = node_id(
        G::new_mut(&storage, &arena, &mut txn)
            .add_n("person", None, None)
            .collect_to_obj()
            .unwrap(),
    );
    let node2_id = node_id(
        G::new_mut(&storage, &arena, &mut txn)
            .add_n("person", None, None)
            .collect_to_obj()
            .unwrap(),
    );
    let edge_id = edge_id(
        G::new_mut(&storage, &arena, &mut txn)
            .add_edge("knows", None, node1_id, node2_id, false, false)
            .collect_to_obj()
            .unwrap(),
    );
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .e_from_id(&edge_id)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::drop_traversal(to_result_iter(traversal), storage.as_ref(), &mut txn).unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .e_from_id(&edge_id)
        .collect_to_obj();
    assert!(matches!(traversal, Err(GraphError::EdgeNotFound)));

    let edges = G::new(&storage, &txn, &arena)
        .n_from_id(&node1_id)
        .out_e("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(edges.is_empty());

    let edges = G::new(&storage, &txn, &arena)
        .n_from_id(&node2_id)
        .in_e("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(edges.is_empty());
}

#[test]
fn test_drop_node() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1_id = node_id(
        G::new_mut(&storage, &arena, &mut txn)
            .add_n("person", props_option(&arena, props!("name" => "n1")), None)
            .collect_to_obj()
            .unwrap(),
    );
    let node2_id = node_id(
        G::new_mut(&storage, &arena, &mut txn)
            .add_n("person", props_option(&arena, props!("name" => "n2")), None)
            .collect_to_obj()
            .unwrap(),
    );
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("knows", None, node1_id, node2_id, false, false)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_id(&node1_id)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::drop_traversal(to_result_iter(traversal), storage.as_ref(), &mut txn).unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let node_val = G::new(&storage, &txn, &arena)
        .n_from_id(&node1_id)
        .collect_to_obj();
    assert!(matches!(node_val, Err(GraphError::NodeNotFound)));

    let edges = G::new(&storage, &txn, &arena)
        .n_from_id(&node2_id)
        .in_e("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    println!("edges: {:?}", edges);
    assert!(edges.is_empty());
}

#[test]
fn test_drop_traversal() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let origin_id = node_id(
        G::new_mut(&storage, &arena, &mut txn)
            .add_n("person", None, None)
            .collect_to_obj()
            .unwrap(),
    );

    let mut neighbor_ids = Vec::new();
    for _ in 0..10 {
        let neighbor_id = node_id(
            G::new_mut(&storage, &arena, &mut txn)
                .add_n("person", None, None)
                .collect_to_obj()
                .unwrap(),
        );
        G::new_mut(&storage, &arena, &mut txn)
            .add_edge("knows", None, origin_id, neighbor_id, false, false)
            .collect_to_obj()
            .unwrap();
        neighbor_ids.push(neighbor_id);
    }
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let neighbors = G::new(&storage, &txn, &arena)
        .n_from_id(&origin_id)
        .out_node("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let origin = G::new(&storage, &txn, &arena)
        .n_from_id(&origin_id)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::drop_traversal(to_result_iter(neighbors), storage.as_ref(), &mut txn).unwrap();
    Drop::drop_traversal(to_result_iter(origin), storage.as_ref(), &mut txn).unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let remaining = G::new(&storage, &txn, &arena)
        .n_from_type("person")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(remaining.is_empty());
    drop(txn);

    // sanity check: ensure ids removed
    assert!(neighbor_ids.len() == 10);
}

#[test]
fn test_node_deletion_in_existing_graph() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let source_id = node_id(
        G::new_mut(&storage, &arena, &mut txn)
            .add_n("person", None, None)
            .collect_to_obj()
            .unwrap(),
    );

    let mut others = Vec::new();
    for _ in 0..10 {
        let id = node_id(
            G::new_mut(&storage, &arena, &mut txn)
                .add_n("person", None, None)
                .collect_to_obj()
                .unwrap(),
        );
        others.push(id);
    }

    for &other in &others {
        let random = others[rand::rng().random_range(0..others.len())];
        G::new_mut(&storage, &arena, &mut txn)
            .add_edge("knows", None, random, other, false, false)
            .collect_to_obj()
            .unwrap();
        G::new_mut(&storage, &arena, &mut txn)
            .add_edge("knows", None, source_id, other, false, false)
            .collect_to_obj()
            .unwrap();
        G::new_mut(&storage, &arena, &mut txn)
            .add_edge("knows", None, other, source_id, false, false)
            .collect_to_obj()
            .unwrap();
    }
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let source = G::new(&storage, &txn, &arena)
        .n_from_id(&source_id)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::drop_traversal(to_result_iter(source), storage.as_ref(), &mut txn).unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let out_edges = G::new(&storage, &txn, &arena)
        .n_from_id(&source_id)
        .out_node("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let arena = Bump::new();
    let in_edges = G::new(&storage, &txn, &arena)
        .n_from_id(&source_id)
        .in_node("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(out_edges.is_empty());
    assert!(in_edges.is_empty());

    let arena = Bump::new();
    let remaining_edges = G::new(&storage, &txn, &arena)
        .e_from_type("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(remaining_edges.len(), others.len());
    assert!(remaining_edges.iter().all(|value| {
        if let TraversalValue::Edge(edge) = value {
            edge.from_node != source_id && edge.to_node != source_id
        } else {
            false
        }
    }));
}

#[test]
fn test_edge_deletion_in_existing_graph() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1_id = node_id(
        G::new_mut(&storage, &arena, &mut txn)
            .add_n("person", None, None)
            .collect_to_obj()
            .unwrap(),
    );
    let node2_id = node_id(
        G::new_mut(&storage, &arena, &mut txn)
            .add_n("person", None, None)
            .collect_to_obj()
            .unwrap(),
    );

    let edge1_id = edge_id(
        G::new_mut(&storage, &arena, &mut txn)
            .add_edge("knows", None, node1_id, node2_id, false, false)
            .collect_to_obj()
            .unwrap(),
    );
    let edge2_id = edge_id(
        G::new_mut(&storage, &arena, &mut txn)
            .add_edge("knows", None, node2_id, node1_id, false, false)
            .collect_to_obj()
            .unwrap(),
    );
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(&storage, &txn, &arena)
        .e_from_id(&edge1_id)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::drop_traversal(to_result_iter(edges), storage.as_ref(), &mut txn).unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(&storage, &txn, &arena)
        .e_from_type("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id(), edge2_id);
}

#[test]
fn test_vector_deletion_in_existing_graph() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node_id = node_id(
        G::new_mut(&storage, &arena, &mut txn)
            .add_n("person", None, None)
            .collect_to_obj()
            .unwrap(),
    );

    let mut vector_ids = Vec::new();
    for _ in 0..10 {
        let id = match G::new_mut(&storage, &arena, &mut txn)
            .insert_v(&[1.0, 1.0, 1.0, 1.0], "vector", None)
            .collect_to_obj()
            .unwrap()
        {
            TraversalValue::Vector(vector) => vector.id,
            other => panic!("unexpected value: {other:?}"),
        };
        vector_ids.push(id);
    }

    let target_vector_id = match G::new_mut(&storage, &arena, &mut txn)
        .insert_v(&[1.0, 1.0, 1.0, 1.0], "vector", None)
        .collect_to_obj()
        .unwrap()
    {
        TraversalValue::Vector(vector) => vector.id,
        other => panic!("unexpected value: {other:?}"),
    };

    for &other in &vector_ids {
        let random = vector_ids[rand::rng().random_range(0..vector_ids.len())];
        G::new_mut(&storage, &arena, &mut txn)
            .add_edge("knows", None, other, random, false, false)
            .collect_to_obj()
            .unwrap();
        G::new_mut(&storage, &arena, &mut txn)
            .add_edge("knows", None, node_id, target_vector_id, false, false)
            .collect_to_obj()
            .unwrap();
        G::new_mut(&storage, &arena, &mut txn)
            .add_edge("knows", None, target_vector_id, node_id, false, false)
            .collect_to_obj()
            .unwrap();
    }
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(&storage, &txn, &arena)
        .e_from_type("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(edges.len(), 30);
    drop(txn);

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_id(&node_id)
        .out_vec("knows", false)
        .filter_ref(|val, _| match val {
            Ok(TraversalValue::Vector(vector)) => Ok(vector.id == target_vector_id),

            Ok(_) => Ok(false),
            Err(err) => Err(GraphError::from(err.to_string())),
        })
        .dedup()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::drop_traversal(to_result_iter(traversal), storage.as_ref(), &mut txn).unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let out_edges = G::new(&storage, &txn, &arena)
        .n_from_id(&node_id)
        .out_vec("knows", false)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let arena = Bump::new();
    let in_edges = G::new(&storage, &txn, &arena)
        .n_from_id(&node_id)
        .in_vec("knows", false)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(out_edges.is_empty());
    assert!(in_edges.is_empty());

    let arena = Bump::new();
    let other_edges = G::new(&storage, &txn, &arena)
        .e_from_type("knows")
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(other_edges.len(), vector_ids.len());
    assert!(other_edges.iter().all(|value| {
        if let TraversalValue::Edge(edge) = value {
            edge.from_node != target_vector_id && edge.to_node != target_vector_id
        } else {
            false
        }
    }));
}
