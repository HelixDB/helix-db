use std::sync::Arc;

use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::{
                g::G,
                in_::{in_e::InEdgesAdapter, to_n::ToNAdapter},
                out::{from_n::FromNAdapter, out::OutAdapter, out_e::OutEdgesAdapter},
                source::{
                    add_e::{AddEAdapter, EdgeType},
                    add_n::AddNAdapter,
                    e_from_id::EFromIdAdapter,
                    e_from_type::EFromTypeAdapter,
                    n_from_id::NFromIdAdapter,
                },
                util::filter_ref::FilterRefAdapter,
                vectors::{insert::InsertVAdapter, search::SearchVAdapter},
            },
            traversal_value::{Traversable, TraversalValue},
        },
        vector_core::{vector::HVector, VectorData},
    },
    props,
    utils::filterable::Filterable,
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
fn test_add_e() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let node1 = node1.first().unwrap();
    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let node2 = node2.first().unwrap();

    txn.commit().unwrap();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let edges = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            node1.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .filter_map(|edge| edge.ok())
        .collect::<Vec<_>>();
    txn.commit().unwrap();
    // Check that the current step contains a single edge
    match edges.first() {
        Some(edge) => {
            assert_eq!(edge.label(), "knows");
            match edge {
                TraversalValue::Edge(edge) => {
                    assert_eq!(edge.from_node(), node1.id());
                    assert_eq!(edge.to_node(), node2.id());
                }
                _ => panic!("Expected Edge value"),
            }
        }
        None => panic!("Expected SingleEdge value"),
    }
}

#[test]
fn test_out_e() {
    let (storage, _temp_dir) = setup_test_db();

    // Create graph: (person1)-[knows]->(person2)

    let mut txn = storage.graph_env.write_txn().unwrap();
    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .filter_map(|node| node.ok())
        .collect::<Vec<_>>();
    let person1 = person1.first().unwrap();
    txn.commit().unwrap();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .filter_map(|node| node.ok())
        .collect::<Vec<_>>();
    let person2 = person2.first().unwrap();
    txn.commit().unwrap();
    let mut txn = storage.graph_env.write_txn().unwrap();
    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            false,
            EdgeType::Node,
        )
        .filter_map(|edge| edge.ok())
        .collect::<Vec<_>>();
    let edge = edge.first().unwrap();
    // println!("traversal edge: {:?}", edge);

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    println!("processing");
    let edges = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&person1.id())
        .out_e("knows")
        .collect_to::<Vec<_>>();
    println!("edges: {}", edges.len());

    // Check that current step is at the edge between person1 and person2
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id(), edge.id());
    assert_eq!(edges[0].label(), "knows");
}

#[test]
fn test_in_e() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph: (person1)-[knows]->(person2)
    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person1 = person1.first().unwrap();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person2 = person2.first().unwrap();
    println!("person1: {person1:?}");
    println!("person2: {person2:?}");

    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            true,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    let edge = edge.first().unwrap();
    println!("edge: {edge:?}");

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    let edges = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&person2.id())
        .in_e("knows")
        .collect_to::<Vec<_>>();

    // Check that current step is at the edge between person1 and person2
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id(), edge.id());
    assert_eq!(edges[0].label(), "knows");
}

#[test]
fn test_in_n() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("per son", Some(props!()), None)
        .collect_to_obj();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to_obj();

    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&edge.id())
        .to_n()
        .collect_to::<Vec<_>>();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person2.id());
}

#[test]
fn test_out_n() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to_obj();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to_obj();

    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&edge.id())
        .from_n()
        .collect_to::<Vec<_>>();
    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person1.id());
}

#[test]
fn test_edge_properties() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let node1 = node1.first().unwrap().clone();
    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to_obj();
    let props = props! { "since" => 2020, "date" => 1744965900, "name" => "hello"};
    let _ = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props.clone()),
            node1.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let edge = G::new_from(Arc::clone(&storage), &txn, vec![node1])
        .out_e("knows")
        .filter_ref(|val, _| {
            if let Ok(val) = val {
                println!("val: {:?}", val.check_property("date"));
                val.check_property("date").map_or(Ok(false), |v| {
                    println!("v: {v:?}");
                    println!("v: {:?}", *v == 1743290007);
                    Ok(*v >= 1743290007)
                })
            } else {
                Ok(false)
            }
        })
        .collect_to::<Vec<_>>();
    let edge = edge.first().unwrap();
    match edge {
        TraversalValue::Edge(edge) => {
            assert_eq!(
                edge.properties.clone().unwrap(),
                props.into_iter().collect()
            );
        }
        _ => {
            panic!("Expected Edge value");
        }
    }
}

#[test]
fn test_e_from_id() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph and edge
    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let edge = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person1.id(),
            person2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    let edge_id = edge.first().unwrap().id();
    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&edge_id)
        .collect_to::<Vec<_>>();

    // Check that the current step contains the correct single edge
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].id(), edge_id);
    assert_eq!(edges[0].label(), "knows");
    if let Some(TraversalValue::Edge(edge)) = edges.first() {
        assert_eq!(edge.from_node(), person1.id());
        assert_eq!(edge.to_node(), person2.id());
    } else {
        panic!("Expected Edge value");
    }
}

#[test]
fn test_e_from_id_nonexistent() {
    let (storage, _temp_dir) = setup_test_db();
    let txn = storage.graph_env.read_txn().unwrap();
    let edges = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&100)
        .collect_to::<Vec<_>>();
    assert!(edges.is_empty());
}

#[test]
fn test_e_from_id_chain_operations() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create test graph and edges
    let person1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();
    let person3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!()), None)
        .collect_to::<Vec<_>>();

    let edge1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!()),
            person2.id(),
            person1.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();
    G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "likes",
            Some(props!()),
            person2.id(),
            person3.id(),
            false,
            EdgeType::Node,
        )
        .collect_to::<Vec<_>>();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(Arc::clone(&storage), &txn)
        .e_from_id(&edge1.id())
        .from_n()
        .collect_to::<Vec<_>>();

    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id(), person2.id());
    assert_eq!(nodes[0].label(), "person");
}

#[test]
fn test_add_e_between_node_and_vector() {
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

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(Arc::clone(&storage), &txn)
        .n_from_id(&node.id())
        .out("knows", &EdgeType::Vec)
        .collect_to::<Vec<_>>();

    println!("traversal: {traversal:?}");

    println!(
        "edges: {:?}",
        G::new(Arc::clone(&storage), &txn)
            .e_from_type("knows")
            .collect_to::<Vec<_>>()
    );

    println!(
        "vectors: {:?}",
        G::new(Arc::clone(&storage), &txn)
            .search_v::<fn(&HVector, &RoTxn) -> bool, _>(
                VectorData::F64(vec![1.0, 2.0, 3.0]),
                10,
                "vector",
                None
            )
            .collect_to::<Vec<_>>()
    );

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), vector.id());
}
