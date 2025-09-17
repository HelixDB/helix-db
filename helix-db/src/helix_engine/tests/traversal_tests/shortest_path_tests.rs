use std::{sync::Arc};

use crate::{helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::{g::G, source::{add_e::{AddEAdapter, EdgeType}, add_n::AddNAdapter}, util::paths::ShortestPathAdapter},
            traversal_value::{Traversable, TraversalValue},
        },
    }, props, utils::filterable::Filterable};

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
fn test_shortest_path() {
    let (storage, _temp_dir) = setup_test_db();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "node1")), None)
        .collect_to_obj();
    let node2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "node2")), None)
        .collect_to_obj();
    let node3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "node3")), None)
        .collect_to_obj();
    let node4 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_n("person", Some(props!("name" => "node4")), None)
        .collect_to_obj();

    let edge1 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!("name" => "edge1")),
            node1.id(),
            node2.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();
    let edge2 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!("name" => "edge2")),
            node2.id(),
            node3.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();
    let edge3 = G::new_mut(Arc::clone(&storage), &mut txn)
        .add_e(
            "knows",
            Some(props!("name" => "edge3")),
            node3.id(),
            node4.id(),
            false,
            EdgeType::Node,
        )
        .collect_to_obj();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let path = G::new_from(Arc::clone(&storage), &txn, vec![node1.clone()])
        .shortest_path(Some("knows"), None, Some(&node4.id()))
        .collect_to::<Vec<_>>();
    assert_eq!(path.len(), 1);

    match path.first() {
        Some(TraversalValue::Path((nodes, edges))) => {
            assert_eq!(nodes.len(), 4);
            assert_eq!(edges.len(), 3);
            assert_eq!(*nodes[0].check_property("name").unwrap(), "node1");
            assert_eq!(*nodes[1].check_property("name").unwrap(), "node2");
            assert_eq!(*nodes[2].check_property("name").unwrap(), "node3");
            assert_eq!(*nodes[3].check_property("name").unwrap(), "node4");
            assert_eq!(*edges[0].check_property("name").unwrap(), "edge1");
            assert_eq!(*edges[1].check_property("name").unwrap(), "edge2");
            assert_eq!(*edges[2].check_property("name").unwrap(), "edge3");
            assert_eq!(*nodes[0].id(), node1.id());
            assert_eq!(*nodes[1].id(), node2.id());
            assert_eq!(*nodes[2].id(), node3.id());
            assert_eq!(*nodes[3].id(), node4.id());
            assert_eq!(*edges[0].id(), edge1.id());
            assert_eq!(*edges[1].id(), edge2.id());
            assert_eq!(*edges[2].id(), edge3.id());
        }
        _ => {
            panic!("Expected Path value");
        }
    }
}
