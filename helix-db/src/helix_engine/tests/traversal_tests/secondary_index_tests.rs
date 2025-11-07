use std::sync::Arc;

use bumpalo::Bump;
use tempfile::TempDir;

use super::test_utils::props_option;
use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::{
            ops::{
                g::G,
                source::{
                    add_n::AddNAdapter, n_from_id::NFromIdAdapter, n_from_index::NFromIndexAdapter,
                },
                util::{drop::Drop, update::UpdateAdapter},
            },
            traversal_value::TraversalValue,
        },
        types::GraphError,
    },
    props,
    protocol::value::Value,
};

fn setup_indexed_db() -> (TempDir, Arc<HelixGraphStorage>) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_str().unwrap();
    let mut config = crate::helix_engine::traversal_core::config::Config::default();
    config.graph_config.as_mut().unwrap().secondary_indices = Some(vec!["name".to_string()]);
    let storage = HelixGraphStorage::new(db_path, config, Default::default()).unwrap();
    (temp_dir, Arc::new(storage))
}

fn to_result_iter(
    values: Vec<TraversalValue>,
) -> impl Iterator<Item = Result<TraversalValue, GraphError>> {
    values.into_iter().map(Ok)
}

#[test]
fn test_delete_node_with_secondary_index() {
    let (_temp_dir, storage) = setup_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj().unwrap();
    let node_id = node.id();

    G::new_mut_from_iter(&storage, &mut txn, std::iter::once(node), &arena)
        .update(&[("name", Value::from("Jane"))])
        .collect_to_obj().unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let jane_nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"Jane".to_string())
        .collect::<Result<Vec<_>,_>>().unwrap();
    assert_eq!(jane_nodes.len(), 1);
    assert_eq!(jane_nodes[0].id(), node_id);

    let john_nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"John".to_string())
        .collect::<Result<Vec<_>,_>>().unwrap();
    assert!(john_nodes.is_empty());
    drop(txn);

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_id(&node_id)
        .collect::<Result<Vec<_>,_>>().unwrap();
    drop(txn);

    let mut txn = storage.graph_env.write_txn().unwrap();
    Drop::drop_traversal(to_result_iter(traversal), storage.as_ref(), &mut txn).unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let node = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"Jane".to_string())
        .collect::<Result<Vec<_>,_>>().unwrap();
    assert!(node.is_empty());
}

#[test]
fn test_update_of_secondary_indices() {
    let (_temp_dir, storage) = setup_indexed_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "John" }),
            Some(&["name"]),
        )
        .collect_to_obj().unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();
    G::new_mut_from_iter(&storage, &mut txn, std::iter::once(node), &arena)
        .update(&[("name", Value::from("Jane"))])
        .collect_to_obj().unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"Jane".to_string())
        .collect::<Result<Vec<_>,_>>().unwrap();
    assert_eq!(nodes.len(), 1);
    if let TraversalValue::Node(node) = &nodes[0] {
        match node.properties.as_ref().unwrap().get("name").unwrap() {
            Value::String(name) => assert_eq!(name, "Jane"),
            other => panic!("unexpected value: {other:?}"),
        }
    } else {
        panic!("expected node");
    }

    let john_nodes = G::new(&storage, &txn, &arena)
        .n_from_index("person", "name", &"John".to_string())
        .collect::<Result<Vec<_>,_>>().unwrap();
    assert!(john_nodes.is_empty());
}
