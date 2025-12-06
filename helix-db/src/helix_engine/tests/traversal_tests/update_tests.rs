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
                source::{add_n::AddNAdapter, n_from_id::NFromIdAdapter},
                util::update::UpdateAdapter,
            },
            traversal_value::TraversalValue,
        },
    },
    props,
    protocol::value::Value,
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

#[test]
fn test_update_node() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let node = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "test")),
            None,
        )
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props!("name" => "test2")),
            None,
        )
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    let arena_read = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena_read)
        .n_from_id(&node.id())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    drop(txn);

    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();
    G::new_mut_from_iter(&storage, &mut txn, traversal.into_iter(), &arena)
        .update(&[("name", Value::from("john"))])
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    txn.commit().unwrap();

    let arena = Bump::new();
    let txn = storage.graph_env.read_txn().unwrap();
    let updated = G::new(&storage, &txn, &arena)
        .n_from_id(&node.id())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(updated.len(), 1);

    match &updated[0] {
        TraversalValue::Node(node) => {
            match node.properties.as_ref().unwrap().get("name").unwrap() {
                Value::String(name) => assert_eq!(name, "john"),
                other => panic!("unexpected value {other:?}"),
            }
        }
        other => panic!("unexpected traversal value: {other:?}"),
    }
}
