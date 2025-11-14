use super::test_utils::props_option;
use std::sync::Arc;

use crate::helix_engine::traversal_core::ops::source::add_e::AddEAdapter;
use crate::helix_engine::{
    storage_core::HelixGraphStorage,
    traversal_core::{
        ops::{g::G, source::add_n::AddNAdapter, util::filter_ref::FilterRefAdapter},
        traversal_value::TraversalValue,
    },
    types::GraphError,
};
use crate::{helix_engine::traversal_core::ops::source::e_from_type::EFromTypeAdapter, props};
use crate::{
    helix_engine::traversal_core::ops::source::n_from_type::NFromTypeAdapter,
    protocol::value::Value,
};
use bumpalo::Bump;
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
fn test_filter_nodes() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create nodes with different properties
    let _ = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 25 }), None)
        .collect_to_obj()
        .unwrap();
    let _ = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 30 }), None)
        .collect_to_obj()
        .unwrap();
    let person3 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 35 }), None)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    let traversal = G::new(&storage, &txn, &arena)
        .n_from_type("person")
        .filter_ref(|val, _| {
            if let Ok(TraversalValue::Node(node)) = val {
                if let Some(value) = node.get_property("age") {
                    match value {
                        Value::F64(age) => Ok(*age > 30.0),
                        Value::I32(age) => Ok(*age > 30),
                        _ => Ok(false),
                    }
                } else {
                    Ok(false)
                }
            } else {
                Ok(false)
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person3.id());
}

#[test]
fn test_filter_macro_single_argument() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let _ = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "Alice" }),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let _ = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "name" => "Bob" }),
            None,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    fn has_name(val: &Result<TraversalValue, GraphError>) -> Result<bool, GraphError> {
        if let Ok(TraversalValue::Node(node)) = val {
            Ok(node.get_property("name").is_some())
        } else {
            Ok(false)
        }
    }

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_type("person")
        .filter_ref(|val, _| has_name(val))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(traversal.len(), 2);
    assert!(
        traversal
            .iter()
            .any(|val| if let TraversalValue::Node(node) = val {
                let name = node.get_property("name").unwrap();
                match name {
                    Value::String(name) => name == "Alice" || name == "Bob",
                    _ => false,
                }
            } else {
                false
            })
    );
}

#[test]
fn test_filter_macro_multiple_arguments() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let _ = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 25 }), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let person2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 30 }), None)
        .collect_to_obj()
        .unwrap();
    txn.commit().unwrap();

    fn age_greater_than(
        val: &Result<TraversalValue, GraphError>,
        min_age: i32,
    ) -> Result<bool, GraphError> {
        if let Ok(TraversalValue::Node(node)) = val {
            if let Some(value) = node.get_property("age") {
                match value {
                    Value::F64(age) => Ok(*age > min_age as f64),
                    Value::I32(age) => Ok(*age > min_age),
                    _ => Ok(false),
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_type("person")
        .filter_ref(|val, _| age_greater_than(val, 27))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person2.id());
}

#[test]
fn test_filter_edges() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let person1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect_to_obj()
        .unwrap();
    let person2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", None, None)
        .collect_to_obj()
        .unwrap();

    let _ = G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "knows",
            props_option(&arena, props! { "since" => 2020 }),
            person1.id(),
            person2.id(),
            false,
        )
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let edge2 = G::new_mut(&storage, &arena, &mut txn)
        .add_edge(
            "knows",
            props_option(&arena, props! { "since" => 2022 }),
            person2.id(),
            person1.id(),
            false,
        )
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    fn recent_edge(
        val: &Result<TraversalValue, GraphError>,
        year: i32,
    ) -> Result<bool, GraphError> {
        if let Ok(TraversalValue::Edge(edge)) = val {
            if let Some(value) = edge.get_property("since") {
                match value {
                    Value::I32(since) => Ok(*since > year),
                    Value::F64(since) => Ok(*since > year as f64),
                    _ => Ok(false),
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    let traversal = G::new(&storage, &txn, &arena)
        .e_from_type("knows")
        .filter_ref(|val, _| recent_edge(val, 2021))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), edge2.id());
}

#[test]
fn test_filter_empty_result() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let _ = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 25 }), None)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();
    let traversal = G::new(&storage, &txn, &arena)
        .n_from_type("person")
        .filter_ref(|val, _| {
            if let Ok(TraversalValue::Node(node)) = val {
                if let Some(value) = node.get_property("age") {
                    match value {
                        Value::I32(age) => Ok(*age > 100),
                        Value::F64(age) => Ok(*age > 100.0),
                        _ => Ok(false),
                    }
                } else {
                    Ok(false)
                }
            } else {
                Ok(false)
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(traversal.is_empty());
}

#[test]
fn test_filter_chain() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let _ = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "age" => 25, "name" => "Alice" }),
            None,
        )
        .collect_to_obj()
        .unwrap();
    let person2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "person",
            props_option(&arena, props! { "age" => 30, "name" => "Bob" }),
            None,
        )
        .collect_to_obj()
        .unwrap();
    let _ = G::new_mut(&storage, &arena, &mut txn)
        .add_n("person", props_option(&arena, props! { "age" => 35 }), None)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    fn has_name(val: &Result<TraversalValue, GraphError>) -> Result<bool, GraphError> {
        if let Ok(TraversalValue::Node(node)) = val {
            node.get_property("name").map_or(Ok(false), |_| Ok(true))
        } else {
            Ok(false)
        }
    }

    fn age_greater_than(
        val: &Result<TraversalValue, GraphError>,
        min_age: i32,
    ) -> Result<bool, GraphError> {
        if let Ok(TraversalValue::Node(node)) = val {
            if let Some(value) = node.get_property("age") {
                match value {
                    Value::F64(age) => Ok(*age > min_age as f64),
                    Value::I32(age) => Ok(*age > min_age),
                    _ => Ok(false),
                }
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    let traversal = G::new(&storage, &txn, &arena)
        .n_from_type("person")
        .filter_ref(|val, _| has_name(val))
        .filter_ref(|val, _| age_greater_than(val, 27))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(traversal.len(), 1);
    assert_eq!(traversal[0].id(), person2.id());
}
