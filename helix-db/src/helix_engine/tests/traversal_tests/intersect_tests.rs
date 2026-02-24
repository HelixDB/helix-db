use super::test_utils::props_option;
use std::collections::HashSet;
use std::sync::Arc;

use crate::{
    helix_engine::{
        storage_core::HelixGraphStorage,
        traversal_core::ops::{
            g::G,
            in_::in_::InAdapter,
            out::out::OutAdapter,
            source::{add_e::AddEAdapter, add_n::AddNAdapter, n_from_type::NFromTypeAdapter},
            util::{filter_ref::FilterRefAdapter, intersect::IntersectAdapter},
        },
    },
    props,
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

/// User-requested scenario:
/// 6 nodes: sources 1-3 (group "alpha"), targets 4-6
/// Edges (type "links"):
///   node1 → node4, node1 → node5
///   node2 → node4, node2 → node5, node2 → node6
///   node3 → node4, node3 → node5, node3 → node6
/// Filter sources where group == "alpha", intersect on out_node("links")
/// Expected: nodes 4 and 5 (reachable from ALL of 1, 2, 3)
#[test]
fn test_intersect_user_scenario() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create source nodes
    let src1 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "source",
            props_option(&arena, props! { "group" => "alpha" }),
            None,
        )
        .collect_to_obj()
        .unwrap();
    let src2 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "source",
            props_option(&arena, props! { "group" => "alpha" }),
            None,
        )
        .collect_to_obj()
        .unwrap();
    let src3 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "source",
            props_option(&arena, props! { "group" => "alpha" }),
            None,
        )
        .collect_to_obj()
        .unwrap();

    // Create target nodes
    let tgt4 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "target",
            props_option(&arena, props! { "name" => "T4" }),
            None,
        )
        .collect_to_obj()
        .unwrap();
    let tgt5 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "target",
            props_option(&arena, props! { "name" => "T5" }),
            None,
        )
        .collect_to_obj()
        .unwrap();
    let tgt6 = G::new_mut(&storage, &arena, &mut txn)
        .add_n(
            "target",
            props_option(&arena, props! { "name" => "T6" }),
            None,
        )
        .collect_to_obj()
        .unwrap();

    // src1 → tgt4, src1 → tgt5
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("links", None, src1.id(), tgt4.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("links", None, src1.id(), tgt5.id(), false, false)
        .collect_to_obj()
        .unwrap();

    // src2 → tgt4, src2 → tgt5, src2 → tgt6
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("links", None, src2.id(), tgt4.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("links", None, src2.id(), tgt5.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("links", None, src2.id(), tgt6.id(), false, false)
        .collect_to_obj()
        .unwrap();

    // src3 → tgt4, src3 → tgt5, src3 → tgt6
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("links", None, src3.id(), tgt4.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("links", None, src3.id(), tgt5.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("links", None, src3.id(), tgt6.id(), false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    let results = G::new(&storage, &txn, &arena)
        .n_from_type("source")
        .filter_ref(|val, _txn| {
            if let Ok(val) = val {
                Ok(val
                    .get_property("group")
                    .map_or(false, |v| v == &Value::String("alpha".to_string())))
            } else {
                Ok(false)
            }
        })
        .intersect(|val, db, txn, arena| {
            G::from_iter(db, txn, std::iter::once(val), arena)
                .out_node("links")
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let result_ids: HashSet<u128> = results.iter().map(|v| v.id()).collect();
    assert_eq!(result_ids.len(), 2);
    assert!(result_ids.contains(&tgt4.id()));
    assert!(result_ids.contains(&tgt5.id()));
    assert!(!result_ids.contains(&tgt6.id()));
}

/// A→[X,Y], B→[Y,Z], C→[Y,W]
/// Expected: [Y]
#[test]
fn test_intersect_basic() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("upstream", None, None)
        .collect_to_obj()
        .unwrap();
    let b = G::new_mut(&storage, &arena, &mut txn)
        .add_n("upstream", None, None)
        .collect_to_obj()
        .unwrap();
    let c = G::new_mut(&storage, &arena, &mut txn)
        .add_n("upstream", None, None)
        .collect_to_obj()
        .unwrap();

    let x = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();
    let y = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();
    let z = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();
    let w = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();

    // A → X, Y
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, a.id(), x.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, a.id(), y.id(), false, false)
        .collect_to_obj()
        .unwrap();

    // B → Y, Z
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, b.id(), y.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, b.id(), z.id(), false, false)
        .collect_to_obj()
        .unwrap();

    // C → Y, W
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, c.id(), y.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, c.id(), w.id(), false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    let results = G::new(&storage, &txn, &arena)
        .n_from_type("upstream")
        .intersect(|val, db, txn, arena| {
            G::from_iter(db, txn, std::iter::once(val), arena)
                .out_node("rel")
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id(), y.id());
}

/// A→[X], B→[Y], C→[Z] — no overlap
/// Expected: empty
#[test]
fn test_intersect_empty_result() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("upstream", None, None)
        .collect_to_obj()
        .unwrap();
    let b = G::new_mut(&storage, &arena, &mut txn)
        .add_n("upstream", None, None)
        .collect_to_obj()
        .unwrap();
    let c = G::new_mut(&storage, &arena, &mut txn)
        .add_n("upstream", None, None)
        .collect_to_obj()
        .unwrap();

    let x = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();
    let y = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();
    let z = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, a.id(), x.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, b.id(), y.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, c.id(), z.id(), false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    let results = G::new(&storage, &txn, &arena)
        .n_from_type("upstream")
        .intersect(|val, db, txn, arena| {
            G::from_iter(db, txn, std::iter::once(val), arena)
                .out_node("rel")
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(results.is_empty());
}

/// Only one upstream node A→[X,Y,Z]
/// Expected: [X,Y,Z]
#[test]
fn test_intersect_single_upstream() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("upstream", None, None)
        .collect_to_obj()
        .unwrap();

    let x = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();
    let y = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();
    let z = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();

    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, a.id(), x.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, a.id(), y.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, a.id(), z.id(), false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    let results = G::new(&storage, &txn, &arena)
        .n_from_type("upstream")
        .intersect(|val, db, txn, arena| {
            G::from_iter(db, txn, std::iter::once(val), arena)
                .out_node("rel")
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let result_ids: HashSet<u128> = results.iter().map(|v| v.id()).collect();
    assert_eq!(result_ids.len(), 3);
    assert!(result_ids.contains(&x.id()));
    assert!(result_ids.contains(&y.id()));
    assert!(result_ids.contains(&z.id()));
}

/// No upstream items (filter matches nothing)
/// Expected: empty
#[test]
fn test_intersect_empty_upstream() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // Create a node of a different type so "upstream" yields nothing
    G::new_mut(&storage, &arena, &mut txn)
        .add_n("other", None, None)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    let results = G::new(&storage, &txn, &arena)
        .n_from_type("upstream")
        .intersect(|val, db, txn, arena| {
            G::from_iter(db, txn, std::iter::once(val), arena)
                .out_node("rel")
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(results.is_empty());
}

/// A→[X,Y], B→[X,Y], C→[X,Y] — all same targets
/// Expected: [X,Y]
#[test]
fn test_intersect_all_same_targets() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("upstream", None, None)
        .collect_to_obj()
        .unwrap();
    let b = G::new_mut(&storage, &arena, &mut txn)
        .add_n("upstream", None, None)
        .collect_to_obj()
        .unwrap();
    let c = G::new_mut(&storage, &arena, &mut txn)
        .add_n("upstream", None, None)
        .collect_to_obj()
        .unwrap();

    let x = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();
    let y = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();

    // All three upstream → X, Y
    for src in [&a, &b, &c] {
        G::new_mut(&storage, &arena, &mut txn)
            .add_edge("rel", None, src.id(), x.id(), false, false)
            .collect_to_obj()
            .unwrap();
        G::new_mut(&storage, &arena, &mut txn)
            .add_edge("rel", None, src.id(), y.id(), false, false)
            .collect_to_obj()
            .unwrap();
    }

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    let results = G::new(&storage, &txn, &arena)
        .n_from_type("upstream")
        .intersect(|val, db, txn, arena| {
            G::from_iter(db, txn, std::iter::once(val), arena)
                .out_node("rel")
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let result_ids: HashSet<u128> = results.iter().map(|v| v.id()).collect();
    assert_eq!(result_ids.len(), 2);
    assert!(result_ids.contains(&x.id()));
    assert!(result_ids.contains(&y.id()));
}

/// A→[X,Y], B→[] (no edges), C→[X,Z]
/// Expected: empty (B contributes nothing so intersection is empty)
#[test]
fn test_intersect_one_upstream_has_no_edges() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    let a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("upstream", None, None)
        .collect_to_obj()
        .unwrap();
    let _b = G::new_mut(&storage, &arena, &mut txn)
        .add_n("upstream", None, None)
        .collect_to_obj()
        .unwrap();
    let c = G::new_mut(&storage, &arena, &mut txn)
        .add_n("upstream", None, None)
        .collect_to_obj()
        .unwrap();

    let x = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();
    let y = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();
    let z = G::new_mut(&storage, &arena, &mut txn)
        .add_n("downstream", None, None)
        .collect_to_obj()
        .unwrap();

    // A → X, Y
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, a.id(), x.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, a.id(), y.id(), false, false)
        .collect_to_obj()
        .unwrap();

    // B has no edges

    // C → X, Z
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, c.id(), x.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("rel", None, c.id(), z.id(), false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    let results = G::new(&storage, &txn, &arena)
        .n_from_type("upstream")
        .intersect(|val, db, txn, arena| {
            G::from_iter(db, txn, std::iter::once(val), arena)
                .out_node("rel")
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert!(results.is_empty());
}

/// Use in_node instead of out_node to verify intersect works with reverse traversals.
/// X→A, Y→A, Y→B, Z→B  (edges point TO upstream nodes)
/// Intersect on in_node("rel") from downstream nodes X,Y:
///   X's in-neighbors via "rel" = [] (X has no incoming edges)
/// So instead: A→X, A→Y, B→Y, B→Z
/// Upstream = downstream nodes [X,Y,Z], intersect via in_node("backlink"):
///   We set up: X←A, X←B ; Y←A ; Z←B
///   Intersection of in-neighbors: X sees [A,B], Y sees [A], Z sees [B] → empty
/// Better setup: X←A, X←B ; Y←A, Y←B → intersection = [A,B]
#[test]
fn test_intersect_with_in_edges() {
    let (_temp_dir, storage) = setup_test_db();
    let arena = Bump::new();
    let mut txn = storage.graph_env.write_txn().unwrap();

    // "source" nodes that point to "target" nodes
    let a = G::new_mut(&storage, &arena, &mut txn)
        .add_n("source", None, None)
        .collect_to_obj()
        .unwrap();
    let b = G::new_mut(&storage, &arena, &mut txn)
        .add_n("source", None, None)
        .collect_to_obj()
        .unwrap();
    let c = G::new_mut(&storage, &arena, &mut txn)
        .add_n("source", None, None)
        .collect_to_obj()
        .unwrap();

    // "target" nodes
    let x = G::new_mut(&storage, &arena, &mut txn)
        .add_n("target", None, None)
        .collect_to_obj()
        .unwrap();
    let y = G::new_mut(&storage, &arena, &mut txn)
        .add_n("target", None, None)
        .collect_to_obj()
        .unwrap();

    // A → X, A → Y
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("points_to", None, a.id(), x.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("points_to", None, a.id(), y.id(), false, false)
        .collect_to_obj()
        .unwrap();

    // B → X, B → Y
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("points_to", None, b.id(), x.id(), false, false)
        .collect_to_obj()
        .unwrap();
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("points_to", None, b.id(), y.id(), false, false)
        .collect_to_obj()
        .unwrap();

    // C → X only (not Y)
    G::new_mut(&storage, &arena, &mut txn)
        .add_edge("points_to", None, c.id(), x.id(), false, false)
        .collect_to_obj()
        .unwrap();

    txn.commit().unwrap();
    let txn = storage.graph_env.read_txn().unwrap();

    // Traverse from target nodes, use in_node to find who points to them
    // X's in-neighbors: [A, B, C]
    // Y's in-neighbors: [A, B]
    // Intersection: [A, B]
    let results = G::new(&storage, &txn, &arena)
        .n_from_type("target")
        .intersect(|val, db, txn, arena| {
            G::from_iter(db, txn, std::iter::once(val), arena)
                .in_node("points_to")
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let result_ids: HashSet<u128> = results.iter().map(|v| v.id()).collect();
    assert_eq!(result_ids.len(), 2);
    assert!(result_ids.contains(&a.id()));
    assert!(result_ids.contains(&b.id()));
    assert!(!result_ids.contains(&c.id()));
}
