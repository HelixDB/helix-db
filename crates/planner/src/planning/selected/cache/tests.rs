use super::*;
use crate::{cost, exec, ir, logical, physical, properties};

fn selected_root() -> exec::SelectedExecutableRunRoot {
    exec::SelectedExecutableRunRoot::alternative(
        logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp),
        physical::PhysicalAlternative::new(
            physical::PhysicalExpr::NoOp,
            properties::DeliveredProperties::default(),
            cost::CostVector::ZERO,
        ),
    )
}

fn selected_run_root() -> SelectedRunRoot {
    SelectedRunRoot {
        root: selected_root(),
        metrics: exec::PlannerMetrics {
            memo_groups: 7,
            selected_cost: cost::CostVector {
                range_seeks: 3,
                ..cost::CostVector::ZERO
            },
            ..exec::PlannerMetrics::default()
        },
    }
}

fn selectable_node_root() -> super::super::root::SelectableRunRoot {
    super::super::root::SelectableRunRoot::new(logical::LogicalExpr::AccessPath(
        logical::AccessPath::Node(logical::NodeAccessPath::new(
            ir::NodeAccessSourcePlan::new(ir::NodeAccessPlan::AllScan).unwrap(),
        )),
    ))
}

fn selectable_edge_root() -> super::super::root::SelectableRunRoot {
    super::super::root::SelectableRunRoot::new(logical::LogicalExpr::AccessPath(
        logical::AccessPath::Edge(logical::EdgeAccessPath::new(
            ir::EdgeAccessSourcePlan::new(ir::EdgeAccessPlan::AllScan).unwrap(),
        )),
    ))
}

#[test]
fn selected_run_root_cached_use_reports_cost_without_duplicate_optimizer_work() {
    let cached = selected_run_root().cached_use();

    assert_eq!(cached.metrics.memo_groups, 0);
    assert_eq!(cached.metrics.selected_cost.range_seeks, 3);
}

#[test]
fn pending_root_use_reports_work_once_and_preserves_cached_cost() {
    let mut optimized = OptimizedSelectedRunRoots::new(vec![selected_run_root()], 1).unwrap();
    let mut pending = PendingSelectedRunRoots::default();
    let root_use = pending.push_or_reuse(selectable_node_root());

    let first = optimized.select(root_use).unwrap();
    assert_eq!(first.metrics.memo_groups, 7);
    assert_eq!(first.metrics.selected_cost.range_seeks, 3);

    let root_use = pending.push_or_reuse(selectable_node_root());
    let second = optimized.select(root_use).unwrap();
    assert_eq!(second.metrics.memo_groups, 0);
    assert_eq!(second.metrics.selected_cost.range_seeks, 3);
}

#[test]
fn ready_root_use_preserves_full_metrics() {
    let selected = selected_run_root();
    let mut optimized = OptimizedSelectedRunRoots::new(Vec::new(), 0).unwrap();

    let ready = optimized
        .select(SelectedRunRootUse::Ready(selected))
        .unwrap();

    assert_eq!(ready.metrics.memo_groups, 7);
    assert_eq!(ready.metrics.selected_cost.range_seeks, 3);
}

#[test]
fn pending_roots_reuse_duplicates_and_preserve_order() {
    let mut pending = PendingSelectedRunRoots::default();
    let node = selectable_node_root();
    let edge = selectable_edge_root();

    assert!(matches!(
        pending.push_or_reuse(node.clone()),
        SelectedRunRootUse::Pending(index) if index.get() == 0
    ));
    assert!(matches!(
        pending.push_or_reuse(edge.clone()),
        SelectedRunRootUse::Pending(index) if index.get() == 1
    ));
    assert!(matches!(
        pending.push_or_reuse(node.clone()),
        SelectedRunRootUse::Pending(index) if index.get() == 0
    ));

    let batch = pending.into_optimizer_batch().unwrap();
    assert_eq!(batch.len(), 2);
    let (root_exprs, entries) = batch.into_parts();
    assert_eq!(root_exprs.as_ref().len(), 2);
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].logical_root, node);
    assert_eq!(entries[1].logical_root, edge);
}

#[test]
fn empty_pending_roots_do_not_build_optimizer_batch() {
    assert!(PendingSelectedRunRoots::default()
        .into_optimizer_batch()
        .is_none());
}

#[test]
fn optimized_selected_roots_reject_misaligned_pending_count() {
    match OptimizedSelectedRunRoots::new(vec![selected_run_root()], 2) {
        Ok(_) => panic!("misaligned pending count must be rejected"),
        Err(error) => assert_eq!(
            error,
            OptimizedSelectedRunRootsError::BatchLengthMismatch {
                roots: 1,
                pending: 2,
            }
        ),
    }
}

#[test]
fn optimized_selected_roots_reject_out_of_range_pending_uses() {
    let mut optimized = OptimizedSelectedRunRoots::new(Vec::new(), 0).unwrap();
    let mut pending = PendingSelectedRunRoots::default();
    let root_use = pending.push_or_reuse(selectable_node_root());

    match optimized.select(root_use) {
        Ok(_) => panic!("out-of-range pending use must be rejected"),
        Err(error) => assert_eq!(
            error,
            OptimizedSelectedRunRootsError::PendingRootMissing {
                index: 0,
                available: 0,
            }
        ),
    }
}

#[test]
fn selected_root_cache_returns_cached_use_and_misses_distinct_roots() {
    let mut cache = SelectedRunRootCache::default();
    let node = selectable_node_root();
    let edge = selectable_edge_root();

    cache.insert(node.clone(), selected_run_root());

    let cached = cache.get(&node).expect("node root was cached");
    assert_eq!(cached.metrics.memo_groups, 0);
    assert_eq!(cached.metrics.selected_cost.range_seeks, 3);
    assert!(cache.get(&edge).is_none());
}
