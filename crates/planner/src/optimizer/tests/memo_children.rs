use super::support;
use crate::{cost, optimizer};

#[test]
fn cascades_optimizer_selects_parent_alternative_by_recursive_child_cost() {
    let exploration = support::RewriteNestedPipelineChildRule;
    let implementation = support::NestedPipelineCostRule;
    let optimizer = support::optimizer(vec![&exploration, &implementation]);

    let result = support::optimize(
        &optimizer,
        support::nested_variable_root_pipeline(1, 9),
        &support::config(),
    );

    assert_eq!(result.memo().group_count(), 3);
    assert_eq!(result.metrics().alternatives_considered, 4);
    let selected = result.best_plan(result.root()).unwrap();
    assert_eq!(
        selected.source_expr.expr,
        support::nested_variable_root_pipeline(2, 9)
    );
    assert_eq!(
        selected.entry.alternative.cost.latency,
        cost::LatencyEstimate::micros(50)
    );
    assert_eq!(
        selected.selected_cost.latency,
        cost::LatencyEstimate::micros(51)
    );
    assert_eq!(
        result.metrics().selected_cost.latency,
        cost::LatencyEstimate::micros(51)
    );
    assert_eq!(
        result.best_alternative(result.root()).unwrap().cost.latency,
        cost::LatencyEstimate::micros(50)
    );
}

#[test]
fn cascades_optimizer_rejects_parent_alternatives_with_unplanned_selected_children() {
    let parent_only = support::OuterPipelineOnlyCostRule;
    let optimizer = support::optimizer(vec![&parent_only]);

    let result = support::optimize(
        &optimizer,
        support::nested_variable_root_pipeline(1, 9),
        &support::config(),
    );

    assert_eq!(result.metrics().alternatives_considered, 1);
    let error = result.best_plan(result.root()).unwrap_err();
    assert!(matches!(
        error,
        optimizer::SelectionError::ChildSelectionFailed { parent_group, .. }
            if parent_group == result.root()
    ));
    assert_eq!(result.metrics().selected_cost, cost::CostVector::ZERO);
}
