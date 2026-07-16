use std::collections::BTreeMap;

use super::support;
use crate::{cost, exec, ir, memo, optimizer, physical, properties, rules};

#[test]
fn cascades_optimizer_assigns_alternative_ids_and_breaks_cost_ties_by_digest() {
    let first_by_digest = support::alternative_with_expr(physical::PhysicalExpr::Sort, 10);
    let second_by_digest = support::alternative_with_expr(physical::PhysicalExpr::Barrier, 10);
    assert_ne!(first_by_digest.digest, second_by_digest.digest);
    let (lower_digest, higher_digest) = if first_by_digest.digest < second_by_digest.digest {
        (first_by_digest, second_by_digest)
    } else {
        (second_by_digest, first_by_digest)
    };
    let implementation = support::StaticRule::new(
        "stable_tie_break",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                higher_digest.clone(),
                vec![lower_digest.clone()],
            ),
        )),
    );
    let optimizer = support::optimizer(vec![&implementation]);

    let result = support::optimize(&optimizer, support::source(), &support::config());
    let retained = result
        .physical()
        .iter()
        .find(|group| group.group == result.root())
        .unwrap();

    assert_eq!(retained.alternatives[0].id.get(), 1);
    assert_eq!(retained.alternatives[1].id.get(), 2);
    assert_eq!(retained.alternatives[0].source_expr.get(), 1);
    assert_eq!(
        retained.alternatives[0].provenance.rule_id().as_ref(),
        "stable_tie_break"
    );
    assert_eq!(
        result.best_alternative(result.root()).unwrap().digest,
        lower_digest.digest
    );
}

#[test]
fn cascades_optimizer_selects_best_alternative_satisfying_required_properties() {
    let cheap_edge = support::alternative_with_element(properties::ElementKind::Edge, 5);
    let matching_node = support::alternative_with_element(properties::ElementKind::Node, 50);
    let implementation = support::StaticRule::new(
        "property_selection",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one_and_rest(cheap_edge.clone(), vec![matching_node.clone()]),
        )),
    );
    let optimizer = support::optimizer(vec![&implementation]);
    let result = support::optimize(&optimizer, support::source(), &support::config());
    let require_node = properties::RequiredProperties {
        element: Some(properties::ElementKind::Node),
        ..properties::RequiredProperties::default()
    };
    let require_order = properties::RequiredProperties {
        ordering: properties::RequiredOrdering::ByKeys(ir::OrderKeys::from(ir::OrderKey {
            property: ir::NonEmptyString::new("name").unwrap(),
            order: helix_ast::traversal::Order::Asc,
        })),
        ..properties::RequiredProperties::default()
    };

    assert_eq!(result.best_alternative(result.root()).unwrap(), &cheap_edge);
    assert_eq!(
        result
            .best_alternative_satisfying(result.root(), &require_node)
            .unwrap(),
        &matching_node
    );
    let selected_entry = result
        .best_alternative_entry_satisfying(result.root(), &require_node)
        .unwrap();
    assert_eq!(selected_entry.source_expr.get(), 1);
    assert_eq!(
        selected_entry.provenance.rule_id().as_ref(),
        "property_selection"
    );
    assert_eq!(&selected_entry.alternative, &matching_node);
    let selected_plan = result
        .best_plan_satisfying(result.root(), &require_node)
        .unwrap();
    assert_eq!(selected_plan.source_expr.id, selected_entry.source_expr);
    assert_eq!(&selected_plan.entry.alternative, &matching_node);
    assert_eq!(
        result
            .best_alternative_satisfying(result.root(), &require_order)
            .unwrap_err(),
        optimizer::SelectionError::UnsatisfiedRequiredProperties {
            group: result.root(),
            required: require_order.clone()
        }
    );
    assert_eq!(
        result
            .best_plan_satisfying(result.root(), &require_order)
            .unwrap_err(),
        optimizer::SelectionError::UnsatisfiedRequiredProperties {
            group: result.root(),
            required: require_order
        }
    );
}

#[test]
fn cascades_optimizer_counts_explicit_rule_rejections() {
    let rejecting = support::StaticRule::new(
        "reject",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Rejected(rules::RuleRejection::new("missing_index").unwrap()),
    );
    let optimizer = support::optimizer(vec![&rejecting]);

    let result = support::optimize(&optimizer, support::source(), &support::config());

    assert_eq!(result.metrics().rule_fires, 1);
    assert_eq!(result.metrics().rejected_alternatives, 1);
    assert_eq!(result.metrics().alternatives_considered, 0);
    assert_eq!(
        result.metrics().selected_cost,
        crate::cost::CostVector::ZERO
    );
    assert_eq!(result.guardrail(), None);
    assert_eq!(
        result.best_alternative(result.root()).unwrap_err(),
        optimizer::SelectionError::NoPhysicalAlternatives {
            group: result.root()
        }
    );
}

#[test]
fn optimization_result_serde_rebuilds_physical_group_index() {
    let implementation = support::StaticRule::new(
        "serde_index",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(support::alternative(7)),
        )),
    );
    let optimizer = support::optimizer(vec![&implementation]);
    let result = support::optimize(&optimizer, support::source(), &support::config());

    let round_trip = serde_json::from_value::<optimizer::OptimizationResult>(
        serde_json::to_value(&result).unwrap(),
    )
    .unwrap();

    assert_eq!(round_trip, result);
    assert_eq!(
        round_trip
            .best_alternative(round_trip.root())
            .unwrap()
            .cost
            .latency,
        cost::LatencyEstimate::micros(7)
    );
}

#[test]
fn optimization_result_rejects_duplicate_physical_groups_on_deserialize() {
    let implementation = support::StaticRule::new(
        "duplicate_physical_group",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(support::alternative(1)),
        )),
    );
    let optimizer = support::optimizer(vec![&implementation]);
    let result = support::optimize(&optimizer, support::source(), &support::config());
    let mut value = serde_json::to_value(&result).unwrap();
    let physical = value
        .get_mut("physical")
        .and_then(serde_json::Value::as_array_mut)
        .expect("optimization result serializes physical alternatives");
    physical.push(physical[0].clone());

    let error = serde_json::from_value::<optimizer::OptimizationResult>(value).unwrap_err();

    assert!(error
        .to_string()
        .contains("physical alternatives for memo group 1 are duplicated"));
}

#[test]
fn optimization_result_rejects_non_sequential_physical_alternative_ids() {
    let implementation = support::StaticRule::new(
        "sparse_physical_alternative",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(support::alternative(1)),
        )),
    );
    let optimizer = support::optimizer(vec![&implementation]);
    let result = support::optimize(&optimizer, support::source(), &support::config());
    let mut value = serde_json::to_value(&result).unwrap();
    value["physical"][0]["alternatives"][0]["id"] = serde_json::json!(2);

    let error = serde_json::from_value::<optimizer::OptimizationResult>(value).unwrap_err();

    assert!(error
        .to_string()
        .contains("physical alternative IDs for memo group 1 must be sequential"));
}

#[test]
fn selection_session_reuses_default_selection_cache_for_repeated_group() {
    let implementation = support::StaticRule::new(
        "session_repeated_group",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(support::alternative(7)),
        )),
    );
    let optimizer = support::optimizer(vec![&implementation]);
    let result = support::optimize(&optimizer, support::source(), &support::config());
    let mut selection = result.selection_session();

    let first = selection.best_plan(result.root()).unwrap();
    assert_eq!(
        first.selected_cost.latency,
        cost::LatencyEstimate::micros(7)
    );
    assert_eq!(selection.cached_default_selection_count(), 1);

    let second = selection.best_plan(result.root()).unwrap();
    assert_eq!(second.entry.id, first.entry.id);
    assert_eq!(second.selected_cost, first.selected_cost);
    assert_eq!(selection.cached_default_selection_count(), 1);
}

#[test]
fn selection_session_reuses_child_selection_cache_across_roots() {
    let implementation = support::StaticRule::new(
        "session_shared_child",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(support::alternative(7)),
        )),
    );
    let optimizer = support::optimizer(vec![&implementation]);
    let result = support::optimize_many(
        &optimizer,
        ir::AtLeast::<_, 1>::from_one_and_rest(
            support::nested_variable_root_pipeline(1, 9),
            vec![support::nested_variable_root_pipeline(1, 10)],
        ),
        &support::config(),
    );
    let roots = result.roots().as_ref();
    let mut selection = result.selection_session();

    let first = selection.best_plan(roots[0]).unwrap();
    assert_eq!(
        first.selected_cost.latency,
        cost::LatencyEstimate::micros(14)
    );
    assert_eq!(selection.cached_default_selection_count(), 2);

    let second = selection.best_plan(roots[1]).unwrap();
    assert_eq!(
        second.selected_cost.latency,
        cost::LatencyEstimate::micros(14)
    );
    assert_eq!(selection.cached_default_selection_count(), 3);
    assert_eq!(first.source_expr.children, second.source_expr.children);
}

fn physical_entry(
    source_expr: memo::MemoExprId,
    latency: u64,
) -> optimizer::result::PendingPhysicalAlternative {
    optimizer::result::PendingPhysicalAlternative {
        source_expr,
        provenance: optimizer::RuleProvenance::from_metadata(&rules::RuleMetadata::new(
            rules::RuleId::new("summary").unwrap(),
            rules::RuleKind::Implementation,
        )),
        alternative: support::alternative(latency),
    }
}

#[test]
fn root_selection_summary_reports_complete_multi_root_cost() {
    let expression =
        memo::MemoExpression::new(support::source(), memo::MemoChildGroups::empty()).unwrap();
    let mut memo = memo::Memo::default();
    let first = memo
        .insert_group_with_expr_id(expression.clone())
        .expect("test memo allocation should fit");
    let second = memo
        .insert_group_with_expr_id(expression)
        .expect("test memo allocation should fit");
    let mut physical = BTreeMap::new();
    physical.insert(first.group, vec![physical_entry(first.expr, 2)]);
    physical.insert(second.group, vec![physical_entry(second.expr, 3)]);
    let result = optimizer::result::OptimizationResult::new(
        memo,
        first.group,
        ir::AtLeast::<_, 1>::from_one_and_rest(first.group, vec![second.group]),
        physical,
        exec::PlannerMetrics::default(),
        None,
    );

    assert_eq!(
        result.root_selection_summary(),
        optimizer::RootSelectionSummary::Complete {
            selected_cost: cost::CostVector {
                latency: cost::LatencyEstimate::micros(5),
                ..cost::CostVector::ZERO
            }
        }
    );
    assert_eq!(
        result.metrics().selected_cost.latency,
        cost::LatencyEstimate::micros(5)
    );
}

#[test]
fn root_selection_summary_keeps_incomplete_multi_root_cost_out_of_metrics() {
    let expression =
        memo::MemoExpression::new(support::source(), memo::MemoChildGroups::empty()).unwrap();
    let mut memo = memo::Memo::default();
    let selected = memo
        .insert_group_with_expr_id(expression.clone())
        .expect("test memo allocation should fit");
    let missing = memo
        .insert_group_with_expr_id(expression)
        .expect("test memo allocation should fit");
    let mut physical = BTreeMap::new();
    physical.insert(selected.group, vec![physical_entry(selected.expr, 2)]);
    let result = optimizer::result::OptimizationResult::new(
        memo,
        selected.group,
        ir::AtLeast::<_, 1>::from_one_and_rest(selected.group, vec![missing.group]),
        physical,
        exec::PlannerMetrics::default(),
        None,
    );

    let summary = result.root_selection_summary();
    let expected = optimizer::RootSelectionSummary::Incomplete {
        successful_cost: cost::CostVector {
            latency: cost::LatencyEstimate::micros(2),
            ..cost::CostVector::ZERO
        },
        failures: ir::AtLeast::<_, 1>::from_one(optimizer::RootSelectionFailure {
            root: missing.group,
            error: optimizer::SelectionError::NoPhysicalAlternatives {
                group: missing.group,
            },
        }),
    };
    assert_eq!(summary, expected);
    assert_eq!(
        serde_json::from_value::<optimizer::RootSelectionSummary>(
            serde_json::to_value(&summary).unwrap()
        )
        .unwrap(),
        expected
    );
    assert_eq!(result.metrics().selected_cost, cost::CostVector::ZERO);
}

#[test]
fn best_plan_reports_missing_memo_group() {
    let implementation = support::StaticRule::new(
        "implementation",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(support::alternative(1)),
        )),
    );
    let optimizer = support::optimizer(vec![&implementation]);
    let result = support::optimize(&optimizer, support::source(), &support::config());
    let missing = memo::MemoGroupId::new(99).unwrap();

    assert_eq!(
        result.best_plan(missing).unwrap_err(),
        optimizer::SelectionError::MissingMemoGroup { group: missing }
    );
}

#[test]
fn best_plan_reports_recursive_selection_cycles() {
    let root_group = memo::MemoGroupId::first();
    let root_expr = support::nested_variable_root_pipeline(1, 9);
    let expression =
        memo::MemoExpression::new(root_expr, memo::MemoChildGroups::new(vec![root_group])).unwrap();
    let mut memo = memo::Memo::default();
    let root = memo
        .insert_group(expression)
        .expect("test memo group allocation should fit");
    assert_eq!(root, root_group);
    let mut physical = BTreeMap::new();
    physical.insert(
        root,
        vec![optimizer::result::PendingPhysicalAlternative {
            source_expr: memo::MemoExprId::first(),
            provenance: optimizer::RuleProvenance::from_metadata(&rules::RuleMetadata::new(
                rules::RuleId::new("cycle").unwrap(),
                rules::RuleKind::Implementation,
            )),
            alternative: support::alternative(1),
        }],
    );
    let result = optimizer::result::OptimizationResult::new(
        memo,
        root,
        ir::AtLeast::<_, 1>::from_one(root),
        physical,
        exec::PlannerMetrics::default(),
        None,
    );

    assert_eq!(
        result.best_plan(root).unwrap_err(),
        optimizer::SelectionError::ChildSelectionFailed {
            parent_group: root,
            child_group: root,
            reason: Box::new(optimizer::SelectionError::RecursiveSelectionCycle { group: root })
        }
    );
    assert_eq!(result.metrics().selected_cost, cost::CostVector::ZERO);
}

#[test]
fn best_plan_reports_alternatives_with_missing_source_expression() {
    let source = support::source();
    let expression = memo::MemoExpression::new(source, memo::MemoChildGroups::empty()).unwrap();
    let mut memo = memo::Memo::default();
    let root = memo
        .insert_group(expression)
        .expect("test memo group allocation should fit");
    let missing_source = memo::MemoExprId::new(99).unwrap();
    let mut physical = BTreeMap::new();
    physical.insert(
        root,
        vec![optimizer::result::PendingPhysicalAlternative {
            source_expr: missing_source,
            provenance: optimizer::RuleProvenance::from_metadata(&rules::RuleMetadata::new(
                rules::RuleId::new("corrupt_source").unwrap(),
                rules::RuleKind::Implementation,
            )),
            alternative: support::alternative(1),
        }],
    );
    let result = optimizer::result::OptimizationResult::new(
        memo,
        root,
        ir::AtLeast::<_, 1>::from_one(root),
        physical,
        exec::PlannerMetrics::default(),
        None,
    );

    assert_eq!(
        result.best_plan(root).unwrap_err(),
        optimizer::SelectionError::MissingSourceExpression {
            group: root,
            alternative: memo::PhysicalAlternativeId::new(1).unwrap(),
            source_expr: missing_source
        }
    );
    assert_eq!(
        result.best_alternative(root).unwrap_err(),
        optimizer::SelectionError::MissingSourceExpression {
            group: root,
            alternative: memo::PhysicalAlternativeId::new(1).unwrap(),
            source_expr: missing_source
        }
    );
    assert_eq!(result.metrics().selected_cost, cost::CostVector::ZERO);
}
