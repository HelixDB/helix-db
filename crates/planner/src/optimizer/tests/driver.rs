use super::support;
use crate::{cost, exec, ir, logical, memo, optimizer, physical, properties, rules};

struct SleepAndExploreRule;

impl optimizer::OptimizerRule for SleepAndExploreRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        static METADATA: std::sync::OnceLock<rules::RuleMetadata> = std::sync::OnceLock::new();
        METADATA.get_or_init(|| {
            rules::RuleMetadata::new(
                rules::RuleId::new("sleep_and_explore").unwrap(),
                rules::RuleKind::Exploration,
            )
        })
    }

    fn apply(&self, _input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        std::thread::sleep(std::time::Duration::from_millis(20));
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(
            ir::AtLeast::<_, 1>::from_one(support::limit()),
        ))
    }
}

struct SleepAndImplementRule;

impl optimizer::OptimizerRule for SleepAndImplementRule {
    fn metadata(&self) -> &rules::RuleMetadata {
        static METADATA: std::sync::OnceLock<rules::RuleMetadata> = std::sync::OnceLock::new();
        METADATA.get_or_init(|| {
            rules::RuleMetadata::new(
                rules::RuleId::new("sleep_and_implement").unwrap(),
                rules::RuleKind::Implementation,
            )
        })
    }

    fn apply(&self, _input: optimizer::RuleInput<'_>) -> optimizer::RuleResult {
        std::thread::sleep(std::time::Duration::from_millis(5));
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(support::alternative(7)),
        ))
    }
}

#[test]
fn cascades_optimizer_explores_logical_rules_and_collects_best_physical_alternative() {
    let exploration = support::StaticRule::new(
        "limit_explore",
        rules::RuleKind::Exploration,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(
            ir::AtLeast::<_, 1>::from_one(support::limit()),
        )),
    );
    let implementation = support::StaticRule::new(
        "sort_impl",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                support::alternative(50),
                vec![support::alternative(10)],
            ),
        )),
    );
    let optimizer = support::optimizer(vec![&exploration, &implementation]);

    let result = support::optimize(&optimizer, support::source(), &support::config());

    assert_eq!(result.root().get(), 1);
    assert_eq!(result.roots().as_ref(), &[result.root()]);
    assert_eq!(result.memo().group_count(), 1);
    assert_eq!(result.memo().expression_count(), 2);
    assert_eq!(result.guardrail(), None);
    assert_eq!(result.metrics().memo_exprs, 2);
    assert_eq!(result.metrics().alternatives_considered, 4);
    let retained = result
        .physical()
        .iter()
        .find(|group| group.group == result.root())
        .unwrap();
    assert_eq!(
        retained
            .alternatives
            .iter()
            .map(|entry| entry.source_expr.get())
            .collect::<Vec<_>>(),
        vec![1, 1, 2, 2]
    );
    assert!(retained
        .alternatives
        .iter()
        .all(|entry| entry.provenance.rule_id().as_ref() == "sort_impl"));
    assert_eq!(
        result.best_alternative(result.root()).unwrap().cost.latency,
        cost::LatencyEstimate::micros(10)
    );
    assert_eq!(
        result
            .best_alternative_entry(result.root())
            .unwrap()
            .source_expr
            .get(),
        1
    );
    let best_plan = result.best_plan(result.root()).unwrap();
    assert_eq!(best_plan.entry.source_expr, best_plan.source_expr.id);
    assert_eq!(best_plan.entry.provenance.rule_id().as_ref(), "sort_impl");
    assert_eq!(best_plan.source_expr.group, result.root());
    assert_eq!(best_plan.source_expr.expr, support::source());
    assert_eq!(
        result.metrics().selected_cost.latency,
        cost::LatencyEstimate::micros(10)
    );
    assert_eq!(exploration.metadata.id.as_ref(), "limit_explore");
}

#[test]
fn cascades_optimizer_returns_finished_seed_when_root_budget_is_exhausted() {
    let implementation = support::StaticRule::new(
        "custom_seed_budget_impl",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(support::alternative(7)),
        )),
    );
    let mut config = support::config();
    config.limits.memo_groups = properties::PositiveUsize::new(1).unwrap();
    let optimizer = support::optimizer(vec![&implementation]);

    let result = support::optimize_many(
        &optimizer,
        ir::AtLeast::<_, 1>::from_one_and_rest(support::source(), vec![support::edge_source()]),
        &config,
    );

    assert_eq!(
        result.guardrail(),
        Some(optimizer::OptimizerGuardrail::MemoGroups)
    );
    assert_eq!(result.metrics().rule_fires, 0);
    assert_eq!(result.metrics().alternatives_considered, 0);
}

#[test]
fn cascades_optimizer_skips_inapplicable_known_rules_before_rule_budget() {
    let inapplicable = support::StaticRule::new(
        "seed_access_path",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(support::alternative(1)),
        )),
    );
    let applicable = support::StaticRule::new(
        "custom_source_impl",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(support::alternative(7)),
        )),
    );
    let mut config = support::config();
    config.limits.rule_fires = properties::PositiveUsize::new(1).unwrap();
    let optimizer = support::optimizer(vec![&inapplicable, &applicable]);

    let result = support::optimize(&optimizer, support::source(), &config);

    assert_eq!(result.guardrail(), None);
    assert_eq!(result.metrics().rule_fires, 1);
    assert_eq!(result.metrics().alternatives_considered, 1);
    let selected = result.best_plan(result.root()).unwrap();
    assert_eq!(
        selected.entry.provenance.rule_id().as_ref(),
        "custom_source_impl"
    );
    assert_eq!(
        selected.entry.alternative.cost.latency,
        cost::LatencyEstimate::micros(7)
    );
}

#[test]
fn cascades_optimizer_returns_logical_effect_guardrail() {
    let exploration = support::StaticRule::new(
        "custom_logical_budget",
        rules::RuleKind::Exploration,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(
            ir::AtLeast::<_, 1>::from_one(support::limit()),
        )),
    );
    let mut config = support::config();
    config.limits.memo_expressions = properties::PositiveUsize::new(1).unwrap();
    let optimizer = support::optimizer(vec![&exploration]);

    let result = support::optimize(&optimizer, support::source(), &config);

    assert_eq!(
        result.guardrail(),
        Some(optimizer::OptimizerGuardrail::MemoExpressions)
    );
    assert_eq!(result.metrics().rule_fires, 1);
}

#[test]
fn cascades_optimizer_returns_physical_effect_guardrail() {
    let implementation = support::StaticRule::new(
        "custom_physical_budget",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one_and_rest(
                support::alternative(1),
                vec![support::alternative(2)],
            ),
        )),
    );
    let mut config = support::config();
    config.limits.alternatives_per_group = properties::PositiveUsize::new(1).unwrap();
    let optimizer = support::optimizer(vec![&implementation]);

    let result = support::optimize(&optimizer, support::source(), &config);

    assert_eq!(
        result.guardrail(),
        Some(optimizer::OptimizerGuardrail::AlternativesPerGroup)
    );
    assert_eq!(result.metrics().rule_fires, 1);
    assert_eq!(result.metrics().alternatives_considered, 1);
}

#[test]
fn cascades_optimizer_stops_on_time_budget_between_tasks() {
    let mut config = support::config();
    config.limits.optimization_micros = properties::PositiveUsize::new(1_000).unwrap();
    let optimizer = support::optimizer(vec![&SleepAndExploreRule]);

    let result = support::optimize(&optimizer, support::source(), &config);

    assert_eq!(
        result.guardrail(),
        Some(optimizer::OptimizerGuardrail::TimeBudget)
    );
}

#[test]
fn cascades_optimizer_keeps_a_physical_alternative_for_every_root_after_time_budget() {
    // A one-millisecond budget is exhausted either by seeding the memo (debug
    // builds already spend longer than that on two roots) or, on faster
    // builds, by the first root's implementation rule, which sleeps past the
    // whole budget. Either way at least one root is popped after the budget
    // has expired. Stopping there must not leave that root without any
    // physical alternative: selection would otherwise fail with
    // `SelectionError::NoPhysicalAlternatives`, which callers see as
    // "selected optimizer result did not contain a best physical alternative".
    let mut config = support::config();
    config.limits.optimization_micros = properties::PositiveUsize::new(1_000).unwrap();
    let optimizer = support::optimizer(vec![&SleepAndImplementRule]);

    let result = support::optimize_many(
        &optimizer,
        ir::AtLeast::<_, 1>::from_one_and_rest(support::source(), vec![support::edge_source()]),
        &config,
    );

    assert_eq!(
        result.guardrail(),
        Some(optimizer::OptimizerGuardrail::TimeBudget)
    );
    for root in result.roots().iter() {
        let selected = result.best_plan(*root);
        assert!(
            selected.is_ok(),
            "root group {} has no physical alternative after the time budget expired: {:?}",
            root.get(),
            selected.err()
        );
    }
}

fn selected_expr_after_time_budget(
    result: &optimizer::OptimizationResult,
    group: memo::MemoGroupId,
) -> physical::PhysicalExpr {
    assert_eq!(
        result.guardrail(),
        Some(optimizer::OptimizerGuardrail::TimeBudget)
    );
    let selected = result.best_plan(group);
    assert!(
        selected.is_ok(),
        "root group {} has no physical alternative after the time budget expired: {:?}",
        group.get(),
        selected.err()
    );
    selected.unwrap().entry.alternative.expr.clone()
}

fn assert_empty_access_selected_after_time_budget(
    result: &optimizer::OptimizationResult,
    group: memo::MemoGroupId,
    element: properties::ElementKind,
) {
    let selected = selected_expr_after_time_budget(result, group);
    assert!(
        matches!(
            &selected,
            physical::PhysicalExpr::Access {
                element: delivered,
                access: physical::PhysicalAccess::Empty,
            } if *delivered == element
        ),
        "expected an empty {element:?} access after the time budget expired, got {selected:?}"
    );
}

#[test]
fn cascades_optimizer_implements_empty_input_root_branch_after_time_budget() {
    // A root branch over an empty access path is routed only to the
    // `root_control_flow_empty` rewrite; the branch implementation rule never
    // matches it. Seeding memoizes the input and body before the root, so the
    // root is the third popped task and a one-microsecond budget has expired
    // long before then. The rewrite must still run after the budget so the
    // root ends with the empty physical access instead of failing selection
    // with `SelectionError::NoPhysicalAlternatives`.
    let rules = rules::SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let mut config = support::config();
    config.limits.optimization_micros = properties::PositiveUsize::new(1).unwrap();
    let root = logical::LogicalExpr::RootBranch(logical::RootBranch::new(
        support::node_access(ir::NodeAccessPlan::Empty),
        ir::BranchPlan::Optional(Box::new(support::node_access(ir::NodeAccessPlan::AllScan))),
    ));

    let result = support::optimize(&optimizer, root, &config);

    assert_empty_access_selected_after_time_budget(
        &result,
        result.root(),
        properties::ElementKind::Node,
    );
}

#[test]
fn cascades_optimizer_implements_empty_input_root_repeat_after_time_budget() {
    // Same contract as the root branch case: an empty-input root repeat is
    // only implementable through the `root_control_flow_empty` rewrite, which
    // must survive the expired budget.
    let rules = rules::SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let mut config = support::config();
    config.limits.optimization_micros = properties::PositiveUsize::new(1).unwrap();
    let root = logical::LogicalExpr::RootRepeat(logical::RootRepeat::new(
        support::edge_access(ir::EdgeAccessPlan::Empty),
        ir::RepeatPlan {
            body: Box::new(support::edge_access(ir::EdgeAccessPlan::AllScan)),
            stop: ir::RepeatStopPlan::MaxDepthOnly,
            emit: ir::RepeatEmitPlan::None,
            max_depth: std::num::NonZeroUsize::new(2).unwrap(),
        },
    ));

    let result = support::optimize(&optimizer, root, &config);

    assert_empty_access_selected_after_time_budget(
        &result,
        result.root(),
        properties::ElementKind::Edge,
    );
}

/// Optimize `target` with the production rule set after the time budget has
/// already expired.
///
/// A leading all-scan root is popped and implemented first, and a
/// one-microsecond budget has expired by the time that is done, so `target`
/// is always popped after the budget. `target` is a shape whose
/// implementation rule defers to a required rewrite, so its group only keeps
/// a physical alternative if that rewrite still runs. Returns the result and
/// the memo group of `target`.
fn optimize_after_time_budget(
    target: logical::LogicalExpr,
) -> (optimizer::OptimizationResult, memo::MemoGroupId) {
    let rules = rules::SeedRuleSet::default();
    let optimizer = rules.optimizer();
    let mut config = support::config();
    config.limits.optimization_micros = properties::PositiveUsize::new(1).unwrap();

    let result = support::optimize_many(
        &optimizer,
        ir::AtLeast::<_, 1>::from_one_and_rest(
            support::node_access(ir::NodeAccessPlan::AllScan),
            vec![target],
        ),
        &config,
    );
    let group = result.roots().as_ref()[1];
    (result, group)
}

#[test]
fn cascades_optimizer_folds_empty_window_root_after_time_budget() {
    // `SeedAccessWindow` rejects a foldable window such as `limit(0)` and
    // leaves it to the `access_window` rewrite, which folds it into an empty
    // access path. That rewrite is the only route to a physical alternative,
    // so it must still run after the budget has expired.
    let root = logical::LogicalExpr::AccessWindow(logical::AccessWindow::new(
        support::node_access_path(ir::NodeAccessPlan::AllScan),
        logical::AccessWindowRange::new(0, Some(0)).unwrap(),
    ));

    let (result, group) = optimize_after_time_budget(root);

    assert_empty_access_selected_after_time_budget(&result, group, properties::ElementKind::Node);
}

#[test]
fn cascades_optimizer_elides_point_id_distinct_root_after_time_budget() {
    // `SeedAccessDistinct` rejects a distinct over point IDs, whose rows are
    // unique by construction, and leaves it to the `access_distinct` rewrite,
    // which drops the distinct and keeps the access path. After the budget
    // the root must still select the bare point read, with no distinct
    // operator, rather than fail selection.
    let root = logical::LogicalExpr::AccessDistinct(logical::AccessDistinct::new(
        support::node_access_path(ir::NodeAccessPlan::PointIds {
            ids: ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one_and_rest(7, vec![9])).unwrap(),
        }),
    ));

    let (result, group) = optimize_after_time_budget(root);

    let selected = selected_expr_after_time_budget(&result, group);
    assert!(
        matches!(
            &selected,
            physical::PhysicalExpr::Access {
                element: properties::ElementKind::Node,
                access: physical::PhysicalAccess::Kv(exec::KvReadPlan::MultiGet(_)),
            }
        ),
        "expected a bare point read after the time budget expired, got {selected:?}"
    );
}

#[test]
fn cascades_optimizer_collapses_adjacent_distinct_pipeline_root_after_time_budget() {
    // `SeedAccessPipeline` rejects a pipeline with adjacent distinct operators
    // and leaves it to the `access_pipeline_simplification` rewrite, which
    // removes one redundant distinct per firing. Three distincts need two
    // firings, each re-queued into the same group, before the implementation
    // rule accepts the pipeline; the whole chain must run after the budget.
    let root = logical::LogicalExpr::AccessPipeline(
        logical::AccessPipeline::new(
            support::node_access_path(ir::NodeAccessPlan::AllScan),
            ir::AtLeast::<_, 1>::from_one_and_rest(
                logical::StreamPipelineOp::Distinct,
                vec![
                    logical::StreamPipelineOp::Distinct,
                    logical::StreamPipelineOp::Distinct,
                ],
            ),
        )
        .unwrap(),
    );

    let (result, group) = optimize_after_time_budget(root);

    let selected = selected_expr_after_time_budget(&result, group);
    let physical::PhysicalExpr::Pipeline(pipeline) = &selected else {
        panic!("expected a physical pipeline after the time budget expired, got {selected:?}");
    };
    assert!(
        matches!(
            pipeline.ops(),
            [
                physical::PhysicalPipelineOp::Access { .. },
                physical::PhysicalPipelineOp::Stream(physical::PhysicalStreamOp::Distinct),
            ]
        ),
        "expected a single distinct over the scan after the time budget expired, got {:?}",
        pipeline.ops()
    );
}

#[test]
fn cascades_optimizer_drops_filter_over_empty_access_root_after_time_budget() {
    // `SeedAccessFilter` rejects a filter over a direct empty access path and
    // leaves it to the `access_filter_simplification` rewrite, which replaces
    // the filter with the empty access. Same contract as the window case: the
    // rewrite is the only route to a physical alternative for this shape.
    let root = logical::LogicalExpr::AccessFilter(logical::AccessFilter::new(
        support::node_access_path(ir::NodeAccessPlan::Empty),
        ir::PredicatePlan::new(helix_ast::expr::Predicate::eq("active", true)).unwrap(),
    ));

    let (result, group) = optimize_after_time_budget(root);

    assert_empty_access_selected_after_time_budget(&result, group, properties::ElementKind::Node);
}

#[test]
fn cascades_optimizer_charges_routed_not_applicable_rules_to_rule_budget() {
    let inapplicable = support::StaticRule::new(
        "custom_not_applicable",
        rules::RuleKind::Exploration,
        optimizer::RuleResult::NotApplicable,
    );
    let implementation = support::StaticRule::new(
        "custom_after_not_applicable",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(support::alternative(7)),
        )),
    );
    let mut config = support::config();
    config.limits.rule_fires = properties::PositiveUsize::new(1).unwrap();
    let optimizer = support::optimizer(vec![&inapplicable, &implementation]);

    let result = support::optimize(&optimizer, support::source(), &config);

    assert_eq!(
        result.guardrail(),
        Some(optimizer::OptimizerGuardrail::RuleFires)
    );
    assert_eq!(result.metrics().rule_fires, 1);
    assert_eq!(result.metrics().alternatives_considered, 0);
}

#[test]
fn cascades_optimizer_records_rejected_rules_and_continues() {
    let rejected = support::StaticRule::new(
        "custom_rejected",
        rules::RuleKind::Exploration,
        optimizer::RuleResult::Rejected(rules::RuleRejection::new("missing_index").unwrap()),
    );
    let implementation = support::StaticRule::new(
        "custom_after_rejection",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(support::alternative(11)),
        )),
    );
    let optimizer = support::optimizer(vec![&rejected, &implementation]);

    let result = support::optimize(&optimizer, support::source(), &support::config());

    assert_eq!(result.guardrail(), None);
    assert_eq!(result.metrics().rule_fires, 2);
    assert_eq!(result.metrics().rejected_alternatives, 1);
    assert_eq!(result.metrics().alternatives_considered, 1);
    assert_eq!(
        result.best_alternative(result.root()).unwrap().cost.latency,
        cost::LatencyEstimate::micros(11)
    );
}

#[test]
fn cascades_optimizer_tracks_many_roots_and_sums_selected_costs() {
    let implementation = support::StaticRule::new(
        "many_roots_impl",
        rules::RuleKind::Implementation,
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Physical(
            ir::AtLeast::<_, 1>::from_one(support::alternative(7)),
        )),
    );
    let optimizer = support::optimizer(vec![&implementation]);

    let result = support::optimize_many(
        &optimizer,
        ir::AtLeast::<_, 1>::from_one_and_rest(support::source(), vec![support::edge_source()]),
        &support::config(),
    );

    assert_eq!(
        result
            .roots()
            .iter()
            .map(|root| root.get())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(result.memo().group_count(), 2);
    assert_eq!(result.metrics().alternatives_considered, 2);
    assert_eq!(
        result.metrics().selected_cost.latency,
        cost::LatencyEstimate::micros(14)
    );
    let second_root = result.roots().as_ref()[1];
    let second_plan = result.best_plan(second_root).unwrap();
    assert_eq!(second_plan.source_expr.expr, support::edge_source());
    assert_eq!(
        second_plan.entry.provenance.rule_id().as_ref(),
        "many_roots_impl"
    );
}

#[test]
fn cascades_optimizer_records_shared_child_groups_for_composed_roots() {
    let implementation = support::StaticRule::new(
        "root_pipeline_impl",
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

    assert_eq!(result.memo().group_count(), 3);
    assert_eq!(result.memo().expression_count(), 3);
    assert_eq!(result.metrics().alternatives_considered, 3);

    let first = result.best_plan(result.roots().as_ref()[0]).unwrap();
    let second = result.best_plan(result.roots().as_ref()[1]).unwrap();
    assert_eq!(first.source_expr.children.len(), 1);
    assert_eq!(first.source_expr.children, second.source_expr.children);

    let child_group = first.source_expr.children.as_slice()[0];
    let child_group = result
        .memo()
        .groups()
        .iter()
        .find(|group| group.id == child_group)
        .unwrap();
    assert_eq!(child_group.expressions.len(), 1);
    assert_eq!(
        child_group.expressions[0].expr,
        support::variable_root_pipeline(1)
    );
}
