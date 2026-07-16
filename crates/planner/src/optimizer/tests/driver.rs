use super::support;
use crate::{cost, ir, optimizer, properties, rules};

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
