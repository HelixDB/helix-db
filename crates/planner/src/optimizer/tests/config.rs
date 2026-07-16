use super::support;
use crate::{context, cost, feedback, ir, optimizer, rules};

#[test]
fn optimizer_config_comes_from_context_and_rule_result_reports_outcomes() {
    let ctx = context::PlannerContext {
        storage: cost::StorageCostProfile {
            default_unknown_scan_rows: cost::EstimatedRows::rows(12),
            ..cost::StorageCostProfile::default()
        },
        ..context::PlannerContext::default()
    };
    let config = optimizer::OptimizerConfig::from_context(&ctx);

    assert_eq!(
        config.storage.default_unknown_scan_rows,
        cost::EstimatedRows::rows(12)
    );
    assert_eq!(
        optimizer::RuleResult::NotApplicable.outcome(),
        rules::RuleOutcome::NotApplicable
    );
    assert_eq!(
        optimizer::RuleResult::Rejected(rules::RuleRejection::new("missing_index").unwrap())
            .outcome(),
        rules::RuleOutcome::Rejected
    );
    assert_eq!(
        optimizer::RuleResult::Applied(optimizer::RuleEffect::Logical(
            ir::AtLeast::<_, 1>::from_one(support::source())
        ))
        .outcome(),
        rules::RuleOutcome::Applied
    );
}

#[test]
fn optimizer_config_applies_runtime_feedback_to_stats() {
    let label = ir::NonEmptyString::new("User").unwrap();
    let ctx = context::PlannerContext {
        stats: context::StatsSnapshot::default().with_node_label_cardinality(label.clone(), 100),
        ..context::PlannerContext::default()
    }
    .with_runtime_feedback(
        feedback::RuntimeFeedbackSnapshot::default()
            .with_node_label_cardinality(label.clone(), feedback::ObservedRows::rows(7)),
    );

    let config = optimizer::OptimizerConfig::from_context(&ctx);

    assert_eq!(ctx.stats.node_label_cardinality[&label], 100);
    assert_eq!(config.stats.node_label_cardinality[&label], 7);
}
