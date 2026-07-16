use crate::feedback;
use crate::planning::tests::support::*;

#[test]
fn runtime_feedback_updates_selected_cost_through_effective_stats() {
    let age_key =
        ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc).unwrap();
    let base_ctx = PlannerContext {
        indexes: builtin_label_indexes().with_node_range(age_key.clone()),
        stats: StatsSnapshot::default().with_node_range_cardinality(age_key.clone(), 100),
        ..PlannerContext::default()
    };
    let feedback_ctx = base_ctx.clone().with_runtime_feedback(
        feedback::RuntimeFeedbackSnapshot::default()
            .with_node_range_cardinality(age_key, feedback::ObservedRows::rows(7)),
    );

    let base = executable_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21)),
        base_ctx,
    );
    let feedback = executable_traversal(
        g().n_with_label_where("User", Predicate::gte("age", 21)),
        feedback_ctx,
    );

    assert_eq!(base.metrics().selected_cost.range_nexts, 100);
    assert_eq!(feedback.metrics().selected_cost.range_nexts, 7);
}
