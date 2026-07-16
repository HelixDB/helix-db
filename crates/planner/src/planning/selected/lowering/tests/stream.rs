use super::support;
use crate::{context, exec, ir, logical};

#[test]
fn selected_root_stream_input_recurses_through_supported_root_stream_variants() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);
    let mut metrics = exec::PlannerMetrics::default();

    let access = planner
        .selected_root_stream_input(
            &logical::RootStream::Access(logical::AccessStream::Path(support::access_path())),
            &mut metrics,
        )
        .unwrap();
    assert!(matches!(access, exec::SelectedRootStreamInput::Access(_)));

    let variable = planner
        .selected_root_stream_input(&support::variable_stream(), &mut metrics)
        .unwrap();
    assert!(matches!(
        variable,
        exec::SelectedRootStreamInput::VariableSource(_)
    ));

    let mutation = support::selected_root_stream_with_parent_context(
        &mut planner,
        &ctx,
        &logical::RootStream::Mutation(Box::new(logical::RootMutation::new(
            support::source_mutation(),
        ))),
        &mut metrics,
    )
    .unwrap();
    assert!(matches!(
        mutation,
        exec::SelectedRootStreamInput::Mutation(_)
    ));

    let branch = support::selected_root_stream_with_parent_context(
        &mut planner,
        &ctx,
        &logical::RootStream::Branch(Box::new(logical::RootBranch::new(
            support::node_root(),
            support::branch_plan(),
        ))),
        &mut metrics,
    )
    .unwrap();
    assert!(matches!(branch, exec::SelectedRootStreamInput::Branch(_)));

    let repeat = support::selected_root_stream_with_parent_context(
        &mut planner,
        &ctx,
        &logical::RootStream::Repeat(Box::new(logical::RootRepeat::new(
            support::node_root(),
            support::repeat_plan(),
        ))),
        &mut metrics,
    )
    .unwrap();
    assert!(matches!(repeat, exec::SelectedRootStreamInput::Repeat(_)));

    let pipeline = support::selected_root_stream_with_parent_context(
        &mut planner,
        &ctx,
        &logical::RootStream::Pipeline(Box::new(support::root_pipeline())),
        &mut metrics,
    )
    .unwrap();
    assert!(matches!(
        pipeline,
        exec::SelectedRootStreamInput::Pipeline(_)
    ));

    let reserved = support::selected_root_stream_with_parent_context(
        &mut planner,
        &ctx,
        &logical::RootStream::Reserved(Box::new(logical::StreamReserved::new(
            support::variable_stream(),
            ir::ReservedOp::Fold,
        ))),
        &mut metrics,
    )
    .unwrap();
    assert!(matches!(
        reserved,
        exec::SelectedRootStreamInput::Terminal(_)
    ));

    let project = support::selected_root_stream_with_parent_context(
        &mut planner,
        &ctx,
        &logical::RootStream::Project(Box::new(logical::StreamProject::new(
            support::variable_stream(),
            ir::ProjectionPlan::Count,
        ))),
        &mut metrics,
    )
    .unwrap();
    assert!(matches!(
        project,
        exec::SelectedRootStreamInput::Terminal(_)
    ));

    let aggregate = support::selected_root_stream_with_parent_context(
        &mut planner,
        &ctx,
        &logical::RootStream::Aggregate(Box::new(logical::StreamAggregate::new(
            support::variable_stream(),
            ir::AggregatePlan::Group(support::name("kind")),
        ))),
        &mut metrics,
    )
    .unwrap();
    assert!(matches!(
        aggregate,
        exec::SelectedRootStreamInput::Terminal(_)
    ));

    let write = support::selected_root_stream_with_parent_context(
        &mut planner,
        &ctx,
        &logical::RootStream::VariableWrite(Box::new(logical::StreamVariableWrite::new(
            support::variable_stream(),
            logical::StreamVariableWriteOp::Store(support::name("saved")),
        ))),
        &mut metrics,
    )
    .unwrap();
    assert!(matches!(write, exec::SelectedRootStreamInput::Terminal(_)));
    assert_eq!(
        metrics.memo_groups, 0,
        "memo-child reconstruction must not double-count optimizer work metrics"
    );
}
