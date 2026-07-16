use super::support;
use crate::{context, cost, exec, ir, logical};

#[test]
fn selected_logical_run_root_reconstructs_nested_pipeline_from_memo_child_plan() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);

    let selected = planner
        .selected_logical_run_root(support::selectable_root(support::nested_root_pipeline()))
        .expect("nested root pipelines are selectable");

    let exec::SelectedExecutableRunRoot::Pipeline(outer) = selected.root else {
        panic!("expected outer selected root pipeline");
    };
    let parent_provenance = outer.provenance().optimizer();
    assert_eq!(parent_provenance.source_children().len(), 1);
    let exec::SelectedRootStreamInput::Pipeline(inner) = outer.input() else {
        panic!("expected inner pipeline selected from memo child group");
    };
    let inner_provenance = inner.provenance().optimizer();

    assert_eq!(
        inner_provenance.group(),
        parent_provenance.source_children()[0]
    );
    assert_eq!(selected.metrics.memo_groups, 2);
    assert_eq!(selected.metrics.memo_exprs, 2);
    assert_eq!(selected.metrics.alternatives_considered, 2);
}

#[test]
fn selected_logical_run_root_reconstructs_nested_terminal_from_memo_child_plan() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);
    let project =
        logical::StreamProject::new(support::variable_stream(), ir::ProjectionPlan::Count);
    let aggregate = logical::LogicalExpr::StreamAggregate(logical::StreamAggregate::new(
        logical::RootStream::Project(Box::new(project)),
        ir::AggregatePlan::Group(support::name("kind")),
    ));

    let selected = planner
        .selected_logical_run_root(support::selectable_root(aggregate))
        .expect("nested terminal roots are selectable");

    let exec::SelectedExecutableRunRoot::Terminal(outer) = selected.root else {
        panic!("expected outer selected terminal root");
    };
    let parent_provenance = outer.provenance().optimizer();
    assert_eq!(parent_provenance.source_children().len(), 1);
    let exec::SelectedRootTerminal::Aggregate { input, .. } = outer.plan() else {
        panic!("expected selected aggregate terminal");
    };
    let exec::SelectedRootStreamInput::Terminal(inner) = input else {
        panic!("expected inner terminal selected from memo child group");
    };
    let inner_provenance = inner.provenance().optimizer();

    assert_eq!(
        inner_provenance.group(),
        parent_provenance.source_children()[0]
    );
    assert_eq!(selected.metrics.memo_groups, 2);
    assert_eq!(selected.metrics.memo_exprs, 2);
    assert_eq!(selected.metrics.alternatives_considered, 2);
}

#[test]
fn selected_logical_run_root_reconstructs_control_flow_from_memo_child_plans() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);
    let branch = logical::LogicalExpr::RootBranch(logical::RootBranch::new(
        support::node_root(),
        ir::BranchPlan::ChooseElse {
            condition: support::predicate(),
            then_plan: Box::new(support::node_root()),
            else_plan: Box::new(support::edge_root()),
        },
    ));

    let selected = planner
        .selected_logical_run_root(support::selectable_root(branch))
        .expect("branch children are selectable memo roots");
    let exec::SelectedExecutableRunRoot::Branch(branch) = selected.root else {
        panic!("expected selected branch root");
    };
    let provenance = branch.provenance().optimizer();
    assert_eq!(provenance.source_children().len(), 3);
    assert_eq!(
        support::selected_optimizer_group(branch.input()),
        provenance.source_children()[0]
    );
    let exec::SelectedBranchPlan::ChooseElse {
        then_plan,
        else_plan,
        ..
    } = branch.plan()
    else {
        panic!("expected selected choose-else branch");
    };
    assert_eq!(
        support::selected_optimizer_group(then_plan.as_ref()),
        provenance.source_children()[1]
    );
    assert_eq!(
        support::selected_optimizer_group(else_plan.as_ref()),
        provenance.source_children()[2]
    );
    assert_eq!(selected.metrics.memo_groups, 3);
    assert_eq!(selected.metrics.alternatives_considered, 3);
    let profile = cost::StorageCostProfile::default();
    let access_cost = profile.range_scan(profile.default_unknown_scan_rows);
    assert_eq!(
        selected.metrics.selected_cost,
        profile
            .barrier()
            .serial(access_cost)
            .serial(access_cost)
            .serial(access_cost)
    );

    let repeat = logical::LogicalExpr::RootRepeat(logical::RootRepeat::new(
        support::node_root(),
        support::repeat_plan(),
    ));
    let selected = planner
        .selected_logical_run_root(support::selectable_root(repeat))
        .expect("repeat children are selectable memo roots");
    let exec::SelectedExecutableRunRoot::Repeat(repeat) = selected.root else {
        panic!("expected selected repeat root");
    };
    let provenance = repeat.provenance().optimizer();
    assert_eq!(provenance.source_children().len(), 2);
    assert_eq!(
        support::selected_optimizer_group(repeat.input()),
        provenance.source_children()[0]
    );
    assert_eq!(
        support::selected_optimizer_group(repeat.plan().body.as_ref()),
        provenance.source_children()[1]
    );
    assert_eq!(
        selected.metrics.selected_cost,
        profile
            .stream_operator(profile.default_unknown_scan_rows)
            .serial(access_cost)
            .serial(access_cost)
    );
}

#[test]
fn selected_logical_run_root_reconstructs_input_mutation_from_memo_child_plan() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);
    let mutation =
        logical::LogicalExpr::RootMutation(logical::RootMutation::new(support::input_mutation()));

    let selected = planner
        .selected_logical_run_root(support::selectable_root(mutation))
        .expect("input-consuming mutation child is selectable memo root");
    let exec::SelectedExecutableRunRoot::Mutation(mutation) = selected.root else {
        panic!("expected selected mutation root");
    };
    let provenance = mutation.provenance().optimizer();
    assert_eq!(provenance.source_children().len(), 1);
    let exec::SelectedMutationPlan::SetProperty { input, .. } = mutation.plan() else {
        panic!("expected selected set-property mutation");
    };
    assert_eq!(
        support::selected_optimizer_group(input.as_ref()),
        provenance.source_children()[0]
    );
    assert_eq!(selected.metrics.memo_groups, 2);
    assert_eq!(selected.metrics.alternatives_considered, 2);
    let profile = cost::StorageCostProfile::default();
    assert_eq!(
        selected.metrics.selected_cost,
        profile
            .barrier()
            .serial(profile.range_scan(profile.default_unknown_scan_rows))
    );
}
