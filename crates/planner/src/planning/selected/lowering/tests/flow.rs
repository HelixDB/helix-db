use super::support;
use crate::{context, exec, ir, logical};

#[test]
fn selected_branch_and_repeat_reconstruction_batch_child_roots() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);
    let mut metrics = exec::PlannerMetrics::default();
    let branch = logical::RootBranch::new(
        support::node_root(),
        ir::BranchPlan::ChooseElse {
            condition: support::predicate(),
            then_plan: Box::new(support::node_root()),
            else_plan: Box::new(support::edge_root()),
        },
    );
    let branch_result =
        support::optimizer_result(&ctx, logical::LogicalExpr::RootBranch(branch.clone()));
    let mut branch_selection = branch_result.selection_session();
    let branch_child_plans = support::root_child_context(&branch_result, &mut branch_selection);

    let (input, plan) = planner
        .selected_branch_input_and_plan(&branch, branch_child_plans, &mut metrics)
        .expect("branch input and both arms are selectable");

    assert!(matches!(
        input,
        exec::SelectedExecutableRunRoot::Alternative(_)
    ));
    assert!(matches!(plan, exec::SelectedBranchPlan::ChooseElse { .. }));

    let repeat = logical::RootRepeat::new(support::node_root(), support::repeat_plan());
    let repeat_result =
        support::optimizer_result(&ctx, logical::LogicalExpr::RootRepeat(repeat.clone()));
    let mut repeat_selection = repeat_result.selection_session();
    let repeat_child_plans = support::root_child_context(&repeat_result, &mut repeat_selection);
    let (input, repeat_plan) = planner
        .selected_repeat_input_and_plan(&repeat, repeat_child_plans, &mut metrics)
        .expect("repeat input and body are selectable");

    assert!(matches!(
        input,
        exec::SelectedExecutableRunRoot::Alternative(_)
    ));
    assert_eq!(repeat_plan.max_depth.get(), 2);
    assert!(matches!(
        repeat_plan.body.as_ref(),
        exec::SelectedExecutableRunRoot::Alternative(_)
    ));
    assert_eq!(
        metrics.memo_groups, 0,
        "memo-child reconstruction must not double-count optimizer work metrics"
    );
}
