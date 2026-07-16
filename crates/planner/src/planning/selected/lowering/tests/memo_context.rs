use super::support;
use crate::{context, exec, ir, logical, physical};

#[test]
fn child_bearing_root_reconstruction_requires_memo_child_context() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);
    let mut metrics = exec::PlannerMetrics::default();

    let branch = logical::LogicalExpr::RootBranch(logical::RootBranch::new(
        support::node_root(),
        ir::BranchPlan::ChooseElse {
            condition: support::predicate(),
            then_plan: Box::new(support::node_root()),
            else_plan: Box::new(support::edge_root()),
        },
    ));

    assert_eq!(
        planner
            .selected_run_root_from_plan(
                branch,
                support::control_alternative(physical::PhysicalControlOp::Branch),
                support::optimizer_provenance(),
                &mut metrics,
            )
            .unwrap_err(),
        super::super::super::rejection::unsupported(
            super::super::super::rejection::Reason::MemoChildContextMissing
        )
    );

    let repeat = logical::LogicalExpr::RootRepeat(logical::RootRepeat::new(
        support::node_root(),
        support::repeat_plan(),
    ));
    assert_eq!(
        planner
            .selected_run_root_from_plan(
                repeat,
                support::control_alternative(physical::PhysicalControlOp::Repeat),
                support::optimizer_provenance(),
                &mut metrics,
            )
            .unwrap_err(),
        super::super::super::rejection::unsupported(
            super::super::super::rejection::Reason::MemoChildContextMissing
        )
    );
}

#[test]
fn input_mutation_reconstruction_requires_memo_child_context() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);
    let mut metrics = exec::PlannerMetrics::default();

    assert_eq!(
        planner
            .selected_mutation_plan(
                &support::input_mutation(),
                super::super::memo_children::MemoChildPlanAvailability::Unavailable,
                &mut metrics,
            )
            .unwrap_err(),
        super::super::super::rejection::unsupported(
            super::super::super::rejection::Reason::MemoChildContextMissing
        )
    );
}

#[test]
fn nested_root_stream_reconstruction_requires_memo_child_context() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);
    let mut metrics = exec::PlannerMetrics::default();

    assert_eq!(
        planner
            .selected_root_stream_input(
                &logical::RootStream::Branch(Box::new(logical::RootBranch::new(
                    support::node_root(),
                    support::branch_plan(),
                ))),
                &mut metrics,
            )
            .unwrap_err(),
        super::super::super::rejection::unsupported(
            super::super::super::rejection::Reason::MemoChildContextMissing
        )
    );
}
