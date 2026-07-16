use super::support;
use crate::{context, exec, ir, logical, memo, optimizer, rules};

#[test]
fn selected_mutation_plan_selects_child_inputs_for_all_mutation_shapes() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);
    let mut metrics = exec::PlannerMetrics::default();
    let cases = vec![
        ir::MutationPlan::AddNode {
            input: ir::MutationInput::FromInput {
                input: Box::new(support::node_root()),
            },
            label: support::name("User"),
            properties: ir::PropertyAssignments::default(),
        },
        ir::MutationPlan::AddEdge {
            input: Box::new(support::node_root()),
            label: support::name("LIKES"),
            to: ir::NodeTargetPlan::PointIds {
                ids: support::ids(1),
            },
            properties: ir::PropertyAssignments::default(),
        },
        support::input_mutation(),
        ir::MutationPlan::RemoveProperty {
            input: Box::new(support::node_root()),
            name: support::name("active"),
        },
        ir::MutationPlan::Drop {
            input: Box::new(support::node_root()),
        },
        ir::MutationPlan::DropEdge {
            input: Box::new(support::node_root()),
            to: ir::NodeTargetPlan::PointIds {
                ids: support::ids(2),
            },
        },
        ir::MutationPlan::DropEdgeLabeled {
            input: Box::new(support::node_root()),
            to: ir::NodeTargetPlan::PointIds {
                ids: support::ids(3),
            },
            label: support::name("LIKES"),
        },
        ir::MutationPlan::DropEdgeById {
            input: ir::MutationInput::FromInput {
                input: Box::new(support::edge_root()),
            },
            edges: ir::EdgeTargetPlan::PointIds {
                ids: support::ids(4),
            },
        },
    ];

    for plan in cases {
        let result = support::optimizer_result(
            &ctx,
            logical::LogicalExpr::RootMutation(logical::RootMutation::new(plan.clone())),
        );
        let mut selection = result.selection_session();
        let child_plans = support::root_child_availability(&result, &mut selection);
        let selected = planner
            .selected_mutation_plan(&plan, child_plans, &mut metrics)
            .expect("test mutation child roots are selectable");

        match selected {
            exec::SelectedMutationPlan::AddNode {
                input: exec::SelectedMutationInput::FromInput(input),
                ..
            }
            | exec::SelectedMutationPlan::DropEdgeById {
                input: exec::SelectedMutationInput::FromInput(input),
                ..
            } => assert!(matches!(
                input.as_ref(),
                exec::SelectedExecutableRunRoot::Alternative(_)
            )),
            exec::SelectedMutationPlan::AddEdge { input, .. }
            | exec::SelectedMutationPlan::SetProperty { input, .. }
            | exec::SelectedMutationPlan::RemoveProperty { input, .. }
            | exec::SelectedMutationPlan::Drop { input }
            | exec::SelectedMutationPlan::DropEdge { input, .. }
            | exec::SelectedMutationPlan::DropEdgeLabeled { input, .. } => assert!(matches!(
                input.as_ref(),
                exec::SelectedExecutableRunRoot::Alternative(_)
            )),
            exec::SelectedMutationPlan::AddNode {
                input: exec::SelectedMutationInput::Source,
                ..
            }
            | exec::SelectedMutationPlan::DropEdgeById {
                input: exec::SelectedMutationInput::Source,
                ..
            } => panic!("test cases use input-consuming mutation shapes"),
        }
    }

    assert_eq!(
        metrics.memo_groups, 0,
        "memo-child reconstruction must not double-count optimizer work metrics"
    );
}

#[test]
fn selected_mutation_plan_rejects_memo_child_arity_mismatch() {
    let ctx = context::PlannerContext::default();
    let mut planner = support::selected_planner(&ctx);
    let mut metrics = exec::PlannerMetrics::default();
    let config = optimizer::OptimizerConfig::from_context(&ctx);
    let result = rules::SeedRuleSet::default()
        .optimizer()
        .optimize(support::node_root(), &config)
        .expect("test optimizer memo allocation should fit");
    let mut selection = result.selection_session();
    let malformed_child_plans = super::super::memo_children::MemoChildPlanContext::for_test(
        &mut selection,
        vec![
            memo::MemoGroupId::new(1).unwrap(),
            memo::MemoGroupId::new(1).unwrap(),
        ],
    );

    assert_eq!(
        planner
            .selected_mutation_plan(
                &support::input_mutation(),
                super::super::memo_children::MemoChildPlanAvailability::Available(
                    malformed_child_plans,
                ),
                &mut metrics,
            )
            .unwrap_err(),
        super::super::super::rejection::unsupported(
            super::super::super::rejection::Reason::MemoChildArityMismatch
        ),
        "selected mutation reconstruction must fail closed with a typed reason when optimizer provenance has the wrong child arity"
    );
    assert_eq!(
        metrics.memo_groups, 0,
        "malformed memo child provenance must not start child re-optimization"
    );
}
