pub(super) use std::num::NonZeroUsize;

use helix_ast::expr::Predicate;
pub(super) use helix_ast::value::PropertyValue;

pub(super) use super::super::super::test_support;
pub(super) use super::super::super::{ExecutionResult, ExecutionScalar, ExecutionValue};
pub(super) use super::super::support::context_variable;
pub(super) use helix_planner::{context, exec, ir};

pub(super) fn access_param_op(param: ir::NonEmptyString) -> exec::ExecOp {
    exec::ExecOp::Access {
        plan: Box::new(exec::ExecAccessPlan::Node(
            exec::ExecNodeAccessPlan::FromParam { param },
        )),
    }
}

pub(super) fn source_context_subplan() -> exec::ExecutableSubplan {
    test_support::subplan(
        vec![test_support::step(
            1,
            Vec::new(),
            exec::ExecOp::Variable {
                op: exec::ExecVariableOp::SourceInject {
                    variable: context_variable(),
                },
            },
        )],
        1,
    )
}

pub(super) fn source_context_limit_subplan(count: usize) -> exec::ExecutableSubplan {
    let source = exec::ExecStepId::new(1).expect("positive step id");
    test_support::subplan(
        vec![
            test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Variable {
                    op: exec::ExecVariableOp::SourceInject {
                        variable: context_variable(),
                    },
                },
            ),
            test_support::step(
                2,
                vec![source],
                exec::ExecOp::Limit {
                    count: ir::StreamBoundPlan::Literal(count),
                },
            ),
        ],
        2,
    )
}

pub(super) fn source_context_expand_nodes_subplan(label: &str) -> exec::ExecutableSubplan {
    let source = exec::ExecStepId::new(1).expect("positive step id");
    test_support::subplan(
        vec![
            test_support::step(
                1,
                Vec::new(),
                exec::ExecOp::Variable {
                    op: exec::ExecVariableOp::SourceInject {
                        variable: context_variable(),
                    },
                },
            ),
            test_support::step(
                2,
                vec![source],
                exec::ExecOp::Expand {
                    plan: ir::ExpandPlan {
                        direction: ir::ExpandDirection::Out,
                        label: ir::ExpandLabelPlan::Label(test_support::name(label)),
                        output: ir::ExpandOutput::Nodes,
                    },
                },
            ),
        ],
        2,
    )
}

pub(super) fn access_param_subplan(param: ir::NonEmptyString) -> exec::ExecutableSubplan {
    test_support::subplan(
        vec![test_support::step(1, Vec::new(), access_param_op(param))],
        1,
    )
}

pub(super) fn add_edge_to_param_subplan(
    from_param: ir::NonEmptyString,
    to_param: ir::NonEmptyString,
    label: &str,
) -> exec::ExecutableSubplan {
    let access = exec::ExecStepId::new(1).expect("positive step id");
    test_support::subplan(
        vec![
            test_support::step(1, Vec::new(), access_param_op(from_param)),
            test_support::step(
                2,
                vec![access],
                exec::ExecOp::Mutation {
                    plan: exec::ExecMutationPlan::AddEdge {
                        label: test_support::name(label),
                        to: ir::NodeTargetPlan::FromParam { param: to_param },
                        properties: test_support::assignments(Vec::new()),
                    },
                },
            ),
        ],
        2,
    )
}

pub(super) fn name_eq(value: &str) -> ir::PredicatePlan {
    ir::PredicatePlan::new(Predicate::eq("name", value)).expect("valid predicate")
}

pub(super) fn scalars(result: ExecutionResult) -> Vec<ExecutionScalar> {
    let Some(ExecutionValue::Scalars(values)) = result.last else {
        panic!("expected scalar result");
    };
    values
}
