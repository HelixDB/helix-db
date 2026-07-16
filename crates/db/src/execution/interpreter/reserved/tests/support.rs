pub(super) use std::collections::BTreeSet;

pub(super) use helix_ast::value::PropertyValue;
pub(super) use helix_planner::{context, exec, ir};

pub(super) use crate::encoding::property::property_value::PropertyValue as DbPropertyValue;

pub(super) use super::super::super::test_support;
pub(super) use super::super::super::{ElementRef, ExecutionScalar, ExecutionValue};

pub(super) fn all_nodes_step(id: usize) -> exec::ExecStep {
    test_support::step(
        id,
        Vec::new(),
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::AllScan,
            )),
        },
    )
}

pub(super) fn node_param_step(id: usize, param: ir::NonEmptyString) -> exec::ExecStep {
    test_support::step(
        id,
        Vec::new(),
        exec::ExecOp::Access {
            plan: Box::new(exec::ExecAccessPlan::Node(
                exec::ExecNodeAccessPlan::FromParam { param },
            )),
        },
    )
}

pub(super) fn expand_out_step(id: usize, dependency: usize) -> exec::ExecStep {
    test_support::step(
        id,
        vec![exec::ExecStepId::new(dependency).expect("positive dependency step id")],
        exec::ExecOp::Expand {
            plan: ir::ExpandPlan {
                direction: ir::ExpandDirection::Out,
                output: ir::ExpandOutput::Nodes,
                label: ir::ExpandLabelPlan::Label(test_support::name("FOLLOWS")),
            },
        },
    )
}

pub(super) fn reserved_step(id: usize, dependency: usize, op: ir::ReservedOp) -> exec::ExecStep {
    test_support::step(
        id,
        vec![exec::ExecStepId::new(dependency).expect("positive dependency step id")],
        exec::ExecOp::Reserved { op },
    )
}

pub(super) fn project_id_step(id: usize, dependency: usize) -> exec::ExecStep {
    project_step(id, dependency, ir::ProjectionPlan::Id)
}

pub(super) fn project_count_step(id: usize, dependency: usize) -> exec::ExecStep {
    project_step(id, dependency, ir::ProjectionPlan::Count)
}

pub(super) fn project_step(
    id: usize,
    dependency: usize,
    projection: ir::ProjectionPlan,
) -> exec::ExecStep {
    test_support::step(
        id,
        vec![exec::ExecStepId::new(dependency).expect("positive dependency step id")],
        exec::ExecOp::Project { projection },
    )
}
