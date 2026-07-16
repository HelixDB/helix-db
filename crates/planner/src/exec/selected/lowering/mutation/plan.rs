//! Selected mutation payload classification before executable emission.

use super::super::*;

pub(super) enum LoweredSelectedMutation {
    Source(ExecMutationPlan),
    Input {
        input: SelectedExecutableRunRoot,
        plan: ExecMutationPlan,
    },
}

pub(super) fn lower_selected_mutation_plan(plan: SelectedMutationPlan) -> LoweredSelectedMutation {
    match plan {
        SelectedMutationPlan::AddNode {
            input: SelectedMutationInput::Source,
            label,
            properties,
        } => LoweredSelectedMutation::Source(ExecMutationPlan::AddNodeSource { label, properties }),
        SelectedMutationPlan::AddNode {
            input: SelectedMutationInput::FromInput(input),
            label,
            properties,
        } => LoweredSelectedMutation::Input {
            input: *input,
            plan: ExecMutationPlan::AddNodeFromInput { label, properties },
        },
        SelectedMutationPlan::AddEdge {
            input,
            label,
            to,
            properties,
        } => LoweredSelectedMutation::Input {
            input: *input,
            plan: ExecMutationPlan::AddEdge {
                label,
                to,
                properties,
            },
        },
        SelectedMutationPlan::SetProperty { input, name, value } => {
            LoweredSelectedMutation::Input {
                input: *input,
                plan: ExecMutationPlan::SetProperty { name, value },
            }
        }
        SelectedMutationPlan::RemoveProperty { input, name } => LoweredSelectedMutation::Input {
            input: *input,
            plan: ExecMutationPlan::RemoveProperty { name },
        },
        SelectedMutationPlan::Drop { input } => LoweredSelectedMutation::Input {
            input: *input,
            plan: ExecMutationPlan::Drop,
        },
        SelectedMutationPlan::DropEdge { input, to } => LoweredSelectedMutation::Input {
            input: *input,
            plan: ExecMutationPlan::DropEdge { to },
        },
        SelectedMutationPlan::DropEdgeLabeled { input, to, label } => {
            LoweredSelectedMutation::Input {
                input: *input,
                plan: ExecMutationPlan::DropEdgeLabeled { to, label },
            }
        }
        SelectedMutationPlan::DropEdgeById {
            input: SelectedMutationInput::Source,
            edges,
        } => LoweredSelectedMutation::Source(ExecMutationPlan::DropEdgeByIdSource { edges }),
        SelectedMutationPlan::DropEdgeById {
            input: SelectedMutationInput::FromInput(input),
            edges,
        } => LoweredSelectedMutation::Input {
            input: *input,
            plan: ExecMutationPlan::DropEdgeByIdFromInput { edges },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cost, ir, logical, physical, properties};
    use helix_ast::value::PropertyInput;

    fn selected_input() -> SelectedExecutableRunRoot {
        SelectedExecutableRunRoot::alternative(
            logical::LogicalExpr::Pure(logical::PureLogicalOp::NoOp),
            physical::PhysicalAlternative::new(
                physical::PhysicalExpr::NoOp,
                properties::DeliveredProperties::default(),
                cost::CostVector::ZERO,
            ),
        )
    }

    #[test]
    fn classifies_source_mutation_payloads() {
        let add_node = lower_selected_mutation_plan(SelectedMutationPlan::AddNode {
            input: SelectedMutationInput::Source,
            label: ir::NonEmptyString::from_static("User"),
            properties: ir::PropertyAssignments::default(),
        });
        assert!(matches!(
            add_node,
            LoweredSelectedMutation::Source(ExecMutationPlan::AddNodeSource { .. })
        ));

        let drop_edge = lower_selected_mutation_plan(SelectedMutationPlan::DropEdgeById {
            input: SelectedMutationInput::Source,
            edges: ir::EdgeTargetPlan::Empty,
        });
        assert!(matches!(
            drop_edge,
            LoweredSelectedMutation::Source(ExecMutationPlan::DropEdgeByIdSource { .. })
        ));
    }

    #[test]
    fn classifies_input_consuming_mutation_payloads() {
        let add_node = lower_selected_mutation_plan(SelectedMutationPlan::AddNode {
            input: SelectedMutationInput::FromInput(Box::new(selected_input())),
            label: ir::NonEmptyString::from_static("User"),
            properties: ir::PropertyAssignments::default(),
        });
        assert!(matches!(
            add_node,
            LoweredSelectedMutation::Input {
                plan: ExecMutationPlan::AddNodeFromInput { .. },
                ..
            }
        ));

        let set_property = lower_selected_mutation_plan(SelectedMutationPlan::SetProperty {
            input: Box::new(selected_input()),
            name: ir::NonEmptyString::from_static("active"),
            value: ir::PropertyInputPlan::new(PropertyInput::from(true)).unwrap(),
        });
        assert!(matches!(
            set_property,
            LoweredSelectedMutation::Input {
                plan: ExecMutationPlan::SetProperty { .. },
                ..
            }
        ));

        let drop_edge = lower_selected_mutation_plan(SelectedMutationPlan::DropEdgeById {
            input: SelectedMutationInput::FromInput(Box::new(selected_input())),
            edges: ir::EdgeTargetPlan::Empty,
        });
        assert!(matches!(
            drop_edge,
            LoweredSelectedMutation::Input {
                plan: ExecMutationPlan::DropEdgeByIdFromInput { .. },
                ..
            }
        ));
    }
}
