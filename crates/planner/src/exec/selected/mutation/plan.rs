//! Selected executable mutation payload ADT.

use super::input::SelectedMutationInput;
use crate::exec::selected::run::SelectedExecutableRunRoot;
use crate::ir;

/// Selected mutation plan.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectedMutationPlan {
    /// Add node.
    AddNode {
        /// Source or input-stream mode.
        input: SelectedMutationInput,
        /// Label.
        label: ir::NonEmptyString,
        /// Properties.
        properties: ir::PropertyAssignments,
    },
    /// Add edge.
    AddEdge {
        /// Selected input run.
        input: Box<SelectedExecutableRunRoot>,
        /// Label.
        label: ir::NonEmptyString,
        /// Target nodes.
        to: ir::NodeTargetPlan,
        /// Properties.
        properties: ir::PropertyAssignments,
    },
    /// Set property.
    SetProperty {
        /// Selected input run.
        input: Box<SelectedExecutableRunRoot>,
        /// Name.
        name: ir::NonEmptyString,
        /// Value.
        value: ir::PropertyInputPlan,
    },
    /// Remove property.
    RemoveProperty {
        /// Selected input run.
        input: Box<SelectedExecutableRunRoot>,
        /// Name.
        name: ir::NonEmptyString,
    },
    /// Drop nodes.
    Drop {
        /// Selected input run.
        input: Box<SelectedExecutableRunRoot>,
    },
    /// Drop edges.
    DropEdge {
        /// Selected input run.
        input: Box<SelectedExecutableRunRoot>,
        /// Target nodes.
        to: ir::NodeTargetPlan,
    },
    /// Drop labeled edges.
    DropEdgeLabeled {
        /// Selected input run.
        input: Box<SelectedExecutableRunRoot>,
        /// Target nodes.
        to: ir::NodeTargetPlan,
        /// Edge label.
        label: ir::NonEmptyString,
    },
    /// Drop edges by ID.
    DropEdgeById {
        /// Source or input-stream mode.
        input: SelectedMutationInput,
        /// Edge reference.
        edges: ir::EdgeTargetPlan,
    },
}
