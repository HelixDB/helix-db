use serde::{Deserialize, Serialize};

use crate::ir;

/// Native executable mutation plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecMutationPlan {
    /// Add one source node.
    AddNodeSource {
        /// Node label.
        label: ir::NonEmptyString,
        /// Properties.
        properties: ir::PropertyAssignments,
    },
    /// Add nodes from the input stream.
    AddNodeFromInput {
        /// Node label.
        label: ir::NonEmptyString,
        /// Properties.
        properties: ir::PropertyAssignments,
    },
    /// Add edges from the input stream.
    AddEdge {
        /// Edge label.
        label: ir::NonEmptyString,
        /// Target nodes.
        to: ir::NodeTargetPlan,
        /// Properties.
        properties: ir::PropertyAssignments,
    },
    /// Set one property on the input stream.
    SetProperty {
        /// Property name.
        name: ir::NonEmptyString,
        /// Property value.
        value: ir::PropertyInputPlan,
    },
    /// Remove one property from the input stream.
    RemoveProperty {
        /// Property name.
        name: ir::NonEmptyString,
    },
    /// Drop input stream nodes.
    Drop,
    /// Drop edges from the input stream.
    DropEdge {
        /// Target nodes.
        to: ir::NodeTargetPlan,
    },
    /// Drop labeled edges from the input stream.
    DropEdgeLabeled {
        /// Target nodes.
        to: ir::NodeTargetPlan,
        /// Edge label.
        label: ir::NonEmptyString,
    },
    /// Drop source edges by target.
    DropEdgeByIdSource {
        /// Edge target.
        edges: ir::EdgeTargetPlan,
    },
    /// Drop input stream edges by target.
    DropEdgeByIdFromInput {
        /// Edge target.
        edges: ir::EdgeTargetPlan,
    },
}
