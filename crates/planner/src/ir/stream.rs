use helix_ast::value::PropertyValue;
use serde::{Deserialize, Serialize};

use super::{NonEmptyString, PhysicalOp, PredicatePlan};

/// Filter execution plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterPlan {
    /// Executor must evaluate a residual predicate over its input.
    Residual {
        /// Residual predicate.
        predicate: PredicatePlan,
    },
}

/// Expansion direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpandDirection {
    /// Outgoing.
    Out,
    /// Incoming.
    In,
    /// Both directions.
    Both,
}

/// Expansion output family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpandOutput {
    /// Output nodes.
    Nodes,
    /// Output edges.
    Edges,
}

/// Edge label scope for graph expansion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpandLabelPlan {
    /// Expand across all edge labels.
    Any,
    /// Expand only through the named edge label.
    Label(NonEmptyString),
}

/// Graph expansion plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpandPlan {
    /// Direction.
    pub direction: ExpandDirection,
    /// Output family.
    pub output: ExpandOutput,
    /// Edge label scope.
    pub label: ExpandLabelPlan,
}

/// Variable plan.
///
/// Source injection is distinct from stream variable operations, so plans like
/// `Within` without an input cannot be represented.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VariablePlan {
    /// Inject a variable as a source stream.
    SourceInject {
        /// Variable name.
        variable: NonEmptyString,
    },
    /// Apply a variable operation to an existing stream.
    Stream {
        /// Input plan.
        input: Box<PhysicalOp>,
        /// Stream variable operation.
        op: StreamVariableOp,
    },
}

/// Variable operation that requires an input stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamVariableOp {
    /// Store current stream.
    As(NonEmptyString),
    /// Store current stream.
    Store(NonEmptyString),
    /// Select stream.
    Select(NonEmptyString),
    /// Bind row-local element.
    Bind(NonEmptyString),
    /// Inject variable into the current stream context.
    Inject(NonEmptyString),
    /// Keep within variable.
    Within(NonEmptyString),
    /// Keep outside variable.
    Without(NonEmptyString),
}

/// Reserved operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReservedOp {
    /// Fold.
    Fold,
    /// Unfold.
    Unfold,
    /// Path.
    Path,
    /// Simple path.
    SimplePath,
    /// Sack initialization.
    WithSack(PropertyValue),
    /// Sack set.
    SackSet(NonEmptyString),
    /// Sack add.
    SackAdd(NonEmptyString),
    /// Sack get.
    SackGet,
}
