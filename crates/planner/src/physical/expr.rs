use serde::{Deserialize, Serialize};

use super::{
    PhysicalAccess, PhysicalControlOp, PhysicalCountPlan, PhysicalPipeline, PhysicalStreamOp,
};
use crate::properties;

/// Physical expression phase.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhysicalExpr {
    /// Identity operator used for proven no-op logical rewrites.
    NoOp,
    /// Empty stream used for proven impossible pure predicates.
    Empty,
    /// Non-empty physical pipeline.
    Pipeline(PhysicalPipeline),
    /// Exact physical cardinality program.
    Cardinality(Box<PhysicalCountPlan>),
    /// Access path.
    Access {
        /// Element kind.
        element: properties::ElementKind,
        /// Access operator.
        access: PhysicalAccess,
    },
    /// Residual filter.
    ResidualFilter,
    /// Generic streaming operator.
    Stream(PhysicalStreamOp),
    /// Traversal control-flow operator.
    Control(PhysicalControlOp),
    /// Root shortest-path operator.
    ShortestPath,
    /// Explicit sort.
    Sort,
    /// Materialization barrier.
    Barrier,
}
