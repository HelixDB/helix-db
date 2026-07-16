//! Shortest-path executable payload contract.
//!
//! Shortest path is a root operation: it consumes node references, resolves
//! each endpoint to exactly one node at execution time, and returns a scalar
//! node-id path. The planner keeps the optional label non-empty and the depth
//! positive before the payload crosses into executable IR.

use std::num::NonZeroUsize;

use helix_ast::graph::NodeRef;
use helix_ast::traversal::ShortestPathDirection;
use serde::{Deserialize, Serialize};

use super::NonEmptyString;

/// Shortest-path execution plan.
///
/// ```
/// use std::num::NonZeroUsize;
/// use helix_ast::{graph::NodeRef, traversal::ShortestPathDirection};
/// use helix_planner::ir::{NonEmptyString, ShortestPathPlan};
///
/// let plan = ShortestPathPlan {
///     source: NodeRef::id(1),
///     target: NodeRef::param("target"),
///     label: Some(NonEmptyString::new("KNOWS").unwrap()),
///     direction: ShortestPathDirection::Both,
///     max_depth: NonZeroUsize::new(3).unwrap(),
/// };
///
/// assert_eq!(plan.max_depth.get(), 3);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShortestPathPlan {
    /// Source node reference.
    pub source: NodeRef,
    /// Target node reference.
    pub target: NodeRef,
    /// Optional edge label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<NonEmptyString>,
    /// Traversal direction.
    pub direction: ShortestPathDirection,
    /// Positive maximum traversal depth.
    pub max_depth: NonZeroUsize,
}
