//! Root shortest-path contract.
//!
//! Shortest path is a root-only read barrier. It carries an executable payload
//! directly because there are no memo children or stream inputs to select.

use serde::{Deserialize, Serialize};

use crate::ir;

/// Root shortest-path query with the executable payload preserved.
///
/// ```
/// use std::num::NonZeroUsize;
/// use helix_ast::{graph::NodeRef, traversal::ShortestPathDirection};
/// use helix_planner::ir::ShortestPathPlan;
/// use helix_planner::logical::RootShortestPath;
///
/// let root = RootShortestPath::new(ShortestPathPlan {
///     source: NodeRef::id(1),
///     target: NodeRef::id(2),
///     label: None,
///     direction: ShortestPathDirection::Out,
///     max_depth: NonZeroUsize::new(4).unwrap(),
/// });
///
/// assert_eq!(root.plan().max_depth.get(), 4);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RootShortestPath {
    plan: ir::ShortestPathPlan,
}

impl RootShortestPath {
    /// Build a root shortest-path contract.
    pub const fn new(plan: ir::ShortestPathPlan) -> Self {
        Self { plan }
    }

    /// Shortest-path payload to lower.
    pub const fn plan(&self) -> &ir::ShortestPathPlan {
        &self.plan
    }
}
