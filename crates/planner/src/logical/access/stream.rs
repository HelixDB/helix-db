//! Selected-lowering access-stream union.

use serde::{Deserialize, Serialize};

use super::{AccessDistinct, AccessFilter, AccessOrder, AccessPath, AccessPipeline, AccessWindow};
use crate::properties;

/// Access-backed stream shape that is detailed enough for selected executable
/// lowering.
///
/// The enum only admits access streams whose payload is available in the
/// logical contract. A terminal cannot be created above a generic
/// detail-free [`PureLogicalOp`](super::super::core::PureLogicalOp), so selected
/// lowering never has to guess which executable operator should be emitted.
///
/// ```
/// use helix_planner::ir::{NodeAccessPlan, NodeAccessSourcePlan};
/// use helix_planner::logical::{AccessPath, AccessStream, NodeAccessPath};
///
/// let source = NodeAccessSourcePlan::new(NodeAccessPlan::AllScan).unwrap();
/// let stream = AccessStream::Path(AccessPath::Node(NodeAccessPath::new(source)));
///
/// assert!(matches!(stream, AccessStream::Path(_)));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessStream {
    /// Residual-free access path.
    Path(AccessPath),
    /// Residual filter applied to a residual-free access path.
    Filter(AccessFilter),
    /// Static stream window applied to a residual-free access path.
    Window(AccessWindow),
    /// Required ordering applied to a residual-free access path.
    Order(AccessOrder),
    /// Distinct applied to a residual-free access path.
    Distinct(AccessDistinct),
    /// Composed access-backed stream pipeline.
    Pipeline(AccessPipeline),
}

impl AccessStream {
    /// Base residual-free access path.
    pub const fn access(&self) -> &AccessPath {
        match self {
            Self::Path(access) => access,
            Self::Filter(filter) => filter.access(),
            Self::Window(window) => window.access(),
            Self::Order(order) => order.access(),
            Self::Distinct(distinct) => distinct.access(),
            Self::Pipeline(pipeline) => pipeline.access(),
        }
    }

    /// Effect introduced by the access stream.
    pub fn effect(&self) -> properties::EffectKind {
        match self {
            Self::Pipeline(pipeline) => pipeline.effect(),
            Self::Path(_)
            | Self::Filter(_)
            | Self::Window(_)
            | Self::Order(_)
            | Self::Distinct(_) => properties::EffectKind::Pure,
        }
    }
}
