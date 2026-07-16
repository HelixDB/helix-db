//! Side-effect-free logical operation contracts.

use serde::{Deserialize, Serialize};

use crate::{ir, properties};

/// Side-effect-free logical operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PureLogicalOp {
    /// Identity operation produced by a proven no-op rewrite.
    NoOp,
    /// Empty stream produced by a proven impossible pure predicate.
    Empty,
    /// Read an element source.
    Source {
        /// Element kind.
        element: properties::ElementKind,
    },
    /// Filter by predicate.
    Filter {
        /// Predicate.
        predicate: ir::PredicatePlan,
    },
    /// Limit stream cardinality.
    Limit {
        /// Bound.
        count: ir::StreamBoundPlan,
    },
    /// Order stream by keys.
    Order {
        /// Required order.
        ordering: properties::RequiredOrdering,
    },
    /// Skip rows from a stream.
    Skip {
        /// Bound.
        count: ir::StreamBoundPlan,
    },
    /// Keep a checked stream range.
    Range {
        /// Range.
        range: ir::StreamRangePlan,
    },
    /// Deduplicate rows.
    Distinct,
    /// Graph expansion.
    Expand {
        /// Element kind produced by the expansion.
        element: properties::ElementKind,
    },
    /// Projection terminal.
    Project,
    /// Aggregation terminal.
    Aggregate,
    /// Variable read or stream-local variable operation.
    Variable,
    /// Reserved pure stream operation.
    Reserved,
}

/// Side-effect-free logical operation family used by fine-grained rule
/// scheduling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PureLogicalOpKind {
    /// `PureLogicalOp::NoOp`.
    NoOp,
    /// `PureLogicalOp::Empty`.
    Empty,
    /// `PureLogicalOp::Source`.
    Source,
    /// `PureLogicalOp::Filter`.
    Filter,
    /// `PureLogicalOp::Limit`.
    Limit,
    /// `PureLogicalOp::Order`.
    Order,
    /// `PureLogicalOp::Skip`.
    Skip,
    /// `PureLogicalOp::Range`.
    Range,
    /// `PureLogicalOp::Distinct`.
    Distinct,
    /// `PureLogicalOp::Expand`.
    Expand,
    /// `PureLogicalOp::Project`.
    Project,
    /// `PureLogicalOp::Aggregate`.
    Aggregate,
    /// `PureLogicalOp::Variable`.
    Variable,
    /// `PureLogicalOp::Reserved`.
    Reserved,
}

impl PureLogicalOpKind {
    /// All side-effect-free logical operation families.
    pub const ALL: [Self; 14] = [
        Self::NoOp,
        Self::Empty,
        Self::Source,
        Self::Filter,
        Self::Limit,
        Self::Order,
        Self::Skip,
        Self::Range,
        Self::Distinct,
        Self::Expand,
        Self::Project,
        Self::Aggregate,
        Self::Variable,
        Self::Reserved,
    ];
}

impl PureLogicalOp {
    /// Return the pure operation family.
    pub const fn kind(&self) -> PureLogicalOpKind {
        match self {
            Self::NoOp => PureLogicalOpKind::NoOp,
            Self::Empty => PureLogicalOpKind::Empty,
            Self::Source { .. } => PureLogicalOpKind::Source,
            Self::Filter { .. } => PureLogicalOpKind::Filter,
            Self::Limit { .. } => PureLogicalOpKind::Limit,
            Self::Order { .. } => PureLogicalOpKind::Order,
            Self::Skip { .. } => PureLogicalOpKind::Skip,
            Self::Range { .. } => PureLogicalOpKind::Range,
            Self::Distinct => PureLogicalOpKind::Distinct,
            Self::Expand { .. } => PureLogicalOpKind::Expand,
            Self::Project => PureLogicalOpKind::Project,
            Self::Aggregate => PureLogicalOpKind::Aggregate,
            Self::Variable => PureLogicalOpKind::Variable,
            Self::Reserved => PureLogicalOpKind::Reserved,
        }
    }
}
