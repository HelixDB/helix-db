//! Terminal projection plan ADTs.

use serde::{Deserialize, Serialize};

use super::{binding, item, property};

/// Projection plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionPlan {
    /// Count terminal.
    Count,
    /// Exists terminal.
    Exists,
    /// ID terminal.
    Id,
    /// Label terminal.
    Label,
    /// Values terminal.
    Values(property::PropertyNames),
    /// Value map terminal.
    ValueMap(property::PropertySelection),
    /// General projection.
    Project(item::ProjectionItems),
    /// Binding projection.
    ProjectBindings {
        /// Projection list.
        projections: binding::BindingProjectionItems,
        /// Output row deduplication mode.
        dedup: ProjectionDedupMode,
    },
    /// Edge properties terminal.
    EdgeProperties,
}

/// Projection output row deduplication mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionDedupMode {
    /// Preserve all projected rows.
    All,
    /// Deduplicate projected rows.
    Distinct,
}
