//! Operation-name ADTs used in planner diagnostics.

/// Repeat count/depth field that must be positive.
///
/// # Examples
///
/// ```
/// use helix_planner::error::RepeatCountField;
///
/// assert_eq!(RepeatCountField::MaxDepth.to_string(), "max_depth");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RepeatCountField {
    /// Maximum traversal depth.
    MaxDepth,
    /// Fixed repeat iteration count.
    Times,
}

impl std::fmt::Display for RepeatCountField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaxDepth => f.write_str("max_depth"),
            Self::Times => f.write_str("times"),
        }
    }
}

/// Shortest-path count/depth field that must be positive.
///
/// # Examples
///
/// ```
/// use helix_planner::error::ShortestPathCountField;
///
/// assert_eq!(ShortestPathCountField::MaxDepth.to_string(), "max_depth");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShortestPathCountField {
    /// Maximum traversal depth.
    MaxDepth,
}

impl std::fmt::Display for ShortestPathCountField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MaxDepth => f.write_str("max_depth"),
        }
    }
}

/// Branch operation whose arity is part of the planner contract.
///
/// # Examples
///
/// ```
/// use helix_planner::error::BranchOp;
///
/// assert_eq!(BranchOp::Coalesce.to_string(), "coalesce");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BranchOp {
    /// Coalesce branch.
    Coalesce,
    /// Union branch.
    Union,
}

impl std::fmt::Display for BranchOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Coalesce => f.write_str("coalesce"),
            Self::Union => f.write_str("union"),
        }
    }
}

/// Operation family rejected inside branch/repeat sub-traversals.
///
/// # Examples
///
/// ```
/// use helix_planner::error::SubTraversalOp;
///
/// assert_eq!(SubTraversalOp::ProjectBindings.to_string(), "project_bindings");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubTraversalOp {
    /// Source/root operation that would ignore the parent context row.
    Source,
    /// Binding projection terminal.
    ProjectBindings,
}

impl std::fmt::Display for SubTraversalOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source => f.write_str("source"),
            Self::ProjectBindings => f.write_str("project_bindings"),
        }
    }
}

/// Operation rejected once a stream may carry row-local bindings.
///
/// # Examples
///
/// ```
/// use helix_planner::error::AfterBindOp;
///
/// assert_eq!(AfterBindOp::OrderBy.to_string(), "order_by");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AfterBindOp {
    /// Conditional branch.
    Choose,
    /// Edge-property filter.
    EdgeHas,
    /// Edge-label filter.
    EdgeHasLabel,
    /// Edge-properties terminal.
    EdgeProperties,
    /// ID terminal.
    Id,
    /// Label terminal.
    Label,
    /// Ordering operation.
    OrderBy,
    /// Projection terminal.
    Project,
    /// Repeat traversal.
    Repeat,
    /// Value-map terminal.
    ValueMap,
    /// Values terminal.
    Values,
}

impl std::fmt::Display for AfterBindOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Choose => f.write_str("choose"),
            Self::EdgeHas => f.write_str("edge_has"),
            Self::EdgeHasLabel => f.write_str("edge_has_label"),
            Self::EdgeProperties => f.write_str("edge_properties"),
            Self::Id => f.write_str("id"),
            Self::Label => f.write_str("label"),
            Self::OrderBy => f.write_str("order_by"),
            Self::Project => f.write_str("project"),
            Self::Repeat => f.write_str("repeat"),
            Self::ValueMap => f.write_str("value_map"),
            Self::Values => f.write_str("values"),
        }
    }
}

/// Batch operation whose entry list must be non-empty.
///
/// # Examples
///
/// ```
/// use helix_planner::error::BatchOp;
///
/// assert_eq!(BatchOp::ForEach.to_string(), "foreach");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BatchOp {
    /// Top-level read/write batch.
    Batch,
    /// Nested foreach body.
    ForEach,
}

impl std::fmt::Display for BatchOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Batch => f.write_str("batch"),
            Self::ForEach => f.write_str("foreach"),
        }
    }
}

/// Projection operation whose item list must be non-empty.
///
/// # Examples
///
/// ```
/// use helix_planner::error::ProjectionOp;
///
/// assert_eq!(ProjectionOp::ProjectBindings.to_string(), "project_bindings");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProjectionOp {
    /// Binding coalesce projection input references.
    Coalesce,
    /// Plain projection list.
    Project,
    /// Binding projection list.
    ProjectBindings,
    /// Values projection property list.
    Values,
}

impl std::fmt::Display for ProjectionOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Coalesce => f.write_str("coalesce"),
            Self::Project => f.write_str("project"),
            Self::ProjectBindings => f.write_str("project_bindings"),
            Self::Values => f.write_str("values"),
        }
    }
}

/// Read-only traversal operation rejected by write batches.
///
/// # Examples
///
/// ```
/// use helix_planner::error::ReadOnlyWriteOp;
///
/// assert_eq!(ReadOnlyWriteOp::Bind.to_string(), "bind");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReadOnlyWriteOp {
    /// Row-local binding capture.
    Bind,
    /// Row-binding projection terminal.
    ProjectBindings,
}

impl std::fmt::Display for ReadOnlyWriteOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bind => f.write_str("bind"),
            Self::ProjectBindings => f.write_str("project_bindings"),
        }
    }
}

/// Batch condition that is invalid at the first batch entry.
///
/// # Examples
///
/// ```
/// use helix_planner::error::InitialBatchCondition;
///
/// assert_eq!(InitialBatchCondition::PrevNotEmpty.to_string(), "prev_not_empty");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InitialBatchCondition {
    /// Previous-result non-empty guard.
    PrevNotEmpty,
}

impl std::fmt::Display for InitialBatchCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PrevNotEmpty => f.write_str("prev_not_empty"),
        }
    }
}
