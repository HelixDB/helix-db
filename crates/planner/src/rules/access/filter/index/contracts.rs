//! Shared access-filter index application contracts.

use super::super::atoms::AccessFilterIndexPlanRejection;
use crate::ir;

#[derive(Debug, Clone, PartialEq)]
pub(super) enum AccessFilterIndexApplication<T> {
    Rewritten(T),
    NotApplicable(AccessFilterIndexRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AccessFilterIndexRejection {
    NoLabel,
    Predicate(AccessFilterIndexPlanRejection),
    MissingIndex(MissingAccessIndex),
    SourceUnchanged,
    /// The source is a unique-key lookup, whose at most one candidate
    /// evaluates the filter itself.
    KeyedSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MissingAccessIndex {
    Equality,
    Range,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum IndexedSourceCombination<T> {
    Rewritten(T),
    Unchanged,
    /// The source is a unique-key lookup: reading another index set would
    /// cost more than evaluating the filter on its at most one candidate.
    Keyed,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum PartialIndexFilterApplication<T> {
    Rewritten {
        source: T,
        residual: Option<ir::PredicatePlan>,
    },
    NotApplicable(PartialIndexFilterRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PartialIndexFilterRejection {
    NoLabel,
    NotConjunction,
    NoIndexedConjunct,
    SourceUnchanged,
    /// The source is a unique-key lookup, whose at most one candidate
    /// evaluates the filter itself.
    KeyedSource,
    /// The source is not broad, or no conjunct is a unique key.
    NoUniqueKey,
    /// Several branches keep residuals but cannot form a branch-residual
    /// union: the filtered source is already narrow, or a branch set is not
    /// index-only.
    ResidualBranchesUnrepresentable,
}
