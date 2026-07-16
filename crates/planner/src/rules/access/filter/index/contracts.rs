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
}
