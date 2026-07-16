//! Shared access-source family contracts.

use crate::{catalog, context, exec, ir, properties};

pub(in crate::rules) enum AccessSourceParts<'a, Plan> {
    Empty,
    PointIds(&'a ir::ElementIds),
    RuntimeInput,
    AllScan,
    LabelScan {
        label: &'a ir::NonEmptyString,
    },
    EqualityIndex {
        key: &'a catalog::ScopedPropertyKey,
        kind: EqualityIndexKind,
    },
    RangeIndex {
        key: &'a catalog::ScopedPropertyDirectionKey,
    },
    VectorSearch {
        k: &'a ir::SearchLimitPlan,
    },
    TextSearch {
        k: &'a ir::SearchLimitPlan,
    },
    Intersect(Vec<&'a Plan>),
    Union(Vec<&'a Plan>),
    ScanThenFilter {
        source: &'a Plan,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::rules) enum EqualityIndexKind {
    Unique,
    NonUnique,
}

pub(in crate::rules) trait AccessSourceFamily {
    type Plan;

    fn element() -> properties::ElementKind;
    fn point_keyspace() -> exec::ElementKeyspace;
    fn all_scan_keyspace() -> exec::ElementKeyspace;
    fn source_parts(plan: &Self::Plan) -> AccessSourceParts<'_, Self::Plan>;
    fn label_cardinality(stats: &context::StatsSnapshot, label: &ir::NonEmptyString)
        -> Option<u64>;
    fn equality_cardinality(
        stats: &context::StatsSnapshot,
        key: &catalog::ScopedPropertyKey,
    ) -> Option<u64>;
    fn range_cardinality(
        stats: &context::StatsSnapshot,
        key: &catalog::ScopedPropertyDirectionKey,
    ) -> Option<u64>;
}
