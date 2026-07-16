//! Source-family classification for residual-free access paths.
//!
//! This module is the optimizer-scheduler contract for access sources. It is
//! intentionally coarser than the executable access IR: schedulers and rule
//! applicability checks need a stable closed family, not every source variant.

use serde::{Deserialize, Serialize};

use crate::ir;

/// Top-level residual-free access source family used by optimizer scheduling.
///
/// This is deliberately coarser than the full node/edge access IR: the
/// optimizer only needs enough information to avoid trying set-specific rules
/// on scan, point, search, and runtime sources.
///
/// ```
/// use helix_planner::ir::{NodeAccessPlan, NodeAccessSourcePlan};
/// use helix_planner::logical::{AccessPath, AccessSourceKind, NodeAccessPath};
///
/// let source = NodeAccessSourcePlan::new(NodeAccessPlan::AllScan).unwrap();
/// let access = AccessPath::Node(NodeAccessPath::new(source));
///
/// assert_eq!(access.source_kind(), AccessSourceKind::Scan);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessSourceKind {
    /// Known empty source.
    Empty,
    /// Concrete point IDs.
    PointIds,
    /// Runtime parameter or variable source.
    Runtime,
    /// Full or label scan.
    Scan,
    /// Equality index lookup.
    Equality,
    /// Range index lookup.
    Range,
    /// Vector or text search lookup.
    Search,
    /// Source intersection.
    Intersection,
    /// Source union.
    Union,
}

impl AccessSourceKind {
    /// All source families in dense scheduler order.
    pub const ALL: [Self; 9] = [
        Self::Empty,
        Self::PointIds,
        Self::Runtime,
        Self::Scan,
        Self::Equality,
        Self::Range,
        Self::Search,
        Self::Intersection,
        Self::Union,
    ];
}

pub(super) const fn classify_node(source: &ir::NodeAccessPlan) -> AccessSourceKind {
    match source {
        ir::NodeAccessPlan::Empty => AccessSourceKind::Empty,
        ir::NodeAccessPlan::PointIds { .. } => AccessSourceKind::PointIds,
        ir::NodeAccessPlan::FromParam { .. } | ir::NodeAccessPlan::FromVar { .. } => {
            AccessSourceKind::Runtime
        }
        ir::NodeAccessPlan::AllScan | ir::NodeAccessPlan::LabelScan { .. } => {
            AccessSourceKind::Scan
        }
        ir::NodeAccessPlan::EqualityIndex { .. } => AccessSourceKind::Equality,
        ir::NodeAccessPlan::RangeIndex { .. } => AccessSourceKind::Range,
        ir::NodeAccessPlan::VectorSearch { .. } | ir::NodeAccessPlan::TextSearch { .. } => {
            AccessSourceKind::Search
        }
        ir::NodeAccessPlan::Intersect(_) => AccessSourceKind::Intersection,
        ir::NodeAccessPlan::Union(_) => AccessSourceKind::Union,
        ir::NodeAccessPlan::ScanThenFilter { .. } => AccessSourceKind::Scan,
    }
}

pub(super) const fn classify_edge(source: &ir::EdgeAccessPlan) -> AccessSourceKind {
    match source {
        ir::EdgeAccessPlan::Empty => AccessSourceKind::Empty,
        ir::EdgeAccessPlan::PointIds { .. } => AccessSourceKind::PointIds,
        ir::EdgeAccessPlan::FromParam { .. } | ir::EdgeAccessPlan::FromVar { .. } => {
            AccessSourceKind::Runtime
        }
        ir::EdgeAccessPlan::AllScan | ir::EdgeAccessPlan::LabelScan { .. } => {
            AccessSourceKind::Scan
        }
        ir::EdgeAccessPlan::EqualityIndex { .. } => AccessSourceKind::Equality,
        ir::EdgeAccessPlan::RangeIndex { .. } => AccessSourceKind::Range,
        ir::EdgeAccessPlan::VectorSearch { .. } | ir::EdgeAccessPlan::TextSearch { .. } => {
            AccessSourceKind::Search
        }
        ir::EdgeAccessPlan::Intersect(_) => AccessSourceKind::Intersection,
        ir::EdgeAccessPlan::Union(_) => AccessSourceKind::Union,
        ir::EdgeAccessPlan::ScanThenFilter { .. } => AccessSourceKind::Scan,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use helix_ast::expr::Predicate;
    use helix_ast::index::RangeIndexDirection;
    use helix_ast::value::{PropertyInput, PropertyValue};

    use super::*;
    use crate::catalog;

    fn search_plan() -> ir::SearchIndexPlan {
        ir::SearchIndexPlan {
            index_id: ir::NonEmptyString::new("search_idx").unwrap(),
            tenant: ir::SearchTenantPlan::Unscoped,
        }
    }

    fn predicate() -> ir::PredicatePlan {
        ir::PredicatePlan::new(Predicate::eq("active", true)).unwrap()
    }

    #[test]
    fn classifies_node_source_families() {
        let point_ids = ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one(7)).unwrap();
        let empty_source = ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::Empty);
        let key = catalog::ScopedPropertyKey::try_new("User", "age").unwrap();
        let range_key =
            catalog::ScopedPropertyDirectionKey::try_new("User", "age", RangeIndexDirection::Asc)
                .unwrap();
        let search_key = catalog::NodeSearchIndexKey::try_new("Doc", "embedding").unwrap();

        let cases = [
            (ir::NodeAccessPlan::Empty, AccessSourceKind::Empty),
            (
                ir::NodeAccessPlan::PointIds { ids: point_ids },
                AccessSourceKind::PointIds,
            ),
            (
                ir::NodeAccessPlan::FromParam {
                    param: ir::NonEmptyString::new("ids").unwrap(),
                },
                AccessSourceKind::Runtime,
            ),
            (
                ir::NodeAccessPlan::FromVar {
                    variable: ir::NonEmptyString::new("saved").unwrap(),
                },
                AccessSourceKind::Runtime,
            ),
            (ir::NodeAccessPlan::AllScan, AccessSourceKind::Scan),
            (
                ir::NodeAccessPlan::LabelScan {
                    label: ir::NonEmptyString::new("User").unwrap(),
                },
                AccessSourceKind::Scan,
            ),
            (
                ir::NodeAccessPlan::ScanThenFilter {
                    source: ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::AllScan),
                    residual: predicate(),
                },
                AccessSourceKind::Scan,
            ),
            (
                ir::NodeAccessPlan::EqualityIndex {
                    index: catalog::NodeEqualityIndexMeta::try_new("node_eq").unwrap(),
                    key: key.clone(),
                    value: ir::IndexValue::Literal(
                        ir::SecondaryIndexLiteral::new(PropertyValue::from(42)).unwrap(),
                    ),
                },
                AccessSourceKind::Equality,
            ),
            (
                ir::NodeAccessPlan::RangeIndex {
                    index: catalog::NodeRangeIndexMeta::try_new("node_range").unwrap(),
                    key: range_key,
                    range: ir::IndexRange::All,
                },
                AccessSourceKind::Range,
            ),
            (
                ir::NodeAccessPlan::VectorSearch {
                    key: search_key,
                    index: search_plan(),
                    query_vector: ir::VectorQueryInputPlan::new(PropertyInput::from(
                        PropertyValue::F32Array(vec![0.1]),
                    ))
                    .unwrap(),
                    k: ir::SearchLimitPlan::Literal(NonZeroUsize::new(1).unwrap()),
                },
                AccessSourceKind::Search,
            ),
            (
                ir::NodeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
                    empty_source.clone(),
                    empty_source.clone(),
                )),
                AccessSourceKind::Intersection,
            ),
            (
                ir::NodeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
                    empty_source.clone(),
                    empty_source,
                )),
                AccessSourceKind::Union,
            ),
        ];

        for (source, expected) in cases {
            assert_eq!(classify_node(&source), expected);
        }
    }

    #[test]
    fn classifies_edge_source_families() {
        let point_ids = ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one(7)).unwrap();
        let empty_source = ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::Empty);
        let key = catalog::ScopedPropertyKey::try_new("LIKES", "weight").unwrap();
        let range_key = catalog::ScopedPropertyDirectionKey::try_new(
            "LIKES",
            "weight",
            RangeIndexDirection::Asc,
        )
        .unwrap();
        let search_key = catalog::EdgeSearchIndexKey::try_new("MENTIONS", "body").unwrap();

        let cases = [
            (ir::EdgeAccessPlan::Empty, AccessSourceKind::Empty),
            (
                ir::EdgeAccessPlan::PointIds { ids: point_ids },
                AccessSourceKind::PointIds,
            ),
            (
                ir::EdgeAccessPlan::FromParam {
                    param: ir::NonEmptyString::new("edge_ids").unwrap(),
                },
                AccessSourceKind::Runtime,
            ),
            (
                ir::EdgeAccessPlan::FromVar {
                    variable: ir::NonEmptyString::new("edges").unwrap(),
                },
                AccessSourceKind::Runtime,
            ),
            (ir::EdgeAccessPlan::AllScan, AccessSourceKind::Scan),
            (
                ir::EdgeAccessPlan::LabelScan {
                    label: ir::NonEmptyString::new("LIKES").unwrap(),
                },
                AccessSourceKind::Scan,
            ),
            (
                ir::EdgeAccessPlan::ScanThenFilter {
                    source: ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::AllScan),
                    residual: predicate(),
                },
                AccessSourceKind::Scan,
            ),
            (
                ir::EdgeAccessPlan::EqualityIndex {
                    index: catalog::EdgeEqualityIndexMeta::try_new("edge_eq").unwrap(),
                    key,
                    value: ir::IndexValue::Literal(
                        ir::SecondaryIndexLiteral::new(PropertyValue::from("hot")).unwrap(),
                    ),
                },
                AccessSourceKind::Equality,
            ),
            (
                ir::EdgeAccessPlan::RangeIndex {
                    index: catalog::EdgeRangeIndexMeta::try_new("edge_range").unwrap(),
                    key: range_key,
                    range: ir::IndexRange::All,
                },
                AccessSourceKind::Range,
            ),
            (
                ir::EdgeAccessPlan::TextSearch {
                    key: search_key,
                    index: search_plan(),
                    query_text: ir::TextQueryInputPlan::new(PropertyInput::from("needle")).unwrap(),
                    k: ir::SearchLimitPlan::Literal(NonZeroUsize::new(1).unwrap()),
                },
                AccessSourceKind::Search,
            ),
            (
                ir::EdgeAccessPlan::Intersect(ir::AtLeast::<_, 2>::from_pair(
                    empty_source.clone(),
                    empty_source.clone(),
                )),
                AccessSourceKind::Intersection,
            ),
            (
                ir::EdgeAccessPlan::Union(ir::AtLeast::<_, 2>::from_pair(
                    empty_source.clone(),
                    empty_source,
                )),
                AccessSourceKind::Union,
            ),
        ];

        for (source, expected) in cases {
            assert_eq!(classify_edge(&source), expected);
        }
    }
}
