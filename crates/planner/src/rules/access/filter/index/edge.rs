//! Edge access-filter index derivation.

use super::contracts::AccessFilterIndexApplication;
use super::contracts::PartialIndexFilterApplication;
use super::shared;
use crate::{analysis, catalog, context, ir, logical};

pub(super) fn index_filter(
    path: &logical::EdgeAccessPath,
    predicate: &helix_ast::expr::Predicate,
    predicate_label: &analysis::FeasibleLabelScope,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> AccessFilterIndexApplication<logical::EdgeAccessPath> {
    shared::index_filter::<EdgeIndexFamily>(
        path,
        predicate,
        predicate_label,
        indexes,
        planner_limits,
    )
}

pub(super) fn partial_index_filter(
    path: &logical::EdgeAccessPath,
    predicate: &helix_ast::expr::Predicate,
    predicate_label: &analysis::FeasibleLabelScope,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> PartialIndexFilterApplication<ir::EdgeAccessSourcePlan> {
    shared::partial_index_filter::<EdgeIndexFamily>(
        path,
        predicate,
        predicate_label,
        indexes,
        planner_limits,
    )
}

pub(super) struct EdgeIndexFamily;

impl shared::AccessFilterIndexFamily for EdgeIndexFamily {
    type Path = logical::EdgeAccessPath;
    type Source = ir::EdgeAccessSourcePlan;
    type EqualityIndex = catalog::EdgeEqualityIndexMeta;
    type RangeIndex = catalog::EdgeRangeIndexMeta;

    fn path_source(path: &Self::Path) -> &Self::Source {
        path.source()
    }

    fn source_common_label(source: &Self::Source) -> Option<&ir::NonEmptyString> {
        super::super::super::sources::edge_source_common_label(source)
    }

    fn path_from_source(source: Self::Source) -> Self::Path {
        logical::EdgeAccessPath::new(source)
    }

    fn equality_index(
        indexes: &catalog::IndexCatalogSnapshot,
        key: &catalog::ScopedPropertyKey,
    ) -> Option<Self::EqualityIndex> {
        indexes.edge_eq.get(key).cloned()
    }

    fn range_index(
        indexes: &catalog::IndexCatalogSnapshot,
        key: &catalog::ScopedPropertyDirectionKey,
    ) -> Option<Self::RangeIndex> {
        indexes.edge_range.get(key).cloned()
    }

    fn equality_source(
        index: Self::EqualityIndex,
        key: catalog::ScopedPropertyKey,
        value: ir::IndexValue,
    ) -> Self::Source {
        ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::EqualityIndex {
            index,
            key,
            value,
        })
    }

    fn range_source(
        index: Self::RangeIndex,
        key: catalog::ScopedPropertyDirectionKey,
        range: ir::IndexRange,
    ) -> Self::Source {
        ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::RangeIndex {
            index,
            key,
            range,
        })
    }

    fn union_source(sources: Vec<Self::Source>) -> Self::Source {
        ir::EdgeAccessSourcePlan::from_unfiltered(
            super::super::super::sources::edge_union_from_sources(sources),
        )
    }

    fn intersection_source(sources: Vec<Self::Source>) -> Self::Source {
        ir::EdgeAccessSourcePlan::from_unfiltered(
            super::super::super::sources::edge_intersection_from_sources(sources),
        )
    }

    fn is_broad_source(source: &Self::Source) -> bool {
        matches!(
            source.as_ref(),
            ir::EdgeAccessPlan::AllScan | ir::EdgeAccessPlan::LabelScan { .. }
        )
    }

    fn intersect_pair(left: Self::Source, right: Self::Source) -> Self::Source {
        ir::EdgeAccessSourcePlan::from_unfiltered(ir::EdgeAccessPlan::Intersect(
            ir::AtLeast::<_, 2>::from_pair(left, right),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::atoms::{AccessFilterIndexAtom, AccessFilterIndexPlanRejection};
    use super::super::contracts::{
        AccessFilterIndexRejection, IndexedSourceCombination, MissingAccessIndex,
    };
    use super::*;

    fn source(plan: ir::EdgeAccessPlan) -> ir::EdgeAccessSourcePlan {
        ir::EdgeAccessSourcePlan::from_unfiltered(plan)
    }

    #[test]
    fn combine_replaces_broad_sources_and_intersects_narrow_sources() {
        let indexed = source(ir::EdgeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new("LIKES").unwrap(),
        });
        assert_eq!(
            shared::combine_indexed_filter_source::<EdgeIndexFamily>(
                &source(ir::EdgeAccessPlan::AllScan),
                indexed.clone()
            ),
            IndexedSourceCombination::Rewritten(indexed.clone())
        );

        let base = source(ir::EdgeAccessPlan::PointIds {
            ids: ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one(7)).unwrap(),
        });
        assert!(matches!(
            shared::combine_indexed_filter_source::<EdgeIndexFamily>(&base, indexed),
            IndexedSourceCombination::Rewritten(source)
                if matches!(source.as_ref(), ir::EdgeAccessPlan::Intersect(children) if children.len() == 2)
        ));
    }

    #[test]
    fn combine_reports_unchanged_sources() {
        let indexed = source(ir::EdgeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new("LIKES").unwrap(),
        });

        assert_eq!(
            shared::combine_indexed_filter_source::<EdgeIndexFamily>(&indexed, indexed.clone()),
            IndexedSourceCombination::Unchanged
        );
    }

    #[test]
    fn catalog_lookup_reports_missing_equality_and_range_indexes() {
        let label = ir::NonEmptyString::new("LIKES").unwrap();
        let indexes = catalog::IndexCatalogSnapshot::default();
        let equality = AccessFilterIndexAtom::Equality {
            property: ir::NonEmptyString::new("weight").unwrap(),
            domain: super::super::super::atoms::AccessEqualityDomain::One(ir::IndexValue::Param(
                ir::NonEmptyString::new("weight").unwrap(),
            )),
        };
        let range = AccessFilterIndexAtom::Range {
            property: ir::NonEmptyString::new("weight").unwrap(),
            range: ir::IndexRange::All,
        };

        assert_eq!(
            shared::index_source_for_atom::<EdgeIndexFamily>(&label, &equality, &indexes),
            Err(MissingAccessIndex::Equality)
        );
        assert_eq!(
            shared::index_source_for_atom::<EdgeIndexFamily>(&label, &range, &indexes),
            Err(MissingAccessIndex::Range)
        );
    }

    #[test]
    fn index_filter_reports_no_label_predicate_and_source_noop_rejections() {
        let no_label_path = logical::EdgeAccessPath::new(source(ir::EdgeAccessPlan::AllScan));
        assert_eq!(
            index_filter(
                &no_label_path,
                &helix_ast::expr::Predicate::eq("weight", 42),
                &analysis::FeasibleLabelScope::Unscoped,
                &catalog::IndexCatalogSnapshot::default(),
                &context::PlannerLimits::default(),
            ),
            AccessFilterIndexApplication::NotApplicable(AccessFilterIndexRejection::NoLabel)
        );

        let label_path = logical::EdgeAccessPath::new(source(ir::EdgeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new("LIKES").unwrap(),
        }));
        assert_eq!(
            index_filter(
                &label_path,
                &helix_ast::expr::Predicate::contains("notes", "rust"),
                &analysis::FeasibleLabelScope::Unscoped,
                &catalog::IndexCatalogSnapshot::default(),
                &context::PlannerLimits::default(),
            ),
            AccessFilterIndexApplication::NotApplicable(AccessFilterIndexRejection::Predicate(
                AccessFilterIndexPlanRejection::NotIndexCandidate
            ))
        );

        let key = catalog::ScopedPropertyKey::try_new("LIKES", "weight").unwrap();
        let indexes = catalog::IndexCatalogSnapshot::default().with_edge_eq(key.clone());
        let indexed = source(ir::EdgeAccessPlan::EqualityIndex {
            index: indexes.edge_eq[&key].clone(),
            key,
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from(42)).unwrap(),
            ),
        });
        let indexed_path = logical::EdgeAccessPath::new(indexed);
        assert_eq!(
            index_filter(
                &indexed_path,
                &helix_ast::expr::Predicate::eq("weight", 42),
                &analysis::FeasibleLabelScope::Unscoped,
                &indexes,
                &context::PlannerLimits::default(),
            ),
            AccessFilterIndexApplication::NotApplicable(
                AccessFilterIndexRejection::SourceUnchanged
            )
        );
    }
}
