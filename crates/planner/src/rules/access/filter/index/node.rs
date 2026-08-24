//! Node access-filter index derivation.

use super::contracts::AccessFilterIndexApplication;
use super::contracts::PartialIndexFilterApplication;
use super::shared;
use crate::{analysis, catalog, context, ir, logical};

pub(super) fn index_filter(
    path: &logical::NodeAccessPath,
    predicate: &helix_ast::expr::Predicate,
    predicate_label: &analysis::FeasibleLabelScope,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> AccessFilterIndexApplication<logical::NodeAccessPath> {
    shared::index_filter::<NodeIndexFamily>(
        path,
        predicate,
        predicate_label,
        indexes,
        planner_limits,
    )
}

pub(super) fn partial_index_filter(
    path: &logical::NodeAccessPath,
    predicate: &helix_ast::expr::Predicate,
    predicate_label: &analysis::FeasibleLabelScope,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> PartialIndexFilterApplication<ir::NodeAccessSourcePlan> {
    shared::partial_index_filter::<NodeIndexFamily>(
        path,
        predicate,
        predicate_label,
        indexes,
        planner_limits,
    )
}

pub(super) struct NodeIndexFamily;

impl shared::AccessFilterIndexFamily for NodeIndexFamily {
    type Path = logical::NodeAccessPath;
    type Source = ir::NodeAccessSourcePlan;
    type EqualityIndex = catalog::NodeEqualityIndexMeta;
    type RangeIndex = catalog::NodeRangeIndexMeta;

    fn path_source(path: &Self::Path) -> &Self::Source {
        path.source()
    }

    fn source_common_label(source: &Self::Source) -> Option<&ir::NonEmptyString> {
        super::super::super::sources::node_source_common_label(source)
    }

    fn path_from_source(source: Self::Source) -> Self::Path {
        logical::NodeAccessPath::new(source)
    }

    fn equality_index(
        indexes: &catalog::IndexCatalogSnapshot,
        key: &catalog::ScopedPropertyKey,
    ) -> Option<Self::EqualityIndex> {
        indexes.node_eq.get(key).cloned()
    }

    fn range_index(
        indexes: &catalog::IndexCatalogSnapshot,
        key: &catalog::ScopedPropertyDirectionKey,
    ) -> Option<Self::RangeIndex> {
        indexes.node_range.get(key).cloned()
    }

    fn equality_source(
        index: Self::EqualityIndex,
        key: catalog::ScopedPropertyKey,
        value: ir::IndexValue,
    ) -> Self::Source {
        ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::EqualityIndex {
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
        ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::RangeIndex {
            index,
            key,
            range,
        })
    }

    fn union_source(sources: Vec<Self::Source>) -> Self::Source {
        ir::NodeAccessSourcePlan::from_unfiltered(
            super::super::super::sources::node_union_from_sources(sources),
        )
    }

    fn intersection_source(sources: Vec<Self::Source>) -> Self::Source {
        ir::NodeAccessSourcePlan::from_unfiltered(
            super::super::super::sources::node_intersection_from_sources(sources),
        )
    }

    fn is_broad_source(source: &Self::Source) -> bool {
        matches!(
            source.as_ref(),
            ir::NodeAccessPlan::AllScan | ir::NodeAccessPlan::LabelScan { .. }
        )
    }

    fn intersect_pair(left: Self::Source, right: Self::Source) -> Self::Source {
        ir::NodeAccessSourcePlan::from_unfiltered(ir::NodeAccessPlan::Intersect(
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

    fn source(plan: ir::NodeAccessPlan) -> ir::NodeAccessSourcePlan {
        ir::NodeAccessSourcePlan::from_unfiltered(plan)
    }

    #[test]
    fn combine_replaces_broad_sources_and_intersects_narrow_sources() {
        let indexed = source(ir::NodeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new("User").unwrap(),
        });
        assert_eq!(
            shared::combine_indexed_filter_source::<NodeIndexFamily>(
                &source(ir::NodeAccessPlan::AllScan),
                indexed.clone()
            ),
            IndexedSourceCombination::Rewritten(indexed.clone())
        );

        let base = source(ir::NodeAccessPlan::PointIds {
            ids: ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one(7)).unwrap(),
        });
        assert!(matches!(
            shared::combine_indexed_filter_source::<NodeIndexFamily>(&base, indexed),
            IndexedSourceCombination::Rewritten(source)
                if matches!(source.as_ref(), ir::NodeAccessPlan::Intersect(children) if children.len() == 2)
        ));
    }

    #[test]
    fn combine_reports_unchanged_sources() {
        let indexed = source(ir::NodeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new("User").unwrap(),
        });

        assert_eq!(
            shared::combine_indexed_filter_source::<NodeIndexFamily>(&indexed, indexed.clone()),
            IndexedSourceCombination::Unchanged
        );
    }

    #[test]
    fn catalog_lookup_reports_missing_equality_and_range_indexes() {
        let label = ir::NonEmptyString::new("User").unwrap();
        let indexes = catalog::IndexCatalogSnapshot::default();
        let equality = AccessFilterIndexAtom::Equality {
            property: ir::NonEmptyString::new("age").unwrap(),
            domain: super::super::super::atoms::AccessEqualityDomain::One(ir::IndexValue::Param(
                ir::NonEmptyString::new("age").unwrap(),
            )),
        };
        let range = AccessFilterIndexAtom::Range {
            property: ir::NonEmptyString::new("age").unwrap(),
            range: ir::IndexRange::All,
        };

        assert_eq!(
            shared::index_source_for_atom::<NodeIndexFamily>(&label, &equality, &indexes),
            Err(MissingAccessIndex::Equality)
        );
        assert_eq!(
            shared::index_source_for_atom::<NodeIndexFamily>(&label, &range, &indexes),
            Err(MissingAccessIndex::Range)
        );
    }

    #[test]
    fn index_filter_reports_no_label_predicate_and_source_noop_rejections() {
        let no_label_path = logical::NodeAccessPath::new(source(ir::NodeAccessPlan::AllScan));
        assert_eq!(
            index_filter(
                &no_label_path,
                &helix_ast::expr::Predicate::eq("age", 42),
                &analysis::FeasibleLabelScope::Unscoped,
                &catalog::IndexCatalogSnapshot::default(),
                &context::PlannerLimits::default(),
            ),
            AccessFilterIndexApplication::NotApplicable(AccessFilterIndexRejection::NoLabel)
        );

        let label_path = logical::NodeAccessPath::new(source(ir::NodeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new("User").unwrap(),
        }));
        assert_eq!(
            index_filter(
                &label_path,
                &helix_ast::expr::Predicate::contains("bio", "rust"),
                &analysis::FeasibleLabelScope::Unscoped,
                &catalog::IndexCatalogSnapshot::default(),
                &context::PlannerLimits::default(),
            ),
            AccessFilterIndexApplication::NotApplicable(AccessFilterIndexRejection::Predicate(
                AccessFilterIndexPlanRejection::NotIndexCandidate
            ))
        );

        let key = catalog::ScopedPropertyKey::try_new("User", "age").unwrap();
        let indexes = catalog::IndexCatalogSnapshot::default().with_node_eq(key.clone());
        let indexed = source(ir::NodeAccessPlan::EqualityIndex {
            index: indexes.node_eq[&key].clone(),
            key,
            value: ir::IndexValue::Literal(
                ir::SecondaryIndexLiteral::new(helix_ast::value::PropertyValue::from(42)).unwrap(),
            ),
        });
        let indexed_path = logical::NodeAccessPath::new(indexed);
        assert_eq!(
            index_filter(
                &indexed_path,
                &helix_ast::expr::Predicate::eq("age", 42),
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
