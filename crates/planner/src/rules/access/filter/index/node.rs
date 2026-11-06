//! Node access-filter index derivation.

use super::contracts::AccessFilterIndexApplication;
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
        label: &str,
        property: &str,
    ) -> Option<Self::EqualityIndex> {
        indexes
            .node_eq
            .get(&(label, property) as &dyn catalog::ScopedPropertyKeyView)
            .cloned()
    }

    fn range_index(
        indexes: &catalog::IndexCatalogSnapshot,
        label: &str,
        property: &str,
        direction: helix_ast::index::RangeIndexDirection,
    ) -> Option<Self::RangeIndex> {
        indexes
            .node_range
            .get(&(label, property, direction) as &dyn catalog::ScopedPropertyDirectionKeyView)
            .cloned()
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
            iteration: crate::ir::RangeScanIteration::Forward,
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

    fn is_unique_key_source(source: &Self::Source) -> bool {
        let ir::NodeAccessPlan::EqualityIndex { index, value, .. } = source.as_ref() else {
            return false;
        };
        index.uniqueness == catalog::IndexUniqueness::Unique
            && match value {
                ir::IndexValue::Literal(literal) => {
                    literal.semantics() != ir::LiteralEqualityIndexValueSemantics::AuthoritativeNull
                }
                ir::IndexValue::Param(_) => true,
                ir::IndexValue::LiteralSet(_) | ir::IndexValue::ParamSet(_) => false,
            }
    }

    fn branch_residual_union(
        branches: Vec<(Self::Source, Option<ir::PredicatePlan>)>,
    ) -> Option<Self::Source> {
        ir::NodeAccessPlan::branch_residual_union(
            branches
                .into_iter()
                .map(|(source, residual)| ir::NodeResidualBranch::new(source, residual))
                .collect(),
        )
        .map(ir::NodeAccessSourcePlan::from_unfiltered)
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
        PartialIndexFilterApplication, PartialIndexFilterRejection,
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

    fn unique_indexes() -> catalog::IndexCatalogSnapshot {
        let mut indexes = catalog::IndexCatalogSnapshot::default()
            .with_node_eq(catalog::ScopedPropertyKey::try_new("User", "team").unwrap());
        for (property, index_id) in [("email", "user-email"), ("handle", "user-handle")] {
            indexes.node_eq.insert(
                catalog::ScopedPropertyKey::try_new("User", property).unwrap(),
                catalog::NodeEqualityIndexMeta::try_new(index_id)
                    .unwrap()
                    .with_uniqueness(catalog::IndexUniqueness::Unique),
            );
        }
        indexes
    }

    fn equality(property: &str, value: ir::IndexValue) -> ir::NodeAccessSourcePlan {
        let indexes = unique_indexes();
        let key = catalog::ScopedPropertyKey::try_new("User", property).unwrap();
        source(ir::NodeAccessPlan::EqualityIndex {
            index: indexes.node_eq[&key].clone(),
            key,
            value,
        })
    }

    fn literal(value: helix_ast::value::PropertyValue) -> ir::SecondaryIndexLiteral {
        ir::SecondaryIndexLiteral::new(value).unwrap()
    }

    #[test]
    fn unique_keys_are_single_non_null_unique_lookups() {
        use helix_ast::value::PropertyValue;
        use shared::AccessFilterIndexFamily;

        let param = || ir::NonEmptyString::new("email").unwrap();
        for (source, keyed) in [
            (
                equality("email", ir::IndexValue::Literal(literal("a".into()))),
                true,
            ),
            (equality("email", ir::IndexValue::Param(param())), true),
            // NaN equals nothing: still at most one candidate.
            (
                equality(
                    "email",
                    ir::IndexValue::Literal(literal(PropertyValue::F64(f64::NAN))),
                ),
                true,
            ),
            (
                equality(
                    "email",
                    ir::IndexValue::Literal(literal(PropertyValue::Null)),
                ),
                false,
            ),
            (
                equality(
                    "email",
                    ir::IndexValue::LiteralSet(ir::AtLeast::<_, 2>::from_pair(
                        literal("a".into()),
                        literal("b".into()),
                    )),
                ),
                false,
            ),
            (
                equality(
                    "email",
                    ir::IndexValue::ParamSet(ir::RuntimeEqualitySet::new(
                        param(),
                        std::num::NonZeroUsize::MIN,
                    )),
                ),
                false,
            ),
            (
                equality("team", ir::IndexValue::Literal(literal("a".into()))),
                false,
            ),
            (
                source(ir::NodeAccessPlan::PointIds {
                    ids: ir::ElementIds::new(ir::AtLeast::<_, 1>::from_one(7)).unwrap(),
                }),
                false,
            ),
        ] {
            assert_eq!(
                NodeIndexFamily::is_unique_key_source(&source),
                keyed,
                "{source:?}"
            );
        }
    }

    #[test]
    fn combine_leaves_unique_keys_to_their_filter() {
        let key = equality("email", ir::IndexValue::Literal(literal("a".into())));
        let other = equality("team", ir::IndexValue::Literal(literal("core".into())));
        assert_eq!(
            shared::combine_indexed_filter_source::<NodeIndexFamily>(&key, other),
            IndexedSourceCombination::Keyed
        );
        assert_eq!(
            shared::combine_indexed_filter_source::<NodeIndexFamily>(&key, key.clone()),
            IndexedSourceCombination::Unchanged
        );
    }

    #[test]
    fn unique_key_filter_drives_conjunctions_over_broad_sources() {
        use helix_ast::expr::Predicate;

        let users = logical::NodeAccessPath::new(source(ir::NodeAccessPlan::LabelScan {
            label: ir::NonEmptyString::new("User").unwrap(),
        }));
        let keyed = |path: &logical::NodeAccessPath, predicate: Predicate| {
            shared::unique_key_filter::<NodeIndexFamily>(
                path,
                &predicate,
                &analysis::FeasibleLabelScope::Unscoped,
                &unique_indexes(),
                &context::PlannerLimits::default(),
            )
        };
        let email = || equality("email", ir::IndexValue::Literal(literal("a".into())));

        // The first key drives through nested conjunctions; the second key
        // and the label conjunct, which every row satisfies, are not
        // residual twice.
        assert_eq!(
            keyed(
                &users,
                Predicate::and(vec![
                    Predicate::eq("team", "core"),
                    Predicate::and(vec![
                        Predicate::eq("$label", "User"),
                        Predicate::eq("email", "a"),
                    ]),
                    Predicate::eq("handle", "h"),
                ]),
            ),
            PartialIndexFilterApplication::Rewritten {
                source: email(),
                residual: shared::conjunction_plan(vec![
                    Predicate::eq("team", "core"),
                    Predicate::eq("handle", "h"),
                ]),
            }
        );
        // Repeats of the key are decided by its lookup too.
        for predicate in [
            Predicate::eq("email", "a"),
            Predicate::and(vec![
                Predicate::eq("email", "a"),
                Predicate::eq("email", "a"),
            ]),
        ] {
            assert_eq!(
                keyed(&users, predicate),
                PartialIndexFilterApplication::Rewritten {
                    source: email(),
                    residual: None,
                }
            );
        }

        for (path, predicate, rejection) in [
            // A key inside a disjunction or a list is not one candidate.
            (
                users.clone(),
                Predicate::or(vec![
                    Predicate::eq("email", "a"),
                    Predicate::eq("team", "x"),
                ]),
                PartialIndexFilterRejection::NoUniqueKey,
            ),
            (
                users.clone(),
                Predicate::is_in(
                    "email",
                    helix_ast::value::PropertyValue::StringArray(vec![
                        "a".to_owned(),
                        "b".to_owned(),
                    ]),
                ),
                PartialIndexFilterRejection::NoUniqueKey,
            ),
            (
                users.clone(),
                Predicate::and(vec![
                    Predicate::eq("email", helix_ast::value::PropertyValue::Null),
                    Predicate::eq("team", "x"),
                ]),
                PartialIndexFilterRejection::NoUniqueKey,
            ),
            // A narrow source keeps its own narrowing.
            (
                logical::NodeAccessPath::new(email()),
                Predicate::and(vec![
                    Predicate::eq("handle", "h"),
                    Predicate::eq("team", "x"),
                ]),
                PartialIndexFilterRejection::NoUniqueKey,
            ),
            (
                logical::NodeAccessPath::new(source(ir::NodeAccessPlan::AllScan)),
                Predicate::eq("email", "a"),
                PartialIndexFilterRejection::NoLabel,
            ),
        ] {
            assert_eq!(
                keyed(&path, predicate.clone()),
                PartialIndexFilterApplication::NotApplicable(rejection),
                "{predicate:?}"
            );
        }
    }
}
