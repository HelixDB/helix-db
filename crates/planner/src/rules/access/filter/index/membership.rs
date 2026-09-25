//! Row-preserving index membership for filters inside node streams.
//!
//! A filter that follows an expansion cannot become an index access path, but
//! the same catalog index can still decide the predicate for every node of
//! the indexed label. This module derives that membership set without an
//! access path, splitting unindexed conjuncts into a residual filter.

use super::node::NodeIndexFamily;
use super::shared;
use crate::{analysis, catalog, context, ir};

/// Membership replacement for one stream filter.
#[derive(Debug, Clone, PartialEq)]
pub(in crate::rules) struct MembershipFilterRewrite {
    /// Membership over the indexed conjuncts.
    pub(in crate::rules) membership: ir::NodeIndexMembershipPlan,
    /// Conjuncts the membership set cannot decide.
    pub(in crate::rules) residual: Option<ir::PredicatePlan>,
}

/// Rewrite a node-stream filter predicate into index membership.
///
/// Label-scoped predicates try only their label. Unscoped predicates try every
/// node label with a catalog index in label order and keep the first label
/// whose index answers the predicate or one of its conjuncts. Null equality
/// needs an authoritative scan and stays residual, so it never reaches the
/// membership set.
pub(in crate::rules) fn index_membership_filter(
    predicate: &ir::PredicatePlan,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> Option<MembershipFilterRewrite> {
    let Ok(analysis::PrunedPredicate::Feasible { predicate, label }) =
        analysis::prune_statically_impossible_branches(predicate.as_ref())
    else {
        return None;
    };
    let labels = match label {
        analysis::FeasibleLabelScope::Scoped(label) => vec![label],
        analysis::FeasibleLabelScope::Unscoped => indexes
            .node_eq
            .keys()
            .map(|key| &key.label)
            .chain(indexes.node_range.keys().map(|key| &key.label))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .cloned()
            .collect(),
    };
    labels.iter().find_map(|label| {
        full_membership(&predicate, label, indexes, planner_limits)
            .or_else(|| partial_membership(&predicate, label, indexes, planner_limits))
    })
}

fn full_membership(
    predicate: &helix_ast::expr::Predicate,
    label: &ir::NonEmptyString,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> Option<MembershipFilterRewrite> {
    let set = shared::predicate_index_source::<NodeIndexFamily>(
        predicate,
        label,
        indexes,
        planner_limits,
    )
    .ok()?;
    let predicate = ir::PredicatePlan::new(predicate.clone()).ok()?;
    Some(MembershipFilterRewrite {
        membership: ir::NodeIndexMembershipPlan::new(set, predicate).ok()?,
        residual: None,
    })
}

fn partial_membership(
    predicate: &helix_ast::expr::Predicate,
    label: &ir::NonEmptyString,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> Option<MembershipFilterRewrite> {
    let split = shared::conjunct_index_split::<NodeIndexFamily>(
        predicate,
        label,
        indexes,
        planner_limits,
        |set, conjunct| {
            ir::PredicatePlan::new(conjunct.clone()).is_ok_and(|conjunct| {
                ir::NodeIndexMembershipPlan::new(set.clone(), conjunct).is_ok()
            })
        },
    )
    .ok()?;
    let decided = shared::conjunction_plan(split.decided)?;
    Some(MembershipFilterRewrite {
        membership: ir::NodeIndexMembershipPlan::new(split.source, decided).ok()?,
        residual: shared::conjunction_plan(split.residual),
    })
}

#[cfg(test)]
mod tests {
    use helix_ast::expr::Predicate;
    use helix_ast::value::PropertyValue;

    use super::*;

    fn plan(predicate: Predicate) -> ir::PredicatePlan {
        ir::PredicatePlan::new(predicate).unwrap()
    }

    fn indexes() -> catalog::IndexCatalogSnapshot {
        catalog::IndexCatalogSnapshot::default()
            .with_node_eq(catalog::ScopedPropertyKey::try_new("Item", "kind").unwrap())
            .with_node_eq(catalog::ScopedPropertyKey::try_new("Item", "status").unwrap())
            .with_node_range(
                catalog::ScopedPropertyDirectionKey::try_new(
                    "Item",
                    "rank",
                    helix_ast::index::RangeIndexDirection::Asc,
                )
                .unwrap(),
            )
    }

    fn rewrite(predicate: Predicate) -> Option<MembershipFilterRewrite> {
        index_membership_filter(
            &plan(predicate),
            &indexes(),
            &context::PlannerLimits::default(),
        )
    }

    #[test]
    fn full_membership_covers_equality_in_range_and_label_scoped_predicates() {
        for (predicate, outside) in [
            (
                Predicate::eq("kind", "B"),
                ir::NodeMembershipOutsideLabel::Evaluate,
            ),
            (
                Predicate::is_in(
                    "kind",
                    PropertyValue::StringArray(vec!["A".into(), "B".into()]),
                ),
                ir::NodeMembershipOutsideLabel::Evaluate,
            ),
            (
                Predicate::gte("rank", 3),
                ir::NodeMembershipOutsideLabel::Evaluate,
            ),
            (
                Predicate::and(vec![
                    Predicate::eq("$label", "Item"),
                    Predicate::eq("kind", "B"),
                ]),
                ir::NodeMembershipOutsideLabel::Reject,
            ),
            (
                Predicate::and(vec![
                    Predicate::eq("kind", "B"),
                    Predicate::eq("status", "live"),
                ]),
                ir::NodeMembershipOutsideLabel::Evaluate,
            ),
        ] {
            let rewrite = rewrite(predicate.clone()).unwrap();
            assert_eq!(rewrite.residual, None, "{predicate:?}");
            assert_eq!(rewrite.membership.predicate().as_ref(), &predicate);
            assert_eq!(rewrite.membership.label().as_ref(), "Item");
            assert_eq!(rewrite.membership.outside_label(), outside);
        }
    }

    #[test]
    fn partial_membership_keeps_unindexed_and_null_conjuncts_residual() {
        let scoped = rewrite(Predicate::and(vec![
            Predicate::eq("$label", "Item"),
            Predicate::eq("kind", "B"),
            Predicate::contains("name", "x"),
            Predicate::eq("status", PropertyValue::Null),
        ]))
        .unwrap();

        assert_eq!(
            scoped.membership.predicate().as_ref(),
            &Predicate::and(vec![
                Predicate::eq("$label", "Item"),
                Predicate::eq("kind", "B")
            ])
        );
        assert_eq!(
            scoped.membership.outside_label(),
            ir::NodeMembershipOutsideLabel::Reject
        );
        assert_eq!(
            scoped.residual.unwrap().as_ref(),
            &Predicate::and(vec![
                Predicate::contains("name", "x"),
                Predicate::eq("status", PropertyValue::Null),
            ])
        );

        let single = rewrite(Predicate::and(vec![
            Predicate::eq("kind", "B"),
            Predicate::contains("name", "x"),
        ]))
        .unwrap();
        assert_eq!(
            single.membership.predicate().as_ref(),
            &Predicate::eq("kind", "B")
        );
        assert_eq!(
            single.residual.unwrap().as_ref(),
            &Predicate::contains("name", "x")
        );
    }

    #[test]
    fn membership_declines_null_unindexed_impossible_and_foreign_label_predicates() {
        for predicate in [
            Predicate::eq("kind", PropertyValue::Null),
            Predicate::and(vec![
                Predicate::eq("kind", PropertyValue::Null),
                Predicate::contains("name", "x"),
            ]),
            Predicate::contains("name", "x"),
            Predicate::eq("color", "red"),
            Predicate::and(vec![
                Predicate::eq("$label", "Group"),
                Predicate::eq("kind", "B"),
            ]),
            Predicate::and(vec![
                Predicate::eq("$label", "Item"),
                Predicate::eq("$label", "Group"),
            ]),
        ] {
            assert_eq!(rewrite(predicate.clone()), None, "{predicate:?}");
        }
    }

    #[test]
    fn membership_keeps_runtime_parameters_for_runtime_classification() {
        let equality = rewrite(Predicate::eq_param("kind", "kind")).unwrap();
        assert!(matches!(
            equality.membership.set().as_ref(),
            ir::NodeAccessPlan::EqualityIndex {
                value: ir::IndexValue::Param(_),
                ..
            }
        ));
        let set = rewrite(Predicate::is_in_param("kind", "kinds")).unwrap();
        assert!(matches!(
            set.membership.set().as_ref(),
            ir::NodeAccessPlan::EqualityIndex {
                value: ir::IndexValue::ParamSet(_),
                ..
            }
        ));
    }

    #[test]
    fn unscoped_membership_picks_the_first_indexed_label_in_order() {
        let indexes = catalog::IndexCatalogSnapshot::default()
            .with_node_eq(catalog::ScopedPropertyKey::try_new("Zeta", "kind").unwrap())
            .with_node_eq(catalog::ScopedPropertyKey::try_new("Alpha", "other").unwrap())
            .with_node_eq(catalog::ScopedPropertyKey::try_new("Beta", "kind").unwrap());
        let rewrite = index_membership_filter(
            &plan(Predicate::eq("kind", "B")),
            &indexes,
            &context::PlannerLimits::default(),
        )
        .unwrap();

        assert_eq!(rewrite.membership.label().as_ref(), "Beta");
        assert_eq!(
            index_membership_filter(
                &plan(Predicate::eq("kind", "B")),
                &catalog::IndexCatalogSnapshot::default(),
                &context::PlannerLimits::default(),
            ),
            None
        );
    }
}
