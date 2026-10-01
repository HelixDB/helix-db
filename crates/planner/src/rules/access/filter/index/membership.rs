//! Row-preserving index membership for filters inside node streams.
//!
//! A filter that follows an expansion cannot become an index access path, but
//! the same catalog index can still decide the predicate for every node of
//! the indexed label. This module derives that membership set without an
//! access path. Conjuncts the set cannot decide stay in the membership's
//! fused residual, which only set matches evaluate, so no separate filter
//! reads the records again.

use super::node::NodeIndexFamily;
use super::shared;
use crate::{analysis, catalog, context, ir};

/// Rewrite a node-stream filter predicate into index membership.
///
/// Label-scoped predicates try only their label. Unscoped predicates try every
/// node label with a catalog index, and use an index set only when exactly
/// one label's index answers the predicate or one of its conjuncts: an
/// expansion's target label is unknown, so with two answering labels any
/// choice may decide none of the stream's rows. Null equality needs an
/// authoritative scan and range conjuncts verify the whole label range, so
/// both stay in the residual.
///
/// Without such an index set, a predicate whose conjuncts constrain `$label`
/// to a finite domain (a label-only filter, a label-scoped but unindexed
/// filter, or an ambiguous unscoped one with a label conjunct) is decided by
/// the `$label` bitmaps of that domain, within the union branch limit, and
/// keeps its other conjuncts as the residual.
///
/// `$label` is always indexed by its bitmaps, so a label conjunct is never
/// decided by reading records, even when no property index answers the rest.
/// The cost is that membership reads whole sets: a short stream over a huge
/// label, such as `.out().where($label == "X").limit(1)` from a node with
/// three neighbours, decodes the label's full bitmap where a per-row filter
/// would read three records, and an `Evaluate` policy also decodes the
/// label bitmap of its index set. The executor resolves each set once per
/// request state and reads the bitmaps of a label domain concurrently.
pub(in crate::rules) fn index_membership_filter(
    predicate: &ir::PredicatePlan,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> Option<ir::NodeIndexMembershipPlan> {
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
    let mut rewrites = labels.iter().filter_map(|label| {
        full_membership(&predicate, label, indexes, planner_limits)
            .or_else(|| partial_membership(&predicate, label, indexes, planner_limits))
    });
    match (rewrites.next(), rewrites.next()) {
        (Some(rewrite), None) => Some(rewrite),
        (None, _) | (Some(_), Some(_)) => label_membership(&predicate, planner_limits),
    }
}

fn full_membership(
    predicate: &helix_ast::expr::Predicate,
    label: &ir::NonEmptyString,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> Option<ir::NodeIndexMembershipPlan> {
    let set = shared::predicate_index_source::<NodeIndexFamily>(
        predicate,
        label,
        indexes,
        planner_limits,
    )
    .ok()?;
    let predicate = ir::PredicatePlan::new(predicate.clone()).ok()?;
    ir::NodeIndexMembershipPlan::new(set, predicate, None).ok()
}

fn partial_membership(
    predicate: &helix_ast::expr::Predicate,
    label: &ir::NonEmptyString,
    indexes: &catalog::IndexCatalogSnapshot,
    planner_limits: &context::PlannerLimits,
) -> Option<ir::NodeIndexMembershipPlan> {
    let split = shared::conjunct_index_split::<NodeIndexFamily>(
        predicate,
        label,
        indexes,
        planner_limits,
        |set, conjunct| {
            ir::PredicatePlan::new(conjunct.clone()).is_ok_and(|conjunct| {
                ir::NodeIndexMembershipPlan::new(set.clone(), conjunct, None).is_ok()
            })
        },
    )
    .ok()?;
    let predicate = ir::PredicatePlan::new(predicate.clone()).ok()?;
    ir::NodeIndexMembershipPlan::new(
        split.source,
        predicate,
        shared::conjunction_plan(split.residual),
    )
    .ok()
}

/// `$label` bitmap membership over the finite label domain of the label
/// conjuncts of `predicate`, with its other conjuncts as the residual.
///
/// A domain wider than the union branch limit keeps the per-row filter, the
/// same bound a label-domain access path obeys.
fn label_membership(
    predicate: &helix_ast::expr::Predicate,
    planner_limits: &context::PlannerLimits,
) -> Option<ir::NodeIndexMembershipPlan> {
    let (domain, residual) = analysis::conjunctive_label_domain(predicate)?;
    let within_limit = match (&domain, planner_limits.max_index_union_branches) {
        (
            analysis::FiniteLabelDomain::Many(labels),
            context::IndexUnionBranchLimit::Limited(limit),
        ) => labels.len() <= limit.get(),
        (analysis::FiniteLabelDomain::Many(_), context::IndexUnionBranchLimit::Disabled) => false,
        (analysis::FiniteLabelDomain::Empty | analysis::FiniteLabelDomain::One(_), _) => true,
    };
    if !within_limit {
        return None;
    }
    let residual = residual.map(ir::PredicatePlan::new).transpose().ok()?;
    ir::NodeIndexMembershipPlan::labels(ir::PredicatePlan::new(predicate.clone()).ok()?, residual)
        .ok()
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

    fn rewrite(predicate: Predicate) -> Option<ir::NodeIndexMembershipPlan> {
        index_membership_filter(
            &plan(predicate),
            &indexes(),
            &context::PlannerLimits::default(),
        )
    }

    fn outside_label(plan: &ir::NodeIndexMembershipPlan) -> ir::NodeMembershipOutsideLabel {
        let ir::NodeMembershipSet::Index {
            label,
            outside_label,
            ..
        } = plan.set()
        else {
            panic!("expected an index set: {:?}", plan.set());
        };
        assert_eq!(label.as_ref(), "Item");
        *outside_label
    }

    fn labels(plan: &ir::NodeIndexMembershipPlan) -> Vec<&str> {
        let ir::NodeMembershipSet::Labels(labels) = plan.set() else {
            panic!("expected a label set: {:?}", plan.set());
        };
        labels.iter().map(AsRef::as_ref).collect()
    }

    #[test]
    fn full_membership_covers_equality_in_and_label_scoped_predicates() {
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
            let membership = rewrite(predicate.clone()).unwrap();
            assert_eq!(membership.residual(), None, "{predicate:?}");
            assert_eq!(membership.predicate().as_ref(), &predicate);
            assert_eq!(outside_label(&membership), outside);
        }
    }

    #[test]
    fn partial_membership_fuses_unindexed_null_and_range_conjuncts_as_its_residual() {
        let whole = Predicate::and(vec![
            Predicate::eq("$label", "Item"),
            Predicate::eq("kind", "B"),
            Predicate::contains("name", "x"),
            Predicate::eq("status", PropertyValue::Null),
        ]);
        let scoped = rewrite(whole.clone()).unwrap();
        assert_eq!(scoped.predicate().as_ref(), &whole);
        assert_eq!(
            outside_label(&scoped),
            ir::NodeMembershipOutsideLabel::Reject
        );
        assert_eq!(
            scoped.residual().unwrap().as_ref(),
            &Predicate::and(vec![
                Predicate::contains("name", "x"),
                Predicate::eq("status", PropertyValue::Null),
            ])
        );

        // A range conjunct would verify the whole label range, so set matches
        // evaluate it instead.
        for (whole, residual) in [
            (
                Predicate::and(vec![
                    Predicate::eq("kind", "B"),
                    Predicate::contains("name", "x"),
                ]),
                Predicate::contains("name", "x"),
            ),
            (
                Predicate::and(vec![Predicate::eq("kind", "B"), Predicate::gte("rank", 3)]),
                Predicate::gte("rank", 3),
            ),
        ] {
            let membership = rewrite(whole.clone()).unwrap();
            assert_eq!(membership.predicate().as_ref(), &whole);
            assert_eq!(membership.residual().unwrap().as_ref(), &residual);
            assert_eq!(
                outside_label(&membership),
                ir::NodeMembershipOutsideLabel::Evaluate
            );
        }
    }

    #[test]
    fn nested_residual_conjunctions_still_fuse_into_index_membership() {
        let kind = Predicate::eq("kind", "B");
        let pair = Predicate::and(vec![
            Predicate::contains("title", "x"),
            Predicate::contains("name", "y"),
        ]);
        let impossible = Predicate::and(vec![
            Predicate::eq("$label", "Item"),
            Predicate::eq("$label", "Group"),
        ]);
        for nested in [
            Predicate::and(vec![kind.clone(), pair.clone()]),
            Predicate::and(vec![
                kind.clone(),
                Predicate::or(vec![pair.clone(), impossible]),
            ]),
        ] {
            let membership = rewrite(nested.clone()).unwrap();
            assert_eq!(
                outside_label(&membership),
                ir::NodeMembershipOutsideLabel::Evaluate,
                "{nested:?}"
            );
            assert_eq!(membership.residual().unwrap().as_ref(), &pair);
        }
    }

    #[test]
    fn membership_declines_null_range_unindexed_and_impossible_predicates() {
        for predicate in [
            Predicate::eq("kind", PropertyValue::Null),
            Predicate::gte("rank", 3),
            Predicate::gte_param("rank", "min"),
            Predicate::or(vec![Predicate::eq("kind", "B"), Predicate::gte("rank", 3)]),
            Predicate::and(vec![
                Predicate::eq("kind", PropertyValue::Null),
                Predicate::contains("name", "x"),
            ]),
            Predicate::contains("name", "x"),
            Predicate::eq("color", "red"),
            // Two required labels are statically impossible.
            Predicate::and(vec![
                Predicate::eq("$label", "Item"),
                Predicate::eq("$label", "Group"),
            ]),
        ] {
            assert_eq!(rewrite(predicate.clone()), None, "{predicate:?}");
        }
    }

    #[test]
    fn label_domains_decide_label_only_and_unindexed_label_scoped_predicates() {
        let group = Predicate::eq("$label", "Group");
        let label_only = rewrite(group.clone()).unwrap();
        assert_eq!(labels(&label_only), ["Group"]);
        assert_eq!(label_only.residual(), None);

        // A label with no index answering the predicate reads its bitmap and
        // evaluates the rest for its nodes only.
        let kind = Predicate::eq("kind", "B");
        let unindexed = rewrite(Predicate::and(vec![group.clone(), kind.clone()])).unwrap();
        assert_eq!(labels(&unindexed), ["Group"]);
        assert_eq!(unindexed.residual().unwrap().as_ref(), &kind);

        // The indexed label's range and null conjuncts stay in the residual.
        let item = Predicate::eq("$label", "Item");
        for residual in [
            Predicate::gte("rank", 3),
            Predicate::eq("kind", PropertyValue::Null),
        ] {
            let scoped = rewrite(Predicate::and(vec![item.clone(), residual.clone()])).unwrap();
            assert_eq!(labels(&scoped), ["Item"]);
            assert_eq!(scoped.residual().unwrap().as_ref(), &residual);
        }

        let domain = Predicate::is_in(
            "$label",
            PropertyValue::StringArray(vec!["Item".into(), "Group".into()]),
        );
        assert_eq!(labels(&rewrite(domain.clone()).unwrap()), ["Item", "Group"]);
        let limited = |limit| {
            index_membership_filter(
                &plan(domain.clone()),
                &indexes(),
                &context::PlannerLimits {
                    max_index_union_branches: context::IndexUnionBranchLimit::from_usize(limit),
                },
            )
        };
        assert!(limited(2).is_some());
        assert_eq!(limited(1), None);
        assert_eq!(limited(0), None);
    }

    #[test]
    fn membership_never_indexes_virtual_properties() {
        // No secondary index answers `$id`, `$score`, or `$distance`, so an
        // unscoped virtual predicate keeps its per-row filter, and a scoped one
        // only reads its label bitmap.
        for property in ["$id", "$score", "$distance"] {
            let predicate = Predicate::eq(property, 1);
            assert_eq!(rewrite(predicate.clone()), None, "{predicate:?}");
            let scoped = rewrite(Predicate::and(vec![
                Predicate::eq("$label", "Item"),
                predicate.clone(),
            ]))
            .unwrap();
            assert_eq!(labels(&scoped), ["Item"]);
            assert_eq!(scoped.residual().unwrap().as_ref(), &predicate);
        }
    }

    #[test]
    fn membership_keeps_runtime_parameters_for_runtime_classification() {
        let equality = rewrite(Predicate::eq_param("kind", "kind")).unwrap();
        assert!(matches!(
            equality.set(),
            ir::NodeMembershipSet::Index { set, .. } if matches!(
                set.as_ref(),
                ir::NodeAccessPlan::EqualityIndex {
                    value: ir::IndexValue::Param(_),
                    ..
                }
            )
        ));
        let set = rewrite(Predicate::is_in_param("kind", "kinds")).unwrap();
        assert!(matches!(
            set.set(),
            ir::NodeMembershipSet::Index { set, .. } if matches!(
                set.as_ref(),
                ir::NodeAccessPlan::EqualityIndex {
                    value: ir::IndexValue::ParamSet(_),
                    ..
                }
            )
        ));
    }

    #[test]
    fn unscoped_membership_needs_exactly_one_answering_label() {
        let rewrite = |predicate: Predicate, indexes: &catalog::IndexCatalogSnapshot| {
            index_membership_filter(
                &plan(predicate),
                indexes,
                &context::PlannerLimits::default(),
            )
            .map(|membership| {
                let ir::NodeMembershipSet::Index { label, .. } = membership.set() else {
                    panic!("expected an index set");
                };
                label.as_ref().to_owned()
            })
        };
        let key = |label, property| catalog::ScopedPropertyKey::try_new(label, property).unwrap();
        let one = catalog::IndexCatalogSnapshot::default()
            .with_node_eq(key("Alpha", "other"))
            .with_node_eq(key("Beta", "kind"));
        let two = one.clone().with_node_eq(key("Zeta", "kind"));

        // Alpha has an index, but only Beta's answers the predicate.
        assert_eq!(
            rewrite(Predicate::eq("kind", "B"), &one).as_deref(),
            Some("Beta")
        );
        // Beta and Zeta both answer, and the stream may reach either or
        // neither. No label conjunct offers a label domain instead, so the
        // filter stays per row.
        assert_eq!(rewrite(Predicate::eq("kind", "B"), &two), None);
        assert_eq!(
            rewrite(
                Predicate::and(vec![
                    Predicate::eq("kind", "B"),
                    Predicate::eq("other", "x")
                ]),
                &one,
            ),
            None
        );
        // A label-scoped predicate names its label, whatever else is indexed.
        assert_eq!(
            rewrite(
                Predicate::and(vec![
                    Predicate::eq("$label", "Zeta"),
                    Predicate::eq("kind", "B")
                ]),
                &two,
            )
            .as_deref(),
            Some("Zeta")
        );
        assert_eq!(
            rewrite(
                Predicate::eq("kind", "B"),
                &catalog::IndexCatalogSnapshot::default()
            ),
            None
        );
    }
}
