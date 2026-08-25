//! Planner-internal reuse of access-filter indexability contracts for stable
//! missing-index diagnostics.

use std::collections::HashSet;

use super::atoms::{
    AccessEqualityDomain, AccessFilterIndexAtom, AccessFilterIndexPlan, AccessFilterIndexPlanMatch,
};
use crate::{catalog, context, ir};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CandidateIndexKind {
    Equality,
    Range,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct MissingIndexCandidate {
    pub(crate) property: ir::NonEmptyString,
    pub(crate) kind: CandidateIndexKind,
}

pub(crate) fn missing_index_candidates(
    element: catalog::ElementKind,
    label: &ir::NonEmptyString,
    predicate: &helix_ast::expr::Predicate,
    ctx: &context::PlannerContext,
) -> Vec<MissingIndexCandidate> {
    let mut plans = Vec::new();
    match super::atoms::access_filter_index_plan(predicate, label, &ctx.limits) {
        AccessFilterIndexPlanMatch::Planned(plan) => plans.push(plan),
        AccessFilterIndexPlanMatch::NotIndexable(_) => {
            let helix_ast::expr::Predicate::And { predicates } = predicate else {
                return Vec::new();
            };
            predicates
                .iter()
                .filter(|predicate| !matches!(predicate, helix_ast::expr::Predicate::Or { .. }))
                .filter_map(|predicate| {
                    match super::atoms::access_filter_index_plan(predicate, label, &ctx.limits) {
                        AccessFilterIndexPlanMatch::Planned(plan) => Some(plan),
                        AccessFilterIndexPlanMatch::NotIndexable(_) => None,
                    }
                })
                .for_each(|plan| plans.push(plan));
        }
    }

    let mut candidates = HashSet::new();
    plans
        .iter()
        .for_each(|plan| collect_missing(element, label, plan, &ctx.indexes, &mut candidates));
    let mut candidates = candidates.into_iter().collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.property
            .cmp(&right.property)
            .then_with(|| candidate_kind_rank(left.kind).cmp(&candidate_kind_rank(right.kind)))
    });
    candidates
}

fn collect_missing(
    element: catalog::ElementKind,
    label: &ir::NonEmptyString,
    plan: &AccessFilterIndexPlan,
    indexes: &catalog::IndexCatalogSnapshot,
    candidates: &mut HashSet<MissingIndexCandidate>,
) {
    match plan {
        AccessFilterIndexPlan::Conjunction(atoms) => atoms
            .as_ref()
            .iter()
            .for_each(|atom| collect_missing_atom(element, label, atom, indexes, candidates)),
        AccessFilterIndexPlan::Disjunction(branches) => branches
            .as_ref()
            .iter()
            .flat_map(|atoms| atoms.as_ref())
            .for_each(|atom| collect_missing_atom(element, label, atom, indexes, candidates)),
    }
}

fn collect_missing_atom(
    element: catalog::ElementKind,
    label: &ir::NonEmptyString,
    atom: &AccessFilterIndexAtom,
    indexes: &catalog::IndexCatalogSnapshot,
    candidates: &mut HashSet<MissingIndexCandidate>,
) {
    let (property, kind, present) = match atom {
        AccessFilterIndexAtom::Equality { property, domain } => {
            if matches!(
                domain,
                AccessEqualityDomain::One(ir::IndexValue::Literal(value))
                    if value.as_property_value() == &helix_ast::value::PropertyValue::Null
            ) {
                return;
            }
            let key = catalog::ScopedPropertyKey::new(label.clone(), property.clone());
            let present = match element {
                catalog::ElementKind::Node => indexes.node_eq.contains_key(&key),
                catalog::ElementKind::Edge => indexes.edge_eq.contains_key(&key),
            };
            (property, CandidateIndexKind::Equality, present)
        }
        AccessFilterIndexAtom::Range { property, .. } => {
            let present = [
                helix_ast::index::RangeIndexDirection::Asc,
                helix_ast::index::RangeIndexDirection::Desc,
            ]
            .into_iter()
            .any(|direction| {
                let key = catalog::ScopedPropertyDirectionKey::new(
                    label.clone(),
                    property.clone(),
                    direction,
                );
                match element {
                    catalog::ElementKind::Node => indexes.node_range.contains_key(&key),
                    catalog::ElementKind::Edge => indexes.edge_range.contains_key(&key),
                }
            });
            (property, CandidateIndexKind::Range, present)
        }
    };
    if !present {
        candidates.insert(MissingIndexCandidate {
            property: property.clone(),
            kind,
        });
    }
}

const fn candidate_kind_rank(kind: CandidateIndexKind) -> u8 {
    match kind {
        CandidateIndexKind::Equality => 0,
        CandidateIndexKind::Range => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_ast::expr::{CompareOp, Expr, Predicate};
    use helix_ast::index::RangeIndexDirection;

    #[test]
    fn candidates_reuse_full_and_partial_indexability_contracts() {
        let label = ir::NonEmptyString::new("User").unwrap();
        let ctx = context::PlannerContext {
            indexes: catalog::IndexCatalogSnapshot::default()
                .with_node_eq(catalog::ScopedPropertyKey::try_new("User", "tenant").unwrap()),
            ..context::PlannerContext::default()
        };
        let predicate = Predicate::and(vec![
            Predicate::eq("tenant", "acme"),
            Predicate::gte("age", 21),
            Predicate::contains("bio", "rust"),
        ]);

        assert_eq!(
            missing_index_candidates(catalog::ElementKind::Node, &label, &predicate, &ctx),
            vec![MissingIndexCandidate {
                property: ir::NonEmptyString::new("age").unwrap(),
                kind: CandidateIndexKind::Range,
            }]
        );
    }

    #[test]
    fn incomplete_disjunctions_do_not_claim_an_index_will_remove_the_scan() {
        let label = ir::NonEmptyString::new("User").unwrap();
        let predicate = Predicate::or(vec![
            Predicate::eq("age", 42),
            Predicate::contains("bio", "rust"),
        ]);

        assert!(missing_index_candidates(
            catalog::ElementKind::Node,
            &label,
            &predicate,
            &context::PlannerContext::default(),
        )
        .is_empty());
    }

    #[test]
    fn equality_range_parameter_and_reversed_atoms_share_one_candidate_contract() {
        let label = ir::NonEmptyString::new("User").unwrap();
        let cases = [
            (Predicate::eq("value", 1), CandidateIndexKind::Equality),
            (
                Predicate::eq_param("value", "needle"),
                CandidateIndexKind::Equality,
            ),
            (Predicate::gt("value", 1), CandidateIndexKind::Range),
            (Predicate::gte("value", 1), CandidateIndexKind::Range),
            (Predicate::lt("value", 1), CandidateIndexKind::Range),
            (Predicate::lte("value", 1), CandidateIndexKind::Range),
            (Predicate::between("value", 1, 9), CandidateIndexKind::Range),
            (
                Predicate::compare(Expr::val(1), CompareOp::Eq, Expr::prop("value")),
                CandidateIndexKind::Equality,
            ),
            (
                Predicate::compare(Expr::val(1), CompareOp::Lt, Expr::prop("value")),
                CandidateIndexKind::Range,
            ),
        ];

        for (predicate, kind) in cases {
            assert_eq!(
                missing_index_candidates(
                    catalog::ElementKind::Node,
                    &label,
                    &predicate,
                    &context::PlannerContext::default(),
                ),
                vec![MissingIndexCandidate {
                    property: ir::NonEmptyString::new("value").unwrap(),
                    kind,
                }]
            );
        }
    }

    #[test]
    fn catalog_suppression_is_scoped_by_element_label_property_and_kind() {
        let label = ir::NonEmptyString::new("User").unwrap();
        let predicate = Predicate::eq("username", "alice");
        let unrelated = catalog::IndexCatalogSnapshot::default()
            .with_edge_eq(catalog::ScopedPropertyKey::try_new("User", "username").unwrap())
            .with_node_eq(catalog::ScopedPropertyKey::try_new("Account", "username").unwrap())
            .with_node_eq(catalog::ScopedPropertyKey::try_new("User", "email").unwrap())
            .with_node_range(
                catalog::ScopedPropertyDirectionKey::try_new(
                    "User",
                    "username",
                    RangeIndexDirection::Asc,
                )
                .unwrap(),
            );
        let unrelated_ctx = context::PlannerContext {
            indexes: unrelated,
            ..context::PlannerContext::default()
        };
        assert_eq!(
            missing_index_candidates(
                catalog::ElementKind::Node,
                &label,
                &predicate,
                &unrelated_ctx,
            )
            .len(),
            1
        );

        let matching_ctx = context::PlannerContext {
            indexes: unrelated_ctx
                .indexes
                .clone()
                .with_node_eq(catalog::ScopedPropertyKey::try_new("User", "username").unwrap()),
            ..context::PlannerContext::default()
        };
        assert!(missing_index_candidates(
            catalog::ElementKind::Node,
            &label,
            &predicate,
            &matching_ctx,
        )
        .is_empty());
    }

    #[test]
    fn either_range_direction_satisfies_a_range_candidate() {
        let label = ir::NonEmptyString::new("FOLLOWS").unwrap();
        for direction in [RangeIndexDirection::Asc, RangeIndexDirection::Desc] {
            let ctx = context::PlannerContext {
                indexes: catalog::IndexCatalogSnapshot::default().with_edge_range(
                    catalog::ScopedPropertyDirectionKey::try_new("FOLLOWS", "weight", direction)
                        .unwrap(),
                ),
                ..context::PlannerContext::default()
            };
            assert!(missing_index_candidates(
                catalog::ElementKind::Edge,
                &label,
                &Predicate::gte("weight", 10),
                &ctx,
            )
            .is_empty());
        }
    }

    #[test]
    fn duplicate_atoms_are_deduplicated_and_sorted_by_property_then_kind() {
        let label = ir::NonEmptyString::new("User").unwrap();
        let predicate = Predicate::and(vec![
            Predicate::gte("same", 1),
            Predicate::eq("zeta", 1),
            Predicate::eq("zeta", 1),
            Predicate::eq("same", 1),
        ]);

        assert_eq!(
            missing_index_candidates(
                catalog::ElementKind::Node,
                &label,
                &predicate,
                &context::PlannerContext::default(),
            ),
            vec![
                MissingIndexCandidate {
                    property: ir::NonEmptyString::new("same").unwrap(),
                    kind: CandidateIndexKind::Equality,
                },
                MissingIndexCandidate {
                    property: ir::NonEmptyString::new("same").unwrap(),
                    kind: CandidateIndexKind::Range,
                },
                MissingIndexCandidate {
                    property: ir::NonEmptyString::new("zeta").unwrap(),
                    kind: CandidateIndexKind::Equality,
                },
            ]
        );
    }

    #[test]
    fn disabled_union_planning_and_unsupported_atoms_return_no_candidates() {
        let label = ir::NonEmptyString::new("User").unwrap();
        let union = Predicate::or(vec![
            Predicate::eq("username", "alice"),
            Predicate::eq("email", "alice@example.com"),
        ]);
        for max_index_union_branches in [
            context::IndexUnionBranchLimit::Disabled,
            context::IndexUnionBranchLimit::limited(1).unwrap(),
        ] {
            let ctx = context::PlannerContext {
                limits: context::PlannerLimits {
                    max_index_union_branches,
                },
                ..context::PlannerContext::default()
            };
            assert!(
                missing_index_candidates(catalog::ElementKind::Node, &label, &union, &ctx,)
                    .is_empty()
            );
        }

        let predicates = [
            Predicate::contains("bio", "rust"),
            Predicate::compare(Expr::prop("left"), CompareOp::Eq, Expr::prop("right")),
        ];

        for predicate in predicates {
            assert!(missing_index_candidates(
                catalog::ElementKind::Node,
                &label,
                &predicate,
                &context::PlannerContext::default(),
            )
            .is_empty());
        }
    }
}
