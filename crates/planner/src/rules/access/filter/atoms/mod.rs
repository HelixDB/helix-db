//! Access-filter index-plan extraction.

mod collect;
mod limits;
mod property;
mod types;

use crate::{analysis, context, ir};

pub(super) use self::collect::access_filter_index_atom;
pub(super) use self::types::{
    AccessEqualityDomain, AccessFilterIndexAtom, AccessFilterIndexAtoms, AccessFilterIndexBranches,
    AccessFilterIndexPlan, AccessFilterIndexPlanMatch, AccessFilterIndexPlanRejection,
};

pub(super) fn access_filter_index_plan(
    predicate: &helix_ast::expr::Predicate,
    label: &ir::NonEmptyString,
    planner_limits: &context::PlannerLimits,
) -> AccessFilterIndexPlanMatch {
    if let Some(plan) = scoped_conjunction_disjunction_plan(predicate, label, planner_limits) {
        return plan;
    }
    match predicate {
        helix_ast::expr::Predicate::Or { predicates } => plan_disjunction_from_atom_results(
            predicates.iter().map(|predicate| {
                collect::access_filter_index_atoms(predicate, label, planner_limits)
            }),
            planner_limits,
        ),
        helix_ast::expr::Predicate::Eq { .. }
        | helix_ast::expr::Predicate::Neq { .. }
        | helix_ast::expr::Predicate::Gt { .. }
        | helix_ast::expr::Predicate::Gte { .. }
        | helix_ast::expr::Predicate::Lt { .. }
        | helix_ast::expr::Predicate::Lte { .. }
        | helix_ast::expr::Predicate::Between { .. }
        | helix_ast::expr::Predicate::HasKey { .. }
        | helix_ast::expr::Predicate::IsNull { .. }
        | helix_ast::expr::Predicate::IsNotNull { .. }
        | helix_ast::expr::Predicate::StartsWith { .. }
        | helix_ast::expr::Predicate::EndsWith { .. }
        | helix_ast::expr::Predicate::Contains { .. }
        | helix_ast::expr::Predicate::IsIn { .. }
        | helix_ast::expr::Predicate::And { .. }
        | helix_ast::expr::Predicate::Not { .. }
        | helix_ast::expr::Predicate::Compare { .. } => {
            match collect::access_filter_index_atoms(predicate, label, planner_limits) {
                Ok(atoms) => {
                    AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Conjunction(atoms))
                }
                Err(reason) => AccessFilterIndexPlanMatch::NotIndexable(reason),
            }
        }
    }
}

fn scoped_conjunction_disjunction_plan(
    predicate: &helix_ast::expr::Predicate,
    label: &ir::NonEmptyString,
    planner_limits: &context::PlannerLimits,
) -> Option<AccessFilterIndexPlanMatch> {
    let helix_ast::expr::Predicate::And { predicates } = predicate else {
        return None;
    };

    let mut disjunction = None;
    let mut shared = Vec::new();
    for predicate in predicates {
        if super::labels::label_equality_matches(predicate, label) {
            continue;
        }
        // This contract covers one disjunctive conjunct. Multiple OR groups
        // retain the existing fallback rather than expanding a Cartesian DNF.
        match predicate {
            helix_ast::expr::Predicate::Or { predicates } if disjunction.is_none() => {
                disjunction = Some(predicates.as_slice());
            }
            helix_ast::expr::Predicate::Or { .. } => return None,
            predicate => shared.push(predicate),
        }
    }

    let branches = disjunction?;
    let disjunction = plan_disjunction_from_atom_results(
        branches
            .iter()
            .map(|branch| collect::access_filter_index_atoms(branch, label, planner_limits)),
        planner_limits,
    );
    if shared.is_empty() {
        return Some(disjunction);
    }
    let AccessFilterIndexPlanMatch::Planned(
        disjunction @ (AccessFilterIndexPlan::Disjunction(_)
        | AccessFilterIndexPlan::Conjunction(_)),
    ) = disjunction
    else {
        return Some(disjunction);
    };
    let predicate = helix_ast::expr::Predicate::and(shared.into_iter().cloned().collect());
    Some(
        match collect::access_filter_index_atoms(&predicate, label, planner_limits) {
            Ok(shared) => match disjunction {
                AccessFilterIndexPlan::Disjunction(branches) => {
                    AccessFilterIndexPlanMatch::Planned(
                        AccessFilterIndexPlan::ConjunctionWithDisjunction { shared, branches },
                    )
                }
                // The disjunction merged into one literal-set atom.
                AccessFilterIndexPlan::Conjunction(atoms) => {
                    match AccessFilterIndexAtoms::new(
                        shared
                            .as_ref()
                            .iter()
                            .chain(atoms.as_ref())
                            .cloned()
                            .collect(),
                    ) {
                        Ok(atoms) => AccessFilterIndexPlanMatch::Planned(
                            AccessFilterIndexPlan::Conjunction(atoms),
                        ),
                        Err(_) => AccessFilterIndexPlanMatch::NotIndexable(
                            AccessFilterIndexPlanRejection::EmptyIndexAtoms,
                        ),
                    }
                }
                AccessFilterIndexPlan::ConjunctionWithDisjunction { .. } => {
                    unreachable!("a disjunction plan never nests a shared conjunction")
                }
            },
            Err(reason) => AccessFilterIndexPlanMatch::NotIndexable(reason),
        },
    )
}

/// Plan an `OR` of index-atom conjunctions as a union of their index sets.
///
/// Branches that are one literal equality on the same property merge first,
/// so an `OR` of many `p == v` reads one literal set rather than one union
/// branch per value. Any number of branches uses the index; only disabled
/// unions keep the per-row filter.
fn plan_disjunction_from_atom_results(
    branches: impl IntoIterator<Item = Result<AccessFilterIndexAtoms, AccessFilterIndexPlanRejection>>,
    planner_limits: &context::PlannerLimits,
) -> AccessFilterIndexPlanMatch {
    let Some(max_branches) = limits::max_index_union_branches(planner_limits) else {
        return AccessFilterIndexPlanMatch::NotIndexable(
            AccessFilterIndexPlanRejection::BranchLimitDisabled,
        );
    };
    let Ok(branches) = branches.into_iter().collect::<Result<Vec<_>, _>>() else {
        return AccessFilterIndexPlanMatch::NotIndexable(
            AccessFilterIndexPlanRejection::BranchNotIndexable,
        );
    };
    let mut branches = merge_literal_equality_branches(branches, max_branches);
    if branches.len() == 1 {
        return AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Conjunction(
            branches.pop().expect("one merged branch"),
        ));
    }
    match AccessFilterIndexBranches::new(branches) {
        Ok(branches) => {
            AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Disjunction(branches))
        }
        Err(_) => AccessFilterIndexPlanMatch::NotIndexable(
            AccessFilterIndexPlanRejection::TooFewIndexBranches,
        ),
    }
}

/// Merge the branches that are one literal equality on the same property
/// into one branch, at the place of the first, whose domain holds every
/// distinct literal: a union of equalities within `max_branches` values, and
/// a literal set beyond. Other branches are kept in order.
fn merge_literal_equality_branches(
    branches: Vec<AccessFilterIndexAtoms>,
    max_branches: usize,
) -> Vec<AccessFilterIndexAtoms> {
    // A function rather than a closure, so the literal borrows from the value.
    fn literal(value: &ir::IndexValue) -> Option<&ir::SecondaryIndexLiteral> {
        match value {
            ir::IndexValue::Literal(literal) => Some(literal),
            ir::IndexValue::Param(_)
            | ir::IndexValue::ParamSet(_)
            | ir::IndexValue::LiteralSet(_) => None,
        }
    }
    // Literal equality branches, grouped by property and then in order, by
    // sorting rather than by searching per branch.
    let mut literal_branches = branches
        .iter()
        .enumerate()
        .filter_map(|(index, branch)| {
            let [AccessFilterIndexAtom::Equality { property, domain }] = branch.as_ref() else {
                return None;
            };
            let literals_only = match domain {
                AccessEqualityDomain::One(value) => literal(value).is_some(),
                AccessEqualityDomain::Many(values) => {
                    values.iter().all(|value| literal(value).is_some())
                }
                AccessEqualityDomain::Batch(_) => true,
                AccessEqualityDomain::Runtime(_) => false,
            };
            literals_only.then_some((property.as_ref(), index, domain))
        })
        .collect::<Vec<_>>();
    literal_branches.sort_unstable_by_key(|(property, index, _)| (*property, *index));
    // Each merged property's domain, keyed by the index of its first branch;
    // its other branches are absorbed.
    let mut absorbed = vec![false; branches.len()];
    let mut merged = Vec::new();
    for run in literal_branches
        .chunk_by(|(left, ..), (right, ..)| left == right)
        .filter(|run| run.len() > 1)
    {
        let mut literals = Vec::new();
        for (_, index, domain) in run {
            absorbed[*index] = true;
            match domain {
                AccessEqualityDomain::One(value) => literals.extend(literal(value).cloned()),
                AccessEqualityDomain::Many(values) => {
                    literals.extend(values.iter().filter_map(literal).cloned());
                }
                AccessEqualityDomain::Batch(values) => literals.extend(values.iter().cloned()),
                AccessEqualityDomain::Runtime(_) => {}
            }
        }
        let mut literals = analysis::distinct_equality_literals(literals);
        let domain = match literals.len() {
            1 => AccessEqualityDomain::One(ir::IndexValue::Literal(
                literals.pop().expect("one distinct literal"),
            )),
            values if values <= max_branches => AccessEqualityDomain::Many(
                ir::AtLeast::try_from_vec(
                    literals.into_iter().map(ir::IndexValue::Literal).collect(),
                )
                .expect("several distinct literals"),
            ),
            _ => AccessEqualityDomain::Batch(
                ir::AtLeast::try_from_vec(literals).expect("several distinct literals"),
            ),
        };
        let (_, first, _) = run[0];
        absorbed[first] = false;
        merged.push((first, domain));
    }
    if merged.is_empty() {
        return branches;
    }
    merged.sort_unstable_by_key(|(first, _)| *first);
    let mut merged = merged.into_iter().peekable();
    branches
        .into_iter()
        .enumerate()
        .filter(|(index, _)| !absorbed[*index])
        .map(|(index, branch)| {
            let Some((_, domain)) = merged.next_if(|(first, _)| *first == index) else {
                return branch;
            };
            let [AccessFilterIndexAtom::Equality { property, .. }] = branch.as_ref() else {
                unreachable!("a merged branch is one equality");
            };
            AccessFilterIndexAtoms::new(vec![AccessFilterIndexAtom::Equality {
                property: property.clone(),
                domain,
            }])
            .expect("a merged branch holds one atom")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user_label() -> ir::NonEmptyString {
        ir::NonEmptyString::new("User").unwrap()
    }

    fn limited(branches: usize) -> context::PlannerLimits {
        context::PlannerLimits {
            max_index_union_branches: context::IndexUnionBranchLimit::limited(branches).unwrap(),
        }
    }

    #[test]
    fn index_plan_reports_or_branch_limit_outcomes() {
        let predicate = helix_ast::expr::Predicate::or(vec![
            helix_ast::expr::Predicate::eq("age", 42),
            helix_ast::expr::Predicate::eq("score", 7),
        ]);

        assert!(matches!(
            access_filter_index_plan(&predicate, &user_label(), &limited(2)),
            AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Disjunction(branches))
                if branches.as_ref().len() == 2
        ));
        // More branches than the union limit still use the index.
        assert!(matches!(
            access_filter_index_plan(&predicate, &user_label(), &limited(1)),
            AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Disjunction(branches))
                if branches.as_ref().len() == 2
        ));

        let disabled = context::PlannerLimits {
            max_index_union_branches: context::IndexUnionBranchLimit::Disabled,
        };
        assert_eq!(
            access_filter_index_plan(&predicate, &user_label(), &disabled),
            AccessFilterIndexPlanMatch::NotIndexable(
                AccessFilterIndexPlanRejection::BranchLimitDisabled
            )
        );
    }

    #[test]
    fn index_plan_reports_unindexable_or_branches() {
        let predicate = helix_ast::expr::Predicate::or(vec![
            helix_ast::expr::Predicate::eq("age", 42),
            helix_ast::expr::Predicate::contains("bio", "rust"),
        ]);

        assert_eq!(
            access_filter_index_plan(&predicate, &user_label(), &limited(2)),
            AccessFilterIndexPlanMatch::NotIndexable(
                AccessFilterIndexPlanRejection::BranchNotIndexable
            )
        );
    }

    #[test]
    fn index_plan_preserves_shared_conjunction_outside_one_or() {
        let predicate = helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("$label", "User"),
            helix_ast::expr::Predicate::eq("tenant_id", "acme"),
            helix_ast::expr::Predicate::or(vec![
                helix_ast::expr::Predicate::eq("username", "alice"),
                helix_ast::expr::Predicate::eq("email", "bob@example.com"),
            ]),
        ]);

        let AccessFilterIndexPlanMatch::Planned(
            AccessFilterIndexPlan::ConjunctionWithDisjunction { shared, branches },
        ) = access_filter_index_plan(&predicate, &user_label(), &limited(2))
        else {
            panic!("expected shared conjunction outside the disjunction");
        };

        assert_eq!(branches.as_ref().len(), 2);
        assert!(branches
            .as_ref()
            .iter()
            .all(|atoms| atoms.as_ref().len() == 1));
        assert!(
            matches!(shared.as_ref(), [AccessFilterIndexAtom::Equality { property, .. }]
            if property.as_ref() == "tenant_id")
        );
    }

    #[test]
    fn index_plan_distributed_or_keeps_branch_limit_and_rejects_multi_or_dnf() {
        let distributed = helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("$label", "User"),
            helix_ast::expr::Predicate::eq("tenant_id", "acme"),
            helix_ast::expr::Predicate::or(vec![
                helix_ast::expr::Predicate::eq("username", "alice"),
                helix_ast::expr::Predicate::eq("username", "bob"),
            ]),
        ]);
        let multi_or = helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("$label", "User"),
            helix_ast::expr::Predicate::or(vec![
                helix_ast::expr::Predicate::eq("username", "alice"),
                helix_ast::expr::Predicate::eq("username", "bob"),
            ]),
            helix_ast::expr::Predicate::or(vec![
                helix_ast::expr::Predicate::eq("status", "active"),
                helix_ast::expr::Predicate::eq("status", "pending"),
            ]),
        ]);

        // The same-property disjunction merges into one literal set, which
        // joins the shared conjunction, however low the union limit.
        assert!(matches!(
            access_filter_index_plan(&distributed, &user_label(), &limited(1)),
            AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Conjunction(atoms))
                if matches!(
                    atoms.as_ref(),
                    [
                        AccessFilterIndexAtom::Equality { .. },
                        AccessFilterIndexAtom::Equality {
                            domain: AccessEqualityDomain::Batch(values),
                            ..
                        },
                    ] if values.len() == 2
                )
        ));
        assert_eq!(
            access_filter_index_plan(&multi_or, &user_label(), &limited(4)),
            AccessFilterIndexPlanMatch::NotIndexable(
                AccessFilterIndexPlanRejection::NotIndexCandidate
            )
        );
    }

    #[test]
    fn same_property_literal_branches_merge_into_one_domain() {
        let branch = |value: i64| helix_ast::expr::Predicate::eq("age", value);
        let wide = helix_ast::expr::Predicate::or((0..200).map(branch).collect());
        assert!(matches!(
            access_filter_index_plan(&wide, &user_label(), &limited(64)),
            AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Conjunction(atoms))
                if matches!(
                    atoms.as_ref(),
                    [AccessFilterIndexAtom::Equality {
                        domain: AccessEqualityDomain::Batch(values),
                        ..
                    }] if values.len() == 200
                )
        ));

        // Repeated values collapse; a narrow merge stays one union.
        let narrow = helix_ast::expr::Predicate::or(vec![
            branch(1),
            helix_ast::expr::Predicate::is_in(
                "age",
                helix_ast::value::PropertyValue::I64Array(vec![2, 1]),
            ),
            branch(2),
        ]);
        assert!(matches!(
            access_filter_index_plan(&narrow, &user_label(), &limited(64)),
            AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Conjunction(atoms))
                if matches!(
                    atoms.as_ref(),
                    [AccessFilterIndexAtom::Equality {
                        domain: AccessEqualityDomain::Many(values),
                        ..
                    }] if values.len() == 2
                )
        ));

        // Other branches keep their place beside the merged one.
        let mixed = helix_ast::expr::Predicate::or(vec![
            branch(1),
            helix_ast::expr::Predicate::eq("score", 7),
            branch(2),
            helix_ast::expr::Predicate::eq_param("age", "age"),
        ]);
        let AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Disjunction(branches)) =
            access_filter_index_plan(&mixed, &user_label(), &limited(64))
        else {
            panic!("expected a disjunction");
        };
        let properties = branches
            .as_ref()
            .iter()
            .map(|atoms| match atoms.as_ref() {
                [AccessFilterIndexAtom::Equality { property, domain }] => (
                    property.as_ref().to_owned(),
                    matches!(domain, AccessEqualityDomain::Many(_)),
                ),
                _ => panic!("every branch is one equality"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            properties,
            [
                ("age".to_owned(), true),
                ("score".to_owned(), false),
                ("age".to_owned(), false),
            ]
        );
    }

    #[test]
    fn index_plan_rejects_unsupported_shared_conjuncts() {
        let predicate = helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::contains("bio", "rust"),
            helix_ast::expr::Predicate::or(vec![
                helix_ast::expr::Predicate::eq("username", "alice"),
                helix_ast::expr::Predicate::eq("username", "bob"),
            ]),
        ]);
        assert_eq!(
            access_filter_index_plan(&predicate, &user_label(), &limited(2)),
            AccessFilterIndexPlanMatch::NotIndexable(
                AccessFilterIndexPlanRejection::NotIndexCandidate
            )
        );
    }

    #[test]
    fn index_plan_label_scoped_or_has_no_shared_membership_work() {
        let predicate = helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("$label", "User"),
            helix_ast::expr::Predicate::or(vec![
                helix_ast::expr::Predicate::eq("username", "alice"),
                helix_ast::expr::Predicate::eq("username", "bob"),
            ]),
        ]);
        assert!(matches!(
            access_filter_index_plan(&predicate, &user_label(), &limited(2)),
            AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Conjunction(atoms))
                if matches!(
                    atoms.as_ref(),
                    [AccessFilterIndexAtom::Equality {
                        domain: AccessEqualityDomain::Many(_),
                        ..
                    }]
                )
        ));
    }
}
