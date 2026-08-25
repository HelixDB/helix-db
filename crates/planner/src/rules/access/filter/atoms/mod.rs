//! Access-filter index-plan extraction.

mod collect;
mod limits;
mod property;
mod types;

use crate::{context, ir};

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
            predicates.len(),
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
        // Multiple disjunctive conjuncts require a Cartesian DNF expansion.
        // Keep that out until it has its own bounded ADT contract.
        match predicate {
            helix_ast::expr::Predicate::Or { predicates } if disjunction.is_none() => {
                disjunction = Some(predicates.as_slice());
            }
            helix_ast::expr::Predicate::Or { .. } => return None,
            predicate => shared.push(predicate),
        }
    }

    let branches = disjunction?;
    Some(plan_disjunction_from_atom_results(
        branches.len(),
        branches.iter().map(|branch| {
            if shared.is_empty() {
                collect::access_filter_index_atoms(branch, label, planner_limits)
            } else {
                let mut predicates = shared
                    .iter()
                    .map(|predicate| (*predicate).clone())
                    .collect::<Vec<_>>();
                predicates.push(branch.clone());
                let distributed = helix_ast::expr::Predicate::and(predicates);
                collect::access_filter_index_atoms(&distributed, label, planner_limits)
            }
        }),
        planner_limits,
    ))
}

fn plan_disjunction_from_atom_results(
    branch_count: usize,
    branches: impl IntoIterator<Item = Result<AccessFilterIndexAtoms, AccessFilterIndexPlanRejection>>,
    planner_limits: &context::PlannerLimits,
) -> AccessFilterIndexPlanMatch {
    let Some(max_branches) = limits::max_index_union_branches(planner_limits) else {
        return AccessFilterIndexPlanMatch::NotIndexable(
            AccessFilterIndexPlanRejection::BranchLimitDisabled,
        );
    };
    if branch_count > max_branches {
        return AccessFilterIndexPlanMatch::NotIndexable(
            AccessFilterIndexPlanRejection::BranchLimitExceeded,
        );
    }
    match branches.into_iter().collect::<Result<Vec<_>, _>>() {
        Ok(branches) => match AccessFilterIndexBranches::new(branches) {
            Ok(branches) => {
                AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Disjunction(branches))
            }
            Err(_) => AccessFilterIndexPlanMatch::NotIndexable(
                AccessFilterIndexPlanRejection::TooFewIndexBranches,
            ),
        },
        Err(_) => AccessFilterIndexPlanMatch::NotIndexable(
            AccessFilterIndexPlanRejection::BranchNotIndexable,
        ),
    }
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
        assert_eq!(
            access_filter_index_plan(&predicate, &user_label(), &limited(1)),
            AccessFilterIndexPlanMatch::NotIndexable(
                AccessFilterIndexPlanRejection::BranchLimitExceeded
            )
        );

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
    fn index_plan_distributes_shared_conjunction_into_one_or() {
        let predicate = helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("$label", "User"),
            helix_ast::expr::Predicate::eq("tenant_id", "acme"),
            helix_ast::expr::Predicate::or(vec![
                helix_ast::expr::Predicate::eq("username", "alice"),
                helix_ast::expr::Predicate::eq("username", "bob"),
            ]),
        ]);

        let AccessFilterIndexPlanMatch::Planned(AccessFilterIndexPlan::Disjunction(branches)) =
            access_filter_index_plan(&predicate, &user_label(), &limited(2))
        else {
            panic!("expected distributed disjunction");
        };

        assert_eq!(branches.as_ref().len(), 2);
        assert!(branches
            .as_ref()
            .iter()
            .all(|atoms| atoms.as_ref().len() == 2));
        assert!(branches.as_ref().iter().all(|atoms| {
            atoms.as_ref().iter().any(|atom| {
                matches!(
                    atom,
                    AccessFilterIndexAtom::Equality { property, .. }
                        if property.as_ref() == "tenant_id"
                )
            })
        }));
    }

    #[test]
    fn index_plan_distributed_or_keeps_branch_limit_and_rejects_multi_or_dnf() {
        let distributed = helix_ast::expr::Predicate::and(vec![
            helix_ast::expr::Predicate::eq("$label", "User"),
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

        assert_eq!(
            access_filter_index_plan(&distributed, &user_label(), &limited(1)),
            AccessFilterIndexPlanMatch::NotIndexable(
                AccessFilterIndexPlanRejection::BranchLimitExceeded
            )
        );
        assert_eq!(
            access_filter_index_plan(&multi_or, &user_label(), &limited(4)),
            AccessFilterIndexPlanMatch::NotIndexable(
                AccessFilterIndexPlanRejection::NotIndexCandidate
            )
        );
    }
}
