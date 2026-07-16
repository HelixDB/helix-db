use helix_ast::expr::Predicate;

use crate::error::PlannerError;

use super::labels::{self, FeasibleLabelScope, LabelScope};
use super::scalar;

pub(crate) fn prune_statically_impossible_branches(
    predicate: &Predicate,
) -> Result<PrunedPredicate, PlannerError> {
    prune_statically_impossible_branches_inner(predicate)
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PrunedPredicate {
    Impossible,
    Tautology,
    Feasible {
        predicate: Predicate,
        label: FeasibleLabelScope,
    },
}

fn prune_statically_impossible_branches_inner(
    predicate: &Predicate,
) -> Result<PrunedPredicate, PlannerError> {
    match predicate {
        Predicate::And { predicates } if !predicates.is_empty() => {
            let mut pruned = Vec::new();
            for predicate in predicates {
                match prune_statically_impossible_branches_inner(predicate)? {
                    PrunedPredicate::Impossible => return Ok(PrunedPredicate::Impossible),
                    PrunedPredicate::Tautology => {}
                    PrunedPredicate::Feasible { predicate, .. } => pruned.push(predicate),
                }
            }
            Ok(if pruned.is_empty() {
                PrunedPredicate::Tautology
            } else if let [predicate] = pruned.as_slice() {
                feasible_pruned_predicate(predicate.clone())?
            } else {
                checked_pruned_predicate(Predicate::and(pruned))?
            })
        }
        Predicate::Or { predicates } if !predicates.is_empty() => {
            let mut pruned = Vec::new();
            for child in predicates {
                match prune_statically_impossible_branches_inner(child)? {
                    PrunedPredicate::Impossible => {}
                    PrunedPredicate::Tautology => return Ok(PrunedPredicate::Tautology),
                    PrunedPredicate::Feasible { predicate, .. } => pruned.push(predicate),
                }
            }
            Ok(match pruned.as_slice() {
                [] => PrunedPredicate::Impossible,
                [predicate] => feasible_pruned_predicate(predicate.clone())?,
                _ => checked_pruned_predicate(Predicate::or(pruned))?,
            })
        }
        Predicate::Eq { .. }
        | Predicate::Neq { .. }
        | Predicate::Gt { .. }
        | Predicate::Gte { .. }
        | Predicate::Lt { .. }
        | Predicate::Lte { .. }
        | Predicate::Between { .. }
        | Predicate::HasKey { .. }
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::StartsWith { .. }
        | Predicate::EndsWith { .. }
        | Predicate::Contains { .. }
        | Predicate::IsIn { .. }
        | Predicate::And { .. }
        | Predicate::Or { .. }
        | Predicate::Not { .. }
        | Predicate::Compare { .. } => checked_pruned_predicate(predicate.clone()),
    }
}

fn checked_pruned_predicate(predicate: Predicate) -> Result<PrunedPredicate, PlannerError> {
    if scalar::predicate_is_statically_tautological(&predicate) {
        return Ok(PrunedPredicate::Tautology);
    }
    if scalar::predicate_is_statically_impossible(&predicate)
        || matches!(labels::label_scope(&predicate)?, LabelScope::Impossible)
    {
        return Ok(PrunedPredicate::Impossible);
    }
    feasible_pruned_predicate(predicate)
}

pub(super) fn feasible_pruned_predicate(
    predicate: Predicate,
) -> Result<PrunedPredicate, PlannerError> {
    match labels::label_scope(&predicate)? {
        LabelScope::Impossible => Ok(PrunedPredicate::Impossible),
        LabelScope::Feasible(label) => {
            debug_assert!(
                !scalar::predicate_is_statically_impossible(&predicate),
                "pruning must not rebuild scalar-impossible predicates"
            );
            debug_assert!(
                !scalar::predicate_is_statically_tautological(&predicate),
                "pruning must not rebuild tautological predicates"
            );
            Ok(PrunedPredicate::Feasible { predicate, label })
        }
    }
}
