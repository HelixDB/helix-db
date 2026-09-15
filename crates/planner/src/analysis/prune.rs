//! Static pruning with borrowed unchanged trees and owned rewritten branches.

use std::borrow::{Borrow, Cow};

use helix_ast::expr::Predicate;

use crate::error::PlannerError;

use super::labels::{self, FeasibleLabelScope, LabelScope};
use super::scalar;

/// Materialize a pruned predicate for consumers that retain it in a plan.
pub(crate) fn prune_statically_impossible_branches(
    predicate: &Predicate,
) -> Result<PrunedPredicate, PlannerError> {
    Ok(match prune_borrowed(predicate)? {
        PrunedPredicate::Impossible => PrunedPredicate::Impossible,
        PrunedPredicate::Tautology => PrunedPredicate::Tautology,
        PrunedPredicate::Feasible { predicate, label } => PrunedPredicate::Feasible {
            predicate: predicate.into_owned(),
            label,
        },
    })
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PrunedPredicate<P = Predicate> {
    Impossible,
    Tautology,
    Feasible {
        predicate: P,
        label: FeasibleLabelScope,
    },
}

/// Borrow unchanged trees for scheduling and validation. Rewrites preserve
/// child order and existing short-circuit/error precedence. Scalar and label
/// proofs retain their own allocation contracts.
pub(crate) fn prune_borrowed(
    predicate: &Predicate,
) -> Result<PrunedPredicate<Cow<'_, Predicate>>, PlannerError> {
    match predicate {
        Predicate::And { predicates } if !predicates.is_empty() => {
            prune_junction(predicate, predicates, Junction::And)
        }
        Predicate::Or { predicates } if !predicates.is_empty() => {
            prune_junction(predicate, predicates, Junction::Or)
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
        | Predicate::Compare { .. } => checked_pruned_predicate(Cow::Borrowed(predicate)),
    }
}

#[derive(Clone, Copy)]
enum Junction {
    And,
    Or,
}

fn prune_junction<'a>(
    original: &'a Predicate,
    children: &'a [Predicate],
    junction: Junction,
) -> Result<PrunedPredicate<Cow<'a, Predicate>>, PlannerError> {
    debug_assert!(!children.is_empty(), "junction pruning requires a child");
    // None proves all visited children are the original objects in order.
    // Allocate a replacement list only at the first removed/rewritten child.
    let mut rewritten: Option<Vec<Cow<'a, Predicate>>> = None;
    for (index, child) in children.iter().enumerate() {
        let retained = match (junction, prune_borrowed(child)?) {
            (Junction::And, PrunedPredicate::Impossible) => return Ok(PrunedPredicate::Impossible),
            (Junction::Or, PrunedPredicate::Tautology) => return Ok(PrunedPredicate::Tautology),
            (_, PrunedPredicate::Feasible { predicate, .. }) => Some(predicate),
            _ => None,
        };
        if rewritten.is_none()
            && matches!(&retained, Some(Cow::Borrowed(next)) if std::ptr::eq(*next, child))
        {
            continue;
        }
        // A borrowed child can be a collapsed descendant, so borrowing alone
        // does not prove the parent remains unchanged.
        rewritten
            .get_or_insert_with(|| children[..index].iter().map(Cow::Borrowed).collect())
            .extend(retained);
    }
    let Some(mut rewritten) = rewritten else {
        return match children {
            [child] => feasible_pruned_predicate(Cow::Borrowed(child)),
            _ => checked_pruned_predicate(Cow::Borrowed(original)),
        };
    };
    match rewritten.len() {
        0 => Ok(match junction {
            Junction::And => PrunedPredicate::Tautology,
            Junction::Or => PrunedPredicate::Impossible,
        }),
        1 => feasible_pruned_predicate(rewritten.pop().expect("one retained predicate")),
        _ => {
            let children = rewritten.into_iter().map(Cow::into_owned).collect();
            checked_pruned_predicate(Cow::Owned(match junction {
                Junction::And => Predicate::and(children),
                Junction::Or => Predicate::or(children),
            }))
        }
    }
}

fn checked_pruned_predicate<P: Borrow<Predicate>>(
    predicate: P,
) -> Result<PrunedPredicate<P>, PlannerError> {
    if scalar::predicate_is_statically_tautological(predicate.borrow()) {
        return Ok(PrunedPredicate::Tautology);
    }
    if scalar::predicate_is_statically_impossible(predicate.borrow())
        || matches!(
            labels::label_scope(predicate.borrow())?,
            LabelScope::Impossible
        )
    {
        return Ok(PrunedPredicate::Impossible);
    }
    feasible_pruned_predicate(predicate)
}

pub(super) fn feasible_pruned_predicate<P: Borrow<Predicate>>(
    predicate: P,
) -> Result<PrunedPredicate<P>, PlannerError> {
    match labels::label_scope(predicate.borrow())? {
        LabelScope::Impossible => Ok(PrunedPredicate::Impossible),
        LabelScope::Feasible(label) => {
            debug_assert!(
                !scalar::predicate_is_statically_impossible(predicate.borrow()),
                "pruning must not rebuild scalar-impossible predicates"
            );
            debug_assert!(
                !scalar::predicate_is_statically_tautological(predicate.borrow()),
                "pruning must not rebuild tautological predicates"
            );
            Ok(PrunedPredicate::Feasible { predicate, label })
        }
    }
}
