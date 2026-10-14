mod borrowed;

use helix_ast::expr::{CompareOp, Predicate};

use super::super::prune::feasible_pruned_predicate;
use super::support::constant;
use crate::analysis::{prune_statically_impossible_branches, PrunedPredicate};
use crate::error::PlannerError;
use crate::ir::NameField;

#[test]
fn branch_pruning_validates_label_scope_at_its_public_boundary() {
    assert_eq!(
        prune_statically_impossible_branches(&Predicate::eq("$label", ""))
            .expect_err("empty labels must remain invalid"),
        PlannerError::InvalidEmptyName {
            field: NameField::Label,
        }
    );
}

#[test]
fn branch_pruning_collapses_compound_tautologies() {
    assert_eq!(
        prune_statically_impossible_branches(&Predicate::and(vec![
            Predicate::or(vec![
                Predicate::compare(constant(10), CompareOp::Gt, constant(1)),
                Predicate::has_key("name"),
            ]),
            Predicate::compare(constant("alice"), CompareOp::Eq, constant("alice")),
        ]))
        .unwrap(),
        PrunedPredicate::Tautology
    );
}

#[test]
fn branch_pruning_flattens_conjunctions_it_leaves_nested() {
    let kind = Predicate::eq("kind", "B");
    let pair = Predicate::and(vec![
        Predicate::contains("title", "x"),
        Predicate::contains("name", "y"),
    ]);
    let impossible = Predicate::and(vec![
        Predicate::eq("$label", "Item"),
        Predicate::eq("$label", "Group"),
    ]);
    let flat = Predicate::and(vec![
        kind.clone(),
        Predicate::contains("title", "x"),
        Predicate::contains("name", "y"),
    ]);
    for nested in [
        Predicate::and(vec![kind.clone(), pair.clone()]),
        Predicate::and(vec![kind.clone(), Predicate::or(vec![pair.clone()])]),
        Predicate::and(vec![kind.clone(), Predicate::or(vec![pair, impossible])]),
    ] {
        assert!(
            matches!(
                prune_statically_impossible_branches(&nested).unwrap(),
                PrunedPredicate::Feasible { predicate, .. } if predicate == flat
            ),
            "{nested:?}"
        );
    }
}

#[test]
fn feasible_pruned_predicate_keeps_label_impossibility_guard() {
    assert_eq!(
        feasible_pruned_predicate(Predicate::and(vec![
            Predicate::eq("$label", "User"),
            Predicate::eq("$label", "Post"),
        ]))
        .unwrap(),
        PrunedPredicate::Impossible
    );
}
