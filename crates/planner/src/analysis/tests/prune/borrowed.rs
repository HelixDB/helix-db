use std::borrow::Cow;

use helix_ast::{expr, value::PropertyValue};

use crate::analysis::{self, PrunedPredicate};
use crate::{error, ir};

use super::super::allocations;

mod legacy;

fn verify(predicate: &expr::Predicate) {
    let expected = legacy::run(predicate);
    let borrowed = analysis::prune_borrowed(predicate).map(|pruned| match pruned {
        PrunedPredicate::Impossible => PrunedPredicate::Impossible,
        PrunedPredicate::Tautology => PrunedPredicate::Tautology,
        PrunedPredicate::Feasible { predicate, label } => PrunedPredicate::Feasible {
            predicate: predicate.into_owned(),
            label,
        },
    });
    // Debug preserves native value variants and non-reflexive NaNs that make
    // derived PartialEq unsuitable as a structural oracle.
    assert_eq!(
        format!("{borrowed:?}"),
        format!("{expected:?}"),
        "{predicate:?}"
    );
    let owned = analysis::prune_statically_impossible_branches(predicate);
    assert_eq!(
        format!("{owned:?}"),
        format!("{expected:?}"),
        "{predicate:?}"
    );
    let (
        Ok(PrunedPredicate::Feasible {
            predicate: actual, ..
        }),
        Ok(PrunedPredicate::Feasible {
            predicate: expected,
            ..
        }),
    ) = (&borrowed, &expected)
    else {
        return;
    };
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
}

#[test]
fn borrowed_pruning_matches_original_branch_and_scalar_contracts() {
    use expr::{CompareOp, Expr, Predicate};
    let values = [
        PropertyValue::Null,
        PropertyValue::Bool(true),
        PropertyValue::I64(1),
        PropertyValue::I64(i64::MAX),
        PropertyValue::F32(-0.0),
        PropertyValue::F64(1.0),
        PropertyValue::F64(f64::NAN),
        PropertyValue::DateTime(1),
        PropertyValue::from("é"),
        PropertyValue::I64Array(vec![1, 2]),
        PropertyValue::Array(vec![PropertyValue::Null]),
        PropertyValue::object([("key", 1)]),
    ];
    let mut leaves = vec![
        Predicate::And { predicates: vec![] },
        Predicate::Or { predicates: vec![] },
        Predicate::has_key("name"),
        Predicate::is_null("age"),
        Predicate::is_not_null("age"),
        Predicate::eq("$label", ""),
        Predicate::eq("$label", "A"),
        Predicate::eq("$label", "B"),
        Predicate::is_in("age", PropertyValue::I64Array(vec![])),
        Predicate::is_in("age", PropertyValue::I64Array(vec![1, 2])),
        Predicate::eq("age", 1),
        Predicate::eq("age", 2),
        Predicate::between("age", 4, 2),
        Predicate::starts_with("name", "a"),
    ];
    for value in values {
        leaves.extend([
            Predicate::eq("value", value.clone()),
            Predicate::neq("value", value.clone()),
            Predicate::compare(
                Expr::Constant(value.clone()),
                CompareOp::Eq,
                Expr::Constant(value),
            ),
        ]);
    }
    for leaf in &leaves {
        verify(leaf);
        verify(&Predicate::not(leaf.clone()));
        verify(&Predicate::and(vec![leaf.clone()]));
        verify(&Predicate::or(vec![leaf.clone()]));
    }
    for left in &leaves {
        for right in &leaves {
            let and = Predicate::and(vec![left.clone(), right.clone()]);
            let or = Predicate::or(vec![left.clone(), right.clone()]);
            for predicate in [
                and.clone(),
                or.clone(),
                Predicate::not(and.clone()),
                Predicate::not(or.clone()),
                Predicate::and(vec![or, left.clone()]),
                Predicate::or(vec![and, right.clone()]),
            ] {
                verify(&predicate);
            }
        }
    }
}

#[test]
fn borrowed_pruning_keeps_short_circuit_and_label_error_order() {
    use expr::{CompareOp, Expr, Predicate};
    let truth = Predicate::compare(
        Expr::Constant(1.into()),
        CompareOp::Eq,
        Expr::Constant(1.into()),
    );
    let falsity = Predicate::compare(
        Expr::Constant(1.into()),
        CompareOp::Eq,
        Expr::Constant(2.into()),
    );
    let invalid = Predicate::eq("$label", "");
    let invalid_label = || {
        Err(error::PlannerError::InvalidEmptyName {
            field: ir::NameField::Label,
        })
    };
    for (predicate, expected) in [
        (
            Predicate::and(vec![falsity.clone(), invalid.clone()]),
            Ok(PrunedPredicate::Impossible),
        ),
        (
            Predicate::or(vec![truth.clone(), invalid.clone()]),
            Ok(PrunedPredicate::Tautology),
        ),
        (
            Predicate::and(vec![invalid.clone(), falsity]),
            invalid_label(),
        ),
        (Predicate::or(vec![invalid.clone(), truth]), invalid_label()),
        (
            Predicate::and(vec![
                Predicate::eq("age", 1),
                Predicate::eq("age", 2),
                invalid.clone(),
            ]),
            invalid_label(),
        ),
        (
            Predicate::and(vec![
                Predicate::and(vec![Predicate::eq("age", 1), Predicate::eq("age", 2)]),
                invalid.clone(),
            ]),
            Ok(PrunedPredicate::Impossible),
        ),
        (
            Predicate::and(vec![
                Predicate::eq("$label", "A"),
                Predicate::eq("$label", "B"),
                invalid,
            ]),
            invalid_label(),
        ),
    ] {
        assert_eq!(
            analysis::prune_statically_impossible_branches(&predicate),
            expected
        );
        verify(&predicate);
    }
}

#[test]
fn unchanged_pruning_borrows_large_predicates_without_tree_allocations() {
    use expr::{Expr, Predicate};
    let large = "x".repeat(1024 * 1024);
    let leaf = Predicate::StartsWith {
        value: Expr::Param("input".into()),
        prefix: Expr::Constant(large.into()),
    };
    for (name, predicate) in [
        ("leaf", leaf.clone()),
        ("not", Predicate::not(leaf.clone())),
        ("and", Predicate::and(vec![leaf.clone(), leaf.clone()])),
        ("or", Predicate::or(vec![leaf.clone(), leaf])),
    ] {
        let (_, before) = allocations::observe(|| legacy::run(&predicate).unwrap());
        let (pruned, after) =
            allocations::observe(|| analysis::prune_borrowed(&predicate).unwrap());
        let PrunedPredicate::Feasible {
            predicate: Cow::Borrowed(borrowed),
            ..
        } = pruned
        else {
            panic!("unchanged {name} must borrow the original predicate");
        };
        assert!(std::ptr::eq(borrowed, &predicate));
        assert!(before.bytes >= 1024 * 1024);
        assert_eq!(after.allocations, 0, "{name}: {after:?}");
        eprintln!("pruning {name}: before {before:?}; after {after:?}");
    }
}

#[test]
fn collapsing_an_owned_rewrite_does_not_copy_its_large_payloads_again() {
    use expr::{CompareOp, Expr, Predicate};
    const PAYLOAD_BYTES: usize = 1024 * 1024;
    let truth = Predicate::compare(
        Expr::Constant(1.into()),
        CompareOp::Eq,
        Expr::Constant(1.into()),
    );
    let leaf = Predicate::StartsWith {
        value: Expr::Param("input".into()),
        prefix: Expr::Constant("x".repeat(PAYLOAD_BYTES).into()),
    };
    let predicate = Predicate::and(vec![
        truth.clone(),
        Predicate::or(vec![Predicate::not(truth), leaf.clone(), leaf]),
    ]);
    let (_, before) = allocations::observe(|| legacy::run(&predicate).unwrap());
    let (pruned, after) = allocations::observe(|| analysis::prune_borrowed(&predicate).unwrap());
    assert!(matches!(
        pruned,
        PrunedPredicate::Feasible {
            predicate: Cow::Owned(_),
            ..
        }
    ));
    // The rewritten OR owns two payloads. Collapsing the enclosing AND moves
    // that result; another payload-sized allocation would exceed this bound.
    assert!(before.bytes >= 4 * PAYLOAD_BYTES, "{before:?}");
    assert!(
        (2 * PAYLOAD_BYTES..3 * PAYLOAD_BYTES).contains(&after.bytes),
        "{after:?}"
    );
    eprintln!("pruning owned collapse: before {before:?}; after {after:?}");
    verify(&predicate);
}

#[test]
fn pruning_rewrites_only_changed_parents_and_borrows_collapsed_descendants() {
    use expr::{CompareOp, Expr, Predicate};
    let truth = Predicate::compare(
        Expr::Constant(1.into()),
        CompareOp::Eq,
        Expr::Constant(1.into()),
    );
    let falsity = Predicate::not(truth.clone());
    let leaf = Predicate::starts_with("name", "a");
    let other = Predicate::ends_with("name", "z");
    for index in 0..3 {
        for (removed, and) in [(truth.clone(), true), (falsity.clone(), false)] {
            let mut children = vec![leaf.clone(), other.clone()];
            children.insert(index, removed);
            let predicate = if and {
                Predicate::and(children)
            } else {
                Predicate::or(children)
            };
            let PrunedPredicate::Feasible {
                predicate: Cow::Owned(_),
                ..
            } = analysis::prune_borrowed(&predicate).unwrap()
            else {
                panic!("two retained children require a rewritten parent");
            };
            verify(&predicate);
        }
    }
    let collapsed = Predicate::and(vec![
        truth.clone(),
        Predicate::or(vec![falsity, leaf.clone()]),
        truth.clone(),
    ]);
    let Predicate::And { predicates } = &collapsed else {
        unreachable!()
    };
    let Predicate::Or { predicates: inner } = &predicates[1] else {
        unreachable!()
    };
    let PrunedPredicate::Feasible {
        predicate: Cow::Borrowed(borrowed),
        ..
    } = analysis::prune_borrowed(&collapsed).unwrap()
    else {
        panic!("a single retained descendant should remain borrowed");
    };
    assert!(std::ptr::eq(borrowed, &inner[1]));
    verify(&collapsed);

    // Borrowed is insufficient evidence that the original child is unchanged.
    let changed = Predicate::or(vec![Predicate::and(vec![leaf.clone()]), other.clone()]);
    assert!(matches!(
        analysis::prune_borrowed(&changed).unwrap(),
        PrunedPredicate::Feasible {
            predicate: Cow::Owned(_),
            ..
        }
    ));
    verify(&changed);
    // Collapsing an owned rewritten child must not clone it again.
    let changed_child = Predicate::and(vec![truth.clone(), leaf, other]);
    verify(&Predicate::and(vec![truth, changed_child]));
}
