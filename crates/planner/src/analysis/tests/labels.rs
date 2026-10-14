use super::allocations;
use crate::{analysis::labels, error, ir};
use helix_ast::{expr, value};

#[test]
fn label_atoms_borrow_the_original_literal_in_both_equality_directions() {
    for length in [1, 1024, 512 * 1024] {
        for reversed in [false, true] {
            for compare in [false, true] {
                let literal = "L".repeat(length);
                let original = literal.as_ptr();
                let property = expr::Expr::Property("$label".into());
                let literal = expr::Expr::Constant(value::PropertyValue::String(literal));
                let (left, right) = if reversed {
                    (literal, property)
                } else {
                    (property, literal)
                };
                let predicate = if compare {
                    expr::Predicate::compare(left, expr::CompareOp::Eq, right)
                } else {
                    expr::Predicate::Eq { left, right }
                };
                let (label, count) =
                    allocations::observe(|| labels::label_equality_atom(&predicate));
                let label = label.unwrap();
                assert_eq!(label.len(), length);
                assert_eq!(label.as_ptr(), original);
                assert_eq!((count.allocations, count.bytes), (0, 0));
            }
        }
    }
}

#[test]
fn unrelated_string_equalities_do_not_allocate_during_label_proofs() {
    for property in ["bio", "$Label", "label", &"p".repeat(4096)] {
        for reversed in [false, true] {
            let property = expr::Expr::Property(property.to_owned());
            let literal =
                expr::Expr::Constant(value::PropertyValue::String("x".repeat(512 * 1024)));
            let (left, right) = if reversed {
                (literal, property)
            } else {
                (property, literal)
            };
            let predicate = expr::Predicate::Eq { left, right };
            let ((scope, atom), count) = allocations::observe(|| {
                (
                    labels::label_scope(&predicate),
                    labels::label_equality_atom(&predicate),
                )
            });
            assert_eq!(
                scope.unwrap(),
                labels::LabelScope::Feasible(labels::FeasibleLabelScope::Unscoped)
            );
            assert!(atom.is_none());
            assert_eq!((count.allocations, count.bytes), (0, 0));
        }
    }
}

#[test]
fn label_scope_owns_only_successful_proofs_and_preserves_boolean_boundaries() {
    let scoped = labels::LabelScope::Feasible(labels::FeasibleLabelScope::Scoped(
        ir::NonEmptyString::new("N").unwrap(),
    ));
    let unscoped = labels::LabelScope::Feasible(labels::FeasibleLabelScope::Unscoped);
    let n = expr::Predicate::eq("$label", "N");
    let m = expr::Predicate::eq("$label", "M");
    let other = expr::Predicate::eq("bio", "text");
    for (predicate, expected) in [
        (n.clone(), scoped.clone()),
        (
            expr::Predicate::and(vec![n.clone(), n.clone()]),
            scoped.clone(),
        ),
        (
            expr::Predicate::or(vec![n.clone(), n.clone()]),
            scoped.clone(),
        ),
        (
            expr::Predicate::and(vec![n.clone(), m.clone()]),
            labels::LabelScope::Impossible,
        ),
        (expr::Predicate::or(vec![n.clone(), m]), unscoped.clone()),
        (
            expr::Predicate::and(vec![n.clone(), other.clone()]),
            scoped.clone(),
        ),
        (
            expr::Predicate::or(vec![n.clone(), other]),
            unscoped.clone(),
        ),
        (expr::Predicate::not(n), unscoped.clone()),
        (expr::Predicate::and(vec![]), unscoped.clone()),
        (expr::Predicate::or(vec![]), unscoped),
    ] {
        let actual = labels::label_scope(&predicate).unwrap();
        drop(predicate);
        assert_eq!(actual, expected);
    }
    for predicate in [
        expr::Predicate::eq("$label", ""),
        expr::Predicate::and(vec![
            expr::Predicate::eq("$label", "N"),
            expr::Predicate::eq("$label", "M"),
            expr::Predicate::eq("$label", ""),
        ]),
        expr::Predicate::or(vec![
            expr::Predicate::eq("$label", "N"),
            expr::Predicate::eq("$label", ""),
        ]),
    ] {
        assert!(matches!(
            labels::label_scope(&predicate),
            Err(error::PlannerError::InvalidEmptyName {
                field: ir::NameField::Label
            })
        ));
    }
}

#[test]
fn impossible_label_proofs_preserve_conjunction_and_disjunction_boundaries() {
    let contradiction = expr::Predicate::and(vec![
        expr::Predicate::eq("$label", "N"),
        expr::Predicate::eq("$label", "M"),
    ]);
    let n = expr::Predicate::eq("$label", "N");
    let unrelated = expr::Predicate::eq("bio", "text");
    let scoped = labels::LabelScope::Feasible(labels::FeasibleLabelScope::Scoped(
        ir::NonEmptyString::new("N").unwrap(),
    ));
    let unscoped = labels::LabelScope::Feasible(labels::FeasibleLabelScope::Unscoped);
    for (predicate, expected) in [
        (
            expr::Predicate::and(vec![contradiction.clone(), n.clone()]),
            labels::LabelScope::Impossible,
        ),
        (
            expr::Predicate::and(vec![n.clone(), contradiction.clone()]),
            labels::LabelScope::Impossible,
        ),
        (
            expr::Predicate::or(vec![contradiction.clone(), contradiction.clone()]),
            labels::LabelScope::Impossible,
        ),
        (
            expr::Predicate::or(vec![contradiction.clone(), n.clone()]),
            scoped.clone(),
        ),
        (expr::Predicate::or(vec![n, contradiction.clone()]), scoped),
        (
            expr::Predicate::or(vec![contradiction.clone(), unrelated.clone()]),
            unscoped.clone(),
        ),
        (
            expr::Predicate::or(vec![unrelated, contradiction]),
            unscoped,
        ),
    ] {
        assert_eq!(
            labels::label_scope(&predicate).unwrap(),
            expected,
            "{predicate:?}"
        );
    }
}
