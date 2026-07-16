use helix_ast::expr::Predicate;
use helix_ast::value::PropertyValue;

use crate::analysis::{
    prune_statically_impossible_branches, scalar_property_conjunction_is_impossible,
    PrunedPredicate,
};

#[test]
fn scalar_conjunction_detects_property_constraint_contradictions() {
    [
        Predicate::and(vec![Predicate::eq("age", 30), Predicate::neq("age", 30)]),
        Predicate::and(vec![Predicate::eq("age", 30), Predicate::eq("age", 31)]),
        Predicate::and(vec![Predicate::gt("age", 30), Predicate::lte("age", 30)]),
        Predicate::and(vec![Predicate::gte("age", 30), Predicate::lt("age", 30)]),
        Predicate::and(vec![
            Predicate::between("age", 20, 30),
            Predicate::gt("age", 30),
        ]),
        Predicate::and(vec![Predicate::eq("age", 20), Predicate::gt("age", 20)]),
        Predicate::and(vec![Predicate::eq("age", 20), Predicate::lt("age", 20)]),
        Predicate::and(vec![
            Predicate::is_in("age", PropertyValue::I64Array(vec![20, 30])),
            Predicate::gt("age", 30),
        ]),
        Predicate::and(vec![
            Predicate::is_in("age", PropertyValue::I64Array(vec![20, 30])),
            Predicate::neq("age", 20),
            Predicate::neq("age", 30),
        ]),
        Predicate::and(vec![
            Predicate::is_in("age", PropertyValue::I64Array(vec![20, 30])),
            Predicate::is_in("age", PropertyValue::I64Array(vec![40, 50])),
        ]),
        Predicate::and(vec![
            Predicate::is_null("deleted_at"),
            Predicate::is_not_null("deleted_at"),
        ]),
        Predicate::and(vec![
            Predicate::is_null("deleted_at"),
            Predicate::eq("deleted_at", 1),
        ]),
        Predicate::and(vec![
            Predicate::is_not_null("deleted_at"),
            Predicate::eq("deleted_at", PropertyValue::Null),
        ]),
        Predicate::is_in("age", PropertyValue::I64Array(Vec::new())),
        Predicate::or(vec![
            Predicate::and(vec![Predicate::eq("age", 30), Predicate::neq("age", 30)]),
            Predicate::and(vec![
                Predicate::gt("score", 10),
                Predicate::lte("score", 10),
            ]),
        ]),
    ]
    .into_iter()
    .for_each(|predicate| {
        assert!(
            scalar_property_conjunction_is_impossible(&predicate),
            "expected impossible scalar predicate: {predicate:?}"
        );
        assert_eq!(
            prune_statically_impossible_branches(&predicate).unwrap(),
            PrunedPredicate::Impossible
        );
    });
}

#[test]
fn scalar_conjunction_keeps_feasible_property_constraints() {
    [
        Predicate::and(vec![Predicate::gte("age", 20), Predicate::lte("age", 30)]),
        Predicate::and(vec![Predicate::gt("age", 20), Predicate::lt("age", 30)]),
        Predicate::and(vec![
            Predicate::is_in("age", PropertyValue::I64Array(vec![20, 30])),
            Predicate::neq("age", 20),
        ]),
        Predicate::and(vec![
            Predicate::is_in("name", PropertyValue::StringArray(vec!["alice".to_owned()])),
            Predicate::eq("name", "alice"),
        ]),
        Predicate::and(vec![
            Predicate::eq("deleted_at", PropertyValue::Null),
            Predicate::is_null("deleted_at"),
        ]),
        Predicate::and(vec![
            Predicate::gt("age", 20),
            Predicate::lt("age_text", "30"),
        ]),
        Predicate::or(vec![
            Predicate::and(vec![Predicate::eq("age", 30), Predicate::neq("age", 30)]),
            Predicate::eq("age", 31),
        ]),
    ]
    .into_iter()
    .for_each(|predicate| {
        assert!(
            !scalar_property_conjunction_is_impossible(&predicate),
            "expected feasible scalar predicate: {predicate:?}"
        );
        assert!(matches!(
            prune_statically_impossible_branches(&predicate).unwrap(),
            PrunedPredicate::Feasible { .. }
        ));
    });
}
