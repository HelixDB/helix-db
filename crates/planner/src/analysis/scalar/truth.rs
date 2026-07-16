//! Constant scalar predicate evaluation.

use std::cmp::Ordering;

use helix_ast::expr::{CompareOp, Expr, Predicate};
use helix_ast::value::PropertyValue;

use super::values::{
    literal_collection_values, property_value_has_reflexive_equality, property_value_ordering,
};

pub(super) fn static_predicate_value(predicate: &Predicate) -> Option<bool> {
    match predicate {
        Predicate::Eq { left, right } => static_eq_value(left, right),
        Predicate::Neq { left, right } => static_eq_value(left, right).map(|value| !value),
        Predicate::Gt { left, right } => {
            static_order_value(left, right, |ordering| ordering == Ordering::Greater)
        }
        Predicate::Gte { left, right } => static_order_value(left, right, |ordering| {
            matches!(ordering, Ordering::Greater | Ordering::Equal)
        }),
        Predicate::Lt { left, right } => {
            static_order_value(left, right, |ordering| ordering == Ordering::Less)
        }
        Predicate::Lte { left, right } => static_order_value(left, right, |ordering| {
            matches!(ordering, Ordering::Less | Ordering::Equal)
        }),
        Predicate::Between { value, min, max } => static_between_value(value, min, max),
        Predicate::StartsWith { value, prefix } => {
            static_string_predicate(value, prefix, |value, prefix| value.starts_with(prefix))
        }
        Predicate::EndsWith { value, suffix } => {
            static_string_predicate(value, suffix, |value, suffix| value.ends_with(suffix))
        }
        Predicate::Contains { value, substring } => {
            static_string_predicate(value, substring, |value, substring| {
                value.contains(substring)
            })
        }
        Predicate::IsIn { value, values } => static_in_value(value, values),
        Predicate::And { predicates } if !predicates.is_empty() => {
            let mut all_true = true;
            for predicate in predicates {
                match static_predicate_value(predicate) {
                    Some(true) => {}
                    Some(false) => return Some(false),
                    None => all_true = false,
                }
            }
            all_true.then_some(true)
        }
        Predicate::Or { predicates } if !predicates.is_empty() => {
            let mut all_false = true;
            for predicate in predicates {
                match static_predicate_value(predicate) {
                    Some(true) => return Some(true),
                    Some(false) => {}
                    None => all_false = false,
                }
            }
            all_false.then_some(false)
        }
        Predicate::Not { predicate } => static_predicate_value(predicate).map(|value| !value),
        Predicate::Compare { left, op, right } => match op {
            CompareOp::Eq => static_eq_value(left, right),
            CompareOp::Neq => static_eq_value(left, right).map(|value| !value),
            CompareOp::Gt => {
                static_order_value(left, right, |ordering| ordering == Ordering::Greater)
            }
            CompareOp::Gte => static_order_value(left, right, |ordering| {
                matches!(ordering, Ordering::Greater | Ordering::Equal)
            }),
            CompareOp::Lt => static_order_value(left, right, |ordering| ordering == Ordering::Less),
            CompareOp::Lte => static_order_value(left, right, |ordering| {
                matches!(ordering, Ordering::Less | Ordering::Equal)
            }),
        },
        Predicate::HasKey { .. }
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::And { .. }
        | Predicate::Or { .. } => None,
    }
}

fn static_eq_value(left: &Expr, right: &Expr) -> Option<bool> {
    let (Expr::Constant(left), Expr::Constant(right)) = (left, right) else {
        return None;
    };
    (property_value_has_reflexive_equality(left) && property_value_has_reflexive_equality(right))
        .then_some(left == right)
}

fn static_order_value(
    left: &Expr,
    right: &Expr,
    predicate: impl FnOnce(Ordering) -> bool,
) -> Option<bool> {
    let (Expr::Constant(left), Expr::Constant(right)) = (left, right) else {
        return None;
    };
    property_value_ordering(left, right).map(predicate)
}

fn static_between_value(value: &Expr, min: &Expr, max: &Expr) -> Option<bool> {
    let (Expr::Constant(value), Expr::Constant(min), Expr::Constant(max)) = (value, min, max)
    else {
        return None;
    };
    let lower_allows = matches!(
        property_value_ordering(value, min)?,
        Ordering::Greater | Ordering::Equal
    );
    if !lower_allows {
        return Some(false);
    }
    Some(matches!(
        property_value_ordering(value, max)?,
        Ordering::Less | Ordering::Equal
    ))
}

fn static_string_predicate(
    value: &Expr,
    expected: &Expr,
    predicate: impl FnOnce(&str, &str) -> bool,
) -> Option<bool> {
    let (
        Expr::Constant(PropertyValue::String(value)),
        Expr::Constant(PropertyValue::String(expected)),
    ) = (value, expected)
    else {
        return None;
    };
    Some(predicate(value, expected))
}

fn static_in_value(value: &Expr, values: &Expr) -> Option<bool> {
    let (Expr::Constant(value), Expr::Constant(values)) = (value, values) else {
        return None;
    };
    property_value_has_reflexive_equality(value)
        .then(|| literal_collection_values(values).map(|values| values.contains(value)))
        .flatten()
}
