//! Predicate traversal for scalar contradiction accumulation.

use std::collections::BTreeMap;

use helix_ast::expr::{Expr, Predicate};

use super::super::extract::{
    between_literal_bounds, equality_literal, inequality_literal, literal_in_values,
    nullability_constraint, range_bound_literal, BoundKind,
};
use super::super::truth::static_predicate_value;
use super::super::values::literal_collection_is_empty;
use super::property::ScalarPropertyConstraint;

pub(super) fn predicate_is_statically_impossible(predicate: &Predicate) -> bool {
    if static_predicate_value(predicate) == Some(false) {
        return true;
    }
    match predicate {
        Predicate::And { predicates } => conjunction_is_statically_impossible(predicates),
        Predicate::Or { predicates } => {
            !predicates.is_empty() && predicates.iter().all(predicate_is_statically_impossible)
        }
        predicate => atomic_predicate_is_statically_impossible(predicate),
    }
}

fn conjunction_is_statically_impossible(predicates: &[Predicate]) -> bool {
    let mut constraints = BTreeMap::new();
    predicates
        .iter()
        .any(|predicate| add_conjunctive_constraint(predicate, &mut constraints))
}

fn add_conjunctive_constraint(
    predicate: &Predicate,
    constraints: &mut BTreeMap<String, ScalarPropertyConstraint>,
) -> bool {
    match predicate {
        Predicate::And { predicates } => predicates
            .iter()
            .any(|predicate| add_conjunctive_constraint(predicate, constraints)),
        Predicate::Or { predicates } => {
            !predicates.is_empty() && predicates.iter().all(predicate_is_statically_impossible)
        }
        predicate => add_atomic_constraint(predicate, constraints),
    }
}

fn atomic_predicate_is_statically_impossible(predicate: &Predicate) -> bool {
    let mut constraints = BTreeMap::new();
    add_atomic_constraint(predicate, &mut constraints)
}

fn add_atomic_constraint(
    predicate: &Predicate,
    constraints: &mut BTreeMap<String, ScalarPropertyConstraint>,
) -> bool {
    if is_in_empty_literal_collection(predicate) {
        return true;
    }
    if let Some((property, nullability)) = nullability_constraint(predicate) {
        return constraints
            .entry(property)
            .or_default()
            .add_nullability(nullability);
    }
    if let Some((property, values)) = literal_in_values(predicate) {
        return constraints
            .entry(property)
            .or_default()
            .add_allowed_values(values);
    }
    if let Some((property, value)) = equality_literal(predicate) {
        return constraints.entry(property).or_default().add_equality(value);
    }
    if let Some((property, value)) = inequality_literal(predicate) {
        return constraints
            .entry(property)
            .or_default()
            .add_inequality(value);
    }
    if let Some((property, kind, bound)) = range_bound_literal(predicate) {
        let constraint = constraints.entry(property).or_default();
        return match kind {
            BoundKind::Lower => constraint.add_lower(bound),
            BoundKind::Upper => constraint.add_upper(bound),
        };
    }
    if let Some((property, lower, upper)) = between_literal_bounds(predicate) {
        let constraint = constraints.entry(property).or_default();
        return constraint.add_lower(lower) || constraint.add_upper(upper);
    }
    false
}

fn is_in_empty_literal_collection(predicate: &Predicate) -> bool {
    matches!(
        predicate,
        Predicate::IsIn {
            values: Expr::Constant(values),
            ..
        } if literal_collection_is_empty(values)
    )
}
