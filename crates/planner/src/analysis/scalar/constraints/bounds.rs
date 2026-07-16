//! Literal-bound satisfaction proofs for scalar constraints.

use std::cmp::Ordering;

use helix_ast::value::PropertyValue;

use super::super::extract::LiteralBound;
use super::super::values::property_value_ordering;

pub(super) fn lower_bound_allows_value(bound: &LiteralBound, value: &PropertyValue) -> bool {
    match property_value_ordering(value, bound.value()) {
        Some(Ordering::Less) => false,
        Some(Ordering::Equal) => bound.is_inclusive(),
        Some(Ordering::Greater) | None => true,
    }
}

pub(super) fn upper_bound_allows_value(bound: &LiteralBound, value: &PropertyValue) -> bool {
    match property_value_ordering(value, bound.value()) {
        Some(Ordering::Greater) => false,
        Some(Ordering::Equal) => bound.is_inclusive(),
        Some(Ordering::Less) | None => true,
    }
}

pub(super) fn range_bounds_are_disjoint(lower: &LiteralBound, upper: &LiteralBound) -> bool {
    match property_value_ordering(lower.value(), upper.value()) {
        Some(Ordering::Greater) => true,
        Some(Ordering::Equal) => !lower.is_inclusive() || !upper.is_inclusive(),
        Some(Ordering::Less) | None => false,
    }
}
