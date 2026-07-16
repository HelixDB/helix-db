//! Per-property scalar constraint state.

use helix_ast::value::PropertyValue;

use super::super::extract::{LiteralBound, NullabilityConstraint};
use super::bounds::{
    lower_bound_allows_value, range_bounds_are_disjoint, upper_bound_allows_value,
};

#[derive(Debug, Default)]
pub(super) struct ScalarPropertyConstraint {
    equality: Option<PropertyValue>,
    inequalities: Vec<PropertyValue>,
    lower_bounds: Vec<LiteralBound>,
    upper_bounds: Vec<LiteralBound>,
    nullability: Option<NullabilityConstraint>,
    allowed_values: Option<Vec<PropertyValue>>,
}

impl ScalarPropertyConstraint {
    pub(super) fn add_equality(&mut self, value: PropertyValue) -> bool {
        if self
            .equality
            .as_ref()
            .is_some_and(|existing| existing != &value)
            || self.inequalities.iter().any(|excluded| excluded == &value)
            || self
                .allowed_values
                .as_ref()
                .is_some_and(|values| !values.contains(&value))
            || self
                .lower_bounds
                .iter()
                .any(|bound| !lower_bound_allows_value(bound, &value))
            || self
                .upper_bounds
                .iter()
                .any(|bound| !upper_bound_allows_value(bound, &value))
        {
            return true;
        }
        let nullability = if value == PropertyValue::Null {
            NullabilityConstraint::NullOrMissing
        } else {
            NullabilityConstraint::NonNull
        };
        if self.add_nullability(nullability) {
            return true;
        }
        self.equality.get_or_insert(value);
        false
    }

    pub(super) fn add_inequality(&mut self, value: PropertyValue) -> bool {
        if self
            .equality
            .as_ref()
            .is_some_and(|existing| existing == &value)
        {
            return true;
        }
        self.inequalities.push(value);
        if self
            .allowed_values
            .as_ref()
            .is_some_and(|values| self.allowed_values_are_impossible(values))
        {
            return true;
        }
        false
    }

    pub(super) fn add_lower(&mut self, bound: LiteralBound) -> bool {
        if self.add_nullability(NullabilityConstraint::NonNull)
            || self
                .equality
                .as_ref()
                .is_some_and(|value| !lower_bound_allows_value(&bound, value))
            || self
                .upper_bounds
                .iter()
                .any(|upper| range_bounds_are_disjoint(&bound, upper))
        {
            return true;
        }
        self.lower_bounds.push(bound);
        self.allowed_values
            .as_ref()
            .is_some_and(|values| self.allowed_values_are_impossible(values))
    }

    pub(super) fn add_upper(&mut self, bound: LiteralBound) -> bool {
        if self.add_nullability(NullabilityConstraint::NonNull)
            || self
                .equality
                .as_ref()
                .is_some_and(|value| !upper_bound_allows_value(&bound, value))
            || self
                .lower_bounds
                .iter()
                .any(|lower| range_bounds_are_disjoint(lower, &bound))
        {
            return true;
        }
        self.upper_bounds.push(bound);
        self.allowed_values
            .as_ref()
            .is_some_and(|values| self.allowed_values_are_impossible(values))
    }

    pub(super) fn add_nullability(&mut self, nullability: NullabilityConstraint) -> bool {
        if self
            .nullability
            .is_some_and(|existing| existing != nullability)
        {
            return true;
        }
        self.nullability.get_or_insert(nullability);
        self.allowed_values
            .as_ref()
            .is_some_and(|values| self.allowed_values_are_impossible(values))
    }

    pub(super) fn add_allowed_values(&mut self, values: Vec<PropertyValue>) -> bool {
        let values = match &self.allowed_values {
            Some(existing) => intersect_property_values(existing, &values),
            None => values,
        };
        if values.is_empty() || self.allowed_values_are_impossible(&values) {
            return true;
        }
        self.allowed_values = Some(values);
        false
    }

    fn allowed_values_are_impossible(&self, values: &[PropertyValue]) -> bool {
        values
            .iter()
            .all(|value| !self.value_satisfies_constraints(value))
    }

    fn value_satisfies_constraints(&self, value: &PropertyValue) -> bool {
        self.equality
            .as_ref()
            .is_none_or(|existing| existing == value)
            && self.inequalities.iter().all(|excluded| excluded != value)
            && self
                .lower_bounds
                .iter()
                .all(|bound| lower_bound_allows_value(bound, value))
            && self
                .upper_bounds
                .iter()
                .all(|bound| upper_bound_allows_value(bound, value))
            && self
                .nullability
                .is_none_or(|nullability| nullability_allows_value(nullability, value))
    }
}

fn intersect_property_values(
    left: &[PropertyValue],
    right: &[PropertyValue],
) -> Vec<PropertyValue> {
    left.iter()
        .filter(|value| right.contains(value))
        .cloned()
        .collect()
}

fn nullability_allows_value(nullability: NullabilityConstraint, value: &PropertyValue) -> bool {
    match nullability {
        NullabilityConstraint::NullOrMissing => value == &PropertyValue::Null,
        NullabilityConstraint::NonNull => value != &PropertyValue::Null,
    }
}
