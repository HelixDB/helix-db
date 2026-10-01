//! Property-value comparison and finite literal-collection contracts.
//!
//! Numeric identity is [`helix_value_semantics::CanonicalNumber`], the same
//! kernel storage uses for `eq_value` and equality-index bytes. Strings,
//! datetimes, and other domains stay typed: `"42"` is not `42`.

use std::cmp::Ordering;

use helix_ast::value::PropertyValue;
use helix_value_semantics::CanonicalNumber;

pub(super) fn literal_collection_is_empty(value: &PropertyValue) -> bool {
    match value {
        PropertyValue::I64Array(values) => values.is_empty(),
        PropertyValue::F64Array(values) => values.is_empty(),
        PropertyValue::F32Array(values) => values.is_empty(),
        PropertyValue::StringArray(values) => values.is_empty(),
        PropertyValue::Array(values) => values.is_empty(),
        PropertyValue::Null
        | PropertyValue::Bool(_)
        | PropertyValue::I64(_)
        | PropertyValue::DateTime(_)
        | PropertyValue::F64(_)
        | PropertyValue::F32(_)
        | PropertyValue::String(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::Object(_) => false,
    }
}

pub(super) fn literal_collection_values(value: &PropertyValue) -> Option<Vec<PropertyValue>> {
    let values = match value {
        PropertyValue::I64Array(values) => values
            .iter()
            .copied()
            .map(PropertyValue::I64)
            .collect::<Vec<_>>(),
        PropertyValue::F64Array(values) if values.iter().all(|value| !value.is_nan()) => values
            .iter()
            .copied()
            .map(PropertyValue::F64)
            .collect::<Vec<_>>(),
        PropertyValue::F32Array(values) if values.iter().all(|value| !value.is_nan()) => values
            .iter()
            .copied()
            .map(PropertyValue::F32)
            .collect::<Vec<_>>(),
        PropertyValue::StringArray(values) => values
            .iter()
            .cloned()
            .map(PropertyValue::String)
            .collect::<Vec<_>>(),
        PropertyValue::Array(values)
            if values.iter().all(property_value_has_reflexive_equality) =>
        {
            values.clone()
        }
        PropertyValue::Null
        | PropertyValue::Bool(_)
        | PropertyValue::I64(_)
        | PropertyValue::DateTime(_)
        | PropertyValue::F64(_)
        | PropertyValue::F32(_)
        | PropertyValue::String(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::Object(_)
        | PropertyValue::F64Array(_)
        | PropertyValue::F32Array(_)
        | PropertyValue::Array(_) => return None,
    };
    Some(dedup_property_values(values))
}

pub(super) fn property_value_has_reflexive_equality(value: &PropertyValue) -> bool {
    match value {
        PropertyValue::F64(value) => !value.is_nan(),
        PropertyValue::F32(value) => !value.is_nan(),
        PropertyValue::Array(values) => values.iter().all(property_value_has_reflexive_equality),
        PropertyValue::Object(values) => values.values().all(property_value_has_reflexive_equality),
        PropertyValue::Null
        | PropertyValue::Bool(_)
        | PropertyValue::I64(_)
        | PropertyValue::DateTime(_)
        | PropertyValue::String(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::I64Array(_)
        | PropertyValue::F64Array(_)
        | PropertyValue::F32Array(_)
        | PropertyValue::StringArray(_) => true,
    }
}

/// Exact identity used by planner proofs. Matches storage `eq_value`.
pub(super) fn property_values_equal(left: &PropertyValue, right: &PropertyValue) -> bool {
    match (left, right) {
        (PropertyValue::Null, PropertyValue::Null) => true,
        (PropertyValue::Bool(left), PropertyValue::Bool(right)) => left == right,
        (
            PropertyValue::I64(_) | PropertyValue::F64(_) | PropertyValue::F32(_),
            PropertyValue::I64(_) | PropertyValue::F64(_) | PropertyValue::F32(_),
        ) => canonical_number(left)
            .zip(canonical_number(right))
            .is_some_and(|(left, right)| left == right),
        (PropertyValue::DateTime(left), PropertyValue::DateTime(right)) => left == right,
        (PropertyValue::String(left), PropertyValue::String(right)) => left == right,
        (PropertyValue::Bytes(left), PropertyValue::Bytes(right)) => left == right,
        (PropertyValue::I64Array(left), PropertyValue::I64Array(right)) => left == right,
        (PropertyValue::F64Array(left), PropertyValue::F64Array(right)) => {
            left.len() == right.len()
                && left.iter().zip(right).all(|(left, right)| {
                    CanonicalNumber::from_f64(*left)
                        .zip(CanonicalNumber::from_f64(*right))
                        .is_some_and(|(left, right)| left == right)
                })
        }
        (PropertyValue::F32Array(left), PropertyValue::F32Array(right)) => {
            left.len() == right.len()
                && left.iter().zip(right).all(|(left, right)| {
                    CanonicalNumber::from_f32(*left)
                        .zip(CanonicalNumber::from_f32(*right))
                        .is_some_and(|(left, right)| left == right)
                })
        }
        (PropertyValue::StringArray(left), PropertyValue::StringArray(right)) => left == right,
        (PropertyValue::Array(left), PropertyValue::Array(right)) => left == right,
        (PropertyValue::Object(left), PropertyValue::Object(right)) => left == right,
        _ => false,
    }
}

pub(super) fn property_value_ordering(
    left: &PropertyValue,
    right: &PropertyValue,
) -> Option<Ordering> {
    match (left, right) {
        (
            PropertyValue::I64(_) | PropertyValue::F64(_) | PropertyValue::F32(_),
            PropertyValue::I64(_) | PropertyValue::F64(_) | PropertyValue::F32(_),
        ) => canonical_number(left)
            .zip(canonical_number(right))
            .map(|(left, right)| left.cmp(&right)),
        (PropertyValue::DateTime(left), PropertyValue::DateTime(right)) => Some(left.cmp(right)),
        (PropertyValue::String(left), PropertyValue::String(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn canonical_number(value: &PropertyValue) -> Option<CanonicalNumber> {
    match value {
        PropertyValue::I64(value) => Some(CanonicalNumber::from_i64(*value)),
        PropertyValue::F64(value) => CanonicalNumber::from_f64(*value),
        PropertyValue::F32(value) => CanonicalNumber::from_f32(*value),
        PropertyValue::Null
        | PropertyValue::Bool(_)
        | PropertyValue::DateTime(_)
        | PropertyValue::String(_)
        | PropertyValue::Bytes(_)
        | PropertyValue::I64Array(_)
        | PropertyValue::F64Array(_)
        | PropertyValue::F32Array(_)
        | PropertyValue::StringArray(_)
        | PropertyValue::Array(_)
        | PropertyValue::Object(_) => None,
    }
}

/// `values` without repeats under [`property_values_equal`], in first-seen
/// order, in linear time.
///
/// Repeats are found by [`property_value_identity`]. Values without an
/// identity (NaN, heterogeneous arrays, objects) all stay, which a proof over
/// the allowed values treats as one value anyway.
fn dedup_property_values(values: Vec<PropertyValue>) -> Vec<PropertyValue> {
    let mut seen = std::collections::HashSet::new();
    let first_seen = values
        .iter()
        .map(|value| property_value_identity(value).is_none_or(|identity| seen.insert(identity)))
        .collect::<Vec<_>>();
    values
        .into_iter()
        .zip(first_seen)
        .filter_map(|(value, first_seen)| first_seen.then_some(value))
        .collect()
}

/// Hashable identity of a property value under [`property_values_equal`]:
/// two values with the same identity are equal by query equality.
///
/// Numbers, alone or in a numeric array, are keyed by their
/// [`CanonicalNumber`], so `1` and `1.0` share an identity while `+inf`,
/// `-inf` and every finite value stay apart. Values whose equality is not
/// reflexive (NaN anywhere), and heterogeneous arrays and objects, whose
/// members compare structurally, have none (`None`); callers keep each such
/// value, which only repeats it.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) enum PropertyValueIdentity<'a> {
    Null,
    Bool(bool),
    Number(CanonicalNumber),
    DateTime(i64),
    String(&'a str),
    Bytes(&'a [u8]),
    I64Array(&'a [i64]),
    F64Array(Vec<CanonicalNumber>),
    F32Array(Vec<CanonicalNumber>),
    StringArray(&'a [String]),
}

/// The [`PropertyValueIdentity`] of `value`, or `None` when it has none.
pub(crate) fn property_value_identity(value: &PropertyValue) -> Option<PropertyValueIdentity<'_>> {
    Some(match value {
        PropertyValue::Null => PropertyValueIdentity::Null,
        PropertyValue::Bool(value) => PropertyValueIdentity::Bool(*value),
        PropertyValue::I64(_) | PropertyValue::F64(_) | PropertyValue::F32(_) => {
            PropertyValueIdentity::Number(canonical_number(value)?)
        }
        PropertyValue::DateTime(value) => PropertyValueIdentity::DateTime(*value),
        PropertyValue::String(value) => PropertyValueIdentity::String(value),
        PropertyValue::Bytes(value) => PropertyValueIdentity::Bytes(value),
        PropertyValue::I64Array(values) => PropertyValueIdentity::I64Array(values),
        PropertyValue::F64Array(values) => PropertyValueIdentity::F64Array(
            values
                .iter()
                .map(|value| CanonicalNumber::from_f64(*value))
                .collect::<Option<_>>()?,
        ),
        PropertyValue::F32Array(values) => PropertyValueIdentity::F32Array(
            values
                .iter()
                .map(|value| CanonicalNumber::from_f32(*value))
                .collect::<Option<_>>()?,
        ),
        PropertyValue::StringArray(values) => PropertyValueIdentity::StringArray(values),
        PropertyValue::Array(_) | PropertyValue::Object(_) => return None,
    })
}
