//! Property-value comparison and finite literal-collection contracts.

use std::cmp::Ordering;

use helix_ast::value::PropertyValue;

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

pub(super) fn property_value_ordering(
    left: &PropertyValue,
    right: &PropertyValue,
) -> Option<Ordering> {
    match (left, right) {
        (PropertyValue::I64(left), PropertyValue::I64(right))
        | (PropertyValue::DateTime(left), PropertyValue::DateTime(right)) => Some(left.cmp(right)),
        (PropertyValue::F64(left), PropertyValue::F64(right)) => left.partial_cmp(right),
        (PropertyValue::F32(left), PropertyValue::F32(right)) => left.partial_cmp(right),
        (PropertyValue::String(left), PropertyValue::String(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn dedup_property_values(values: Vec<PropertyValue>) -> Vec<PropertyValue> {
    values.into_iter().fold(Vec::new(), |mut unique, value| {
        if !unique.contains(&value) {
            unique.push(value);
        }
        unique
    })
}
