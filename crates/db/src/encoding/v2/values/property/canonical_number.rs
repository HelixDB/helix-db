//! Shared exact numeric semantics adapted to database property values.

pub(crate) use helix_value_semantics::CanonicalNumber;

use super::property_value::PropertyValue;

/// Returns `None` for non-numeric database values and for NaN.
pub(crate) fn from_property(value: &PropertyValue) -> Option<CanonicalNumber> {
    match value {
        PropertyValue::I64(value) => Some(CanonicalNumber::from_i64(*value)),
        PropertyValue::F64(value) => CanonicalNumber::from_f64(*value),
        PropertyValue::F32(value) => CanonicalNumber::from_f64(*value),
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
