use super::super::*;

pub(super) fn label_value(properties: &[Property]) -> Option<&str> {
    property_value(properties, "$label").and_then(DbPropertyValue::as_str)
}

pub(super) fn property_value<'a>(
    properties: &'a [Property],
    name: &str,
) -> Option<&'a DbPropertyValue> {
    properties
        .iter()
        .find(|property| property.name == name)
        .map(|property| &property.value)
}
