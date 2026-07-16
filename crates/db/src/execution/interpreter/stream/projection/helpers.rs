//! Projection object helper contracts.

use std::collections::BTreeMap;

use super::*;

pub(in crate::execution::interpreter::stream::projection) fn label_property_name(
) -> ir::NonEmptyString {
    ir::NonEmptyString::new("$label").expect("label virtual property name is non-empty")
}

pub(in crate::execution::interpreter::stream::projection) fn properties_to_object(
    properties: Vec<Property>,
) -> BTreeMap<String, DbPropertyValue> {
    properties
        .into_iter()
        .map(|property| (property.name, property.value))
        .collect()
}
