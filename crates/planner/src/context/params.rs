use std::collections::BTreeMap;

use helix_ast::query::QueryValue;
use helix_ast::value::PropertyValue;
use serde::{Deserialize, Serialize};

use crate::ir;

/// Runtime parameter bindings.
///
/// Parameter names are [`ir::NonEmptyString`] values, so empty runtime parameter
/// names cannot be inserted through this builder API.
///
/// # Examples
///
/// ```
/// use helix_planner::context::ParamBindings;
/// use helix_planner::ir::NonEmptyString;
///
/// let name = NonEmptyString::new("limit").unwrap();
/// let params = ParamBindings::default().with_value(name.clone(), 10);
///
/// assert_eq!(params.values[&name].as_i64(), Some(10));
/// ```
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ParamBindings {
    /// Property-compatible parameters.
    pub values: BTreeMap<ir::NonEmptyString, PropertyValue>,
    /// JSON-compatible query parameters retained for executor use.
    pub query_values: BTreeMap<ir::NonEmptyString, QueryValue>,
}

impl ParamBindings {
    /// Insert a property-compatible parameter.
    pub fn with_value(mut self, name: ir::NonEmptyString, value: impl Into<PropertyValue>) -> Self {
        self.values.insert(name, value.into());
        self
    }

    /// Insert a JSON-compatible query parameter.
    pub fn with_query_value(mut self, name: ir::NonEmptyString, value: QueryValue) -> Self {
        self.query_values.insert(name, value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builders_keep_property_and_query_values_separate() {
        let property_name = ir::NonEmptyString::new("limit").unwrap();
        let query_name = ir::NonEmptyString::new("payload").unwrap();
        let query_value = QueryValue::String("raw".to_string());

        let params = ParamBindings::default()
            .with_value(property_name.clone(), 10)
            .with_query_value(query_name.clone(), query_value.clone());

        assert_eq!(
            params
                .values
                .get(&property_name)
                .and_then(PropertyValue::as_i64),
            Some(10)
        );
        assert_eq!(params.query_values.get(&query_name), Some(&query_value));
        assert!(!params.values.contains_key(&query_name));
        assert!(!params.query_values.contains_key(&property_name));
    }
}
