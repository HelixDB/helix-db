use std::{borrow::Cow, collections::HashMap};

use crate::{
    helix_engine::types::GraphError,
    utils::{
        items::{Edge, Node},
    },
    protocol:: {
        return_values::ReturnValue,
        value::Value,
    },
};

#[derive(Debug, Clone)]
pub enum FilterableType {
    Node,
    Edge,
    Vector,
}

/// Trait for types that can be filtered based on their properties.
/// Implemented by both Node and Edge types.
pub trait Filterable {
    fn type_name(&self) -> FilterableType;

    fn id(&self) -> &u128;

    fn uuid(&self) -> String;

    fn label(&self) -> &str;

    fn from_node(&self) -> u128;

    fn from_node_uuid(&self) -> String;

    fn to_node(&self) -> u128;

    fn to_node_uuid(&self) -> String;

    fn properties(self) -> Option<HashMap<String, Value>>;

    fn vector_data(&self) -> &[f64];
    fn score(&self) -> f64;

    fn properties_mut(&mut self) -> &mut Option<HashMap<String, Value>>;

    fn properties_ref(&self) -> &Option<HashMap<String, Value>>;

    fn check_property(&self, key: &str) -> Result<Cow<'_,Value>, GraphError>;

    fn find_property<'a>(
        &'a self,
        key: &str,
        secondary_properties: &'a HashMap<String, ReturnValue>,
        property: &'a mut ReturnValue,
    ) -> Option<&'a ReturnValue>;
}

impl Filterable for Node {
    #[inline(always)]
    fn type_name(&self) -> FilterableType {
        FilterableType::Node
    }

    #[inline(always)]
    fn id(&self) -> &u128 {
        &self.id
    }

    #[inline(always)]
    fn uuid(&self) -> String {
        uuid::Uuid::from_u128(self.id).to_string()
    }

    #[inline(always)]
    fn label(&self) -> &str {
        &self.label
    }

    #[inline(always)]
    fn from_node(&self) -> u128 {
        unreachable!()
    }

    #[inline(always)]
    fn from_node_uuid(&self) -> String {
        unreachable!()
    }

    #[inline(always)]
    fn to_node(&self) -> u128 {
        unreachable!()
    }

    #[inline(always)]
    fn to_node_uuid(&self) -> String {
        unreachable!()
    }

    #[inline(always)]
    fn vector_data(&self) -> &[f64] {
        unreachable!()
    }

    #[inline(always)]
    fn score(&self) -> f64 {
        unreachable!()
    }

    #[inline(always)]
    fn properties(self) -> Option<HashMap<String, Value>> {
        self.properties
    }

    #[inline(always)]
    fn properties_ref(&self) -> &Option<HashMap<String, Value>> {
        &self.properties
    }

    #[inline(always)]
    fn properties_mut(&mut self) -> &mut Option<HashMap<String, Value>> {
        &mut self.properties
    }

    #[inline(always)]
    fn check_property(&self, key: &str) -> Result<Cow<'_,Value>, GraphError> {
        match key {
            "id" => Ok(Cow::Owned(Value::from(self.uuid()))),
            "label" => Ok(Cow::Owned(Value::from(self.label.to_string()))),
            _ =>  match &self.properties {
                Some(properties) => properties
                    .get(key)
                    .ok_or(GraphError::ConversionError(format!(
                        "Property {key} not found"
                    ))).map(Cow::Borrowed),
                None => Err(GraphError::ConversionError(format!(
                    "Property {key} not found"
                ))),
            },
        }
    }

    #[inline(always)]
    fn find_property<'a>(
        &'a self,
        key: &str,
        secondary_properties: &'a HashMap<String, ReturnValue>,
        property: &'a mut ReturnValue,
    ) -> Option<&'a ReturnValue> {
        match &self.properties {
            Some(properties) => match properties.get(key) {
                Some(value) => {
                    property.clone_from(&ReturnValue::Value(value.clone()));
                    Some(property)
                }
                None => secondary_properties.get(key),
            },
            None => secondary_properties.get(key),
        }
    }
}

impl Filterable for Edge {
    #[inline(always)]
    fn type_name(&self) -> FilterableType {
        FilterableType::Edge
    }

    #[inline(always)]
    fn id(&self) -> &u128 {
        &self.id
    }

    #[inline(always)]
    fn uuid(&self) -> String {
        uuid::Uuid::from_u128(self.id).to_string()
    }

    #[inline(always)]
    fn label(&self) -> &str {
        &self.label
    }

    #[inline(always)]
    fn from_node(&self) -> u128 {
        self.from_node
    }

    #[inline(always)]
    fn from_node_uuid(&self) -> String {
        uuid::Uuid::from_u128(self.from_node).to_string()
    }

    #[inline(always)]
    fn to_node(&self) -> u128 {
        self.to_node
    }

    #[inline(always)]
    fn to_node_uuid(&self) -> String {
        uuid::Uuid::from_u128(self.to_node).to_string()
    }

    #[inline(always)]
    fn vector_data(&self) -> &[f64] {
        unreachable!()
    }

    #[inline(always)]
    fn score(&self) -> f64 {
        unreachable!()
    }

    #[inline(always)]
    fn properties(self) -> Option<HashMap<String, Value>> {
        self.properties
    }

    #[inline(always)]
    fn properties_ref(&self) -> &Option<HashMap<String, Value>> {
        &self.properties
    }

    #[inline(always)]
    fn properties_mut(&mut self) -> &mut Option<HashMap<String, Value>> {
        &mut self.properties
    }

    #[inline(always)]
    fn check_property(&self, key: &str) -> Result<Cow<'_,Value>, GraphError> {
        match key {
            "id" => Ok(Cow::Owned(Value::from(self.uuid()))),
            "label" => Ok(Cow::Owned(Value::from(self.label.to_string()))),
            "from_node" => Ok(Cow::Owned(Value::from(self.from_node_uuid()))),
            "to_node" => Ok(Cow::Owned(Value::from(self.to_node_uuid()))),
            _ =>  match &self.properties {
                Some(properties) => properties
                    .get(key)
                    .ok_or(GraphError::ConversionError(format!(
                        "Property {key} not found"
                    ))).map(Cow::Borrowed),
                None => Err(GraphError::ConversionError(format!(
                    "Property {key} not found"
                ))),
            }
        }
    }

    #[inline(always)]
    fn find_property<'a>(
        &'a self,
        key: &str,
        secondary_properties: &'a HashMap<String, ReturnValue>,
        property: &'a mut ReturnValue,
    ) -> Option<&'a ReturnValue> {
        match &self.properties {
            Some(properties) => match properties.get(key) {
                Some(value) => {
                    property.clone_from(&ReturnValue::Value(value.clone()));
                    Some(property)
                }
                None => secondary_properties.get(key),
            },
            None => secondary_properties.get(key),
        }
    }
}
