//! Persisted graph property representation shared by nodes and edges.

use crate::encoding::property::property_value::PropertyValue;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize};

/// Property attached to a node or edge
///
/// Properties are key-value pairs with string keys and typed values.
#[derive(
    Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Serialize, Deserialize,
)]
pub struct Property {
    /// Property name/key
    pub name: String,
    /// Property value (typed)
    pub value: PropertyValue,
}

impl Property {
    /// Returns whether the name and value have exactly the same persisted
    /// representation.
    pub(crate) fn same_v1_representation(&self, other: &Self) -> bool {
        self.name == other.name && self.value.same_v1_representation(&other.value)
    }

    /// Create a new property with a typed value
    #[inline]
    pub fn new(name: impl Into<String>, value: impl Into<PropertyValue>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }

    /// Create a string property
    #[inline]
    pub fn string(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, PropertyValue::String(value.into()))
    }

    /// Create an i64 property
    #[inline]
    pub fn i64(name: impl Into<String>, value: i64) -> Self {
        Self::new(name, PropertyValue::I64(value))
    }

    /// Create a datetime property from UTC epoch milliseconds.
    #[inline]
    pub fn datetime_millis(name: impl Into<String>, value: i64) -> Self {
        Self::new(name, PropertyValue::DateTime(value))
    }

    /// Create an f64 property
    #[inline]
    pub fn f64(name: impl Into<String>, value: f64) -> Self {
        Self::new(name, PropertyValue::F64(value))
    }

    /// Create a bool property
    #[inline]
    pub fn bool(name: impl Into<String>, value: bool) -> Self {
        Self::new(name, PropertyValue::Bool(value))
    }

    /// Create a bytes property
    #[inline]
    pub fn bytes(name: impl Into<String>, value: Vec<u8>) -> Self {
        Self::new(name, PropertyValue::Bytes(value))
    }

    /// Create an i64 array property
    #[inline]
    pub fn i64_array(name: impl Into<String>, value: Vec<i64>) -> Self {
        Self::new(name, PropertyValue::I64Array(value))
    }

    /// Create an f64 array property
    #[inline]
    pub fn f64_array(name: impl Into<String>, value: Vec<f64>) -> Self {
        Self::new(name, PropertyValue::F64Array(value))
    }

    /// Create an f32 array property.
    ///
    /// ```
    /// use db::encoding::property::Property;
    ///
    /// let embedding = Property::f32_array("embedding", vec![1.0, 2.0]);
    /// assert_eq!(embedding.name, "embedding");
    /// ```
    #[inline]
    pub fn f32_array(name: impl Into<String>, value: Vec<f32>) -> Self {
        Self::new(name, PropertyValue::F32Array(value))
    }

    /// Create a string array property
    #[inline]
    pub fn string_array(name: impl Into<String>, value: Vec<String>) -> Self {
        Self::new(name, PropertyValue::StringArray(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_create_expected_property_values() {
        assert_eq!(
            Property::string("name", "value").value,
            PropertyValue::String("value".into())
        );
        assert_eq!(Property::i64("age", 42).value, PropertyValue::I64(42));
        assert_eq!(
            Property::datetime_millis("created", 1_000).value,
            PropertyValue::DateTime(1_000)
        );
        assert_eq!(Property::f64("score", 1.5).value, PropertyValue::F64(1.5));
        assert_eq!(
            Property::bool("active", true).value,
            PropertyValue::Bool(true)
        );
        assert_eq!(
            Property::bytes("blob", vec![1, 2]).value,
            PropertyValue::Bytes(vec![1, 2])
        );
        assert_eq!(
            Property::i64_array("items", vec![1, 2]).value,
            PropertyValue::I64Array(vec![1, 2])
        );
        assert_eq!(
            Property::f64_array("items", vec![1.0, 2.0]).value,
            PropertyValue::F64Array(vec![1.0, 2.0])
        );
        assert_eq!(
            Property::f32_array("items", vec![1.0, 2.0]).value,
            PropertyValue::F32Array(vec![1.0, 2.0])
        );
        assert_eq!(
            Property::string_array("items", vec!["a".to_string()]).value,
            PropertyValue::StringArray(vec!["a".to_string()])
        );
        assert!(Property::f64("score", f64::NAN)
            .same_v1_representation(&Property::f64("score", f64::NAN)));
        assert!(!Property::f64("score", -0.0).same_v1_representation(&Property::f64("score", 0.0)));
        assert!(!Property::f64("score", 0.0).same_v1_representation(&Property::f64("other", 0.0)));
    }
}
