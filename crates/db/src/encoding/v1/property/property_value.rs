//! Property value types for typed graph properties

use chrono::Utc;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::{Deserialize, Serialize, Serializer};
use std::{cmp::Ordering, collections::BTreeMap};

use crate::encoding::property::{datetime_millis_to_rfc3339, sortable_i64_index_string};

///
/// Supports primitive types (bool, i64, f64, string, bytes) and arrays.
/// Uses rkyv for efficient zero-copy serialization.
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Deserialize)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(bounds(__C: rkyv::validation::ArchiveContext)))]
pub enum PropertyValue {
    /// Null/missing value
    Null,
    /// Boolean value
    Bool(bool),
    /// 64-bit signed integer
    I64(i64),
    /// UTC datetime stored as epoch milliseconds
    DateTime(i64),
    /// 64-bit floating point
    F64(f64),
    /// 32-bit floating point
    F32(f64),
    /// UTF-8 string
    String(String),
    /// Raw bytes
    Bytes(Vec<u8>),
    /// Array of i64
    I64Array(Vec<i64>),
    /// Array of f64
    F64Array(Vec<f64>),
    /// Array of f32
    F32Array(Vec<f32>),
    /// Array of strings
    StringArray(Vec<String>),
    /// Heterogeneous array value
    Array(#[rkyv(omit_bounds)] Vec<PropertyValue>),
    /// Object/map value
    Object(#[rkyv(omit_bounds)] BTreeMap<String, PropertyValue>),
}

impl PropertyValue {
    /// Compare two property values for ordering
    ///
    /// Numeric types can be compared across i64/f64/f32.
    /// Non-comparable types return Equal.
    pub fn compare(&self, other: &PropertyValue) -> Ordering {
        match (self, other) {
            (PropertyValue::I64(a), PropertyValue::I64(b)) => a.cmp(b),
            (PropertyValue::DateTime(a), PropertyValue::DateTime(b)) => a.cmp(b),
            (PropertyValue::F64(a), PropertyValue::F64(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (PropertyValue::F32(a), PropertyValue::F32(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (PropertyValue::String(a), PropertyValue::String(b)) => a.cmp(b),
            // Cross-type numeric comparison
            (PropertyValue::I64(a), PropertyValue::F64(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (PropertyValue::F64(a), PropertyValue::I64(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            (PropertyValue::I64(a), PropertyValue::F32(b)) => {
                (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (PropertyValue::F32(a), PropertyValue::I64(b)) => {
                a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
            }
            (PropertyValue::F64(a), PropertyValue::F32(b))
            | (PropertyValue::F32(a), PropertyValue::F64(b)) => {
                a.partial_cmp(b).unwrap_or(Ordering::Equal)
            }
            (PropertyValue::Bool(a), PropertyValue::Bool(b)) => a.cmp(b),
            // Non-comparable types
            _ => Ordering::Equal,
        }
    }

    /// Check equality with another value (type-aware)
    pub fn eq_value(&self, other: &PropertyValue) -> bool {
        match (self, other) {
            (PropertyValue::Null, PropertyValue::Null) => true,
            (PropertyValue::Bool(a), PropertyValue::Bool(b)) => a == b,
            (PropertyValue::I64(a), PropertyValue::I64(b)) => a == b,
            (PropertyValue::DateTime(a), PropertyValue::DateTime(b)) => a == b,
            (PropertyValue::F64(a), PropertyValue::F64(b)) => a == b,
            (PropertyValue::F32(a), PropertyValue::F32(b)) => a == b,
            (PropertyValue::String(a), PropertyValue::String(b)) => a == b,
            (PropertyValue::Bytes(a), PropertyValue::Bytes(b)) => a == b,
            (PropertyValue::I64Array(a), PropertyValue::I64Array(b)) => a == b,
            (PropertyValue::F64Array(a), PropertyValue::F64Array(b)) => a == b,
            (PropertyValue::F32Array(a), PropertyValue::F32Array(b)) => a == b,
            (PropertyValue::StringArray(a), PropertyValue::StringArray(b)) => a == b,
            (PropertyValue::Array(a), PropertyValue::Array(b)) => a == b,
            (PropertyValue::Object(a), PropertyValue::Object(b)) => a == b,
            // Cross-type numeric equality
            (PropertyValue::I64(a), PropertyValue::F64(b)) => (*a as f64) == *b,
            (PropertyValue::F64(a), PropertyValue::I64(b)) => *a == (*b as f64),
            (PropertyValue::I64(a), PropertyValue::F32(b)) => (*a as f64) == *b,
            (PropertyValue::F32(a), PropertyValue::I64(b)) => *a == (*b as f64),
            (PropertyValue::F64(a), PropertyValue::F32(b))
            | (PropertyValue::F32(a), PropertyValue::F64(b)) => a == b,
            _ => false,
        }
    }

    /// Get as string if this is a String variant
    pub fn as_str(&self) -> Option<&str> {
        let PropertyValue::String(s) = self else {
            return None;
        };
        Some(s)
    }

    /// Get as i64 if this is an I64 variant
    pub fn as_i64(&self) -> Option<i64> {
        let PropertyValue::I64(n) = self else {
            return None;
        };
        Some(*n)
    }

    /// Get as UTC epoch milliseconds if this is a DateTime variant.
    pub fn as_datetime_millis(&self) -> Option<i64> {
        let PropertyValue::DateTime(n) = self else {
            return None;
        };
        Some(*n)
    }

    /// Get as f64 if this is an F64 variant
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            PropertyValue::F64(n) => Some(*n),
            PropertyValue::F32(n) => Some(*n),
            PropertyValue::I64(n) => Some(*n as f64),
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

    /// Get as bool if this is a Bool variant
    pub fn as_bool(&self) -> Option<bool> {
        let PropertyValue::Bool(b) = self else {
            return None;
        };
        Some(*b)
    }

    /// Convert to a string representation suitable for index keys
    ///
    /// Numbers are zero-padded for lexicographic ordering:
    /// - i64: 20-character zero-padded decimal
    /// - f64: Scientific notation with sign prefix
    /// - Strings: As-is
    /// - Other types: Debug representation (not indexable)
    pub fn to_index_string(&self) -> String {
        match self {
            PropertyValue::Null => "null".to_string(),
            PropertyValue::Bool(b) => b.to_string(),
            // was PropertyValue::I64(n) => format!("{:020}", n), check compatibility
            PropertyValue::I64(n) => sortable_i64_index_string(*n),
            PropertyValue::DateTime(n) => sortable_i64_index_string(*n),
            PropertyValue::F64(n) => format!("{:+024.15e}", n),
            PropertyValue::String(s) => s.clone(),
            PropertyValue::Bytes(b) => format!("<bytes:{}>", b.len()),
            PropertyValue::I64Array(a) => format!("<i64[{}]>", a.len()),
            PropertyValue::F64Array(a) => format!("<f64[{}]>", a.len()),
            PropertyValue::StringArray(a) => format!("<str[{}]>", a.len()),
            PropertyValue::F32(n) => format!("{:+024.15e}", n),
            PropertyValue::F32Array(items) => format!("<f32[{}]>", items.len()),
            PropertyValue::Array(a) => format!("<array[{}]>", a.len()),
            PropertyValue::Object(o) => format!("<object[{}]>", o.len()),
        }
    }
}

// From implementations for convenient construction
impl From<String> for PropertyValue {
    fn from(s: String) -> Self {
        PropertyValue::String(s)
    }
}

impl From<&str> for PropertyValue {
    fn from(s: &str) -> Self {
        PropertyValue::String(s.to_string())
    }
}

impl From<i64> for PropertyValue {
    fn from(n: i64) -> Self {
        PropertyValue::I64(n)
    }
}

impl From<chrono::DateTime<Utc>> for PropertyValue {
    fn from(value: chrono::DateTime<Utc>) -> Self {
        PropertyValue::DateTime(value.timestamp_millis())
    }
}

impl From<i32> for PropertyValue {
    fn from(n: i32) -> Self {
        PropertyValue::I64(n as i64)
    }
}

impl From<f64> for PropertyValue {
    fn from(n: f64) -> Self {
        PropertyValue::F64(n)
    }
}

impl From<bool> for PropertyValue {
    fn from(b: bool) -> Self {
        PropertyValue::Bool(b)
    }
}

impl From<Vec<u8>> for PropertyValue {
    fn from(b: Vec<u8>) -> Self {
        PropertyValue::Bytes(b)
    }
}

impl From<Vec<i64>> for PropertyValue {
    fn from(v: Vec<i64>) -> Self {
        PropertyValue::I64Array(v)
    }
}

impl From<Vec<f64>> for PropertyValue {
    fn from(v: Vec<f64>) -> Self {
        PropertyValue::F64Array(v)
    }
}

impl From<Vec<String>> for PropertyValue {
    fn from(v: Vec<String>) -> Self {
        PropertyValue::StringArray(v)
    }
}

impl From<Vec<PropertyValue>> for PropertyValue {
    fn from(v: Vec<PropertyValue>) -> Self {
        PropertyValue::Array(v)
    }
}

impl From<BTreeMap<String, PropertyValue>> for PropertyValue {
    fn from(v: BTreeMap<String, PropertyValue>) -> Self {
        PropertyValue::Object(v)
    }
}

impl From<&String> for PropertyValue {
    fn from(s: &String) -> Self {
        PropertyValue::String(s.clone())
    }
}

impl std::fmt::Display for PropertyValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PropertyValue::Null => write!(f, "null"),
            PropertyValue::Bool(b) => write!(f, "{}", b),
            PropertyValue::I64(n) => write!(f, "{}", n),
            PropertyValue::DateTime(n) => write!(
                f,
                "{}",
                datetime_millis_to_rfc3339(*n).unwrap_or_else(|| n.to_string())
            ),
            PropertyValue::F64(n) => write!(f, "{}", n),
            PropertyValue::String(s) => write!(f, "{}", s),
            PropertyValue::Bytes(b) => write!(f, "<bytes:{}>", b.len()),
            PropertyValue::I64Array(a) => write!(f, "<i64[{}]>", a.len()),
            PropertyValue::F64Array(a) => write!(f, "<f64[{}]>", a.len()),
            PropertyValue::StringArray(a) => write!(f, "<str[{}]>", a.len()),
            PropertyValue::F32(n) => write!(f, "{}", n),
            PropertyValue::F32Array(items) => write!(f, "<f32[{}]>", items.len()),
            PropertyValue::Array(a) => write!(f, "<array[{}]>", a.len()),
            PropertyValue::Object(o) => write!(f, "<object[{}]>", o.len()),
        }
    }
}

impl Serialize for PropertyValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            PropertyValue::Null => serializer.serialize_unit(),
            PropertyValue::Bool(value) => serializer.serialize_bool(*value),
            PropertyValue::I64(value) => serializer.serialize_i64(*value),
            PropertyValue::DateTime(value) => {
                serializer.serialize_str(&datetime_millis_to_rfc3339(*value).ok_or_else(|| {
                    serde::ser::Error::custom(format!(
                        "datetime millis '{}' cannot be rendered as RFC3339",
                        value
                    ))
                })?)
            }
            PropertyValue::F64(value) => serializer.serialize_f64(*value),
            PropertyValue::F32(value) => serializer.serialize_f64(*value),
            PropertyValue::String(value) => serializer.serialize_str(value),
            PropertyValue::Bytes(value) => serializer.collect_seq(value),
            PropertyValue::I64Array(values) => serializer.collect_seq(values),
            PropertyValue::F64Array(values) => serializer.collect_seq(values),
            PropertyValue::F32Array(values) => serializer.collect_seq(values),
            PropertyValue::StringArray(values) => serializer.collect_seq(values),
            PropertyValue::Array(values) => serde::Serialize::serialize(values, serializer),
            PropertyValue::Object(values) => serde::Serialize::serialize(values, serializer),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn i64_index_strings_preserve_numeric_order() {
        let values = [i64::MIN, -2, -1, 0, 1, 2, i64::MAX];
        let encoded = values
            .iter()
            .map(|value| PropertyValue::I64(*value).to_index_string())
            .collect::<Vec<_>>();

        assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn property_value_accessors_and_equality_work_for_common_types() {
        assert_eq!(PropertyValue::from("hello").as_str(), Some("hello"));
        assert_eq!(PropertyValue::from(42i64).as_i64(), Some(42));
        assert_eq!(PropertyValue::from(42i64).as_f64(), Some(42.0));
        assert_eq!(PropertyValue::F32(1.25).as_f64(), Some(1.25));
        assert_eq!(PropertyValue::from(true).as_bool(), Some(true));
        assert!(PropertyValue::from(42i64).eq_value(&PropertyValue::from(42.0)));
        assert!(PropertyValue::F32(42.0).eq_value(&PropertyValue::from(42i64)));
        assert_eq!(
            PropertyValue::from(41i64).compare(&PropertyValue::from(42i64)),
            Ordering::Less
        );
    }

    #[test]
    fn display_formats_datetime_when_possible() {
        assert_eq!(
            PropertyValue::DateTime(0).to_string(),
            "1970-01-01T00:00:00.000Z"
        );
    }

    #[test]
    fn compare_covers_ordered_and_fallback_variants() {
        assert_eq!(
            PropertyValue::DateTime(1).compare(&PropertyValue::DateTime(2)),
            Ordering::Less
        );
        assert_eq!(
            PropertyValue::F64(2.0).compare(&PropertyValue::F64(1.0)),
            Ordering::Greater
        );
        assert_eq!(
            PropertyValue::F32(1.0).compare(&PropertyValue::F32(1.0)),
            Ordering::Equal
        );
        assert_eq!(
            PropertyValue::F64(f64::NAN).compare(&PropertyValue::F64(1.0)),
            Ordering::Equal
        );
        assert_eq!(
            PropertyValue::String("a".to_string()).compare(&PropertyValue::String("b".to_string())),
            Ordering::Less
        );
        assert_eq!(
            PropertyValue::Bool(false).compare(&PropertyValue::Bool(true)),
            Ordering::Less
        );
        assert_eq!(
            PropertyValue::F64(2.5).compare(&PropertyValue::F32(3.5)),
            Ordering::Less
        );
        assert_eq!(
            PropertyValue::F32(3.5).compare(&PropertyValue::F64(2.5)),
            Ordering::Greater
        );
        assert_eq!(
            PropertyValue::I64(2).compare(&PropertyValue::F64(2.5)),
            Ordering::Less
        );
        assert_eq!(
            PropertyValue::F64(2.5).compare(&PropertyValue::I64(2)),
            Ordering::Greater
        );
        assert_eq!(
            PropertyValue::I64(2).compare(&PropertyValue::F32(2.5)),
            Ordering::Less
        );
        assert_eq!(
            PropertyValue::F32(2.5).compare(&PropertyValue::I64(2)),
            Ordering::Greater
        );
        assert_eq!(
            PropertyValue::Bytes(vec![1]).compare(&PropertyValue::Bytes(vec![2])),
            Ordering::Equal
        );
    }

    #[test]
    fn eq_value_covers_all_variants_and_numeric_cross_types() {
        let mut object = BTreeMap::new();
        object.insert("nested".to_string(), PropertyValue::Bool(true));

        let values = vec![
            PropertyValue::Null,
            PropertyValue::Bool(true),
            PropertyValue::I64(7),
            PropertyValue::DateTime(8),
            PropertyValue::F64(1.5),
            PropertyValue::F32(1.5),
            PropertyValue::String("x".to_string()),
            PropertyValue::Bytes(vec![1, 2]),
            PropertyValue::I64Array(vec![1, 2]),
            PropertyValue::F64Array(vec![1.0, 2.0]),
            PropertyValue::F32Array(vec![1.0, 2.0]),
            PropertyValue::StringArray(vec!["a".to_string()]),
            PropertyValue::Array(vec![PropertyValue::I64(1)]),
            PropertyValue::Object(object),
        ];

        for value in values {
            assert!(value.eq_value(&value));
        }

        assert!(PropertyValue::I64(2).eq_value(&PropertyValue::F32(2.0)));
        assert!(PropertyValue::I64(2).eq_value(&PropertyValue::F64(2.0)));
        assert!(PropertyValue::F64(2.0).eq_value(&PropertyValue::I64(2)));
        assert!(PropertyValue::F32(2.0).eq_value(&PropertyValue::I64(2)));
        assert!(PropertyValue::F32(2.0).eq_value(&PropertyValue::F64(2.0)));
        assert!(PropertyValue::F64(2.0).eq_value(&PropertyValue::F32(2.0)));
        assert!(!PropertyValue::F64(2.1).eq_value(&PropertyValue::I64(2)));
        assert!(!PropertyValue::String("2".to_string()).eq_value(&PropertyValue::I64(2)));
    }

    #[test]
    fn accessors_return_none_for_wrong_variants() {
        assert_eq!(PropertyValue::Null.as_str(), None);
        assert_eq!(PropertyValue::Null.as_i64(), None);
        assert_eq!(PropertyValue::DateTime(123).as_datetime_millis(), Some(123));
        assert_eq!(PropertyValue::Null.as_datetime_millis(), None);
        assert_eq!(PropertyValue::F64(2.5).as_f64(), Some(2.5));
        assert_eq!(PropertyValue::String("x".to_string()).as_f64(), None);
        assert_eq!(PropertyValue::I64(1).as_bool(), None);
    }

    #[test]
    fn index_string_and_display_cover_all_variants() {
        let mut object = BTreeMap::new();
        object.insert("nested".to_string(), PropertyValue::Bool(true));

        let cases = [
            (PropertyValue::Null, "null", "null"),
            (PropertyValue::Bool(true), "true", "true"),
            (PropertyValue::I64(1), "09223372036854775809", "1"),
            (
                PropertyValue::DateTime(0),
                "09223372036854775808",
                "1970-01-01T00:00:00.000Z",
            ),
            (PropertyValue::F64(1.5), "+00001.500000000000000e0", "1.5"),
            (PropertyValue::F32(2.5), "+00002.500000000000000e0", "2.5"),
            (PropertyValue::String("value".to_string()), "value", "value"),
            (PropertyValue::Bytes(vec![1, 2]), "<bytes:2>", "<bytes:2>"),
            (PropertyValue::I64Array(vec![1, 2]), "<i64[2]>", "<i64[2]>"),
            (PropertyValue::F64Array(vec![1.0]), "<f64[1]>", "<f64[1]>"),
            (PropertyValue::F32Array(vec![1.0]), "<f32[1]>", "<f32[1]>"),
            (
                PropertyValue::StringArray(vec!["a".to_string()]),
                "<str[1]>",
                "<str[1]>",
            ),
            (
                PropertyValue::Array(vec![PropertyValue::I64(1)]),
                "<array[1]>",
                "<array[1]>",
            ),
            (PropertyValue::Object(object), "<object[1]>", "<object[1]>"),
        ];

        for (value, index, display) in cases {
            assert_eq!(value.to_index_string(), index);
            assert_eq!(value.to_string(), display);
        }
        assert_eq!(
            PropertyValue::DateTime(i64::MAX).to_string(),
            i64::MAX.to_string()
        );
    }

    #[test]
    fn serde_serializes_every_variant_and_rejects_unrenderable_datetime() {
        let mut object = BTreeMap::new();
        object.insert("nested".to_string(), PropertyValue::Bool(true));

        let values = [
            PropertyValue::Null,
            PropertyValue::Bool(true),
            PropertyValue::I64(1),
            PropertyValue::DateTime(0),
            PropertyValue::F64(1.5),
            PropertyValue::F32(2.5),
            PropertyValue::String("value".to_string()),
            PropertyValue::Bytes(vec![1, 2]),
            PropertyValue::I64Array(vec![1, 2]),
            PropertyValue::F64Array(vec![1.0]),
            PropertyValue::F32Array(vec![1.0]),
            PropertyValue::StringArray(vec!["a".to_string()]),
            PropertyValue::Array(vec![PropertyValue::I64(1)]),
            PropertyValue::Object(object),
        ];

        for value in values {
            serde_json::to_value(value).unwrap();
        }
        assert!(serde_json::to_value(PropertyValue::DateTime(i64::MAX)).is_err());
    }

    #[test]
    fn from_impls_cover_owned_borrowed_and_collection_inputs() {
        let owned = "owned".to_string();
        let mut object = BTreeMap::new();
        object.insert("key".to_string(), PropertyValue::Bool(true));
        let timestamp = chrono::DateTime::<Utc>::from_timestamp_millis(1_234).unwrap();

        assert_eq!(
            PropertyValue::from(owned.clone()),
            PropertyValue::String(owned.clone())
        );
        assert_eq!(PropertyValue::from(&owned), PropertyValue::String(owned));
        assert_eq!(
            PropertyValue::from("borrowed"),
            PropertyValue::String("borrowed".into())
        );
        assert_eq!(PropertyValue::from(1_i64), PropertyValue::I64(1));
        assert_eq!(PropertyValue::from(1_i32), PropertyValue::I64(1));
        assert_eq!(
            PropertyValue::from(timestamp),
            PropertyValue::DateTime(1_234)
        );
        assert_eq!(PropertyValue::from(1.5_f64), PropertyValue::F64(1.5));
        assert_eq!(PropertyValue::from(true), PropertyValue::Bool(true));
        assert_eq!(
            PropertyValue::from(vec![1_u8, 2]),
            PropertyValue::Bytes(vec![1, 2])
        );
        assert_eq!(
            PropertyValue::from(vec![1_i64, 2]),
            PropertyValue::I64Array(vec![1, 2])
        );
        assert_eq!(
            PropertyValue::from(vec![1.0_f64]),
            PropertyValue::F64Array(vec![1.0])
        );
        assert_eq!(
            PropertyValue::from(vec!["a".to_string()]),
            PropertyValue::StringArray(vec!["a".to_string()])
        );
        assert_eq!(
            PropertyValue::from(vec![PropertyValue::I64(1)]),
            PropertyValue::Array(vec![PropertyValue::I64(1)])
        );
        assert_eq!(
            PropertyValue::from(object.clone()),
            PropertyValue::Object(object)
        );
    }
}
