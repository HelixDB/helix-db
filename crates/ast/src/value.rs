use std::collections::{BTreeMap, HashMap};

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::expr::Expr;
/// Arbitrary nested parameter value.
pub type ParamValue = PropertyValue;

/// Object-shaped parameter payload.
pub type ParamObject = BTreeMap<String, PropertyValue>;
/// A property value that can be stored on nodes or edges.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PropertyValue {
    /// Null value.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    I64(i64),
    /// UTC datetime stored as epoch milliseconds.
    DateTime(i64),
    /// 64-bit floating point.
    F64(f64),
    /// 32-bit floating point.
    F32(f32),
    /// UTF-8 string.
    String(String),
    /// Raw bytes.
    Bytes(Vec<u8>),
    /// Array of i64 values.
    I64Array(Vec<i64>),
    /// Array of f64 values.
    F64Array(Vec<f64>),
    /// Array of f32 values.
    F32Array(Vec<f32>),
    /// Array of strings.
    StringArray(Vec<String>),
    /// Heterogeneous array.
    Array(Vec<PropertyValue>),
    /// Object/map value.
    Object(BTreeMap<String, PropertyValue>),
}

impl PropertyValue {
    /// Create a heterogeneous array value.
    pub fn array<V>(values: impl IntoIterator<Item = V>) -> Self
    where
        V: Into<PropertyValue>,
    {
        Self::Array(values.into_iter().map(Into::into).collect())
    }

    /// Create an object/map value.
    pub fn object<K, V>(values: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: Into<PropertyValue>,
    {
        Self::Object(
            values
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        )
    }

    /// Get value as string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    /// Get value as i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::I64(value) => Some(*value),
            _ => None,
        }
    }

    /// Create a datetime value from UTC epoch milliseconds.
    pub fn datetime_millis(millis: i64) -> Self {
        Self::DateTime(millis)
    }

    /// Get datetime as UTC epoch milliseconds.
    pub fn as_datetime_millis(&self) -> Option<i64> {
        match self {
            Self::DateTime(value) => Some(*value),
            _ => None,
        }
    }

    /// Get value as f64.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::F64(value) => Some(*value),
            Self::F32(value) => Some(*value as f64),
            _ => None,
        }
    }

    /// Get value as bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    /// Get value as array reference.
    pub fn as_array(&self) -> Option<&[PropertyValue]> {
        match self {
            Self::Array(values) => Some(values),
            _ => None,
        }
    }

    /// Get value as object reference.
    pub fn as_object(&self) -> Option<&BTreeMap<String, PropertyValue>> {
        match self {
            Self::Object(values) => Some(values),
            _ => None,
        }
    }
}

impl From<&str> for PropertyValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<String> for PropertyValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<i64> for PropertyValue {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<i32> for PropertyValue {
    fn from(value: i32) -> Self {
        Self::I64(value as i64)
    }
}

impl From<f64> for PropertyValue {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

impl From<f32> for PropertyValue {
    fn from(value: f32) -> Self {
        Self::F32(value)
    }
}

impl From<bool> for PropertyValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<Vec<u8>> for PropertyValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

impl From<Vec<i64>> for PropertyValue {
    fn from(value: Vec<i64>) -> Self {
        Self::I64Array(value)
    }
}

impl From<Vec<f64>> for PropertyValue {
    fn from(value: Vec<f64>) -> Self {
        Self::F64Array(value)
    }
}

impl From<Vec<f32>> for PropertyValue {
    fn from(value: Vec<f32>) -> Self {
        Self::F32Array(value)
    }
}

impl From<Vec<String>> for PropertyValue {
    fn from(value: Vec<String>) -> Self {
        Self::StringArray(value)
    }
}

impl From<Vec<PropertyValue>> for PropertyValue {
    fn from(value: Vec<PropertyValue>) -> Self {
        Self::Array(value)
    }
}

impl From<BTreeMap<String, PropertyValue>> for PropertyValue {
    fn from(value: BTreeMap<String, PropertyValue>) -> Self {
        Self::Object(value)
    }
}

impl From<HashMap<String, PropertyValue>> for PropertyValue {
    fn from(value: HashMap<String, PropertyValue>) -> Self {
        Self::Object(value.into_iter().collect())
    }
}

/// UTC datetime represented as epoch milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateTime(i64);

impl DateTime {
    /// Create from UTC epoch milliseconds.
    pub fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// Parse an RFC3339 datetime string and normalize it to UTC.
    pub fn parse_rfc3339(input: &str) -> Result<Self, chrono::ParseError> {
        Ok(Self(
            chrono::DateTime::parse_from_rfc3339(input)?
                .with_timezone(&Utc)
                .timestamp_millis(),
        ))
    }

    /// Return UTC epoch milliseconds.
    pub fn millis(self) -> i64 {
        self.0
    }

    /// Format as canonical RFC3339 UTC.
    pub fn to_rfc3339(self) -> Option<String> {
        chrono::DateTime::<Utc>::from_timestamp_millis(self.0)
            .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true))
    }
}

impl From<DateTime> for PropertyValue {
    fn from(value: DateTime) -> Self {
        Self::DateTime(value.millis())
    }
}
/// Mutation input value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PropertyInput {
    /// Literal value.
    Value(PropertyValue),
    /// Runtime expression.
    Expr(Expr),
}

impl PropertyInput {
    /// Create an input from a runtime parameter.
    pub fn param(name: impl Into<String>) -> Self {
        Self::Expr(Expr::param(name))
    }

    /// Convert to expression.
    pub fn into_expr(self) -> Expr {
        match self {
            Self::Value(value) => Expr::Constant(value),
            Self::Expr(expr) => expr,
        }
    }
}

impl<T> From<T> for PropertyInput
where
    PropertyValue: From<T>,
{
    fn from(value: T) -> Self {
        Self::Value(value.into())
    }
}

impl From<Expr> for PropertyInput {
    fn from(value: Expr) -> Self {
        Self::Expr(value)
    }
}
/// Helper type alias for property maps.
pub type PropertyMap = HashMap<String, PropertyValue>;
