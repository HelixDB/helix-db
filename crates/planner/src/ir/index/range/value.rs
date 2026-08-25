use std::cmp::Ordering;

use helix_ast::value::PropertyValue;
use helix_value_semantics::CanonicalNumber;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ir::NonEmptyString;

/// Range-index lookup value.
///
/// Range indexes can only be bounded by ordered scalar literals or runtime
/// parameters whose values are checked later. Equality indexes use
/// [`crate::ir::IndexValue`], whose literal arm excludes nested array/object
/// values that secondary index storage rejects.
///
/// ```
/// use helix_ast::value::PropertyValue;
/// use helix_planner::ir::RangeIndexValue;
///
/// assert!(RangeIndexValue::literal(PropertyValue::from(18)).is_some());
/// assert!(RangeIndexValue::literal(PropertyValue::from(true)).is_none());
/// assert!(RangeIndexValue::param("limit").is_some());
/// assert!(RangeIndexValue::param("").is_none());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeIndexValue {
    /// Ordered scalar literal.
    Literal(RangeIndexLiteral),
    /// Runtime parameter value.
    Param(NonEmptyString),
}

impl RangeIndexValue {
    /// Build a literal range value when the property value is orderable.
    pub fn literal(value: PropertyValue) -> Option<Self> {
        RangeIndexLiteral::try_from_property_value(value).map(Self::Literal)
    }

    /// Build a parameter range value.
    pub fn param(name: impl Into<String>) -> Option<Self> {
        NonEmptyString::new(name).map(Self::Param)
    }
}

/// Ordered scalar literal allowed in a range index bound.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeIndexLiteral {
    /// 64-bit signed integer.
    I64(i64),
    /// UTC datetime stored as epoch milliseconds.
    DateTime(i64),
    /// Non-NaN 64-bit floating point.
    F64(RangeIndexF64),
    /// Non-NaN 32-bit floating point.
    F32(RangeIndexF32),
    /// UTF-8 string.
    String(String),
}

/// Non-NaN `f64` value that can participate in range-index ordering.
///
/// ```
/// use helix_planner::ir::RangeIndexF64;
///
/// assert_eq!(serde_json::to_string(&RangeIndexF64::new(12.5).unwrap()).unwrap(), "12.5");
/// assert!(RangeIndexF64::new(f64::NAN).is_none());
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RangeIndexF64 {
    value: f64,
}

impl RangeIndexF64 {
    /// Build a range-index float, returning `None` for `NaN`.
    pub fn new(value: f64) -> Option<Self> {
        (!value.is_nan()).then_some(Self { value })
    }

    /// Return the checked `f64` value.
    ///
    /// ```
    /// use helix_planner::ir::RangeIndexF64;
    ///
    /// assert_eq!(RangeIndexF64::new(1.5).unwrap().get(), 1.5);
    /// ```
    pub fn get(self) -> f64 {
        self.value
    }
}

impl Serialize for RangeIndexF64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(self.value)
    }
}

impl<'de> Deserialize<'de> for RangeIndexF64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        Self::new(value).ok_or_else(|| D::Error::custom("expected non-NaN f64"))
    }
}

/// Non-NaN `f32` value that can participate in range-index ordering.
///
/// ```
/// use helix_planner::ir::RangeIndexF32;
///
/// assert_eq!(serde_json::to_string(&RangeIndexF32::new(7.25).unwrap()).unwrap(), "7.25");
/// assert!(RangeIndexF32::new(f32::NAN).is_none());
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RangeIndexF32 {
    value: f32,
}

impl RangeIndexF32 {
    /// Build a range-index float, returning `None` for `NaN`.
    pub fn new(value: f32) -> Option<Self> {
        (!value.is_nan()).then_some(Self { value })
    }

    /// Return the checked `f32` value.
    ///
    /// ```
    /// use helix_planner::ir::RangeIndexF32;
    ///
    /// assert_eq!(RangeIndexF32::new(1.5).unwrap().get(), 1.5);
    /// ```
    pub fn get(self) -> f32 {
        self.value
    }
}

impl Serialize for RangeIndexF32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f32(self.value)
    }
}

impl<'de> Deserialize<'de> for RangeIndexF32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = f32::deserialize(deserializer)?;
        Self::new(value).ok_or_else(|| D::Error::custom("expected non-NaN f32"))
    }
}

impl RangeIndexLiteral {
    /// Convert the range literal back into a property value.
    ///
    /// ```
    /// use helix_ast::value::PropertyValue;
    /// use helix_planner::ir::RangeIndexLiteral;
    ///
    /// let literal = RangeIndexLiteral::try_from_property_value(PropertyValue::from(7)).unwrap();
    /// assert_eq!(literal.to_property_value().as_i64(), Some(7));
    /// ```
    pub fn to_property_value(&self) -> PropertyValue {
        match self {
            Self::I64(value) => PropertyValue::I64(*value),
            Self::DateTime(value) => PropertyValue::DateTime(*value),
            Self::F64(value) => PropertyValue::F64(value.get()),
            Self::F32(value) => PropertyValue::F32(value.get()),
            Self::String(value) => PropertyValue::String(value.clone()),
        }
    }

    /// Convert a property value into an orderable range literal.
    pub fn try_from_property_value(value: PropertyValue) -> Option<Self> {
        match value {
            PropertyValue::I64(value) => Some(Self::I64(value)),
            PropertyValue::DateTime(value) => Some(Self::DateTime(value)),
            PropertyValue::F64(value) => RangeIndexF64::new(value).map(Self::F64),
            PropertyValue::F32(value) => RangeIndexF32::new(value).map(Self::F32),
            PropertyValue::String(value) => Some(Self::String(value)),
            PropertyValue::Null
            | PropertyValue::Bool(_)
            | PropertyValue::Bytes(_)
            | PropertyValue::I64Array(_)
            | PropertyValue::F64Array(_)
            | PropertyValue::F32Array(_)
            | PropertyValue::StringArray(_)
            | PropertyValue::Array(_)
            | PropertyValue::Object(_) => None,
        }
    }

    pub(super) fn partial_cmp_same_type(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (
                Self::I64(_) | Self::F64(_) | Self::F32(_),
                Self::I64(_) | Self::F64(_) | Self::F32(_),
            ) => {
                let left = match self {
                    Self::I64(value) => CanonicalNumber::from_i64(*value),
                    Self::F64(value) => {
                        CanonicalNumber::from_f64(value.get()).expect("range f64 excludes NaN")
                    }
                    Self::F32(value) => {
                        CanonicalNumber::from_f32(value.get()).expect("range f32 excludes NaN")
                    }
                    Self::DateTime(_) | Self::String(_) => {
                        unreachable!("numeric range arm contains only numbers")
                    }
                };
                let right = match other {
                    Self::I64(value) => CanonicalNumber::from_i64(*value),
                    Self::F64(value) => {
                        CanonicalNumber::from_f64(value.get()).expect("range f64 excludes NaN")
                    }
                    Self::F32(value) => {
                        CanonicalNumber::from_f32(value.get()).expect("range f32 excludes NaN")
                    }
                    Self::DateTime(_) | Self::String(_) => {
                        unreachable!("numeric range arm contains only numbers")
                    }
                };
                Some(left.cmp(&right))
            }
            (Self::DateTime(left), Self::DateTime(right)) => Some(left.cmp(right)),
            (Self::String(left), Self::String(right)) => Some(left.cmp(right)),
            (
                Self::I64(_) | Self::DateTime(_) | Self::F64(_) | Self::F32(_) | Self::String(_),
                _,
            ) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_cross_numeric_comparison_preserves_integer_boundaries() {
        let exact_integer = RangeIndexLiteral::I64(9_007_199_254_740_992);
        let next_integer = RangeIndexLiteral::I64(9_007_199_254_740_993);
        let rounded_float = RangeIndexLiteral::F64(
            RangeIndexF64::new(9_007_199_254_740_992.0).expect("finite test value"),
        );

        assert_eq!(
            exact_integer.partial_cmp_same_type(&rounded_float),
            Some(Ordering::Equal)
        );
        assert_eq!(
            next_integer.partial_cmp_same_type(&rounded_float),
            Some(Ordering::Greater)
        );
    }

    #[test]
    fn signed_zero_is_equal_across_float_widths_but_datetime_is_not_numeric() {
        let negative_zero =
            RangeIndexLiteral::F32(RangeIndexF32::new(-0.0).expect("zero is not NaN"));
        let positive_zero =
            RangeIndexLiteral::F64(RangeIndexF64::new(0.0).expect("zero is not NaN"));

        assert_eq!(
            negative_zero.partial_cmp_same_type(&positive_zero),
            Some(Ordering::Equal)
        );
        assert_eq!(
            RangeIndexLiteral::DateTime(0).partial_cmp_same_type(&RangeIndexLiteral::I64(0)),
            None
        );
    }
}
