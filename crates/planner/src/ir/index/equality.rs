use helix_ast::value::PropertyValue;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use crate::ir::NonEmptyString;

/// Invalid literal payload for a secondary index lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecondaryIndexLiteralError {
    /// Secondary indexes do not store nested array/object values.
    NestedValue,
}

/// Storage behavior proven for an equality-index lookup value.
///
/// This classification deliberately contains no physical key information.
/// The database remains responsible for encoding an indexed value and for
/// resolving authoritative null and runtime-dependent behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EqualityIndexValueSemantics {
    /// Value has one canonical secondary-equality encoding.
    Indexed,
    /// Null is served by an authoritative graph scan because it is not stored.
    AuthoritativeNull,
    /// Equality is non-reflexive and is therefore statically empty.
    NonReflexive,
    /// Runtime parameter must be classified after binding.
    RuntimeDependent,
}

/// Storage behavior proven for a validated literal equality value.
///
/// Unlike [`EqualityIndexValueSemantics`], this type cannot represent runtime
/// dispatch: a [`SecondaryIndexLiteral`] has already ruled parameters out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiteralEqualityIndexValueSemantics {
    /// Value has one canonical secondary-equality encoding.
    Indexed,
    /// Null is served by an authoritative graph scan because it is not stored.
    AuthoritativeNull,
    /// Equality is non-reflexive and is therefore statically empty.
    NonReflexive,
}

/// Literal value that can be looked up in a secondary equality index.
///
/// Secondary equality indexes share the storage-side value contract used by
/// secondary indexes. Nested heterogeneous arrays and objects are rejected.
/// Null is resolved through an authoritative scan, while `"null"` is an
/// ordinary typed string.
///
/// ```
/// use helix_ast::value::PropertyValue;
/// use helix_planner::ir::{SecondaryIndexLiteral, SecondaryIndexLiteralError};
///
/// let value = SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap();
/// assert_eq!(
///     serde_json::to_string(&value).unwrap(),
///     r#"{"string":"alice"}"#
/// );
/// assert_eq!(
///     SecondaryIndexLiteral::new(PropertyValue::array([1])),
///     Err(SecondaryIndexLiteralError::NestedValue)
/// );
/// assert!(SecondaryIndexLiteral::new(PropertyValue::Null).is_ok());
/// assert!(SecondaryIndexLiteral::new(PropertyValue::from("null")).is_ok());
/// ```
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SecondaryIndexLiteral {
    value: PropertyValue,
}

impl SecondaryIndexLiteral {
    /// Build a secondary-index literal, rejecting nested array/object values.
    pub fn new(value: PropertyValue) -> Result<Self, SecondaryIndexLiteralError> {
        match value {
            PropertyValue::Array(_) | PropertyValue::Object(_) => {
                Err(SecondaryIndexLiteralError::NestedValue)
            }
            value => Ok(Self { value }),
        }
    }

    /// Borrow the validated literal value.
    ///
    /// ```
    /// use helix_ast::value::PropertyValue;
    /// use helix_planner::ir::SecondaryIndexLiteral;
    ///
    /// let literal = SecondaryIndexLiteral::new(PropertyValue::from("alice")).unwrap();
    /// assert_eq!(literal.as_property_value().as_str(), Some("alice"));
    /// ```
    pub fn as_property_value(&self) -> &PropertyValue {
        &self.value
    }

    /// Return the storage behavior implied by this validated literal.
    ///
    /// ```
    /// use helix_ast::value::PropertyValue;
    /// use helix_planner::ir::{LiteralEqualityIndexValueSemantics, SecondaryIndexLiteral};
    ///
    /// let nan = SecondaryIndexLiteral::new(PropertyValue::F64(f64::NAN)).unwrap();
    /// let null = SecondaryIndexLiteral::new(PropertyValue::Null).unwrap();
    /// assert_eq!(nan.semantics(), LiteralEqualityIndexValueSemantics::NonReflexive);
    /// assert_eq!(null.semantics(), LiteralEqualityIndexValueSemantics::AuthoritativeNull);
    /// ```
    pub fn semantics(&self) -> LiteralEqualityIndexValueSemantics {
        match &self.value {
            PropertyValue::Null => LiteralEqualityIndexValueSemantics::AuthoritativeNull,
            PropertyValue::F64(value) if value.is_nan() => {
                LiteralEqualityIndexValueSemantics::NonReflexive
            }
            PropertyValue::F32(value) if value.is_nan() => {
                LiteralEqualityIndexValueSemantics::NonReflexive
            }
            PropertyValue::F64Array(values) if values.iter().any(|value| value.is_nan()) => {
                LiteralEqualityIndexValueSemantics::NonReflexive
            }
            PropertyValue::F32Array(values) if values.iter().any(|value| value.is_nan()) => {
                LiteralEqualityIndexValueSemantics::NonReflexive
            }
            PropertyValue::Bool(_)
            | PropertyValue::I64(_)
            | PropertyValue::DateTime(_)
            | PropertyValue::F64(_)
            | PropertyValue::F32(_)
            | PropertyValue::String(_)
            | PropertyValue::Bytes(_)
            | PropertyValue::I64Array(_)
            | PropertyValue::F64Array(_)
            | PropertyValue::F32Array(_)
            | PropertyValue::StringArray(_) => LiteralEqualityIndexValueSemantics::Indexed,
            PropertyValue::Array(_) | PropertyValue::Object(_) => {
                unreachable!("secondary-index literals reject nested values")
            }
        }
    }
}

impl<'de> Deserialize<'de> for SecondaryIndexLiteral {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = PropertyValue::deserialize(deserializer)?;
        Self::new(value).map_err(|_| D::Error::custom("expected non-nested secondary index value"))
    }
}

/// Equality-index lookup value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexValue {
    /// Literal value.
    Literal(SecondaryIndexLiteral),
    /// Runtime parameter value.
    Param(NonEmptyString),
}

impl IndexValue {
    /// Return the statically known storage behavior for this lookup value.
    pub fn semantics(&self) -> EqualityIndexValueSemantics {
        match self {
            Self::Literal(value) => match value.semantics() {
                LiteralEqualityIndexValueSemantics::Indexed => EqualityIndexValueSemantics::Indexed,
                LiteralEqualityIndexValueSemantics::AuthoritativeNull => {
                    EqualityIndexValueSemantics::AuthoritativeNull
                }
                LiteralEqualityIndexValueSemantics::NonReflexive => {
                    EqualityIndexValueSemantics::NonReflexive
                }
            },
            Self::Param(_) => EqualityIndexValueSemantics::RuntimeDependent,
        }
    }
}
