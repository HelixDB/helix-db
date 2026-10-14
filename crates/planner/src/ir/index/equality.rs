use helix_ast::value::PropertyValue;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};
use std::num::NonZeroUsize;

use crate::ir::NonEmptyString;

/// Largest canonical secondary-equality value that storage indexes. Storage
/// rejects indexing a larger value, so no indexed element can equal one. The
/// database codec asserts that this equals its own bound.
pub const MAX_INDEXED_EQUALITY_BYTES: usize = 1024 * 1024 - 64;

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
        Self::validate_value(&value)?;
        Ok(Self { value })
    }

    /// Borrowed eligibility shared by scheduling and owned literal construction.
    pub(crate) fn validate_value(value: &PropertyValue) -> Result<(), SecondaryIndexLiteralError> {
        match value {
            PropertyValue::Array(_) | PropertyValue::Object(_) => {
                Err(SecondaryIndexLiteralError::NestedValue)
            }
            _ => Ok(()),
        }
    }

    /// Whether this value's canonical encoding may exceed
    /// [`MAX_INDEXED_EQUALITY_BYTES`]. The estimate allows 64 bytes for the
    /// value header and 16 bytes per array item, which covers every number and
    /// string length prefix, so a lookup of any other value fits a key.
    ///
    /// ```
    /// use helix_ast::value::PropertyValue;
    /// use helix_planner::ir::{SecondaryIndexLiteral, MAX_INDEXED_EQUALITY_BYTES};
    ///
    /// let literal = |value| SecondaryIndexLiteral::new(value).unwrap();
    /// assert!(!literal(PropertyValue::from("alice")).may_exceed_index_key());
    /// assert!(literal(PropertyValue::from("x".repeat(MAX_INDEXED_EQUALITY_BYTES)))
    ///     .may_exceed_index_key());
    /// assert!(literal(PropertyValue::I64Array(vec![0; MAX_INDEXED_EQUALITY_BYTES / 16]))
    ///     .may_exceed_index_key());
    /// ```
    pub fn may_exceed_index_key(&self) -> bool {
        let items = |count: usize| count.saturating_mul(16);
        let payload = match &self.value {
            PropertyValue::String(value) => value.len(),
            PropertyValue::Bytes(value) => value.len(),
            PropertyValue::StringArray(values) => {
                values.iter().fold(items(values.len()), |bytes, value| {
                    bytes.saturating_add(value.len())
                })
            }
            PropertyValue::I64Array(values) => items(values.len()),
            PropertyValue::F64Array(values) => items(values.len()),
            PropertyValue::F32Array(values) => items(values.len()),
            PropertyValue::Null
            | PropertyValue::Bool(_)
            | PropertyValue::I64(_)
            | PropertyValue::F64(_)
            | PropertyValue::F32(_)
            | PropertyValue::DateTime(_)
            | PropertyValue::Array(_)
            | PropertyValue::Object(_) => 0,
        };
        payload.saturating_add(64) > MAX_INDEXED_EQUALITY_BYTES
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
    /// Runtime parameter interpreted as a bounded equality domain.
    ParamSet(RuntimeEqualitySet),
    /// Finite literal equality domain wider than one index union; read as
    /// batched multi-gets.
    ///
    /// It stays one source however many values it holds, so set rules never
    /// compare its members pairwise.
    LiteralSet(super::super::AtLeast<SecondaryIndexLiteral, 2>),
}

impl IndexValue {
    /// Return the statically known storage behavior for this lookup value.
    ///
    /// A literal set is `AuthoritativeNull` when any member is null,
    /// `Indexed` when another member is indexed, and `NonReflexive` only when
    /// every member is non-reflexive; non-reflexive members match nothing.
    ///
    /// ```
    /// use helix_ast::value::PropertyValue;
    /// use helix_planner::ir::{
    ///     AtLeast, EqualityIndexValueSemantics, IndexValue, SecondaryIndexLiteral,
    /// };
    ///
    /// let literal = |value| SecondaryIndexLiteral::new(value).unwrap();
    /// let set = |values: Vec<PropertyValue>| {
    ///     IndexValue::LiteralSet(
    ///         AtLeast::try_from_vec(values.into_iter().map(literal).collect()).unwrap(),
    ///     )
    /// };
    /// assert_eq!(
    ///     set(vec![PropertyValue::from(1), PropertyValue::F64(f64::NAN)]).semantics(),
    ///     EqualityIndexValueSemantics::Indexed
    /// );
    /// assert_eq!(
    ///     set(vec![PropertyValue::from(1), PropertyValue::Null]).semantics(),
    ///     EqualityIndexValueSemantics::AuthoritativeNull
    /// );
    /// assert_eq!(
    ///     set(vec![PropertyValue::F64(f64::NAN), PropertyValue::F32(f32::NAN)]).semantics(),
    ///     EqualityIndexValueSemantics::NonReflexive
    /// );
    /// ```
    pub fn semantics(&self) -> EqualityIndexValueSemantics {
        let literal = |value: &SecondaryIndexLiteral| match value.semantics() {
            LiteralEqualityIndexValueSemantics::Indexed => EqualityIndexValueSemantics::Indexed,
            LiteralEqualityIndexValueSemantics::AuthoritativeNull => {
                EqualityIndexValueSemantics::AuthoritativeNull
            }
            LiteralEqualityIndexValueSemantics::NonReflexive => {
                EqualityIndexValueSemantics::NonReflexive
            }
        };
        match self {
            Self::Literal(value) => literal(value),
            Self::LiteralSet(values) => values.iter().map(literal).fold(
                EqualityIndexValueSemantics::NonReflexive,
                |set, member| match (set, member) {
                    (EqualityIndexValueSemantics::AuthoritativeNull, _)
                    | (_, EqualityIndexValueSemantics::AuthoritativeNull) => {
                        EqualityIndexValueSemantics::AuthoritativeNull
                    }
                    (EqualityIndexValueSemantics::Indexed, _)
                    | (_, EqualityIndexValueSemantics::Indexed) => {
                        EqualityIndexValueSemantics::Indexed
                    }
                    (set, _) => set,
                },
            ),
            Self::Param(_) | Self::ParamSet(_) => EqualityIndexValueSemantics::RuntimeDependent,
        }
    }

    /// Hard upper bound on the elements a unique equality index read of this
    /// value can return.
    ///
    /// Each indexed literal has at most one owner and a non-reflexive literal
    /// none, so a literal or literal set is bounded by its indexed members.
    /// Null is not held by the unique lane: the read returns every label row
    /// whose property is null or missing, so a null literal, or a set holding
    /// one, has no bound. Neither does a runtime parameter or domain, which
    /// may bind null.
    ///
    /// ```
    /// use helix_ast::value::PropertyValue;
    /// use helix_planner::ir::{AtLeast, IndexValue, NonEmptyString, SecondaryIndexLiteral};
    ///
    /// let literal = |value| SecondaryIndexLiteral::new(value).unwrap();
    /// let set = |values: Vec<PropertyValue>| {
    ///     IndexValue::LiteralSet(
    ///         AtLeast::try_from_vec(values.into_iter().map(literal).collect()).unwrap(),
    ///     )
    /// };
    /// assert_eq!(IndexValue::Literal(literal("a".into())).unique_hard_upper_bound(), Some(1));
    /// assert_eq!(
    ///     set(vec!["a".into(), "b".into(), PropertyValue::F64(f64::NAN)]).unique_hard_upper_bound(),
    ///     Some(2)
    /// );
    /// assert_eq!(set(vec!["a".into(), PropertyValue::Null]).unique_hard_upper_bound(), None);
    /// assert_eq!(
    ///     IndexValue::Param(NonEmptyString::new("email").unwrap()).unique_hard_upper_bound(),
    ///     None
    /// );
    /// ```
    pub fn unique_hard_upper_bound(&self) -> Option<usize> {
        let indexed = |literal: &SecondaryIndexLiteral| match literal.semantics() {
            LiteralEqualityIndexValueSemantics::Indexed => Some(1),
            LiteralEqualityIndexValueSemantics::NonReflexive => Some(0),
            LiteralEqualityIndexValueSemantics::AuthoritativeNull => None,
        };
        match self {
            Self::Literal(literal) => indexed(literal),
            Self::LiteralSet(literals) => literals.iter().try_fold(0usize, |sum, literal| {
                Some(sum.saturating_add(indexed(literal)?))
            }),
            Self::Param(_) | Self::ParamSet(_) => None,
        }
    }
}

/// Genuinely late-bound, bounded equality-domain parameter.
///
/// The positive limit bounds each runtime index union, so a domain of any size
/// is read as unions of at most that many values.
///
/// ```
/// use helix_planner::ir::{NonEmptyString, RuntimeEqualitySet};
/// use std::num::NonZeroUsize;
///
/// let values = RuntimeEqualitySet::new(
///     NonEmptyString::new("orbit_ids").unwrap(),
///     NonZeroUsize::new(64).unwrap(),
/// );
/// assert_eq!(values.param().as_ref(), "orbit_ids");
/// assert_eq!(values.max_values().get(), 64);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEqualitySet {
    param: NonEmptyString,
    max_values: NonZeroUsize,
}

impl RuntimeEqualitySet {
    /// Build a bounded runtime equality domain.
    pub const fn new(param: NonEmptyString, max_values: NonZeroUsize) -> Self {
        Self { param, max_values }
    }

    /// Runtime parameter name.
    pub const fn param(&self) -> &NonEmptyString {
        &self.param
    }

    /// Distinct equality values read by one index union: the batch width of
    /// one multi-get. A wider domain is read as several unions, never by a
    /// scan.
    pub const fn max_values(&self) -> NonZeroUsize {
        self.max_values
    }
}
