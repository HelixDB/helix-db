use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::GraphError;

const ENVELOPE: &str = "__helix_external_id_v1";
const MAX_DEPTH: usize = 64;
const MAX_ENCODED_LEN: usize = 64 * 1024;

/// Lossless, deterministically ordered external graph identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ExternalId {
    Null,
    Boolean(bool),
    /// Canonical arbitrary-precision decimal integer.
    Integer(String),
    /// Exact IEEE-754 bit representation.
    Float(u64),
    String(String),
    Bytes(Vec<u8>),
    Tuple(Vec<Self>),
    FrozenSet(BTreeSet<Self>),
}

impl ExternalId {
    /// Borrow the payload when this identity is a string.
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            Self::Null
            | Self::Boolean(_)
            | Self::Integer(_)
            | Self::Float(_)
            | Self::Bytes(_)
            | Self::Tuple(_)
            | Self::FrozenSet(_) => None,
        }
    }

    /// Construct a validated arbitrary-precision integer identity.
    pub fn integer(value: impl Into<String>) -> Result<Self, GraphError> {
        let value = value.into();
        validate_integer(&value)?;
        Ok(Self::Integer(value))
    }

    /// Construct an exact floating-point identity.
    pub const fn float(value: f64) -> Self {
        Self::Float(value.to_bits())
    }

    /// Construct a tuple identity and validate nesting/size limits.
    pub fn tuple(values: Vec<Self>) -> Result<Self, GraphError> {
        let value = Self::Tuple(values);
        value.validate()?;
        Ok(value)
    }

    /// Construct a canonical frozen-set identity.
    pub fn frozen_set(values: impl IntoIterator<Item = Self>) -> Result<Self, GraphError> {
        let value = Self::FrozenSet(values.into_iter().collect());
        value.validate()?;
        Ok(value)
    }

    /// Validate canonical payloads and resource bounds.
    pub fn validate(&self) -> Result<(), GraphError> {
        self.validate_depth(0)?;
        let encoded = serde_json::to_vec(&self.tagged_value())
            .map_err(|error| GraphError::InvalidExternalId(error.to_string()))?;
        if encoded.len() > MAX_ENCODED_LEN {
            return Err(GraphError::InvalidExternalId(format!(
                "encoded identity exceeds {MAX_ENCODED_LEN} bytes"
            )));
        }
        Ok(())
    }

    /// Encode the canonical tagged JSON envelope used by bindings and stored
    /// tagged identity properties.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, GraphError> {
        self.validate()?;
        serde_json::to_vec(&self.tagged_value())
            .map_err(|error| GraphError::InvalidExternalId(error.to_string()))
    }

    /// Decode one canonical tagged JSON envelope.
    pub fn from_tagged_value(value: Value) -> Result<Self, GraphError> {
        let Value::Object(mut outer) = value else {
            return Err(invalid("tagged identity must be an object"));
        };
        if outer.len() != 1 {
            return Err(invalid(
                "tagged identity must contain exactly one envelope key",
            ));
        }
        let Some(payload) = outer.remove(ENVELOPE) else {
            return Err(invalid("missing tagged identity envelope"));
        };
        let Value::Object(mut payload) = payload else {
            return Err(invalid("tagged identity payload must be an object"));
        };
        let Some(Value::String(kind)) = payload.remove("type") else {
            return Err(invalid("tagged identity type must be a string"));
        };
        let value = payload.remove("value");
        if !payload.is_empty() {
            return Err(invalid("tagged identity payload contains unknown fields"));
        }
        let identity = match (kind.as_str(), value) {
            ("null", None) => Self::Null,
            ("boolean", Some(Value::Bool(value))) => Self::Boolean(value),
            ("integer", Some(Value::String(value))) => Self::integer(value)?,
            ("float", Some(Value::String(value))) => {
                if value.len() != 16
                    || value.bytes().any(|byte| byte.is_ascii_uppercase())
                    || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
                {
                    return Err(invalid("float identity must contain 16 hexadecimal digits"));
                }
                Self::Float(
                    u64::from_str_radix(&value, 16)
                        .map_err(|_| invalid("invalid float identity bits"))?,
                )
            }
            ("string", Some(Value::String(value))) => Self::String(value),
            ("bytes", Some(Value::String(value))) => Self::Bytes(decode_hex(&value)?),
            ("tuple", Some(Value::Array(values))) => Self::Tuple(
                values
                    .into_iter()
                    .map(Self::from_tagged_value)
                    .collect::<Result<_, _>>()?,
            ),
            ("frozenset", Some(Value::Array(values))) => {
                let values = values
                    .into_iter()
                    .map(Self::from_tagged_value)
                    .collect::<Result<Vec<_>, _>>()?;
                let set = values.iter().cloned().collect::<BTreeSet<_>>();
                if set.len() != values.len() || !values.windows(2).all(|pair| pair[0] < pair[1]) {
                    return Err(invalid("frozenset identity must be sorted and unique"));
                }
                Self::FrozenSet(set)
            }
            ("null", Some(_)) => return Err(invalid("null identity must not contain a value")),
            _ => return Err(invalid("invalid tagged identity type or value")),
        };
        identity.validate()?;
        Ok(identity)
    }

    /// Convert an ordinary JSON scalar without erasing its type.
    pub fn from_scalar(value: Value) -> Result<Self, GraphError> {
        let identity = match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Boolean(value),
            Value::String(value) => Self::String(value),
            Value::Number(value) if value.is_i64() || value.is_u64() => {
                Self::integer(value.to_string())?
            }
            Value::Number(value) => Self::float(
                value
                    .as_f64()
                    .ok_or_else(|| invalid("identity number cannot be represented as f64"))?,
            ),
            Value::Array(_) | Value::Object(_) => {
                return Err(invalid("scalar identity must not be an array or object"));
            }
        };
        identity.validate()?;
        Ok(identity)
    }

    fn validate_depth(&self, depth: usize) -> Result<(), GraphError> {
        if depth > MAX_DEPTH {
            return Err(invalid(format!(
                "identity nesting exceeds {MAX_DEPTH} levels"
            )));
        }
        match self {
            Self::Integer(value) => validate_integer(value),
            Self::Tuple(values) => values
                .iter()
                .try_for_each(|value| value.validate_depth(depth + 1)),
            Self::FrozenSet(values) => values
                .iter()
                .try_for_each(|value| value.validate_depth(depth + 1)),
            Self::Null | Self::Boolean(_) | Self::Float(_) | Self::String(_) | Self::Bytes(_) => {
                Ok(())
            }
        }
    }

    fn tagged_value(&self) -> Value {
        let (kind, value) = match self {
            Self::Null => ("null", None),
            Self::Boolean(value) => ("boolean", Some(Value::Bool(*value))),
            Self::Integer(value) => ("integer", Some(Value::String(value.clone()))),
            Self::Float(bits) => ("float", Some(Value::String(format!("{bits:016x}")))),
            Self::String(value) => ("string", Some(Value::String(value.clone()))),
            Self::Bytes(value) => ("bytes", Some(Value::String(encode_hex(value)))),
            Self::Tuple(values) => (
                "tuple",
                Some(Value::Array(
                    values.iter().map(Self::tagged_value).collect(),
                )),
            ),
            Self::FrozenSet(values) => (
                "frozenset",
                Some(Value::Array(
                    values.iter().map(Self::tagged_value).collect(),
                )),
            ),
        };
        let mut payload = BTreeMap::from([("type".to_string(), Value::String(kind.to_string()))]);
        if let Some(value) = value {
            payload.insert("value".to_string(), value);
        }
        Value::Object(serde_json::Map::from_iter([(
            ENVELOPE.to_string(),
            Value::Object(payload.into_iter().collect()),
        )]))
    }
}

impl Serialize for ExternalId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.tagged_value().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ExternalId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let identity = if value
            .as_object()
            .is_some_and(|object| object.contains_key(ENVELOPE))
        {
            Self::from_tagged_value(value)
        } else {
            Self::from_scalar(value)
        };
        identity.map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for ExternalId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&serde_json::to_string(&self.tagged_value()).map_err(|_| fmt::Error)?)
    }
}

impl From<String> for ExternalId {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for ExternalId {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<&String> for ExternalId {
    fn from(value: &String) -> Self {
        Self::String(value.clone())
    }
}

impl From<&ExternalId> for ExternalId {
    fn from(value: &ExternalId) -> Self {
        value.clone()
    }
}

impl From<bool> for ExternalId {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl From<i64> for ExternalId {
    fn from(value: i64) -> Self {
        Self::Integer(value.to_string())
    }
}

impl From<u64> for ExternalId {
    fn from(value: u64) -> Self {
        Self::Integer(value.to_string())
    }
}

impl From<f64> for ExternalId {
    fn from(value: f64) -> Self {
        Self::float(value)
    }
}

impl PartialEq<&str> for ExternalId {
    fn eq(&self, other: &&str) -> bool {
        matches!(self, Self::String(value) if value == *other)
    }
}

impl PartialEq<String> for ExternalId {
    fn eq(&self, other: &String) -> bool {
        matches!(self, Self::String(value) if value == other)
    }
}

/// Property selected as a graph identity source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphProperty(String);

impl GraphProperty {
    pub fn new(value: impl Into<String>) -> Result<Self, GraphError> {
        let value = value.into();
        if value.is_empty() {
            return Err(GraphError::InvalidExternalId(
                "identity property must not be empty".to_string(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Explicit node identity selection contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentitySelection {
    InternalId,
    ScalarProperty(GraphProperty),
    TaggedProperty(GraphProperty),
}

impl IdentitySelection {
    pub fn property(&self) -> Option<&GraphProperty> {
        match self {
            Self::InternalId => None,
            Self::ScalarProperty(property) | Self::TaggedProperty(property) => Some(property),
        }
    }

    pub const fn is_tagged(&self) -> bool {
        matches!(self, Self::TaggedProperty(_))
    }
}

fn validate_integer(value: &str) -> Result<(), GraphError> {
    let digits = value.strip_prefix('-').unwrap_or(value);
    let canonical = !digits.is_empty()
        && digits.bytes().all(|byte| byte.is_ascii_digit())
        && (digits == "0" || !digits.starts_with('0'))
        && value != "-0";
    if canonical {
        Ok(())
    } else {
        Err(invalid("integer identity is not canonical decimal"))
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_hex(value: &str) -> Result<Vec<u8>, GraphError> {
    if value.len() % 2 != 0 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid(
            "bytes identity must contain lowercase hexadecimal pairs",
        ));
    }
    if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(invalid("bytes identity hexadecimal must be lowercase"));
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("hexadecimal is ASCII");
            u8::from_str_radix(text, 16).map_err(|_| invalid("invalid bytes identity"))
        })
        .collect()
}

fn invalid(message: impl Into<String>) -> GraphError {
    GraphError::InvalidExternalId(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tagged_round_trip_preserves_every_identity_variant() {
        let values = [
            ExternalId::Null,
            ExternalId::Boolean(true),
            ExternalId::integer("123456789012345678901234567890").unwrap(),
            ExternalId::float(-0.0),
            ExternalId::String(String::new()),
            ExternalId::Bytes(vec![0, 1, 255]),
            ExternalId::tuple(vec![ExternalId::from(1_i64), ExternalId::from("1")]).unwrap(),
            ExternalId::frozen_set([ExternalId::from("b"), ExternalId::from("a")]).unwrap(),
        ];
        for value in values {
            let encoded = value.to_json_bytes().unwrap();
            assert_eq!(
                serde_json::from_slice::<ExternalId>(&encoded).unwrap(),
                value
            );
        }
    }

    #[test]
    fn identity_types_do_not_collide_and_invalid_canonical_forms_fail() {
        assert_ne!(ExternalId::from(1_i64), ExternalId::from("1"));
        assert_ne!(ExternalId::from(true), ExternalId::from("true"));
        assert_ne!(ExternalId::float(0.0), ExternalId::float(-0.0));
        assert!(ExternalId::integer("01").is_err());
        assert!(ExternalId::integer("-0").is_err());
        assert!(serde_json::from_value::<ExternalId>(serde_json::json!({
            (ENVELOPE): {"type": "bytes", "value": "FF"}
        }))
        .is_err());
    }

    #[test]
    fn scalar_and_selection_contracts_cover_every_variant() {
        let property = GraphProperty::new("identity").unwrap();
        assert_eq!(property.as_str(), "identity");
        assert!(matches!(
            GraphProperty::new(""),
            Err(GraphError::InvalidExternalId(_))
        ));

        let internal = IdentitySelection::InternalId;
        assert_eq!(internal.property(), None);
        assert!(!internal.is_tagged());
        let scalar = IdentitySelection::ScalarProperty(property.clone());
        assert_eq!(scalar.property(), Some(&property));
        assert!(!scalar.is_tagged());
        let tagged = IdentitySelection::TaggedProperty(property.clone());
        assert_eq!(tagged.property(), Some(&property));
        assert!(tagged.is_tagged());

        assert_eq!(
            ExternalId::from_scalar(Value::Null).unwrap(),
            ExternalId::Null
        );
        assert_eq!(
            ExternalId::from_scalar(Value::Bool(false)).unwrap(),
            ExternalId::Boolean(false)
        );
        assert_eq!(
            ExternalId::from_scalar(Value::String("identity".to_string())).unwrap(),
            "identity"
        );
        assert_eq!(
            ExternalId::from_scalar(serde_json::json!(42)).unwrap(),
            ExternalId::from(42_u64)
        );
        assert_eq!(
            ExternalId::from_scalar(serde_json::json!(1.5)).unwrap(),
            ExternalId::from(1.5_f64)
        );
        assert!(ExternalId::from_scalar(serde_json::json!([])).is_err());
        assert!(ExternalId::from_scalar(serde_json::json!({})).is_err());

        let owned = "owned".to_string();
        let borrowed = ExternalId::from(&owned);
        assert_eq!(borrowed.as_string(), Some("owned"));
        assert_eq!(ExternalId::from(&borrowed), borrowed);
        assert_eq!(borrowed, owned);
        for value in [
            ExternalId::Null,
            ExternalId::Boolean(false),
            ExternalId::from(1_u64),
            ExternalId::from(1.0_f64),
            ExternalId::Bytes(Vec::new()),
            ExternalId::tuple(Vec::new()).unwrap(),
            ExternalId::frozen_set([]).unwrap(),
        ] {
            assert_eq!(value.as_string(), None);
        }
    }

    #[test]
    fn tagged_decoder_rejects_every_noncanonical_envelope_shape() {
        let invalid_values = [
            serde_json::json!(null),
            serde_json::json!({(ENVELOPE): {"type": "null"}, "extra": null}),
            serde_json::json!({"wrong": {"type": "null"}}),
            serde_json::json!({(ENVELOPE): null}),
            serde_json::json!({(ENVELOPE): {"value": null}}),
            serde_json::json!({(ENVELOPE): {"type": 1}}),
            serde_json::json!({(ENVELOPE): {"type": "null", "unknown": true}}),
            serde_json::json!({(ENVELOPE): {"type": "float", "value": "0"}}),
            serde_json::json!({(ENVELOPE): {"type": "float", "value": "000000000000000A"}}),
            serde_json::json!({(ENVELOPE): {"type": "float", "value": "000000000000000g"}}),
            serde_json::json!({(ENVELOPE): {"type": "bytes", "value": "0"}}),
            serde_json::json!({(ENVELOPE): {"type": "bytes", "value": "gg"}}),
            serde_json::json!({(ENVELOPE): {"type": "bytes", "value": "FF"}}),
            serde_json::json!({
                (ENVELOPE): {
                    "type": "frozenset",
                    "value": [
                        {(ENVELOPE): {"type": "string", "value": "b"}},
                        {(ENVELOPE): {"type": "string", "value": "a"}}
                    ]
                }
            }),
            serde_json::json!({
                (ENVELOPE): {
                    "type": "frozenset",
                    "value": [
                        {(ENVELOPE): {"type": "string", "value": "a"}},
                        {(ENVELOPE): {"type": "string", "value": "a"}}
                    ]
                }
            }),
            serde_json::json!({(ENVELOPE): {"type": "null", "value": null}}),
            serde_json::json!({(ENVELOPE): {"type": "unknown"}}),
            serde_json::json!({(ENVELOPE): {"type": "boolean", "value": "false"}}),
        ];
        for value in invalid_values {
            assert!(matches!(
                ExternalId::from_tagged_value(value),
                Err(GraphError::InvalidExternalId(_))
            ));
        }
    }

    #[test]
    fn identity_resource_bounds_return_typed_errors() {
        let nested =
            (0..=MAX_DEPTH).fold(ExternalId::Null, |value, _| ExternalId::Tuple(vec![value]));
        assert!(matches!(
            nested.validate(),
            Err(GraphError::InvalidExternalId(message))
                if message.contains("nesting exceeds")
        ));

        let oversized = ExternalId::String("x".repeat(MAX_ENCODED_LEN));
        assert!(matches!(
            oversized.validate(),
            Err(GraphError::InvalidExternalId(message))
                if message.contains("encoded identity exceeds")
        ));
    }
}
