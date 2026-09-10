use std::{cmp::Ordering, collections::BTreeMap};

use super::{QueryError, Result};

/// Graph identity retains the complete storage ID domain.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum Entity {
    Node(u64),
    Relationship(u64),
}

/// A path always has one more node than relationships.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "PathInput")]
pub struct Path {
    nodes: Vec<u64>,
    relationships: Vec<u64>,
}

#[derive(serde::Deserialize)]
struct PathInput {
    nodes: Vec<u64>,
    relationships: Vec<u64>,
}
impl TryFrom<PathInput> for Path {
    type Error = QueryError;
    fn try_from(input: PathInput) -> Result<Self> {
        Self::new(input.nodes, input.relationships)
    }
}

impl Path {
    pub fn new(nodes: Vec<u64>, relationships: Vec<u64>) -> Result<Self> {
        if nodes.len() != relationships.len().saturating_add(1) {
            return Err(QueryError::compile(
                "InternalPlannerError",
                "InvalidPath",
                "a path must alternate nodes and relationships",
            ));
        }
        Ok(Self {
            nodes,
            relationships,
        })
    }

    pub fn nodes(&self) -> &[u64] {
        &self.nodes
    }
    pub fn relationships(&self) -> &[u64] {
        &self.relationships
    }
}

/// Query values are distinct from stored property values and wire JSON.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(#[serde(with = "float_bits")] f64),
    String(String),
    List(Vec<Value>),
    Map(BTreeMap<String, Value>),
    Entity(Entity),
    Path(Path),
}

// Planner serialization is also memo identity. JSON's null encoding for NaN
// and infinities must not merge different resolved constants. This is separate
// from the public result encoding and preserves signed zero and NaN payloads.
mod float_bits {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{:016x}", value.to_bits()))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        let bits = String::deserialize(deserializer)?;
        if bits.len() != 16 {
            return Err(serde::de::Error::custom(
                "a planner float requires 16 hexadecimal digits",
            ));
        }
        u64::from_str_radix(&bits, 16)
            .map(f64::from_bits)
            .map_err(serde::de::Error::custom)
    }
}

impl Value {
    /// Validate externally constructed nested literals before recursive use.
    pub fn validate_shape(&self) -> Result<()> {
        let mut pending = vec![(self, 0_usize)];
        let mut count = 0_usize;
        while let Some((value, depth)) = pending.pop() {
            count += 1;
            if depth >= super::MAX_EXPRESSION_DEPTH || count > 200_000 {
                return Err(QueryError::compile(
                    "ResourceLimit",
                    "ValueDepth",
                    "value exceeds structural limits",
                ));
            }
            match value {
                Self::List(values) => pending.extend(values.iter().map(|v| (v, depth + 1))),
                Self::Map(values) => pending.extend(values.values().map(|v| (v, depth + 1))),
                Self::Null
                | Self::Boolean(_)
                | Self::Integer(_)
                | Self::Float(_)
                | Self::String(_)
                | Self::Entity(_)
                | Self::Path(_) => {}
            }
        }
        Ok(())
    }
    pub fn truth(&self) -> Result<Option<bool>> {
        match self {
            Self::Null => Ok(None),
            Self::Boolean(value) => Ok(Some(*value)),
            _ => Err(QueryError::runtime(
                "TypeError",
                "InvalidArgumentType",
                "expected a boolean or null",
            )),
        }
    }

    /// Conservative owned allocation accounting for the query memory budget.
    pub fn allocated_bytes(&self) -> usize {
        let nested = match self {
            Self::String(s) => s.capacity(),
            Self::List(xs) => xs.iter().fold(
                xs.capacity()
                    .saturating_sub(xs.len())
                    .saturating_mul(size_of::<Self>()),
                |bytes, value| bytes.saturating_add(value.allocated_bytes()),
            ),
            Self::Map(xs) => xs.iter().fold(0_usize, |bytes, (k, v)| {
                bytes
                    .saturating_add(k.capacity())
                    .saturating_add(v.allocated_bytes())
                    .saturating_add(64)
            }),
            Self::Path(p) => p
                .nodes
                .capacity()
                .saturating_add(p.relationships.capacity())
                .saturating_mul(size_of::<u64>()),
            Self::Null | Self::Boolean(_) | Self::Integer(_) | Self::Float(_) | Self::Entity(_) => {
                0
            }
        };
        size_of::<Self>().saturating_add(nested)
    }

    /// Cypher predicate equality propagates unknown through nested containers.
    pub fn equals(&self, other: &Self) -> Option<bool> {
        match (self, other) {
            (Self::Null, _) | (_, Self::Null) => None,
            (Self::List(a), Self::List(b)) => {
                if a.len() != b.len() {
                    return Some(false);
                }
                let mut unknown = false;
                for (a, b) in a.iter().zip(b) {
                    match a.equals(b) {
                        Some(false) => return Some(false),
                        None => unknown = true,
                        Some(true) => {}
                    }
                }
                (!unknown).then_some(true)
            }
            (Self::Map(a), Self::Map(b)) => {
                if a.keys().ne(b.keys()) {
                    return Some(false);
                }
                let mut unknown = false;
                for (a, b) in a.values().zip(b.values()) {
                    match a.equals(b) {
                        Some(false) => return Some(false),
                        None => unknown = true,
                        Some(true) => {}
                    }
                }
                (!unknown).then_some(true)
            }
            (Self::Integer(_) | Self::Float(_), Self::Integer(_) | Self::Float(_)) => {
                Some(self.number_cmp(other) == Some(Ordering::Equal))
            }
            _ => Some(self == other),
        }
    }

    pub fn number_cmp(&self, other: &Self) -> Option<Ordering> {
        use helix_value_semantics::CanonicalNumber;
        let number = |value: &Self| match value {
            Self::Integer(x) => Some(CanonicalNumber::from_i64(*x)),
            Self::Float(x) => CanonicalNumber::from_f64(*x),
            _ => None,
        };
        Some(number(self)?.cmp(&number(other)?))
    }

    /// Total equivalence/order used by grouping, DISTINCT and sorting, not WHERE.
    pub fn total_cmp(&self, other: &Self) -> Ordering {
        let rank = |v: &Self| match v {
            Self::Map(_) => 0,
            Self::Entity(Entity::Node(_)) => 1,
            Self::Entity(Entity::Relationship(_)) => 2,
            Self::List(_) => 3,
            Self::Path(_) => 4,
            Self::String(_) => 5,
            Self::Boolean(_) => 6,
            Self::Integer(_) | Self::Float(_) => 7,
            Self::Null => 8,
        };
        let compare_lists = |a: &[Self], b: &[Self]| {
            a.iter()
                .zip(b)
                .map(|(a, b)| a.total_cmp(b))
                .find(|o| !o.is_eq())
                .unwrap_or_else(|| a.len().cmp(&b.len()))
        };
        match (self, other) {
            (Self::Integer(_) | Self::Float(_), Self::Integer(_) | Self::Float(_)) => self
                .number_cmp(other)
                .unwrap_or_else(|| match (self, other) {
                    (Self::Float(a), Self::Float(b)) if a.is_nan() && b.is_nan() => Ordering::Equal,
                    (Self::Float(a), Self::Float(_)) if !a.is_nan() => Ordering::Less,
                    (Self::Float(_), _) => Ordering::Greater,
                    _ => Ordering::Less,
                }),
            (Self::String(a), Self::String(b)) => a.cmp(b),
            (Self::Boolean(a), Self::Boolean(b)) => a.cmp(b),
            (Self::Entity(a), Self::Entity(b)) => a.cmp(b),
            (Self::List(a), Self::List(b)) => compare_lists(a, b),
            (Self::Map(a), Self::Map(b)) => a
                .len()
                .cmp(&b.len())
                .then_with(|| a.keys().cmp(b.keys()))
                .then_with(|| {
                    a.values()
                        .zip(b.values())
                        .map(|(a, b)| a.total_cmp(b))
                        .find(|o| !o.is_eq())
                        .unwrap_or(Ordering::Equal)
                }),
            (Self::Path(a), Self::Path(b)) => a
                .nodes
                .cmp(&b.nodes)
                .then_with(|| a.relationships.cmp(&b.relationships)),
            _ => rank(self).cmp(&rank(other)),
        }
    }
}

/// Hash/grouping equality is total, including null and NaN, and shares exact
/// numeric normalization with ordering. Predicate joins still check `equals`.
#[derive(Debug, Clone)]
pub struct GroupingKey(Value);
impl GroupingKey {
    pub fn new(value: Value) -> Result<Self> {
        value.validate_shape()?;
        Ok(Self(value))
    }
    pub fn value(&self) -> &Value {
        &self.0
    }
}
impl PartialEq for GroupingKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0).is_eq()
    }
}
impl Eq for GroupingKey {}
impl std::hash::Hash for GroupingKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        fn hash_value<H: std::hash::Hasher>(value: &Value, state: &mut H) {
            match value {
                Value::Integer(value) => {
                    0_u8.hash(state);
                    Some(helix_value_semantics::CanonicalNumber::from_i64(*value)).hash(state);
                }
                Value::Float(value) => {
                    0_u8.hash(state);
                    helix_value_semantics::CanonicalNumber::from_f64(*value).hash(state);
                }
                Value::Null => 1_u8.hash(state),
                Value::Boolean(value) => {
                    2_u8.hash(state);
                    value.hash(state);
                }
                Value::String(value) => {
                    3_u8.hash(state);
                    value.hash(state);
                }
                Value::Entity(value) => {
                    4_u8.hash(state);
                    value.hash(state);
                }
                Value::Path(value) => {
                    5_u8.hash(state);
                    value.nodes.hash(state);
                    value.relationships.hash(state);
                }
                Value::List(values) => {
                    6_u8.hash(state);
                    values.len().hash(state);
                    for value in values {
                        hash_value(value, state);
                    }
                }
                Value::Map(values) => {
                    7_u8.hash(state);
                    values.len().hash(state);
                    for (key, value) in values {
                        key.hash(state);
                        hash_value(value, state);
                    }
                }
            }
        }
        hash_value(&self.0, state);
    }
}
