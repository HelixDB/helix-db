use crate::utils::id::ID;
use crate::{helix_engine::types::GraphError, helixc::generator::utils::GenRef};
use chrono::Utc;
use serde::{
    Deserializer, Serializer,
    de::{DeserializeSeed, VariantAccess, Visitor},
};
use serde_json::Value as JsonValue;
use sonic_rs::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::{self, Display},
};

/// A flexible value type that can represent various property values in nodes and edges.
/// Handles both JSON and binary serialisation formats via custom implementaions of the Serialize and Deserialize traits.
#[derive(Clone, PartialEq, Debug)]
pub enum Value {
    String(String),
    F32(f32),
    F64(f64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    Boolean(bool),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
    Empty,
}

impl Value {
    pub fn to_string(&self) -> String {
        match self {
            Value::String(s) => s.to_string(),
            Value::F32(f) => f.to_string(),
            Value::F64(f) => f.to_string(),
            Value::I8(i) => i.to_string(),
            Value::I16(i) => i.to_string(),
            Value::I32(i) => i.to_string(),
            Value::I64(i) => i.to_string(),
            Value::U8(u) => u.to_string(),
            Value::U16(u) => u.to_string(),
            Value::U32(u) => u.to_string(),
            Value::U64(u) => u.to_string(),
            Value::U128(u) => u.to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Array(arr) => arr
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>()
                .join(" "),
            Value::Object(obj) => obj
                .iter()
                .map(|(k, v)| format!("{} {}", k, v.to_string()))
                .collect::<Vec<String>>()
                .join(" "),
            _ => panic!("Not primitive"),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Value::String(s) => s.as_str(),
            _ => panic!("Not a string"),
        }
    }

    #[inline]
    #[allow(unused_variables)] // default is not used but needed for function signature
    pub fn map_value_or(
        self,
        default: bool,
        f: impl Fn(&Value) -> bool,
    ) -> Result<bool, GraphError> {
        Ok(f(&self))
    }
}
impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(_) => write!(f, "String"),
            Value::F32(_) => write!(f, "F32"),
            Value::F64(_) => write!(f, "F64"),
            Value::I8(_) => write!(f, "I8"),
            Value::I16(_) => write!(f, "I16"),
            Value::I32(_) => write!(f, "I32"),
            Value::I64(_) => write!(f, "I64"),
            Value::U8(_) => write!(f, "U8"),
            Value::U16(_) => write!(f, "U16"),
            Value::U32(_) => write!(f, "U32"),
            Value::U64(_) => write!(f, "U64"),
            Value::U128(_) => write!(f, "U128"),
            Value::Boolean(_) => write!(f, "Boolean"),
            Value::Array(_) => write!(f, "Array"),
            Value::Object(_) => write!(f, "Object"),
            Value::Empty => write!(f, "Empty"),
        }
    }
}
impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::String(s), Value::String(o)) => s.cmp(o),
            (Value::F32(s), Value::F32(o)) => match s.partial_cmp(o) {
                Some(o) => o,
                None => Ordering::Equal,
            },
            (Value::F64(s), Value::F64(o)) => match s.partial_cmp(o) {
                Some(o) => o,
                None => Ordering::Equal,
            },
            (Value::I8(s), Value::I8(o)) => s.cmp(o),
            (Value::I16(s), Value::I16(o)) => s.cmp(o),
            (Value::I32(s), Value::I32(o)) => s.cmp(o),
            (Value::I64(s), Value::I64(o)) => s.cmp(o),
            (Value::U8(s), Value::U8(o)) => s.cmp(o),
            (Value::U16(s), Value::U16(o)) => s.cmp(o),
            (Value::U32(s), Value::U32(o)) => s.cmp(o),
            (Value::U64(s), Value::U64(o)) => s.cmp(o),
            (Value::U128(s), Value::U128(o)) => s.cmp(o),
            (Value::Boolean(s), Value::Boolean(o)) => s.cmp(o),
            (Value::Array(s), Value::Array(o)) => s.cmp(o),
            (Value::Empty, Value::Empty) => Ordering::Equal,
            (Value::Empty, _) => Ordering::Less,
            (_, Value::Empty) => Ordering::Greater,
            (_, _) => Ordering::Equal,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Value {}

impl PartialEq<u8> for Value {
    fn eq(&self, other: &u8) -> bool {
        match self {
            Value::U8(u) => u == other,
            _ => false,
        }
    }
}
impl PartialEq<u16> for Value {
    fn eq(&self, other: &u16) -> bool {
        match self {
            Value::U16(u) => u == other,
            _ => false,
        }
    }
}
impl PartialEq<u32> for Value {
    fn eq(&self, other: &u32) -> bool {
        match self {
            Value::U32(u) => u == other,
            _ => false,
        }
    }
}
impl PartialEq<u64> for Value {
    fn eq(&self, other: &u64) -> bool {
        match self {
            Value::U64(u) => u == other,
            _ => false,
        }
    }
}
impl PartialEq<u128> for Value {
    fn eq(&self, other: &u128) -> bool {
        match self {
            Value::U128(u) => u == other,
            _ => false,
        }
    }
}
impl PartialEq<i8> for Value {
    fn eq(&self, other: &i8) -> bool {
        match self {
            Value::I8(i) => i == other,
            _ => false,
        }
    }
}
impl PartialEq<i16> for Value {
    fn eq(&self, other: &i16) -> bool {
        match self {
            Value::I16(i) => i == other,
            _ => false,
        }
    }
}
impl PartialEq<i32> for Value {
    fn eq(&self, other: &i32) -> bool {
        match self {
            Value::I32(i) => i == other,
            _ => false,
        }
    }
}
impl PartialEq<i64> for Value {
    fn eq(&self, other: &i64) -> bool {
        match self {
            Value::I64(i) => i == other,
            _ => false,
        }
    }
}

impl PartialEq<f32> for Value {
    fn eq(&self, other: &f32) -> bool {
        match self {
            Value::F32(f) => f == other,
            _ => false,
        }
    }
}
impl PartialEq<f64> for Value {
    fn eq(&self, other: &f64) -> bool {
        match self {
            Value::F64(f) => f == other,
            _ => false,
        }
    }
}

impl PartialEq<String> for Value {
    fn eq(&self, other: &String) -> bool {
        match self {
            Value::String(s) => s == other,
            _ => false,
        }
    }
}

impl PartialEq<bool> for Value {
    fn eq(&self, other: &bool) -> bool {
        match self {
            Value::Boolean(b) => b == other,
            _ => false,
        }
    }
}

impl PartialEq<&str> for Value {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Value::String(s) => s == other,
            _ => false,
        }
    }
}

impl PartialOrd<i8> for Value {
    fn partial_cmp(&self, other: &i8) -> Option<Ordering> {
        match self {
            Value::I8(i) => i.partial_cmp(other),
            _ => None,
        }
    }
}
impl PartialOrd<i16> for Value {
    fn partial_cmp(&self, other: &i16) -> Option<Ordering> {
        match self {
            Value::I16(i) => i.partial_cmp(other),
            _ => None,
        }
    }
}
impl PartialOrd<i32> for Value {
    fn partial_cmp(&self, other: &i32) -> Option<Ordering> {
        match self {
            Value::I32(i) => i.partial_cmp(other),
            _ => None,
        }
    }
}
impl PartialOrd<i64> for Value {
    fn partial_cmp(&self, other: &i64) -> Option<Ordering> {
        match self {
            Value::I64(i) => i.partial_cmp(other),
            _ => None,
        }
    }
}
impl PartialOrd<f32> for Value {
    fn partial_cmp(&self, other: &f32) -> Option<Ordering> {
        match self {
            Value::F32(f) => f.partial_cmp(other),
            _ => None,
        }
    }
}
impl PartialOrd<f64> for Value {
    fn partial_cmp(&self, other: &f64) -> Option<Ordering> {
        match self {
            Value::F64(f) => f.partial_cmp(other),
            _ => None,
        }
    }
}
impl PartialOrd<u8> for Value {
    fn partial_cmp(&self, other: &u8) -> Option<Ordering> {
        match self {
            Value::U8(u) => u.partial_cmp(other),
            _ => None,
        }
    }
}
impl PartialOrd<u16> for Value {
    fn partial_cmp(&self, other: &u16) -> Option<Ordering> {
        match self {
            Value::U16(u) => u.partial_cmp(other),
            _ => None,
        }
    }
}
impl PartialOrd<u32> for Value {
    fn partial_cmp(&self, other: &u32) -> Option<Ordering> {
        match self {
            Value::U32(u) => u.partial_cmp(other),
            _ => None,
        }
    }
}
impl PartialOrd<u64> for Value {
    fn partial_cmp(&self, other: &u64) -> Option<Ordering> {
        match self {
            Value::U64(u) => u.partial_cmp(other),
            _ => None,
        }
    }
}
impl PartialOrd<u128> for Value {
    fn partial_cmp(&self, other: &u128) -> Option<Ordering> {
        match self {
            Value::U128(u) => u.partial_cmp(other),
            _ => None,
        }
    }
}
/// Custom serialisation implementation for Value that removes enum variant names in JSON
/// whilst preserving them for binary formats like bincode.
impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            match self {
                Value::String(s) => s.serialize(serializer),
                Value::F32(f) => f.serialize(serializer),
                Value::F64(f) => f.serialize(serializer),
                Value::I8(i) => i.serialize(serializer),
                Value::I16(i) => i.serialize(serializer),
                Value::I32(i) => i.serialize(serializer),
                Value::I64(i) => i.serialize(serializer),
                Value::U8(i) => i.serialize(serializer),
                Value::U16(i) => i.serialize(serializer),
                Value::U32(i) => i.serialize(serializer),
                Value::U64(i) => i.serialize(serializer),
                Value::U128(i) => i.serialize(serializer),
                Value::Boolean(b) => b.serialize(serializer),
                Value::Array(arr) => {
                    use serde::ser::SerializeSeq;
                    let mut seq = serializer.serialize_seq(Some(arr.len()))?;
                    for value in arr {
                        seq.serialize_element(&value)?;
                    }
                    seq.end()
                }
                Value::Object(obj) => {
                    use serde::ser::SerializeMap;
                    let mut map = serializer.serialize_map(Some(obj.len()))?;
                    for (k, v) in obj {
                        map.serialize_entry(k, v)?;
                    }
                    map.end()
                }
                Value::Empty => serializer.serialize_none(),
            }
        } else {
            match self {
                Value::String(s) => serializer.serialize_newtype_variant("Value", 0, "String", s),
                Value::F32(f) => serializer.serialize_newtype_variant("Value", 1, "F32", f),
                Value::F64(f) => serializer.serialize_newtype_variant("Value", 2, "F64", f),
                Value::I8(i) => serializer.serialize_newtype_variant("Value", 3, "I8", i),
                Value::I16(i) => serializer.serialize_newtype_variant("Value", 4, "I16", i),
                Value::I32(i) => serializer.serialize_newtype_variant("Value", 5, "I32", i),
                Value::I64(i) => serializer.serialize_newtype_variant("Value", 6, "I64", i),
                Value::U8(i) => serializer.serialize_newtype_variant("Value", 7, "U8", i),
                Value::U16(i) => serializer.serialize_newtype_variant("Value", 8, "U16", i),
                Value::U32(i) => serializer.serialize_newtype_variant("Value", 9, "U32", i),
                Value::U64(i) => serializer.serialize_newtype_variant("Value", 10, "U64", i),
                Value::U128(i) => serializer.serialize_newtype_variant("Value", 11, "U128", i),
                Value::Boolean(b) => {
                    serializer.serialize_newtype_variant("Value", 12, "Boolean", b)
                }
                Value::Array(a) => serializer.serialize_newtype_variant("Value", 13, "Array", a),
                Value::Object(obj) => {
                    serializer.serialize_newtype_variant("Value", 14, "Object", obj)
                }
                Value::Empty => serializer.serialize_unit_variant("Value", 15, "Empty"),
            }
        }
    }
}

/// Custom deserialisation implementation for Value that handles both JSON and binary formats.
/// For JSON, parses raw values directly.
/// For binary formats like bincode, reconstructs the full enum structure.
impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        /// Visitor implementation that handles conversion of raw values into Value enum variants.
        /// Supports both direct value parsing for JSON and enum variant parsing for binary formats.
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            #[inline]
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string, number, boolean, array, null, or Value enum")
            }

            #[inline]
            fn visit_str<E>(self, value: &str) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::String(value.to_owned()))
            }

            #[inline]
            fn visit_string<E>(self, value: String) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::String(value))
            }

            #[inline]
            fn visit_f32<E>(self, value: f32) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::F32(value))
            }

            #[inline]
            fn visit_f64<E>(self, value: f64) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::F64(value))
            }

            #[inline]
            fn visit_i8<E>(self, value: i8) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::I8(value))
            }

            #[inline]
            fn visit_i16<E>(self, value: i16) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::I16(value))
            }

            #[inline]
            fn visit_i32<E>(self, value: i32) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::I32(value))
            }

            #[inline]
            fn visit_i64<E>(self, value: i64) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::I64(value))
            }

            #[inline]
            fn visit_u8<E>(self, value: u8) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::U8(value))
            }

            #[inline]
            fn visit_u16<E>(self, value: u16) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::U16(value))
            }

            #[inline]
            fn visit_u32<E>(self, value: u32) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::U32(value))
            }

            #[inline]
            fn visit_u64<E>(self, value: u64) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::U64(value))
            }

            #[inline]
            fn visit_u128<E>(self, value: u128) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::U128(value))
            }

            #[inline]
            fn visit_bool<E>(self, value: bool) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::Boolean(value))
            }

            #[inline]
            fn visit_none<E>(self) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Value::Empty)
            }

            /// Handles array values by recursively deserialising each element
            fn visit_seq<A>(self, mut seq: A) -> Result<Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element()? {
                    values.push(value);
                }
                Ok(Value::Array(values))
            }

            /// Handles binary format deserialisation using numeric indices to identify variants
            /// Maps indices 0-5 to corresponding Value enum variants
            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::EnumAccess<'de>,
            {
                let (variant_idx, variant_data) = data.variant_seed(VariantIdxDeserializer)?;
                match variant_idx {
                    0 => Ok(Value::String(variant_data.newtype_variant()?)),
                    1 => Ok(Value::F32(variant_data.newtype_variant()?)),
                    2 => Ok(Value::F64(variant_data.newtype_variant()?)),
                    3 => Ok(Value::I8(variant_data.newtype_variant()?)),
                    4 => Ok(Value::I16(variant_data.newtype_variant()?)),
                    5 => Ok(Value::I32(variant_data.newtype_variant()?)),
                    6 => Ok(Value::I64(variant_data.newtype_variant()?)),
                    7 => Ok(Value::U8(variant_data.newtype_variant()?)),
                    8 => Ok(Value::U16(variant_data.newtype_variant()?)),
                    9 => Ok(Value::U32(variant_data.newtype_variant()?)),
                    10 => Ok(Value::U64(variant_data.newtype_variant()?)),
                    11 => Ok(Value::U128(variant_data.newtype_variant()?)),
                    12 => Ok(Value::Boolean(variant_data.newtype_variant()?)),
                    13 => Ok(Value::Array(variant_data.newtype_variant()?)),
                    14 => Ok(Value::Object(variant_data.newtype_variant()?)),
                    15 => {
                        variant_data.unit_variant()?;
                        Ok(Value::Empty)
                    }
                    _ => Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(variant_idx as u64),
                        &"variant index 0 through 5",
                    )),
                }
            }
        }

        /// Helper deserialiser for handling numeric variant indices in binary format
        struct VariantIdxDeserializer;

        impl<'de> DeserializeSeed<'de> for VariantIdxDeserializer {
            type Value = u32;
            #[inline]
            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_u32(self)
            }
        }

        impl<'de> Visitor<'de> for VariantIdxDeserializer {
            type Value = u32;

            #[inline]
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("variant index")
            }

            #[inline]
            fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(v)
            }
        }
        // Choose deserialisation strategy based on format
        if deserializer.is_human_readable() {
            // For JSON, accept any value type
            deserializer.deserialize_any(ValueVisitor)
        } else {
            // For binary, use enum variant indices
            deserializer.deserialize_enum(
                "Value",
                &[
                    "String", "F32", "F64", "I8", "I16", "I32", "I64", "U8", "U16", "U32", "U64",
                    "U128", "Boolean", "Array", "Object", "Empty",
                ],
                ValueVisitor,
            )
        }
    }
}

/// Module for custom serialisation of property hashmaps
/// Ensures consistent handling of Value enum serialisation within property maps
pub mod properties_format {
    use super::*;

    #[inline]
    pub fn serialize<S>(
        properties: &Option<HashMap<String, Value>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match properties {
            Some(properties) => {
                use serde::ser::SerializeMap;
                let mut map = serializer.serialize_map(Some(properties.len()))?;
                for (k, v) in properties {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
            None => serializer.serialize_none(),
        }
    }

    #[inline]
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<HashMap<String, Value>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<HashMap<String, Value>>::deserialize(deserializer) {
            Ok(properties) => Ok(properties),
            Err(e) => Err(e),
        }
    }
}

impl From<&str> for Value {
    #[inline]
    fn from(s: &str) -> Self {
        Value::String(s.trim_matches('"').to_string())
    }
}

impl From<String> for Value {
    #[inline]
    fn from(s: String) -> Self {
        Value::String(s.trim_matches('"').to_string())
    }
}
impl From<bool> for Value {
    #[inline]
    fn from(b: bool) -> Self {
        Value::Boolean(b)
    }
}

impl From<f32> for Value {
    #[inline]
    fn from(f: f32) -> Self {
        Value::F32(f)
    }
}

impl From<f64> for Value {
    #[inline]
    fn from(f: f64) -> Self {
        Value::F64(f)
    }
}

impl From<i8> for Value {
    #[inline]
    fn from(i: i8) -> Self {
        Value::I8(i)
    }
}

impl From<i16> for Value {
    #[inline]
    fn from(i: i16) -> Self {
        Value::I16(i)
    }
}

impl From<i32> for Value {
    #[inline]
    fn from(i: i32) -> Self {
        Value::I32(i)
    }
}

impl From<i64> for Value {
    #[inline]
    fn from(i: i64) -> Self {
        Value::I64(i)
    }
}

impl From<u8> for Value {
    #[inline]
    fn from(i: u8) -> Self {
        Value::U8(i)
    }
}

impl From<u16> for Value {
    #[inline]
    fn from(i: u16) -> Self {
        Value::U16(i)
    }
}

impl From<u32> for Value {
    #[inline]
    fn from(i: u32) -> Self {
        Value::U32(i)
    }
}

impl From<u64> for Value {
    #[inline]
    fn from(i: u64) -> Self {
        Value::U64(i)
    }
}

impl From<u128> for Value {
    #[inline]
    fn from(i: u128) -> Self {
        Value::U128(i)
    }
}

impl From<Vec<Value>> for Value {
    #[inline]
    fn from(v: Vec<Value>) -> Self {
        Value::Array(v)
    }
}

impl From<Vec<bool>> for Value {
    #[inline(always)]
    fn from(v: Vec<bool>) -> Self {
        Value::Array(v.into_iter().map(|v| v.into()).collect())
    }
}

impl From<Vec<String>> for Value {
    #[inline(always)]
    fn from(v: Vec<String>) -> Self {
        Value::Array(v.into_iter().map(|v| v.into()).collect())
    }
}

impl From<Vec<i64>> for Value {
    #[inline(always)]
    fn from(v: Vec<i64>) -> Self {
        Value::Array(v.into_iter().map(|v| v.into()).collect())
    }
}

impl From<Vec<i32>> for Value {
    #[inline(always)]
    fn from(v: Vec<i32>) -> Self {
        Value::Array(v.into_iter().map(|v| v.into()).collect())
    }
}

impl From<Vec<i16>> for Value {
    #[inline(always)]
    fn from(v: Vec<i16>) -> Self {
        Value::Array(v.into_iter().map(|v| v.into()).collect())
    }
}

impl From<Vec<i8>> for Value {
    #[inline(always)]
    fn from(v: Vec<i8>) -> Self {
        Value::Array(v.into_iter().map(|v| v.into()).collect())
    }
}

impl From<Vec<u128>> for Value {
    #[inline(always)]
    fn from(v: Vec<u128>) -> Self {
        Value::Array(v.into_iter().map(|v| v.into()).collect())
    }
}

impl From<Vec<u64>> for Value {
    #[inline(always)]
    fn from(v: Vec<u64>) -> Self {
        Value::Array(v.into_iter().map(|v| v.into()).collect())
    }
}

impl From<Vec<u32>> for Value {
    #[inline(always)]
    fn from(v: Vec<u32>) -> Self {
        Value::Array(v.into_iter().map(|v| v.into()).collect())
    }
}

impl From<Vec<u16>> for Value {
    #[inline(always)]
    fn from(v: Vec<u16>) -> Self {
        Value::Array(v.into_iter().map(|v| v.into()).collect())
    }
}

impl From<Vec<u8>> for Value {
    #[inline(always)]
    fn from(v: Vec<u8>) -> Self {
        Value::Array(v.into_iter().map(|v| v.into()).collect())
    }
}

impl From<Vec<f64>> for Value {
    #[inline(always)]
    fn from(v: Vec<f64>) -> Self {
        Value::Array(v.into_iter().map(|v| v.into()).collect())
    }
}

impl From<Vec<f32>> for Value {
    #[inline(always)]
    fn from(v: Vec<f32>) -> Self {
        Value::Array(v.into_iter().map(|v| v.into()).collect())
    }
}

impl From<usize> for Value {
    #[inline]
    fn from(v: usize) -> Self {
        if cfg!(target_pointer_width = "64") {
            Value::U64(v as u64)
        } else {
            Value::U128(v as u128)
        }
    }
}

impl From<Value> for String {
    #[inline]
    fn from(v: Value) -> Self {
        match v {
            Value::String(s) => s,
            _ => panic!("Value is not a string"),
        }
    }
}
impl From<JsonValue> for Value {
    #[inline]
    fn from(v: JsonValue) -> Self {
        match v {
            JsonValue::String(s) => Value::String(s),
            JsonValue::Number(n) => {
                if let Some(u) = n.as_u64() {
                    Value::U64(u)
                } else if let Some(i) = n.as_i64() {
                    Value::I64(i)
                } else {
                    Value::F64(n.as_f64().expect(""))
                }
            }
            JsonValue::Bool(b) => Value::Boolean(b),
            JsonValue::Array(a) => Value::Array(a.into_iter().map(|v| v.into()).collect()),
            JsonValue::Object(o) => {
                Value::Object(o.into_iter().map(|(k, v)| (k, v.into())).collect())
            }
            JsonValue::Null => Value::Empty,
        }
    }
}

impl From<ID> for Value {
    #[inline]
    fn from(id: ID) -> Self {
        Value::String(id.to_string())
    }
}

impl<'a, K> From<&'a K> for Value
where
    K: Into<Value> + Serialize + Clone,
{
    #[inline]
    fn from(k: &'a K) -> Self {
        k.clone().into()
    }
}

impl From<chrono::DateTime<Utc>> for Value {
    #[inline]
    fn from(dt: chrono::DateTime<Utc>) -> Self {
        Value::String(dt.to_rfc3339())
    }
}

pub trait Encodings {
    fn decode_properties(bytes: &[u8]) -> Result<HashMap<String, Value>, GraphError>;
    fn encode_properties(&self) -> Result<Vec<u8>, GraphError>;
}

impl Encodings for HashMap<String, Value> {
    fn decode_properties(bytes: &[u8]) -> Result<HashMap<String, Value>, GraphError> {
        match bincode::deserialize(bytes) {
            Ok(properties) => Ok(properties),
            Err(e) => Err(GraphError::ConversionError(format!(
                "Error deserializing properties: {e}"
            ))),
        }
    }

    fn encode_properties(&self) -> Result<Vec<u8>, GraphError> {
        match bincode::serialize(self) {
            Ok(bytes) => Ok(bytes),
            Err(e) => Err(GraphError::ConversionError(format!(
                "Error serializing properties: {e}"
            ))),
        }
    }
}

impl From<Value> for GenRef<String> {
    fn from(v: Value) -> Self {
        match v {
            Value::String(s) => GenRef::Literal(s),
            Value::I8(i) => GenRef::Std(format!("{i}")),
            Value::I16(i) => GenRef::Std(format!("{i}")),
            Value::I32(i) => GenRef::Std(format!("{i}")),
            Value::I64(i) => GenRef::Std(format!("{i}")),
            Value::F32(f) => GenRef::Std(format!("{f:?}")), // {:?} forces decimal point
            Value::F64(f) => GenRef::Std(format!("{f:?}")),
            Value::Boolean(b) => GenRef::Std(format!("{b}")),
            Value::U8(u) => GenRef::Std(format!("{u}")),
            Value::U16(u) => GenRef::Std(format!("{u}")),
            Value::U32(u) => GenRef::Std(format!("{u}")),
            Value::U64(u) => GenRef::Std(format!("{u}")),
            Value::U128(u) => GenRef::Std(format!("{u}")),
            Value::Array(_a) => unimplemented!(),
            Value::Object(_o) => unimplemented!(),
            Value::Empty => GenRef::Literal("".to_string()),
        }
    }
}
