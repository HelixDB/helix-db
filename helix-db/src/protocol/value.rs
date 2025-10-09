use crate::debug_println;
use crate::helix_gateway::mcp::tools::{FilterValues, Operator};
use crate::protocol::date::Date;
use crate::utils::id::ID;
use crate::{helix_engine::types::GraphError, helixc::generator::utils::GenRef};
use chrono::{DateTime, Utc};
use serde::{
    Deserializer, Serializer,
    de::{DeserializeSeed, VariantAccess, Visitor},
};
use sonic_rs::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::{self},
};

/// A flexible value type that can represent various property values in nodes and edges.
/// Handles both JSON and binary serialisation formats via custom implementaions of the Serialize and Deserialize traits.
#[derive(Clone, Debug)]
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
    Date(Date),
    Boolean(bool),
    Id(ID),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
    Empty,
}

impl Value {
    pub fn inner_stringify(&self) -> String {
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
            Value::Date(d) => d.to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Id(id) => id.stringify(),
            Value::Array(arr) => arr
                .iter()
                .map(|v| v.inner_stringify())
                .collect::<Vec<String>>()
                .join(" "),
            Value::Object(obj) => obj
                .iter()
                .map(|(k, v)| format!("{k} {}", v.inner_stringify()))
                .collect::<Vec<String>>()
                .join(" "),
            _ => panic!("Not primitive"),
        }
    }

    pub fn to_variant_string(&self) -> &str {
        match self {
            Value::String(_) => "String",
            Value::F32(_) => "F32",
            Value::F64(_) => "F64",
            Value::I8(_) => "I8",
            Value::I16(_) => "I16",
            Value::I32(_) => "I32",
            Value::I64(_) => "I64",
            Value::U8(_) => "U8",
            Value::U16(_) => "U16",
            Value::U32(_) => "U32",
            Value::U64(_) => "U64",
            Value::U128(_) => "U128",
            Value::Date(_) => "Date",
            Value::Boolean(_) => "Boolean",
            Value::Id(_) => "Id",
            Value::Array(_) => "Array",
            Value::Object(_) => "Object",
            Value::Empty => "Empty",
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

    #[inline]
    pub fn is_in<T>(&self, values: &[T]) -> bool
    where
        T: PartialEq,
        Value: IntoPrimitive<T> + Into<T>,
    {
        values.contains(self.into_primitive())
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        let to_i128 = |value: &Value| -> Option<i128> {
            match value {
                Value::I8(v) => Some(*v as i128),
                Value::I16(v) => Some(*v as i128),
                Value::I32(v) => Some(*v as i128),
                Value::I64(v) => Some(*v as i128),
                Value::U8(v) => Some(*v as i128),
                Value::U16(v) => Some(*v as i128),
                Value::U32(v) => Some(*v as i128),
                Value::U64(v) => Some(*v as i128),
                Value::U128(v) => {
                    if *v <= i128::MAX as u128 {
                        Some(*v as i128)
                    } else {
                        None
                    }
                }
                _ => None,
            }
        };
        let is_integer = |value: &Value| -> bool {
            matches!(
                value,
                Value::I8(_)
                    | Value::I16(_)
                    | Value::I32(_)
                    | Value::I64(_)
                    | Value::U8(_)
                    | Value::U16(_)
                    | Value::U32(_)
                    | Value::U64(_)
                    | Value::U128(_)
            )
        };

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
            (Value::Date(s), Value::Date(o)) => s.cmp(o),
            (Value::Boolean(s), Value::Boolean(o)) => s.cmp(o),
            (Value::Array(s), Value::Array(o)) => s.cmp(o),
            (Value::Empty, Value::Empty) => Ordering::Equal,
            (Value::Empty, _) => Ordering::Less,
            (_, Value::Empty) => Ordering::Greater,
            (s, o) if is_integer(s) && is_integer(o) => match (to_i128(s), to_i128(o)) {
                (Some(s), Some(o)) => s.cmp(&o),
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (None, None) => match (self, other) {
                    (Value::U128(s), Value::U128(o)) => s.cmp(o),
                    _ => unreachable!(),
                },
            },
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

impl PartialEq<Value> for Value {
    fn eq(&self, other: &Value) -> bool {
        let to_f64 = |value: &Value| -> Option<f64> {
            match value {
                Value::I8(v) => Some(*v as f64),
                Value::I16(v) => Some(*v as f64),
                Value::I32(v) => Some(*v as f64),
                Value::I64(v) => Some(*v as f64),
                Value::U8(v) => Some(*v as f64),
                Value::U16(v) => Some(*v as f64),
                Value::U32(v) => Some(*v as f64),
                Value::U64(v) => Some(*v as f64),
                Value::U128(v) => Some(*v as f64),
                Value::F32(v) => Some(*v as f64),
                Value::F64(v) => Some(*v),
                _ => None,
            }
        };

        let is_numeric = |value: &Value| -> bool {
            matches!(
                value,
                Value::I8(_)
                    | Value::I16(_)
                    | Value::I32(_)
                    | Value::I64(_)
                    | Value::U8(_)
                    | Value::U16(_)
                    | Value::U32(_)
                    | Value::U64(_)
                    | Value::U128(_)
                    | Value::F32(_)
                    | Value::F64(_)
            )
        };

        match (self, other) {
            (Value::String(s), Value::String(o)) => s == o,
            (Value::Date(s), Value::Date(o)) => s == o,
            (Value::Boolean(s), Value::Boolean(o)) => s == o,
            (Value::Array(s), Value::Array(o)) => s == o,
            (Value::Empty, Value::Empty) => true,
            (Value::Empty, _) => false,
            (_, Value::Empty) => false,

            (s, o) if is_numeric(s) && is_numeric(o) => match (to_f64(s), to_f64(o)) {
                (Some(s_val), Some(o_val)) => {
                    if !matches!(self, Value::F32(_) | Value::F64(_))
                        && !matches!(other, Value::F32(_) | Value::F64(_))
                    {
                        self.cmp(other) == Ordering::Equal
                    } else {
                        s_val == o_val
                    }
                }
                _ => false,
            },

            _ => false,
        }
    }
}

impl PartialEq<ID> for Value {
    fn eq(&self, other: &ID) -> bool {
        match self {
            Value::Id(id) => id == other,
            Value::String(s) => &ID::from(s) == other,
            Value::U128(u) => &ID::from(*u) == other,
            _ => false,
        }
    }
}
impl PartialEq<u8> for Value {
    fn eq(&self, other: &u8) -> bool {
        self == &Value::from(*other)
    }
}
impl PartialEq<u16> for Value {
    fn eq(&self, other: &u16) -> bool {
        self == &Value::from(*other)
    }
}
impl PartialEq<u32> for Value {
    fn eq(&self, other: &u32) -> bool {
        self == &Value::from(*other)
    }
}
impl PartialEq<u64> for Value {
    fn eq(&self, other: &u64) -> bool {
        self == &Value::from(*other)
    }
}
impl PartialEq<u128> for Value {
    fn eq(&self, other: &u128) -> bool {
        self == &Value::from(*other)
    }
}
impl PartialEq<i8> for Value {
    fn eq(&self, other: &i8) -> bool {
        self == &Value::from(*other)
    }
}
impl PartialEq<i16> for Value {
    fn eq(&self, other: &i16) -> bool {
        self == &Value::from(*other)
    }
}
impl PartialEq<i32> for Value {
    fn eq(&self, other: &i32) -> bool {
        self == &Value::from(*other)
    }
}
impl PartialEq<i64> for Value {
    fn eq(&self, other: &i64) -> bool {
        self == &Value::from(*other)
    }
}

impl PartialEq<f32> for Value {
    fn eq(&self, other: &f32) -> bool {
        self == &Value::from(*other)
    }
}
impl PartialEq<f64> for Value {
    fn eq(&self, other: &f64) -> bool {
        self == &Value::from(*other)
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
        self == &Value::from(*other)
    }
}

impl PartialEq<&str> for Value {
    fn eq(&self, other: &&str) -> bool {
        self == &Value::from(*other)
    }
}

impl PartialEq<DateTime<Utc>> for Value {
    fn eq(&self, other: &DateTime<Utc>) -> bool {
        self == &Value::from(*other)
    }
}

impl PartialOrd<i8> for Value {
    fn partial_cmp(&self, other: &i8) -> Option<Ordering> {
        self.partial_cmp(&Value::from(*other))
    }
}
impl PartialOrd<i16> for Value {
    fn partial_cmp(&self, other: &i16) -> Option<Ordering> {
        self.partial_cmp(&Value::from(*other))
    }
}
impl PartialOrd<i32> for Value {
    fn partial_cmp(&self, other: &i32) -> Option<Ordering> {
        self.partial_cmp(&Value::from(*other))
    }
}
impl PartialOrd<i64> for Value {
    fn partial_cmp(&self, other: &i64) -> Option<Ordering> {
        self.partial_cmp(&Value::from(*other))
    }
}
impl PartialOrd<f32> for Value {
    fn partial_cmp(&self, other: &f32) -> Option<Ordering> {
        self.partial_cmp(&Value::from(*other))
    }
}
impl PartialOrd<f64> for Value {
    fn partial_cmp(&self, other: &f64) -> Option<Ordering> {
        self.partial_cmp(&Value::from(*other))
    }
}
impl PartialOrd<u8> for Value {
    fn partial_cmp(&self, other: &u8) -> Option<Ordering> {
        self.partial_cmp(&Value::from(*other))
    }
}
impl PartialOrd<u16> for Value {
    fn partial_cmp(&self, other: &u16) -> Option<Ordering> {
        self.partial_cmp(&Value::from(*other))
    }
}
impl PartialOrd<u32> for Value {
    fn partial_cmp(&self, other: &u32) -> Option<Ordering> {
        self.partial_cmp(&Value::from(*other))
    }
}
impl PartialOrd<u64> for Value {
    fn partial_cmp(&self, other: &u64) -> Option<Ordering> {
        self.partial_cmp(&Value::from(*other))
    }
}
impl PartialOrd<u128> for Value {
    fn partial_cmp(&self, other: &u128) -> Option<Ordering> {
        self.partial_cmp(&Value::from(*other))
    }
}

impl PartialOrd<ID> for Value {
    fn partial_cmp(&self, other: &ID) -> Option<Ordering> {
        match self {
            Value::Id(id) => id.partial_cmp(other),
            Value::String(s) => Some(ID::from(s).partial_cmp(other)?),
            Value::U128(u) => Some(u.partial_cmp(other)?),
            _ => None,
        }
    }
}

impl PartialOrd<DateTime<Utc>> for Value {
    fn partial_cmp(&self, other: &DateTime<Utc>) -> Option<Ordering> {
        self.partial_cmp(&Value::from(*other))
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
                Value::Date(d) => d.serialize(serializer),
                Value::Id(id) => id.serialize(serializer),
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
                Value::Date(d) => serializer.serialize_newtype_variant("Value", 12, "Date", d),
                Value::Boolean(b) => {
                    serializer.serialize_newtype_variant("Value", 13, "Boolean", b)
                }
                Value::Id(id) => serializer.serialize_newtype_variant("Value", 14, "Id", id),
                Value::Array(a) => serializer.serialize_newtype_variant("Value", 15, "Array", a),
                Value::Object(obj) => {
                    serializer.serialize_newtype_variant("Value", 16, "Object", obj)
                }
                Value::Empty => serializer.serialize_unit_variant("Value", 17, "Empty"),
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

            /// Handles object values by recursively deserialising each key-value pair
            fn visit_map<A>(self, mut map: A) -> Result<Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut object = HashMap::new();
                while let Some((key, value)) = map.next_entry()? {
                    object.insert(key, value);
                }
                Ok(Value::Object(object))
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
                    12 => Ok(Value::Date(variant_data.newtype_variant()?)),
                    13 => Ok(Value::Boolean(variant_data.newtype_variant()?)),
                    14 => Ok(Value::Id(variant_data.newtype_variant()?)),
                    15 => Ok(Value::Array(variant_data.newtype_variant()?)),
                    16 => Ok(Value::Object(variant_data.newtype_variant()?)),
                    17 => {
                        variant_data.unit_variant()?;
                        Ok(Value::Empty)
                    }
                    _ => Err(serde::de::Error::invalid_value(
                        serde::de::Unexpected::Unsigned(variant_idx as u64),
                        &"variant index 0 through 17",
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
                    "U128", "Date", "Boolean", "Id", "Array", "Object", "Empty",
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
            Value::Date(d) => GenRef::Std(format!("{d:?}")),
            Value::Id(id) => GenRef::Literal(id.stringify()),
            Value::Array(_a) => unimplemented!(),
            Value::Object(_o) => unimplemented!(),
            Value::Empty => GenRef::Literal("".to_string()),
        }
    }
}

impl FilterValues for Value {
    #[inline]
    fn compare(&self, value: &Value, operator: Option<Operator>) -> bool {
        debug_println!("comparing value1: {:?}, value2: {:?}", self, value);
        let comparison = match (self, value) {
            (Value::Array(a1), Value::Array(a2)) => a1.iter().any(|a1_item| {
                a2.iter()
                    .any(|a2_item| a1_item.compare(a2_item, operator.clone()))
            }),
            (value, Value::Array(a)) => a
                .iter()
                .any(|a_item| value.compare(a_item, operator.clone())),
            (value1, value2) => match operator {
                Some(op) => op.execute(value1, value2),
                None => value1 == value2,
            },
        };
        debug_println!("comparison: {:?}", comparison);
        comparison
    }
}

impl From<Value> for i8 {
    fn from(val: Value) -> Self {
        match val {
            Value::I8(i) => i,
            Value::I16(i) => i as i8,
            Value::I32(i) => i as i8,
            Value::I64(i) => i as i8,
            Value::U8(i) => i as i8,
            Value::U16(i) => i as i8,
            Value::U32(i) => i as i8,
            Value::U64(i) => i as i8,
            Value::U128(i) => i as i8,
            Value::F32(i) => i as i8,
            Value::F64(i) => i as i8,
            Value::Boolean(i) => i as i8,
            Value::String(s) => s.parse::<i8>().unwrap(),
            _ => panic!("Value cannot be cast to i8"),
        }
    }
}

impl From<Value> for i16 {
    fn from(val: Value) -> Self {
        match val {
            Value::I16(i) => i,
            Value::I8(i) => i as i16,
            Value::I32(i) => i as i16,
            Value::I64(i) => i as i16,
            Value::U8(i) => i as i16,
            Value::U16(i) => i as i16,
            Value::U32(i) => i as i16,
            Value::U64(i) => i as i16,
            Value::U128(i) => i as i16,
            Value::F32(i) => i as i16,
            Value::F64(i) => i as i16,
            Value::Boolean(i) => i as i16,
            Value::String(s) => s.parse::<i16>().unwrap(),
            _ => panic!("Value cannot be cast to i16"),
        }
    }
}

impl From<Value> for i32 {
    fn from(val: Value) -> Self {
        match val {
            Value::I32(i) => i,
            Value::I8(i) => i as i32,
            Value::I16(i) => i as i32,
            Value::I64(i) => i as i32,
            Value::U8(i) => i as i32,
            Value::U16(i) => i as i32,
            Value::U32(i) => i as i32,
            Value::U64(i) => i as i32,
            Value::U128(i) => i as i32,
            Value::F32(i) => i as i32,
            Value::F64(i) => i as i32,
            Value::Boolean(i) => i as i32,
            Value::String(s) => s.parse::<i32>().unwrap(),
            _ => panic!("Value cannot be cast to i32"),
        }
    }
}

impl From<Value> for i64 {
    fn from(val: Value) -> Self {
        match val {
            Value::I64(i) => i,
            Value::I8(i) => i as i64,
            Value::I16(i) => i as i64,
            Value::I32(i) => i as i64,
            Value::U8(i) => i as i64,
            Value::U16(i) => i as i64,
            Value::U32(i) => i as i64,
            Value::U64(i) => i as i64,
            Value::U128(i) => i as i64,
            Value::F32(i) => i as i64,
            Value::F64(i) => i as i64,
            Value::Boolean(i) => i as i64,
            Value::String(s) => s.parse::<i64>().unwrap(),
            _ => panic!("Value cannot be cast to i64"),
        }
    }
}

impl From<Value> for u8 {
    fn from(val: Value) -> Self {
        match val {
            Value::U8(i) => i,
            Value::I8(i) => i as u8,
            Value::I16(i) => i as u8,
            Value::I32(i) => i as u8,
            Value::I64(i) => i as u8,
            Value::U16(i) => i as u8,
            Value::U32(i) => i as u8,
            Value::U64(i) => i as u8,
            Value::U128(i) => i as u8,
            Value::F32(i) => i as u8,
            Value::F64(i) => i as u8,
            Value::Boolean(i) => i as u8,
            Value::String(s) => s.parse::<u8>().unwrap(),
            _ => panic!("Value cannot be cast to u8"),
        }
    }
}

impl From<Value> for u16 {
    fn from(val: Value) -> Self {
        match val {
            Value::U16(i) => i,
            Value::I8(i) => i as u16,
            Value::I16(i) => i as u16,
            Value::I32(i) => i as u16,
            Value::I64(i) => i as u16,
            Value::U8(i) => i as u16,
            Value::U32(i) => i as u16,
            Value::U64(i) => i as u16,
            Value::U128(i) => i as u16,
            Value::F32(i) => i as u16,
            Value::F64(i) => i as u16,
            Value::Boolean(i) => i as u16,
            Value::String(s) => s.parse::<u16>().unwrap(),
            _ => panic!("Value cannot be cast to u16"),
        }
    }
}

impl From<Value> for u32 {
    fn from(val: Value) -> Self {
        match val {
            Value::U32(i) => i,
            Value::I8(i) => i as u32,
            Value::I16(i) => i as u32,
            Value::I32(i) => i as u32,
            Value::I64(i) => i as u32,
            Value::U8(i) => i as u32,
            Value::U16(i) => i as u32,
            Value::U64(i) => i as u32,
            Value::U128(i) => i as u32,
            Value::F32(i) => i as u32,
            Value::F64(i) => i as u32,
            Value::Boolean(i) => i as u32,
            Value::String(s) => s.parse::<u32>().unwrap(),
            _ => panic!("Value cannot be cast to u32"),
        }
    }
}

impl From<Value> for u64 {
    fn from(val: Value) -> Self {
        match val {
            Value::U64(i) => i,
            Value::I8(i) => i as u64,
            Value::I16(i) => i as u64,
            Value::I32(i) => i as u64,
            Value::U8(i) => i as u64,
            Value::U16(i) => i as u64,
            Value::U32(i) => i as u64,
            Value::U128(i) => i as u64,
            Value::F32(i) => i as u64,
            Value::F64(i) => i as u64,
            Value::Boolean(i) => i as u64,
            Value::String(s) => s.parse::<u64>().unwrap(),
            _ => panic!("Value cannot be cast to u64"),
        }
    }
}

impl From<Value> for u128 {
    fn from(val: Value) -> Self {
        match val {
            Value::U128(i) => i,
            Value::I8(i) => i as u128,
            Value::I16(i) => i as u128,
            Value::I32(i) => i as u128,
            Value::I64(i) => i as u128,
            Value::U8(i) => i as u128,
            Value::U16(i) => i as u128,
            Value::U32(i) => i as u128,
            Value::U64(i) => i as u128,
            Value::F32(i) => i as u128,
            Value::F64(i) => i as u128,
            Value::Boolean(i) => i as u128,
            Value::String(s) => s.parse::<u128>().unwrap(),
            _ => panic!("Value cannot be cast to u128"),
        }
    }
}

impl From<Value> for Date {
    fn from(val: Value) -> Self {
        match val {
            Value::String(s) => Date::new(&Value::String(s)).unwrap(),
            Value::I64(i) => Date::new(&Value::I64(i)).unwrap(),
            Value::U64(i) => Date::new(&Value::U64(i)).unwrap(),
            _ => panic!("Value cannot be cast to date"),
        }
    }
}
impl From<Value> for bool {
    fn from(val: Value) -> Self {
        match val {
            Value::Boolean(b) => b,
            _ => panic!("Value cannot be cast to boolean"),
        }
    }
}

impl From<Value> for ID {
    fn from(val: Value) -> Self {
        match val {
            Value::Id(id) => id,
            Value::String(s) => ID::from(s),
            Value::U128(i) => ID::from(i),
            _ => panic!("Value cannot be cast to id"),
        }
    }
}

impl From<Value> for Vec<Value> {
    fn from(val: Value) -> Self {
        match val {
            Value::Array(a) => a,
            _ => panic!("Value cannot be cast to array"),
        }
    }
}

impl From<Value> for HashMap<String, Value> {
    fn from(val: Value) -> Self {
        match val {
            Value::Object(o) => o,
            _ => panic!("Value cannot be cast to object"),
        }
    }
}

impl From<Value> for f32 {
    fn from(val: Value) -> Self {
        match val {
            Value::F32(f) => f,
            Value::F64(f) => f as f32,
            Value::I8(i) => i as f32,
            Value::I16(i) => i as f32,
            Value::I32(i) => i as f32,
            Value::I64(i) => i as f32,
            Value::U8(i) => i as f32,
            Value::U16(i) => i as f32,
            Value::U32(i) => i as f32,
            Value::U64(i) => i as f32,
            Value::U128(i) => i as f32,
            Value::String(s) => s.parse::<f32>().unwrap(),
            _ => panic!("Value cannot be cast to f32"),
        }
    }
}

impl From<Value> for f64 {
    fn from(val: Value) -> Self {
        match val {
            Value::F64(f) => f,
            Value::F32(f) => f as f64,
            Value::I8(i) => i as f64,
            Value::I16(i) => i as f64,
            Value::I32(i) => i as f64,
            Value::I64(i) => i as f64,
            Value::U8(i) => i as f64,
            Value::U16(i) => i as f64,
            Value::U32(i) => i as f64,
            Value::U64(i) => i as f64,
            Value::U128(i) => i as f64,
            Value::String(s) => s.parse::<f64>().unwrap(),
            _ => panic!("Value cannot be cast to f64"),
        }
    }
}

pub mod casting {
    use crate::helixc::parser::types::FieldType;

    use super::*;

    #[derive(Debug)]
    pub enum CastType {
        String,
        I8,
        I16,
        I32,
        I64,
        U8,
        U16,
        U32,
        U64,
        U128,
        F32,
        F64,
        Date,
        Boolean,
        Id,
        Array,
        Object,
        Empty,
    }

    pub fn cast(value: Value, cast_type: CastType) -> Value {
        match cast_type {
            CastType::String => Value::String(value.inner_stringify()),
            CastType::I8 => Value::I8(value.into()),
            CastType::I16 => Value::I16(value.into()),
            CastType::I32 => Value::I32(value.into()),
            CastType::I64 => Value::I64(value.into()),
            CastType::U8 => Value::U8(value.into()),
            CastType::U16 => Value::U16(value.into()),
            CastType::U32 => Value::U32(value.into()),
            CastType::U64 => Value::U64(value.into()),
            CastType::U128 => Value::U128(value.into()),
            CastType::F32 => Value::F32(value.into()),
            CastType::F64 => Value::F64(value.into()),
            CastType::Date => Value::Date(value.into()),
            CastType::Boolean => Value::Boolean(value.into()),
            CastType::Id => Value::Id(value.into()),
            CastType::Array => Value::Array(value.into()),
            CastType::Object => Value::Object(value.into()),
            CastType::Empty => Value::Empty,
        }
    }

    impl std::fmt::Display for CastType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                CastType::String => write!(f, "String"),
                CastType::I8 => write!(f, "I8"),
                CastType::I16 => write!(f, "I16"),
                CastType::I32 => write!(f, "I32"),
                CastType::I64 => write!(f, "I64"),
                CastType::U8 => write!(f, "U8"),
                CastType::U16 => write!(f, "U16"),
                CastType::U32 => write!(f, "U32"),
                CastType::U64 => write!(f, "U64"),
                CastType::U128 => write!(f, "U128"),
                CastType::F32 => write!(f, "F32"),
                CastType::F64 => write!(f, "F64"),
                CastType::Date => write!(f, "Date"),
                CastType::Boolean => write!(f, "Boolean"),
                CastType::Id => write!(f, "Id"),
                CastType::Array => write!(f, "Array"),
                CastType::Object => write!(f, "Object"),
                CastType::Empty => write!(f, "Empty"),
            }
        }
    }

    impl From<FieldType> for CastType {
        fn from(value: FieldType) -> Self {
            match value {
                FieldType::String => CastType::String,
                FieldType::I8 => CastType::I8,
                FieldType::I16 => CastType::I16,
                FieldType::I32 => CastType::I32,
                FieldType::I64 => CastType::I64,
                FieldType::U8 => CastType::U8,
                FieldType::U16 => CastType::U16,
                FieldType::U32 => CastType::U32,
                FieldType::U64 => CastType::U64,
                FieldType::U128 => CastType::U128,

                FieldType::F32 => CastType::F32,
                FieldType::F64 => CastType::F64,
                FieldType::Date => CastType::Date,
                FieldType::Boolean => CastType::Boolean,
                FieldType::Uuid => CastType::Id,
                FieldType::Array(_) => CastType::Array,
                FieldType::Object(_) => CastType::Object,
                _ => CastType::Empty,
            }
        }
    }
}

pub trait IntoPrimitive<T> {
    fn into_primitive(&self) -> &T;
}

impl IntoPrimitive<i8> for Value {
    fn into_primitive(&self) -> &i8 {
        match self {
            Value::I8(i) => i,
            _ => panic!("Value is not an i8"),
        }
    }
}

impl IntoPrimitive<i16> for Value {
    fn into_primitive(&self) -> &i16 {
        match self {
            Value::I16(i) => i,
            _ => panic!("Value is not an i16"),
        }
    }
}

impl IntoPrimitive<i32> for Value {
    fn into_primitive(&self) -> &i32 {
        match self {
            Value::I32(i) => i,
            _ => panic!("Value is not an i32"),
        }
    }
}

impl IntoPrimitive<i64> for Value {
    fn into_primitive(&self) -> &i64 {
        match self {
            Value::I64(i) => i,
            _ => panic!("Value is not an i64"),
        }
    }
}

impl IntoPrimitive<u8> for Value {
    fn into_primitive(&self) -> &u8 {
        match self {
            Value::U8(i) => i,
            _ => panic!("Value is not an u8"),
        }
    }
}

impl IntoPrimitive<u16> for Value {
    fn into_primitive(&self) -> &u16 {
        match self {
            Value::U16(i) => i,
            _ => panic!("Value is not an u16"),
        }
    }
}

impl IntoPrimitive<u32> for Value {
    fn into_primitive(&self) -> &u32 {
        match self {
            Value::U32(i) => i,
            _ => panic!("Value is not an u32"),
        }
    }
}

impl IntoPrimitive<u64> for Value {
    fn into_primitive(&self) -> &u64 {
        match self {
            Value::U64(i) => i,
            _ => panic!("Value is not an u64"),
        }
    }
}

impl IntoPrimitive<u128> for Value {
    fn into_primitive(&self) -> &u128 {
        match self {
            Value::U128(i) => i,
            _ => panic!("Value is not an u128"),
        }
    }
}

impl IntoPrimitive<f32> for Value {
    fn into_primitive(&self) -> &f32 {
        match self {
            Value::F32(i) => i,
            _ => panic!("Value is not an f32"),
        }
    }
}

impl IntoPrimitive<f64> for Value {
    fn into_primitive(&self) -> &f64 {
        match self {
            Value::F64(i) => i,
            _ => panic!("Value is not an f64"),
        }
    }
}

impl IntoPrimitive<bool> for Value {
    fn into_primitive(&self) -> &bool {
        match self {
            Value::Boolean(i) => i,
            _ => panic!("Value is not a boolean"),
        }
    }
}

impl IntoPrimitive<ID> for Value {
    fn into_primitive(&self) -> &ID {
        match self {
            Value::Id(i) => i,
            _ => panic!("Value is not an id"),
        }
    }
}

impl IntoPrimitive<Vec<Value>> for Value {
    fn into_primitive(&self) -> &Vec<Value> {
        match self {
            Value::Array(i) => i,
            _ => panic!("Value is not an array"),
        }
    }
}

impl IntoPrimitive<HashMap<String, Value>> for Value {
    fn into_primitive(&self) -> &HashMap<String, Value> {
        match self {
            Value::Object(i) => i,
            _ => panic!("Value is not an object"),
        }
    }
}

impl IntoPrimitive<Date> for Value {
    fn into_primitive(&self) -> &Date {
        match self {
            Value::Date(i) => i,
            _ => panic!("Value is not a date"),
        }
    }
}

#[test]
fn test_value_eq() {
    assert_eq!(Value::I64(1), Value::I64(1));
    assert_eq!(Value::U64(1), Value::U64(1));
    assert_eq!(Value::F64(1.0), Value::F64(1.0));
    assert_eq!(Value::I64(1), Value::U64(1));
    assert_eq!(Value::U64(1), Value::I64(1));
    assert_eq!(Value::I32(1), 1 as i32);
    assert_eq!(Value::U32(1), 1 as i32);
}
