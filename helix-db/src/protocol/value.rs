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
use std::borrow::Cow;
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::{self},
};
/// A flexible value type that can represent various property values in nodes and edges.
/// Handles both JSON and binary serialisation formats via custom implementaions of the Serialize and Deserialize traits.
#[derive(Clone, Debug, Default)]
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
    #[default]
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

    pub fn inner_str(&self) -> Cow<'_, str> {
        match self {
            Value::String(s) => Cow::Borrowed(s.as_str()),
            Value::F32(f) => Cow::Owned(f.to_string()),
            Value::F64(f) => Cow::Owned(f.to_string()),
            Value::I8(i) => Cow::Owned(i.to_string()),
            Value::I16(i) => Cow::Owned(i.to_string()),
            Value::I32(i) => Cow::Owned(i.to_string()),
            Value::I64(i) => Cow::Owned(i.to_string()),
            Value::U8(u) => Cow::Owned(u.to_string()),
            Value::U16(u) => Cow::Owned(u.to_string()),
            Value::U32(u) => Cow::Owned(u.to_string()),
            Value::U64(u) => Cow::Owned(u.to_string()),
            Value::U128(u) => Cow::Owned(u.to_string()),
            Value::Date(d) => Cow::Owned(d.to_string()),
            Value::Id(id) => Cow::Owned(id.stringify()),
            Value::Boolean(b) => Cow::Borrowed(if *b { "true" } else { "false" }),
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

    /// Checks if this value contains the needle value (as strings).
    /// Converts both values to their string representations and performs substring matching.
    pub fn contains(&self, needle: &str) -> bool {
        self.inner_str().contains(needle)
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
            (Value::Array(a1), Value::Array(a2)) => a1
                .iter()
                .any(|a1_item| a2.iter().any(|a2_item| a1_item.compare(a2_item, operator))),
            (value, Value::Array(a)) => a.iter().any(|a_item| value.compare(a_item, operator)),
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

impl IntoPrimitive<String> for Value {
    fn into_primitive(&self) -> &String {
        match self {
            Value::String(s) => s,
            _ => panic!("Value is not a string"),
        }
    }
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

impl Value {
    #[inline(always)]
    pub fn as_f64(&self) -> f64 {
        *self.into_primitive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Value Creation and From Implementations
    // ============================================================================

    #[test]
    fn test_value_from_primitives() {
        assert!(matches!(Value::from("test"), Value::String(_)));
        assert!(matches!(
            Value::from(String::from("test")),
            Value::String(_)
        ));
        assert!(matches!(Value::from(true), Value::Boolean(true)));
        assert!(matches!(Value::from(42i8), Value::I8(42)));
        assert!(matches!(Value::from(42i16), Value::I16(42)));
        assert!(matches!(Value::from(42i32), Value::I32(42)));
        assert!(matches!(Value::from(42i64), Value::I64(42)));
        assert!(matches!(Value::from(42u8), Value::U8(42)));
        assert!(matches!(Value::from(42u16), Value::U16(42)));
        assert!(matches!(Value::from(42u32), Value::U32(42)));
        assert!(matches!(Value::from(42u64), Value::U64(42)));
        assert!(matches!(Value::from(42u128), Value::U128(42)));
        assert!(matches!(Value::from(3.14f32), Value::F32(_)));
        assert!(matches!(Value::from(3.14f64), Value::F64(_)));
    }

    #[test]
    fn test_value_from_string_trims_quotes() {
        let val = Value::from("\"quoted\"");
        assert_eq!(val, Value::String("quoted".to_string()));

        let val2 = Value::from(String::from("\"test\""));
        assert_eq!(val2, Value::String("test".to_string()));
    }

    #[test]
    fn test_value_from_vec() {
        let vec_vals = vec![Value::I32(1), Value::I32(2), Value::I32(3)];
        let val = Value::from(vec_vals.clone());
        assert!(matches!(val, Value::Array(_)));
        if let Value::Array(arr) = val {
            assert_eq!(arr.len(), 3);
        }

        // Test From<Vec<primitive>>
        let vec_i64 = vec![1i64, 2i64, 3i64];
        let val = Value::from(vec_i64);
        assert!(matches!(val, Value::Array(_)));

        let vec_str = vec![String::from("a"), String::from("b")];
        let val = Value::from(vec_str);
        assert!(matches!(val, Value::Array(_)));
    }

    #[test]
    fn test_value_from_usize() {
        let val = Value::from(42usize);
        // Should be U64 on 64-bit systems
        if cfg!(target_pointer_width = "64") {
            assert!(matches!(val, Value::U64(42)));
        } else {
            assert!(matches!(val, Value::U128(42)));
        }
    }

    #[test]
    #[ignore]
    fn test_value_from_datetime() {
        let dt = Utc::now();
        let val = Value::from(dt);
        // Now returns Value::Date instead of Value::String
        assert!(matches!(val, Value::Date(_)));
        if let Value::Date(d) = val {
            // Should be RFC3339 format when converted to string
            let s = d.to_rfc3339();
            assert!(s.contains('T'));
            assert!(s.contains('Z') || s.contains('+'));
        }
    }

    // ============================================================================
    // Equality Tests (PartialEq)
    // ============================================================================

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

    #[test]
    fn test_value_cross_type_numeric_equality() {
        // Integer cross-type equality
        assert_eq!(Value::I8(42), Value::I16(42));
        assert_eq!(Value::I8(42), Value::I32(42));
        assert_eq!(Value::U8(42), Value::U16(42));
        assert_eq!(Value::U8(42), Value::I32(42));

        // Float cross-type equality (use value that's exactly representable in both f32 and f64)
        assert_eq!(Value::F32(2.0), Value::F64(2.0));

        // Integer to float equality
        assert_eq!(Value::I32(42), Value::F64(42.0));
        assert_eq!(Value::U64(100), Value::F32(100.0));
    }

    #[test]
    fn test_value_string_equality() {
        let val = Value::String("test".to_string());
        assert_eq!(val, Value::String("test".to_string()));
        assert_eq!(val, String::from("test"));
        assert_eq!(val, "test");
        assert_ne!(val, "other");
    }

    #[test]
    fn test_value_boolean_equality() {
        assert_eq!(Value::Boolean(true), Value::Boolean(true));
        assert_eq!(Value::Boolean(true), true);
        assert_eq!(Value::Boolean(false), false);
        assert_ne!(Value::Boolean(true), Value::Boolean(false));
    }

    #[test]
    fn test_value_array_equality() {
        let arr1 = Value::Array(vec![Value::I32(1), Value::I32(2)]);
        let arr2 = Value::Array(vec![Value::I32(1), Value::I32(2)]);
        let arr3 = Value::Array(vec![Value::I32(1), Value::I32(3)]);

        assert_eq!(arr1, arr2);
        assert_ne!(arr1, arr3);
    }

    #[test]
    fn test_value_empty_equality() {
        assert_eq!(Value::Empty, Value::Empty);
        assert_ne!(Value::Empty, Value::I32(0));
        assert_ne!(Value::Empty, Value::String(String::new()));
    }

    // ============================================================================
    // Ordering Tests (Ord, PartialOrd)
    // ============================================================================

    #[test]
    fn test_value_ordering_integers() {
        assert!(Value::I32(1) < Value::I32(2));
        assert!(Value::I32(2) > Value::I32(1));
        assert!(Value::I32(1) == Value::I32(1));

        // Cross-type integer ordering
        assert!(Value::I8(10) < Value::I32(20));
        assert!(Value::U8(5) < Value::I16(10));
    }

    #[test]
    fn test_value_ordering_floats() {
        assert!(Value::F64(1.5) < Value::F64(2.5));
        assert!(Value::F32(3.14) > Value::F32(2.71));
    }

    #[test]
    fn test_value_ordering_strings() {
        assert!(Value::String("apple".to_string()) < Value::String("banana".to_string()));
        assert!(Value::String("xyz".to_string()) > Value::String("abc".to_string()));
    }

    #[test]
    fn test_value_ordering_empty() {
        // Empty is always less than other values
        assert!(Value::Empty < Value::I32(0));
        assert!(Value::Empty < Value::String(String::new()));
        assert!(Value::Empty < Value::Boolean(false));
        assert_eq!(Value::Empty.cmp(&Value::Empty), Ordering::Equal);
    }

    #[test]
    fn test_value_ordering_mixed_types() {
        // Non-comparable types should return Equal
        assert_eq!(
            Value::String("test".to_string()).cmp(&Value::I32(42)),
            Ordering::Equal
        );
        assert_eq!(Value::Boolean(true).cmp(&Value::F64(3.14)), Ordering::Equal);
    }

    #[test]
    fn test_value_ordering_u128_edge_cases() {
        // Test U128 values that exceed i128::MAX
        let large_u128 = u128::MAX;
        let small_u128 = 100u128;

        assert!(Value::U128(small_u128) < Value::U128(large_u128));
        assert!(Value::U128(large_u128) > Value::U128(small_u128));
    }

    // ============================================================================
    // Value Methods
    // ============================================================================

    #[test]
    fn test_inner_stringify() {
        assert_eq!(Value::String("test".to_string()).inner_stringify(), "test");
        assert_eq!(Value::I32(42).inner_stringify(), "42");
        assert_eq!(Value::F64(3.14).inner_stringify(), "3.14");
        assert_eq!(Value::Boolean(true).inner_stringify(), "true");
        assert_eq!(Value::U64(100).inner_stringify(), "100");
    }

    #[test]
    fn test_inner_stringify_array() {
        let arr = Value::Array(vec![Value::I32(1), Value::I32(2), Value::I32(3)]);
        let result = arr.inner_stringify();
        assert_eq!(result, "1 2 3");
    }

    #[test]
    fn test_inner_stringify_object() {
        let mut map = HashMap::new();
        map.insert("key1".to_string(), Value::I32(1));
        map.insert("key2".to_string(), Value::I32(2));

        let obj = Value::Object(map);
        let result = obj.inner_stringify();
        // Order may vary, but should contain both key-value pairs
        assert!(result.contains("key1") && result.contains("1"));
        assert!(result.contains("key2") && result.contains("2"));
    }

    #[test]
    #[should_panic(expected = "Not primitive")]
    fn test_inner_stringify_empty_panics() {
        Value::Empty.inner_stringify();
    }

    #[test]
    fn test_to_variant_string() {
        assert_eq!(
            Value::String("test".to_string()).to_variant_string(),
            "String"
        );
        assert_eq!(Value::I32(42).to_variant_string(), "I32");
        assert_eq!(Value::F64(3.14).to_variant_string(), "F64");
        assert_eq!(Value::Boolean(true).to_variant_string(), "Boolean");
        assert_eq!(Value::Empty.to_variant_string(), "Empty");
        assert_eq!(Value::Array(vec![]).to_variant_string(), "Array");
        assert_eq!(Value::Object(HashMap::new()).to_variant_string(), "Object");
    }

    #[test]
    fn test_as_str() {
        let val = Value::String("test".to_string());
        assert_eq!(val.as_str(), "test");
    }

    #[test]
    #[should_panic(expected = "Not a string")]
    fn test_as_str_panics_on_non_string() {
        Value::I32(42).as_str();
    }

    // ============================================================================
    // Serialization/Deserialization
    // ============================================================================

    #[test]
    fn test_json_serialization() {
        // JSON should serialize without enum variant names
        let val = Value::I32(42);
        let json = sonic_rs::to_string(&val).unwrap();
        assert_eq!(json, "42");

        let val = Value::String("test".to_string());
        let json = sonic_rs::to_string(&val).unwrap();
        assert_eq!(json, "\"test\"");

        let val = Value::Boolean(true);
        let json = sonic_rs::to_string(&val).unwrap();
        assert_eq!(json, "true");
    }

    #[test]
    fn test_json_deserialization() {
        let val: Value = sonic_rs::from_str("42").unwrap();
        assert_eq!(val, Value::I64(42)); // JSON integers default to I64

        let val: Value = sonic_rs::from_str("\"test\"").unwrap();
        assert_eq!(val, Value::String("test".to_string()));

        let val: Value = sonic_rs::from_str("true").unwrap();
        assert_eq!(val, Value::Boolean(true));

        let val: Value = sonic_rs::from_str("3.14").unwrap();
        assert!(matches!(val, Value::F64(_)));
    }

    #[test]
    fn test_json_array_serialization() {
        let arr = Value::Array(vec![Value::I32(1), Value::I32(2), Value::I32(3)]);
        let json = sonic_rs::to_string(&arr).unwrap();
        assert_eq!(json, "[1,2,3]");
    }

    #[test]
    fn test_json_object_serialization() {
        let mut map = HashMap::new();
        map.insert("key".to_string(), Value::String("value".to_string()));
        let obj = Value::Object(map);
        let json = sonic_rs::to_string(&obj).unwrap();
        assert!(json.contains("\"key\""));
        assert!(json.contains("\"value\""));
    }

    #[test]
    fn test_bincode_serialization_roundtrip() {
        let test_values = vec![
            Value::String("test".to_string()),
            Value::I32(42),
            Value::F64(3.14),
            Value::Boolean(true),
            Value::U128(u128::MAX),
            Value::Empty,
            Value::Array(vec![Value::I32(1), Value::I32(2)]),
        ];

        for val in test_values {
            let encoded = bincode::serialize(&val).unwrap();
            let decoded: Value = bincode::deserialize(&encoded).unwrap();
            assert_eq!(val, decoded);
        }
    }
    // ============================================================================
    // Type Conversions (Into implementations)
    // ============================================================================

    #[test]
    fn test_value_into_primitives() {
        let val = Value::I32(42);
        let i: i32 = val.into();
        assert_eq!(i, 42);

        let val = Value::F64(3.14);
        let f: f64 = val.into();
        assert_eq!(f, 3.14);

        let val = Value::Boolean(true);
        let b: bool = val.into();
        assert_eq!(b, true);

        let val = Value::String("test".to_string());
        let s: String = val.into();
        assert_eq!(s, "test");
    }

    #[test]
    fn test_value_into_cross_type_conversion() {
        // I32 to I64
        let val = Value::I32(42);
        let i: i64 = val.into();
        assert_eq!(i, 42);

        // U8 to U32
        let val = Value::U8(255);
        let u: u32 = val.into();
        assert_eq!(u, 255);

        // I32 to F32
        let val = Value::I32(42);
        let f: f32 = val.into();
        assert_eq!(f, 42.0);
    }

    #[test]
    fn test_value_string_parsing_conversion() {
        let val = Value::String("42".to_string());
        let i: i32 = val.into();
        assert_eq!(i, 42);

        let val = Value::String("3.14".to_string());
        let f: f64 = val.into();
        assert_eq!(f, 3.14);
    }

    #[test]
    #[should_panic(expected = "Value is not a string")]
    fn test_value_into_string_panics_on_non_string() {
        let val = Value::I32(42);
        let _: String = val.into();
    }

    #[test]
    #[should_panic(expected = "Value cannot be cast to boolean")]
    fn test_value_into_bool_panics_on_non_boolean() {
        let val = Value::I32(1);
        let _: bool = val.into();
    }

    #[test]
    fn test_value_into_array() {
        let arr = vec![Value::I32(1), Value::I32(2)];
        let val = Value::Array(arr.clone());
        let result: Vec<Value> = val.into();
        assert_eq!(result, arr);
    }

    #[test]
    #[should_panic(expected = "Value cannot be cast to array")]
    fn test_value_into_array_panics_on_non_array() {
        let val = Value::I32(42);
        let _: Vec<Value> = val.into();
    }

    // ============================================================================
    // IntoPrimitive Trait
    // ============================================================================

    #[test]
    fn test_into_primitive() {
        let val = Value::I32(42);
        let i: &i32 = val.into_primitive();
        assert_eq!(*i, 42);

        let val = Value::Boolean(true);
        let b: &bool = val.into_primitive();
        assert_eq!(*b, true);

        let val = Value::String("test".to_string());
        let s = val.as_str();
        assert_eq!(s, "test");
    }

    #[test]
    #[should_panic(expected = "Value is not an i32")]
    fn test_into_primitive_panics_on_wrong_type() {
        let val = Value::I64(42);
        let _: &i32 = val.into_primitive();
    }

    // ============================================================================
    // Edge Cases and UTF-8
    // ============================================================================

    #[test]
    fn test_value_utf8_strings() {
        let utf8_strings = vec![
            "Hello",
            "世界",   // Chinese
            "🚀🌟",   // Emojis
            "Привет", // Russian
            "مرحبا",  // Arabic
            "Ñoño",   // Spanish with tildes
        ];

        for s in utf8_strings {
            let val = Value::String(s.to_string());
            assert_eq!(val.inner_stringify(), s);

            // Test serialization roundtrip
            let json = sonic_rs::to_string(&val).unwrap();
            let decoded: Value = sonic_rs::from_str(&json).unwrap();
            assert_eq!(val, decoded);
        }
    }

    #[test]
    fn test_value_large_numbers() {
        let val = Value::U128(u128::MAX);
        assert_eq!(val, Value::U128(u128::MAX));

        let val = Value::I64(i64::MAX);
        assert_eq!(val, Value::I64(i64::MAX));

        let val = Value::I64(i64::MIN);
        assert_eq!(val, Value::I64(i64::MIN));
    }

    #[test]
    fn test_value_nested_arrays() {
        let inner = vec![Value::I32(1), Value::I32(2)];
        let outer = Value::Array(vec![
            Value::Array(inner.clone()),
            Value::Array(inner.clone()),
        ]);

        assert!(matches!(outer, Value::Array(_)));
        if let Value::Array(arr) = outer {
            assert_eq!(arr.len(), 2);
            assert!(matches!(arr[0], Value::Array(_)));
        }
    }

    #[test]
    fn test_value_nested_objects() {
        let mut inner = HashMap::new();
        inner.insert("inner_key".to_string(), Value::I32(42));

        let mut outer = HashMap::new();
        outer.insert("outer_key".to_string(), Value::Object(inner));

        let val = Value::Object(outer);
        assert!(matches!(val, Value::Object(_)));
    }

    #[test]
    fn test_value_empty_collections() {
        let empty_arr = Value::Array(vec![]);
        assert_eq!(empty_arr.inner_stringify(), "");

        let empty_obj = Value::Object(HashMap::new());
        assert_eq!(empty_obj.inner_stringify(), "");
    }

    // ============================================================================
    // Casting Module
    // ============================================================================

    #[test]
    fn test_cast_to_string() {
        let val = Value::I32(42);
        let result = casting::cast(val, casting::CastType::String);
        assert_eq!(result, Value::String("42".to_string()));
    }

    #[test]
    fn test_cast_between_numeric_types() {
        let val = Value::I32(42);
        let result = casting::cast(val, casting::CastType::I64);
        assert_eq!(result, Value::I64(42));

        let val = Value::F64(3.14);
        let result = casting::cast(val, casting::CastType::F32);
        assert!(matches!(result, Value::F32(_)));
    }

    #[test]
    fn test_cast_to_empty() {
        let val = Value::I32(42);
        let result = casting::cast(val, casting::CastType::Empty);
        assert_eq!(result, Value::Empty);
    }

    // ============================================================================
    // Additional Edge Case Tests for inner_str()
    // ============================================================================

    #[test]
    fn test_inner_str_string_returns_borrowed() {
        let val = Value::String("test".to_string());
        let cow = val.inner_str();
        assert!(matches!(cow, std::borrow::Cow::Borrowed(_)));
        assert_eq!(&*cow, "test");
    }

    #[test]
    fn test_inner_str_numeric_returns_owned() {
        let val = Value::I32(42);
        let cow = val.inner_str();
        assert!(matches!(cow, std::borrow::Cow::Owned(_)));
        assert_eq!(&*cow, "42");
    }

    #[test]
    fn test_inner_str_boolean_returns_borrowed() {
        let val_true = Value::Boolean(true);
        let cow_true = val_true.inner_str();
        assert!(matches!(cow_true, std::borrow::Cow::Borrowed(_)));
        assert_eq!(&*cow_true, "true");

        let val_false = Value::Boolean(false);
        let cow_false = val_false.inner_str();
        assert!(matches!(cow_false, std::borrow::Cow::Borrowed(_)));
        assert_eq!(&*cow_false, "false");
    }

    #[test]
    fn test_inner_str_all_numeric_types() {
        assert_eq!(&*Value::I8(-42).inner_str(), "-42");
        assert_eq!(&*Value::I16(-1000).inner_str(), "-1000");
        assert_eq!(&*Value::I32(-100000).inner_str(), "-100000");
        assert_eq!(&*Value::I64(-1000000000).inner_str(), "-1000000000");
        assert_eq!(&*Value::U8(255).inner_str(), "255");
        assert_eq!(&*Value::U16(65535).inner_str(), "65535");
        assert_eq!(&*Value::U32(4294967295).inner_str(), "4294967295");
        assert_eq!(&*Value::U64(18446744073709551615).inner_str(), "18446744073709551615");
        assert_eq!(&*Value::U128(u128::MAX).inner_str(), u128::MAX.to_string());
    }

    #[test]
    fn test_inner_str_float_precision() {
        let val = Value::F64(3.141592653589793);
        let cow = val.inner_str();
        assert!(cow.starts_with("3.14159"));
    }

    #[test]
    #[should_panic(expected = "Not primitive")]
    fn test_inner_str_empty_panics() {
        Value::Empty.inner_str();
    }

    #[test]
    #[should_panic(expected = "Not primitive")]
    fn test_inner_str_array_panics() {
        Value::Array(vec![Value::I32(1)]).inner_str();
    }

    // ============================================================================
    // contains() Method Tests
    // ============================================================================

    #[test]
    fn test_contains_string_with_substring() {
        let val = Value::String("hello world".to_string());
        assert!(val.contains("world"));
        assert!(val.contains("hello"));
        assert!(val.contains("o w"));
        assert!(val.contains(""));
    }

    #[test]
    fn test_contains_string_without_substring() {
        let val = Value::String("hello world".to_string());
        assert!(!val.contains("xyz"));
        assert!(!val.contains("World")); // Case sensitive
    }

    #[test]
    fn test_contains_numeric_converted_to_string() {
        let val = Value::I32(12345);
        assert!(val.contains("123"));
        assert!(val.contains("345"));
        assert!(val.contains("12345"));
        assert!(!val.contains("999"));
    }

    #[test]
    fn test_contains_boolean() {
        let val_true = Value::Boolean(true);
        assert!(val_true.contains("true"));
        assert!(val_true.contains("ru"));
        assert!(!val_true.contains("false"));

        let val_false = Value::Boolean(false);
        assert!(val_false.contains("false"));
        assert!(val_false.contains("als"));
    }

    // ============================================================================
    // to_variant_string() Complete Coverage
    // ============================================================================

    #[test]
    fn test_to_variant_string_all_variants() {
        assert_eq!(Value::String("".to_string()).to_variant_string(), "String");
        assert_eq!(Value::F32(0.0).to_variant_string(), "F32");
        assert_eq!(Value::F64(0.0).to_variant_string(), "F64");
        assert_eq!(Value::I8(0).to_variant_string(), "I8");
        assert_eq!(Value::I16(0).to_variant_string(), "I16");
        assert_eq!(Value::I32(0).to_variant_string(), "I32");
        assert_eq!(Value::I64(0).to_variant_string(), "I64");
        assert_eq!(Value::U8(0).to_variant_string(), "U8");
        assert_eq!(Value::U16(0).to_variant_string(), "U16");
        assert_eq!(Value::U32(0).to_variant_string(), "U32");
        assert_eq!(Value::U64(0).to_variant_string(), "U64");
        assert_eq!(Value::U128(0).to_variant_string(), "U128");
        assert_eq!(Value::Boolean(false).to_variant_string(), "Boolean");
        assert_eq!(Value::Empty.to_variant_string(), "Empty");
        assert_eq!(Value::Array(vec![]).to_variant_string(), "Array");
        assert_eq!(Value::Object(HashMap::new()).to_variant_string(), "Object");
    }

    // ============================================================================
    // Float Edge Cases (NaN, Infinity)
    // ============================================================================

    #[test]
    fn test_float_nan_ordering() {
        let nan = Value::F64(f64::NAN);
        let num = Value::F64(1.0);
        // NaN comparisons should return Equal as per the implementation
        assert_eq!(nan.cmp(&num), Ordering::Equal);
        assert_eq!(nan.cmp(&nan), Ordering::Equal);
    }

    #[test]
    fn test_float_infinity() {
        let inf = Value::F64(f64::INFINITY);
        let neg_inf = Value::F64(f64::NEG_INFINITY);
        let num = Value::F64(1000.0);

        assert!(inf > num);
        assert!(neg_inf < num);
        assert!(inf > neg_inf);
    }

    #[test]
    fn test_float_negative_zero() {
        let pos_zero = Value::F64(0.0);
        let neg_zero = Value::F64(-0.0);
        // IEEE 754: 0.0 == -0.0
        assert_eq!(pos_zero, neg_zero);
    }

    // ============================================================================
    // inner_stringify() Complete Coverage
    // ============================================================================

    #[test]
    fn test_inner_stringify_all_numeric_types() {
        assert_eq!(Value::I8(-128).inner_stringify(), "-128");
        assert_eq!(Value::I8(127).inner_stringify(), "127");
        assert_eq!(Value::I16(-32768).inner_stringify(), "-32768");
        assert_eq!(Value::I32(i32::MIN).inner_stringify(), i32::MIN.to_string());
        assert_eq!(Value::I64(i64::MAX).inner_stringify(), i64::MAX.to_string());
        assert_eq!(Value::U8(0).inner_stringify(), "0");
        assert_eq!(Value::U8(255).inner_stringify(), "255");
        assert_eq!(Value::U16(65535).inner_stringify(), "65535");
        assert_eq!(Value::U32(u32::MAX).inner_stringify(), u32::MAX.to_string());
        assert_eq!(Value::U64(u64::MAX).inner_stringify(), u64::MAX.to_string());
        assert_eq!(Value::U128(u128::MAX).inner_stringify(), u128::MAX.to_string());
    }

    // ============================================================================
    // Numeric Cross-Type Comparison Edge Cases
    // ============================================================================

    #[test]
    fn test_numeric_cross_type_ordering() {
        // Compare different integer types
        assert!(Value::I8(10) < Value::I64(100));
        assert!(Value::U8(50) > Value::I8(25));
        assert!(Value::U64(1000) > Value::I16(500));
    }

    #[test]
    fn test_u128_greater_than_i128_max() {
        let large = Value::U128((i128::MAX as u128) + 1);
        let small = Value::U128(100);
        // When one U128 exceeds i128::MAX, special handling applies
        assert!(large > small);
    }

    #[test]
    fn test_u128_comparison_both_large() {
        let a = Value::U128(u128::MAX);
        let b = Value::U128(u128::MAX - 1);
        assert!(a > b);
    }

    // ============================================================================
    // Default Trait
    // ============================================================================

    #[test]
    fn test_value_default_is_empty() {
        let val: Value = Default::default();
        assert!(matches!(val, Value::Empty));
    }

    // ============================================================================
    // Clone Trait
    // ============================================================================

    #[test]
    fn test_value_clone() {
        let original = Value::String("test".to_string());
        let cloned = original.clone();
        assert_eq!(original, cloned);

        let arr_original = Value::Array(vec![Value::I32(1), Value::I32(2)]);
        let arr_cloned = arr_original.clone();
        assert_eq!(arr_original, arr_cloned);
    }
}
