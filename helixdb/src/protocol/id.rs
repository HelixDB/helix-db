use core::fmt;
use std::ops::Deref;

use serde::{
    de::{DeserializeSeed, VariantAccess, Visitor},
    ser::Error,
    Deserializer, Serializer,
};
use sonic_rs::{Deserialize, Serialize};
// pub type ID = String;
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(transparent)]
// #[serde(transparent)]
pub struct ID(u128);
impl ID {
    pub fn inner(&self) -> u128 {
        self.0
    }
}

impl Serialize for ID {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u128(self.0)
    }
}

struct IDVisitor;

impl<'de> Visitor<'de> for IDVisitor {
    type Value = ID;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid UUID")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match uuid::Uuid::parse_str(v) {
            Ok(uuid) => Ok(ID(uuid.as_u128())),
            Err(e) => Err(E::custom(e.to_string())),
        }
    }
}
impl<'de> Deserialize<'de> for ID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(IDVisitor)
    }
}

impl Deref for ID {
    type Target = u128;
    #[inline]
    fn deref(&self) -> &u128 {
        &self.0
    }
}

impl From<u128> for ID {
    fn from(id: u128) -> Self {
        ID(id)
    }
}

impl From<ID> for u128 {
    fn from(id: ID) -> Self {
        id.0
    }
}
