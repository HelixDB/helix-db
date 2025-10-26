//! ID type for nodes and edges.
//!
//! This is a wrapper around a 128-bit UUID.
//!
//! It is used to deserialize a string UUID into a 128-bit integer so that
//! it can be serialized properly for use with LMDB.
//!
//! The ID type can be dereferenced to a 128-bit integer for use with other functions that expect a 128-bit integer.

use core::fmt;
use std::ops::Deref;

use serde::{Deserializer, Serializer, de::Visitor};
use sonic_rs::{Deserialize, Serialize};

/// A wrapper around a 128-bit UUID.
///
/// This is used to represent the ID of a node or edge.
#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord)]
#[repr(transparent)]
/// The inner ID.
pub struct ID(u128);
impl ID {
    pub fn inner(&self) -> u128 {
        self.0
    }

    pub fn id(&self) -> u128 {
        self.0
    }

    pub fn stringify(&self) -> String {
        uuid::Uuid::from_u128(self.0).to_string()
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

    /// Visits a string UUID and parses it into a 128-bit integer.
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

/// Deserializes a string UUID into a 128-bit integer.
impl<'de> Deserialize<'de> for ID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(IDVisitor)
    }
}

/// Dereferences the ID to a 128-bit integer.
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

impl From<String> for ID {
    fn from(id: String) -> Self {
        ID(uuid::Uuid::parse_str(&id).unwrap().as_u128())
    }
}
impl From<&String> for ID {
    fn from(id: &String) -> Self {
        ID(uuid::Uuid::parse_str(id).unwrap().as_u128())
    }
}

impl From<&str> for ID {
    fn from(id: &str) -> Self {
        ID(uuid::Uuid::parse_str(id).unwrap().as_u128())
    }
}

impl From<ID> for u128 {
    fn from(id: ID) -> Self {
        id.0
    }
}

/// Generates a new v6 UUID.
///
/// This is used to generate a new UUID for a node or edge.
/// The UUID is generated using the current time and a random number.
#[inline(always)]
pub fn v6_uuid() -> u128 {
    uuid::Uuid::now_v6(&[1, 2, 3, 4, 5, 6]).as_u128()
}

#[cfg(test)]
mod tests {
    use sonic_rs::json;

    use super::*;

    #[test]
    fn test_uuid_deserialization() {
        let uuid = json!({ "id": "1f07ae4b-e354-6660-b5f0-fd3ce8bc4b49" });

        #[derive(Deserialize)]
        struct IDWrapper {
            id: ID,
        }

        let deserialized: IDWrapper = sonic_rs::from_value(&uuid).unwrap();
        assert_eq!(
            deserialized.id.stringify(),
            "1f07ae4b-e354-6660-b5f0-fd3ce8bc4b49"
        );
    }

    #[test]
    fn test_uuid_serialization() {
        let uuid = "1f07ae4b-e354-6660-b5f0-fd3ce8bc4b49";
        let id = ID::from(uuid);

        let serialized = sonic_rs::to_string(&id).unwrap();

        let uuid_u128 = str::parse::<u128>(&serialized).unwrap();
        let uuid = uuid::Uuid::from_u128(uuid_u128);

        assert_eq!(uuid.to_string(), "1f07ae4b-e354-6660-b5f0-fd3ce8bc4b49");
    }
}
