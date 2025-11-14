use crate::{
    helix_engine::types::VectorError,
    protocol::{custom_serde::vector_serde::VectoWithoutDataDeSeed, value::Value},
    utils::{id::uuid_str_from_buf, properties::ImmutablePropertiesMap},
};
use bincode::Options;
use core::fmt;
use serde::{Serialize, ser::SerializeMap};
use std::fmt::Debug;
// TODO: make this generic over the type of encoding (f32, f64, etc)
// TODO: use const param to set dimension
// TODO: set level as u8

#[repr(C, align(16))]
#[derive(Clone, Copy)]
pub struct VectorWithoutData<'arena> {
    /// The id of the HVector
    pub id: u128,
    /// The label of the HVector
    pub label: &'arena str,
    /// the version of the vector
    pub version: u8,
    /// whether the vector is deleted
    pub deleted: bool,
    /// The level of the HVector
    pub level: usize,

    /// The properties of the HVector
    pub properties: Option<ImmutablePropertiesMap<'arena>>,
}

// Custom Serialize implementation to conditionally include id field
// For JSON serialization, the id field is included, but for bincode it is skipped
impl<'arena> Serialize for VectorWithoutData<'arena> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        // Check if this is a human-readable format (like JSON)
        if serializer.is_human_readable() {
            // Include id for JSON serialization
            let mut buffer = [0u8; 36];
            let mut state = serializer.serialize_map(Some(
                6 + self.properties.as_ref().map(|p| p.len()).unwrap_or(0),
            ))?;
            state.serialize_entry("id", uuid_str_from_buf(self.id, &mut buffer))?;
            state.serialize_entry("label", self.label)?;
            state.serialize_entry("version", &self.version)?;
            state.serialize_entry("deleted", &self.deleted)?;
            state.serialize_entry("level", &self.level)?;
            if let Some(properties) = &self.properties {
                for (key, value) in properties.iter() {
                    state.serialize_entry(key, value)?;
                }
            }
            state.end()
        } else {
            // Skip id for bincode serialization
            let mut state = serializer.serialize_struct("VectorWithoutData", 5)?;
            state.serialize_field("label", self.label)?;
            state.serialize_field("version", &self.version)?;
            state.serialize_field("deleted", &self.deleted)?;
            state.serialize_field("level", &self.level)?;
            state.serialize_field("properties", &self.properties)?;
            state.end()
        }
    }
}

impl Debug for VectorWithoutData<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ \nid: {},\nlevel: {} }}",
            uuid::Uuid::from_u128(self.id),
            self.level,
        )
    }
}

impl<'arena> VectorWithoutData<'arena> {
    #[inline(always)]
    pub fn from_properties(
        id: u128,
        label: &'arena str,
        level: usize,
        properties: ImmutablePropertiesMap<'arena>,
    ) -> Self {
        VectorWithoutData {
            id,
            label,
            version: 1,
            level,
            properties: Some(properties),
            deleted: false,
        }
    }

    pub fn from_bincode_bytes<'txn>(
        arena: &'arena bumpalo::Bump,
        properties: &'txn [u8],
        id: u128,
    ) -> Result<Self, VectorError> {
        bincode::options()
            .with_fixint_encoding()
            .allow_trailing_bytes()
            .deserialize_seed(VectoWithoutDataDeSeed { arena, id }, properties)
            .map_err(|e| VectorError::ConversionError(format!("Error deserializing vector: {e}")))
    }

    #[inline(always)]
    pub fn to_bincode_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }
    /// Returns the id of the HVector
    #[inline(always)]
    pub fn get_id(&self) -> u128 {
        self.id
    }

    /// Returns the level of the HVector
    #[inline(always)]
    pub fn get_level(&self) -> usize {
        self.level
    }

    #[inline(always)]
    pub fn get_label(&self) -> &'arena str {
        self.label
    }

    #[inline(always)]
    pub fn get_property(&self, key: &str) -> Option<&'arena Value> {
        self.properties.as_ref().and_then(|value| value.get(key))
    }

    pub fn id(&self) -> &u128 {
        &self.id
    }

    pub fn label(&self) -> &'arena str {
        self.label
    }
}

impl PartialEq for VectorWithoutData<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for VectorWithoutData<'_> {}
