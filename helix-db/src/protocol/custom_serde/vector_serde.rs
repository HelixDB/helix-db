use crate::{
    helix_engine::{
        types::VectorError,
        vector_core::{vector::HVector, vector_without_data::VectorWithoutData},
    },
    protocol::value::Value,
    utils::properties::ImmutablePropertiesMap,
};
use bincode::Options;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
struct StoredVectorMetadata {
    label: String,
    version: u8,
    deleted: bool,
    properties: Option<HashMap<String, Value>>,
}

#[derive(Deserialize)]
struct StoredVectorMetadataLegacy {
    label: String,
    version: u8,
    deleted: bool,
    #[serde(rename = "level")]
    _level: usize,
    properties: Option<HashMap<String, Value>>,
}

fn deserialize_metadata(bytes: &[u8]) -> Result<StoredVectorMetadata, VectorError> {
    let options = bincode::options()
        .with_fixint_encoding()
        .allow_trailing_bytes();

    options
        .deserialize::<StoredVectorMetadata>(bytes)
        .or_else(|_| {
            options
                .deserialize::<StoredVectorMetadataLegacy>(bytes)
                .map(|legacy| {
                    let StoredVectorMetadataLegacy {
                        label,
                        version,
                        deleted,
                        _level: _,
                        properties,
                    } = legacy;

                    StoredVectorMetadata {
                        label,
                        version,
                        deleted,
                        properties,
                    }
                })
        })
        .map_err(|e| VectorError::ConversionError(format!("Error deserializing vector: {e}")))
}

fn alloc_properties<'arena>(
    arena: &'arena bumpalo::Bump,
    properties: Option<HashMap<String, Value>>,
) -> Option<ImmutablePropertiesMap<'arena>> {
    properties.map(|properties| {
        let len = properties.len();
        ImmutablePropertiesMap::new(
            len,
            properties.into_iter().map(|(key, value)| {
                let key: &'arena str = arena.alloc_str(&key);
                (key, value)
            }),
            arena,
        )
    })
}

pub fn hvector_from_bincode_bytes<'arena>(
    arena: &'arena bumpalo::Bump,
    properties: Option<&[u8]>,
    raw_vector_data: &[u8],
    id: u128,
) -> Result<HVector<'arena>, VectorError> {
    let properties = properties
        .ok_or_else(|| VectorError::ConversionError("Vector properties missing".to_string()))?;
    let metadata = deserialize_metadata(properties)?;
    let label = arena.alloc_str(&metadata.label);
    let data = HVector::cast_raw_vector_data(arena, raw_vector_data);

    Ok(HVector {
        id,
        label,
        deleted: metadata.deleted,
        version: metadata.version,
        level: 0,
        distance: None,
        data,
        properties: alloc_properties(arena, metadata.properties),
    })
}

pub fn vector_without_data_from_bincode_bytes<'arena>(
    arena: &'arena bumpalo::Bump,
    properties: &[u8],
    id: u128,
) -> Result<VectorWithoutData<'arena>, VectorError> {
    let metadata = deserialize_metadata(properties)?;
    let label = arena.alloc_str(&metadata.label);

    Ok(VectorWithoutData {
        id,
        label,
        version: metadata.version,
        deleted: metadata.deleted,
        level: 0,
        properties: alloc_properties(arena, metadata.properties),
    })
}
