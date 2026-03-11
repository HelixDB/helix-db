use crate::{
    helix_engine::{
        types::VectorError,
        vector_core::{vector::HVector, vector_without_data::VectorWithoutData},
    },
    utils::properties::{ImmutablePropertiesMap, ImmutablePropertiesMapDeSeed},
};
use bincode::Options;
use serde::de::{DeserializeSeed, Visitor};
use std::fmt;

struct OptionPropertiesMapDeSeed<'arena> {
    arena: &'arena bumpalo::Bump,
}

impl<'de, 'arena> DeserializeSeed<'de> for OptionPropertiesMapDeSeed<'arena> {
    type Value = Option<ImmutablePropertiesMap<'arena>>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct OptionPropertiesVisitor<'arena> {
            arena: &'arena bumpalo::Bump,
        }

        impl<'de, 'arena> Visitor<'de> for OptionPropertiesVisitor<'arena> {
            type Value = Option<ImmutablePropertiesMap<'arena>>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("Option<ImmutablePropertiesMap>")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(None)
            }

            fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                ImmutablePropertiesMapDeSeed { arena: self.arena }
                    .deserialize(deserializer)
                    .map(Some)
            }
        }

        deserializer.deserialize_option(OptionPropertiesVisitor { arena: self.arena })
    }
}

pub struct VectorDeSeed<'txn, 'arena> {
    pub arena: &'arena bumpalo::Bump,
    pub raw_vector_data: &'txn [u8],
    pub id: u128,
}

impl<'de, 'txn, 'arena> DeserializeSeed<'de> for VectorDeSeed<'txn, 'arena> {
    type Value = HVector<'arena>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct VectorVisitor<'txn, 'arena> {
            arena: &'arena bumpalo::Bump,
            raw_vector_data: &'txn [u8],
            id: u128,
        }

        impl<'de, 'txn, 'arena> Visitor<'de> for VectorVisitor<'txn, 'arena> {
            type Value = HVector<'arena>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct HVector")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let label_string: &'de str = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let label = self.arena.alloc_str(label_string);

                let version: u8 = seq.next_element()?.unwrap_or(0);
                let deleted: bool = seq.next_element()?.unwrap_or(false);

                let properties = seq
                    .next_element_seed(OptionPropertiesMapDeSeed { arena: self.arena })?
                    .ok_or_else(|| serde::de::Error::custom("Expected properties field"))?;

                let data = HVector::cast_raw_vector_data(self.arena, self.raw_vector_data);

                Ok(HVector {
                    id: self.id,
                    label,
                    deleted,
                    version,
                    level: 0,
                    distance: None,
                    data,
                    properties,
                })
            }
        }

        deserializer.deserialize_struct(
            "HVector",
            &["label", "version", "deleted", "properties"],
            VectorVisitor {
                arena: self.arena,
                raw_vector_data: self.raw_vector_data,
                id: self.id,
            },
        )
    }
}

pub struct LegacyVectorDeSeed<'txn, 'arena> {
    pub arena: &'arena bumpalo::Bump,
    pub raw_vector_data: &'txn [u8],
    pub id: u128,
}

impl<'de, 'txn, 'arena> DeserializeSeed<'de> for LegacyVectorDeSeed<'txn, 'arena> {
    type Value = HVector<'arena>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LegacyVectorVisitor<'txn, 'arena> {
            arena: &'arena bumpalo::Bump,
            raw_vector_data: &'txn [u8],
            id: u128,
        }

        impl<'de, 'txn, 'arena> Visitor<'de> for LegacyVectorVisitor<'txn, 'arena> {
            type Value = HVector<'arena>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("legacy struct HVector")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let label_string: &'de str = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let label = self.arena.alloc_str(label_string);

                let version: u8 = seq.next_element()?.unwrap_or(0);
                let deleted: bool = seq.next_element()?.unwrap_or(false);

                let _legacy_level: usize = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::custom("Expected legacy level field"))?;

                let properties = seq
                    .next_element_seed(OptionPropertiesMapDeSeed { arena: self.arena })?
                    .ok_or_else(|| serde::de::Error::custom("Expected properties field"))?;

                let data = HVector::cast_raw_vector_data(self.arena, self.raw_vector_data);

                Ok(HVector {
                    id: self.id,
                    label,
                    deleted,
                    version,
                    level: 0,
                    distance: None,
                    data,
                    properties,
                })
            }
        }

        deserializer.deserialize_struct(
            "HVector",
            &["label", "version", "deleted", "level", "properties"],
            LegacyVectorVisitor {
                arena: self.arena,
                raw_vector_data: self.raw_vector_data,
                id: self.id,
            },
        )
    }
}

pub struct VectoWithoutDataDeSeed<'arena> {
    pub arena: &'arena bumpalo::Bump,
    pub id: u128,
}

impl<'de, 'arena> DeserializeSeed<'de> for VectoWithoutDataDeSeed<'arena> {
    type Value = VectorWithoutData<'arena>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct VectorWithoutDataVisitor<'arena> {
            arena: &'arena bumpalo::Bump,
            id: u128,
        }

        impl<'de, 'arena> Visitor<'de> for VectorWithoutDataVisitor<'arena> {
            type Value = VectorWithoutData<'arena>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct VectorWithoutData")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let label_string: &'de str = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let label = self.arena.alloc_str(label_string);

                let version: u8 = seq.next_element()?.unwrap_or(0);
                let deleted: bool = seq.next_element()?.unwrap_or(false);

                let properties = seq
                    .next_element_seed(OptionPropertiesMapDeSeed { arena: self.arena })?
                    .ok_or_else(|| serde::de::Error::custom("Expected properties field"))?;

                Ok(VectorWithoutData {
                    id: self.id,
                    label,
                    version,
                    deleted,
                    level: 0,
                    properties,
                })
            }
        }

        deserializer.deserialize_struct(
            "VectorWithoutData",
            &["label", "version", "deleted", "properties"],
            VectorWithoutDataVisitor {
                arena: self.arena,
                id: self.id,
            },
        )
    }
}

pub struct LegacyVectoWithoutDataDeSeed<'arena> {
    pub arena: &'arena bumpalo::Bump,
    pub id: u128,
}

impl<'de, 'arena> DeserializeSeed<'de> for LegacyVectoWithoutDataDeSeed<'arena> {
    type Value = VectorWithoutData<'arena>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LegacyVectorWithoutDataVisitor<'arena> {
            arena: &'arena bumpalo::Bump,
            id: u128,
        }

        impl<'de, 'arena> Visitor<'de> for LegacyVectorWithoutDataVisitor<'arena> {
            type Value = VectorWithoutData<'arena>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("legacy struct VectorWithoutData")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let label_string: &'de str = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let label = self.arena.alloc_str(label_string);

                let version: u8 = seq.next_element()?.unwrap_or(0);
                let deleted: bool = seq.next_element()?.unwrap_or(false);

                let _legacy_level: usize = seq
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::custom("Expected legacy level field"))?;

                let properties = seq
                    .next_element_seed(OptionPropertiesMapDeSeed { arena: self.arena })?
                    .ok_or_else(|| serde::de::Error::custom("Expected properties field"))?;

                Ok(VectorWithoutData {
                    id: self.id,
                    label,
                    version,
                    deleted,
                    level: 0,
                    properties,
                })
            }
        }

        deserializer.deserialize_struct(
            "VectorWithoutData",
            &["label", "version", "deleted", "level", "properties"],
            LegacyVectorWithoutDataVisitor {
                arena: self.arena,
                id: self.id,
            },
        )
    }
}

pub fn hvector_from_bincode_bytes<'arena>(
    arena: &'arena bumpalo::Bump,
    properties: Option<&[u8]>,
    raw_vector_data: &[u8],
    id: u128,
) -> Result<HVector<'arena>, VectorError> {
    let properties = properties
        .ok_or_else(|| VectorError::ConversionError("Vector properties missing".to_string()))?;

    bincode::options()
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_seed(
            VectorDeSeed {
                arena,
                id,
                raw_vector_data,
            },
            properties,
        )
        .or_else(|_| {
            bincode::options()
                .with_fixint_encoding()
                .allow_trailing_bytes()
                .deserialize_seed(
                    LegacyVectorDeSeed {
                        arena,
                        id,
                        raw_vector_data,
                    },
                    properties,
                )
        })
        .map_err(|e| VectorError::ConversionError(format!("Error deserializing vector: {e}")))
}

pub fn vector_without_data_from_bincode_bytes<'arena>(
    arena: &'arena bumpalo::Bump,
    properties: &[u8],
    id: u128,
) -> Result<VectorWithoutData<'arena>, VectorError> {
    bincode::options()
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_seed(VectoWithoutDataDeSeed { arena, id }, properties)
        .or_else(|_| {
            bincode::options()
                .with_fixint_encoding()
                .allow_trailing_bytes()
                .deserialize_seed(LegacyVectoWithoutDataDeSeed { arena, id }, properties)
        })
        .map_err(|e| VectorError::ConversionError(format!("Error deserializing vector: {e}")))
}
