//! Shared bounded primitives for canonical value codecs.

/// Frozen value framing version.
pub(crate) const INDEX_V2_VALUE_VERSION: u8 = 0x01;

/// Value framing version for V3 split-bearing rows.
pub(crate) const INDEX_V3_SPLIT_VALUE_VERSION: u8 = 0x02;

use bytes::{BufMut, Bytes};

use crate::config::{RangeIndexDirection, TextAnalyzerKind};
use crate::encoding::error::EncodingError;
use crate::encoding::indexes::range::RangeIndexDirection as StorageRangeIndexDirection;
use crate::encoding::v2::keys::scope::{DataScope, TenantId};
use crate::encoding::v2::keys::{CanonicalSecondaryValue, SecondaryEntryLane};
use crate::index_lifecycle::work::{BlobRef, SplitPruning, SplitRef, SPLIT_PRUNING_BLOOM_WORDS};
use crate::index_lifecycle::{
    ActiveVectorCodecV2, ClaimSequence, CosineNormPolicyV2, IndexComponent, IndexCursor,
    IndexElementKind, IndexEntityId, IndexGenerationId, IndexId, IndexIdentity,
    IndexIdentityFamily, IndexOperationBlocker, IndexOperationId, IndexOperationRevision,
    IndexRevision, OperationClaim, OperationCounters, PhysicalGeneration, TextLogicalVersion,
    TextManifestRevision, TextPartition, ValidatedDynamicIndexDefinition,
    ValidatedSecondaryIndexDefinition, ValidatedTextIndexDefinition,
    ValidatedVectorIndexDefinition, VectorGenerationDescriptor, VectorPhysicalIndexId,
    VectorPhysicalLayout, VectorRoutingLayoutV2, VectorScoreSemanticV2, WriterEpoch,
};
use crate::search::vector::VectorDistanceMetric;

pub(super) const ENCODING_TYPE_LEN: usize = core::mem::size_of::<u8>();
pub(super) const U8_LEN: usize = core::mem::size_of::<u8>();
pub(super) const U16_LEN: usize = core::mem::size_of::<u16>();
pub(super) const U32_LEN: usize = core::mem::size_of::<u32>();
pub(super) const U64_LEN: usize = core::mem::size_of::<u64>();
pub(super) const UUID_LEN: usize = 16;
pub(super) const HASH_LEN: usize = 32;
pub(super) const MAX_LENGTH_DELIMITED_FIELD: usize = 16 * 1024 * 1024;
pub(super) const MAX_COLLECTION_ITEMS: usize = u16::MAX as usize;

pub(super) struct ValueEncoder {
    bytes: Vec<u8>,
}

impl ValueEncoder {
    pub(super) fn with_header(kind: u8) -> Self {
        Self::with_versioned_header(INDEX_V2_VALUE_VERSION, kind)
    }

    pub(super) fn with_versioned_header(version: u8, kind: u8) -> Self {
        let mut bytes = Vec::new();
        bytes.put_u8(version);
        bytes.put_u8(kind);
        Self { bytes }
    }

    pub(super) fn finish(self) -> Bytes {
        Bytes::from(self.bytes)
    }

    pub(super) fn put_u8(&mut self, value: u8) {
        self.bytes.put_u8(value);
    }

    pub(super) fn put_u16(&mut self, value: u16) {
        self.bytes.put_u16(value);
    }

    pub(super) fn put_u32(&mut self, value: u32) {
        self.bytes.put_u32(value);
    }

    pub(super) fn put_u64(&mut self, value: u64) {
        self.bytes.put_u64(value);
    }

    pub(super) fn put_f32(&mut self, value: f32) {
        assert!(value.is_finite(), "validated V2 float must be finite");
        self.bytes.put_u32(value.to_bits());
    }

    pub(super) fn put_bool(&mut self, value: bool) {
        self.put_u8(u8::from(value));
    }

    pub(super) fn put_bytes(&mut self, value: &[u8]) {
        self.put_u32(u32::try_from(value.len()).expect("bounded V2 field fits u32"));
        self.bytes.put_slice(value);
    }

    pub(super) fn put_uuid(&mut self, value: &[u8; UUID_LEN]) {
        self.bytes.put_slice(value);
    }

    pub(super) fn put_raw(&mut self, value: &[u8]) {
        self.bytes.put_slice(value);
    }
}

pub(super) struct ValueDecoder<'a> {
    version: u8,
    kind: u8,
    remaining: &'a [u8],
}

impl<'a> ValueDecoder<'a> {
    pub(super) fn new(value: &'a [u8]) -> Result<Self, EncodingError> {
        Self::with_supported_versions(value, &[INDEX_V2_VALUE_VERSION])
    }

    pub(super) fn with_supported_versions(
        value: &'a [u8],
        supported_versions: &[u8],
    ) -> Result<Self, EncodingError> {
        const HEADER_LEN: usize = U8_LEN + U8_LEN;
        const VERSION_OFFSET: usize = 0;
        const KIND_OFFSET: usize = VERSION_OFFSET + U8_LEN;
        const BODY_OFFSET: usize = KIND_OFFSET + U8_LEN;
        if value.len() < HEADER_LEN {
            return Err(EncodingError::BufferTooShort {
                expected: HEADER_LEN,
                actual: value.len(),
            });
        }
        if !supported_versions.contains(&value[VERSION_OFFSET]) {
            return Err(EncodingError::Custom(format!(
                "unsupported V2 value version {:#04x}",
                value[VERSION_OFFSET]
            )));
        }
        Ok(Self {
            version: value[VERSION_OFFSET],
            kind: value[KIND_OFFSET],
            remaining: &value[BODY_OFFSET..BODY_OFFSET + value.len() - BODY_OFFSET],
        })
    }

    pub(super) const fn version(&self) -> u8 {
        self.version
    }

    pub(super) const fn kind(&self) -> u8 {
        self.kind
    }

    pub(super) fn take_raw(&mut self, len: usize) -> Result<&'a [u8], EncodingError> {
        const FIELD_OFFSET: usize = 0;
        if self.remaining.len() < len {
            return Err(EncodingError::BufferTooShort {
                expected: len,
                actual: self.remaining.len(),
            });
        }
        let value = &self.remaining[FIELD_OFFSET..FIELD_OFFSET + len];
        self.remaining = &self.remaining[FIELD_OFFSET + len..FIELD_OFFSET + self.remaining.len()];
        Ok(value)
    }

    pub(super) fn take_array<const LEN: usize>(&mut self) -> Result<[u8; LEN], EncodingError> {
        Ok(self
            .take_raw(LEN)?
            .try_into()
            .expect("fixed V2 decoder slice matches requested length"))
    }

    pub(super) fn take_u8(&mut self) -> Result<u8, EncodingError> {
        const BYTE_OFFSET: usize = 0;
        Ok(self.take_raw(U8_LEN)?[BYTE_OFFSET])
    }

    pub(super) fn take_u16(&mut self) -> Result<u16, EncodingError> {
        Ok(u16::from_be_bytes(self.take_array::<U16_LEN>()?))
    }

    pub(super) fn take_u32(&mut self) -> Result<u32, EncodingError> {
        Ok(u32::from_be_bytes(self.take_array::<U32_LEN>()?))
    }

    pub(super) fn take_u64(&mut self) -> Result<u64, EncodingError> {
        Ok(u64::from_be_bytes(self.take_array::<U64_LEN>()?))
    }

    pub(super) fn take_f32(&mut self) -> Result<f32, EncodingError> {
        let value = f32::from_bits(self.take_u32()?);
        if !value.is_finite() {
            return Err(EncodingError::Custom("V2 float must be finite".to_string()));
        }
        Ok(value)
    }

    pub(super) fn take_bool(&mut self) -> Result<bool, EncodingError> {
        match self.take_u8()? {
            0x00 => Ok(false),
            0x01 => Ok(true),
            unknown => Err(EncodingError::Custom(format!(
                "noncanonical V2 boolean {unknown:#04x}"
            ))),
        }
    }

    pub(super) fn take_bytes(&mut self, maximum: usize) -> Result<Bytes, EncodingError> {
        let len = self.take_u32()? as usize;
        if len > maximum {
            return Err(EncodingError::Custom(format!(
                "V2 field length {len} exceeds maximum {maximum}"
            )));
        }
        Ok(Bytes::copy_from_slice(self.take_raw(len)?))
    }

    pub(super) fn take_string(&mut self, maximum: usize) -> Result<String, EncodingError> {
        let bytes = self.take_bytes(maximum)?;
        Ok(std::str::from_utf8(&bytes)?.to_owned())
    }

    pub(super) fn take_option<T>(
        &mut self,
        decode: impl FnOnce(&mut Self) -> Result<T, EncodingError>,
    ) -> Result<Option<T>, EncodingError> {
        match self.take_u8()? {
            0x00 => Ok(None),
            0x01 => decode(self).map(Some),
            unknown => Err(EncodingError::Custom(format!(
                "noncanonical V2 option tag {unknown:#04x}"
            ))),
        }
    }

    pub(super) const fn is_finished(&self) -> bool {
        self.remaining.is_empty()
    }

    pub(super) fn finish(self) -> Result<(), EncodingError> {
        if !self.remaining.is_empty() {
            return Err(EncodingError::Custom(format!(
                "V2 value has {} trailing bytes",
                self.remaining.len()
            )));
        }
        Ok(())
    }
}

pub(super) fn put_option<T>(
    encoder: &mut ValueEncoder,
    value: Option<&T>,
    encode: impl FnOnce(&mut ValueEncoder, &T),
) {
    match value {
        Some(value) => {
            encoder.put_u8(0x01);
            encode(encoder, value);
        }
        None => encoder.put_u8(0x00),
    }
}

pub(super) fn put_index_id(encoder: &mut ValueEncoder, value: IndexId) {
    encoder.put_u64(value.get());
}

pub(super) fn take_index_id(decoder: &mut ValueDecoder<'_>) -> Result<IndexId, EncodingError> {
    IndexId::new(decoder.take_u64()?).map_err(model_error)
}

pub(super) fn put_generation(encoder: &mut ValueEncoder, value: IndexGenerationId) {
    encoder.put_u64(value.get());
}

pub(super) fn take_generation(
    decoder: &mut ValueDecoder<'_>,
) -> Result<IndexGenerationId, EncodingError> {
    IndexGenerationId::new(decoder.take_u64()?).map_err(model_error)
}

pub(super) fn put_revision(encoder: &mut ValueEncoder, value: IndexRevision) {
    encoder.put_u64(value.get());
}

pub(super) fn take_revision(
    decoder: &mut ValueDecoder<'_>,
) -> Result<IndexRevision, EncodingError> {
    IndexRevision::new(decoder.take_u64()?).map_err(model_error)
}

pub(super) fn put_operation_revision(encoder: &mut ValueEncoder, value: IndexOperationRevision) {
    encoder.put_u64(value.get());
}

pub(super) fn take_operation_revision(
    decoder: &mut ValueDecoder<'_>,
) -> Result<IndexOperationRevision, EncodingError> {
    IndexOperationRevision::new(decoder.take_u64()?).map_err(model_error)
}

pub(super) fn put_operation_id(encoder: &mut ValueEncoder, value: IndexOperationId) {
    encoder.put_uuid(value.as_bytes());
}

pub(super) fn take_operation_id(
    decoder: &mut ValueDecoder<'_>,
) -> Result<IndexOperationId, EncodingError> {
    IndexOperationId::from_bytes(decoder.take_array::<UUID_LEN>()?).map_err(model_error)
}

pub(super) fn put_identity(encoder: &mut ValueEncoder, identity: &IndexIdentity) {
    encoder.put_u8(identity.family() as u8);
    encoder.put_u8(identity.element_kind() as u8);
    encoder.put_bytes(identity.label().as_str().as_bytes());
    encoder.put_bytes(identity.property().as_str().as_bytes());
}

pub(super) fn take_identity(
    decoder: &mut ValueDecoder<'_>,
) -> Result<IndexIdentity, EncodingError> {
    let family = match decoder.take_u8()? {
        0x01 => IndexIdentityFamily::SecondaryEquality,
        0x02 => IndexIdentityFamily::SecondaryRange,
        0x03 => IndexIdentityFamily::Vector,
        0x04 => IndexIdentityFamily::Text,
        unknown => return Err(unknown_discriminant("identity family", unknown)),
    };
    let element_kind = take_element_kind(decoder)?;
    let label = IndexComponent::try_new(
        "label",
        decoder.take_string(crate::index_lifecycle::INDEX_COMPONENT_MAX_LEN)?,
    )
    .map_err(model_error)?;
    let property = IndexComponent::try_new(
        "property",
        decoder.take_string(crate::index_lifecycle::INDEX_COMPONENT_MAX_LEN)?,
    )
    .map_err(model_error)?;
    Ok(IndexIdentity::new(family, element_kind, label, property))
}

pub(super) fn put_definition(
    encoder: &mut ValueEncoder,
    definition: &ValidatedDynamicIndexDefinition,
) {
    match definition {
        ValidatedDynamicIndexDefinition::Secondary(definition) => {
            encoder.put_u8(0x01);
            match definition {
                ValidatedSecondaryIndexDefinition::NodeEquality {
                    label,
                    property,
                    unique,
                } => {
                    encoder.put_u8(0x01);
                    encoder.put_bytes(label.as_str().as_bytes());
                    encoder.put_bytes(property.as_str().as_bytes());
                    encoder.put_bool(*unique);
                }
                ValidatedSecondaryIndexDefinition::NodeRange {
                    label,
                    property,
                    direction,
                } => {
                    encoder.put_u8(0x02);
                    encoder.put_bytes(label.as_str().as_bytes());
                    encoder.put_bytes(property.as_str().as_bytes());
                    put_range_direction(encoder, *direction);
                }
                ValidatedSecondaryIndexDefinition::EdgeEquality { label, property } => {
                    encoder.put_u8(0x03);
                    encoder.put_bytes(label.as_str().as_bytes());
                    encoder.put_bytes(property.as_str().as_bytes());
                }
                ValidatedSecondaryIndexDefinition::EdgeRange {
                    label,
                    property,
                    direction,
                } => {
                    encoder.put_u8(0x04);
                    encoder.put_bytes(label.as_str().as_bytes());
                    encoder.put_bytes(property.as_str().as_bytes());
                    put_range_direction(encoder, *direction);
                }
            }
        }
        ValidatedDynamicIndexDefinition::Vector(definition) => {
            encoder.put_u8(0x02);
            put_element_kind(encoder, definition.element_kind());
            encoder.put_bytes(definition.label().as_str().as_bytes());
            encoder.put_bytes(definition.property().as_str().as_bytes());
            put_option(
                encoder,
                definition.tenant_property(),
                |encoder, property| encoder.put_bytes(property.as_str().as_bytes()),
            );
            encoder.put_u32(definition.dimension());
            put_metric(encoder, definition.metric());
            encoder.put_u8(definition.codec() as u8);
            encoder.put_u32(definition.m());
            encoder.put_u32(definition.m0());
            encoder.put_u32(definition.ef_construction());
            encoder.put_f32(definition.ml());
            encoder.put_u32(definition.simhash_threshold());
            encoder.put_f32(definition.sampling_ratio());
            encoder.put_bool(definition.adaptive_enabled());
            encoder.put_f32(definition.adaptive_failure_probability());
        }
        ValidatedDynamicIndexDefinition::Text(definition) => {
            encoder.put_u8(0x03);
            put_element_kind(encoder, definition.element_kind());
            encoder.put_bytes(definition.label().as_str().as_bytes());
            encoder.put_bytes(definition.property().as_str().as_bytes());
            put_option(
                encoder,
                definition.tenant_property(),
                |encoder, property| encoder.put_bytes(property.as_str().as_bytes()),
            );
            encoder.put_u8(match definition.analyzer() {
                TextAnalyzerKind::Standard => 0x01,
                TextAnalyzerKind::StandardStemEn => 0x02,
                TextAnalyzerKind::WhitespaceLowercase => 0x03,
            });
            encoder.put_bool(definition.positions_enabled());
        }
    }
}

pub(super) fn take_definition(
    decoder: &mut ValueDecoder<'_>,
) -> Result<ValidatedDynamicIndexDefinition, EncodingError> {
    match decoder.take_u8()? {
        0x01 => {
            let secondary = match decoder.take_u8()? {
                0x01 => ValidatedSecondaryIndexDefinition::NodeEquality {
                    label: take_component(decoder, "label")?,
                    property: take_component(decoder, "property")?,
                    unique: decoder.take_bool()?,
                },
                0x02 => ValidatedSecondaryIndexDefinition::NodeRange {
                    label: take_component(decoder, "label")?,
                    property: take_component(decoder, "property")?,
                    direction: take_range_direction(decoder)?,
                },
                0x03 => ValidatedSecondaryIndexDefinition::EdgeEquality {
                    label: take_component(decoder, "label")?,
                    property: take_component(decoder, "property")?,
                },
                0x04 => ValidatedSecondaryIndexDefinition::EdgeRange {
                    label: take_component(decoder, "label")?,
                    property: take_component(decoder, "property")?,
                    direction: take_range_direction(decoder)?,
                },
                unknown => return Err(unknown_discriminant("secondary definition", unknown)),
            };
            Ok(ValidatedDynamicIndexDefinition::Secondary(secondary))
        }
        0x02 => {
            let element_kind = take_element_kind(decoder)?;
            let label = decoder.take_string(crate::index_lifecycle::INDEX_COMPONENT_MAX_LEN)?;
            let property = decoder.take_string(crate::index_lifecycle::INDEX_COMPONENT_MAX_LEN)?;
            let tenant_property = decoder.take_option(|decoder| {
                decoder.take_string(crate::index_lifecycle::INDEX_COMPONENT_MAX_LEN)
            })?;
            let dimension = decoder.take_u32()?;
            let metric = take_metric(decoder)?;
            if decoder.take_u8()? != ActiveVectorCodecV2::F32V1 as u8 {
                return Err(EncodingError::Custom(
                    "unsupported V2 production vector codec".to_string(),
                ));
            }
            let definition = ValidatedVectorIndexDefinition::try_new(
                element_kind,
                label,
                property,
                tenant_property,
                dimension,
                metric,
                decoder.take_u32()?,
                decoder.take_u32()?,
                decoder.take_u32()?,
                decoder.take_f32()?,
                decoder.take_u32()?,
                decoder.take_f32()?,
                decoder.take_bool()?,
                decoder.take_f32()?,
            )
            .map_err(model_error)?;
            Ok(ValidatedDynamicIndexDefinition::Vector(definition))
        }
        0x03 => {
            let element_kind = take_element_kind(decoder)?;
            let label = decoder.take_string(crate::index_lifecycle::INDEX_COMPONENT_MAX_LEN)?;
            let property = decoder.take_string(crate::index_lifecycle::INDEX_COMPONENT_MAX_LEN)?;
            let tenant_property = decoder.take_option(|decoder| {
                decoder.take_string(crate::index_lifecycle::INDEX_COMPONENT_MAX_LEN)
            })?;
            let analyzer = match decoder.take_u8()? {
                0x01 => TextAnalyzerKind::Standard,
                0x02 => TextAnalyzerKind::StandardStemEn,
                0x03 => TextAnalyzerKind::WhitespaceLowercase,
                unknown => return Err(unknown_discriminant("text analyzer", unknown)),
            };
            Ok(ValidatedDynamicIndexDefinition::Text(
                ValidatedTextIndexDefinition::try_new(
                    element_kind,
                    label,
                    property,
                    tenant_property,
                    analyzer,
                    decoder.take_bool()?,
                )
                .map_err(model_error)?,
            ))
        }
        unknown => Err(unknown_discriminant("definition", unknown)),
    }
}

pub(super) fn put_physical_generation(encoder: &mut ValueEncoder, physical: &PhysicalGeneration) {
    match physical {
        PhysicalGeneration::Secondary { generation } => {
            encoder.put_u8(0x01);
            put_generation(encoder, *generation);
        }
        PhysicalGeneration::Vector {
            generation,
            layout,
            descriptor,
        } => {
            encoder.put_u8(match descriptor.routing_layout() {
                VectorRoutingLayoutV2::LegacyHnsw => 0x02,
                VectorRoutingLayoutV2::SimHashDirectoryV1 => 0x04,
            });
            put_generation(encoder, *generation);
            match layout {
                VectorPhysicalLayout::Unpartitioned { physical_index_id } => {
                    encoder.put_u8(0x01);
                    encoder.put_u64(physical_index_id.get());
                }
                VectorPhysicalLayout::Partitioned => encoder.put_u8(0x02),
            }
            put_vector_descriptor_semantics(encoder, *descriptor);
        }
        PhysicalGeneration::Text { generation } => {
            encoder.put_u8(0x03);
            put_generation(encoder, *generation);
        }
    }
}

pub(super) fn take_physical_generation(
    decoder: &mut ValueDecoder<'_>,
) -> Result<PhysicalGeneration, EncodingError> {
    match decoder.take_u8()? {
        0x01 => Ok(PhysicalGeneration::Secondary {
            generation: take_generation(decoder)?,
        }),
        kind @ (0x02 | 0x04) => Ok(PhysicalGeneration::Vector {
            generation: take_generation(decoder)?,
            layout: match decoder.take_u8()? {
                0x01 => VectorPhysicalLayout::Unpartitioned {
                    physical_index_id: VectorPhysicalIndexId::new(decoder.take_u64()?)
                        .map_err(model_error)?,
                },
                0x02 => VectorPhysicalLayout::Partitioned,
                unknown => return Err(unknown_discriminant("vector physical layout", unknown)),
            },
            descriptor: take_vector_descriptor(
                decoder,
                if kind == 0x02 {
                    VectorRoutingLayoutV2::LegacyHnsw
                } else {
                    VectorRoutingLayoutV2::SimHashDirectoryV1
                },
            )?,
        }),
        0x03 => Ok(PhysicalGeneration::Text {
            generation: take_generation(decoder)?,
        }),
        unknown => Err(unknown_discriminant("physical generation", unknown)),
    }
}

fn put_vector_descriptor_semantics(
    encoder: &mut ValueEncoder,
    descriptor: VectorGenerationDescriptor,
) {
    encoder.put_u32(descriptor.dimension());
    put_metric(encoder, descriptor.metric());
    encoder.put_u8(descriptor.codec() as u8);
    encoder.put_u8(descriptor.score_semantic() as u8);
    encoder.put_u8(descriptor.cosine_norm_policy() as u8);
}

fn take_vector_descriptor(
    decoder: &mut ValueDecoder<'_>,
    routing_layout: VectorRoutingLayoutV2,
) -> Result<VectorGenerationDescriptor, EncodingError> {
    let dimension = decoder.take_u32()?;
    let metric = take_metric(decoder)?;
    let codec = match decoder.take_u8()? {
        0x01 => ActiveVectorCodecV2::F32V1,
        unknown => return Err(unknown_discriminant("active vector codec", unknown)),
    };
    let score = match decoder.take_u8()? {
        0x01 => VectorScoreSemanticV2::CosineHalfF32V1,
        0x02 => VectorScoreSemanticV2::SquaredEuclideanF32V1,
        0x03 => VectorScoreSemanticV2::ManhattanF32V1,
        unknown => return Err(unknown_discriminant("vector score semantic", unknown)),
    };
    let norm = match decoder.take_u8()? {
        0x00 => CosineNormPolicyV2::NotApplicable,
        0x01 => CosineNormPolicyV2::RejectZeroScaledL2V1,
        unknown => return Err(unknown_discriminant("cosine norm policy", unknown)),
    };
    VectorGenerationDescriptor::try_new_with_routing(
        dimension,
        metric,
        codec,
        score,
        norm,
        routing_layout,
    )
    .map_err(model_error)
}

pub(super) fn put_partition(encoder: &mut ValueEncoder, partition: &TextPartition) {
    match partition {
        TextPartition::Unpartitioned => encoder.put_u8(0x01),
        TextPartition::TenantValue(value) => {
            encoder.put_u8(0x02);
            encoder.put_bytes(value);
        }
    }
}

pub(super) fn take_partition(
    decoder: &mut ValueDecoder<'_>,
) -> Result<TextPartition, EncodingError> {
    match decoder.take_u8()? {
        0x01 => Ok(TextPartition::Unpartitioned),
        0x02 => TextPartition::try_tenant_value(decoder.take_bytes(MAX_LENGTH_DELIMITED_FIELD)?)
            .map_err(work_model_error),
        unknown => Err(unknown_discriminant("partition", unknown)),
    }
}

pub(super) fn put_blob_ref(encoder: &mut ValueEncoder, blob: BlobRef) {
    encoder.put_raw(blob.hash());
    encoder.put_u64(blob.size());
}

pub(super) fn take_blob_ref(decoder: &mut ValueDecoder<'_>) -> Result<BlobRef, EncodingError> {
    Ok(BlobRef::new(
        decoder.take_array::<HASH_LEN>()?,
        decoder.take_u64()?,
    ))
}

pub(super) fn put_split_ref(encoder: &mut ValueEncoder, split: SplitRef) {
    put_blob_ref(encoder, split.blob());
    encoder.put_u64(split.footer_offset());
    encoder.put_u32(split.footer_length());
    encoder.put_u32(split.hot_cache_length());
    encoder.put_u64(split.total_size());
    match split.pruning() {
        SplitPruning::Unavailable => encoder.put_u8(0x00),
        SplitPruning::TermBloom256(bits) => {
            encoder.put_u8(0x01);
            for word in bits {
                encoder.put_u64(word);
            }
        }
    }
}

pub(super) fn take_split_ref(
    decoder: &mut ValueDecoder<'_>,
    pruning_version: bool,
) -> Result<SplitRef, EncodingError> {
    let blob = take_blob_ref(decoder)?;
    let footer_offset = decoder.take_u64()?;
    let footer_length = decoder.take_u32()?;
    let hot_cache_length = decoder.take_u32()?;
    let total_size = decoder.take_u64()?;
    let pruning = if pruning_version {
        match decoder.take_u8()? {
            0x00 => SplitPruning::Unavailable,
            0x01 => {
                let mut bits = [0; SPLIT_PRUNING_BLOOM_WORDS];
                for word in &mut bits {
                    *word = decoder.take_u64()?;
                }
                SplitPruning::TermBloom256(bits)
            }
            unknown => return Err(unknown_discriminant("split pruning", unknown)),
        }
    } else {
        SplitPruning::Unavailable
    };
    SplitRef::try_new(
        blob,
        footer_offset,
        footer_length,
        hot_cache_length,
        total_size,
        pruning,
    )
    .map_err(work_model_error)
}

pub(super) fn put_scope(encoder: &mut ValueEncoder, scope: DataScope) {
    match scope {
        DataScope::LegacyUnscoped => encoder.put_u8(0x00),
        DataScope::Tenant(tenant_id) => {
            encoder.put_u8(0x01);
            encoder.put_raw(&tenant_id.as_u128().to_be_bytes());
        }
    }
}

pub(super) fn take_scope(decoder: &mut ValueDecoder<'_>) -> Result<DataScope, EncodingError> {
    match decoder.take_u8()? {
        0x00 => Ok(DataScope::LegacyUnscoped),
        0x01 => Ok(DataScope::Tenant(TenantId::from_u128(u128::from_be_bytes(
            decoder.take_array::<16>()?,
        )))),
        unknown => Err(unknown_discriminant("scope", unknown)),
    }
}

pub(super) fn put_cursor(encoder: &mut ValueEncoder, cursor: Option<&IndexCursor>) {
    put_option(encoder, cursor, |encoder, cursor| {
        encoder.put_bytes(cursor.as_bytes())
    });
}

pub(super) fn take_cursor(
    decoder: &mut ValueDecoder<'_>,
) -> Result<Option<IndexCursor>, EncodingError> {
    decoder.take_option(|decoder| {
        IndexCursor::try_new(decoder.take_bytes(crate::index_lifecycle::INDEX_CURSOR_MAX_LEN)?)
            .map_err(operation_model_error)
    })
}

pub(super) fn put_counters(encoder: &mut ValueEncoder, counters: OperationCounters) {
    encoder.put_u64(counters.entities);
    encoder.put_u64(counters.input_bytes);
    encoder.put_u64(counters.output_operations);
    encoder.put_u64(counters.output_bytes);
}

pub(super) fn take_counters(
    decoder: &mut ValueDecoder<'_>,
) -> Result<OperationCounters, EncodingError> {
    Ok(OperationCounters {
        entities: decoder.take_u64()?,
        input_bytes: decoder.take_u64()?,
        output_operations: decoder.take_u64()?,
        output_bytes: decoder.take_u64()?,
    })
}

pub(super) fn put_claim(encoder: &mut ValueEncoder, claim: OperationClaim) {
    put_writer_epoch(encoder, claim.writer_epoch);
    encoder.put_u64(claim.sequence.get());
}

pub(super) fn take_claim(decoder: &mut ValueDecoder<'_>) -> Result<OperationClaim, EncodingError> {
    Ok(OperationClaim {
        writer_epoch: take_writer_epoch(decoder)?,
        sequence: ClaimSequence::new(decoder.take_u64()?).map_err(operation_model_error)?,
    })
}

pub(super) fn put_blocker(encoder: &mut ValueEncoder, blocker: &IndexOperationBlocker) {
    match blocker {
        IndexOperationBlocker::InvalidSourceData {
            entity_kind,
            entity_id,
        } => {
            encoder.put_u8(0x01);
            put_element_kind(encoder, *entity_kind);
            encoder.put_u64(entity_id.get());
        }
        IndexOperationBlocker::UniquenessViolation {
            first_entity_id,
            second_entity_id,
        } => {
            encoder.put_u8(0x02);
            encoder.put_u64(first_entity_id.get());
            encoder.put_u64(second_entity_id.get());
        }
        IndexOperationBlocker::OversizedEntity {
            entity_kind,
            entity_id,
            observed,
            limit,
        } => {
            encoder.put_u8(0x03);
            put_element_kind(encoder, *entity_kind);
            encoder.put_u64(entity_id.get());
            encoder.put_u64(*observed);
            encoder.put_u64(*limit);
        }
        IndexOperationBlocker::ManifestLimit {
            partition,
            observed,
            limit,
        } => {
            encoder.put_u8(0x04);
            put_partition(encoder, partition);
            encoder.put_u64(*observed);
            encoder.put_u64(*limit);
        }
        IndexOperationBlocker::ObjectStoreConfigurationUnavailable => encoder.put_u8(0x06),
        IndexOperationBlocker::InvariantViolation => encoder.put_u8(0x07),
        IndexOperationBlocker::InvalidLegacyPhysical => encoder.put_u8(0x0A),
    }
}

pub(super) enum DecodedIndexOperationBlocker {
    Current(IndexOperationBlocker),
    LegacyReaderCoordinationUnavailable,
}

pub(super) fn take_blocker(
    decoder: &mut ValueDecoder<'_>,
) -> Result<DecodedIndexOperationBlocker, EncodingError> {
    let blocker = match decoder.take_u8()? {
        0x01 => DecodedIndexOperationBlocker::Current(IndexOperationBlocker::InvalidSourceData {
            entity_kind: take_element_kind(decoder)?,
            entity_id: IndexEntityId::new(decoder.take_u64()?),
        }),
        0x02 => DecodedIndexOperationBlocker::Current(IndexOperationBlocker::UniquenessViolation {
            first_entity_id: IndexEntityId::new(decoder.take_u64()?),
            second_entity_id: IndexEntityId::new(decoder.take_u64()?),
        }),
        0x03 => DecodedIndexOperationBlocker::Current(IndexOperationBlocker::OversizedEntity {
            entity_kind: take_element_kind(decoder)?,
            entity_id: IndexEntityId::new(decoder.take_u64()?),
            observed: decoder.take_u64()?,
            limit: decoder.take_u64()?,
        }),
        0x04 => DecodedIndexOperationBlocker::Current(IndexOperationBlocker::ManifestLimit {
            partition: take_partition(decoder)?,
            observed: decoder.take_u64()?,
            limit: decoder.take_u64()?,
        }),
        0x05 => DecodedIndexOperationBlocker::LegacyReaderCoordinationUnavailable,
        0x06 => DecodedIndexOperationBlocker::Current(
            IndexOperationBlocker::ObjectStoreConfigurationUnavailable,
        ),
        0x07 => DecodedIndexOperationBlocker::Current(IndexOperationBlocker::InvariantViolation),
        0x0A => DecodedIndexOperationBlocker::Current(IndexOperationBlocker::InvalidLegacyPhysical),
        unknown => return Err(unknown_discriminant("operation blocker", unknown)),
    };
    if let DecodedIndexOperationBlocker::Current(blocker) = &blocker {
        blocker.validate().map_err(operation_model_error)?;
    }
    Ok(blocker)
}

pub(super) fn put_secondary_value(encoder: &mut ValueEncoder, value: &CanonicalSecondaryValue) {
    match value {
        CanonicalSecondaryValue::Equality(value) => {
            encoder.put_u8(0x01);
            encoder.put_raw(value.digest());
            encoder.put_bytes(value.canonical());
        }
        CanonicalSecondaryValue::Range(bytes) => {
            encoder.put_u8(0x02);
            encoder.put_u8(match bytes.direction() {
                StorageRangeIndexDirection::Asc => 0x01,
                StorageRangeIndexDirection::Desc => 0x02,
            });
            encoder.put_bytes(bytes.encoded());
        }
    }
}

pub(super) fn take_secondary_value(
    decoder: &mut ValueDecoder<'_>,
) -> Result<CanonicalSecondaryValue, EncodingError> {
    match decoder.take_u8()? {
        0x01 => {
            let digest = decoder.take_array::<{
                crate::encoding::v2::values::property::equality_index_value::EQUALITY_DIGEST_LEN
            }>()?;
            let canonical = decoder.take_bytes(MAX_LENGTH_DELIMITED_FIELD)?;
            Ok(CanonicalSecondaryValue::Equality(
                crate::encoding::v2::values::property::equality_index_value::CanonicalEqualityValue::try_from_parts(
                    digest,
                    canonical,
                )?,
            ))
        }
        0x02 => {
            let direction = match decoder.take_u8()? {
                0x01 => StorageRangeIndexDirection::Asc,
                0x02 => StorageRangeIndexDirection::Desc,
                unknown => return Err(unknown_discriminant("range value direction", unknown)),
            };
            Ok(CanonicalSecondaryValue::range(
                crate::encoding::v2::values::property::range_index_value::CanonicalRangeValue::try_from_encoded(
                    direction,
                    decoder.take_bytes(MAX_LENGTH_DELIMITED_FIELD)?,
                )?,
            ))
        }
        unknown => Err(unknown_discriminant("canonical secondary value", unknown)),
    }
}

pub(super) fn put_secondary_lane(encoder: &mut ValueEncoder, lane: SecondaryEntryLane) {
    encoder.put_u8(lane.as_u8());
}

pub(super) fn take_secondary_lane(
    decoder: &mut ValueDecoder<'_>,
) -> Result<SecondaryEntryLane, EncodingError> {
    SecondaryEntryLane::try_from_u8(decoder.take_u8()?)
}

pub(super) fn take_manifest_revision(
    decoder: &mut ValueDecoder<'_>,
) -> Result<TextManifestRevision, EncodingError> {
    TextManifestRevision::new(decoder.take_u64()?).map_err(model_error)
}

pub(super) fn take_logical_version(
    decoder: &mut ValueDecoder<'_>,
) -> Result<TextLogicalVersion, EncodingError> {
    TextLogicalVersion::new(decoder.take_u64()?).map_err(model_error)
}

pub(super) fn put_writer_epoch(encoder: &mut ValueEncoder, value: WriterEpoch) {
    encoder.put_uuid(value.as_bytes());
}

pub(super) fn take_writer_epoch(
    decoder: &mut ValueDecoder<'_>,
) -> Result<WriterEpoch, EncodingError> {
    WriterEpoch::from_bytes(decoder.take_array::<UUID_LEN>()?).map_err(model_error)
}

pub(super) fn put_element_kind(encoder: &mut ValueEncoder, kind: IndexElementKind) {
    encoder.put_u8(kind as u8);
}

pub(super) fn take_element_kind(
    decoder: &mut ValueDecoder<'_>,
) -> Result<IndexElementKind, EncodingError> {
    match decoder.take_u8()? {
        0x01 => Ok(IndexElementKind::Node),
        0x02 => Ok(IndexElementKind::Edge),
        unknown => Err(unknown_discriminant("element kind", unknown)),
    }
}

fn put_range_direction(encoder: &mut ValueEncoder, direction: RangeIndexDirection) {
    encoder.put_u8(match direction {
        RangeIndexDirection::Asc => 0x01,
        RangeIndexDirection::Desc => 0x02,
    });
}

fn take_range_direction(
    decoder: &mut ValueDecoder<'_>,
) -> Result<RangeIndexDirection, EncodingError> {
    match decoder.take_u8()? {
        0x01 => Ok(RangeIndexDirection::Asc),
        0x02 => Ok(RangeIndexDirection::Desc),
        unknown => Err(unknown_discriminant("range direction", unknown)),
    }
}

fn put_metric(encoder: &mut ValueEncoder, metric: VectorDistanceMetric) {
    encoder.put_u8(match metric {
        VectorDistanceMetric::Cosine => 0x01,
        VectorDistanceMetric::Euclidean => 0x02,
        VectorDistanceMetric::Manhattan => 0x03,
    });
}

fn take_metric(decoder: &mut ValueDecoder<'_>) -> Result<VectorDistanceMetric, EncodingError> {
    match decoder.take_u8()? {
        0x01 => Ok(VectorDistanceMetric::Cosine),
        0x02 => Ok(VectorDistanceMetric::Euclidean),
        0x03 => Ok(VectorDistanceMetric::Manhattan),
        unknown => Err(unknown_discriminant("vector metric", unknown)),
    }
}

fn take_component(
    decoder: &mut ValueDecoder<'_>,
    kind: &'static str,
) -> Result<IndexComponent, EncodingError> {
    IndexComponent::try_new(
        kind,
        decoder.take_string(crate::index_lifecycle::INDEX_COMPONENT_MAX_LEN)?,
    )
    .map_err(model_error)
}

pub(super) fn unknown_discriminant(kind: &str, value: u8) -> EncodingError {
    EncodingError::Custom(format!("unknown V2 {kind} discriminant {value:#04x}"))
}

pub(super) fn model_error(error: crate::index_lifecycle::IndexV2ModelError) -> EncodingError {
    EncodingError::Custom(error.to_string())
}

pub(super) fn operation_model_error(
    error: crate::index_lifecycle::IndexOperationModelError,
) -> EncodingError {
    EncodingError::Custom(error.to_string())
}

pub(super) fn work_model_error(
    error: crate::index_lifecycle::work::IndexWorkModelError,
) -> EncodingError {
    EncodingError::Custom(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decoder(bytes: &[u8]) -> ValueDecoder<'_> {
        ValueDecoder {
            version: INDEX_V2_VALUE_VERSION,
            kind: 0,
            remaining: bytes,
        }
    }

    #[test]
    fn bounded_primitives_reject_noncanonical_or_unbounded_frames_before_allocation() {
        let mut boolean = ValueDecoder::new(&[INDEX_V2_VALUE_VERSION, 0x01, 0x02]).unwrap();
        assert!(boolean.take_bool().is_err());

        let mut option = ValueDecoder::new(&[INDEX_V2_VALUE_VERSION, 0x01, 0x02]).unwrap();
        assert!(option.take_option(ValueDecoder::take_u8).is_err());

        let oversized_len = u32::try_from(MAX_LENGTH_DELIMITED_FIELD + 1)
            .unwrap()
            .to_be_bytes();
        let mut oversized = vec![INDEX_V2_VALUE_VERSION, 0x01];
        oversized.extend_from_slice(&oversized_len);
        let mut oversized = ValueDecoder::new(&oversized).unwrap();
        assert!(oversized.take_bytes(MAX_LENGTH_DELIMITED_FIELD).is_err());

        let mut invalid_utf8 =
            ValueDecoder::new(&[INDEX_V2_VALUE_VERSION, 0x01, 0x00, 0x00, 0x00, 0x01, 0xFF])
                .unwrap();
        assert!(invalid_utf8.take_string(1).is_err());

        let mut non_finite = vec![INDEX_V2_VALUE_VERSION, 0x01];
        non_finite.extend_from_slice(&f32::NAN.to_bits().to_be_bytes());
        let mut non_finite = ValueDecoder::new(&non_finite).unwrap();
        assert!(non_finite.take_f32().is_err());
    }

    #[test]
    #[should_panic(expected = "validated V2 float must be finite")]
    fn encoder_panics_if_internal_validation_admits_a_non_finite_float() {
        ValueEncoder::with_header(0x01).put_f32(f32::NAN);
    }

    #[test]
    fn closed_discriminants_reject_unknown_values() {
        assert!(take_identity(&mut decoder(&[0xFF])).is_err());
        assert!(take_definition(&mut decoder(&[0x01, 0xFF])).is_err());
        assert!(
            take_definition(&mut decoder(&[0x03, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF])).is_err()
        );
        assert!(take_definition(&mut decoder(&[0xFF])).is_err());
        assert!(
            take_physical_generation(&mut decoder(&[0x02, 0, 0, 0, 0, 0, 0, 0, 1, 0xFF,])).is_err()
        );
        assert!(take_physical_generation(&mut decoder(&[0xFF])).is_err());
        assert!(take_partition(&mut decoder(&[0xFF])).is_err());
        assert!(take_scope(&mut decoder(&[0xFF])).is_err());
        assert!(take_blocker(&mut decoder(&[0xFF])).is_err());
        assert!(take_secondary_value(&mut decoder(&[0xFF])).is_err());
        assert!(take_element_kind(&mut decoder(&[0xFF])).is_err());
        assert!(take_range_direction(&mut decoder(&[0xFF])).is_err());
        assert!(take_metric(&mut decoder(&[0xFF])).is_err());
    }

    #[test]
    fn secondary_equality_values_reject_digest_canonical_mismatches() {
        const HEADER_LEN: usize = U8_LEN + U8_LEN;
        const EQUALITY_VARIANT_LEN: usize = U8_LEN;
        const EQUALITY_DIGEST_OFFSET: usize = HEADER_LEN + EQUALITY_VARIANT_LEN;
        let value = CanonicalSecondaryValue::equality_string("value");
        let mut encoder = ValueEncoder::with_header(0x01);
        put_secondary_value(&mut encoder, &value);
        let mut encoded = encoder.finish().to_vec();
        encoded[EQUALITY_DIGEST_OFFSET] ^= 0xFF;
        let mut decoder = ValueDecoder::new(&encoded).unwrap();

        assert!(matches!(
            take_secondary_value(&mut decoder),
            Err(EncodingError::CanonicalEqualityDigestMismatch)
        ));
    }

    #[test]
    fn vector_definition_and_descriptor_reject_unsupported_semantics() {
        let mut definition = vec![0x02, 0x01];
        definition.extend_from_slice(&0_u32.to_be_bytes());
        definition.extend_from_slice(&0_u32.to_be_bytes());
        definition.push(0x00);
        definition.extend_from_slice(&1_u32.to_be_bytes());
        definition.extend_from_slice(&[0x01, 0xFF]);
        assert!(take_definition(&mut decoder(&definition)).is_err());

        let mut descriptor = 1_u32.to_be_bytes().to_vec();
        descriptor.extend_from_slice(&[0x01, 0xFF]);
        assert!(take_vector_descriptor(
            &mut decoder(&descriptor),
            VectorRoutingLayoutV2::SimHashDirectoryV1,
        )
        .is_err());

        let mut descriptor = 1_u32.to_be_bytes().to_vec();
        descriptor.extend_from_slice(&[0x01, 0x01, 0xFF]);
        assert!(take_vector_descriptor(
            &mut decoder(&descriptor),
            VectorRoutingLayoutV2::SimHashDirectoryV1,
        )
        .is_err());

        let mut descriptor = 1_u32.to_be_bytes().to_vec();
        descriptor.extend_from_slice(&[0x01, 0x01, 0x01, 0xFF]);
        assert!(take_vector_descriptor(
            &mut decoder(&descriptor),
            VectorRoutingLayoutV2::SimHashDirectoryV1,
        )
        .is_err());

        let mut manhattan = 1_u32.to_be_bytes().to_vec();
        manhattan.extend_from_slice(&[0x03, 0x01, 0x03, 0x00]);
        assert_eq!(
            take_vector_descriptor(
                &mut decoder(&manhattan),
                VectorRoutingLayoutV2::SimHashDirectoryV1,
            )
            .unwrap(),
            VectorGenerationDescriptor::try_new(
                1,
                VectorDistanceMetric::Manhattan,
                ActiveVectorCodecV2::F32V1,
                VectorScoreSemanticV2::ManhattanF32V1,
                CosineNormPolicyV2::NotApplicable,
            )
            .unwrap()
        );
    }

    #[test]
    fn identity_decoder_accepts_every_persisted_family() {
        for family in [0x01, 0x02, 0x03, 0x04] {
            let mut bytes = vec![family, 0x01];
            bytes.extend_from_slice(&1_u32.to_be_bytes());
            bytes.push(b'L');
            bytes.extend_from_slice(&1_u32.to_be_bytes());
            bytes.push(b'p');
            assert!(take_identity(&mut decoder(&bytes)).is_ok());
        }
    }

    #[test]
    fn operation_model_errors_are_projected_into_encoding_errors() {
        let error = operation_model_error(
            crate::index_lifecycle::IndexOperationModelError::ZeroClaimSequence,
        );
        assert!(error.to_string().contains("claim sequence"));
    }
}

#[inline]
pub(super) fn ensure_min_len(data: &[u8], expected: usize) -> Result<(), EncodingError> {
    if data.len() < expected {
        return Err(EncodingError::BufferTooShort {
            expected,
            actual: data.len(),
        });
    }

    Ok(())
}

#[inline]
pub(super) fn ensure_exact_len(data: &[u8], expected: usize) -> Result<(), EncodingError> {
    if data.len() != expected {
        return Err(EncodingError::BufferTooShort {
            expected,
            actual: data.len(),
        });
    }

    Ok(())
}

#[inline]
pub(super) fn checked_len_with_element_count(
    prefix_len: usize,
    count: usize,
    element_len: usize,
    overflow_message: &str,
) -> Result<usize, EncodingError> {
    let payload_len = count
        .checked_mul(element_len)
        .ok_or_else(|| EncodingError::Custom(overflow_message.to_string()))?;
    prefix_len
        .checked_add(payload_len)
        .ok_or_else(|| EncodingError::Custom(overflow_message.to_string()))?;
    Ok(payload_len + prefix_len)
}

#[inline]
pub(super) fn take_slice<'a>(
    data: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], EncodingError> {
    let start = *offset;
    let end = start
        .checked_add(len)
        .ok_or(EncodingError::BufferTooShort {
            expected: usize::MAX,
            actual: data.len(),
        })?;
    let slice = data.get(start..end).ok_or(EncodingError::BufferTooShort {
        expected: end,
        actual: data.len(),
    })?;
    *offset = end;
    Ok(slice)
}

#[inline]
pub(super) fn take_u8(data: &[u8], offset: &mut usize) -> Result<u8, EncodingError> {
    Ok(take_slice(data, offset, ENCODING_TYPE_LEN)?[0])
}

#[inline]
pub(super) fn take_u32_le(data: &[u8], offset: &mut usize) -> Result<usize, EncodingError> {
    let bytes = take_slice(data, offset, U32_LEN)?;
    Ok(u32::from_le_bytes(bytes.try_into().expect("u32 field is 4 bytes")) as usize)
}

#[inline]
pub(super) fn take_u32_be(data: &[u8], offset: &mut usize) -> Result<usize, EncodingError> {
    let bytes = take_slice(data, offset, U32_LEN)?;
    Ok(u32::from_be_bytes(bytes.try_into().expect("u32 field is 4 bytes")) as usize)
}

#[inline]
pub(super) fn take_u64_le(data: &[u8], offset: &mut usize) -> Result<u64, EncodingError> {
    let bytes = take_slice(data, offset, U64_LEN)?;
    Ok(u64::from_le_bytes(
        bytes.try_into().expect("u64 field is 8 bytes"),
    ))
}

#[cfg(test)]
mod raw_tests {
    use super::*;

    #[test]
    fn ensure_len_helpers_report_exact_contracts() {
        assert!(ensure_min_len(&[1, 2], 2).is_ok());
        assert!(matches!(
            ensure_min_len(&[1], 2),
            Err(EncodingError::BufferTooShort {
                expected: 2,
                actual: 1
            })
        ));
        assert!(ensure_exact_len(&[1, 2], 2).is_ok());
        assert!(matches!(
            ensure_exact_len(&[1, 2, 3], 2),
            Err(EncodingError::BufferTooShort {
                expected: 2,
                actual: 3
            })
        ));
    }

    #[test]
    fn checked_len_reports_multiplication_and_addition_overflow() {
        assert_eq!(
            checked_len_with_element_count(2, 3, 4, "overflow").unwrap(),
            14
        );
        assert!(matches!(
            checked_len_with_element_count(0, usize::MAX, 2, "overflow"),
            Err(EncodingError::Custom(message)) if message == "overflow"
        ));
        assert!(matches!(
            checked_len_with_element_count(usize::MAX, 1, 1, "overflow"),
            Err(EncodingError::Custom(message)) if message == "overflow"
        ));
    }

    #[test]
    fn take_helpers_advance_offsets_and_report_bounds() {
        let data = [0xAB, 1, 2, 3, 4, 8, 7, 6, 5, 4, 3, 2, 1];
        let mut offset = 0;

        assert_eq!(take_u8(&data, &mut offset).unwrap(), 0xAB);
        assert_eq!(offset, 1);
        assert_eq!(take_u32_be(&data, &mut offset).unwrap(), 0x0102_0304);
        assert_eq!(take_u32_le(&data, &mut offset).unwrap(), 0x0506_0708);
        assert_eq!(take_slice(&data, &mut offset, 2).unwrap(), &[4, 3]);

        let mut overflow_offset = usize::MAX;
        assert!(matches!(
            take_slice(&data, &mut overflow_offset, 1),
            Err(EncodingError::BufferTooShort {
                expected: usize::MAX,
                actual: 13
            })
        ));

        let mut short_offset = data.len();
        assert!(matches!(
            take_u64_le(&data, &mut short_offset),
            Err(EncodingError::BufferTooShort {
                expected: 21,
                actual: 13
            })
        ));
    }
}
