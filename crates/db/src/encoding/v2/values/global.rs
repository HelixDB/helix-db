//! Values stored under database-global V2 control keys.

use bytes::Bytes;

use crate::encoding::error::EncodingError;
use crate::index_lifecycle::{
    DroppedGenerationRows, DroppedGenerationValue, IndexDefinitionFamily, IndexStorageVersion,
    IndexV2MetadataValue, LegacyVectorPhysicalReservation, LogicalIndexIdWatermark,
    OperationQueuePointerValue, TextCompactionPointerValue, VectorPhysicalIdWatermark,
};

use super::*;

/// Layout byte of a dropped vector generation with one physical id, which
/// follows the byte.
const DROPPED_LAYOUT_UNPARTITIONED: u8 = 0x01;
/// Layout byte of a dropped partitioned vector generation.
const DROPPED_LAYOUT_PARTITIONED: u8 = 0x02;
/// Layout byte of a dropped secondary or text generation.
const DROPPED_LAYOUT_KEYED_BY_GENERATION: u8 = 0x03;

#[test]
fn dropped_generation_value_rejects_family_and_layout_pairs_that_do_not_exist() {
    let scope = crate::encoding::v2::keys::scope::DataScope::LegacyUnscoped;
    let encoded = encode_metadata_value(&IndexV2MetadataValue::DroppedGeneration(
        DroppedGenerationValue {
            scope,
            rows: DroppedGenerationRows::Secondary,
        },
    ));
    // [version][0x09][scope][family][layout]
    const FAMILY_OFFSET: usize = 3;
    const LAYOUT_OFFSET: usize = FAMILY_OFFSET + 1;
    let valid = [
        (0x01, DROPPED_LAYOUT_KEYED_BY_GENERATION),
        (0x02, DROPPED_LAYOUT_PARTITIONED),
        (0x03, DROPPED_LAYOUT_KEYED_BY_GENERATION),
    ];
    for family in 0..=u8::MAX {
        for layout in 0..=u8::MAX {
            let mut bytes = encoded.to_vec();
            bytes[FAMILY_OFFSET] = family;
            bytes[LAYOUT_OFFSET] = layout;
            assert_eq!(
                decode_metadata_value(&bytes).is_ok(),
                valid.contains(&(family, layout)),
                "family {family:#04x} layout {layout:#04x}"
            );
        }
    }
    // The unpartitioned layout needs its physical id, which must be non-zero,
    // and no layout accepts trailing bytes.
    let mut unpartitioned = encoded.to_vec();
    unpartitioned[FAMILY_OFFSET] = 0x02;
    unpartitioned[LAYOUT_OFFSET] = DROPPED_LAYOUT_UNPARTITIONED;
    assert!(decode_metadata_value(&unpartitioned).is_err());
    unpartitioned.extend_from_slice(&0_u64.to_be_bytes());
    assert!(decode_metadata_value(&unpartitioned).is_err());
    let mut trailing = encoded.to_vec();
    trailing.push(0);
    assert!(decode_metadata_value(&trailing).is_err());
}

#[test]
fn retired_membership_activation_value_is_not_reused() {
    for mode in 0..=u8::MAX {
        assert!(decode_metadata_value(&[INDEX_V2_VALUE_VERSION, 0x08, mode]).is_err());
    }
}

pub(crate) fn encode_metadata_value(value: &IndexV2MetadataValue) -> Bytes {
    let kind = match value {
        IndexV2MetadataValue::StorageVersion(_) => 0x01,
        IndexV2MetadataValue::LogicalIndexIdWatermark(_) => 0x02,
        IndexV2MetadataValue::VectorPhysicalIdWatermark(_) => 0x03,
        IndexV2MetadataValue::OperationQueuePointer(_) => 0x04,
        IndexV2MetadataValue::LegacyVectorPhysicalReservation(_) => 0x06,
        IndexV2MetadataValue::TextCompactionPointer(_) => 0x07,
        IndexV2MetadataValue::DroppedGeneration(_) => 0x09,
    };
    let mut encoder = ValueEncoder::with_header(kind);
    match value {
        IndexV2MetadataValue::StorageVersion(version) => encoder.put_u16(version.get()),
        IndexV2MetadataValue::LogicalIndexIdWatermark(watermark) => {
            put_index_id(&mut encoder, watermark.next_id)
        }
        IndexV2MetadataValue::VectorPhysicalIdWatermark(watermark) => {
            encoder.put_u64(watermark.next_id.get())
        }
        IndexV2MetadataValue::OperationQueuePointer(pointer) => {
            put_scope(&mut encoder, pointer.scope);
            put_index_id(&mut encoder, pointer.index_id);
            put_generation(&mut encoder, pointer.generation);
            put_operation_revision(&mut encoder, pointer.record_revision);
        }
        IndexV2MetadataValue::LegacyVectorPhysicalReservation(reservation) => match reservation {
            LegacyVectorPhysicalReservation::LegacySource => encoder.put_u8(0x01),
            LegacyVectorPhysicalReservation::AdoptionBuilding {
                index_id,
                generation,
                operation_id,
            } => {
                encoder.put_u8(0x02);
                put_index_id(&mut encoder, *index_id);
                put_generation(&mut encoder, *generation);
                put_operation_id(&mut encoder, *operation_id);
            }
            LegacyVectorPhysicalReservation::AdoptedActive {
                index_id,
                generation,
            } => {
                encoder.put_u8(0x03);
                put_index_id(&mut encoder, *index_id);
                put_generation(&mut encoder, *generation);
            }
            LegacyVectorPhysicalReservation::RetiringSource {
                index_id,
                generation,
            } => {
                encoder.put_u8(0x04);
                put_index_id(&mut encoder, *index_id);
                put_generation(&mut encoder, *generation);
            }
        },
        IndexV2MetadataValue::TextCompactionPointer(pointer) => {
            encoder.put_u64(pointer.revision.get())
        }
        IndexV2MetadataValue::DroppedGeneration(dropped) => {
            put_scope(&mut encoder, dropped.scope);
            let (family, layout) = match dropped.rows {
                DroppedGenerationRows::UnpartitionedVector { .. } => {
                    (IndexDefinitionFamily::Vector, DROPPED_LAYOUT_UNPARTITIONED)
                }
                DroppedGenerationRows::PartitionedVector => {
                    (IndexDefinitionFamily::Vector, DROPPED_LAYOUT_PARTITIONED)
                }
                DroppedGenerationRows::Secondary => (
                    IndexDefinitionFamily::Secondary,
                    DROPPED_LAYOUT_KEYED_BY_GENERATION,
                ),
                DroppedGenerationRows::Text => (
                    IndexDefinitionFamily::Text,
                    DROPPED_LAYOUT_KEYED_BY_GENERATION,
                ),
            };
            encoder.put_u8(family as u8);
            encoder.put_u8(layout);
            match dropped.rows {
                DroppedGenerationRows::UnpartitionedVector { physical_index_id } => {
                    encoder.put_u64(physical_index_id.get())
                }
                DroppedGenerationRows::PartitionedVector
                | DroppedGenerationRows::Secondary
                | DroppedGenerationRows::Text => {}
            }
        }
    }
    encoder.finish()
}

pub(crate) fn decode_metadata_value(value: &[u8]) -> Result<IndexV2MetadataValue, EncodingError> {
    let mut decoder = ValueDecoder::new(value)?;
    let decoded = match decoder.kind() {
        0x01 => {
            IndexV2MetadataValue::StorageVersion(IndexStorageVersion::new(decoder.take_u16()?)?)
        }
        0x02 => IndexV2MetadataValue::LogicalIndexIdWatermark(LogicalIndexIdWatermark {
            next_id: take_index_id(&mut decoder)?,
        }),
        0x03 => IndexV2MetadataValue::VectorPhysicalIdWatermark(VectorPhysicalIdWatermark {
            next_id: crate::index_lifecycle::VectorPhysicalIndexId::new(decoder.take_u64()?)
                .map_err(model_error)?,
        }),
        0x04 => IndexV2MetadataValue::OperationQueuePointer(OperationQueuePointerValue {
            scope: take_scope(&mut decoder)?,
            index_id: take_index_id(&mut decoder)?,
            generation: take_generation(&mut decoder)?,
            record_revision: take_operation_revision(&mut decoder)?,
        }),
        0x06 => IndexV2MetadataValue::LegacyVectorPhysicalReservation(match decoder.take_u8()? {
            0x01 => LegacyVectorPhysicalReservation::LegacySource,
            0x02 => LegacyVectorPhysicalReservation::AdoptionBuilding {
                index_id: take_index_id(&mut decoder)?,
                generation: take_generation(&mut decoder)?,
                operation_id: take_operation_id(&mut decoder)?,
            },
            0x03 => LegacyVectorPhysicalReservation::AdoptedActive {
                index_id: take_index_id(&mut decoder)?,
                generation: take_generation(&mut decoder)?,
            },
            0x04 => LegacyVectorPhysicalReservation::RetiringSource {
                index_id: take_index_id(&mut decoder)?,
                generation: take_generation(&mut decoder)?,
            },
            unknown => {
                return Err(unknown_discriminant(
                    "legacy vector physical reservation",
                    unknown,
                ));
            }
        }),
        0x07 => IndexV2MetadataValue::TextCompactionPointer(TextCompactionPointerValue {
            revision: crate::index_lifecycle::TextManifestRevision::new(decoder.take_u64()?)
                .map_err(model_error)?,
        }),
        // 0x08 is retired (experimental membership activation); do not reuse it.
        0x09 => {
            const SECONDARY: u8 = IndexDefinitionFamily::Secondary as u8;
            const VECTOR: u8 = IndexDefinitionFamily::Vector as u8;
            const TEXT: u8 = IndexDefinitionFamily::Text as u8;
            let scope = take_scope(&mut decoder)?;
            let rows = match (decoder.take_u8()?, decoder.take_u8()?) {
                (VECTOR, DROPPED_LAYOUT_UNPARTITIONED) => {
                    DroppedGenerationRows::UnpartitionedVector {
                        physical_index_id: crate::index_lifecycle::VectorPhysicalIndexId::new(
                            decoder.take_u64()?,
                        )
                        .map_err(model_error)?,
                    }
                }
                (VECTOR, DROPPED_LAYOUT_PARTITIONED) => DroppedGenerationRows::PartitionedVector,
                (SECONDARY, DROPPED_LAYOUT_KEYED_BY_GENERATION) => DroppedGenerationRows::Secondary,
                (TEXT, DROPPED_LAYOUT_KEYED_BY_GENERATION) => DroppedGenerationRows::Text,
                (family, layout) => {
                    return Err(EncodingError::Custom(format!(
                        "unknown V2 dropped generation family {family:#04x} with layout \
                         {layout:#04x}"
                    )));
                }
            };
            IndexV2MetadataValue::DroppedGeneration(DroppedGenerationValue { scope, rows })
        }
        unknown => return Err(unknown_discriminant("metadata value", unknown)),
    };
    decoder.finish()?;
    Ok(decoded)
}
