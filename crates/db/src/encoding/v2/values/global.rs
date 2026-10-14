//! Values stored under database-global V2 control keys.

use bytes::Bytes;

use crate::encoding::error::EncodingError;
use crate::index_lifecycle::{
    IndexStorageVersion, IndexV2MetadataValue, LegacyVectorPhysicalReservation,
    LogicalIndexIdWatermark, OperationQueuePointerValue, TextCompactionPointerValue,
    VectorPhysicalIdWatermark,
};

use super::*;

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
        unknown => return Err(unknown_discriminant("metadata value", unknown)),
    };
    decoder.finish()?;
    Ok(decoded)
}
