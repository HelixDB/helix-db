//! Lifecycle index-record values.

//! Canonical metadata, logical index-record, and operation codecs.

use bytes::Bytes;

use crate::encoding::error::EncodingError;
use crate::index_lifecycle::{IndexRecordV2, IndexStateV2};

use super::*;

pub(crate) fn encode_index_record(record: &IndexRecordV2) -> Bytes {
    let mut encoder = ValueEncoder::with_header(INDEX_RECORD_KIND);
    put_index_id(&mut encoder, record.index_id());
    put_identity(&mut encoder, record.identity());
    put_definition(&mut encoder, record.definition());
    put_revision(&mut encoder, record.revision());
    put_index_state(&mut encoder, record.state());
    encoder.finish()
}

/// Decodes and cross-validates a canonical logical index record.
pub(crate) fn decode_index_record(value: &[u8]) -> Result<IndexRecordV2, EncodingError> {
    let mut decoder = ValueDecoder::new(value)?;
    if decoder.kind() != INDEX_RECORD_KIND {
        return Err(EncodingError::UnexpectedValueKind {
            expected: INDEX_RECORD_KIND,
            actual: decoder.kind(),
        });
    }
    let index_id = take_index_id(&mut decoder)?;
    let identity = take_identity(&mut decoder)?;
    let definition = take_definition(&mut decoder)?;
    let revision = take_revision(&mut decoder)?;
    let state = take_index_state(&mut decoder)?;
    decoder.finish()?;
    IndexRecordV2::try_new(index_id, identity, definition, revision, state).map_err(model_error)
}

fn put_index_state(encoder: &mut ValueEncoder, state: &IndexStateV2) {
    match state {
        IndexStateV2::Building {
            physical,
            build_operation_id,
        } => {
            encoder.put_u8(0x01);
            put_physical_generation(encoder, physical);
            put_operation_id(encoder, *build_operation_id);
        }
        IndexStateV2::Active {
            physical,
            completed_build_operation_id,
        } => {
            encoder.put_u8(0x02);
            put_physical_generation(encoder, physical);
            put_operation_id(encoder, *completed_build_operation_id);
        }
        IndexStateV2::Aborting {
            physical,
            build_operation_id,
        } => {
            encoder.put_u8(0x03);
            put_physical_generation(encoder, physical);
            put_operation_id(encoder, *build_operation_id);
        }
        IndexStateV2::Dropping {
            physical,
            drop_operation_id,
        } => {
            encoder.put_u8(0x04);
            put_physical_generation(encoder, physical);
            put_operation_id(encoder, *drop_operation_id);
        }
        IndexStateV2::Dropped {
            last_generation,
            completed_operation_id,
        } => {
            encoder.put_u8(0x05);
            put_generation(encoder, *last_generation);
            put_operation_id(encoder, *completed_operation_id);
        }
    }
}

fn take_index_state(decoder: &mut ValueDecoder<'_>) -> Result<IndexStateV2, EncodingError> {
    match decoder.take_u8()? {
        0x01 => Ok(IndexStateV2::Building {
            physical: take_physical_generation(decoder)?,
            build_operation_id: take_operation_id(decoder)?,
        }),
        0x02 => Ok(IndexStateV2::Active {
            physical: take_physical_generation(decoder)?,
            completed_build_operation_id: take_operation_id(decoder)?,
        }),
        0x03 => Ok(IndexStateV2::Aborting {
            physical: take_physical_generation(decoder)?,
            build_operation_id: take_operation_id(decoder)?,
        }),
        0x04 => Ok(IndexStateV2::Dropping {
            physical: take_physical_generation(decoder)?,
            drop_operation_id: take_operation_id(decoder)?,
        }),
        0x05 => Ok(IndexStateV2::Dropped {
            last_generation: take_generation(decoder)?,
            completed_operation_id: take_operation_id(decoder)?,
        }),
        unknown => Err(unknown_discriminant("index state", unknown)),
    }
}
