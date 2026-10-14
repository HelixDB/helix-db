//! Lifecycle entity-state values.

//! Canonical metadata, logical index-record, and operation codecs.

use bytes::Bytes;

use crate::encoding::error::EncodingError;
use crate::index_lifecycle::work::{
    AppliedEntityStateValue, AppliedFamilyState, CoalescedBuildDeltaState, CoalescedBuildDeltaValue,
};

use super::*;

pub(crate) fn encode_build_delta(value: &CoalescedBuildDeltaValue) -> Bytes {
    let mut encoder = ValueEncoder::with_header(BUILD_DELTA_KIND);
    put_index_id(&mut encoder, value.index_id);
    put_generation(&mut encoder, value.generation);
    put_element_kind(&mut encoder, value.entity_kind);
    encoder.put_u64(value.entity_id.get());
    match &value.state {
        CoalescedBuildDeltaState::Marker => {}
        CoalescedBuildDeltaState::SecondaryBefore(previous) => {
            encoder.put_u8(0x01);
            put_option(&mut encoder, previous.as_ref(), put_secondary_value);
        }
        CoalescedBuildDeltaState::VectorBefore(previous) => {
            encoder.put_u8(0x02);
            put_option(&mut encoder, previous.as_ref(), put_partition);
        }
    }
    encoder.finish()
}

pub(crate) fn decode_build_delta(value: &[u8]) -> Result<CoalescedBuildDeltaValue, EncodingError> {
    let mut decoder = ValueDecoder::new(value)?;
    if decoder.kind() != BUILD_DELTA_KIND {
        return Err(EncodingError::UnexpectedValueKind {
            expected: BUILD_DELTA_KIND,
            actual: decoder.kind(),
        });
    }
    let index_id = take_index_id(&mut decoder)?;
    let generation = take_generation(&mut decoder)?;
    let entity_kind = take_element_kind(&mut decoder)?;
    let entity_id = crate::index_lifecycle::IndexEntityId::new(decoder.take_u64()?);
    let state = if decoder.is_finished() {
        CoalescedBuildDeltaState::Marker
    } else {
        match decoder.take_u8()? {
            0x01 => CoalescedBuildDeltaState::SecondaryBefore(
                decoder.take_option(take_secondary_value)?,
            ),
            0x02 => CoalescedBuildDeltaState::VectorBefore(decoder.take_option(take_partition)?),
            unknown => return Err(unknown_discriminant("build-delta family state", unknown)),
        }
    };
    let decoded = CoalescedBuildDeltaValue {
        index_id,
        generation,
        entity_kind,
        entity_id,
        state,
    };
    decoder.finish()?;
    Ok(decoded)
}

pub(crate) fn encode_applied_state(value: &AppliedEntityStateValue) -> Bytes {
    let mut encoder = ValueEncoder::with_header(APPLIED_STATE_KIND);
    put_index_id(&mut encoder, value.index_id);
    put_generation(&mut encoder, value.generation);
    put_element_kind(&mut encoder, value.entity_kind);
    encoder.put_u64(value.entity_id.get());
    match &value.state {
        AppliedFamilyState::Secondary(state) => {
            encoder.put_u8(0x01);
            put_option(&mut encoder, state.as_ref(), put_secondary_value);
        }
        AppliedFamilyState::Vector(state) => {
            encoder.put_u8(0x02);
            put_option(&mut encoder, state.as_ref(), put_partition);
        }
        AppliedFamilyState::Text(state) => {
            encoder.put_u8(0x03);
            put_option(&mut encoder, state.as_ref(), |encoder, state| {
                put_partition(encoder, &state.0);
                encoder.put_u64(state.1.get());
            });
        }
    }
    encoder.finish()
}

pub(crate) fn decode_applied_state(value: &[u8]) -> Result<AppliedEntityStateValue, EncodingError> {
    let mut decoder = ValueDecoder::new(value)?;
    if decoder.kind() != APPLIED_STATE_KIND {
        return Err(EncodingError::UnexpectedValueKind {
            expected: APPLIED_STATE_KIND,
            actual: decoder.kind(),
        });
    }
    let index_id = take_index_id(&mut decoder)?;
    let generation = take_generation(&mut decoder)?;
    let entity_kind = take_element_kind(&mut decoder)?;
    let entity_id = crate::index_lifecycle::IndexEntityId::new(decoder.take_u64()?);
    let state = match decoder.take_u8()? {
        0x01 => AppliedFamilyState::Secondary(decoder.take_option(take_secondary_value)?),
        0x02 => AppliedFamilyState::Vector(decoder.take_option(take_partition)?),
        0x03 => AppliedFamilyState::Text(decoder.take_option(|decoder| {
            Ok((take_partition(decoder)?, take_logical_version(decoder)?))
        })?),
        unknown => return Err(unknown_discriminant("applied-state family", unknown)),
    };
    decoder.finish()?;
    Ok(AppliedEntityStateValue {
        index_id,
        generation,
        entity_kind,
        entity_id,
        state,
    })
}
