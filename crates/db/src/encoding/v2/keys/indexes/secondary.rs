//! Generation-qualified managed secondary-index keys.

use bytes::BufMut;

use crate::encoding::error::EncodingError;
use crate::encoding::v2::values::property::{
    equality_index_value::{CanonicalEqualityValue, EQUALITY_DIGEST_LEN},
    range_index_value::CanonicalRangeValue,
};
use crate::index_lifecycle::{IndexElementKind, IndexEntityId, IndexGenerationId, IndexId};

use super::super::{KEY_MAX_LEN, KIND_LEN, PREFIX_LEN, U32_LEN, U64_LEN};
use super::SecondaryEntryLane;

/// Deployed V3 generation-qualified equality entry key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SecondaryEqualityEntryKey {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) lane: SecondaryEntryLane,
    pub(crate) value: CanonicalEqualityValue,
    pub(crate) entity_id: Option<IndexEntityId>,
}

impl SecondaryEqualityEntryKey {
    pub(crate) fn try_new(
        index_id: IndexId,
        generation: IndexGenerationId,
        lane: SecondaryEntryLane,
        value: CanonicalEqualityValue,
        entity_id: Option<IndexEntityId>,
    ) -> Result<Self, EncodingError> {
        if !lane.is_equality() || lane.is_unique() != entity_id.is_none() {
            return Err(EncodingError::InvalidKey(
                "secondary equality lane/entity shape mismatch".to_string(),
            ));
        }
        let encoded_len = PREFIX_LEN
            + KIND_LEN
            + U64_LEN
            + U64_LEN
            + KIND_LEN
            + EQUALITY_DIGEST_LEN
            + U32_LEN
            + value.canonical().len()
            + entity_id.map_or(0, |_| U64_LEN);
        if encoded_len > KEY_MAX_LEN {
            return Err(EncodingError::InvalidKey(
                "secondary equality key exceeds 1 MiB".to_string(),
            ));
        }
        Ok(Self {
            index_id,
            generation,
            lane,
            value,
            entity_id,
        })
    }

    pub(crate) fn encoded_suffix_len(&self) -> usize {
        U64_LEN
            + U64_LEN
            + KIND_LEN
            + EQUALITY_DIGEST_LEN
            + U32_LEN
            + self.value.canonical().len()
            + self.entity_id.map_or(0, |_| U64_LEN)
    }

    pub(crate) fn encode_suffix<B: BufMut>(&self, buffer: &mut B) {
        buffer.put_u64(self.index_id.get());
        buffer.put_u64(self.generation.get());
        buffer.put_u8(self.lane.as_u8());
        buffer.put_slice(self.value.digest());
        buffer.put_u32(
            u32::try_from(self.value.canonical().len())
                .expect("canonical equality values are bounded below u32"),
        );
        buffer.put_slice(self.value.canonical());
        self.entity_id
            .iter()
            .for_each(|entity_id| buffer.put_u64(entity_id.get()));
    }
}

/// V4 non-unique equality key with all matching entities stored in its value.
///
/// The digest accelerates comparisons but is not authoritative. Construction
/// and parsing both retain the complete typed canonical value, whose codec
/// validates that the digest matches those bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SecondaryEqualityBitmapKey {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) element_kind: IndexElementKind,
    pub(crate) value: CanonicalEqualityValue,
}

impl SecondaryEqualityBitmapKey {
    pub(crate) fn try_new(
        index_id: IndexId,
        generation: IndexGenerationId,
        element_kind: IndexElementKind,
        value: CanonicalEqualityValue,
    ) -> Result<Self, EncodingError> {
        let encoded_len = PREFIX_LEN
            + KIND_LEN
            + U64_LEN
            + U64_LEN
            + KIND_LEN
            + EQUALITY_DIGEST_LEN
            + U32_LEN
            + value.canonical().len();
        if encoded_len > KEY_MAX_LEN {
            return Err(EncodingError::InvalidKey(
                "secondary equality bitmap key exceeds 1 MiB".to_string(),
            ));
        }
        Ok(Self {
            index_id,
            generation,
            element_kind,
            value,
        })
    }

    pub(crate) fn encoded_suffix_len(&self) -> usize {
        U64_LEN + U64_LEN + KIND_LEN + EQUALITY_DIGEST_LEN + U32_LEN + self.value.canonical().len()
    }

    pub(crate) fn encode_suffix<B: BufMut>(&self, buffer: &mut B) {
        buffer.put_u64(self.index_id.get());
        buffer.put_u64(self.generation.get());
        buffer.put_u8(self.element_kind as u8);
        buffer.put_slice(self.value.digest());
        buffer.put_u32(
            u32::try_from(self.value.canonical().len())
                .expect("canonical equality values are bounded below u32"),
        );
        buffer.put_slice(self.value.canonical());
    }
}

/// Deployed V3 generation-qualified range entry key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct SecondaryRangeEntryKey {
    pub(crate) index_id: IndexId,
    pub(crate) generation: IndexGenerationId,
    pub(crate) lane: SecondaryEntryLane,
    pub(crate) value: CanonicalRangeValue,
    pub(crate) entity_id: IndexEntityId,
}

impl SecondaryRangeEntryKey {
    pub(crate) fn try_new(
        index_id: IndexId,
        generation: IndexGenerationId,
        lane: SecondaryEntryLane,
        value: CanonicalRangeValue,
        entity_id: IndexEntityId,
    ) -> Result<Self, EncodingError> {
        let Some(direction) = lane.range_direction() else {
            return Err(EncodingError::InvalidKey(
                "secondary range key requires a range lane".to_string(),
            ));
        };
        if direction != value.direction() {
            return Err(EncodingError::InvalidKey(
                "secondary range lane/value direction mismatch".to_string(),
            ));
        }
        let encoded_len =
            PREFIX_LEN + KIND_LEN + U64_LEN + U64_LEN + KIND_LEN + value.encoded().len() + U64_LEN;
        if encoded_len > KEY_MAX_LEN {
            return Err(EncodingError::InvalidKey(
                "secondary range key exceeds 1 MiB".to_string(),
            ));
        }
        Ok(Self {
            index_id,
            generation,
            lane,
            value,
            entity_id,
        })
    }

    pub(crate) fn encoded_suffix_len(&self) -> usize {
        U64_LEN + U64_LEN + KIND_LEN + self.value.encoded().len() + U64_LEN
    }

    pub(crate) fn encode_suffix<B: BufMut>(&self, buffer: &mut B) {
        buffer.put_u64(self.index_id.get());
        buffer.put_u64(self.generation.get());
        buffer.put_u8(self.lane.as_u8());
        buffer.put_slice(self.value.encoded());
        buffer.put_u64(self.entity_id.get());
    }
}
