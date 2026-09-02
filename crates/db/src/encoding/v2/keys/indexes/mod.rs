//! Canonical property, text, vector, and lifecycle-managed index keys.

use bytes::Bytes;

use crate::encoding::error::EncodingError;
use crate::encoding::v2::values::property::equality_index_value::CanonicalEqualityValue;
use crate::encoding::v2::values::property::range_index_value::CanonicalRangeValue;
use crate::index_lifecycle::{IndexEntityId, IndexGenerationId, IndexId};

pub(crate) mod canonical_label;
pub(crate) mod direction;
pub(crate) mod equality;
pub(crate) mod label;
pub(crate) mod prefix;
pub(crate) mod property;
pub(crate) mod range;
pub(crate) mod secondary;
pub(crate) mod text;
pub(crate) mod vector;

pub(crate) use canonical_label::CanonicalLabel;
pub(crate) use direction::EdgeDirection;
pub(crate) use prefix::IndexPrefix;
pub(crate) use property::{
    hash_property_name, hash_property_value, PropertyIndexKey, INDEX_PREFIX_LEN, NODE_ID_MAX_LEN,
    PROPERTY_HASH_MAX_LEN, VALUE_HASH_MAX_LEN,
};
pub use property::{PropertyHash, ValueHash};
use range::RangeIndexDirection;
pub(crate) use secondary::{
    SecondaryEqualityBitmapKey, SecondaryEqualityEntryKey, SecondaryRangeEntryKey,
};

/// Frozen generation-qualified V3 secondary lanes.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SecondaryEntryLane {
    NodeEquality = 0x01,
    NodeUniqueEquality = 0x02,
    NodeRangeAscending = 0x03,
    NodeRangeDescending = 0x04,
    EdgeEquality = 0x05,
    EdgeRangeAscending = 0x06,
    EdgeRangeDescending = 0x07,
}

impl SecondaryEntryLane {
    pub(crate) const fn as_u8(self) -> u8 {
        self as u8
    }

    pub(crate) fn try_from_u8(value: u8) -> Result<Self, EncodingError> {
        match value {
            0x01 => Ok(Self::NodeEquality),
            0x02 => Ok(Self::NodeUniqueEquality),
            0x03 => Ok(Self::NodeRangeAscending),
            0x04 => Ok(Self::NodeRangeDescending),
            0x05 => Ok(Self::EdgeEquality),
            0x06 => Ok(Self::EdgeRangeAscending),
            0x07 => Ok(Self::EdgeRangeDescending),
            unknown => Err(EncodingError::InvalidKey(format!(
                "unknown V2 secondary lane {unknown:#04x}"
            ))),
        }
    }

    pub(crate) const fn is_unique(self) -> bool {
        matches!(self, Self::NodeUniqueEquality)
    }

    pub(crate) const fn is_equality(self) -> bool {
        matches!(
            self,
            Self::NodeEquality | Self::NodeUniqueEquality | Self::EdgeEquality
        )
    }

    pub(crate) const fn range_direction(self) -> Option<RangeIndexDirection> {
        match self {
            Self::NodeRangeAscending | Self::EdgeRangeAscending => Some(RangeIndexDirection::Asc),
            Self::NodeRangeDescending | Self::EdgeRangeDescending => {
                Some(RangeIndexDirection::Desc)
            }
            Self::NodeEquality | Self::NodeUniqueEquality | Self::EdgeEquality => None,
        }
    }
}

/// Canonical secondary value bytes whose shape is fixed by the lane.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum CanonicalSecondaryValue {
    Equality(CanonicalEqualityValue),
    Range(CanonicalRangeValue),
}

impl CanonicalSecondaryValue {
    pub(crate) const fn equality(value: CanonicalEqualityValue) -> Self {
        Self::Equality(value)
    }

    #[cfg(test)]
    pub(crate) fn equality_string(value: &str) -> Self {
        let crate::encoding::v2::values::property::equality_index_value::EqualityValueProjection::Indexed(value) =
            crate::encoding::v2::values::property::equality_index_value::project_equality_value(
                &crate::encoding::property::property_value::PropertyValue::String(
                    value.to_string(),
                ),
            )
        else {
            panic!("string equality fixtures are always indexable");
        };
        Self::Equality(value)
    }

    pub(crate) const fn range(value: CanonicalRangeValue) -> Self {
        Self::Range(value)
    }

    #[cfg(test)]
    pub(crate) fn range_string(direction: RangeIndexDirection, value: &str) -> Self {
        let crate::encoding::v2::values::property::range_index_value::RangeValueProjection::Indexed(
            value,
        ) = crate::encoding::v2::values::property::range_index_value::project_range_value(
            &crate::encoding::property::property_value::PropertyValue::String(value.to_string()),
            direction,
        )
        else {
            panic!("string range fixtures are always indexable");
        };
        Self::Range(value)
    }

    pub(crate) fn try_encoded_range(
        direction: RangeIndexDirection,
        value: Bytes,
    ) -> Result<Self, EncodingError> {
        Ok(Self::Range(CanonicalRangeValue::try_from_encoded(
            direction, value,
        )?))
    }
}

/// Exhaustive typed dispatch for the deployed V3 secondary-entry record kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum SecondaryEntryKey {
    Equality(SecondaryEqualityEntryKey),
    Range(SecondaryRangeEntryKey),
}

impl SecondaryEntryKey {
    pub(crate) fn try_new(
        index_id: IndexId,
        generation: IndexGenerationId,
        lane: SecondaryEntryLane,
        value: CanonicalSecondaryValue,
        entity_id: Option<IndexEntityId>,
    ) -> Result<Self, EncodingError> {
        match value {
            CanonicalSecondaryValue::Equality(value) => {
                SecondaryEqualityEntryKey::try_new(index_id, generation, lane, value, entity_id)
                    .map(Self::Equality)
            }
            CanonicalSecondaryValue::Range(value) => {
                let Some(entity_id) = entity_id else {
                    return Err(EncodingError::InvalidKey(
                        "secondary range entry requires an entity ID".to_string(),
                    ));
                };
                SecondaryRangeEntryKey::try_new(index_id, generation, lane, value, entity_id)
                    .map(Self::Range)
            }
        }
    }

    pub(crate) const fn index_id(&self) -> IndexId {
        match self {
            Self::Equality(key) => key.index_id,
            Self::Range(key) => key.index_id,
        }
    }

    pub(crate) const fn generation(&self) -> IndexGenerationId {
        match self {
            Self::Equality(key) => key.generation,
            Self::Range(key) => key.generation,
        }
    }

    pub(crate) const fn lane(&self) -> SecondaryEntryLane {
        match self {
            Self::Equality(key) => key.lane,
            Self::Range(key) => key.lane,
        }
    }

    pub(crate) const fn entity_id(&self) -> Option<IndexEntityId> {
        match self {
            Self::Equality(key) => key.entity_id,
            Self::Range(key) => Some(key.entity_id),
        }
    }

    pub(crate) const fn range_value(&self) -> Option<&CanonicalRangeValue> {
        match self {
            Self::Equality(_) => None,
            Self::Range(key) => Some(&key.value),
        }
    }

    pub(crate) fn encoded_suffix_len(&self) -> usize {
        match self {
            Self::Equality(key) => key.encoded_suffix_len(),
            Self::Range(key) => key.encoded_suffix_len(),
        }
    }

    pub(crate) fn encode_suffix<B: bytes::BufMut>(&self, buffer: &mut B) {
        match self {
            Self::Equality(key) => key.encode_suffix(buffer),
            Self::Range(key) => key.encode_suffix(buffer),
        }
    }
}
