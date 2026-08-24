//! Stored values for lifecycle-managed range indexes.

use bytes::Bytes;

use crate::encoding::error::EncodingError;
use crate::encoding::v2::keys::SecondaryEntryLane;
use crate::index_lifecycle::work::SecondaryEntryValue;
use crate::index_lifecycle::IndexEntityId;

use super::super::{
    put_generation, put_index_id, put_secondary_lane, take_generation, take_index_id,
    take_secondary_lane, ValueDecoder, ValueEncoder,
};

const SECONDARY_ENTRY_KIND: u8 = 0x05;

/// Validated deployed range-row presence value.
///
/// Range membership is encoded entirely in the key. A non-empty value is not
/// a deployed row and must fail closed during lifecycle recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) struct SecondaryRangePresence;

#[cfg(any(test, feature = "fuzzing"))]
impl SecondaryRangePresence {
    /// Returns the exact deployed empty presence value.
    #[cfg(test)]
    pub(crate) const fn encode() -> Bytes {
        Bytes::new()
    }

    /// Accepts only the exact deployed empty presence value.
    pub(crate) fn decode(data: &[u8]) -> Result<Self, EncodingError> {
        if !data.is_empty() {
            return Err(EncodingError::Custom(format!(
                "secondary range presence value must be empty, got {} bytes",
                data.len()
            )));
        }
        Ok(Self)
    }
}

pub(crate) fn encode_entry(value: &SecondaryEntryValue) -> Result<Bytes, EncodingError> {
    if value.lane.is_equality() {
        return Err(EncodingError::Custom(
            "equality lane cannot use the range value codec".to_string(),
        ));
    }
    let mut encoder = ValueEncoder::with_header(SECONDARY_ENTRY_KIND);
    put_index_id(&mut encoder, value.index_id);
    put_generation(&mut encoder, value.generation);
    put_secondary_lane(&mut encoder, value.lane);
    encoder.put_u64(value.entity_id.get());
    Ok(encoder.finish())
}

pub(crate) fn decode_entry(
    expected_lane: SecondaryEntryLane,
    value: &[u8],
) -> Result<SecondaryEntryValue, EncodingError> {
    if expected_lane.is_equality() {
        return Err(EncodingError::Custom(
            "equality lane cannot use the range value codec".to_string(),
        ));
    }
    let mut decoder = ValueDecoder::new(value)?;
    if decoder.kind() != SECONDARY_ENTRY_KIND {
        return Err(EncodingError::UnexpectedValueKind {
            expected: SECONDARY_ENTRY_KIND,
            actual: decoder.kind(),
        });
    }
    let decoded = SecondaryEntryValue {
        index_id: take_index_id(&mut decoder)?,
        generation: take_generation(&mut decoder)?,
        lane: take_secondary_lane(&mut decoder)?,
        entity_id: IndexEntityId::new(decoder.take_u64()?),
    };
    decoder.finish()?;
    if decoded.lane != expected_lane {
        return Err(EncodingError::Custom(
            "secondary range key/value lane mismatch".to_string(),
        ));
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().fold(String::new(), |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to String cannot fail");
            output
        })
    }

    #[test]
    fn range_entry_value_is_lane_bound_and_byte_frozen() {
        let value = SecondaryEntryValue {
            index_id: crate::index_lifecycle::IndexId::new(1).unwrap(),
            generation: crate::index_lifecycle::IndexGenerationId::new(2).unwrap(),
            lane: SecondaryEntryLane::NodeRangeAscending,
            entity_id: IndexEntityId::new(3),
        };
        let encoded = encode_entry(&value).unwrap();

        assert_eq!(
            decode_entry(SecondaryEntryLane::NodeRangeAscending, &encoded).unwrap(),
            value
        );
        assert!(decode_entry(SecondaryEntryLane::NodeRangeDescending, &encoded).is_err());
        assert!(decode_entry(SecondaryEntryLane::NodeEquality, &encoded).is_err());
        insta::assert_snapshot!(
            hex(&encoded),
            @"010500000000000000010000000000000002030000000000000003"
        );
    }

    #[test]
    fn range_presence_accepts_only_the_deployed_empty_value() {
        assert_eq!(SecondaryRangePresence::encode(), Bytes::new());
        assert_eq!(
            SecondaryRangePresence::decode(&SecondaryRangePresence::encode()).unwrap(),
            SecondaryRangePresence
        );
        assert!(SecondaryRangePresence::decode(&[0]).is_err());
    }
}
