//! Stored values for lifecycle-managed equality indexes.

use std::io::Cursor;

use bytes::Bytes;
use roaring::RoaringTreemap;

use crate::encoding::error::EncodingError;
use crate::encoding::v2::keys::SecondaryEntryLane;
use crate::index_lifecycle::work::SecondaryEntryValue;
use crate::index_lifecycle::IndexEntityId;

use super::super::{
    put_generation, put_index_id, put_secondary_lane, take_generation, take_index_id,
    take_secondary_lane, ValueDecoder, ValueEncoder,
};

const SECONDARY_ENTRY_KIND: u8 = 0x05;

/// Portable V4 value for one non-unique equality key.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SecondaryEqualityBitmapValue(RoaringTreemap);

impl SecondaryEqualityBitmapValue {
    pub(crate) fn new(ids: RoaringTreemap) -> Self {
        Self(ids)
    }

    pub(crate) fn encode(&self) -> Bytes {
        let mut bytes = Vec::new();
        self.0
            .serialize_into(&mut bytes)
            .expect("serializing a RoaringTreemap into memory is infallible");
        Bytes::from(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, EncodingError> {
        let mut cursor = Cursor::new(bytes);
        let ids = RoaringTreemap::deserialize_from(&mut cursor).map_err(|error| {
            EncodingError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to decode secondary equality bitmap: {error}"),
            ))
        })?;
        if cursor.position() != bytes.len() as u64 {
            return Err(EncodingError::Custom(format!(
                "secondary equality bitmap has {} trailing bytes",
                bytes.len() as u64 - cursor.position()
            )));
        }
        Ok(Self(ids))
    }

    pub(crate) fn ids(&self) -> &RoaringTreemap {
        &self.0
    }

    pub(crate) fn into_ids(self) -> RoaringTreemap {
        self.0
    }
}

pub(crate) fn encode_entry(value: &SecondaryEntryValue) -> Result<Bytes, EncodingError> {
    if !value.lane.is_equality() {
        return Err(EncodingError::Custom(
            "range lane cannot use the equality value codec".to_string(),
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
    if !expected_lane.is_equality() {
        return Err(EncodingError::Custom(
            "range lane cannot use the equality value codec".to_string(),
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
            "secondary equality key/value lane mismatch".to_string(),
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
    fn bitmap_value_round_trips_portable_roaring_bytes() {
        let ids = RoaringTreemap::from_iter([1, 9, u64::from(u32::MAX) + 1]);
        let encoded = SecondaryEqualityBitmapValue::new(ids.clone()).encode();
        let decoded = SecondaryEqualityBitmapValue::decode(&encoded).unwrap();

        assert_eq!(decoded.ids(), &ids);
        assert!(SecondaryEqualityBitmapValue::decode(b"not roaring").is_err());
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert!(SecondaryEqualityBitmapValue::decode(&trailing).is_err());
    }

    #[test]
    fn equality_entry_value_is_lane_bound_and_byte_frozen() {
        let value = SecondaryEntryValue {
            index_id: crate::index_lifecycle::IndexId::new(1).unwrap(),
            generation: crate::index_lifecycle::IndexGenerationId::new(2).unwrap(),
            lane: SecondaryEntryLane::NodeEquality,
            entity_id: IndexEntityId::new(3),
        };
        let encoded = encode_entry(&value).unwrap();

        assert_eq!(
            decode_entry(SecondaryEntryLane::NodeEquality, &encoded).unwrap(),
            value
        );
        assert!(decode_entry(SecondaryEntryLane::EdgeEquality, &encoded).is_err());
        assert!(decode_entry(SecondaryEntryLane::NodeRangeAscending, &encoded).is_err());
        insta::assert_snapshot!(
            hex(&encoded),
            @"010500000000000000010000000000000002010000000000000003"
        );
    }
}

/// Decoded current equality-row value containing node or edge identifiers.
///
/// The key family determines whether the identifiers are nodes or edges. The
/// value deliberately retains the deployed untyped integer bitmap so existing
/// bytes remain unchanged; callers receive it only after this codec validates
/// the portable Roaring representation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SecondaryEqualityValue(RoaringTreemap);

impl SecondaryEqualityValue {
    /// Encodes identifiers with the exact current portable Roaring format.
    pub(crate) fn encode_ids(ids: &RoaringTreemap) -> Bytes {
        let mut bytes = Vec::new();
        ids.serialize_into(&mut bytes)
            .expect("serializing a RoaringTreemap into memory is infallible");
        Bytes::from(bytes)
    }

    /// Decodes the exact current portable Roaring equality-row value.
    pub(crate) fn decode(data: &[u8]) -> Result<Self, EncodingError> {
        let ids = RoaringTreemap::deserialize_from(Cursor::new(data)).map_err(|error| {
            EncodingError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to decode RoaringTreemap: {error}"),
            ))
        })?;
        Ok(Self(ids))
    }

    /// Returns whether this physical equality row contains an exact entity ID.
    #[cfg(test)]
    pub(crate) fn contains(&self, id: u64) -> bool {
        self.0.contains(id)
    }

    /// Returns the number of entity IDs represented by this physical row.
    #[cfg(test)]
    pub(crate) fn len(&self) -> u64 {
        self.0.len()
    }

    /// Releases the validated identifier set to existing search callers.
    pub(crate) fn into_ids(self) -> RoaringTreemap {
        self.0
    }
}

#[cfg(test)]
mod deployed_row_tests {
    use super::*;

    #[test]
    fn equality_value_preserves_ids_and_rejects_malformed_bytes() {
        let ids = RoaringTreemap::from_iter([7, 9, u64::from(u32::MAX) + 1]);
        let encoded = SecondaryEqualityValue::encode_ids(&ids);
        let decoded = SecondaryEqualityValue::decode(&encoded).unwrap();

        assert_eq!(decoded.len(), 3);
        assert!(decoded.contains(7));
        assert!(decoded.contains(u64::from(u32::MAX) + 1));
        assert!(!decoded.contains(8));
        assert_eq!(decoded.into_ids(), ids);
        assert!(SecondaryEqualityValue::decode(b"not a bitmap").is_err());
    }
}
