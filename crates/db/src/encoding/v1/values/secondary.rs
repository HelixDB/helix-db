//! Byte-compatible codecs for current secondary-index row values.
//!
//! Equality rows persist a portable [`RoaringTreemap`] while range rows use an
//! empty value as a presence marker. This module owns both deployed value
//! contracts so search, V2 lifecycle recovery, and cleanup never deserialize or
//! validate those bytes independently. It does not add a version header or
//! otherwise change the current physical format.

use std::io::Cursor;

use bytes::Bytes;
use roaring::RoaringTreemap;

use crate::encoding::error::EncodingError;

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

/// Validated current range-row presence value.
///
/// Range membership is encoded entirely in the key. A non-empty value is not
/// a current-format row and must fail closed during V2 lifecycle recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) struct SecondaryRangePresence;

#[cfg(any(test, feature = "fuzzing"))]
impl SecondaryRangePresence {
    /// Returns the exact deployed empty presence value.
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

#[cfg(test)]
mod tests {
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
