//! Canonical ordered node secondary-index key codecs.
//!
//! Ascending values retain their UTF-8 bytes. Descending values invert bytes,
//! escape embedded zeroes, and end with a fixed terminator. V2 generation
//! Generation-qualified entries reuse this value encoding through [`decode_range_value`], keeping
//! ordering and validation identical across both physical namespaces.

use crate::encoding::{
    error::EncodingError,
    indexes::{IndexPrefix, PropertyHash, INDEX_PREFIX_LEN, PROPERTY_HASH_MAX_LEN},
    keys::{KeyPrefix, ID_LEN, PREFIX_LEN},
    v2::keys::codec::read_u64,
    NodeId,
};
use bytes::BufMut;
use std::borrow::Cow;

/// Decodes one canonical ascending or descending range-value payload.
///
/// The caller owns the surrounding key layout; this contract validates only
/// the variable-length value bytes shared by legacy and V2 secondary rows.
pub(crate) fn decode_range_value(
    direction: RangeIndexDirection,
    value_bytes: &[u8],
) -> Result<Cow<'_, str>, EncodingError> {
    match direction {
        RangeIndexDirection::Asc => Ok(Cow::Borrowed(std::str::from_utf8(value_bytes)?)),
        RangeIndexDirection::Desc => {
            if !value_bytes.ends_with(&[0xFF, 0xFE]) {
                return Err(EncodingError::InvalidIndexKey(
                    "descending range value missing terminator".to_string(),
                ));
            }

            let mut decoded = Vec::with_capacity(value_bytes.len().saturating_sub(2));
            let mut index = 0;
            let value_body_len = value_bytes.len() - 2;
            while index < value_body_len {
                let byte = value_bytes[index];
                if byte == 0xFF {
                    if value_bytes.get(index + 1) != Some(&0x00) {
                        return Err(EncodingError::InvalidIndexKey(
                            "invalid descending range value escape".to_string(),
                        ));
                    }
                    decoded.push(0x00);
                    index += 2;
                } else {
                    decoded.push(!byte);
                    index += 1;
                }
            }

            Ok(Cow::Owned(std::str::from_utf8(&decoded)?.to_owned()))
        }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RangeIndexDirection {
    /// Store range keys in ascending value order.
    #[default]
    Asc,
    /// Store range keys in descending value order.
    Desc,
}

impl RangeIndexDirection {
    pub(crate) fn as_u8(&self) -> u8 {
        match self {
            RangeIndexDirection::Asc => 0x01,
            RangeIndexDirection::Desc => 0x05,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        match slice {
            [0x01] => Ok(RangeIndexDirection::Asc),
            [0x05] => Ok(RangeIndexDirection::Desc),
            _ => Err(EncodingError::InvalidIndexKey(
                "expected range index direction".to_string(),
            )),
        }
    }
}

/// Range index: property+value+nodeId -> presence
///
/// ```text
/// Asc:  [0x03][0x01][prop_hash:4][value:var][node_id:8]
/// Desc: [0x03][0x05][prop_hash:4][desc_value:var][node_id:8]
/// Value: empty/presence
/// ```
///
///
/// Note BOTH direction use this key variant but reside in different indexes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RangeIndexKey<'a> {
    pub(in crate::encoding::v2::keys::indexes) range_direction: RangeIndexDirection,
    property_hash: PropertyHash,
    pub(in crate::encoding::v2::keys::indexes) value: Cow<'a, str>,
    node_id: NodeId,
}

impl<'a> RangeIndexKey<'a> {
    pub fn new(
        range_direction: RangeIndexDirection,
        property_hash: PropertyHash,
        value: Cow<'a, str>,
        node_id: NodeId,
    ) -> Self {
        Self {
            range_direction,
            property_hash,
            value,
            node_id,
        }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix(&self) -> IndexPrefix {
        IndexPrefix::Range(self.range_direction)
    }

    pub(crate) fn value(&self) -> &str {
        self.value.as_ref()
    }

    pub(crate) const fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub(crate) const fn property_hash(&self) -> &PropertyHash {
        &self.property_hash
    }

    pub(crate) const fn direction(&self) -> RangeIndexDirection {
        self.range_direction
    }

    /// Parse a range index key from a slice.
    ///
    /// key is `[0x03][0x01][prop_hash:4][value:var][node_id:8]`
    pub fn parse_from_slice(slice: &'a [u8]) -> Result<Self, EncodingError> {
        // Checks AT LEAST the expected length
        // Variable length value means `value` and `node_id` access should be checked
        let expected = PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + ID_LEN;
        if slice.len() < expected {
            return Err(EncodingError::BufferTooShort {
                expected,
                actual: slice.len(),
            });
        }

        // |> key prefix
        // safe to do unwrap because we checked the length above
        let key_prefix = KeyPrefix::from_u8(*slice.first().unwrap())?;
        if key_prefix != RangeIndexKey::key_prefix() {
            return Err(EncodingError::Custom(format!(
                "expected PropertyIndex key prefix, got {:?}",
                key_prefix
            )));
        }

        // key prefix |> index direction prefix
        let direction = match IndexPrefix::from_slice(slice)? {
            IndexPrefix::Range(direction) => direction,
            IndexPrefix::Equality
            | IndexPrefix::EdgeEquality
            | IndexPrefix::EdgeLabel
            | IndexPrefix::EdgeLabelNeighbor(_)
            | IndexPrefix::EdgeRange(..)
            | IndexPrefix::GlobalEdgeEquality
            | IndexPrefix::GlobalEdgeRange(_) => {
                return Err(EncodingError::InvalidIndexKey(
                    "expected range index key".to_string(),
                ));
            }
        };

        let property_hash = slice
            [PREFIX_LEN + INDEX_PREFIX_LEN..PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN]
            .try_into()
            .expect("property hash slice is 4 bytes");

        let value_bytes = slice
            .get(PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN..slice.len() - ID_LEN)
            .ok_or(EncodingError::BufferTooShort {
                expected: PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + ID_LEN,
                actual: slice.len(),
            })?;
        let value = decode_range_value(direction, value_bytes)?;

        // prefix + index prefix + property hash + value |> node id
        let node_id = read_u64(slice, slice.len() - ID_LEN)?;
        Ok(Self::new(direction, property_hash, value, node_id))
    }

    /// Encode the range key into a buffer.
    ///
    /// For descending direction, the value is encoded as follows:
    /// - Each byte is inverted (0x00 -> 0xFF, 0xFF -> 0x00)
    /// - If a byte is 0x00, it is escaped with 0xFF 0x00
    /// - The final bytes are 0xFF 0xFE to indicate the end of the value.
    ///
    /// size of descending value is `2 + number of 0x00 bytes` longer than ascending value
    pub fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::PropertyIndex.as_u8());
        buf.put_u8(self.range_direction.as_u8());
        buf.put_slice(&self.property_hash);
        match self.range_direction {
            RangeIndexDirection::Asc => buf.put_slice(self.value.as_bytes()),
            RangeIndexDirection::Desc => {
                for byte in self.value.as_bytes().iter() {
                    buf.put_u8(!byte);
                    if *byte == 0x00 {
                        buf.put_u8(!0xFF);
                    }
                }
                buf.put_u8(!0x00);
                buf.put_u8(!0x01);
            }
        }
        buf.put_u64(self.node_id);
    }
}

impl From<&RangeIndexKey<'_>> for KeyPrefix {
    fn from(_: &RangeIndexKey<'_>) -> KeyPrefix {
        RangeIndexKey::key_prefix()
    }
}

impl From<&RangeIndexKey<'_>> for IndexPrefix {
    fn from(key: &RangeIndexKey<'_>) -> IndexPrefix {
        key.index_prefix()
    }
}
