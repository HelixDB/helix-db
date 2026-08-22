//! Equality-index node key codecs and byte-compatible parsing contracts.

use crate::encoding::{
    error::EncodingError,
    indexes::{
        IndexPrefix, PropertyHash, ValueHash, INDEX_PREFIX_LEN, PROPERTY_HASH_MAX_LEN,
        VALUE_HASH_MAX_LEN,
    },
    keys::{KeyPrefix, PREFIX_LEN},
};
use bytes::BufMut;

/// Equality index: property+value -> set of NodeIds
///
/// ```text
/// Key: [0x03][0x00][prop_hash:4][value_hash:8]
/// Value: RoaringTreemap<NodeId>
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EqualityIndexKey {
    pub(crate) property_hash: PropertyHash,
    pub(crate) value_hash: ValueHash,
}

impl EqualityIndexKey {
    pub fn new(property_hash: PropertyHash, value_hash: ValueHash) -> Self {
        Self {
            property_hash,
            value_hash,
        }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::PropertyIndex
    }

    #[inline]
    pub(crate) const fn index_prefix() -> IndexPrefix {
        IndexPrefix::Equality
    }

    /// Returns the exact scoped-property hash encoded by this row.
    #[cfg(test)]
    pub(crate) const fn property_hash(&self) -> &PropertyHash {
        &self.property_hash
    }

    /// Returns the exact indexed-value hash encoded by this row.
    #[cfg(test)]
    pub(crate) const fn value_hash(&self) -> &ValueHash {
        &self.value_hash
    }

    /// Parse the equality key from a slice.
    ///
    /// key is `[0x03][0x00][prop_hash:4][value_hash:8]`
    pub fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let expected = PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + VALUE_HASH_MAX_LEN;
        match slice.len().cmp(&expected) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidIndexKey(format!(
                    "expected {expected} bytes, got {}",
                    slice.len()
                )));
            }
        }

        // |> key prefix
        // safe to do unwrap because we checked the length above
        let key_prefix = KeyPrefix::from_u8(*slice.first().unwrap())?;
        if !matches!(key_prefix, KeyPrefix::PropertyIndex) {
            return Err(EncodingError::Custom(format!(
                "expected PropertyIndex key prefix, got {:?}",
                key_prefix
            )));
        }

        // key prefix |> index prefix
        let index_prefix = IndexPrefix::from_slice(slice)?;
        if !matches!(index_prefix, IndexPrefix::Equality) {
            return Err(EncodingError::Custom(format!(
                "expected Equality index prefix, got {:?}",
                index_prefix
            )));
        }

        // key prefix + index prefix |> property hash
        let property_hash = slice
            [PREFIX_LEN + INDEX_PREFIX_LEN..PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN]
            .try_into()
            .expect("property hash slice is 4 bytes");
        // key prefix + index prefix + property hash |> value
        let value_hash = slice[PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN
            ..PREFIX_LEN + INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN + VALUE_HASH_MAX_LEN]
            .try_into()
            .expect("value hash slice is 8 bytes");

        Ok(Self::new(property_hash, value_hash))
    }

    pub fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_slice(IndexPrefix::from(self).as_slice());
        buf.put_slice(&self.property_hash);
        buf.put_slice(&self.value_hash);
    }
}

impl From<&EqualityIndexKey> for KeyPrefix {
    fn from(_: &EqualityIndexKey) -> KeyPrefix {
        EqualityIndexKey::key_prefix()
    }
}

impl From<&EqualityIndexKey> for IndexPrefix {
    fn from(_: &EqualityIndexKey) -> IndexPrefix {
        EqualityIndexKey::index_prefix()
    }
}
