//! Retired pair-addressed edge property key.

use bytes::BufMut;

use crate::encoding::error::EncodingError;
use crate::encoding::v2::keys::codec::read_u64;
use crate::encoding::v2::keys::{KeyPrefix, NodeId, ID_LEN, PREFIX_LEN};

/// Retired `[0x01][from:8][to:8]` edge property key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LegacyEdgePropertyPairKey {
    from: NodeId,
    to: NodeId,
}

impl LegacyEdgePropertyPairKey {
    pub(crate) const fn new(from: NodeId, to: NodeId) -> Self {
        Self { from, to }
    }

    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::EdgePropertyPair
    }

    pub(crate) fn parse_from_slice(slice: &[u8]) -> Result<Self, EncodingError> {
        let expected = PREFIX_LEN + ID_LEN * 2;
        match slice.len().cmp(&expected) {
            core::cmp::Ordering::Less => {
                return Err(EncodingError::BufferTooShort {
                    expected,
                    actual: slice.len(),
                });
            }
            core::cmp::Ordering::Equal => {}
            core::cmp::Ordering::Greater => {
                return Err(EncodingError::InvalidKey(format!(
                    "expected {expected} bytes, got {}",
                    slice.len()
                )));
            }
        }
        if slice[0] != Self::key_prefix().as_u8() {
            return Err(EncodingError::InvalidKey(format!(
                "expected legacy edge property pair prefix ({:#04x}), got {:#04x}",
                Self::key_prefix().as_u8(),
                slice[0]
            )));
        }
        Ok(Self::new(
            read_u64(slice, PREFIX_LEN)?,
            read_u64(slice, PREFIX_LEN + ID_LEN)?,
        ))
    }

    pub(crate) const fn from(&self) -> NodeId {
        self.from
    }

    pub(crate) const fn to(&self) -> NodeId {
        self.to
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        PREFIX_LEN + ID_LEN * 2
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(Self::key_prefix().as_u8());
        buf.put_u64(self.from);
        buf.put_u64(self.to);
    }
}

impl From<&LegacyEdgePropertyPairKey> for KeyPrefix {
    fn from(_: &LegacyEdgePropertyPairKey) -> Self {
        LegacyEdgePropertyPairKey::key_prefix()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_pair_key_bytes_and_errors_are_frozen() {
        let key = LegacyEdgePropertyPairKey::new(1, 2);
        let mut encoded = Vec::new();
        key.encode_into(&mut encoded);
        assert_eq!(encoded, [1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2]);
        assert_eq!(
            LegacyEdgePropertyPairKey::parse_from_slice(&encoded).unwrap(),
            key
        );
        assert!(LegacyEdgePropertyPairKey::parse_from_slice(&encoded[..16]).is_err());
        assert!(
            LegacyEdgePropertyPairKey::parse_from_slice(&[encoded.as_slice(), &[0]].concat())
                .is_err()
        );
    }
}
