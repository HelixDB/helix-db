//! Metadata keys for allocators, manifests, and scoped database state.

use bytes::{BufMut, Bytes};

use crate::encoding::error::EncodingError;

use super::{KeyPrefix, PREFIX_LEN};

/// DataKey for next node ID high watermark (for lease-based allocation)
pub const NEXT_NODE_ID: &[u8] = b"next_node_id";
/// DataKey for next edge ID high watermark (for lease-based allocation)
pub const NEXT_EDGE_ID: &[u8] = b"next_edge_id";
/// Metadata storage key.
///
/// ```text
/// [0xFF][name:var]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MetadataKey<'a> {
    name: &'a [u8],
}

impl<'a> MetadataKey<'a> {
    pub(crate) const fn new(name: &'a [u8]) -> Self {
        Self { name }
    }

    #[inline]
    pub(crate) const fn key_prefix() -> KeyPrefix {
        KeyPrefix::Metadata
    }

    #[inline]
    pub(crate) fn parse_from_slice(slice: &'a [u8]) -> Result<Self, EncodingError> {
        if slice.len() < PREFIX_LEN {
            return Err(EncodingError::BufferTooShort {
                expected: PREFIX_LEN,
                actual: slice.len(),
            });
        }
        if slice[0] != Self::key_prefix().as_u8() {
            return Err(EncodingError::InvalidKey(format!(
                "expected Metadata key prefix ({:#04x}), got {:#04x}",
                Self::key_prefix().as_u8(),
                slice[0]
            )));
        }

        Ok(Self::new(&slice[PREFIX_LEN..]))
    }

    pub(crate) const fn name(&self) -> &'a [u8] {
        self.name
    }

    pub(crate) const fn encoded_len(&self) -> usize {
        PREFIX_LEN + self.name.len()
    }

    pub(crate) fn encode_into<B: BufMut>(&self, buf: &mut B) {
        buf.put_u8(KeyPrefix::from(self).as_u8());
        buf.put_slice(self.name);
    }

    pub(crate) fn to_bytes(self) -> Bytes {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        Bytes::from(buf)
    }
}

impl<'a> MetadataKey<'a> {
    /// DataKey for next node ID high watermark
    #[inline]
    pub fn next_node_id_key() -> Self {
        Self::new(NEXT_NODE_ID)
    }

    /// DataKey for next edge ID high watermark
    #[inline]
    pub fn next_edge_id_key() -> Self {
        Self::new(NEXT_EDGE_ID)
    }
}

impl<'a> From<&MetadataKey<'a>> for KeyPrefix {
    fn from(_: &MetadataKey<'a>) -> KeyPrefix {
        MetadataKey::key_prefix()
    }
}
